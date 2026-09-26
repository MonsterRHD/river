# 生产者租约生命周期修正 - 产品需求文档

## Overview

- **Summary**: 为 River worker 的 producer（生产者）引入持久、可续期、带代际栅栏（fencing generation）的数据库租约：启动时获取，运行中续期，正常停止时主动释放，进程崩溃后由当前维护领导者在租约过期后回收，并以事务内通知保证下线变化在领导者切换间不漏发、不重发。
- **Purpose**: 进程被强杀后，运维面与后续实例在租约过期后即可准确判断生产者下线，避免看到错误的队列容量与活跃状态；同时防止旧进程迟到的心跳在租约易主后复活新记录。
- **Target Users**: 部署多实例 River worker 的运维人员、依赖生产者/队列活跃状态的上层系统（如运维面/UI）、River 维护者。

## Goals

- 每次 producer 启动都在数据库中持有一行可持久辨识的租约记录，租约带有明确的过期时间与单调代际号。
- 租约按固定间隔续期；正常 `Stop`（含队列移除、客户端优雅关闭）在有限重试内主动释放租约。
- 进程崩溃（无法主动释放）时，租约在 TTL 过期后由**当前领导者**运行的回收服务标记下线，并物理清理长期已下线的记录。
- 续期、释放、回收三类写操作都以 `(queue_name, client_id, generation)` 为栅栏条件：旧代际实例的迟到心跳/释放影响 0 行，绝不覆盖或复活新代际记录；同代际的续期与回收竞争由数据库行锁串行化，条件在语句内重新求值，只有租约的合法持有者能赢。
- 续期或通知遇到短暂数据库故障时退避重试，并以数据库中的持久状态为准进行恢复；租约已被回收则重新获取，租约已被更新代际取代则本实例停止抓取新作业并排空在途作业。
- 下线变化（主动释放与崩溃回收）通过数据库事务内通知发布；领导者切换后以持久状态对账，保证不漏发、不重复发布。
- 只入队、不执行作业的客户端不产生任何 producer 租约记录。
- 既有 `river_queue` 队列清理（24h 保留期）与作业抓取/执行/救援语义保持不变。

## Non-Goals

- 不新增面向终端用户的 Client 公共 API（如新的订阅事件类型、队列容量查询接口）；`river_producer` 与驱动查询作为内部能力（`riverdriver` 本就是内部适配层）。
- 不改变 `JobGetAvailable`/job completer/job rescuer 等既有作业处理路径的行为。
- 不改变 `river_queue` 表结构、`QueueCleaner` 删除条件与保留期。
- 不实现跨网络的"僵尸进程"强杀；被代际栅栏淘汰的实例以停止抓取并优雅排空为边界。
- 不改动 River Pro 专有 pilot；`StandardPilot` 之外的 pilot 行为不在本次范围内（接口保持可实现）。

## Background & Context

- 现状：`StandardPilot.ProducerInit` 仅分配进程内自增序号，`ProducerKeepAlive` 与 `ProducerShutdown` 均为 no-op（见 [standard_pilot.go](file:///Users/ding/Documents/swe/09224/project-02/rivershared/riverpilot/standard_pilot.go#L74-L85)）；活跃信号仅靠 producer 每 10 分钟刷新 `river_queue.updated_at`，24 小时后才被 `QueueCleaner` 删除。
- 早期迁移（005）曾建过 `river_client`/`river_client_queue` 表，但 007 迁移已将其删除；`riverdriver.ProducerKeepAliveParams` 与 pilot 钩子保留了形状但未持久化。
- 领导者机制成熟可复用：`Elector` 选举 + TTL 续租；`QueueMaintainer` 仅在领导者节点上运行维护服务（[queue_maintainer_leader.go](file:///Users/ding/Documents/swe/09224/project-02/internal/maintenance/queue_maintainer_leader.go)）。
- 通知抽象统一为 `Executor.NotifyMany`：Postgres 在事务内 `pg_notify`（提交才投递，回滚即丢弃，见 `LeaderResign` 的 CTE 写法），SQLite 写入 `river_notification` outbox 由监听器轮询。两种机制都支持"状态变更与通知同一事务提交"。
- Producer 仅在 `Config.willExecuteJobs()`（配置了 Queues）时创建（[client.go](file:///Users/ding/Documents/swe/09224/project-02/client.go#L902-L933)）；纯入队客户端天然没有 producer。
- 三驱动：riverpgxv5、riverdatabasesql（与 pgx 共用 pg 方言 SQL，迁移由 rsync 同步）、riversqlite（时间以 `'2006-01-02 15:04:05.000'` UTC 文本存储，TTL 用 Go 计算后传入）。
- 环境无 `sqlc` 二进制，生成代码需按 sqlc v1.31.0 既有风格手写，并通过编译与 `make test` 验证；CI 另以 `sqlc diff` 校验，故 SQL 源文件与生成代码必须严格同构。

## Functional Requirements

- **FR-1（租约表与迁移）**: 新增迁移 008，在三驱动创建 `river_producer` 表：主键 `(queue_name, client_id)`，列包含 `producer_id bigint`（每次启动随机生成的实例标识）、`generation bigint`（代际，初始 1）、`max_workers bigint`（声明容量）、`created_at`、`updated_at`、`expires_at`、`reaped_at`（NULL 表示有效租约）；并在 `(expires_at)` 上建立仅覆盖未回收行的部分索引。提供 down 迁移。
- **FR-2（获取租约）**: producer 启动时执行 upsert：插入新租约；同槽位冲突时 `generation + 1`、写入新的 `producer_id`/`max_workers`/`expires_at`、清空 `reaped_at`；RETURNING 返回最终行。producer 在内存状态中持有本次的 `generation` 与 `producer_id`。
- **FR-3（续期）**: producer 按 `ProducerReportInterval`（默认 30s）执行栅栏续期：仅当 `(queue_name, client_id, generation)` 匹配且 `reaped_at IS NULL` 时刷新 `updated_at`/`expires_at = now + TTL`（TTL 默认 5 分钟），并返回更新后行；不匹配时返回"未找到"。
- **FR-4（主动释放）**: producer 优雅停止时，在事务内执行栅栏释放（仅同代际且未回收时置 `reaped_at = now`），并在**同一事务**为发生状态转换的行发布一条 offline 通知；影响 0 行则不发布。释放沿用现有最多 4 次指数退避重试，且在关闭上下文取消/连接池关闭时优雅放弃。
- **FR-5（领导者回收）**: 新增仅在领导者上运行的 `ProducerReaper` 维护服务（默认间隔 5s，启动带 stagger 并立即执行首轮）。每批在一个事务内：把 `expires_at < now AND reaped_at IS NULL` 的行置 `reaped_at = now` 并 RETURNING；对被回收行逐条（可合并为一次 NotifyMany）在同事务发布 offline 通知；提交。循环分批直至取不满一批。
- **FR-6（物理清理）**: `ProducerReaper` 顺带分批删除 `reaped_at < now - 保留期`（默认 24h）的已下线行；该清理只作用于新表，不触碰 `river_queue` 与 `QueueCleaner`。
- **FR-7（故障退避与持久状态恢复）**: 续期遇到普通数据库错误时记录日志并等待下一周期重试（既有 ticker 循环即退避）；续期返回未找到时，先 `ProducerGet` 读取持久状态：行不存在或已回收 → 重新执行 FR-2 获取新代际租约后继续；行存在但代际更新（已被另一实例接管）→ 记录错误日志，停止抓取新作业并进入既有优雅排空/关闭路径，其最终释放因代际不匹配而影响 0 行、不发通知。回收服务任一批失败（含通知失败导致事务回滚）时回滚该批并在下一周期重试。
- **FR-8（下线通知 exactly-once 语义）**: offline 通知与 `reaped_at` 状态转换在同一事务；谓词 `reaped_at IS NULL` 保证同一行的下线转换只发生一次。领导者切换后，新领导者首轮即以持久行对账：已回收行不重复通知，未回收的过期行补做回收与通知。通知负载包含 `action`、`queue`、`client_id`、`producer_id`、`generation`，新主题为内部主题 `river_producer`。
- **FR-9（纯入队客户端无租约）**: 未配置 Queues 的客户端启动、入队、停止全过程不写 `river_producer`，也不启动 producer/reaper。
- **FR-10（语义保持）**: `river_queue` 的创建/刷新/暂停/恢复/清理行为不变；作业抓取、完成、重试、救援行为与参数不变；producer 报告队列状态（10 分钟刷 `river_queue.updated_at`）保持。
- **FR-11（三驱动一致）**: pgx、database/sql、sqlite 三驱动实现同一批驱动查询与参数/结果类型，迁移与 sqlc 源/生成代码同步；`riverdrivertest` 新增共享一致性套件覆盖所有新查询的栅栏语义。
- **FR-12（容量可辨识）**: 租约行持久记录 `max_workers`，活跃生产者集合的权威定义为 `reaped_at IS NULL AND expires_at >= now` 的行；运维面据此即可得到准确的活跃状态与容量，无需依赖 `river_queue.updated_at` 推断。

## Non-Functional Requirements

- **NFR-1（正确性/并发）**: 续期与回收的竞争安全由数据库单条语句的行锁 + WHERE 重新求值保证，不依赖应用侧互斥；必须有并发/跨代际测试证据。
- **NFR-2（可测性）**: 复用 `baseservice`/`startstop`/`testsignal` 模式为关键路径暴露测试信号；关键时序用短间隔配置驱动，避免固定 sleep。
- **NFR-3（可观测性）**: 获取、续期失败、代际丢失、回收、释放均有结构化日志（debug/error/warn 分级合理）。
- **NFR-4（兼容性）**: 遵循仓库迁移规范（pgx 与 databasesql 迁移逐字节同步）、gci/gofumpt 格式、sqlc v1.31.0 生成代码风格；无新增第三方依赖。
- **NFR-5（性能）**: 回收查询走部分索引，常规运行（无过期行）为廉价索引扫描；批量与短事务避免锁膨胀；续期频率维持现状（30s）。

## Constraints

- **Technical**: Go 1.27；多模块 workspace；sqlc 不可用需手写生成代码；SQLite 时间为 UTC 毫秒文本、TTL 在 Go 侧计算；SQLite 不依赖外键级联（驱动未启用 foreign_keys），物理清理须由应用查询完成。
- **Business**: 不破坏 007 版本既有数据库的升级路径（008 只增表/索引）；不改变公共 API 语义。
- **Dependencies**: `Elector`/`QueueMaintainerLeader` 的领导者生命周期；`Notifier`/`NotifyMany`；三驱动迁移与 sqlc 体系；`testsignal`/`startstop` 测试设施。

## Assumptions

- `client_id` 在同一时刻的同一队列上唯一标识一个进程槽位；部署重叠期（滚动发布）可能出现两个同 `client_id` 进程并存，代际栅栏正是为该场景设计。
- 时钟以数据库服务器时间为准（续期/回收均由 `now()` 或调用方传入的当前时间计算），与既有 leader TTL 机制一致。
- offline 通知的消费方当前在仓库内无订阅者；通知主题与负载作为内部契约先行落地，由上层系统订阅，测试通过监听器/outbox 表验证投递。
- 回收间隔 5s、TTL 5min、物理保留 24h 作为内部默认常量，不新增用户配置项。

## Acceptance Criteria

### AC-1: 启动获取持久代际租约
- **Type**: `rule`
- **Given**: 一个配置了队列的客户端启动 producer
- **When**: `ProducerInit` 完成
- **Then**: `river_producer` 中存在 `(queue_name, client_id)` 行，`generation >= 1`、`producer_id` 非零、`max_workers` 等于队列配置、`reaped_at IS NULL`、`expires_at > now`；producer 内存状态持有返回的 generation
- **Pass Condition**: 三驱动一致性套件与 producer 集成测试断言行内容与内存代际一致
- **Evidence**: `riverdrivertest` 新套件输出、`producer_test.go` 新增测试通过

### AC-2: 同槽位再次启动代际单调递增
- **Type**: `rule`
- **Given**: 槽位已存在一行（任意 generation G）
- **When**: 同一 `(queue, client_id)` 再次执行租约获取
- **Then**: 同一行 `generation = G + 1`、`reaped_at` 被清空、`producer_id` 更新为新值、`expires_at` 刷新；`created_at` 语义合理（沿用首次创建时间）
- **Pass Condition**: 连续两次 upsert 后代际严格 +1 且不新增第二行
- **Evidence**: 一致性套件断言

### AC-3: 栅栏续期只刷新同代际有效租约
- **Type**: `rule`
- **Given**: 槽位代际为 G2（G2 由 G1 递增而来），另有持有 G1 的旧 producer
- **When**: G1 以旧代际执行续期；G2 以当前代际执行续期
- **Then**: G1 续期返回未找到且行的 `expires_at`/`updated_at`/`generation` 均不被改动；G2 续期成功并把 `expires_at` 延长到 `now + TTL`
- **Pass Condition**: 直接驱动测试：旧代际续期影响 0 行、新代际续期影响 1 行且时间戳被刷新
- **Evidence**: 一致性套件 + 竞态测试

### AC-4: 正常停止主动释放并发布下线通知
- **Type**: `rule`
- **Given**: 持有代际 G 的 producer 正在运行且有监听器订阅 `river_producer`
- **When**: producer 优雅停止
- **Then**: 行被置 `reaped_at`（仍保留在表中）；同事务投递恰好一条 offline 通知，负载中的 queue/client_id/producer_id/generation 与租约一致；停止流程在既有重试预算内完成
- **Pass Condition**: 收到恰一条通知且行已标记；再次停止/重复释放不产生第二条通知
- **Evidence**: producer 集成测试（pg LISTEN 与 sqlite outbox 两路至少各有覆盖）

### AC-5: 崩溃后由当前领导者在过期后回收
- **Type**: `rule`
- **Given**: producer 进程消失且未释放，租约 `expires_at` 已过期；另存在未过期租约与已回收租约各行
- **When**: 领导者 `ProducerReaper` 运行
- **Then**: 仅过期且 `reaped_at IS NULL` 的行被置 `reaped_at` 并发布 offline 通知；未过期行与已回收行不动
- **Pass Condition**: 一次运行后集合差异与通知条数精确匹配；再运行一次为零变更零通知
- **Evidence**: `producer_reaper_test.go`

### AC-6: 续期与回收竞争的同代际/跨代际胜负
- **Type**: `rule`
- **Given**: 代际 G 的租约接近/达到过期点，回收与续期并发提交
- **When**: 两事务围绕同一行竞争（交错两种先后顺序各验证）
- **Then**: 续期先提交则其新 `expires_at` 在未来，回收语句重新求值后不命中、行保持有效；回收先提交则 `reaped_at` 被置位，同代际续期因 `reaped_at IS NULL` 谓词不命中；无论哪种顺序，旧代际 G 的迟到写操作都不能影响已存在的 G+1 行
- **Pass Condition**: 两类交错顺序下行终态与影响行数均符合，且存在 G+1 时任何 G 写操作影响 0 行
- **Evidence**: 驱动层与集成层竞态测试（可借助短 TTL/屏障通道调度）

### AC-7: 短暂故障退避并从持久状态恢复
- **Type**: `rule`
- **Given**: producer 续期或 reaper 通知遭遇瞬时数据库错误（或事务回滚）
- **When**: 故障恢复后下一个周期到来
- **Then**: 操作以既有指数退避/周期重试成功，无需重启；producer 续期遇"未找到"后先读持久状态：行已回收则重新获取代际并继续运行；行代际更新则停止抓取新作业、排空在途作业且不再发心跳
- **Pass Condition**: 注入失败后服务自愈；两种"未找到"分支行为分别有测试断言（含被取代 producer 不再发起成功续期）
- **Evidence**: 恢复路径测试与日志/信号

### AC-8: 领导者切换不漏发不重发
- **Type**: `rule`
- **Given**: 过期租约存在；模拟"回收事务已提交即发生领导者切换"与"回收前领导者已停止"两种时序
- **When**: 新领导者接管并运行首轮回收
- **Then**: 已在已提交事务中转换的行不再产生第二条 offline 通知；尚未转换的过期行被补做并通知一次；每个 `(queue, client_id, 下线时的 generation)` 恰好一条下线通知
- **Evidence**: `producer_reaper_test.go` 中以两个 reaper 实例/两次接管模拟切换
- **Pass Condition**: 通知去重由 `reaped_at IS NULL` 谓词与同事务通知共同保证，测试计数精确

### AC-9: 纯入队客户端不产生租约
- **Type**: `rule`
- **Given**: 未配置 Queues 的客户端（仅 Workers=nil/无 producer）
- **When**: 客户端启动、入队若干作业、停止
- **Then**: `river_producer` 表始终为空，且不存在 reaper 服务运行迹象
- **Pass Condition**: 客户端测试直接查表断言 0 行
- **Evidence**: `client_test.go` 新增测试

### AC-10: 既有队列清理与作业处理语义不变
- **Type**: `rule`
- **Given**: 变更后的代码
- **When**: 运行既有测试套件
- **Then**: `QueueCleaner` 仍按 `updated_at` 24h  horizon 删除 `river_queue`；暂停/恢复/元数据/插入/抓取/完成/救援相关既有用例全部通过；producer 对 `river_queue.updated_at` 的 10 分钟刷新保留
- **Pass Condition**: 既有测试无回归（仅允许因配置字段更名导致的必要编译调整）
- **Evidence**: `make test` 全模块通过

### AC-11: 三驱动迁移与回滚一致
- **Type**: `rule`
- **Given**: 干净数据库
- **When**: 迁移到 008 后再回滚 008
- **Then**: pgx/database/sql（同源）与 sqlite 均成功创建 `river_producer` 与索引，回滚后表/索引消失；`MigrationLineMainTruncateTables(8)` 包含新表
- **Pass Condition**: 迁移测试与三驱动建表/回滚通过
- **Evidence**: rivermigrate/riverdbtest 相关测试

### AC-12: 实现与仓库既有模式的一致性
- **Type**: `rubric`
- **Dimension**: 代码质量与模式一致性
- **Scale**: 1-5
- **Anchors**: 1 = 引入临时 SQL/绕过驱动层、破坏 startstop/testsignal 模式或引入新依赖；3 = 功能正确但存在模式偏差或命名/分层不一致；5 = 严格遵循 sqlc→driver→pilot→service 分层、并行测试规范、testsignal、日志分级、退避复用、无 ad-hoc SQL（测试除外）
- **Pass Threshold**: >= 4
- **Evidence**: 代码审查、`make lint`、独立评审

## Open Questions

- 无阻塞性开放问题。以下为已作出的设计决策（如审阅有异议可在审批阶段提出）：
  - [x] 下线行物理清理放在 `ProducerReaper` 内（24h horizon），不依赖 SQLite 默认关闭的外键级联，不改动 `QueueCleaner`。
  - [x] Postgres 表使用 `UNLOGGED`（与 `river_leader`/历史 `river_client` 一致；租约为可重建的 ephemeral 状态）。
  - [x] 被更新代际取代的旧 producer 的处理为"停止抓取 + 优雅排空"，不取消在途作业。
  - [x] offline 通知主题 `river_producer` 为内部主题，仓库内不新增公共订阅事件类型。
