# River 队列排空（Queue Drain / 交接）- 产品需求文档

## Overview
- **Summary**: 为 River 增加可等待的「队列排空」操作：发起后所有客户端立即停止为指定队列领取新作业，已领取（running）的作业继续正常执行并发布正常完成事件；调用方可观测运行中数量与本次交接的稳定标识；数量归零后排空结束，队列继续保持停领，直到显式恢复。排空意图与进度持久化在数据库中，支持实例退出/领导者切换后由其他实例接续，且同一请求重试不会创建第二次交接。
- **Purpose**: 值班工程师滚动下线一组 worker 时，需要把指定队列干净地交给仍存活的实例，而不是依赖暂停队列（不关心在执行作业何时归零）或直接取消进程（打断在执行作业）来碰运气。
- **Target Users**: 运维/值班工程师、执行滚动发布或缩容编排的自动化系统、需要优雅下线 worker 的 River 使用方。

## Goals
- 提供可等待的排空操作，返回稳定交接标识（Key）与当前运行中作业数量。
- 发起排空后，**所有客户端**立即停止为目标队列领取新作业（通知即时生效，轮询保证收敛）。
- 已被领取的作业不受影响：继续执行、正常完成（completed/failed/snoozed 等正常终态）并发布既有的作业完成事件；排空不取消任何作业的执行上下文。
- 仅当该队列 `running` 作业数量归零，排空才转为完成；完成后队列仍然停领，只有显式恢复操作才能重新放开领取。
- 排空意图与进度存于数据库：发起实例退出或领导者切换后，另一实例凭同一 Key 可接续等待/推进；同 Key 重试绝不产生第二次交接。
- 通知丢失或驱动不支持 listener 时，仅靠轮询也能收敛。
- 等待超时/取消只结束调用方的等待，排空状态与停领效果保持不变，绝不暗中恢复队列。
- 未使用排空操作的启动、暂停、恢复、停止、取消行为保持完全不变。

## Non-Goals
- 不支持 `*` 批量排空所有队列；排空一次只针对一个具名队列。
- 不提供排空的中止/取消操作（在数量未归零前强行恢复领取）。
- 不提供 `QueueDrainTx` 等事务变体（排空是可能长时间等待的操作，不应绑定事务）。
- 不改动作业取消（JobCancel）、停止（Stop/StopAndCancel）、暂停/恢复（QueuePause/QueueResume）的现有语义。
- 不依赖 River Pro 的 producer 跟踪能力；基于 StandardPilot 与 `river_job.state` 即可工作。
- 不新增命令行（river CLI）子命令，不新增文档站点内容。

## Background & Context
- 现有暂停机制：`river_queue.paused_at` 记录暂停态；producer 启动时读队列行得到本地 `paused`，运行期通过 `river_control` 通知主题（pause/resume 动作）或 poll-only 模式下的 `pollForSettingChanges` 轮询收敛；`paused=true` 时 producer `innerFetchLoop` 将 fetch 数量置 0。暂停不会等待在执行作业归零，也没有交接标识。
- 领取 SQL `JobGetAvailable` 本身不检查 `paused_at`，停领闸门目前在 producer 本地。
- 运行中作业以 `river_job.state = 'running'` 体现；死亡 producer 遗留的 running 作业由 JobRescuer 在卡住阈值后转为 `available`。
- 领导者（`internal/leadership`）只用于维护类服务；producer/队列控制不依赖领导者。
- 客户端 API 先例：`QueuePause/QueueResume`（含 Tx 变体）在一个事务内更新行并通过 `NotifyMany` 发 `river_control` 通知，提交后对无 listener 驱动再做本进程 producer 注入（`notifyProducerWithoutListenerQueueControlEvent`）。
- 驱动结构：pgx 与 sqlite 各有 dbsqlc 查询与迁移；`riverdatabasesql` 复用 pgx 的 SQL 文件并由 rsync 同步迁移；所有驱动共享 `riverdriver.Executor` 接口与 `riverdrivertest` 一致性套件。当前迁移最新版本为 007。

## Functional Requirements

### FR-1：发起排空（持久化意图 + 全客户端停领）
- `Client.QueueDrain(ctx, name, opts)` 在数据库中为队列 `name` 创建一条排空记录（状态 `draining`），并在同一事务中发出 `river_control` 排空控制通知；事务提交后，无 listener 驱动下对本进程 producer 注入同一控制事件。
- 一个队列同一时刻至多存在一个活跃（`draining` 或 `drained`）交接，由数据库约束保证。
- 所有配置了该队列的 producer 在收到通知或轮询观测到活跃排空记录后停止为该队列领取新作业；此闸门与 `paused` 状态正交（任一为真即停领）。
- 不支持 `*` 队列名与空队列名，返回明确校验错误。

### FR-2：稳定交接标识（幂等键）
- `QueueDrainOpts.Key` 为可选幂等键：为空时由服务端生成随机键（如十六进制随机串），并在返回值中提供；非空时使用调用方提供的值。
- 相同 Key 的串行或并发重试（含在另一实例上的重试）必须返回同一个交接（同 Key、同 started_at），不得创建第二条记录或第二次交接。
- 队列已有活跃交接且请求 Key 不同（或为空）时，返回类型化冲突错误 `QueueDrainAlreadyActiveError`，其中携带当前活跃交接的 Key，供调用方显式接续。
- Key 同时作为排空控制通知负载与事件负载的一部分，便于关联。

### FR-3：在执行作业不受影响并发布正常事件
- 排空只关闭新作业领取；producer 的工作上下文（workCtx）不被取消，已领取作业继续执行到其自然终态（completed/失败重试/snoozed/cancelled 等），completer 与作业事件发布路径不变。
- 排空不得导致作业被 interrupt 或以 shutdown 名义改状态。

### FR-4：进度观测（运行中数量）
- 排空返回值 `QueueDrainResult` 包含：`Queue`、`Key`、`State`（`draining`/`drained`）、`RunningJobs`（该队列当前 `state='running'` 的作业数）、`StartedAt`、`DrainedAt`。
- 另提供 `Client.QueueDrainStatus(ctx, name)` 非阻塞查询当前活跃交接及其运行中数量；无活跃交接时返回 `rivertype.ErrNotFound`。
- 运行中数量的权威来源是数据库（`river_job.state='running'`），而非单实例内存计数，因此跨实例可见、实例死亡后仍准确。

### FR-5：归零完成（原子推进）
- 排空只能由一条条件更新推进到 `drained`：仅当目标队列不存在 `state='running'` 的作业且当前状态为 `draining` 时，更新才生效；任意客户端或 producer 均可尝试，数据库条件保证并发下只完成一次。
- 完成更新与 `drain_completed` 控制通知在同一事务内发出；提交后对无 listener 驱动注入本进程事件。
- 死亡实例遗留在 `running` 的作业被 rescuer 转为 `available` 后不再计入运行数量，因此不会无限阻塞排空；这些作业在排空期间不会被领取，在显式恢复后由存活实例领取。

### FR-6：完成后仍停领，显式恢复才放开
- `drained` 状态下 producer 继续停止领取。
- 仅 `Client.QueueDrainResume(ctx, name)` 可结束交接：条件更新 `drained → resumed`，发出 `drain_resumed` 控制通知；producer 观测后解除排空闸门，若队列未暂停则立即触发一次领取。
- `QueueDrainResume` 不修改 `paused_at`；`QueueResume` 也不触碰排空记录——暂停与排空两条生命周期互不干扰。
- 排空尚处于 `draining`（数量未归零）时调用 `QueueDrainResume` 返回类型化错误 `QueueDrainInProgressError`；不存在活跃交接时调用为幂等 no-op 成功。

### FR-7：等待语义与超时不恢复
- `QueueDrain` 创建/接续交接后进入等待：通过控制通知即时唤醒，并以固定间隔轮询数据库（不依赖通知是否可达）；每次轮询读取活跃交接快照并尝试 FR-5 的条件完成。
- 等待到 `drained` 时返回 `RunningJobs=0` 的结果与 nil error。
- 当传入 ctx 超时或取消：立即结束等待，返回最近一次快照（State 仍为 `draining`、RunningJobs 为最近值）与该 ctx 错误；**不得**更改排空记录、**不得**恢复领取；调用方可稍后用同一 Key 再次调用 `QueueDrain` 接续等待。

### FR-8：持久化接续（实例退出/领导者切换）
- 交接记录（意图、Key、状态、时间戳）与完成进度均在数据库；任何实例都不持有完成该交接所需的独占状态（不依赖领导者身份）。
- producer 启动时读取活跃排空记录：若存在，即使本实例从未收到过通知，也以停领状态启动。
- 接续场景：实例 A 发起后排空未完成（A 退出、超时放弃或发生领导者切换），实例 B 以同一 Key 调用 `QueueDrain`，应返回同一交接并继续等待/推进直至 `drained`；没有 Key 的一方也可通过 `QueueDrainStatus` 或冲突错误中的 Key 发现并接续。

### FR-9：轮询收敛
- 排空等待方无论是否配置 notifier 都进行数据库轮询，通知只用于降低延迟。
- producer 在 poll-only 模式下既有的设置轮询必须同时观测排空状态迁移并注入相应控制事件；在具备 notifier 的模式下也必须有兜底轮询，使通知丢失时排空状态（停领/完成/恢复）仍能在有限时间内收敛。
- producer 在排空期间、本实例在执行作业归零（且队列为 draining）时，机会性地尝试 FR-5 的条件完成，使等待方全部消失时排空仍可由作业完成事件驱动收敛。

### FR-10：生命周期事件
- 新增三个订阅事件种类：`queue_drain_started`、`queue_drained`、`queue_drain_resumed`，事件负载包含队列名与 Key；作业完成事件（`job_completed` 等）保持不变。

### FR-11：既有行为不变
- 未涉及排空的 Start、QueuePause、QueueResume、Stop、StopAndCancel、JobCancel 行为、时序与事件保持不变。
- 对于不存在排空记录的队列，producer 的领取、轮询、通知处理路径与现状保持一致（仅允许新增不可感知的兜底轮询，不得改变暂停/恢复的可观察行为）。
- 进入 `resumed` 终态的交接记录保留（保证同 Key 永久幂等），但其清理不影响队列；由队列清理维护顺带清理陈旧的 `resumed` 记录，避免无限累积。

## Non-Functional Requirements
- **NFR-1 一致性**：变更必须在 pgx、sqlite、databasesql 三个驱动上等价落地（迁移、sqlc 查询、Executor 适配、riverdrivertest 一致性套件）。
- **NFR-2 原子性**：交接创建、完成、恢复均为单条条件 SQL / 单事务，并发安全；完成通知与状态更新同事务。
- **NFR-3 开销**：对未排空队列无可观察行为变化；新增轮询仅在有活跃交接时产生有意义的工作，兜底轮询间隔应保守（不显著增加常规部署的数据库负载）。
- **NFR-4 可观测性**：关键迁移（started/drained/resumed/冲突/接续/超时返回）有 slog 日志与测试信号（test signal）。
- **NFR-5 仓库规范**：遵守 AGENTS.md——sqlc 查询走 driver 接口并重新生成（本环境 sqlc 不可用时手写与 v1.31.0 生成结果一致的代码）、`make test`/`make lint` 通过、require 断言、并行测试 bundle 模式、gci 导入分区、字母序组织、导出符号有注释。
- **NFR-6 迁移安全**：008 迁移 up/down 在 pgx 与 sqlite 均可重复往返；down 丢弃新表；不影响既有数据。

## Constraints
- **Technical**: Go 1.27 工作区多模块；sqlc v1.31.0；PostgreSQL 与 SQLite 双方言（sqlite 时间用 `datetime('now','subsec')` 文本列，jsonb 用自定义 jsonb() 等）；databasesql 复用 pgx SQL；迁移经 rsync 同步。
- **Business**: 不得破坏 semver 下既有公开 API 的行为；driver 接口为内部 seam，可按需调整。
- **Dependencies**: 复用 notifier、subscriptionManager、jobcompleter、JobRescuer、QueueCleaner、testsignal、randutil；不引入新外部依赖。

## Assumptions
- 「所有客户端」指共享同一数据库/schema 的全部 River 客户端。
- 运行中数量以 `river_job.state='running'` 计数；卡住作业的既有 rescue 机制负责把死亡实例的作业移出 running，排空不另设超时强制。
- 等待轮询间隔采用秒级常量（等待方约 1s 级）；producer 兜底轮询复用队列设置轮询节奏（poll-only 约 2s，notifier 模式采用更保守的固定间隔）。
- 排空只面向具名队列；编排方需要多队列排空时逐队列调用。

## Acceptance Criteria

### AC-1: 发起排空后全客户端立即停止领取
- **Type**: `rule`
- **Given**: 同一数据库上有两个以上已启动、均配置队列 Q 且有空闲 worker 的客户端，队列中持续有待领取作业
- **When**: 任一客户端调用 `QueueDrain(ctx, Q, opts)` 成功返回（提交完成）
- **Then**: 在通知延迟或一个兜底轮询周期内，所有客户端均不再为 Q 领取新作业（新插入 Q 的作业保持 `available`）；而其他队列的领取不受影响
- **Pass Condition**: 多客户端集成测试中断言排空提交后两个客户端在限定时间内均无新作业被领取，对照队列仍正常消费
- **Evidence**: 根包集成测试 `queue_drain_test.go` 中多客户端测试用例

### AC-2: 在执行作业正常跑完并发完成事件
- **Type**: `rule`
- **Given**: 队列 Q 中有作业正在 worker 内执行（阻塞在测试通道上）
- **When**: 对 Q 发起排空并放行该作业
- **Then**: 作业以正常 `completed` 结束、发布 `job_completed` 事件、`finalized_at` 正常；作业执行 ctx 未被取消，数据库中无 interrupted/shutdown 痕迹
- **Pass Condition**: 测试断言作业状态为 completed、收到 job_completed 事件、作业 Work 内 ctx.Err()==nil
- **Evidence**: 根包集成测试

### AC-3: 返回稳定标识与运行中数量
- **Type**: `rule`
- **Given**: Q 上有 N 个 running 作业
- **When**: 发起排空（Key 留空），并随后调用 `QueueDrainStatus`
- **Then**: 返回结果含非空稳定 Key、State=draining、RunningJobs=N；Status 返回同一 Key 且 RunningJobs 随作业完成递减
- **Pass Condition**: 测试断言 Key 非空且两次一致、RunningJobs 与数据库 running 计数一致并最终为 0、State 变为 drained
- **Evidence**: 根包集成测试 + driver 一致性测试

### AC-4: 运行中数量归零才完成排空
- **Type**: `rule`
- **Given**: Q 上存在 running 作业时发起排空
- **When**: running 作业尚未全部终态化
- **Then**: 即使排空等待方轮询/条件完成被反复触发，记录保持 draining；最后一个 running 作业终态化后的下一次条件更新使其变为 drained 且仅一次（drained_at 被设置一次）
- **Pass Condition**: 测试断言有 running 时 QueueDrain 超时返回仍为 draining；归零后再次接续立即得到 drained；DB 行状态迁移恰好一次
- **Evidence**: 根包集成测试 + driver 层 QueueDrainComplete 条件更新测试

### AC-5: 归零后保持停领直到显式恢复
- **Type**: `rule`
- **Given**: 排空已到 drained，Q 中有 available 作业
- **When**: 尚未调用 QueueDrainResume；调用 QueueResume（清暂停）也不改变排空闸门；随后调用 QueueDrainResume
- **Then**: Resume 前作业不被领取；QueueDrainResume 后 producer 在未暂停前提下恢复领取并消费积压；QueueDrainResume 不改变 paused_at，QueueResume 不清除排空
- **Pass Condition**: 测试断言 drained 期间插入作业不被消费；QueueResume 后仍不消费；QueueDrainResume 后在限定时间内被消费，且 paused_at 始终为 nil
- **Evidence**: 根包集成测试

### AC-6: 排空意图与进度持久化、跨实例/领导者切换接续
- **Type**: `rule`
- **Given**: 客户端 A 对 Q 发起带固定 Key 的排空，A 在归零前退出（Stop）或仅等待超时返回
- **When**: 客户端 B 以同一 Key 调用 QueueDrain
- **Then**: B 接到同一记录（Key/started_at 不变），等待到 drained；全程数据库中只有一条该队列的活跃交接记录；领导者身份不影响结果
- **Pass Condition**: 测试断言 A 退出后 B 同 Key 接续并完成，记录计数为 1；另验证新启动客户端面对既有活跃 drain 行时以停领启动
- **Evidence**: 根包多客户端集成测试、producer 启动路径测试

### AC-7: 同 Key 重试幂等、异键冲突显式报错
- **Type**: `rule`
- **Given**: Q 已有活跃排空（Key=K1）
- **When**: 以 K1 并发/串行重复调用 QueueDrain；再以 K2 或以空 Key 调用
- **Then**: K1 调用全部返回同一交接（不新增记录）；K2/空 Key 返回 `QueueDrainAlreadyActiveError` 且错误中携带 K1；同 Key 即使在记录已 resumed 后也返回该历史交接而不新建
- **Pass Condition**: driver/集成测试断言记录唯一、返回体一致、冲突错误类型与字段正确
- **Evidence**: driver 一致性测试 + 根包集成测试

### AC-8: 通知丢失/无 listener 时轮询收敛
- **Type**: `rule`
- **Given**: 排空开始/完成/恢复通知不可达（或驱动不支持 listener）
- **When**: 排空生命周期推进
- **Then**: 等待方仅靠轮询观察到 drained；producer 仅靠设置轮询完成停领/完成/恢复状态收敛；收敛时间受相应轮询间隔上界约束
- **Pass Condition**: ①sqlite/databasesql 驱动一致性与行为测试通过；②producer 测试在不注入控制事件、仅推进轮询时钟时断言状态迁移发生
- **Evidence**: driver 套件 + producer 轮询测试

### AC-9: 等待超时只结束等待、不恢复队列
- **Type**: `rule`
- **Given**: Q 上排空进行中且仍有 running 作业
- **When**: 调用方使用短超时 ctx 调用 QueueDrain 并收到 DeadlineExceeded
- **Then**: 返回结果快照 State=draining 且 RunningJobs>=1，错误为 ctx 超时；数据库记录不变、producer 继续停领；随后用同 Key 再次调用可继续等待
- **Pass Condition**: 测试断言错误为 context.DeadlineExceeded、超时后新插入作业仍不被领取、同 Key 接续成功
- **Evidence**: 根包集成测试

### AC-10: 排空生命周期事件发布
- **Type**: `rule`
- **Given**: 订阅者订阅 queue_drain_started/queue_drained/queue_drain_resumed
- **When**: 排空发起、归零完成、显式恢复依次发生
- **Then**: 订阅者依次收到三个事件，事件中 Queue.Name 与 Key 正确；job_completed 等既有事件照常
- **Pass Condition**: 订阅通道收到三个事件且字段匹配
- **Evidence**: 根包集成测试

### AC-11: 死亡实例作业经 rescue 后不阻塞排空、恢复后被存活者领取
- **Type**: `rule`
- **Given**: 排空期间某 running 作业的 producer 进程死亡（作业滞留 running）
- **When**: rescuer 将其置为 available，随后排空归零并被显式恢复
- **Then**: 排空在 rescue 后可完成；恢复后该作业由存活客户端领取执行，不丢失
- **Pass Condition**: 集成测试（或结合现有 rescuer 测试设施）断言上述链路
- **Evidence**: 根包集成测试

### AC-12: 既有启动/暂停/恢复/停止/取消行为不变
- **Type**: `rule`
- **Given**: 不发起任何排空
- **When**: 执行既有的启动、QueuePause/QueueResume（含 `*`）、Stop、StopAndCancel、JobCancel 流程
- **Then**: 行为、时序、事件、数据库写入与变更前一致；现有相关测试全部不修改通过
- **Pass Condition**: 既有测试套件（含 example、producer、client、maintenance）全绿；`git diff` 中这些路径除新增分支外无旧逻辑改写
- **Evidence**: `make test` 结果 + 代码审查

### AC-13: 三驱动迁移与生成代码一致
- **Type**: `rule`
- **Given**: pgx、sqlite、databasesql 三驱动
- **When**: 应用 008 迁移并执行往返 down/up，调用排空相关 Executor 方法
- **Then**: 三驱动迁移均可上下往返；新表/约束/索引存在；riverdrivertest 新增排空一致性用例在三驱动通过；truncate 表清单与迁移枚举同步更新
- **Pass Condition**: `riverdrivertest` 套件三驱动全绿，migration 往返测试通过
- **Evidence**: 三驱动 driver 测试输出

### AC-14: 实现质量与仓库风格一致性
- **Type**: `rubric`
- **Dimension**: 代码是否遵循 River 既有分层（driver/sqlc → pilot/executor → producer/client）、命名与注释规范、测试 bundle 模式、字母序组织、最小依赖、日志与测试信号完备
- **Scale**: 1-5
- **Anchors**: 1 = 绕过 driver 接口写临时 SQL 到运行时代码、测试缺失或风格割裂；3 = 功能正确但分层或测试组织有明显瑕疵；5 = 如同原生 River 维护者写出的代码，读者无法区分新旧
- **Pass Threshold**: >= 4
- **Evidence**: 独立评审审查代码与测试

## Open Questions
- 无（API 形态、幂等键策略、事件范围已经用户确认：三个独立方法、Key 可选自动生成、新增三个生命周期事件）。
