# 生产者租约生命周期修正 - 实现计划

## Task 1: 迁移 008 — river_producer 租约表
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新增 pgx 迁移 `riverdriver/riverpgxv5/migration/main/008_producer_lease.up.sql` / `.down.sql`：`CREATE UNLOGGED TABLE river_producer`（PK `(queue_name, client_id)`，列 `producer_id bigint`、`generation bigint NOT NULL DEFAULT 1 CHECK (>0)`、`max_workers bigint CHECK (>0)`、`created_at/updated_at timestamptz NOT NULL DEFAULT now()`、`expires_at timestamptz NOT NULL`、`reaped_at timestamptz`，名称长度 CHECK），部分索引 `river_producer_active_expires_at_idx ON (expires_at) WHERE reaped_at IS NULL`；down 为 DROP TABLE（索引随表删除）。
  - sqlite 迁移 `riverdriver/riversqlite/migration/main/008_producer_lease.up.sql` / `.down.sql`：同结构普通表（`integer` 主键列、timestamp 文本默认值），部分索引写法与 sqlite 007 迁移一致（schema 模板前缀）。
  - 用 `rsync -au`（等价 `make generate/migrations`）同步 pgx migration 到 riverdatabasesql。
  - 更新 `riverdriver/river_driver_interface.go` 的 `MigrationLineMainTruncateTables`：`case 0, 8` 返回列表包含 `river_producer`。
- **Acceptance Criteria Addressed**: AC-11
- **Test Requirements**:
  - `rule` TR-1.1: pgx 与 databasesql 的 008 迁移文件逐字节一致（`make verify/migrations` 或 diff 通过）；三驱动 TestSchema 迁移至最新版本成功
  - `rule` TR-1.2: 008 down 后 `river_producer` 与索引不存在（rivermigrate 既有回滚测试覆盖新文件）
  - `rule` TR-1.3: `MigrationLineMainTruncateTables(0)` 与 `(8)` 均包含 `river_producer`，旧版本行为不变（编译+调用方测试）
- **Notes**: 不设外键（sqlite 驱动未启用 foreign_keys）；`river_queue` 行在 producer 获取租约前已创建，关联由应用层保证。

## Task 2: 驱动查询 — sqlc 源、三驱动生成代码与接线
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - pgx 新增 `riverdriver/riverpgxv5/internal/dbsqlc/river_producer.sql`：文件头 `CREATE UNLOGGED TABLE`（与迁移一致）+ 查询：
    - `ProducerInsert :one`（upsert，冲突时 `generation = river_producer.generation + 1`、清空 reaped_at、刷新 producer_id/max_workers/expires_at，RETURNING *；TTL 用 `make_interval(secs => @ttl)`，now 走 narg）
    - `ProducerKeepAlive :one`（WHERE queue+client+generation+`reaped_at IS NULL`，SET updated_at/expires_at，RETURNING *）
    - `ProducerFinish :one`（WHERE queue+client+generation+`reaped_at IS NULL`，SET reaped_at，RETURNING *）
    - `ProducerGet :one`（WHERE queue+client）
    - `ProducerReapExpired :many`（行值 IN 子查询分批：`WHERE (queue_name, client_id) IN (SELECT ... WHERE expires_at < now AND reaped_at IS NULL ORDER BY expires_at LIMIT max)`，SET reaped_at，RETURNING *）
    - `ProducerDeleteReaped :many`（同样分批，`reaped_at < horizon`，DELETE，RETURNING *）
  - sqlite 新增 `riverdriver/riversqlite/internal/dbsqlc/river_producer.sql`：同语义方言版（时间 cast text、TTL 由 Go 侧直接传入 `@expires_at` 文本而非 SQL 运算；`unixepoch()` 比较或直接字符串比较与既有 leader 写法一致；LIMIT 可直接用于 UPDATE 子查询）。
  - 三个 dbsqlc 的 `sqlc.yaml` queries+schema 列表加入 `river_producer.sql`（databasesql 引用 pgx 源相对路径）。
  - 手写 sqlc v1.31.0 风格生成代码 `river_producer.sql.go`：pgx（pgx/v5 风格）、databasesql（database/sql + jsonb→string 约定，本文件无 jsonb 列）、sqlite（时间为 string、`emit_pointers_for_null_types`）。
  - 三驱动 `models.go` 增加 `RiverProducer` 结构体。
  - `riverdriver/river_driver_interface.go`：新增 `Producer` 行类型与 `ProducerInsertParams/ProducerKeepAliveParams/ProducerFinishParams/ProducerGetParams/ProducerReapExpiredParams/ProducerDeleteReapedParams`（重塑既有 `ProducerKeepAliveParams`：ClientID/Generation/TTL/Now/QueueName/Schema），Executor 接口增加 6 个方法。
  - 三驱动 Executor（river_pgx_v5_driver.go、river_database_sql_driver.go、river_sqlite_driver.go）接线新方法与 `producerFromInternal` 映射；sqlite 侧 TTL 在 Go 侧算出 expires_at 文本传入。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-5, AC-12
- **Test Requirements**:
  - `rule` TR-2.1: 四模块 `go build ./...` 与 go vet 通过；sqlc 源中的查询名称/参数与生成代码一一对应（人工比对 sqlc v1.31.0 既有输出风格）
  - `rule` TR-2.2: upsert 首次插入 generation=1、二次冲突 generation=2 且仅一行（Task 3 套件实证）
  - `rule` TR-2.3: 错误/未找到语义统一为 `rivertype.ErrNotFound`（interpretError 覆盖 pgx/sqlite/dbsql）
  - `rubric` TR-2.4: 分层一致性；scale 1-5；anchors 1=绕过 sqlc 层写临时 SQL/风格偏离，3=可用但命名或映射不一致，5=与既有 queue/leader 查询完全同构；threshold >= 4；evidence 代码审查

## Task 3: 跨驱动一致性套件 riverdrivertest
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `riverdriver/riverdrivertest/producer.go` 新增 `exerciseProducer`：覆盖 ProducerInsert（新插入/冲突代际递增/created_at 保留）、ProducerGet（存在/ErrNotFound）、ProducerKeepAlive（同代际刷新 expires_at；旧代际与已回收行 → ErrNotFound 且行不变）、ProducerFinish（同代际成功、重复执行与旧代际 → ErrNotFound/0 行）、ProducerReapExpired（仅过期未回收行被回收、未过期/已回收不动、批量 limit）、ProducerDeleteReaped（仅超过 horizon 的已回收行被删）。
  - 接入 pgx、sqlite、databasesql 三个驱动的 driver_test 入口；时间断言用 `driver.TimePrecision()`。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-5, AC-6, AC-11
- **Test Requirements**:
  - `rule` TR-3.1: 三驱动下 `exerciseProducer` 全绿（pgx 用 docker PG，sqlite 用临时库，databasesql 同 pg）
  - `rule` TR-3.2: 旧代际续期/回收后行字段（generation、expires_at、reaped_at）零改动有显式断言
- **Notes**: 遵循仓库并行 bundle 规范：外层/子测试 `t.Parallel()` 开头，setup 闭包 `t.Helper()`。

## Task 4: StandardPilot 租约实现与下线通知契约
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - `internal/notifier/notifier.go` 新增内部主题 `NotificationTopicProducer = "river_producer"` 并加入 `notificationTopicAll`（长度校验/注册一致性）。
  - 根包新增（或置于 producer.go 同包）通知负载类型：`producerNotificationPayload{Action, Queue, ClientID, ProducerID, Generation}` 与 action 常量 `producerNotificationActionOffline = "offline"`，JSON tag snake_case。
  - `rivershared/riverpilot/pilot.go`：扩展 `ProducerInitParams`（增 `MaxWorkers int`、`TTL time.Duration`、`Now *time.Time`）与 `ProducerShutdownParams`（增 `ClientID string`、`Generation int64`）。
  - `standardProducerState` 扩展为持有 ClientID/Queue/ProducerID/Generation/MaxWorkers（仍实现 `JobFinish` no-op）。
  - `StandardPilot`：
    - `ProducerInit`：生成随机正 int64 producer_id（randutil），调 `exec.ProducerInsert`，返回 producer_id 与带 generation 的 state；去掉进程内 seq。
    - `ProducerKeepAlive`：按 state？——签名只拿 params，故由 producer 把 ClientID/Generation/TTL/Now 装入 `ProducerKeepAliveParams`；pilot 直接委托 `exec.ProducerKeepAlive`（ErrNotFound 透传）。
    - `ProducerShutdown`：`Begin` 事务 → `ProducerFinish`（同代际栅栏）→ 若返回行（发生转换）则 `tx.NotifyMany(topic=producer, [offline payload])` → Commit；ErrNotFound 时安静提交、不通知；RollbackWithoutCancel 兜底。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-4, AC-12
- **Test Requirements**:
  - `rule` TR-4.1: ProducerInsert 返回的 generation 存入 state，后续 KeepAlive/Shutdown 使用该代际（根包 DB 级 pilot 测试，pgx）
  - `rule` TR-4.2: Shutdown 在行存在时：行被置 reaped_at 且同事务恰好一条 offline 负载（pg LISTEN 收到；sqlite outbox 表可见）；行已回收/代际不匹配时：无通知、无错误
  - `rubric` TR-4.3: 通知与状态转换的事务原子性实现质量；scale 1-5；anchors 1=事务外通知或可丢可重，3=同事务但错误路径有遗漏，5=提交/回滚/ErrNotFound 三路径均严密；threshold >= 4；evidence 代码+测试

## Task 5: producer.go — 获取、续期、恢复、栅栏自停、释放
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - `producerConfig`：`StaleProducerRetentionPeriod` 更名为 `LeaseTTL`（默认由 client 传入 5min，常量 `producerLeaseTTLDefault = 5 * time.Minute`）；校验 >0。更新全部测试构造点与 `client.producerAdd`。
  - producer 启动：`ProducerInitParams` 传 MaxWorkers/TTL/Now；p.state 持有代际；租约获取失败仍按现逻辑返回错误、不启动。
  - `reportProducerStatusOnce`：以 state 组装 `ProducerKeepAliveParams`（Generation/TTL/Now）；成功→信号；普通错误→日志并等下周期（保持周期循环，不做激进重试）；`ErrNotFound`→调 `ProducerGet` 读持久状态：
    - 行不存在或 `reaped_at != nil`：重新 `ProducerInit` 式 upsert 获取新代际（复用 pilot.ProducerInit 或 pilot 新增内部 helper），更新 state，warn 日志，继续运行；
    - 行存在且 generation 更新：error 日志，触发 `leaseLost` 通道。
  - `fetchAndRunLoop` 增加 `case <-p.leaseLostCh:`：退出抓取循环，进入既有 `executorShutdownLoop` 排空路径；`finalizeShutdown` 以当前（旧）代际执行栅栏释放（预期 0 行、不发通知）。
  - `finalizeShutdown`：`ProducerShutdownParams` 带 ClientID/Generation；保留 4 次指数退避与 Canceled/ErrClosedPool 放弃逻辑。
  - 更新 `client_pilot_test.go` spy 与 producer 测试中的字段名/断言。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4, AC-6, AC-7
- **Test Requirements**:
  - `rule` TR-5.1: producer 启动后租约行字段正确；运行中 expires_at 被周期续期刷新（短间隔配置驱动，testsignal 等待）
  - `rule` TR-5.2: 优雅停止后行 reaped_at 非空；重复停止/再次 Finalize 不产生第二条通知
  - `rule` TR-5.3: 旧代际续期 ErrNotFound 且行内为更新代际时，producer 停止抓取新作业、在途作业排空完成、此后无成功续期（用屏障/计数断言）
  - `rule` TR-5.4: 行被回收（reaped_at 非空、无更新代际）时 producer 重新获取新代际并可继续抓取作业
- **Notes**: 不新增用户配置；间隔沿用 ProducerReportInterval（30s），TTL 5min。

## Task 6: ProducerReaper 领导者维护服务与客户端接线
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - `internal/maintenance/producer_reaper.go`：`ProducerReaper` 服务（嵌入 `QueueMaintainerServiceBase` + `BaseStartStop`，风格对齐 QueueCleaner）：
    - 配置：`Interval`（默认 `producerReaperIntervalDefault = 5 * time.Second`）、`ReapedRetentionPeriod`（默认 24h）、`Schema`、BatchSizes（默认/缩减）、缩减批量熔断器；`timeutil.NewTickerWithInitialTick` 首轮即跑、StaggerStart。
    - `runOnce`：分批循环，每批**独立事务** `Begin → ProducerReapExpired(now, batch) → 对返回行组一条 NotifyMany(offline 负载,含 generation) → Commit`；取不满一批结束；错误则该批回滚、日志、退避到下一周期（不跨批持有事务）。
    - 随后分批 `ProducerDeleteReaped(now-retention)` 物理清理，批间既有 BatchBackoff 睡眠。
    - TestSignals：`ReapedProducer TestSignal[*riverdriver.Producer]`（每行）、`DeletedReapedBatch TestSignal[struct{}]`。
  - `client.go`：在 willExecuteJobs 的 maintenanceServices 中加入 ProducerReaper（因此仅领导者运行）；`clientTestSignals` 增加 producerReaper 字段与 Init。
- **Acceptance Criteria Addressed**: AC-5, AC-6, AC-8, AC-12
- **Test Requirements**:
  - `rule` TR-6.1: 过期行被回收并为每行恰好一条 offline 负载（断言负载字段含正确 generation）；未过期/已回收行零影响；二次运行零变更零通知
  - `rule` TR-6.2: 模拟领导者切换（服务 A 运行一轮后停止，服务 B 启动再跑）：同一代际下行只转换一次、通知不重复；A 未跑的过期行由 B 补做
  - `rule` TR-6.3: 批事务内通知失败注入（如用失败 ExecTx 包装或 sqlite 锁）→ 该批回滚（reaped_at 仍 NULL），下一周期成功，通知最终恰一条
  - `rule` TR-6.4: reaped_at 早于 24h horizon 的行被物理删除，未到 horizon 的保留；river_queue 行不受影响
  - `rubric` TR-6.5: 服务风格（baseservice/startstop/testsignal/熔断器/分批）与既有维护服务一致性；scale 1-5；anchors 1=自创循环模式，3=基本一致但缺退避或信号，5=与 JobCleaner/QueueCleaner 同构；threshold >= 4；evidence 代码审查

## Task 7: 端到端集成测试 — 纯入队无租约、崩溃回收、晚到心跳
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Description**:
  - `client_test.go`：未配置 Queues 的客户端 Start→Insert→Stop 后 `river_producer` 始终 0 行（pgx）。
  - `producer_test.go`/新文件：完整客户端场景——工作客户端启动后租约存在且与队列配置一致；模拟崩溃（直接把行 expires_at 调到过去并停止心跳实例/或用独立 producer 实例不停止）后，领导者 reaper 周期内回收并发布 offline；随后同 client_id 新实例获取 G+1；旧实例再触发一次 reportProducerStatusOnce 断言 ErrNotFound/0 行且新行不被改动，并触发 leaseLost。
  - sqlite 驱动至少一条等价链路测试（可放 river 包内 sqlite 客户端既有测试基础设施处），验证 outbox 通知与回收。
- **Acceptance Criteria Addressed**: AC-6, AC-7, AC-8, AC-9
- **Test Requirements**:
  - `rule` TR-7.1: 纯入队客户端全程 river_producer 0 行、无 reaper 信号
  - `rule` TR-7.2: 崩溃→回收→重启代际递增→旧心跳无效的端到端链路通过（pgx 与 sqlite 各一条）
  - `rule` TR-7.3: 离线通知在完整链路中每个下线代际恰好一条（监听器/outbox 计数）

## Task 8: 全量验证、lint 与 CHANGELOG
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 3, Task 5, Task 6, Task 7
- **Description**:
  - 准备 docker 测试库（postgres:17，创建 river_test 或 TEST_DATABASE_URL 指到容器），运行根模块与三驱动模块 `make test`（必要时分包 `-run` 复现），`make test/race` 至少覆盖 producer/reaper 相关包。
  - `make lint`（golangci-lint，可用时）；全模块 `go vet ./...` 与 `gofumpt`/`goimports` 自检；`go build ./...` 四模块。
  - `make verify/migrations`；若 sqlc 可安装则补 `sqlc diff`，否则以严格人工同构比对为准并记录。
  - CHANGELOG.md `[Unreleased] ### Added` 增加一条生产者租约/回收说明。
- **Acceptance Criteria Addressed**: AC-10, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-8.1: `make test` 全模块通过（含既有 producer/queue/maintenance/migration 测试无回归）
  - `rule` TR-8.2: lint/vet/build 干净；迁移同步校验通过
  - `rubric` TR-8.3: 变更整体可维护性与测试覆盖质量；scale 1-5；anchors 1=有回归或测试缺口，3=功能通过但关键竞态靠人工推断，5=竞态/故障/切换均有自动化证据；threshold >= 4；evidence 命令输出与测试清单
