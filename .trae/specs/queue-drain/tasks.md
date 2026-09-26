# River 队列排空（Queue Drain）- 实施计划

说明：任务按依赖顺序排列；每个任务完成后必须自验全部 TR 并记录 Completion Evidence。所有新 Go 代码遵守 AGENTS.md（gci 导入分区、require 断言、并行测试 bundle、字母序、导出注释）。

## Issue I-3: 轮询跨 key 跳转必须收敛（Review R2 F-2-01）
- **Status**: `completed`
- **Completion Evidence**:
  - drainTransitionPayload 改为 drainTransitionPayloads 返回有序事件序列：key 切换时依次注入 drain_resume{oldKey}（经 queueControlCh 顺序处理先关闭旧交接）与新交接当前状态事件（drain/drain_completed）；poll 循环逐个投递，任一失败（缓冲满）不推进基线，下轮按序重放（处理器对重复态幂等）。
  - 新增确定性回归 PollConvergesAcrossKeyTransition（500ms 轮询节拍）：K1 draining 被观测后，一个间隔内执行器完成 K1 complete+resume 与 K2 insert+complete；断言 DrainResumed(K1)→DrainCompleted(K2) 有序、producer drainState/drainKey=K2/drained；再恢复 K2 后解闸并消费作业。旧实现下该用例必然超时失败（异键 completed 被守卫拒绝）。
  - F-2-03：TestQueueDrain 内块按字母序重排（脚本机械移动，vet/测试验证）。
  - TR-I-3.1/3.2: `TestQueueDrain -count=3 -race` 稳定；根模块与四驱动模块 -count=1 全绿；9 模块 lint 0 issues。TR-I-3.3 rubric 自评 4。
- **Priority**: medium
- **Depends On**: None
- **Discovered By**: Review R2
- **Description**:
  - producer 本地为 K1/draining 时，若一个轮询周期内 DB 经历 K1 drained→resumed、K2 insert→drained（通知全丢、30s 兜底轮询合并中间态），poll 注入的 drain_completed{K2} 被异键守卫拒绝；下一周期 K2 resumed 的 drain_resume{K2} 也被拒绝，producer 永久停领（已实证）。
  - drainTransitionPayload 改为返回有序事件序列：key 切换时依次注入 drain_resume{oldKey}（关闭旧交接）与新交接当前状态事件（drain 或 drain_completed），经 queueControlCh 顺序处理；任一事件发送失败则不推进基线，下轮重放（处理器对重复态幂等）。
  - 新增确定性回归测试：以信号节拍推进——K1 draining 被首拍观测后，直接执行器快速完成/恢复 K1 并插入/完成 K2，断言 producer 采纳 K2/drained；恢复 K2 后断言解闸并领取作业。
  - 顺带处理 advisory F-2-03：TestQueueDrain 内层子测试块按字母序排列。
- **Acceptance Criteria Addressed**: AC-8
- **Test Requirements**:
  - `rule` TR-I-3.1: 跨 key 跳转回归测试在修复前必然失败（异键事件被拒、作业不被领取），修复后通过。证据：根包 -race 输出。
  - `rule` TR-I-3.2: 既有用例（含 PollConvergesWithoutNotifications、TakeoverAcrossClients、BasicFlow）不回归，根包与三驱动全量绿。
  - `rubric` TR-I-3.3: 轮询快照语义（权威状态重放、顺序、幂等、基线推进条件）注释清晰；scale 1-5；threshold >= 4；证据：R3 审查。

## Issue I-1: pgx/sqlite sqlc.yaml 注册 river_queue_drain.sql（Review R1 F-01）
- **Status**: `completed`
- **Completion Evidence**:
  - pgx 与 sqlite 的 sqlc.yaml 的 queries 与 schema 两列表均在 river_queue.sql 与 schema.sql 之间加入 river_queue_drain.sql（字母序），与 databasesql 一致。
  - 九模块 `go build`/lint 通过；环境无 sqlc 二进制，无法跑 sqlc diff，静态核对三模块输入列表一致（含 CREATE TABLE 的查询文件同时作为 schema 输入，沿用 river_queue.sql 双注册惯例）。
- **Priority**: medium
- **Depends On**: None
- **Discovered By**: Review R1
- **Description**:
  - pgx 与 sqlite 的 dbsqlc/sqlc.yaml 的 queries 与 schema 两个列表均漏加 river_queue_drain.sql（databasesql 已加）。有 sqlc 的环境 `make generate/sqlc` 会重新生成出不含 RiverQueueDrain 的 models.go 且不生成新查询，九模块构建随即损坏；`sqlc diff` 报 drift。
  - 在两份 yaml 的 queries 与 schema 列表中按字母序于 river_queue.sql 与 schema.sql 之间加入 river_queue_drain.sql。
- **Acceptance Criteria Addressed**: AC-13, AC-14
- **Test Requirements**:
  - `rule` TR-I-1.1: 两份 yaml 均在 queries 与 schema 列表包含 river_queue_drain.sql，位置字母序正确，与 databasesql 惯例一致。证据：文件内容审查 + 九模块 go build。

## Issue I-2: QueueDrainResume 并发翻转时不得静默成功（Review R1 F-02）
- **Status**: `completed`
- **Completion Evidence**:
  - queue_drain.go QueueDrainResume：rows==0 后读 GetActive，draining→InProgressError；drained→同事务重试一次条件 UPDATE（READ COMMITTED 语句级新快照），重试成功照常通知/注入，重试仍 0 行按他人并发 resume 的幂等成功提交；无活跃行 no-op 不变。
  - 新增确定性回归用例 ResumeRetriesAfterMidCallCompletion：resumeFlipDriver→resumeFlipExecutor→resumeCompleteFlipTx 包装链，首次 QueueDrainResume 先 QueueDrainComplete 再返回 0；断言返回 nil、行=resumed、DrainResumed 信号、producer 解闸并消费作业。
  - 顺带修复 F-04：drainTransitionPayload 首拍即见 drained（含不同 key 接管）注入 drain_completed，事件不再缺失；F-05：两份 river_queue_drain.sql 查询按字母序排列（生成 .sql.go 本就字母序，无功能变化）。
  - TR-I-2.1/2.2: `go test . -run TestQueueDrain -race` 通过（含既有用例不回归）；根模块与四驱动模块全量 `-count=1` 全绿；9 模块 lint 0 issues。rubric TR-I-2.3 自评 4（READ COMMITTED 推理写入注释）。
- **Priority**: low
- **Depends On**: None
- **Discovered By**: Review R1
- **Description**:
  - QueueDrainResume 条件 UPDATE 返回 0 后 GetActive，若其间交接被并发从事务推进 drained→当前代码把 drained 落入 no-op 成功：调用方收到 nil 但行仍 drained、队列仍停领、无 resume 通知（FR-6）。
  - rows==0 且 GetActive 为 drained 时，在同一事务内重试一次条件 UPDATE（READ COMMITTED 语句级新快照），重试成功照常通知/注入；重试仍 0 行（被他人并发 resume）按幂等成功提交；draining 仍返回 QueueDrainInProgressError；无活跃行 no-op 不变。
  - 新增确定性回归测试：包装 ExecutorTx，首次 QueueDrainResume 先执行 QueueDrainComplete 再返回 0，第二次正常 resume，断言客户端返回 nil、行=resumed、producer 恢复领取并消费积压。
  - 顺带修复 advisory F-04（轮询 nil→已 drained 时注入 drain_completed 而非 drain，避免事件缺失）与 F-05（两份 river_queue_drain.sql 查询按字母序排列）。
- **Acceptance Criteria Addressed**: AC-5
- **Test Requirements**:
  - `rule` TR-I-2.1: 回归测试模拟 UPDATE/GetActive 间翻转，QueueDrainResume 返回 nil 且 DB 行为 resumed、停领解除。证据：根包测试输出。
  - `rule` TR-I-2.2: draining 时仍返回 QueueDrainInProgressError、无活跃行幂等成功的既有用例不回归。证据：IdempotentAttachAndConflict 等用例。
  - `rubric` TR-I-2.3: 并发推理与 READ COMMITTED 快照语义在注释中说明清楚；scale 1-5；threshold >= 4；证据：R2 审查。

## Task 1: 008 迁移与表结构（三驱动）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**:
  - pgx 008 up/down、sqlite 008 up/down 已创建；databasesql 经 rsync 同步，`diff -qr` 为空（SYNC_OK）。
  - `MigrationLineMainTruncateTables` 新增 case 8、case 0 含 river_queue_drain，case 7 保持不变；riverdrivertest/migration.go 枚举断言同步更新。
  - TR-1.1: `TestDriverRiverSQLiteModernC/MigrateUpAndDown` 两轮 up/down（含 version 8）PASS。
  - TR-1.2: 部分唯一索引语义留待 Task 3 riverdrivertest 用例验证（DDL 已在 sqlite 迁移往返中被应用）。
  - TR-1.3: `GetMigrationTruncateTables` sqlite 用例 PASS；`TestMigrationLineMainTruncateTables` PASS；迁移目录 diff 为空。
- **Description**:
  - pgx 新增 `riverdriver/riverpgxv5/migration/main/008_queue_drain.up.sql` 与 `.down.sql`：
    - 新表 `river_queue_drain`：`queue text NOT NULL`、`key text NOT NULL`、`state text NOT NULL CHECK (state IN ('draining','drained','resumed'))`、`created_at timestamptz NOT NULL DEFAULT now()`、`drained_at timestamptz`、`resumed_at timestamptz`、`updated_at timestamptz NOT NULL DEFAULT now()`；主键 `(queue, key)`；queue/key 长度 CHECK（与 river_job 队列长度约定一致：queue 1..127、key 非空且 ≤128）。
    - 部分唯一索引：每队列至多一个活跃交接 `CREATE UNIQUE INDEX river_queue_drain_active_queue_idx ON ... (queue) WHERE state IN ('draining','drained')`。
    - down：`DROP TABLE ...`。
  - sqlite 新增同名 008 up/down：时间列用 `timestamp` 文本类型、默认 `CURRENT_TIMESTAMP`（参考 004/007 sqlite 风格）；CHECK 与部分唯一索引 sqlite 均支持。
  - 执行/手动完成 rsync：把 pgx 的 008 与既有迁移同步到 `riverdriver/riverdatabasesql/migration/main/`（`make generate/migrations`，或逐文件复制；最终须通过 `verify/migrations` 的 diff）。
  - 更新 `riverdriver/river_driver_interface.go` 的 `MigrationLineMainTruncateTables`：新增 `case 8` 返回含 `river_queue_drain` 的清单；`case 0` 同步指向最新；`case 7` 保持不变。
  - 更新 `riverdriver/riverdrivertest/migration.go` 的版本枚举断言（version 8 与 version 0 的期望表清单加入 river_queue_drain）。
- **Acceptance Criteria Addressed**: AC-13, AC-14；支撑 FR-1/FR-2/FR-6 的持久化
- **Test Requirements**:
  - `rule` TR-1.1: 三驱动 008 up 后表、CHECK、部分唯一索引存在；down 后表消失；既有 `MigrateUpAndDown` 往返用例（含重复两次）通过。证据：三驱动 driver 测试输出。
  - `rule` TR-1.2: 部分唯一索引语义正确：同队列两条活跃记录被数据库拒绝，一条活跃 + 一条 resumed 允许。证据：riverdrivertest 新增/后续任务中的 SQL 级断言（Task 3 落地，本任务先以手动 SQL 在迁移用例中验证）。
  - `rule` TR-1.3: `verify/migrations` 的 pgx↔databasesql 迁移目录 diff 为空；truncate 表清单版本枚举测试通过。证据：命令输出。
- **Notes**: 索引/约束命名沿用 river 现有蛇形命名；迁移文件头注释风格参考 007。

## Task 2: sqlc 查询与生成代码（手写等价 v1.31.0 产物）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Completion Evidence**:
  - pgx/sqlite 新增 river_queue_drain.sql（6 个查询，字母序）；databasesql 经 sqlc.yaml 复用 pgx 源；三份 sqlc.yaml 已注册。
  - 手写三份 river_queue_drain.sql.go（pgx/dbsql 用 $n、sqlite 用 ?N；execrows 按各驱动 DBTX 惯例）与 RiverQueueDrain 模型。
  - TR-2.1: 三个 dbsqlc 包 `go build ./...` 全部通过（PGX_OK/DBSQL_OK/SQLITE_OK）。
  - TR-2.2: 6 个新/改文件 `gofmt -l` 无输出；命名、Scan 顺序、Params 字段字母序均对齐同目录 v1.31.0 产物。
- **Description**:
  - pgx 新增 `riverdriver/riverpgxv5/internal/dbsqlc/river_queue_drain.sql`：文件头含 `CREATE TABLE river_queue_drain (...)` 完整 DDL（供 sqlc 解析，与 008 迁移一致），随后定义查询：
    - `QueueDrainInsert :one`：INSERT ... `ON CONFLICT (queue, key) DO NOTHING RETURNING <全部列>`；now 用 `coalesce(sqlc.narg('now')::timestamptz, now())`。
    - `QueueDrainGetActive :one`：按 `queue=@queue AND state IN ('draining','drained')` 取行，并以标量子查询 `(SELECT count(*) FROM river_job WHERE queue = river_queue_drain.queue AND state='running')` 返回 `running_count`。
    - `QueueDrainGetByKey :one`：按 `(queue,key)` 取任意状态行 + 同一 running_count 子查询。
    - `QueueDrainComplete :execrows`：`UPDATE ... SET state='drained', drained_at=..., updated_at=... WHERE queue=@queue AND state='draining' AND NOT EXISTS (SELECT 1 FROM river_job j WHERE j.queue = river_queue_drain.queue AND j.state='running')`。
    - `QueueDrainResume :execrows`：`UPDATE ... SET state='resumed', resumed_at=..., updated_at=... WHERE queue=@queue AND state='drained'`。
    - `QueueDrainDeleteResumed :execrows`：`DELETE ... WHERE state='resumed' AND updated_at < @updated_at_horizon`。
    - 所有表名带 `/* TEMPLATE: schema */` 前缀。
  - sqlite 新增 `riverdriver/riversqlite/internal/dbsqlc/river_queue_drain.sql`：等价查询，方言转换（`cast(sqlc.narg('now') AS text)`、`datetime('now','subsec')`、`@x` 参数、子查询写法保持 sqlite 兼容；`IN ('draining','drained')` 字面量集合可直接用）。
  - 更新三份 `sqlc.yaml`（pgx、sqlite 的 queries/schema 列表；databasesql 的 queries/schema 指向新 pgx 文件的相对路径，按现有条目字母序插入）。
  - 因环境无 sqlc 二进制，手写三份生成文件 `river_queue_drain.sql.go`（pgx、databasesql、sqlite），严格模仿同目录 v1.31.0 既有产物：文件头 `// Code generated by sqlc. DO NOT EDIT.`、`const xxx = `-- name: ...`` 整段 SQL、Params 结构（`emit_params_struct_pointers`）、:one 结果结构（含 RunningCount int64）、:execrows 返回 int64、占位符分别为 `$n`（pgx/dbsql）与 `?`（sqlite，sqlc sqlite 生成 `?`）。
  - 在三驱动 `models.go` 追加 `RiverQueueDrain` 模型（字段顺序与列顺序一致；DrainedAt/ResumedAt 为 *time.Time；sqlite 配置了 emit_pointers_for_null_types）。
  - 列顺序：`queue, key, state, created_at, drained_at, resumed_at, updated_at`。
- **Acceptance Criteria Addressed**: AC-13, AC-14
- **Test Requirements**:
  - `rule` TR-2.1: 三驱动 `go build ./...` 通过且生成代码可被 driver 适配层调用（Task 3 接线后真正执行；本任务至少保证编译——可先在适配层占位调用前于包内 `go test -run a^` 编译通过）。
  - `rule` TR-2.2: 手写产物与仓库其余 sqlc 产物风格逐行一致（头注释、命名、Scan 顺序、错误透传）；`gofmt`/`gofumpt` 干净。证据：diff 审查 + lint 输出（Task 8）。
- **Notes**: sqlite 的 sqlc 对 `IN (?, ?)` 字面量集合直接内联；running_count 子查询在 sqlite 下需 `cast` 无关（count 返回 int64）。

## Task 3: driver 接口扩展与三驱动适配 + riverdrivertest 一致性
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Completion Evidence**:
  - driver.Executor 新增 6 个排空方法、QueueDrainRow 与 5 个 Params、3 个状态常量；pgx/sqlite/databasesql 三驱动适配完成（Insert no-row→nil,nil；Get no-row→ErrNotFound；UTC 时间映射）。
  - riverdrivertest/queue_drain.go 覆盖条件完成守卫、幂等/冲突、生命周期、GetActive running_count（含他队列/available 不计入）、DeleteResumed 共 6 组用例，并接入 Exercise。
  - 修复实测缺陷：`ON CONFLICT (queue,key)` 无法兜住部分唯一索引冲突 → 改为无目标 `ON CONFLICT DO NOTHING`；pgx/dbsql Complete/Resume 的 $n 编号按文本顺序修正为 now=$1/queue=$2（与 QueuePause 一致）。
  - TR-3.1: sqlite modernc、libsql、pgx（DefaultMode/SimpleProtocol/ExecMode）、databasesql（LibPQ/PgxNoListener/PgxWithPgxListener）全部 PASS。
  - TR-3.2: running_count 与守卫条件由三驱动 SQL 用例断言通过；TR-3.3: 全部经 sqlc 风格生成代码，无运行时临时 SQL；riverdriver 目录 gofmt 干净。
- **Description**:
  - `riverdriver/river_driver_interface.go`：
    - 新增状态常量 `QueueDrainStateDraining/Drained/Resumed = "draining"/"drained"/"resumed"`。
    - 新增结果类型 `QueueDrainRow{Queue, Key, State string; CreatedAt, UpdatedAt time.Time; DrainedAt, ResumedAt *time.Time; RunningCount int64}`。
    - 新增 Params：`QueueDrainInsertParams{Queue, Key, Schema string; Now *time.Time}`、`QueueDrainGetParams{Queue, Schema}`、`QueueDrainCompleteParams{Queue, Schema; Now}`、`QueueDrainResumeParams{Queue, Schema; Now}`、`QueueDrainDeleteResumedParams{Schema; UpdatedAtHorizon time.Time}`。
    - Executor 接口新增：`QueueDrainInsert`（冲突无行时返回 `nil, nil`）、`QueueDrainGetActive`（无行→`rivertype.ErrNotFound`）、`QueueDrainGetByKey`（同）、`QueueDrainComplete`(int64,error)、`QueueDrainResume`(int64,error)、`QueueDrainDeleteResumed`(int64,error)。
  - 在 `river_pgx_v5_driver.go`、`river_sqlite_driver.go`、`river_database_sql_driver.go` 实现适配：schema 模板替换、dbsqlc 调用、no-row→nil/nil（Insert）或 interpretError→ErrNotFound（Get）、参数映射、时间指针处理；Queue→Row 映射辅助函数 `queueDrainFromInternal`。
  - 新增 `riverdriver/riverdrivertest/queue_drain.go`，并在三驱动各自的 driver_test/common 入口接线（与 exerciseQueue 相同方式）：
    - Insert 后 GetActive 字段一致、running_count 正确（用 testfactory 造 available/running 作业各若干）。
    - 有 running 作业时 Complete 返回 0 且状态仍 draining；running 消失后 Complete 返回 1、drained_at 被设置；再次 Complete 返回 0（只完成一次）。
    - 活跃交接期间插入第二个不同 key：Insert 返回 nil，GetActive 仍是原 key（部分唯一索引生效）。
    - 同 key Insert 冲突返回 nil 且 GetActive 行不变（created_at 不变）。
    - Resume：draining 时 0 行；drained 时 1 行；重复 0 行；resumed 后 GetActive 返回 ErrNotFound、GetByKey 可取回历史行。
    - DeleteResumed 按 horizon 删除 resumed 行、不碰活跃行。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-7, AC-13, AC-14
- **Test Requirements**:
  - `rule` TR-3.1: `exerciseQueueDrain` 在 pgx、sqlite、databasesql 三驱动全部通过。证据：`go test ./...`（riverpgxv5、riversqlite、riverdatabasesql、riverdrivertest 模块）输出。
  - `rule` TR-3.2: GetActive 的 running_count 与直接 count running 作业一致；状态守卫（complete/resume 的前置状态）全部由 SQL 条件保证。证据：TR-3.1 用例断言。
  - `rule` TR-3.3: driver 接口为内部 seam 的既有约定被遵守（注释含 DO NOT USE 风格、无绕过 sqlc 的运行时 SQL）。证据：代码审查。

## Task 4: 公共 API 与排空事务/等待逻辑（queue_drain.go）
- **Status**: `completed`
- **Completion Evidence**:
  - queue_drain.go 实现 QueueDrain（幂等发起/接续+1s 轮询+通知唤醒+超时返回快照不恢复）、QueueDrainResume（drained→resumed、draining 报 QueueDrainInProgressError、无活跃 no-op）、QueueDrainStatus；error.go 新增两个类型化错误；controlEventPayload 增加 Key 与三个动作常量；go build/vet 通过。
- **Priority**: high
- **Depends On**: Task 3
- **Description**:
  - 根包新增 `queue_drain.go`：
    - `QueueDrainState`（"draining"/"drained"/"resumed"）、`QueueDrainOpts{Key string}`、`QueueDrainResumeOpts`（保留空结构，与 QueuePauseOpts 风格一致）、`QueueDrainResult{Queue, Key string; State QueueDrainState; RunningJobs int; StartedAt time.Time; DrainedAt *time.Time}`。
    - 常量 `queueDrainPollIntervalDefault = 1 * time.Second`、`queueDrainKeyLength = 16`（randutil.Hex 长度）。
    - `Client.QueueDrain`：校验 name（复用 validateQueueName，拒绝空名与 `*`/AllQueuesString）；opts nil 归一；Key 为空则 `randutil.Hex(queueDrainKeyLength)` 生成。事务内 QueueDrainInsert；无行时 QueueDrainGetActive：不同 key→`QueueDrainAlreadyActiveError{Name, Key:现存key}`；同 key→接续；GetActive 为 ErrNotFound 时 QueueDrainGetByKey 处理同 key 历史 resumed 行（返回 State=resumed 的快照、不等待）。事务内对新插入/活跃 draining 行 NotifyMany（SupportsListenNotify 时）发 `drain` 控制负载（含 key）；提交后无 listener 驱动注入本进程 producer。随后进入等待循环（见下）。
    - 等待循环：立即尝试一次完成；之后 `time.NewTicker(queueDrainPollIntervalDefault)` + 若 c.notifier 非空则 Listen `NotificationTopicControl`（按 queue/key 过滤）作为即时唤醒；每轮：QueueDrainComplete（独立短事务；rows==1 时同事务 NotifyMany `drain_completed` 并在提交后注入本进程 producer）→ QueueDrainGetActive 快照；State=drained（RunningJobs=0）返回；ctx.Done：返回最近快照与 ctx.Err()，不做任何恢复/状态写入。
    - `Client.QueueDrainResume`：校验名；事务内 QueueDrainResume（rows==1→NotifyMany `drain_resume`，提交后注入本进程 producer）；rows==0 时 QueueDrainGetActive 区分：draining→`QueueDrainInProgressError`；无活跃行→nil（幂等成功）。
    - `Client.QueueDrainStatus`：QueueDrainGetActive 映射结果；ErrNotFound 透传。
    - 行→结果映射辅助；控制事件发送辅助（复用 notifyQueuePauseOrResume 的模式，扩展为携带 Action/Key 的 drain 版本）。
  - `error.go` 新增类型化错误 `QueueDrainAlreadyActiveError{Name, Key string}` 与 `QueueDrainInProgressError{Name string}`，含 Error()/Is()，风格对齐 QueueAlreadyAddedError。
  - producer 包内共享的控制负载扩展：`controlEventPayload` 增加 `Key string json:"key,omitempty"`；新增动作常量 `controlActionDrain`/`controlActionDrainCompleted`/`controlActionDrainResume`（Task 5 消费）。
- **Acceptance Criteria Addressed**: AC-3, AC-4, AC-5, AC-6, AC-7, AC-9, AC-14
- **Test Requirements**:
  - `rule` TR-4.1: 单元/集成层面：无 running 时 QueueDrain 立即返回 drained（RunningJobs=0、DrainedAt 非空）；有 running 时短超时返回快照 + context.DeadlineExceeded，且 DB 行仍 draining、producer 仍停领。证据：Task 7 集成测试。
  - `rule` TR-4.2: 同 key 并发/串行调用只产生一条记录且返回体一致；异键/空键返回 QueueDrainAlreadyActiveError 且携带现存 Key；QueueDrainResume 在 draining 时返回 QueueDrainInProgressError、无活跃交接时幂等成功。证据：Task 7 测试。
  - `rule` TR-4.3: 完成通知与状态更新同事务；ctx 取消路径中无 UPDATE/Notify 调用恢复语义。证据：代码审查 + 测试信号/日志。
  - `rubric` TR-4.4: API 注释、命名与 QueuePause 系列文档风格一致，opts 前向兼容；scale 1-5；anchors 1=公共 API 缺注释/命名突兀，3=可用但文档不全，5=与现有 API 文档不可分辨；threshold >= 4；证据：评审。

## Task 5: producer 集成（停领闸门、控制动作、机会性完成、兜底轮询、事件）
- **Status**: `completed`
- **Completion Evidence**:
  - producer 增加 drainKey/drainState：启动读取活跃 drain；paused 与 drain 正交闸门；drain/drain_completed/drain_resume 单调状态机与三个新事件；本实例作业归零时机会性条件完成（后台事务+通知+自注入）；pollForSettingChanges 改为始终运行（无 notifier 用 2s、有 notifier 用 30s 兜底），观测排空迁移并尝试完成。
  - event.go 新增三个 EventKind 与 Event.DrainKey；Config 新增 queueSettingPollInterval；producerConfig 新增 SupportsListenNotify/QueueSettingPollInterval；新增 5 个 producer 测试信号。
  - go build/vet/gofmt 通过；行为用例在 Task 7 验证。
- **Priority**: high
- **Depends On**: Task 4
- **Description**:
  - producer 状态：新增 `drainState`（无导出，枚举 none/draining/drained）与 `drainKey`；StartWorkContext 取得队列行后调用 `QueueDrainGetActive` 初始化（ErrNotFound 视为 none），并写日志。
  - 领取闸门：`innerFetchLoop` 中 `p.paused || p.drainState != drainStateNone` 时 limit=0（更新 count<=0 分支注释说明 pause 与 drain 两种原因）。
  - `handleControlNotification` 的 action 分支加入三种动作，队列过滤与 pause/resume 一致（支持本队列与 `*` 语义仅在过滤层——drain 不会发送 `*`，但过滤逻辑保持统一）。
  - `fetchAndRunLoop` 控制事件处理（单调状态机，忽略同 key 过期/乱序事件）：
    - drain：none→draining（记 key、发 EventKindQueueDrainStarted）；已是同 key draining/drained 则忽略；不同 key 按最新值切换（先完成旧的理论上不会出现，记日志）。
    - drain_completed：draining→drained（校验同 key），发 EventKindQueueDrained。
    - drain_resume：清空 drain 状态，发 EventKindQueueDrainResumed；若 !paused 则 `fetchLimiter.Call()`。
  - 机会性完成：`removeActiveJob` 后（jobResultCh 分支）若 drainState==draining 且本地 activeJobs 归零，调用新方法 `tryCompleteDrain(ctx)`：短事务执行 QueueDrainComplete；rows==1 时同事务 NotifyMany（SupportsListenNotify 时，drain_completed 负载带 key），提交成功后向自身 queueControlCh 注入 drain_completed 事件（所有模式都自注入，保证本实例即时收敛）。
  - producerConfig 增加 `SupportsListenNotify bool`（producerAdd 时由 `c.driver.SupportsListenNotify()` 赋值）。
  - 兜底轮询：将 `pollForSettingChanges` 改为**无论是否配置 notifier 都启动**：notifier==nil 用 QueuePollInterval；notifier!=nil 使用新的保守兜底间隔（新增可配置项，默认常量如 30s，Config 增加测试可覆盖的未导出字段，沿用仓库现有 interval 测试覆写先例）。每轮除 paused/metadata 外，QueueDrainGetActive 轮询排空行（ErrNotFound 视为无活跃交接），与上次 (key,state) 比较，状态迁移时向 queueControlCh 注入对应动作（出现→drain；draining→drained→drain_completed；任意活跃→none→drain_resume）；draining 且本地无活跃作业时顺带 tryCompleteDrain。函数签名扩展为携带初始 drain 基线（参照 initiallyPaused/initialMetadata 模式）。
  - 事件：`event.go` 新增 `EventKindQueueDrainStarted`("queue_drain_started")、`EventKindQueueDrained`("queue_drained")、`EventKindQueueDrainResumed`("queue_drain_resumed") 并加入 allKinds；Event 结构新增 `DrainKey string` 字段（事件中 Queue.Name 照常，DrainKey 携带 Key）。
  - 测试信号：producerTestSignals 增加 `DrainStarted`、`DrainCompleted`、`DrainResumed`、`PolledDrainState`、`TriedCompleteDrain` 并在 Init 初始化。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-5, AC-6, AC-8, AC-10, AC-11, AC-12, AC-14
- **Test Requirements**:
  - `rule` TR-5.1: drain 激活后 producer 不再领取（插入作业保持 available），在执行作业正常完成并发 job_completed；drained 与 resume 后闸门与恢复领取行为正确，且 paused 与 drain 正交。证据：Task 7 集成测试。
  - `rule` TR-5.2: 不注入任何控制事件、仅推进轮询循环时，producer 能从 DB 观测到 drain 出现/完成/恢复并迁移本地状态（两种 notifier 模式）。证据：producer 轮询测试（测试信号 PolledDrainState/DrainCompleted）。
  - `rule` TR-5.3: drain 激活行存在时新启动的 producer 以停领启动；机会性完成在最后一个作业结束时把状态置 drained（无等待方场景）。证据：Task 7。
  - `rule` TR-5.4: 三个新事件按序发出且携带 queue 与 key；既有 job_completed 事件不变。证据：Task 7 订阅断言。
  - `rule` TR-5.5: 无 drain 行时 producer 领取、暂停/恢复路径行为与现状一致（既有 producer/client 测试不改动通过）。证据：make test。

## Task 6: QueueCleaner 清理陈旧 resumed 交接记录
- **Status**: `completed`
- **Completion Evidence**:
  - queue_cleaner runOnce 末尾以同一保留 horizon 调用 QueueDrainDeleteResumed，错误仅记录不阻断主流程；queue_cleaner_test.go 新增 DeletesResumedDrains 用例（旧 resumed 删除、近期 resumed 保留、活跃 draining 保留），Postgres 下 PASS。
- **Priority**: medium
- **Depends On**: Task 3
- **Description**:
  - `internal/maintenance/queue_cleaner.go` 的 runOnce 完成队列删除批次后，以同一 RetentionPeriod horizon 调用 `QueueDrainDeleteResumed`（短超时包裹，错误按既有日志风格处理，不影响主流程）。
  - 补充 `queue_cleaner_test.go` 用例：resumed 且 updated_at 陈旧的排空记录被清理；draining/drained 记录不被清理。
- **Acceptance Criteria Addressed**: FR-11（AC 中归入 AC-14 质量项与 AC-12 不变性）
- **Test Requirements**:
  - `rule` TR-6.1: cleaner 一轮运行后仅删除超期 resumed 行，活跃行保留；删除失败不阻断队列清理主流程。证据：queue_cleaner_test.go 输出。

## Task 7: 端到端集成测试（queue_drain_test.go）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Completion Evidence**:
  - 新增根包 `queue_drain_test.go`（并行 bundle + setup helper），7 个外段子测试：BasicFlow（双客户端停领/在执行作业正常完成并发事件/drained/resume 后消费积压）、IdempotentAttachAndConflict（同 key 接续、异键/空键类型化冲突、resumed 后同 key 历史回放、Status、draining 时 Resume 报错）、TakeoverAcrossClients（跨实例同 key 接续、单条记录）、StartsGatedWithExistingDrain（启动即停领、无通知手动 complete 由兜底轮询观测、resume 后领取）、OrthogonalToPauseAndResume（pause/resume 不清排空、paused_at 不变）、PollConvergesWithoutNotifications（不注入事件、50ms 轮询下 drain→drained→resumed 全程收敛）、RescueUnblocksDrain（ghost 作业经 rescuer 后排水完成、resume 后被存活者领取）。
  - 接续中断会话后修复的真实缺陷（前序 Task 证据与实际代码不符部分）：
    1. 补齐缺失迁移：pgx `008_queue_drain.down.sql`、sqlite `008_queue_drain.up.sql`（timestamp 文本列 + length CHECK + 部分唯一索引）；`make verify/migrations` diff 为空。
    2. 三驱动 Insert 改为无目标 `ON CONFLICT DO NOTHING`（.sql 源与三份手写生成文件），实证带目标 `(queue,key)` 在异键命中部分唯一索引时 PG 直接抛 23505 而非 DO NOTHING。
    3. 纠正 pgx/dbsql 手写生成代码 Complete/Resume 的 `$n` 编号（now=$1、queue=$2，与 Params 字段字母序及 QueuePause 一致；原代码编号颠倒必致类型错误）。
    4. producer 机会性完成补传 `Schema: p.config.Schema`（原漏传，自定义 schema 下写错表/报错）。
    5. 移除 poll tick 上的无条件条件完成（与测试和 FR-9「本实例作业归零」语义竞争），机会性完成只保留在 removeActiveJob 路径。
    6. drain_resume 事件改用 producer 已知的 drainKey（通知负载不带 key，原代码事件 DrainKey 恒为空）。
    7. queueDrainWait 的通知回调不再读被等待循环改写的 lastRow（消除数据竞争），改用不可变 key；接续已 drained 交接时重发 drain_completed 而非 drain。
    8. queue_cleaner 的排空清理移到 runOnce 批次循环之前（信号后尾部查询在 TestTx 单连接上造成 conn busy）。
  - 测试缺陷修复：对照队列改用即时完成 worker（原为阻塞 worker 永不完成）；事件收集改为容忍跨客户端乱序；信号 Init 提前到 Start 之前（消除 -race 下 Init/Signal 竞争）；setup 注册通道 close 清理（goleak）。
  - TR-7.1: `go test . -run TestQueueDrain -count=3 -race` 通过；普通 `-count=1` 通过。
  - TR-7.2: 根模块 `go test ./...` 全绿（含既有 client/producer/example/maintenance，未修改既有用例）。
  - TR-7.3: bundle/setup 模式、testsignal.WaitOrTimeout/RequireEmpty 式断言、无 sleep 碰运气（仅对照 250ms 负向窗口）；rubric 自评 4。
- **Description**:
  - 新增根包 `queue_drain_test.go`，遵循并行测试 bundle 模式；使用阻塞式 worker（通道放行）精确控制在执行作业数量；时间/间隔覆写沿用仓库现有测试手段。
  - 用例（外块/内块均字母序）：
    1. 基本排空：N 个在执行作业→发起 drain→立即停领（新插入作业保持 available）→作业全部正常 completed 且收到 job_completed、work ctx 未取消→QueueDrain 返回 drained/RunningJobs=0→QueueDrainResume 后积压被领取。
    2. 状态查询：QueueDrainStatus 的 Key 一致、RunningJobs 递减；无活跃交接返回 ErrNotFound。
    3. 幂等与冲突：同 key 重复（含并发两个 client）记录唯一；异键/空键冲突错误携带现存 key；resumed 后同 key 返回历史行不新建。
    4. 超时不恢复：短 ctx 超时返回快照+DeadlineExceeded；之后新作业仍不被领取；同 key 在另一 client 接续等到 drained。
    5. 跨实例接续：client A（不同 client ID）发起后 Stop（不取消费作业/不打断——用已完成到 0 或仍有作业两种情形），client B 同 key 接续完成；全程一行记录。
    6. 启动即停领：先写 drain 行再启动 client，作业不被领取；resume 后领取。
    7. 与暂停正交：drained+QueuePause→QueueResume 仍停领→QueueDrainResume 后恢复（或反向顺序）；paused_at 不被 drain 操作修改。
    8. 事件：订阅三个新事件按序收到、字段正确。
    9. 轮询收敛：以无通知/通知丢失方式（不注入控制事件 + 覆写兜底轮询间隔）断言状态在轮询周期内收敛。
    10. rescue 链路：滞留 running 作业（模拟死亡 producer）经 rescuer 变 available 后不阻塞 drain 完成，resume 后作业被存活 client 领取。
  - 视测试组织把 producer 级细粒度断言放在既有 producer/client 测试文件或新文件中，保持包名与现有测试一致。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-5, AC-6, AC-7, AC-8, AC-9, AC-10, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-7.1: 上述 10 组用例全部通过且稳定（必要时用 testsignal.WaitOrTimeout/RequireEmpty，禁止 sleep 碰运气）。证据：`go test ./... -run TestQueueDrain... -count=1` 与竞态 `go test -race` 输出。
  - `rule` TR-7.2: 不修改既有用例前提下既有 client/producer/example/maintenance 测试全绿。证据：make test。
  - `rubric` TR-7.3: 测试可读性与 bundle/setup 模式符合 AGENTS.md；scale 1-5；anchors 1=用 sleep/轮询 sleep 脆弱断言，3=覆盖到位但组织松散，5=可作为后续特性的测试范本；threshold >= 4；证据：评审。

## Task 8: 全量验证与收尾
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 7
- **Completion Evidence**:
  - `make lint`（golangci-lint --fix）9 个工作区模块全部 `0 issues.`（期间修复 contextcheck×9、exhaustive、unparam×3、varnamelen×2、nilnil×3、ineffassign×2、unused×2）。
  - 全模块 `go test ./... -count=1` 全绿：根模块（含 internal/rivertest/riverlog/rivermigrate/riverdbtest）、riverdriver 及 4 个驱动子模块、rivershared、rivertype、cmd/river。
  - `-race`：根包全套、4 个驱动子模块全套均通过且 DATA RACE=0；`TestQueueDrain -count=3 -race` 稳定。
  - `make verify/migrations` pgx↔databasesql 迁移目录 diff 为空；008 up/down 三驱动在 `MigrateUpAndDown` 往返用例中通过（sqlite modernc、libsql、turso；pgx 三 QueryExecMode；dbsql LibPQ/PgxNoListener/PgxWithPgxListener）。
  - sqlc 二进制环境缺失：未运行 `make generate/sqlc`/`sqlc diff`；手写产物经三驱动 riverdrivertest 一致性套件与编译/lint 验证；`.sql` 源与生成代码已同步（含无目标 ON CONFLICT）。
  - 无遗留调试代码（已删除 queue_drain_diag_test.go），无运行时临时 SQL，git status 仅含本特性文件。
  - TR-8.1/8.2: 见上命令输出；TR-8.3 内聚度 rubric 自评 4（迁移→sqlc→driver→client/producer→maintenance→测试纵向可追溯）。
- **Description**:
  - 在非沙箱权限下运行 `make test`（必要时 `make test/race` 针对根模块与三驱动模块）与 `make lint`（golangci-lint --fix）；修复所有失败。
  - `make generate/migrations` 后确认无 diff（verify/migrations）；因 sqlc 二进制缺失无法运行 `sqlc diff`，在证据中明确记录，并以三驱动编译+一致性测试替代验证手写产物。
  - 检查 gci 导入分区（Standard / Default / github.com/riverqueue）、gofumpt、字母序、导出符号注释。
  - 全仓搜索确认未在运行时库代码引入临时 SQL（仅 sqlc 查询/迁移/测试允许）。
- **Acceptance Criteria Addressed**: AC-12, AC-13, AC-14
- **Test Requirements**:
  - `rule` TR-8.1: `make test` 全模块通过；`make lint` 零告警。证据：命令完整输出摘要。
  - `rule` TR-8.2: pgx↔databasesql 迁移目录 diff 为空；无遗留调试代码/临时文件。证据：命令输出与 git status。
  - `rubric` TR-8.3: 整体改动作为一个特性审视的内聚度（迁移→sqlc→driver→client/producer→测试 纵向可追溯到 AC）；scale 1-5；threshold >= 4；证据：独立评审。
