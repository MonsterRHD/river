# River 队列排空（Queue Drain）- Independent Review

- [x] CP-R1: 发起排空后全客户端立即停领，在执行作业正常完成并发既有事件
  - **Type**: `rule`
  - **Covers**: AC-1, AC-2
  - **Evidence**: R1/R2/R3 三轮独立核验 pass；BasicFlow 双客户端 DrainStarted 后 gated 作业保持 available、对照队列正常消费；阻塞作业 work ctx 未取消、completed + job_completed；闸门 producer.go innerFetchLoop 与 paused 正交；`TestQueueDrain -race` 稳定。

- [x] CP-R2: 返回稳定 Key 与运行中数量，归零才原子完成且仅一次
  - **Type**: `rule`
  - **Covers**: AC-3, AC-4
  - **Evidence**: riverdrivertest Complete 三用例（有 running 0 行、归零 1 行、再次 0 行）在 9 驱动配置通过；running_count 标量子查询 available/他队列不计入；IdempotentAttachAndConflict 断言数量与 Key。

- [x] CP-R3: drained 后保持停领到显式恢复，与暂停生命周期正交
  - **Type**: `rule`
  **Covers**: AC-5
  - **Evidence**: OrthogonalToPauseAndResume、BasicFlow；QueueDrainResume 并发翻转重试（I-2/F-02）+ ResumeRetriesAfterMidCallCompletion 确定性测试。

- [x] CP-R4: 意图/进度持久化，跨实例接续，同 Key 幂等、异键类型化冲突
  - **Type**: `rule`
  - **Covers**: AC-6, AC-7
  - **Evidence**: TakeoverAcrossClients、StartsGatedWithExistingDrain、IdempotentAttachAndConflict（resumed 历史回放不新建）；部分唯一索引 + 无目标 ON CONFLICT 经 9 配置实证。

- [x] CP-R5: 通知丢失仅靠轮询收敛；等待超时只结束等待且不恢复队列
  - **Type**: `rule`
  - **Covers**: AC-8, AC-9
  - **Evidence**: PollConvergesWithoutNotifications、PollConvergesAcrossKeyTransition（I-3/F-2-01 回归）；drainExpectTimeout 断言 DeadlineExceeded + draining 快照且 DB 不写恢复；poll 2s/30s 双间隔始终运行。

- [x] CP-R6: 三个生命周期事件按序发布且携带 queue/key，既有作业事件不变
  - **Type**: `rule`
  - **Covers**: AC-10
  - **Evidence**: 3 EventKind 入 allKinds；resume 事件用 producer 已知 drainKey；BasicFlow 事件收集断言。

- [x] CP-R7: 死亡实例作业经 rescue 后不阻塞排空，恢复后由存活者领取
  - **Type**: `rule`
  - **Covers**: AC-11
  - **Evidence**: RescueUnblocksDrain 端到端（Rescuer 推进、drained 期间不领取、resume 后领取）。

- [x] CP-R8: 未使用排空时启动/暂停/恢复/停止/取消行为不变
  - **Type**: `rule`
  - **Covers**: AC-12
  - **Evidence**: git diff 既有路径纯增量；根模块 `go test ./... -p 1`、根包全套 -race、8 个工作区模块全绿；既有用例零修改。

- [x] CP-R9: 三驱动迁移往返、sqlc 查询与 riverdrivertest 一致性等价
  - **Type**: `rule`
  - **Covers**: AC-13
  - **Evidence**: 008 up/down 三驱动 MigrateUpAndDown（9 配置）；verify/migrations diff 空；truncate case 0/8 与枚举同步；6 查询手写产物与 .sql 源等价（R1 修正 $n 编号与 ON CONFLICT，R2 核实 sqlc.yaml 三模块注册）。

- [x] CP-U1: 实现质量与 River 分层/风格一致性
  - **Type**: `rubric`
  - **Covers**: AC-14
  - **Scale**: 1-5
  - **Anchors**: 1 = 绕过 driver 接口写临时 SQL、测试缺失或风格割裂；3 = 功能正确但分层或测试组织有明显瑕疵；5 = 如同原生 River 维护者写出的代码
  - **Pass Threshold**: >= 4
  - **Evidence**: R1 4/5 → R2 4/5 → **R3 4/5（通过）**。分层 migration→sqlc→driver→client/producer→maintenance→测试纵向可追溯；9 模块 lint 0 issues；测试 bundle/信号/字母序规范。未达 5 分因本机无 sqlc 二进制，生成闭环以手写产物 + 三驱动一致性套件替代验证（已在 tasks.md 记录）。

## Review History

### Review R3
- **Result**: `pass`
- **Evidence**:
  - 全新审查员独立推演 drainTransitionPayloads × 三个 action 守卫的全组合状态矩阵（nil/draining/drained × 同/异 key × 缓冲满重放），确认 F-2-01 永久停领路径消除；回归用例在旧实现下必然失败。
  - `TestQueueDrain -count=3 -race` 全绿、DATA RACE=0；根模块 `go test ./... -p 1` 与 8 个工作区模块全绿；riverdrivertest 9 驱动配置覆盖；9 模块 golangci-lint 0 issues；verify/migrations 空 diff。
  - 0 个 actionable；2 个 advisory：F-3-01（极端选择性丢通知下轮询基线发散，暴露面与既有 pause/resume 轮询相同，接受现状）、F-3-02（跨 key 合成 resume 与 adopt 事件间的瞬时 fetchLimiter 窗口）。
  - F-3-02 已在 R3 后顺手加固：合成 resume 载荷带仅内存标记 suppressFetchTrigger，不触发 fetchLimiter（真正解除停领的末态事件才触发）；加固后 drain -race×2、lint、根模块与全部驱动模块全量复测全绿。
  - F-3-01 不修复（需要把轮询对账改为以 fetchAndRunLoop 权威本地状态为准，属既有轮询机制的共性问题，超出本特性范围；前提需控制事件缓冲 100 溢出且恰好丢失末个 resume）。

### Review R2
- **Result**: `fail`
- **Evidence**:
  - F-01/F-02/F-05 修复核实有效；F-04 主体修复正确但引入新缺陷 F-2-01；F-03 风险可接受（有界 goroutine、-race/goleak 干净）。
  - CP-R1..R4、R6..R9 pass；CP-U1 4/5；CP-R5 fail：轮询在「单周期跨 key 跳转（K1 draining → K2 drained → resumed）」时不收敛——drain_completed{K2} 与 drain_resume{K2} 被异键守卫拒绝，producer 永久停领（审查员外部程序实证 STUCK_GATED）。
  - 1 个 actionable：F-2-01（低-中）→ Issue I-3；2 个 advisory（F-2-02=F-03 接受现状；F-2-03 子测试块字母序——本轮顺手整理）。
  - 测试输出：TestQueueDrain -race -count=3 全绿；根模块串行全量 EXIT=0；9 驱动配置 603 PASS；lint 0 issues；并行全量 2 处超时经单包/串行复测判定为环境过载 flake（jobcompleter 本特性零改动）。

### Review R1
- **Result**: `fail`
- **Evidence**:
  - CP-R1..CP-R9 全部 pass（独立运行 9 模块构建/测试/竞态/lint、riverdrivertest 2829 项 9 驱动配置全绿、make verify/migrations 为空；SQL 与手写生成代码逐行比对）。
  - CP-U1: 4/5（分层与风格达原生水准，但生成闭环未完成 + 一个窄并发竞态，未达 5 分）。
  - 2 个 actionable 发现：
    - F-01（中）：pgx 与 sqlite 的 dbsqlc/sqlc.yaml 未在 queries/schema 列表注册 river_queue_drain.sql（仅 databasesql 注册），有 sqlc 环境下 `make generate/sqlc` 会重新生成掉 RiverQueueDrain 模型与查询，造成构建损坏与 sqlc drift；tasks.md Task 2/8 相关证据失实。→ Issue I-1。
    - F-02（低）：QueueDrainResume 的 UPDATE 返回 0 与 GetActive 之间，若交接被并发从 draining 推进到 drained，当前代码把 drained 当 no-op 成功返回，行实际仍 drained、队列仍停领且无 resume 通知，违反 FR-6。→ Issue I-2。
  - 3 个 advisory（不阻断）：F-03 机会性完成 goroutine 脱离 WaitGroup/Stop 期错误日志级别；F-04 轮询首拍直接看到 drained 时事件缺 queue_drained（本轮顺手修复 nil→drained 注入动作）；F-05 .sql 查询未字母序（本轮顺手修复）。
  - 完整报告：见本次 Review 委派输出（2026-09-25）。
