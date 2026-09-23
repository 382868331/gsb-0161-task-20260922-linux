# tasktree — 结构化任务树的取消、异常归并与监督策略

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举验收
python demo.py                            # 正常结果 + 异常组/等待链 + 监督策略 + 拒绝边界
```

## 模型

- 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
- 每个任务同时是其子任务的 scope；任务树 = 根 scope 嵌套全部后代。
- 任务生命周期：`active` →（finish/fail 记录结局）→ `draining`
  （等待后代退出）→ `exited`。**父任务只有在全部后代退出后才算完成**。
- scope 生命周期：`open` →（close_scope）→ `closing`（先请求取消全部
  未结束后代，等待子树退出）→ `closed`。

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None) -> Receipt` | 在 open scope 下创建任务；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `watch(task_id=0, strategy="all", count=None) -> Receipt` | 在 scope 上安装监督观察者（`all`/`first`/`quorum`）；成员在调用时冻结，此后该 scope 拒绝 spawn |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/children/watcher） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链，含 watcher 清理确认等待）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `watcher_summary(task_id=0) -> dict \| None` | watcher 当前/定稿汇总；未安装返回 None |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |

取消与结局的优先级：

- 取消已投递后 `finish` → 结局 `cancelled`（结果被取消取代）；
- 取消已投递后 `fail(exc)` → 结局 `failed`（业务异常优先于取消）；
- shield 内取消未投递时 `finish` → 正常 `finished`，离开时 pending 取消被丢弃。

## 监督策略（watch）

`watch(scope_id, strategy=...)` 在某个 scope 上安装一个监督观察者，监督
对象是该 scope 的**直接子任务**（孙任务不算成员，但清理取消仍沿子树传播）：

| strategy | 结束（满足）条件 | required |
|---|---|---|
| `"all"`（默认） | 全部成员成功 | 成员数 |
| `"first"` | 任意一个成员成功 | 1 |
| `"quorum"` | `count` 个成员成功，必须同时传 `count=<int>`（≥1 且 ≤ 成员数） | count |

语义要点：

- **成员集在 `watch()` 时冻结**：先 spawn 全部成员、再 watch；安装后向该
  scope `spawn` 抛 `ScopeClosedError`。允许安装时部分成员已有结局，按这些
  结局落定的事件顺序补登记后立即判定一次。
- **按输入事件顺序判定**：每个成员记录结局（`finish`/`fail`/取消后退出）
  的当下更新计数。成功数达到需求 → `satisfied`；成功数 + 仍未决数 < 需求
  （数学上已不可能满足）→ `unsatisfied`。两个条件都没到就继续等待，
  **绝不提前判定**。
- **结束即清理，等确认才汇总**：一旦判定（满足或不满足），scope 立刻
  `open → closing`，取消全部未完成后代（走原有取消协议：shield 只记
  pending，离开才投递），并等待**每个成员退出**（清理确认）。全部退出后
  scope 才 `closed`，watcher 的 `closed_seq` 才赋值、汇总才定稿。
- **失败与取消分别记录**：汇总中 `succeeded` / `failed` / `cancelled`
  三个列表各按输入事件序排列、互斥且只含成员。`failed` 是业务失败结局
  （`fail()`，包括清理期间才发生的失败）；`cancelled` 是取消投递后以
  `finish` 退出的结局。清理确认期间（决策事件之后）记录业务失败的成员
  另记入 `cleanup_failures`；**清理失败不推翻已达成的满足结论**，但作为
  真实业务异常照样进入 `exception_group()`。
- watcher 的 `state`：`pending`（未达结束条件）→ `satisfied` /
  `unsatisfied`（已判定，等清理确认或已确认）；若在结束条件达成前手工
  `close_scope`，watcher 记为 `aborted`、`satisfied=None`，不给成功/失败
  结论，但已发生的成员结局仍如实记账。
- 每个 scope 至多一个 watcher；watcher 只在 scope `open`、且属主任务体
  未记录结局时可安装。

`watcher_summary(task_id)` 返回的字典字段：
`scope, strategy, required, members, state, satisfied, succeeded, failed,
cancelled, cleanup_failures, decided_seq, closed_seq`；同样的内容也出现在
`status()[id]["watcher"]`。未安装 watcher 的 scope 返回 `None`。

## 错误语义

- 非法输入即时抛出、可定位（消息含参数名与值），不静默纠正：
  - 整数参数拒绝 `bool`（`TypeError`）；
  - 数值结果拒绝 NaN/Infinity（`ValueError`，检查 `float`/`complex`）；
  - `fail` 只接受 `Exception` 实例；`TaskCancelled` 实例被拒绝
    （取消不是业务异常，必须用 `cancel`/`close_scope` 表达）；
  - `UnknownTaskError`、`ScopeClosedError`、`FinishConflictError`、
    `ShieldError`（leave 多于 enter）、`TaskStateError`（如对 root 的
    任务体操作）、`ResourceLimitError`（超上限）、`StrategyError`
    （watch 策略名/count 非法、成员为空或永不可能、scope 不可 watch、
    重复 watch）均为 `TaskTreeError` 子类。
- 每个操作要么完整生效、要么抛异常且不改状态；不存在未说明的部分变更。

## 输入上限

- `MAX_TASKS = 10_000`（含根，可在构造时以 `max_tasks=` 调整）
- `MAX_DEPTH = 128`（根深度为 0，`max_depth=` 可调）
- `MAX_NAME_LEN = 64`（任务名最大字符数）

## 浮点误差口径

本库不做任何浮点运算：浮点结果仅做有限性校验（拒绝 NaN/Infinity），
存储与幂等比较均用精确 `==`，不涉及任何容差。因此没有误差区间可声明；
任何"近似相等"的语义都有意不提供。

## 设计取舍

- **取消是请求而非强制**：与结构化并发一致，取消投递后任务仍需以
  finish/fail 显式退出，scope 关闭因此天然形成等待链，便于演示与测试。
- **取消沿子树传播**：`cancel(t)` 与 `close_scope(t)` 都作用于整个未结束
  子树，保证关闭总能取得进展；shield 按任务逐个拦截。
- **异常组扁平按创建序**：父 id 恒小于子 id，直接按 id 序归并即满足
  "父先于子"；不嵌套子组，语义更简单、可预测。
- **finish 冲突报错、跨通道晚到忽略**：同通道重复完成且结果不一致几乎
  一定是 bug；而异步中"任务已退出后迟到的完成"是常态，记 notes 忽略。
- **监督成员集在 watch() 时冻结**：让"需要几个成功"的分母确定、可判定
  （count > 成员数直接拒绝），也避免清理途中又加入新成员导致永不收敛。
  允许 watch 前先结束部分成员，补登记按结局事件序，规则只有一处。
- **满足结论不被清理失败推翻**：首个/配额成功是既成事实；清理中的业务
  失败单独记录（`cleanup_failures`）并入异常组，由调用方决定如何处理，
  而不是把一次成功的监督改判为失败。
- **汇总只在清理确认后定稿**：决策与成员实际退出之间有明确时间窗口
  （`decided_seq` vs `closed_seq`），等待链在该窗口内可见，杜绝"汇总结论
  已出、任务还在跑"的半更新状态。

## 已知限制

- 纯同步模型：不模拟时间、定时器或真正的并发交错；事件顺序完全由
  调用方决定。
- 监督范围限定为一层：watcher 只监督 scope 的直接子任务；嵌套需求请在
  内层 scope 上再安装 watcher（可并存）。
- 成员集在 `watch()` 时冻结，不支持监督途中动态加入；每个 scope 至多一个
  watcher；本次迭代不提供资源额度、截止时间分析等未要求能力。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
