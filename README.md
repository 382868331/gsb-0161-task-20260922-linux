# tasktree — 结构化任务树的取消、异常归并与成功策略

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举验收
python demo.py                            # 正常/边界/错误 + 成功策略、清理失败
```

## 模型

- 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
- 每个任务同时是其子任务的 scope；任务树 = 根 scope 嵌套全部后代。
- 任务生命周期：`active` →（finish/fail 记录结局）→ `draining`
  （等待后代退出）→ `exited`。**父任务只有在全部后代退出后才算完成**。
- scope 生命周期：`open` →（close_scope）→ `closing`（先请求取消全部
  未结束后代，等待子树退出）→ `closed`。
- watch 生命周期：`watching`（按事件序判定）→ `cleaning`（取消未完成
  成员并等清理确认）→ `done`（汇总）。

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None) -> Receipt` | 在 open scope 下创建任务；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `watch(scope_id=0, strategy="all", need=None) -> Receipt` | 对 scope 的**直接子任务**（创建时刻快照，成员集冻结）布防成功策略；返回回执带 `watch_id` |
| `watch_result(watch_id) -> dict` | 清理确认完成（done）后读取汇总；未完成抛 `WatchStateError`，未知 id 抛 `UnknownWatchError` |
| `watches() -> dict` | 全部布防的快照（phase/成员/各类计数等） |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/children，新增 outcome_seq/watch/watch_phase） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |

取消与结局的优先级：

- 取消已投递后 `finish` → 结局 `cancelled`（结果被取消取代）；
- 取消已投递后 `fail(exc)` → 结局 `failed`（业务异常优先于取消）；
- shield 内取消未投递时 `finish` → 正常 `finished`，离开时 pending 取消被丢弃。

## 成功策略（watch）

`watch(scope_id, strategy, need=None)` 对某 scope 的**直接子任务**布防，
成员集为创建时刻的快照（之后冻结），严格**按输入事件顺序**判定：

| strategy | 达标条件 | 永不可能达标 |
|---|---|---|
| `"all"` | 全部成员 `finished` | 任一成员 `failed`/`cancelled`，或取消已**投递**给任一尚未成功的成员 |
| `"first"` | 第一个 `finished` 出现（按事件序） | 不存在仍可能成功的成员（全部 failed/cancelled 或取消已投递） |
| `"n_succeed"`（需 `need>=1`） | `finished` 数达到 `need` | `finished + 仍可能成功数 < need` |

"仍可能成功"= 未记录结局且取消尚未投递。取消一旦投递（`cancel_delivered`），
该成员的结局只能是 `cancelled`（finish）或 `failed`（fail），故按注定
不成功计入；shield 内仅 pending 的取消不算（任务仍可正常 finish 后丢弃
pending）。判定在每个被接受事件（含外部 `cancel`/`close_scope`/
`leave_shield`）后进行，因此外部动作也能让目标即时变为不可达。

阶段流转：`watching` →（达到结束条件）→ `cleaning`（请求取消全部未完成
成员的**整棵子树**，等待全部成员退出 = 清理确认）→ `done`（生成汇总）。
触发与汇总可能在同一事件内完成（如最后一个成员成功时）。

汇总（`watch_result`）的关键字段：

- `status`：`satisfied` / `satisfied_with_cleanup_errors`（达标但清理期
  出现业务失败）/ `unsatisfiable`；
- `goal_met`：结束条件是否因达标触发；
- `members` / `winners`：冻结的成员 id / 计入达标的成功 id；
- `successes`：`{task_id,name,value}`，**按结局事件序**；
- `failures`：`{task_id,name,error,phase}`，按事件序；`phase` 区分
  `"trigger"`（判定阶段失败，序号 ≤ trigger_seq）与
  `"cleanup"`（**清理失败**：取消已请求后成员仍以业务异常退出）；
- `cancelled`：被取消的成员 id（按事件序）；
- `trigger_seq` / `done_seq`：触发与汇总就绪的全局事件序号。

规则与边界：

- 失败与取消分别记录；清理取消沿成员整棵子树传播（与 `close_scope`
  一致），但汇总只统计**成员本身**。
- 清理请求受 shield 约束（取消挂为 pending），因此汇总天然等待 shield
  离开；手动 `cancel`、`close_scope` 与 watch 清理可以叠加，互不破坏。
- watch 进行中（watching/cleaning）禁止向该 scope `spawn`
  （`WatchStateError`）；成员自己的 scope 内仍可 spawn。
- 每个 scope 同一时刻至多一个进行中的 watch；`done` 后可再次布防，
  新成员集为当时全部直接子任务（含已退出者，其历史结局按历史事件序参与
  判定，支持"补防/回填"）。
- 边界：空 scope 上 `all` 立即 `satisfied`（0 个成员 vacuously true）；
  `first`/`n_succeed` 立即 `unsatisfiable`。
- 每个事件回执新增 `watch_updates=((watch_id, 新阶段), ...)`，记录本事件
  内发生的阶段变迁（可同事件连续 watching→cleaning→done）；`watch()`
  回执另带 `watch_id` 与 `watch_phase`。

## 错误语义

- 非法输入即时抛出、可定位（消息含参数名与值），不静默纠正：
  - 整数参数拒绝 `bool`（`TypeError`）；
  - 数值结果拒绝 NaN/Infinity（`ValueError`，检查 `float`/`complex`）；
  - `fail` 只接受 `Exception` 实例；`TaskCancelled` 实例被拒绝
    （取消不是业务异常，必须用 `cancel`/`close_scope` 表达）；
  - `UnknownTaskError`、`ScopeClosedError`、`FinishConflictError`、
    `ShieldError`（leave 多于 enter）、`TaskStateError`（如对 root 的
    任务体操作）、`ResourceLimitError`（超上限）均为 `TaskTreeError` 子类。
  - watch 相关：`WatchError`（基类）及其子类 `WatchStateError`（进行中
    spawn、重复进行中布防、closing/closed scope 布防、done 前取汇总）与
    `UnknownWatchError`（同时是 `LookupError`）；非法 `strategy`/`need`
    为普通 `ValueError`/`TypeError`，与其它输入校验口径一致。
- 每个操作要么完整生效、要么即时抛异常且不改状态（含 log 与 watch 状态
  不变）；不存在未说明的部分变更。

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
- **watch 判定只认事件序**：以结局记录时的全局事件序号排序，`winners`/
  `successes`/`failures`/`cancelled` 均按事件序而非创建序；清理失败
  （取消请求后仍以业务异常退出）与判定阶段失败用 `phase` 区分但都保留，
  达标不掩盖清理错误。
- **watch 不重复实现取消**：清理复用既有的子树取消 + shield + 等待链
  机制，因此 watch 清理与手动 cancel、close_scope 可安全叠加；汇总只
  统计成员本身，成员后代的结局仍走原有 exception_group。
- **成员集在创建时冻结**：判定对象必须确定；进行中禁止向该 scope spawn。
  再布防时已退出成员按历史结局参与，避免"补防后历史成功消失"的歧义。

## 已知限制

- 纯同步模型：不模拟时间、定时器或真正的并发交错；事件顺序完全由
  调用方决定。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- `watch_result` 只重建外层容器防止外部篡改；结果值与异常对象保持身份、
  不做深拷贝（与 finish 结果的存储约定一致）。
- `n_succeed` 的 `need` 若大于成员数，布防事件即判定结束条件不可达
  （立即进入 cleaning 取消全部成员，done 后 `unsatisfiable`），不单独
  报错，因为该目标在数学上不可达。
- watch 只观察直接子任务一层；嵌套作用域请分别布防。
- 每次事件后对全部进行中 watch 做不动点推进，最坏开销为
  O(事件数 × watch 数 × 成员子树规模)；未做大规模性能优化，题面上限
  （1 万任务/128 深度）下的常规用法足够。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
