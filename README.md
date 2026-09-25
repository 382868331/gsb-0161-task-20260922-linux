# tasktree — 结构化任务树的取消、异常归并与逻辑时钟截止时间

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。时间是非递减整数逻辑时钟，只能由 `advance_time` 显式推进；
不 sleep、不创建真实线程、不使用真实定时器。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
./.venv/bin/python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举验收
./.venv/bin/python demo.py                            # 正常结果 + 异常组/等待链 + 拒绝边界 + 逻辑时钟截止时间
```

## 模型

- 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
- 每个任务同时是其子任务的 scope；任务树 = 根 scope 嵌套全部后代。
- 任务生命周期：`active` →（finish/fail 记录结局）→ `draining`
  （等待后代退出）→ `exited`。**父任务只有在全部后代退出后才算完成**。
- scope 生命周期：`open` →（close_scope）→ `closing`（先请求取消全部
  未结束后代，等待子树退出）→ `closed`。
- 逻辑时钟：`now` 从 0 开始的非递减整数，仅由 `advance_time` 推进；
  推进到时刻 T 时，先按 (截止时刻, 任务创建序) 触发全部到期条目
  （`deadline <= T`，恰好相等也算到期），再处理调用方的下一条业务事件。

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None) -> Receipt` | 在 open scope 下创建任务；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `set_deadline(task_id, deadline) -> Receipt` | 给任务或 scope（含 root）设置/更新截止时刻（整数，不得小于 `now`）；更新使旧定时条目作废 |
| `clear_deadline(task_id) -> Receipt` | 清除截止时刻；幂等 |
| `advance_time(now) -> Receipt` | 推进逻辑时钟（非递减，相等允许）；触发全部到期条目后返回 |
| `next_deadline() -> (int, int) \| None` | 下一待触发时刻与到期来源 `(deadline, task_id)`，供模拟器逐步驱动 |
| `now -> int` | 当前逻辑时钟时刻（属性） |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/deadline/children） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链，含未到的截止时间）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |

取消与结局的优先级：

- 取消已投递后 `finish` → 结局 `cancelled`（结果被取消取代）；
- 取消已投递后 `fail(exc)` → 结局 `failed`（业务异常优先于取消）；
- shield 内取消未投递时 `finish` → 正常 `finished`，离开时 pending 取消被丢弃。

截止时间语义：

- **到期只提出取消请求**：任务到期等价于对其子树执行 `cancel` 的取消
  部分；root scope 到期 = 请求取消全部未结束后代。shield 内仍只记
  pending，scope 的关闭仍必须满足 `close_scope` 的既有等待条件，
  到期不绕过它（root 到期后 scope 仍是 `open`）。
- **父 scope 可设自己的截止时间**；子任务不直接复制父时间，但父到期
  引发的取消按原路径（子树传播）影响未结束后代；已完成子任务不受影响。
- **完成后到期记录失效**：任务记录结局（finish/fail）或 scope closed
  后，其截止时间立即作废；更新截止时间会使旧定时条目按代次作废，
  旧条目不得影响同一任务的新一代定时。
- **到期取消不是业务异常**：cancelled 不进入 `exception_group`，超时
  不会被记成业务异常混入聚合。
- 截止时刻 finish 的先后顺序由事件序决定：先 finish 再推进 → 正常完成；
  先推进到（恰好）截止时刻再 finish → cancelled。

## 错误语义

- 非法输入即时抛出、可定位（消息含参数名与值），不静默纠正：
  - 整数参数（含 `now`、`deadline`）拒绝 `bool`（`TypeError`）；
  - 数值结果拒绝 NaN/Infinity（`ValueError`，检查 `float`/`complex`）；
  - 时间倒退（`advance_time` 小于当前 `now`）与过去时刻的截止时间
    拒绝（`ValueError`）；
  - 对已结局任务或已 closed 的 scope 设置截止时间拒绝（`TaskStateError`）；
  - `fail` 只接受 `Exception` 实例；`TaskCancelled` 实例被拒绝
    （取消不是业务异常，必须用 `cancel`/`close_scope` 表达）；
  - `UnknownTaskError`、`ScopeClosedError`、`FinishConflictError`、
    `ShieldError`（leave 多于 enter）、`TaskStateError`（如对 root 的
    任务体操作）、`ResourceLimitError`（超上限）均为 `TaskTreeError` 子类。
- 每个操作要么完整生效、要么抛异常且不改状态；不存在未说明的部分变更。

## 输入上限

- `MAX_TASKS = 10_000`（含根，可在构造时以 `max_tasks=` 调整）
- `MAX_DEPTH = 128`（根深度为 0，`max_depth=` 可调）
- `MAX_NAME_LEN = 64`（任务名最大字符数）
- 逻辑时钟与截止时间为非负整数，无单独上限（受 Python int 精度限制）

## 浮点误差口径

本库不做任何浮点运算：浮点结果仅做有限性校验（拒绝 NaN/Infinity），
存储与幂等比较均用精确 `==`，不涉及任何容差。因此没有误差区间可声明；
任何"近似相等"的语义都有意不提供。

## 设计取舍

- **取消是请求而非强制**：与结构化并发一致，取消投递后任务仍需以
  finish/fail 显式退出，scope 关闭因此天然形成等待链，便于演示与测试。
- **到期复用取消路径**：截止时间到期不引入新的结局类型，只是按
  (截止时刻, 创建序) 批量发出取消请求；shield、传播、异常归并语义
  与手工 `cancel` 完全一致，到期处理集中在 `advance_time` 单点。
- **逻辑时钟而非真实时间**：所有演示与测试用 `advance_time` 显式推进，
  不 sleep、不创建真实线程，结果完全确定、可重放。
- **定时条目惰性失效**：更新/清除/完成时只递增代次，堆中旧条目在
  `advance_time`/`next_deadline` 触碰时剔除，避免原地删堆元素。
- **取消沿子树传播**：`cancel(t)`、`close_scope(t)` 与 `t` 到期都作用
  于整个未结束子树，保证关闭总能取得进展；shield 按任务逐个拦截。
- **异常组扁平按创建序**：父 id 恒小于子 id，直接按 id 序归并即满足
  "父先于子"；不嵌套子组，语义更简单、可预测。
- **finish 冲突报错、跨通道晚到忽略**：同通道重复完成且结果不一致几乎
  一定是 bug；而异步中"任务已退出后迟到的完成"是常态，记 notes 忽略。

## 已知限制

- 纯同步模型：逻辑时钟只由 `advance_time` 显式推进，不模拟真实时间、
  定时器或真正的并发交错；事件顺序完全由调用方决定。
- 截止时间在 `set_deadline` 时不会立即触发（即使等于当前 `now`），
  只在下一次 `advance_time` 时统一处理。
- 单次 `advance_time` 的到期处理是批量的：同一批到期的取消请求按
  (截止时刻, 创建序) 依次发出，但中途不交错调用方的业务事件。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
