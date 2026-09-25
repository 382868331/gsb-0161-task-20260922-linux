# tasktree — 结构化任务树的取消、异常归并与截止时间

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。时间也是显式事件：逻辑时钟只由 `advance_time` 推进，
不 sleep、不创建真实定时器。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
./.venv/bin/python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举
./.venv/bin/python demo.py                            # 正常/异常组/截止时间/拒绝边界
```

测试与演示均可在约 90 秒内完成（实测秒级），不需要 Docker、外部服务或账号。

## 模型

- 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
- 每个任务同时是其子任务的 scope；任务树 = 根 scope 嵌套全部后代。
- 任务生命周期：`active` →（finish/fail 记录结局）→ `draining`
  （等待后代退出）→ `exited`。**父任务只有在全部后代退出后才算完成**。
- scope 生命周期：`open` →（close_scope）→ `closing`（先请求取消全部
  未结束后代，等待子树退出）→ `closed`。
- 时间是独立的显式维度：逻辑时钟初值 0，只由 `advance_time` 非递减
  推进；截止时间在时钟到达时投出取消请求，不改变上述生命周期。

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None) -> Receipt` | 在 open scope 下创建任务；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `set_deadline(task_id, deadline) -> Receipt` | 为任务/root scope 安排截止时刻（严格未来的整数）；重设即作废旧条目 |
| `advance_time(target) -> Receipt` | 逻辑时钟非递减推进；先按 (时刻, 创建序) 触发全部 `deadline <= target`（相等也算），收敛后才返回 |
| `next_deadline() -> DeadlineInfo \| None` | 下一待触发时刻与到期来源，供模拟器逐步驱动 |
| `due_deadlines() -> list[DeadlineInfo]` | 全部有效到期记录，按 (截止时刻, 创建序) 排列；只读 |
| `now` / `Receipt.clock` / `Receipt.fired` | 当前时钟读数；事件后时钟；`advance_time` 本次实际触发的来源列表 |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/deadline/children） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |

## 截止时间与逻辑时钟

- 时钟是非递减整数（构造时可用 `start_time=` 设定初值，默认 0）。
  `advance_time(t)` 把时钟拨到 `t`（`t < now` 抛
  `NonMonotonicTimeError`），然后**一次性**处理堆中所有
  `deadline <= t` 的到期记录：先到时刻者先触发，同一时刻按任务创建序
  （id 升序，root id=0 最前），处理完才返回。因此调用方的下一条业务
  事件看到的总是到期收敛后的状态；时间恰好相等也算到期。
- 到期效果只是**提出取消请求**，等价于在该时刻插入一次
  `cancel`/`close_scope`：
  - 非 root 任务到期 = `cancel(该任务)`，沿原路径传播到整个子树；
    任务在 shield 内时取消仍只记 pending，必须等它离开 shield、以
    finish/fail 退出、子树退出条件满足并经 close_scope 收尾，
    **到期不能绕过任何既有收尾**；
  - root 到期 = `close_scope(0)`，请求关闭根 scope 并等待。
- 父 scope 可以设置自己的截止时间；**子任务不复制父时间**，自己的
  `deadline` 字段保持独立。父到期只是按既有取消路径把请求传播给后代。
- 非 root 任务的截止时刻管的是任务本体取消，与其子 scope 是否
  closing 无关（关闭子 scope 不会作废它）；任务记录结局（finish/fail）
  后其到期记录立即失效；重设截止时间也作废旧条目。堆中旧条目带代号
  （generation），到期时惰性丢弃，**旧定时条目不可能影响同 id 的
  新一代定时**。
- 超时只产生取消，不产生异常：`cancelled` 结局照旧不进入
  `exception_group`，超时不会被当作业务异常混入聚合。
- 模拟器驱动范式：
  ```python
  while (d := rt.next_deadline()) is not None:
      r = rt.advance_time(d.deadline)   # r.fired 给出本次到期来源
      # ... 在 r.clock 时刻插入业务事件（finish/fail/spawn/...）
  ```

取消与结局的优先级：

- 取消已投递后 `finish` → 结局 `cancelled`（结果被取消取代）；
- 取消已投递后 `fail(exc)` → 结局 `failed`（业务异常优先于取消）；
- shield 内取消未投递时 `finish` → 正常 `finished`，离开时 pending 取消被丢弃。

## 错误语义

- 非法输入即时抛出、可定位（消息含参数名与值），不静默纠正：
  - 整数参数拒绝 `bool`（`TypeError`）；
  - 数值结果拒绝 NaN/Infinity（`ValueError`，检查 `float`/`complex`）；
  - `fail` 只接受 `Exception` 实例；`TaskCancelled` 实例被拒绝
    （取消不是业务异常，必须用 `cancel`/`close_scope` 表达）；
  - `UnknownTaskError`、`ScopeClosedError`、`FinishConflictError`、
    `ShieldError`（leave 多于 enter）、`TaskStateError`（如对 root 的
    任务体操作）、`ResourceLimitError`（超上限）均为 `TaskTreeError` 子类。
- 时钟与截止时间：`advance_time` 回拨抛 `NonMonotonicTimeError`
  （同时是 `ValueError`）；`set_deadline` 要求严格未来时刻，
  `deadline <= now` 抛 `ValueError`；向 closing/closed 的 root scope、
  或已记录结局的任务安排截止时间分别抛 `ScopeClosedError` /
  `TaskStateError`。
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
- **到期只是定时的取消请求**：deadline 不引入新的结局类型或强制中断，
  只在指定时刻投出一次与 `cancel`/`close_scope` 同路径的请求，因此
  shield、等待链、异常归并规则全部原样适用，没有"超时专用收尾"分支。
- **每条定时条目带代号（generation）**：重设/失效不立即从堆中删条目，
  触发时按代号惰性丢弃；避免在大堆上做 O(n) 删除，且从结构上杜绝旧
  条目误伤同 id 的新一代定时。
- **root 与普通任务的截止时刻归属不同**：root 没有任务体，其截止时刻
  绑定根 scope（到期即请求关闭）；普通任务的截止时刻绑定任务本体
  （到期即请求取消），不受其子 scope 关闭影响。

## 已知限制

- 纯同步模型：不模拟真正的并发交错；除 `advance_time` 触发的到期外，
  事件顺序完全由调用方决定。时钟是逻辑整数，不与墙钟挂钩。
- 到期处理在一次 `advance_time` 内按固定序全部完成：没有"到期事件之间
  插入业务事件"的半步语义；需要交错时应分多次推进到各截止时刻。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 定时堆中的作废条目只在其时刻到达（`advance_time`）时才被物理丢弃；
  长期只向前小步推进且大量重设时堆会保留作废条目（不影响正确性）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
