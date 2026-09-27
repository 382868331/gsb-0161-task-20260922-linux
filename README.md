# tasktree — 结构化任务树的取消与异常归并

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举验收
python demo.py                            # 正常结果 + 异常组/等待链 + 拒绝边界
```

## 模型

- 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
- 每个任务同时是其子任务的 scope；任务树 = 根 scope 嵌套全部后代。
- 任务生命周期：`active` →（finish/fail 记录结局）→ `draining`
  （等待后代退出）→ `exited`。**父任务只有在全部后代退出后才算完成**。
- scope 生命周期：`open` →（close_scope）→ `closing`（先请求取消全部
  未结束后代，等待子树退出）→ `closed`。

## 时间与截止时间

- 运行时只有**显式逻辑时钟**：构造时 `now=`（默认 0），用 `advance(t)`
  单调推进，`now()` 读取当前时刻。不做真实等待、不启动任何后台计时，
  没有回调或线程。
- 每个 scope（含根 scope）都可有一个整数截止时刻：根用构造参数
  `deadline=`，子 scope 用 `spawn(parent, name, deadline=...)`；缺省
  继承父作用域期限。**子任务可以更早，但不得晚于父作用域期限**
  （违反抛 `DeadlineError`）。
- `advance(T)` 时，所有 `deadline <= T` 且尚未触发的期限按
  **(deadline, 创建 id) 的固定顺序**触发：对仍未记录结局的子树发起
  取消请求（同样尊重 shield：shield 内只记 pending）。
- **到期取消只是请求**：被到期的任务不会被自动标记完成/取消，仍须
  `finish`/`fail` 显式退出；scope 也不会因到期自动关闭，仍需
  `close_scope`（到期只负责投递取消，等待链照常呈现）。
- **完成与到期同一时刻的固定顺序**：以显式事件序为准——若任务在
  `advance` 到达该时刻之前已 `finish`/`fail`，同一时刻的到期取消不再
  覆盖该结局（完成优先于到期）；未记录结局的任务才在本次 `advance`
  收到取消请求。

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None, *, deadline=None) -> Receipt` | 在 open scope 下创建任务；`deadline` 缺省继承父作用域期限，子期限越过父期限或落在过去抛 `DeadlineError`；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `advance(to_time) -> Receipt` | 显式单调推进逻辑时钟；按 (deadline, id) 固定顺序触发到期取消，不做真实等待 |
| `now() -> int` / `deadline_of(task_id)` | 当前逻辑时刻 / scope 截止时刻（无期限为 None） |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/deadline/scope/children） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |

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
    任务体操作）、`ResourceLimitError`（超上限）、`DeadlineError`（子期限
    越过父作用域期限、期限落在过去、时钟回退）均为 `TaskTreeError` 子类。
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
  一定是 bug；而异步中“任务已退出后迟到的完成”是常态，记 notes 忽略。
- **时间是外部输入而非内部机制**：截止时间到期不抢跑任何事件，触发点
  是调用方显式给出的 `advance`，且只产出取消请求；因此“完成与到期同
  一时刻谁优先”有可复现的确定答案（已记录的结局优先），不依赖调度器。
- **期限只收窄不放宽**：子 scope 只能比父 scope 更早到期；缺省即继承，
  保证任意子树的存活时间不超过其全部祖先作用域。

## 已知限制

- 纯同步模型：不做真实等待、无后台定时器或真正的并发交错；时间仅由
  调用方以 `advance` 显式推进，事件顺序完全由调用方决定。
- 截止时刻仅支持非负整数逻辑时间（拒绝 `bool`）；不支持周期定时器、
  修改已设定的期限或 wall-clock 换算。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
