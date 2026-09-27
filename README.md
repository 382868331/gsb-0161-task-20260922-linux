# tasktree — 结构化任务树的取消、截止时间与异常归并

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

## 接口

| 方法 | 语义 |
|---|---|
| `spawn(parent=0, name=None) -> Receipt` | 在 open scope 下创建任务；scope closing/closed 或属主任务已记录结局时拒绝（`ScopeClosedError`） |
| `finish(task_id, result=None) -> Receipt` | 记录成功结局；幂等（同结果忽略），不同结果抛 `FinishConflictError`；对已 failed/cancelled 任务的晚到 finish 忽略并记入 notes |
| `fail(task_id, exc) -> Receipt` | 记录业务异常结局；对已结局任务的晚到 fail 忽略并记入 notes |
| `cancel(task_id) -> Receipt` | 请求取消该任务及其整个子树（仅请求，任务仍需 finish/fail 退出） |
| `enter_shield(task_id)` / `leave_shield(task_id)` | 屏蔽嵌套；shield 内取消只记 pending，最外层离开（深度归 0）才投递 |
| `close_scope(task_id) -> Receipt` | 关闭子 scope：请求取消全部未结束后代并等待子树退出；幂等 |
| `advance_time(new_now) -> Receipt` | 显式推进逻辑时钟（单调、不倒退）；对 deadline 已到且未完成的任务按创建序发起取消（仅请求） |
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/children） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` / `now` | 查询辅助 |

## 截止时间

- 时间是纯逻辑时钟：构造时 `now=` 指定起点（默认 0），只能由调用方用
  `advance_time(new_now)` 显式、单调推进；库不使用真实等待，也不启动
  任何后台计时器或线程。
- `spawn(..., deadline=T)` 设定任务截止时刻；`TaskRuntime(deadline=T)`
  设定根 scope 期限。子任务可以有更早的期限，但不得越过父 scope 的
  期限（父有期限时，子必须设期限且 ≤ 父期限），违反即 `ValueError`。
- `advance_time(T)` 时，所有 `deadline <= T` 且尚无结局的任务被发起
  取消（shield 内记 pending）；根期限到期 = 请求取消全部未结束后代。
  到期取消同样只是请求：任务保持 active，仍需 finish/fail 显式退出，
  不会被直接标成完成；每个期限只触发一次。
- 同一时刻的固定处理顺序：调用方在当前时刻发出的显式 finish/fail
  事件总是先于随后 `advance_time` 触发的到期扫描；同一次推进内多个
  任务到期时按任务创建序（id 序）依次投递取消。
- 时刻与期限拒绝 bool（`TypeError`）与 NaN/Infinity（`ValueError`）；
  期限不得早于当前 `now`，时间不得倒退。

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
- **期限沿树向下收紧**：子期限 ≤ 父期限，因此父到期时后代必然同时或
  更早到期，到期取消无需再沿子树传播，扫描顺序完全确定。
- **finish 冲突报错、跨通道晚到忽略**：同通道重复完成且结果不一致几乎
  一定是 bug；而异步中"任务已退出后迟到的完成"是常态，记 notes 忽略。

## 已知限制

- 纯同步模型：时钟由调用方显式推进，不模拟真实等待、定时器或真正的
  并发交错；事件顺序完全由调用方决定。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
