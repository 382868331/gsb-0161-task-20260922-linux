# tasktree — 结构化任务树的取消与异常归并

离线、纯事件驱动的合成任务树库：不启线程、无事件循环，所有状态变化由
显式有序的方法调用（事件）触发，每个事件返回包含状态与待完成原因的
`Receipt`。本版新增具名容量池，限制同时占用某种资源的子任务数量。

## 环境

- Python 3.14.7（使用了标准库 `ExceptionGroup`，需 ≥ 3.11）
- 仅标准库，无需安装任何第三方依赖；核心、测试、演示全部离线

## 运行

```bash
python -m unittest discover -s tests -v   # 单元测试 + 合法轨迹枚举验收
python demo.py                            # 正常结果 + 异常组/等待链 + 拒绝边界
```

venv 准备（Python 3.14.7，无第三方依赖，无需 pip install）：

```bash
python3.14 -m venv .venv
source .venv/bin/activate
```

容量池演示（现场计算，无 sleep，远小于 90 秒）：

```bash
python demo_iteration.py                 # FIFO 排队/取消归还/守恒核对/拒绝边界
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
| `status() -> dict` | 整树快照（state/outcome/shield/pending/scope/children） |
| `pending_reasons(task_id=0) -> list[str]` | 子树待完成原因（等待链）；空列表 = 已完整退出 |
| `exception_group(task_id=0) -> ExceptionGroup \| None` | 按任务创建序（父先于子）归并子树内全部业务异常；cancelled 不入组 |
| `is_closed(task_id=0)` / `outcome(task_id)` | 查询辅助 |
| `create_pool(name, capacity) -> Receipt` | 创建具名容量池；容量固定，无管理界面；重名抛 `PoolError` |
| `acquire(task_id, pool, amount=1) -> Receipt` | 申请许可：空闲且队空立即授予，否则严格 FIFO 排队；`amount > capacity` 立即抛 `ResourceLimitError`，不入队 |
| `release(task_id, pool, amount=None) -> Receipt` | 持有任务归还许可（默认全部）；归还后从队首起依次授予，队首不满足即停 |
| `pool_status(pool=None) -> dict` | 池快照：`capacity`/`free`/`holders`/`queue` |

取消与结局的优先级：

- 取消已投递后 `finish` → 结局 `cancelled`（结果被取消取代）；
- 取消已投递后 `fail(exc)` → 结局 `failed`（业务异常优先于取消）；
- shield 内取消未投递时 `finish` → 正常 `finished`，离开时 pending 取消被丢弃。

容量池语义：

- 等待严格 FIFO：队首暂时拿不到时，后面的轻量请求不得插队。
- 许可归持有任务所有，只能由持有者 `release`；任务记录结局
  （finish/fail，含取消投递后的收尾与收尾抛错）时自动归还其全部许可，
  且只归还一次（幂等 finish / 晚到 fail 不会重复释放）。
- 还在排队的任务被取消投递（或记录结局）时立即移出队列，不会迟到获授；
  父任务取消整棵子树时，每个后代的排队请求同样出队。
- shield 只延迟取消交付：shield 内的排队任务暂不出队（甚至可能先获授），
  最外层离开交付取消时才出队；已发出的释放不会因此重复执行。
- 守恒不变量：任意事件后 `free + Σ holders == capacity`，演示逐步核对。

## 错误语义

- 非法输入即时抛出、可定位（消息含参数名与值），不静默纠正：
  - 整数参数拒绝 `bool`（`TypeError`）；
  - 数值结果拒绝 NaN/Infinity（`ValueError`，检查 `float`/`complex`）；
  - `fail` 只接受 `Exception` 实例；`TaskCancelled` 实例被拒绝
    （取消不是业务异常，必须用 `cancel`/`close_scope` 表达）；
  - `UnknownTaskError`、`ScopeClosedError`、`FinishConflictError`、
    `ShieldError`（leave 多于 enter）、`TaskStateError`（如对 root 的
    任务体操作）、`ResourceLimitError`（超上限/超池容量）、`PoolError`
    （池语义错误：重复创建、重复排队、无持有归还）、`UnknownPoolError`
    （未知池名）均为 `TaskTreeError` 子类。
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

## 已知限制

- 纯同步模型：不模拟时间、定时器或真正的并发交错；事件顺序完全由
  调用方决定。
- `finish` 幂等比较用 `==`，对自定义不可比值按冲突处理。
- 结果值若为容器，其内部的 NaN 不做深检查（只校验顶层 float/complex）。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
- 不宣称兼容 Trio/asyncio 等任何现行标准的完整语义，仅实现本契约子集。
- 容量池：同一任务在同一池只允许一个排队请求（重复排队抛 `PoolError`）；
  已记录结局或已交付取消的任务不得再 `acquire`；池容量创建后固定，
  不提供扩容/缩容或任何管理界面；池不随 scope 关闭而销毁。
