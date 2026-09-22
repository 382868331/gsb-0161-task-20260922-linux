# tasktree — 结构化任务树的取消与异常归并

离线、纯事件驱动的合成任务树库。所有状态变化由显式有序操作触发，**不启线程、
不做 I/O、不依赖网络**。仅使用 Python 标准库。

- Python 版本：**3.14.7**（使用了内置 `ExceptionGroup`；需 ≥ 3.11 语法）
- 依赖：**无**（`requirements.txt` 为空约定，仅声明标准库）
- 运行环境：Linux 原生，离线

## 命令

```bash
python -m unittest discover -s tests -v   # 运行全部测试
python demo.py                            # 固定输入演示（正常结果 + 拒绝边界，< 1 秒）
```

## 接口

```python
from tasktree import TaskTree

tree = TaskTree()                 # 自动创建根 scope，id = 0
a = tree.spawn(0, "worker")       # 在 scope 0 内创建任务，返回新任务 id
tree.finish(a, result=42)         # 正常完成（幂等；不同结果冲突报错）
tree.fail(a, ValueError("boom"))  # 业务异常失败；请求取消其未结束子任务
tree.cancel(a, "reason")          # 请求取消任务及其未结束子树
tree.enter_shield(a)              # 进入取消屏蔽（可嵌套）
tree.leave_shield(a)              # 离开屏蔽；最外层离开时投递 pending 取消
tree.close_scope(0)               # 关闭 scope：取消全部未结束子任务并等待子树退出
```

每个操作返回一个 `Event`（`seq, op, task, status, note, task_status, waiting`），
其中 `waiting` 是该 scope 的**待完成原因**列表（如 "child #3 not done"、
"cancel pending behind shield"）。`status` 为 `"applied"` 或 `"no-op"`
（合法但无效，如迟到取消、幂等重复 finish）；非法操作**抛出异常**，不产生事件。

只读接口：`status(id)`、`outcome(id)`、`children(id)`、`parent(id)`、
`pending_reasons(id)`、`exception_group(id)`、`events`、`snapshot()`、
`task_ids()`、`is_done(id)`、`render()`。

## 语义契约

- **生命周期**：`active → completing → done`。任务获得结局（finish/fail/cancel/
  close）后进入 `completing`，**等整棵子树退出**才 `done`。不存在"父已完成而
  子仍活跃"的状态（枚举测试验证）。
- **close_scope**：先请求取消全部未结束子任务，等子树退出后 scope 才完成；
  幂等（重复关闭为 no-op）。`fail` 同样请求取消其未结束子任务；`finish`
  **不**取消子任务，只等待。
- **shield**：屏蔽期间取消仅记为 pending，**最外层** `leave_shield` 时才投递；
  嵌套屏蔽逐层计数。无配对的 `leave_shield` 抛 `ShieldError`。
- **异常归并**：scope 完成时，将其子树内全部**业务异常**按**任务创建序**聚合为
  内置 `ExceptionGroup`（`exception_group(id)`，未完成时调用抛 `TaskTreeError`）。
  **取消不是业务异常**，不进入异常组。
- **finish 幂等**：相同结果重复 finish 为 no-op；不同结果（含 `True` vs `1`，
  严格区分 bool 与 int）或不同结局种类（如取消后迟到 finish）抛
  `ResultConflictError`。
- **禁止已关闭 scope 再 spawn**：目标 scope 非 `active` 时 `spawn` 抛
  `ScopeClosedError`。
- **原子性**：每个操作先完成全部参数校验再修改状态；被拒绝的操作不留下任何
  部分变更（测试用 snapshot 逐字节比对验证）。

## 错误语义

| 异常 | 触发 |
|---|---|
| `InvalidInputError` | 参数类型错误、NaN/Infinity 结果、空名字、非异常实例等；消息含操作名与参数 |
| `UnknownTaskError` | 任务 id 不存在 |
| `ScopeClosedError` | 向 closing/closed scope spawn |
| `ResultConflictError` | 与已记录结局冲突的 finish/fail |
| `ShieldError` | 无配对的 leave_shield |
| `TaskTreeError` | 上述一切的基类；也用于"未完成时读取异常组" |

整数参数（任务 id）拒绝 `bool`；数值结果拒绝 NaN/Infinity（float 与 complex）。
本库**不产生浮点输出**，因此无浮点误差口径问题；结果相等性采用严格比较
（`==` 且 bool 不与 int 混同），不存在用宽容差掩盖边界的情形。

## 输入上限

- 任务数、事件数、屏蔽嵌套深度：无硬编码上限，受内存限制；测试与 demo 使用
  ≤ 8 事件的小树。枚举测试的搜索预算为 200,000 个去重状态（实际穷尽 39,149 个，
  `exhausted=True`；预算耗尽与完整探索在统计中明确区分）。
- 任务名/取消原因：非空/字符串类型校验，长度无上限。

## 设计取舍

- **同步事件应用**：每个操作立即、确定性地传播（取消沿子树下推、完成沿父链
  上提），无事件队列、无调度器——可复现、可枚举。
- **fail 会取消子任务**：子树必须退出失败才最终确定，与 close_scope 一致；
  这是契约选择，不宣称与任何既有运行时（asyncio/trio/structured
  concurrency 标准）完全兼容。
- **异常组扁平化**：子树的业务异常按创建序扁平汇入祖先的 `ExceptionGroup`，
  而非嵌套分组——与题面"按任务创建序聚合"一致。
- **迟到取消为 no-op、迟到 finish 为冲突错误**：取消是请求（可安全忽略），
  结局是事实（不可静默覆盖）。

## 已知限制

- 单进程、单线程、内存态；无持久化、无跨树操作。
- 任务一旦 `done` 不可复活；id 不复用。
- `finish` 结果的相等性对自定义对象依赖其 `__eq__`；NaN/Infinity 在入口即拒绝。
- 枚举器只覆盖 ≤ 3 任务、≤ 8 事件的操作字母表，用于验证结构不变量，
  不是对无限轨迹空间的证明。
