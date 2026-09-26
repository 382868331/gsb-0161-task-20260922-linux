"""tasktree — 结构化任务树的取消、异常归并与具名容量池（纯事件驱动）。

模型
----
* 每个任务是一个节点；节点 0 是根（root），只有 scope 没有任务体。
* 每个任务同时是其子任务的 scope。任务树 = 根 scope 嵌套所有后代。
* 所有状态变化由显式有序的方法调用（事件）驱动；每个事件返回 Receipt，
  内含该事件后目标任务状态与整棵树"待完成原因"。

生命周期
--------
任务:  active --(finish/fail 记录 outcome)--> draining --(后代全部退出)--> exited
       active 且取消已投递时 finish -> outcome 记为 cancelled（取消优先于业务结果）；
       cancel 已投递时 fail(exc)   -> outcome 记为 failed（业务异常优先于取消）。
scope: open --(close_scope)--> closing --(子任务全部退出)--> closed。

取消语义
--------
* cancel(t) 请求取消 t 及其整个子树；close_scope(t) 关闭 t 的子 scope 并
  请求取消全部未结束后代，随后等待子树退出才 closed。
* 任务处于 shield（shield_depth > 0）时，取消只记为 pending；
  最外层 leave_shield（深度回到 0）时才投递。
* 取消本身不是业务异常：cancelled 不进入 exception_group。

异常归并
--------
exception_group(t) 按任务创建序（父先于子，id 即创建序）聚合子树内全部
failed 任务的业务异常，返回 ExceptionGroup；无则返回 None。

容量池
------
* create_pool(name, capacity) 创建具名容量池；容量创建后固定，无管理界面。
* acquire(task_id, pool, amount) 申请许可：池空闲足够且队列为空时立即授予，
  否则排入严格 FIFO 队列；队首暂时拿不到时，后面的轻量请求不得插队；
  申请量超过池总容量立即拒绝（ResourceLimitError），不入队。
* release(task_id, pool, amount=None) 显式归还；许可归持有任务所有，
  只能由持有任务自己归还。
* 任务记录结局（finish/fail，含取消投递后的收尾）时自动归还其全部许可，
  且只归还一次；还在排队的任务被取消投递（或记录结局）时移出队列，
  不会迟到获授。shield 只延迟取消交付，不造成重复释放。
* 守恒不变量：任意事件后 sum(holders) + free == capacity。
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional

MAX_TASKS = 10_000   # 单棵运行时允许的最大任务数（含根）
MAX_DEPTH = 128      # 任务树最大深度（根深度为 0）
MAX_NAME_LEN = 64    # 任务名最大字符数

__all__ = [
    "MAX_TASKS", "MAX_DEPTH", "MAX_NAME_LEN",
    "TaskTreeError", "UnknownTaskError", "ScopeClosedError",
    "FinishConflictError", "ShieldError", "TaskStateError",
    "ResourceLimitError", "TaskCancelled",
    "PoolError", "UnknownPoolError",
    "Receipt", "TaskRuntime",
]


# ---------------------------------------------------------------- 错误类型

class TaskTreeError(Exception):
    """所有任务树语义错误的基类。"""


class UnknownTaskError(TaskTreeError, LookupError):
    """引用了不存在的 task_id。"""


class ScopeClosedError(TaskTreeError):
    """向已 closing/closed 的 scope 发起 spawn。"""


class FinishConflictError(TaskTreeError):
    """对已完成任务重复 finish 且结果不一致。"""


class ShieldError(TaskTreeError):
    """shield 配对错误（如 leave 多于 enter）。"""


class TaskStateError(TaskTreeError):
    """当前任务状态不允许该操作（如对 root 的任务体操作）。"""


class ResourceLimitError(TaskTreeError):
    """超出任务数/深度等接口上限。"""


class TaskCancelled(Exception):
    """取消标记：取消不是业务异常。fail() 拒绝该类的实例。"""


class PoolError(TaskTreeError):
    """容量池语义错误（重复创建、非法申请/归还等）的基类。"""


class UnknownPoolError(PoolError, LookupError):
    """引用了不存在的容量池名。"""


# ---------------------------------------------------------------- 数据结构

@dataclass
class _Task:
    id: int
    parent: Optional[int]
    name: str
    depth: int
    children: list[int] = field(default_factory=list)
    state: str = "active"              # active | draining | exited
    outcome_kind: Optional[str] = None  # finished | failed | cancelled
    outcome_value: Any = None
    shield_depth: int = 0
    pending_cancels: int = 0
    cancel_delivered: bool = False
    scope_state: str = "open"          # open | closing | closed


@dataclass
class _Pool:
    name: str
    capacity: int
    holders: dict[int, int] = field(default_factory=dict)  # task_id -> 持有量
    queue: deque = field(default_factory=deque)            # (task_id, amount)

    @property
    def free(self) -> int:
        return self.capacity - sum(self.holders.values())


@dataclass(frozen=True)
class Receipt:
    """单个事件的处理回执。

    seq         事件序号（从 1 开始，全局有序）
    op          操作名
    task_id     目标任务/scope 属主 id
    ok          事件是否被接受（语义拒绝会抛异常而不是返回 ok=False）
    notes       该事件产生的说明（含 pending/late/idempotent 等细节）
    state       事件后目标任务状态（root 为 None）
    scope_state 事件后目标 scope 状态
    pending     事件后整棵树的待完成原因（pending_reasons(0)）
    new_task_id 仅 spawn：新任务 id
    """
    seq: int
    op: str
    task_id: Optional[int]
    ok: bool
    notes: tuple[str, ...]
    state: Optional[str]
    scope_state: Optional[str]
    pending: tuple[str, ...]
    new_task_id: Optional[int] = None


# ---------------------------------------------------------------- 校验

def _require_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(
            f"{name} must be an int, got {type(value).__name__}: {value!r}")
    return value


def _check_result(value: Any) -> None:
    # 数值结果拒绝 NaN/Infinity；不做任何静默纠正。
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"result must be a finite number, got {value!r}")
    if isinstance(value, complex) and not (
            math.isfinite(value.real) and math.isfinite(value.imag)):
        raise ValueError(f"result must be finite, got {value!r}")


# ---------------------------------------------------------------- 运行时

class TaskRuntime:
    """一棵合成任务树。所有操作同步、有序、无线程。"""

    def __init__(self, *, max_tasks: int = MAX_TASKS, max_depth: int = MAX_DEPTH):
        _require_int(max_tasks, "max_tasks")
        _require_int(max_depth, "max_depth")
        if max_tasks < 1:
            raise ValueError(f"max_tasks must be >= 1, got {max_tasks}")
        if max_depth < 1:
            raise ValueError(f"max_depth must be >= 1, got {max_depth}")
        self.max_tasks = max_tasks
        self.max_depth = max_depth
        self._tasks: dict[int, _Task] = {
            0: _Task(id=0, parent=None, name="<root>", depth=0)}
        self._next_id = 1
        self._seq = 0
        self.log: list[Receipt] = []
        self._pools: dict[str, _Pool] = {}

    # ---------------------------------------------------------- 内部工具

    def _get(self, task_id: Any) -> _Task:
        _require_int(task_id, "task_id")
        try:
            return self._tasks[task_id]
        except KeyError:
            raise UnknownTaskError(f"unknown task_id: {task_id}") from None

    def _subtree(self, task_id: int) -> list[_Task]:
        """按创建序（父先于子）返回 task_id 及其全部后代。"""
        out: list[_Task] = []

        def visit(n: _Task) -> None:
            out.append(n)
            for c in n.children:
                visit(self._tasks[c])

        visit(self._tasks[task_id])
        return out

    def _reap(self) -> None:
        """不动点收敛：outcome 已记录且后代全退出 -> exited；
        closing 且子任务全退出 -> closed。"""
        changed = True
        while changed:
            changed = False
            for t in self._tasks.values():
                if (t.id != 0 and t.state != "exited"
                        and t.outcome_kind is not None
                        and all(self._tasks[c].state == "exited"
                                for c in t.children)):
                    t.state = "exited"
                    changed = True
                if (t.scope_state == "closing"
                        and all(self._tasks[c].state == "exited"
                                for c in t.children)):
                    t.scope_state = "closed"
                    changed = True

    def _request_cancel(self, node: _Task, notes: list[str]) -> None:
        if node.shield_depth > 0:
            node.pending_cancels += 1
            notes.append(
                f"task#{node.id} shielded (depth {node.shield_depth}); "
                f"cancellation recorded as pending ({node.pending_cancels})")
        elif node.cancel_delivered:
            notes.append(f"task#{node.id}: cancellation already delivered")
        else:
            node.cancel_delivered = True
            notes.append(f"cancellation delivered to task#{node.id}")
            self._dequeue_task(node.id, notes, "cancellation delivered")

    def _cancel_nodes(self, nodes: list[_Task], notes: list[str]) -> None:
        for n in nodes:
            if n.outcome_kind is not None:
                continue  # 已有结局的任务不再接受取消
            self._request_cancel(n, notes)

    def _receipt(self, op: str, task_id: Optional[int],
                 notes: list[str], new_task_id: Optional[int] = None) -> Receipt:
        self._seq += 1
        t = self._tasks.get(task_id) if task_id is not None else None
        r = Receipt(
            seq=self._seq, op=op, task_id=task_id, ok=True,
            notes=tuple(notes),
            state=(t.state if (t is not None and t.id != 0) else None),
            scope_state=(t.scope_state if t is not None else None),
            pending=tuple(self.pending_reasons(0)),
            new_task_id=new_task_id,
        )
        self.log.append(r)
        return r

    # ---------------------------------------------------------- 容量池内部

    def _get_pool(self, pool: Any) -> _Pool:
        if not isinstance(pool, str):
            raise TypeError(
                f"pool must be a str, got {type(pool).__name__}: {pool!r}")
        try:
            return self._pools[pool]
        except KeyError:
            raise UnknownPoolError(f"unknown pool: {pool!r}") from None

    @staticmethod
    def _require_amount(amount: Any) -> int:
        _require_int(amount, "amount")
        if amount < 1:
            raise ValueError(f"amount must be >= 1, got {amount}")
        return amount

    def _try_grant(self, p: _Pool, notes: list[str]) -> None:
        """严格 FIFO：只从队首起依次授予；队首拿不到即停，不许插队。"""
        while p.queue:
            task_id, amount = p.queue[0]
            if p.free < amount:
                return
            p.queue.popleft()
            p.holders[task_id] = p.holders.get(task_id, 0) + amount
            notes.append(
                f"granted {amount} permit(s) of pool '{p.name}' to "
                f"task#{task_id} from queue head (free now {p.free})")

    def _dequeue_task(self, task_id: int, notes: list[str],
                      reason: str) -> None:
        """把任务从所有池的等待队列移除（取消投递/记录结局时调用）。"""
        for p in self._pools.values():
            kept = [(tid, amt) for tid, amt in p.queue if tid != task_id]
            if len(kept) != len(p.queue):
                p.queue = deque(kept)
                notes.append(
                    f"task#{task_id} removed from queue of pool '{p.name}' "
                    f"({reason}); it can never be granted late")
                self._try_grant(p, notes)

    def _release_permits_on_outcome(self, task: _Task,
                                    notes: list[str]) -> None:
        """任务记录结局时归还其全部许可并移出所有等待队列。

        只在 outcome_kind 从 None 迁移的唯一位置调用，因此只释放一次。
        """
        self._dequeue_task(task.id, notes, "outcome recorded")
        for p in self._pools.values():
            held = p.holders.pop(task.id, 0)
            if held:
                notes.append(
                    f"auto-released {held} permit(s) of pool '{p.name}' "
                    f"on exit of task#{task.id} (free now {p.free})")
                self._try_grant(p, notes)

    # ---------------------------------------------------------- 事件：spawn

    def spawn(self, parent: int = 0, name: Optional[str] = None) -> Receipt:
        p = self._get(parent)
        if p.scope_state != "open":
            raise ScopeClosedError(
                f"scope of task#{p.id} is {p.scope_state}; "
                f"spawn into a closing/closed scope is forbidden")
        if p.id != 0 and p.outcome_kind is not None:
            raise ScopeClosedError(
                f"task#{p.id} already recorded outcome "
                f"({p.outcome_kind}); its scope no longer accepts spawn")
        if name is None:
            name = f"task-{self._next_id}"
        elif not isinstance(name, str):
            raise TypeError(
                f"name must be a str or None, got {type(name).__name__}: {name!r}")
        elif len(name) > MAX_NAME_LEN:
            raise ValueError(
                f"name length {len(name)} exceeds MAX_NAME_LEN={MAX_NAME_LEN}")
        if len(self._tasks) >= self.max_tasks:
            raise ResourceLimitError(
                f"task limit reached: max_tasks={self.max_tasks}")
        depth = p.depth + 1
        if depth > self.max_depth:
            raise ResourceLimitError(
                f"depth limit exceeded: max_depth={self.max_depth}")
        tid = self._next_id
        self._next_id += 1
        self._tasks[tid] = _Task(id=tid, parent=p.id, name=name, depth=depth)
        p.children.append(tid)
        return self._receipt(
            "spawn", tid,
            [f"spawned task#{tid} '{name}' under task#{p.id}"],
            new_task_id=tid)

    # ---------------------------------------------------------- 事件：finish

    def finish(self, task_id: int, result: Any = None) -> Receipt:
        t = self._get(task_id)
        _check_result(result)
        if t.id == 0:
            raise TaskStateError(
                "root has no task body; use close_scope(0) to close the tree")
        if t.outcome_kind is not None:
            if t.outcome_kind == "finished":
                try:
                    same = bool(t.outcome_value == result)
                except Exception:
                    same = False
                if same:
                    return self._receipt(
                        "finish", t.id,
                        ["idempotent finish: identical result already "
                         "recorded; ignored"])
                raise FinishConflictError(
                    f"task#{t.id} already finished with "
                    f"{t.outcome_value!r}; conflicting result {result!r}")
            return self._receipt(
                "finish", t.id,
                [f"late finish ignored: task#{t.id} already ended as "
                 f"{t.outcome_kind}"])
        if t.cancel_delivered:
            t.outcome_kind = "cancelled"
            t.outcome_value = None
            notes = [f"cancellation had been delivered to task#{t.id}; "
                     f"result {result!r} superseded by cancellation"]
        else:
            t.outcome_kind = "finished"
            t.outcome_value = result
            notes = [f"task#{t.id} finished with {result!r}"]
        t.state = "draining"
        self._release_permits_on_outcome(t, notes)
        self._reap()
        return self._receipt("finish", t.id, notes)

    # ---------------------------------------------------------- 事件：fail

    def fail(self, task_id: int, exc: BaseException) -> Receipt:
        t = self._get(task_id)
        if not isinstance(exc, BaseException):
            raise TypeError(
                f"exc must be an exception instance, got "
                f"{type(exc).__name__}: {exc!r}")
        if not isinstance(exc, Exception):
            raise ValueError(
                f"exc must be an Exception, not a BaseException such as "
                f"{type(exc).__name__} (KeyboardInterrupt/SystemExit)")
        if isinstance(exc, TaskCancelled):
            raise ValueError(
                "TaskCancelled is not a business error; request cancellation "
                "via cancel()/close_scope() instead of fail()")
        if t.id == 0:
            raise TaskStateError("root has no task body; cannot fail root")
        if t.outcome_kind is not None:
            return self._receipt(
                "fail", t.id,
                [f"late failure ignored: task#{t.id} already ended as "
                 f"{t.outcome_kind}"])
        t.outcome_kind = "failed"
        t.outcome_value = exc
        notes = [f"task#{t.id} failed with {type(exc).__name__}: {exc}"]
        if t.cancel_delivered:
            notes.append("cancellation had been delivered; "
                         "business failure takes precedence")
        t.state = "draining"
        self._release_permits_on_outcome(t, notes)
        self._reap()
        return self._receipt("fail", t.id, notes)

    # ---------------------------------------------------------- 事件：cancel

    def cancel(self, task_id: int) -> Receipt:
        t = self._get(task_id)
        if t.id == 0:
            raise TaskStateError(
                "cannot cancel the root task body; "
                "use close_scope(0) to cancel the whole tree")
        notes: list[str] = []
        self._cancel_nodes(self._subtree(t.id), notes)
        if not notes:
            notes.append(
                f"task#{t.id} and its subtree already have recorded "
                f"outcomes; nothing to cancel")
        self._reap()
        return self._receipt("cancel", t.id, notes)

    # ------------------------------------------------- 事件：shield 进出

    def enter_shield(self, task_id: int) -> Receipt:
        t = self._get(task_id)
        if t.id == 0:
            raise TaskStateError("root has no task body; cannot shield root")
        if t.outcome_kind is not None:
            raise TaskStateError(
                f"task#{t.id} already ended as {t.outcome_kind}; "
                f"cannot enter_shield")
        t.shield_depth += 1
        return self._receipt(
            "enter_shield", t.id,
            [f"task#{t.id} shield depth now {t.shield_depth}"])

    def leave_shield(self, task_id: int) -> Receipt:
        t = self._get(task_id)
        if t.id == 0:
            raise TaskStateError("root has no task body; cannot shield root")
        if t.shield_depth == 0:
            raise ShieldError(
                f"task#{t.id}: leave_shield without matching enter_shield")
        t.shield_depth -= 1
        notes = [f"task#{t.id} shield depth now {t.shield_depth}"]
        if t.shield_depth == 0 and t.pending_cancels:
            count = t.pending_cancels
            t.pending_cancels = 0
            if t.outcome_kind is None:
                t.cancel_delivered = True
                notes.append(
                    f"outermost shield left; {count} pending "
                    f"cancellation(s) delivered to task#{t.id}")
                self._dequeue_task(t.id, notes, "cancellation delivered")
            else:
                notes.append(
                    f"outermost shield left; {count} pending "
                    f"cancellation(s) discarded: task#{t.id} already ended "
                    f"as {t.outcome_kind}")
        self._reap()
        return self._receipt("leave_shield", t.id, notes)

    # ------------------------------------------------- 事件：close_scope

    def close_scope(self, task_id: int) -> Receipt:
        t = self._get(task_id)
        if t.scope_state != "open":
            notes = [f"scope of task#{t.id} already {t.scope_state}; "
                     f"close_scope is idempotent"]
        else:
            t.scope_state = "closing"
            notes = [f"scope of task#{t.id} closing: requesting "
                     f"cancellation of all unfinished descendants"]
            before = len(notes)
            self._cancel_nodes(self._subtree(t.id)[1:], notes)
            if len(notes) == before:
                notes.append("no unfinished descendants")
        self._reap()
        if t.scope_state == "closed":
            notes.append(f"scope of task#{t.id} closed")
        return self._receipt("close_scope", t.id, notes)

    # ---------------------------------------------------------- 查询
    # ------------------------------------------------- 事件：容量池

    def create_pool(self, name: str, capacity: int) -> Receipt:
        """创建具名容量池；容量固定，本题不提供管理界面。"""
        if not isinstance(name, str):
            raise TypeError(
                f"name must be a str, got {type(name).__name__}: {name!r}")
        if not name:
            raise ValueError("pool name must be non-empty")
        if len(name) > MAX_NAME_LEN:
            raise ValueError(
                f"pool name length {len(name)} exceeds "
                f"MAX_NAME_LEN={MAX_NAME_LEN}")
        _require_int(capacity, "capacity")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if name in self._pools:
            raise PoolError(f"pool {name!r} already exists")
        self._pools[name] = _Pool(name=name, capacity=capacity)
        return self._receipt(
            "create_pool", None,
            [f"created pool '{name}' with capacity {capacity}"])

    def acquire(self, task_id: int, pool: str, amount: int = 1) -> Receipt:
        """申请许可：立即授予或排入严格 FIFO 队列；超容量立即拒绝。"""
        t = self._get(task_id)
        p = self._get_pool(pool)
        self._require_amount(amount)
        if t.id == 0:
            raise TaskStateError("root has no task body; cannot acquire")
        if t.outcome_kind is not None:
            raise TaskStateError(
                f"task#{t.id} already ended as {t.outcome_kind}; "
                f"cannot acquire")
        if t.cancel_delivered:
            raise TaskStateError(
                f"cancellation already delivered to task#{t.id}; "
                f"it must exit, not acquire")
        if amount > p.capacity:
            raise ResourceLimitError(
                f"amount {amount} exceeds capacity {p.capacity} of pool "
                f"'{p.name}'; rejected immediately, not queued")
        if any(tid == t.id for tid, _ in p.queue):
            raise PoolError(
                f"task#{t.id} already has a queued request in pool "
                f"'{p.name}'")
        notes: list[str] = []
        if not p.queue and p.free >= amount:
            p.holders[t.id] = p.holders.get(t.id, 0) + amount
            notes.append(
                f"granted {amount} permit(s) of pool '{p.name}' to "
                f"task#{t.id} immediately (free now {p.free})")
        else:
            p.queue.append((t.id, amount))
            pos = len(p.queue)
            notes.append(
                f"task#{t.id} queued for {amount} permit(s) of pool "
                f"'{p.name}' at position {pos} (strict FIFO, free "
                f"{p.free})")
        return self._receipt("acquire", t.id, notes)

    def release(self, task_id: int, pool: str,
                amount: Optional[int] = None) -> Receipt:
        """持有任务显式归还许可；amount=None 表示归还在该池的全部持有。"""
        t = self._get(task_id)
        p = self._get_pool(pool)
        held = p.holders.get(t.id, 0)
        if held == 0:
            raise PoolError(
                f"task#{t.id} holds no permits of pool '{p.name}'; "
                f"permits are owned by their holder and cannot be "
                f"released twice")
        if amount is None:
            amount = held
        self._require_amount(amount)
        if amount > held:
            raise ValueError(
                f"task#{t.id} holds only {held} permit(s) of pool "
                f"'{p.name}'; cannot release {amount}")
        left = held - amount
        if left:
            p.holders[t.id] = left
        else:
            del p.holders[t.id]
        notes = [f"task#{t.id} released {amount} permit(s) of pool "
                 f"'{p.name}' (free now {p.free})"]
        self._try_grant(p, notes)
        return self._receipt("release", t.id, notes)

    def pool_status(self, pool: Optional[str] = None) -> dict:
        """池快照：capacity/free/holders/queue；pool=None 时返回全部池。"""
        def snap(p: _Pool) -> dict:
            return {
                "capacity": p.capacity,
                "free": p.free,
                "holders": dict(sorted(p.holders.items())),
                "queue": [(tid, amt) for tid, amt in p.queue],
            }
        if pool is not None:
            return {pool: snap(self._get_pool(pool))}
        return {name: snap(p) for name, p in self._pools.items()}

    def status(self) -> dict[int, dict[str, Any]]:
        """整棵树快照（按任务创建序）。"""
        out: dict[int, dict[str, Any]] = {}
        for tid, t in sorted(self._tasks.items()):
            out[tid] = {
                "name": t.name,
                "parent": t.parent,
                "state": t.state,
                "outcome": t.outcome_kind,
                "value": (t.outcome_value
                          if t.outcome_kind == "finished" else None),
                "error": (repr(t.outcome_value)
                          if t.outcome_kind == "failed" else None),
                "shield_depth": t.shield_depth,
                "pending_cancels": t.pending_cancels,
                "cancel_delivered": t.cancel_delivered,
                "scope": t.scope_state,
                "children": list(t.children),
            }
        return out

    def pending_reasons(self, task_id: int = 0) -> list[str]:
        """子树尚未完成的原因列表；空列表表示该子树已完整退出。"""
        t = self._get(task_id)
        reasons: list[str] = []
        for n in self._subtree(t.id):
            if n.id != 0:
                if n.outcome_kind is None:
                    if n.cancel_delivered:
                        reasons.append(
                            f"task#{n.id} '{n.name}': cancellation "
                            f"delivered, awaiting exit (finish/fail)")
                    elif n.pending_cancels:
                        reasons.append(
                            f"task#{n.id} '{n.name}': active, shielded "
                            f"(depth {n.shield_depth}) with "
                            f"{n.pending_cancels} pending cancellation(s)")
                    else:
                        reasons.append(
                            f"task#{n.id} '{n.name}': active")
                elif n.state != "exited":
                    waiting = sum(
                        1 for d in self._subtree(n.id)[1:]
                        if d.state != "exited")
                    reasons.append(
                        f"task#{n.id} '{n.name}': outcome "
                        f"{n.outcome_kind} recorded, waiting on "
                        f"{waiting} descendant(s)")
            if n.scope_state == "closing":
                open_children = [c for c in n.children
                                 if self._tasks[c].state != "exited"]
                if open_children:
                    reasons.append(
                        f"scope of task#{n.id}: closing, waiting on "
                        f"child task(s) {open_children}")
        subtree_ids = {n.id for n in self._subtree(t.id)}
        for p in self._pools.values():
            for pos, (tid, amt) in enumerate(p.queue, start=1):
                if tid in subtree_ids:
                    reasons.append(
                        f"task#{tid}: queued for {amt} permit(s) of pool "
                        f"'{p.name}' at position {pos}")
        return reasons

    def exception_group(self, task_id: int = 0) -> Optional[ExceptionGroup]:
        """按任务创建序归并子树内全部业务异常；无则 None。

        cancelled 结局不是业务异常，不进入异常组。
        """
        self._get(task_id)
        # 按创建序（id 序）归并；父 id 恒小于子 id，故父先于子。
        errors = [n.outcome_value
                  for n in sorted(self._subtree(task_id),
                                  key=lambda n: n.id)
                  if n.outcome_kind == "failed"]
        if not errors:
            return None
        return ExceptionGroup(
            f"unhandled business errors in subtree of task#{task_id}",
            errors)

    def is_closed(self, task_id: int = 0) -> bool:
        return self._get(task_id).scope_state == "closed"

    def outcome(self, task_id: int) -> tuple[Optional[str], Any]:
        t = self._get(task_id)
        return t.outcome_kind, t.outcome_value
