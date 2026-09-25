"""tasktree — 结构化任务树的取消与异常归并（合成、纯事件驱动、不启线程）。

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

逻辑时钟与截止时间
------------------
* 运行时装维护一个非递减的整数逻辑时钟 now（初始 0），只能由
  advance_time(now) 显式推进；时间为非递减整数，倒退被拒绝。
* set_deadline(t, d) 给任务或 scope（含 root）设置截止时刻 d（整数，
  不得小于当前时刻）；子任务不复制父任务的截止时间。
* advance_time(T) 先按 (截止时刻, 任务创建序) 触发全部到期条目
  （deadline <= T，恰好相等也算到期），再返回；调用方的下一条业务事件
  因此在所有到期处理之后才被处理。
* 到期只提出取消请求：等价于对该任务子树执行 cancel 的取消部分
  （root 到期 = 请求取消全部未结束后代），shield 内仍只记 pending，
  scope 的关闭仍必须走 close_scope 的既有等待条件，到期不绕过它。
* 任务记录结局（finish/fail）或其 scope closed 后，到期记录立即失效；
  更新截止时间会使旧定时条目作废（按代次惰性剔除），旧条目不得影响
  同一任务的新一代定时。
* 到期引发的取消仍是取消：cancelled 不进入 exception_group，超时不被
  记成业务异常。
* next_deadline() 返回 (下一到期时刻, task_id) 供模拟器逐步驱动。
"""

from __future__ import annotations

import heapq
import math
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
    deadline: Optional[int] = None     # 截止时刻（逻辑时钟）；None = 无
    deadline_gen: int = 0              # 定时条目代次：更新/失效时递增


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
    now         事件处理后的逻辑时钟时刻
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
    now: int = 0


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
        self._now = 0
        # 定时条目堆：(deadline, task_id, generation)；task_id 即创建序。
        # 失效条目（代次/时刻不匹配）惰性剔除，不原地删除。
        self._timers: list[tuple[int, int, int]] = []
        self.log: list[Receipt] = []

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
                    # scope 完成关闭后，其到期记录一并失效
                    self._invalidate_deadline(t)
                    changed = True

    def _invalidate_deadline(self, t: _Task) -> None:
        """使 t 的到期记录失效；堆中旧条目成为垃圾，由代次检查惰性剔除。"""
        if t.deadline is not None:
            t.deadline = None
            t.deadline_gen += 1

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
            now=self._now,
        )
        self.log.append(r)
        return r

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
        if t.deadline is not None:
            notes.append(f"deadline of task#{t.id} (at {t.deadline}) "
                         f"invalidated: outcome recorded")
            self._invalidate_deadline(t)
        t.state = "draining"
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
        if t.deadline is not None:
            notes.append(f"deadline of task#{t.id} (at {t.deadline}) "
                         f"invalidated: outcome recorded")
            self._invalidate_deadline(t)
        t.state = "draining"
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

    # ------------------------------------------------- 事件：deadline 设置

    def set_deadline(self, task_id: int, deadline: int) -> Receipt:
        """给任务或 scope（含 root）设置/更新截止时刻。

        子任务不复制父任务的截止时间；更新会使旧定时条目作废。
        截止时刻不得小于当前逻辑时钟（过去时刻无意义，拒绝而非静默触发）。
        """
        t = self._get(task_id)
        _require_int(deadline, "deadline")
        if deadline < 0:
            raise ValueError(f"deadline must be >= 0, got {deadline}")
        if deadline < self._now:
            raise ValueError(
                f"deadline {deadline} is in the past (now={self._now})")
        if t.id != 0 and t.outcome_kind is not None:
            raise TaskStateError(
                f"task#{t.id} already ended as {t.outcome_kind}; "
                f"cannot set deadline")
        if t.scope_state == "closed":
            raise TaskStateError(
                f"scope of task#{t.id} is closed; cannot set deadline")
        old = t.deadline
        t.deadline = deadline
        t.deadline_gen += 1
        heapq.heappush(self._timers, (deadline, t.id, t.deadline_gen))
        if old is None:
            notes = [f"deadline of task#{t.id} set to {deadline} "
                     f"(now={self._now})"]
        else:
            notes = [f"deadline of task#{t.id} changed {old} -> {deadline}; "
                     f"previous timer entry invalidated "
                     f"(generation {t.deadline_gen})"]
        return self._receipt("set_deadline", t.id, notes)

    def clear_deadline(self, task_id: int) -> Receipt:
        """清除截止时刻；幂等。堆中旧条目由代次检查惰性剔除。"""
        t = self._get(task_id)
        if t.deadline is None:
            notes = [f"task#{t.id} has no deadline; "
                     f"clear_deadline is idempotent"]
        else:
            old = t.deadline
            self._invalidate_deadline(t)
            notes = [f"deadline of task#{t.id} (at {old}) cleared; "
                     f"timer entry invalidated"]
        return self._receipt("clear_deadline", t.id, notes)

    # ------------------------------------------------- 事件：advance_time

    def advance_time(self, now: int) -> Receipt:
        """把逻辑时钟推进到 now（非递减整数；相等允许，用于触发恰等到期）。

        先按 (截止时刻, 任务创建序) 触发全部到期条目（deadline <= now，
        恰好相等也算到期），再返回——调用方的下一条业务事件因此在所有
        到期处理之后才被处理。到期只提出取消请求：shield 内记 pending，
        scope 关闭仍走 close_scope 的既有等待条件。
        """
        _require_int(now, "now")
        if now < self._now:
            raise ValueError(
                f"time is non-decreasing: cannot advance from {self._now} "
                f"back to {now}")
        if now == self._now:
            notes = [f"time stays at {now}; checking for due deadlines"]
        else:
            notes = [f"time advanced {self._now} -> {now}"]
        self._now = now
        fired = 0
        while self._timers and self._timers[0][0] <= now:
            deadline, tid, gen = heapq.heappop(self._timers)
            t = self._tasks[tid]
            if t.deadline != deadline or t.deadline_gen != gen:
                continue  # 旧代次/已失效的定时条目，不得影响当前任务
            self._invalidate_deadline(t)
            fired += 1
            if t.id == 0:
                notes.append(
                    f"deadline of root scope expired at {deadline}: "
                    f"requesting cancellation of all unfinished descendants")
                self._cancel_nodes(self._subtree(0)[1:], notes)
            else:
                notes.append(
                    f"deadline of task#{t.id} '{t.name}' expired at "
                    f"{deadline}: cancellation requested for its subtree")
                self._cancel_nodes(self._subtree(t.id), notes)
        if not fired:
            notes.append("no deadlines due")
        self._reap()
        return self._receipt("advance_time", 0, notes)

    # ---------------------------------------------------------- 查询

    @property
    def now(self) -> int:
        """当前逻辑时钟时刻（非递减整数，初始 0）。"""
        return self._now

    def next_deadline(self) -> Optional[tuple[int, int]]:
        """下一待触发时刻与到期来源：(deadline, task_id)；无则 None。

        供模拟器逐步驱动：反复 advance_time(next_deadline()[0]) 即可按
        (截止时刻, 创建序) 依次触发到期。失效条目在此惰性剔除。
        """
        while self._timers:
            deadline, tid, gen = self._timers[0]
            t = self._tasks[tid]
            if t.deadline == deadline and t.deadline_gen == gen:
                return (deadline, tid)
            heapq.heappop(self._timers)
        return None

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
                "deadline": t.deadline,
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
            if n.deadline is not None:
                who = (f"task#{n.id} '{n.name}'" if n.id != 0
                       else "root scope")
                reasons.append(
                    f"{who}: deadline at {n.deadline} (now={self._now})")
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
