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

成功策略（watch）
----------------
watch(scope_id, strategy=...) 对某 scope 的**直接子任务**（以创建时刻的
快照为成员集，之后冻结）设定成功策略，按输入事件顺序判定：

* "all"       全部成功：任一成员业务失败即永不可能达标；否则全部成员成功才达标。
* "first"     首个成功：第一个 finished 结局出现即达标；全部成员终结且无成功则不可能。
* "n_succeed" 指定数量成功：finished 数达到 need 即达标；
              finished + 未完成 < need 时永不可能达标。

达到结束条件后进入 cleaning：请求取消全部未完成成员的子树，等待全部成员
退出（清理确认）后才生成汇总（done）。汇总中成功、失败、取消分别记录；
失败再按发生时刻分为判定阶段失败与清理失败（清理期间成员以业务异常退出）。
watch 进行中（watching/cleaning）禁止向该 scope spawn；同一 scope 同一时刻
只允许一个进行中的 watch。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional

MAX_TASKS = 10_000   # 单棵运行时允许的最大任务数（含根）
MAX_DEPTH = 128      # 任务树最大深度（根深度为 0）
MAX_NAME_LEN = 64    # 任务名最大字符数

WATCH_STRATEGIES = ("all", "first", "n_succeed")  # 支持的成功策略

__all__ = [
    "MAX_TASKS", "MAX_DEPTH", "MAX_NAME_LEN", "WATCH_STRATEGIES",
    "TaskTreeError", "UnknownTaskError", "ScopeClosedError",
    "FinishConflictError", "ShieldError", "TaskStateError",
    "ResourceLimitError", "TaskCancelled",
    "WatchError", "WatchStateError", "UnknownWatchError",
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


class WatchError(TaskTreeError):
    """成功策略（watch）相关错误的基类。"""


class WatchStateError(WatchError):
    """watch 状态不允许该操作（如重复布防、汇总未就绪、成员重叠）。"""


class UnknownWatchError(WatchError, LookupError):
    """引用了不存在的 watch_id。"""


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
    outcome_seq: Optional[int] = None   # 记录结局时的事件序号（事件顺序判定用）
    shield_depth: int = 0
    pending_cancels: int = 0
    cancel_delivered: bool = False
    scope_state: str = "open"          # open | closing | closed


@dataclass
class _Watch:
    """一个成功策略布防。成员集在创建时冻结（scope 当时的直接子任务）。"""
    id: int
    scope_id: int
    strategy: str                      # all | first | n_succeed
    need: Optional[int]                # 仅 n_succeed
    members: tuple[int, ...]           # 成员任务 id（创建序）
    phase: str = "watching"            # watching | cleaning | done
    trigger_seq: Optional[int] = None  # 达到结束条件的事件序号
    goal_met: Optional[bool] = None    # 结束条件是否因达标而触发
    done_seq: Optional[int] = None     # 汇总就绪的事件序号
    result: Optional[dict] = None      # 汇总（见 watch_result）


@dataclass(frozen=True)
class Receipt:
    """单个事件的处理回执。

    seq         事件序号（从 1 开始，全局有序）
    op          操作名
    task_id     目标任务/scope 属主 id
    ok          事件是否被接受（语义拒绝会抛异常而不是返回 ok=False）
    notes       该事件产生的说明（含 pending/late/idempotent/watch 等细节）
    state       事件后目标任务状态（root 为 None）
    scope_state 事件后目标 scope 状态
    pending     事件后整棵树的待完成原因（pending_reasons(0)）
    new_task_id 仅 spawn：新任务 id
    watch_id    仅 watch：新布防 id
    watch_phase 仅 watch：布防在事件后的阶段（watching/cleaning/done）
    watch_updates 本事件中发生阶段变迁的 watch：((watch_id, 新阶段), ...)
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
    watch_id: Optional[int] = None
    watch_phase: Optional[str] = None
    watch_updates: tuple[tuple[int, str], ...] = ()


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
        self._watches: dict[int, _Watch] = {}
        self._next_watch_id = 1
        self.log: list[Receipt] = []

    # ---------------------------------------------------------- 内部工具

    def _get(self, task_id: Any) -> _Task:
        _require_int(task_id, "task_id")
        try:
            return self._tasks[task_id]
        except KeyError:
            raise UnknownTaskError(f"unknown task_id: {task_id}") from None

    def _get_watch(self, watch_id: Any) -> _Watch:
        _require_int(watch_id, "watch_id")
        try:
            return self._watches[watch_id]
        except KeyError:
            raise UnknownWatchError(f"unknown watch_id: {watch_id}") from None

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

    def _cancel_nodes(self, nodes: list[_Task], notes: list[str]) -> None:
        for n in nodes:
            if n.outcome_kind is not None:
                continue  # 已有结局的任务不再接受取消
            self._request_cancel(n, notes)

    def _receipt(self, op: str, task_id: Optional[int],
                 notes: list[str], new_task_id: Optional[int] = None,
                 watch_id: Optional[int] = None,
                 watch_updates: tuple[tuple[int, str], ...] = ()) -> Receipt:
        self._seq += 1
        t = self._tasks.get(task_id) if task_id is not None else None
        w = self._watches.get(watch_id) if watch_id is not None else None
        r = Receipt(
            seq=self._seq, op=op, task_id=task_id, ok=True,
            notes=tuple(notes),
            state=(t.state if (t is not None and t.id != 0) else None),
            scope_state=(t.scope_state if t is not None else None),
            pending=tuple(self.pending_reasons(0)),
            new_task_id=new_task_id,
            watch_id=watch_id,
            watch_phase=(w.phase if w is not None else None),
            watch_updates=watch_updates,
        )
        self.log.append(r)
        return r

    # ------------------------------------------------- watch 推进与汇总

    def _pump_watches(self, notes: list[str],
                      updates: list[tuple[int, str]] | None = None) -> None:
        """每个被接受的事件后推进全部 watch（不动点收敛）。

        顺序：判定 watching 是否达到结束条件 -> cleaning 中确认全部成员
        退出后生成汇总。结局序号由 finish/fail 在记录结局时直接写入，
        因此本方法只需做不动点推进。发生的阶段变迁（含同事件连续变迁）
        记入 updates：(watch_id, 新阶段)。
        """
        changed = True
        while changed:
            changed = False
            for w in sorted(self._watches.values(), key=lambda w: w.id):
                if w.phase == "watching" and self._watch_end_condition(w):
                    self._start_cleanup(w, notes)
                    if updates is not None:
                        updates.append((w.id, "cleaning"))
                    changed = True
                if w.phase == "cleaning" and all(
                        self._tasks[m].state == "exited" for m in w.members):
                    self._finalize_watch(w, notes)
                    if updates is not None:
                        updates.append((w.id, "done"))
                    changed = True

    def _watch_end_condition(self, w: _Watch) -> bool:
        """按输入事件顺序判定是否达到结束条件（达标或永不可能达标）。

        已被投递取消但尚未结局的成员注定无法 finished（取消后 finish 只能
        得到 cancelled、fail 只能得到 failed），因此算作"不可能再成功"；
        shield 内仅 pending 的取消不算（仍可正常 finish 后丢弃 pending）。
        """
        tasks = self._tasks
        n_fin = n_fail = n_cancel = n_open = 0
        n_possible = 0  # 未结局且取消未投递：仍可能 finished
        for m in w.members:
            kind = tasks[m].outcome_kind
            if kind == "finished":
                n_fin += 1
            elif kind == "failed":
                n_fail += 1
            elif kind == "cancelled":
                n_cancel += 1
            else:
                n_open += 1
                if not tasks[m].cancel_delivered:
                    n_possible += 1
        if w.strategy == "all":
            # 任一成员 failed/cancelled/已投递取消即注定无法全部成功
            if n_fail > 0 or n_cancel > 0 or n_possible < n_open:
                w.goal_met = False
                return True
            if n_open == 0:
                w.goal_met = n_fin == len(w.members)
                return True
            return False
        if w.strategy == "first":
            if n_fin > 0:
                w.goal_met = True
                return True
            if n_possible == 0:
                w.goal_met = False
                return True
            return False
        # n_succeed：failed/cancelled/已投递取消都不占成功名额
        if n_fin >= w.need:
            w.goal_met = True
            return True
        if n_fin + n_possible < w.need:
            w.goal_met = False
            return True
        return False

    def _start_cleanup(self, w: _Watch, notes: list[str]) -> None:
        """达到结束条件：请求取消全部未完成成员的子树，进入清理等待。"""
        w.phase = "cleaning"
        w.trigger_seq = self._seq + 1
        goal = "satisfied" if w.goal_met else "unsatisfiable"
        notes.append(
            f"watch#{w.id} ({w.strategy}) end condition met ({goal}); "
            f"cancelling unfinished member subtrees and awaiting cleanup")
        for m in w.members:
            t = self._tasks[m]
            if t.outcome_kind is None:
                self._cancel_nodes(self._subtree(m), notes)

    def _finalize_watch(self, w: _Watch, notes: list[str]) -> None:
        """清理确认（全部成员退出）后生成汇总：成功/失败/取消分别记录。"""
        w.phase = "done"
        w.done_seq = self._seq + 1
        recs = sorted(
            (self._tasks[m] for m in w.members),
            key=lambda t: (t.outcome_seq, t.id))
        successes = [{"task_id": t.id, "name": t.name, "value": t.outcome_value}
                     for t in recs if t.outcome_kind == "finished"]
        # 失败按发生时刻分流：trigger_seq 之后记录的结局属于清理阶段
        failures = [{"task_id": t.id, "name": t.name, "error": t.outcome_value,
                     "phase": ("cleanup"
                               if t.outcome_seq > w.trigger_seq
                               else "trigger")}
                    for t in recs if t.outcome_kind == "failed"]
        cancelled = [t.id for t in recs if t.outcome_kind == "cancelled"]
        cleanup_failures = [f for f in failures if f["phase"] == "cleanup"]
        if w.goal_met:
            status = ("satisfied_with_cleanup_errors"
                      if cleanup_failures else "satisfied")
        else:
            status = "unsatisfiable"
        if w.strategy == "all":
            winners = ([s["task_id"] for s in successes]
                       if status != "unsatisfiable" else [])
        elif w.strategy == "first":
            winners = ([successes[0]["task_id"]] if successes else [])
        else:  # n_succeed
            winners = [s["task_id"] for s in successes[:w.need]]
        w.result = {
            "watch_id": w.id,
            "scope_id": w.scope_id,
            "strategy": w.strategy,
            "need": w.need,
            "status": status,
            "goal_met": w.goal_met,
            "members": list(w.members),
            "winners": winners,
            "successes": successes,
            "failures": failures,
            "cancelled": cancelled,
            "trigger_seq": w.trigger_seq,
            "done_seq": w.done_seq,
        }
        notes.append(
            f"watch#{w.id} cleanup confirmed; summary ready "
            f"(status={status}, successes={len(successes)}, "
            f"failures={len(failures)}, cancelled={len(cancelled)})")

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
        for w in self._watches.values():
            if w.scope_id == p.id and w.phase != "done":
                raise WatchStateError(
                    f"scope of task#{p.id} has active watch#{w.id} "
                    f"({w.phase}); spawn is forbidden until the watch is "
                    f"done (watch membership is frozen at creation)")
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
        t.outcome_seq = self._seq + 1  # 本事件序号（receipt 尚未编号）
        self._reap()
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)
        return self._receipt("finish", t.id, notes,
                             watch_updates=tuple(updates))

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
        t.outcome_seq = self._seq + 1  # 本事件序号（receipt 尚未编号）
        self._reap()
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)
        return self._receipt("fail", t.id, notes,
                             watch_updates=tuple(updates))

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
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)
        return self._receipt("cancel", t.id, notes,
                             watch_updates=tuple(updates))

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
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)
        return self._receipt("leave_shield", t.id, notes,
                             watch_updates=tuple(updates))

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
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)
        if t.scope_state == "closed":
            notes.append(f"scope of task#{t.id} closed")
        return self._receipt("close_scope", t.id, notes,
                             watch_updates=tuple(updates))

    # ------------------------------------------------- 事件：watch（成功策略）

    def watch(self, scope_id: int = 0, strategy: str = "all",
              need: Optional[int] = None) -> Receipt:
        """对 scope 的直接子任务（创建时刻快照）布防成功策略。

        strategy: "all" 全部成功 | "first" 首个成功 | "n_succeed" 指定数量
        成功（需 need >= 1）。成员集在创建时冻结；布防进行中禁止向该 scope
        spawn。判定按输入事件顺序进行，达到结束条件后取消未完成成员并等待
        清理确认，汇总就绪后通过 watch_result(watch_id) 读取。
        """
        t = self._get(scope_id)
        if not isinstance(strategy, str):
            raise TypeError(
                f"strategy must be a str, got {type(strategy).__name__}: "
                f"{strategy!r}")
        if strategy not in WATCH_STRATEGIES:
            raise ValueError(
                f"strategy must be one of {WATCH_STRATEGIES}, "
                f"got {strategy!r}")
        if strategy == "n_succeed":
            if need is None:
                raise ValueError(
                    "need is required for strategy 'n_succeed' "
                    "(int >= 1)")
            _require_int(need, "need")
            if need < 1:
                raise ValueError(f"need must be >= 1, got {need}")
        elif need is not None:
            raise ValueError(
                f"need is only valid for strategy 'n_succeed', "
                f"got need={need!r} with strategy={strategy!r}")
        if t.scope_state != "open":
            raise WatchStateError(
                f"scope of task#{t.id} is {t.scope_state}; "
                f"cannot watch a closing/closed scope")
        members = tuple(t.children)
        for w in self._watches.values():
            if w.phase != "done" and w.scope_id == t.id:
                raise WatchStateError(
                    f"scope of task#{t.id} already has active watch#{w.id} "
                    f"({w.phase}); one active watch per scope")
        wid = self._next_watch_id
        self._next_watch_id += 1
        w = _Watch(id=wid, scope_id=t.id, strategy=strategy, need=need,
                   members=members)
        self._watches[wid] = w
        notes = [
            f"watch#{wid} armed on scope of task#{t.id}: strategy="
            f"{strategy!r}"
            + (f", need={need}" if strategy == "n_succeed" else "")
            + f", members={list(members)} (frozen at creation)"]
        updates: list[tuple[int, str]] = []
        self._pump_watches(notes, updates)  # 成员可能已终结，立即按事件序判定
        return self._receipt("watch", t.id, notes, watch_id=wid,
                             watch_updates=tuple(updates))

    # ---------------------------------------------------------- 查询

    def status(self) -> dict[int, dict[str, Any]]:
        """整棵树快照（按任务创建序）。"""
        active_by_scope: dict[int, _Watch] = {}
        for w in self._watches.values():
            if w.phase != "done":
                active_by_scope[w.scope_id] = w
        out: dict[int, dict[str, Any]] = {}
        for tid, t in sorted(self._tasks.items()):
            w = active_by_scope.get(tid)
            out[tid] = {
                "name": t.name,
                "parent": t.parent,
                "state": t.state,
                "outcome": t.outcome_kind,
                "value": (t.outcome_value
                          if t.outcome_kind == "finished" else None),
                "error": (repr(t.outcome_value)
                          if t.outcome_kind == "failed" else None),
                "outcome_seq": t.outcome_seq,
                "shield_depth": t.shield_depth,
                "pending_cancels": t.pending_cancels,
                "cancel_delivered": t.cancel_delivered,
                "scope": t.scope_state,
                "children": list(t.children),
                "watch": (w.id if w is not None else None),
                "watch_phase": (w.phase if w is not None else None),
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
        for w in sorted(self._watches.values(), key=lambda w: w.id):
            if w.phase == "done":
                continue
            if not any(n.id == w.scope_id for n in self._subtree(t.id)):
                continue
            if w.phase == "watching":
                reasons.append(
                    f"watch#{w.id} on scope of task#{w.scope_id}: watching "
                    f"({w.strategy})")
            else:
                waiting = [m for m in w.members
                           if self._tasks[m].state != "exited"]
                reasons.append(
                    f"watch#{w.id} on scope of task#{w.scope_id}: cleaning, "
                    f"waiting on member task(s) {waiting}")
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

    # ------------------------------------------------- 查询：watch

    def watch_result(self, watch_id: int) -> dict:
        """读取 watch 汇总；仅在清理确认完成（done）后可用。

        返回 dict 的键：
        watch_id/scope_id/strategy/need  布防标识与参数
        status     satisfied | satisfied_with_cleanup_errors | unsatisfiable
        goal_met   结束条件是否因达标而触发
        members    成员任务 id（创建序，创建时冻结）
        winners    计入达标的成功任务 id（事件序）
        successes  [{"task_id","name","value"}...]（事件序）
        failures   [{"task_id","name","error","phase"}...]（事件序）；
                   phase 为 "trigger"（判定阶段）或 "cleanup"（清理失败）
        cancelled  被取消的成员任务 id（事件序）
        trigger_seq/done_seq  触发与汇总就绪的事件序号
        """
        w = self._get_watch(watch_id)
        if w.phase != "done":
            raise WatchStateError(
                f"watch#{w.id} is {w.phase}; summary is available only "
                f"after cleanup is confirmed (phase 'done')")
        # 重建一层容器，防止外部改到内部状态；结果值/异常对象保持身份
        # （与库内不深拷贝 finish 结果的约定一致）。
        r = w.result
        return {
            **r,
            "members": list(r["members"]),
            "winners": list(r["winners"]),
            "cancelled": list(r["cancelled"]),
            "successes": [dict(s) for s in r["successes"]],
            "failures": [dict(f) for f in r["failures"]],
        }

    def watches(self) -> dict[int, dict[str, Any]]:
        """全部 watch 的快照（按布防序）。"""
        out: dict[int, dict[str, Any]] = {}
        for wid, w in sorted(self._watches.items()):
            kinds = [self._tasks[m].outcome_kind for m in w.members]
            out[wid] = {
                "scope_id": w.scope_id,
                "strategy": w.strategy,
                "need": w.need,
                "members": list(w.members),
                "phase": w.phase,
                "goal_met": w.goal_met,
                "trigger_seq": w.trigger_seq,
                "done_seq": w.done_seq,
                "finished": kinds.count("finished"),
                "failed": kinds.count("failed"),
                "cancelled": kinds.count("cancelled"),
                "open": kinds.count(None),
            }
        return out
