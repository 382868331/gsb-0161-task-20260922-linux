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

监督策略（watch）
----------------
watch(scope, strategy=...) 在某个 scope 上安装一个监督观察者：

* "all"    —— 全部成功：所有直接子任务成功才满足；
* "first"  —— 首个成功：任意一个直接子任务成功即满足；
* "quorum" —— 指定数量成功：count 个直接子任务成功即满足。

判定严格按输入事件顺序：watch() 时成员集合（该 scope 的直接子任务）冻结，
此后不允许再向该 scope spawn。每个成员记录结局（finish/fail）时：
成功数达到需求 -> 满足结束；成功数 + 未决数 < 需求 -> 永不可能满足，以
不满足结束。任一结束都会：scope 置 closing，取消全部未完成成员（沿子树
传播，shield 仍只记 pending），等待每个成员退出（清理确认）后 scope 才
closed，watcher 汇总才定稿。失败结局与取消结局分别记录；清理期间发生的
业务失败照样进入 exception_group。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional

MAX_TASKS = 10_000   # 单棵运行时允许的最大任务数（含根）
MAX_DEPTH = 128      # 任务树最大深度（根深度为 0）
MAX_NAME_LEN = 64    # 任务名最大字符数

__all__ = [
    "MAX_TASKS", "MAX_DEPTH", "MAX_NAME_LEN",
    "STRATEGIES",
    "TaskTreeError", "UnknownTaskError", "ScopeClosedError",
    "FinishConflictError", "ShieldError", "TaskStateError",
    "ResourceLimitError", "StrategyError", "TaskCancelled",
    "Receipt", "Watcher", "TaskRuntime",
]

# 监督策略名 -> 规范化小写形式（构造时允许大小写混用的余地不提供，仅接受这三个）
STRATEGIES = ("all", "first", "quorum")


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


class StrategyError(TaskTreeError):
    """监督策略参数非法，或 watch 的前置条件不满足。"""


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
    watcher: "Optional[Watcher]" = None
    outcome_seq: Optional[int] = None  # 结局首次落定的事件序号（用于事件序归并）


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


@dataclass
class Watcher:
    """某个 scope 上的监督观察者（watch() 的公开状态）。

    scope            安装 watcher 的 scope 属主任务 id
    strategy         all | first | quorum
    required         需要成功的成员数（all=成员总数，first=1，quorum=count）
    members          成员冻结集：watch() 时该 scope 的直接子任务 id（创建序）
    state            pending（未达结束条件）| satisfied | unsatisfied |
                     aborted（scope 在结束条件达成前被手工 close_scope）
    satisfied        便于直接取用的布尔结论；pending/aborted 时为 None
    succeeded        记录成功结局的成员（按输入事件顺序）
    failed           记录业务失败结局的成员（按输入事件顺序；
                     含清理确认期间才失败的成员）
    cancelled        记录取消结局的成员（按输入事件顺序）
    cleanup_failures 清理期间（取消请求发出后）记录业务失败的成员
    decided_seq      触发结束条件的事件序号；未定稿为 None
    closed_seq       清理完成、scope closed、汇总定稿的事件序号
    """
    scope: int
    strategy: str
    required: int
    members: tuple[int, ...]
    state: str = "pending"
    satisfied: Optional[bool] = None
    succeeded: list[int] = field(default_factory=list)
    failed: list[int] = field(default_factory=list)
    cancelled: list[int] = field(default_factory=list)
    cleanup_failures: list[int] = field(default_factory=list)
    decided_seq: Optional[int] = None
    closed_seq: Optional[int] = None

    def as_dict(self) -> dict[str, Any]:
        """公开快照（全部为可直接比较的内建类型，可用于断言）。"""
        return {
            "scope": self.scope,
            "strategy": self.strategy,
            "required": self.required,
            "members": list(self.members),
            "state": self.state,
            "satisfied": self.satisfied,
            "succeeded": list(self.succeeded),
            "failed": list(self.failed),
            "cancelled": list(self.cancelled),
            "cleanup_failures": list(self.cleanup_failures),
            "decided_seq": self.decided_seq,
            "closed_seq": self.closed_seq,
        }


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

    def _cancel_nodes(self, nodes: list[_Task], notes: list[str]) -> None:
        for n in nodes:
            if n.outcome_kind is not None:
                continue  # 已有结局的任务不再接受取消
            self._request_cancel(n, notes)

    # ----------------------------------------------------- watcher 机制

    @staticmethod
    def _bucket_for(w: Watcher, kind: str) -> list[int]:
        """结局类型 -> watcher 对应的记账列表。"""
        return {
            "finished": w.succeeded,
            "failed": w.failed,
            "cancelled": w.cancelled,
        }[kind]

    def _note_member_outcome(self, node: _Task, notes: list[str]) -> None:
        """成员记录结局后，按输入事件顺序更新所属 scope 的 watcher。

        成功数达到需求 -> 满足结束；成功数 + 未决数 < 需求 -> 不可能满足，
        以不满足结束。两种结束都立即把 scope 置 closing 并取消全部未结束
        后代，进入"等待清理确认"阶段（scope 在成员全部退出后才 closed）。
        """
        if node.parent is None:
            return
        owner = self._tasks[node.parent]
        w = owner.watcher
        if w is None or node.id not in w.members:
            return
        bucket = self._bucket_for(w, node.outcome_kind)
        if node.id not in bucket:
            bucket.append(node.id)
        # 清理确认期间（scope 已在收尾）才记录的业务失败
        if (node.outcome_kind == "failed" and owner.scope_state == "closing"
                and node.id not in w.cleanup_failures):
            w.cleanup_failures.append(node.id)
        if w.state != "pending":
            return
        n_succ = len(w.succeeded)
        n_settled = len(w.succeeded) + len(w.failed) + len(w.cancelled)
        unresolved = len(w.members) - n_settled
        if n_succ >= w.required:
            self._decide_watcher(w, True, notes)
        elif n_succ + unresolved < w.required:
            self._decide_watcher(w, False, notes)

    def _decide_watcher(self, w: Watcher, satisfied: bool,
                        notes: list[str]) -> None:
        owner = self._tasks[w.scope]
        w.state = "satisfied" if satisfied else "unsatisfied"
        w.satisfied = satisfied
        w.decided_seq = self._seq + 1  # 当前事件的 Receipt 尚未编号
        verdict = "satisfied" if satisfied else "cannot be satisfied"
        notes.append(
            f"watcher on scope of task#{w.scope} ({w.strategy}): end "
            f"condition {verdict} after {len(w.succeeded)} success(es) "
            f"(need {w.required}); cancelling unfinished members and "
            f"awaiting cleanup confirmation")
        if owner.scope_state == "open":
            owner.scope_state = "closing"
            before = len(notes)
            self._cancel_nodes(self._subtree(owner.id)[1:], notes)
            if len(notes) == before:
                notes.append("no unfinished descendants")

    def _finalize_watchers(self, notes: list[str]) -> None:
        """清理确认：成员全部退出、scope closed 后，watcher 汇总才定稿。"""
        for t in self._tasks.values():
            w = t.watcher
            if (w is None or w.closed_seq is not None
                    or t.scope_state != "closed"):
                continue
            w.closed_seq = self._seq
            notes.append(
                f"watcher on scope of task#{w.scope} finalized as "
                f"{w.state}: {len(w.succeeded)} succeeded, "
                f"{len(w.failed)} failed, {len(w.cancelled)} cancelled "
                f"(summary now settled)")

    def _receipt(self, op: str, task_id: Optional[int],
                 notes: list[str], new_task_id: Optional[int] = None) -> Receipt:
        self._seq += 1
        # scope 在本事件中收敛为 closed -> watcher 清理确认完成，汇总定稿
        self._finalize_watchers(notes)
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
        if p.watcher is not None:
            raise ScopeClosedError(
                f"scope of task#{p.id} has a watcher installed "
                f"({p.watcher.strategy}); its member set was frozen at "
                f"watch() time and no longer accepts spawn")
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
            t.outcome_seq = self._seq + 1
            notes = [f"cancellation had been delivered to task#{t.id}; "
                     f"result {result!r} superseded by cancellation"]
        else:
            t.outcome_kind = "finished"
            t.outcome_value = result
            t.outcome_seq = self._seq + 1
            notes = [f"task#{t.id} finished with {result!r}"]
        t.state = "draining"
        self._note_member_outcome(t, notes)
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
        t.outcome_seq = self._seq + 1
        notes = [f"task#{t.id} failed with {type(exc).__name__}: {exc}"]
        if t.cancel_delivered:
            notes.append("cancellation had been delivered; "
                         "business failure takes precedence")
        t.state = "draining"
        self._note_member_outcome(t, notes)
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
            # 监督结束条件达成前手工关闭：watcher 作废，不产生成功/失败结论
            w = t.watcher
            if w is not None and w.state == "pending":
                w.state = "aborted"
                w.satisfied = None
                w.decided_seq = self._seq + 1
                notes.append(
                    f"watcher on scope of task#{t.id} aborted: scope was "
                    f"closed manually before its {w.strategy} end "
                    f"condition was reached")
            before = len(notes)
            self._cancel_nodes(self._subtree(t.id)[1:], notes)
            if len(notes) == before:
                notes.append("no unfinished descendants")
        self._reap()
        if t.scope_state == "closed":
            notes.append(f"scope of task#{t.id} closed")
        return self._receipt("close_scope", t.id, notes)

    # ----------------------------------------------------- 事件：watch

    def watch(self, task_id: int = 0, strategy: str = "all",
              count: Optional[int] = None) -> Receipt:
        """在 task_id 的 scope 上安装监督观察者。

        strategy: "all"（全部成功）/ "first"（首个成功）/
                  "quorum"（count 个成功）。
        成员 = watch() 时该 scope 的直接子任务，立即冻结，此后拒绝 spawn。
        必须在 scope open、且尚未安装 watcher 时调用。
        """
        t = self._get(task_id)
        if not isinstance(strategy, str):
            raise StrategyError(
                f"strategy must be a str, got {type(strategy).__name__}: "
                f"{strategy!r}")
        if strategy not in STRATEGIES:
            raise StrategyError(
                f"unknown strategy {strategy!r}; expected one of {STRATEGIES}")
        if count is not None:
            _require_int(count, "count")
        if strategy == "quorum":
            if count is None:
                raise StrategyError(
                    "strategy 'quorum' requires count=<int>")
            if count < 1:
                raise StrategyError(
                    f"count must be >= 1 for quorum, got {count}")
        elif count is not None:
            raise StrategyError(
                f"count is only valid with strategy 'quorum', "
                f"got strategy={strategy!r} count={count}")
        if t.scope_state != "open":
            raise StrategyError(
                f"scope of task#{t.id} is {t.scope_state}; "
                f"watch() requires an open scope")
        if t.watcher is not None:
            raise StrategyError(
                f"scope of task#{t.id} already has a watcher "
                f"({t.watcher.strategy}); exactly one watcher per scope")
        members = tuple(t.children)
        if not members:
            raise StrategyError(
                f"scope of task#{t.id} has no child tasks at watch() time; "
                f"spawn members before watching")
        if t.id != 0 and t.outcome_kind is not None:
            raise StrategyError(
                f"task#{t.id} already recorded outcome ({t.outcome_kind}); "
                f"cannot watch a scope whose owner has ended")
        if strategy == "all":
            required = len(members)
        elif strategy == "first":
            required = 1
        else:
            required = count
            if count > len(members):
                raise StrategyError(
                    f"quorum count {count} exceeds number of members "
                    f"{len(members)}; it could never succeed")
        w = Watcher(scope=t.id, strategy=strategy, required=required,
                    members=members)
        t.watcher = w
        notes = [
            f"watcher installed on scope of task#{t.id}: strategy={strategy}, "
            f"members={list(members)}, required successes={required}; "
            f"member set frozen (spawn into this scope now rejected)"]
        # 成员可能在 watch 前已有结局：统一按事件序补登记并做一次判定，
        # 让"安装即满足/不满足"与运行中到达走同一处规则。
        self._note_watch_install(w, notes)
        self._reap()
        return self._receipt("watch", t.id, notes)

    def _note_watch_install(self, w: Watcher, notes: list[str]) -> None:
        """watch() 时若成员已有结局（允许先 finish 部分成员再 watch），
        按结局落定的事件顺序补登记并立即做一次结束判定。"""
        settled = [self._tasks[m] for m in w.members
                   if self._tasks[m].outcome_kind is not None]
        settled.sort(key=lambda m: m.outcome_seq)
        for m in settled:
            self._bucket_for(w, m.outcome_kind).append(m.id)
        n_succ = len(w.succeeded)
        n_settled = len(w.succeeded) + len(w.failed) + len(w.cancelled)
        unresolved = len(w.members) - n_settled
        if n_succ >= w.required or n_succ + unresolved < w.required:
            # 安装前 scope 一直 open、不可能已发起清理；结论恒按普通路径走
            self._decide_watcher(w, n_succ >= w.required, notes)

    # ---------------------------------------------------------- 查询

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
                "watcher": (t.watcher.as_dict()
                            if t.watcher is not None else None),
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
            w = n.watcher
            if w is not None:
                unresolved_members = [
                    m for m in w.members
                    if self._tasks[m].state != "exited"]
                if unresolved_members:
                    phase = ("awaiting cleanup confirmation"
                             if n.scope_state == "closing"
                             else "end condition pending")
                    reasons.append(
                        f"watcher on scope of task#{n.id} ({w.strategy}, "
                        f"state={w.state}): {phase}; members not exited: "
                        f"{unresolved_members}")
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

    def watcher_summary(self, task_id: int = 0) -> Optional[dict[str, Any]]:
        """返回该 scope 上 watcher 的定稿/当前汇总；未安装返回 None。

        state == "pending" 表示结束条件尚未达成；scope 清理确认完成后
        closed_seq 被赋值，汇总（succeeded/failed/cancelled）才定稿。
        """
        t = self._get(task_id)
        return t.watcher.as_dict() if t.watcher is not None else None

    def outcome(self, task_id: int) -> tuple[Optional[str], Any]:
        t = self._get(task_id)
        return t.outcome_kind, t.outcome_value
