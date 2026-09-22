"""Structured task-tree cancellation and exception aggregation core.

Pure standard-library, event-driven, no threads.  Every mutation is an
explicit ordered operation (spawn / finish / fail / cancel / enter_shield /
leave_shield / close_scope) applied synchronously to an in-memory task
tree.  Each operation returns an :class:`Event` describing what happened,
the resulting status of the affected scope, and the reasons it is still
waiting to complete.

Semantics (see README.md for the full contract):

* A task is ACTIVE, then COMPLETING once an outcome (finish / fail /
  cancel / close) has been requested, then DONE once its whole subtree
  has exited.  A scope never completes while any descendant is not DONE.
* ``close_scope`` requests cancellation of all unfinished children and
  waits for the subtree to exit before the scope completes.
* ``fail`` also requests cancellation of the failing task's unfinished
  children (the subtree must exit before the failure is final).
* ``finish`` waits for children without cancelling them.
* Cancellation inside a shield is recorded as pending and only delivered
  when the outermost shield is left.
* Cancellation itself is not a business exception: cancelled tasks never
  contribute to the aggregated :class:`ExceptionGroup`.
* Business exceptions of a scope and its descendants are aggregated by
  task creation order (a parent is always created before its children).
* ``finish`` is idempotent for an equal result; a different result (or a
  different outcome kind, e.g. a late ``finish`` after cancellation)
  raises :class:`ResultConflictError`.
* Spawning into a closing or closed scope raises :class:`ScopeClosedError`.

Every operation validates all of its inputs *before* mutating any state,
so a rejected operation never leaves a partial change behind.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

__all__ = [
    "ACTIVE",
    "COMPLETING",
    "DONE",
    "Event",
    "InvalidInputError",
    "ResultConflictError",
    "ScopeClosedError",
    "ShieldError",
    "TaskTree",
    "TaskTreeError",
    "UnknownTaskError",
]

ACTIVE = "active"
COMPLETING = "completing"
DONE = "done"

_OUTCOME_KINDS = ("finished", "failed", "cancelled")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TaskTreeError(Exception):
    """Base class for all errors raised by this module."""


class InvalidInputError(TaskTreeError, ValueError):
    """An argument failed validation (wrong type, NaN/Infinity, ...).

    The message always names the operation and the offending parameter so
    the caller can locate the problem.
    """


class UnknownTaskError(TaskTreeError, LookupError):
    """The referenced task id does not exist in this tree."""


class ScopeClosedError(TaskTreeError):
    """spawn was attempted on a scope that is closing or closed."""


class ResultConflictError(TaskTreeError):
    """A completion conflicts with an already-recorded outcome."""


class ShieldError(TaskTreeError):
    """leave_shield was called without a matching enter_shield."""


# ---------------------------------------------------------------------------
# Public event record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """Record of one applied (or rejected-as-no-op) operation.

    ``status`` is ``"applied"`` when the operation changed state and
    ``"no-op"`` when it was legal but had no effect (e.g. a late cancel
    of a finished task, an idempotent repeated finish, closing an
    already-closing scope).  Rejected operations raise instead of
    producing an event.

    ``waiting`` lists the human-readable reasons the affected scope is
    not yet DONE (empty once it is done).
    """

    seq: int
    op: str
    task: int
    status: str  # "applied" | "no-op"
    note: str
    task_status: str
    waiting: tuple[str, ...]


# ---------------------------------------------------------------------------
# Internal task record
# ---------------------------------------------------------------------------


@dataclass
class _Task:
    id: int
    name: str
    parent: int | None
    order: int  # creation order, monotonically increasing
    status: str = ACTIVE
    outcome_kind: str | None = None  # None | "finished" | "failed" | "cancelled"
    result: object = None
    error: BaseException | None = None
    cancel_reason: str | None = None
    children: list[int] = field(default_factory=list)
    shield_depth: int = 0
    pending_cancels: list[str] = field(default_factory=list)
    cancel_requested: bool = False
    close_requested: bool = False
    aggregate: tuple[BaseException, ...] = ()  # filled when DONE


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _check_task_id(value, op):
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidInputError(
            f"{op}: task id must be an int, got {type(value).__name__} ({value!r})"
        )


def _check_name(value, op):
    if not isinstance(value, str):
        raise InvalidInputError(
            f"{op}: name must be a str, got {type(value).__name__} ({value!r})"
        )
    if not value:
        raise InvalidInputError(f"{op}: name must be a non-empty string")


def _check_reason(value, op):
    if not isinstance(value, str):
        raise InvalidInputError(
            f"{op}: reason must be a str, got {type(value).__name__} ({value!r})"
        )


def _check_result(value, op):
    """Reject NaN / Infinity in numeric results (float and complex)."""
    if isinstance(value, bool):
        return  # a bool result is not an integer parameter; allowed as-is
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        raise InvalidInputError(f"{op}: result must be finite, got {value!r}")
    if isinstance(value, complex) and (
        math.isnan(value.real)
        or math.isnan(value.imag)
        or math.isinf(value.real)
        or math.isinf(value.imag)
    ):
        raise InvalidInputError(f"{op}: result must be finite, got {value!r}")


def _results_equal(a, b):
    """Strict equality for finish-idempotency: bool never equals 0/1."""
    if isinstance(a, bool) or isinstance(b, bool):
        return type(a) is type(b) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return type(a) is type(b) and a == b


# ---------------------------------------------------------------------------
# The task tree
# ---------------------------------------------------------------------------


class TaskTree:
    """An explicitly-driven structured task tree (single root scope)."""

    ROOT_ID = 0

    def __init__(self, root_name: str = "root"):
        _check_name(root_name, "TaskTree()")
        self._tasks: dict[int, _Task] = {
            self.ROOT_ID: _Task(id=self.ROOT_ID, name=root_name, parent=None, order=0)
        }
        self._next_id = 1
        self._events: list[Event] = []

    # -- introspection ----------------------------------------------------

    @property
    def events(self) -> tuple[Event, ...]:
        return tuple(self._events)

    def task_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self._tasks))

    def _get(self, task_id, op) -> _Task:
        _check_task_id(task_id, op)
        try:
            return self._tasks[task_id]
        except KeyError:
            raise UnknownTaskError(f"{op}: no task with id {task_id}") from None

    def status(self, task_id: int) -> str:
        return self._get(task_id, "status").status

    def outcome(self, task_id: int) -> dict:
        """Final/requested outcome of a task as a plain dict."""
        t = self._get(task_id, "outcome")
        return {
            "kind": t.outcome_kind,
            "result": t.result if t.outcome_kind == "finished" else None,
            "error": t.error if t.outcome_kind == "failed" else None,
            "cancel_reason": t.cancel_reason if t.outcome_kind == "cancelled" else None,
        }

    def is_done(self, task_id: int) -> bool:
        return self._get(task_id, "is_done").status == DONE

    def children(self, task_id: int) -> tuple[int, ...]:
        """Ids of the task's direct children, in creation order."""
        return tuple(self._get(task_id, "children").children)

    def parent(self, task_id: int) -> int | None:
        return self._get(task_id, "parent").parent

    def pending_reasons(self, task_id: int) -> tuple[str, ...]:
        """Why this scope has not completed yet (empty once DONE)."""
        return tuple(self._pending_reasons(self._get(task_id, "pending_reasons")))

    def exception_group(self, task_id: int) -> ExceptionGroup | None:
        """Aggregated business exceptions of the task's subtree.

        Returns ``None`` when the subtree produced no business exception
        (cancellation never counts).  Raises :class:`TaskTreeError` if the
        task has not completed yet, because the aggregate is not final.
        """
        t = self._get(task_id, "exception_group")
        if t.status != DONE:
            raise TaskTreeError(
                f"exception_group: task #{t.id} is {t.status}; "
                "the aggregate is only final once the task is done"
            )
        if not t.aggregate:
            return None
        return ExceptionGroup(
            f"business exceptions in subtree of task #{t.id} '{t.name}'",
            list(t.aggregate),
        )

    def snapshot(self) -> tuple:
        """Hashable full-state snapshot (used by tests and enumeration)."""
        return tuple(
            (
                t.id,
                t.name,
                t.parent,
                t.order,
                t.status,
                t.outcome_kind,
                repr(t.result),
                repr(t.error),
                t.cancel_reason,
                tuple(t.children),
                t.shield_depth,
                tuple(t.pending_cancels),
                t.cancel_requested,
                t.close_requested,
                tuple(repr(e) for e in t.aggregate),
            )
            for t in sorted(self._tasks.values(), key=lambda x: x.id)
        )

    def render(self) -> str:
        lines = []
        for t in sorted(self._tasks.values(), key=lambda x: x.id):
            outcome = t.outcome_kind or "-"
            extra = []
            if t.shield_depth:
                extra.append(f"shield={t.shield_depth}")
            if t.pending_cancels:
                extra.append(f"pending_cancels={len(t.pending_cancels)}")
            if t.cancel_requested:
                extra.append("cancel_requested")
            if t.close_requested:
                extra.append("close_requested")
            suffix = (" [" + ", ".join(extra) + "]") if extra else ""
            lines.append(
                f"#{t.id} '{t.name}' parent={t.parent} status={t.status} "
                f"outcome={outcome} children={t.children}{suffix}"
            )
        return "\n".join(lines)

    # -- event recording --------------------------------------------------

    def _record(self, op: str, t: _Task, status: str, note: str) -> Event:
        ev = Event(
            seq=len(self._events),
            op=op,
            task=t.id,
            status=status,
            note=note,
            task_status=t.status,
            waiting=tuple(self._pending_reasons(t)),
        )
        self._events.append(ev)
        return ev

    def _pending_reasons(self, t: _Task) -> list[str]:
        if t.status == DONE:
            return []
        reasons: list[str] = []
        if t.status == COMPLETING:
            cause = t.outcome_kind or "close"
            reasons.append(f"scope completing ({cause}); waiting for subtree exit")
        for cid in t.children:
            ct = self._tasks[cid]
            if ct.status != DONE:
                reasons.append(f"child #{ct.id} '{ct.name}' not done ({ct.status})")
        if t.pending_cancels:
            reasons.append(
                f"{len(t.pending_cancels)} cancel request(s) pending behind "
                f"shield depth {t.shield_depth}"
            )
        if not reasons and t.status == ACTIVE:
            reasons.append("task active; no completion requested yet")
        return reasons

    # -- completion machinery ----------------------------------------------

    def _finalize(self, t: _Task) -> None:
        """Mark COMPLETING tasks DONE once their subtree has exited."""
        while t is not None and t.status == COMPLETING:
            if any(self._tasks[c].status != DONE for c in t.children):
                return
            t.status = DONE
            t.aggregate = tuple(self._collect_business_exceptions(t))
            t = self._tasks[t.parent] if t.parent is not None else None

    def _collect_business_exceptions(self, t: _Task):
        """Business exceptions of the subtree, ordered by creation order."""
        pairs: list[tuple[int, BaseException]] = []
        if t.outcome_kind == "failed" and t.error is not None:
            pairs.append((t.order, t.error))
        for cid in t.children:
            ct = self._tasks[cid]
            pairs.extend((ct.order, e) for e in ct.aggregate)
        pairs.sort(key=lambda p: p[0])
        return [e for _, e in pairs]

    # -- cancellation machinery ---------------------------------------------

    def _request_cancel(self, t: _Task, reason: str) -> None:
        if t.status == DONE:
            return
        t.cancel_requested = True
        if t.shield_depth > 0:
            t.pending_cancels.append(reason)
            return
        self._deliver_cancel(t, reason)

    def _deliver_cancel(self, t: _Task, reason: str) -> None:
        if t.status == DONE:
            return
        if t.outcome_kind is None:
            t.outcome_kind = "cancelled"
            t.cancel_reason = reason
            t.status = COMPLETING
        # Cancellation propagates to the whole unfinished subtree.
        for cid in t.children:
            self._request_cancel(self._tasks[cid], reason)
        self._finalize(t)

    # -- public operations ---------------------------------------------------

    def spawn(self, parent: int, name: str) -> int:
        """Create a child task in the scope of ``parent``; returns its id."""
        _check_name(name, "spawn")
        p = self._get(parent, "spawn")
        if p.status != ACTIVE:
            raise ScopeClosedError(
                f"spawn: scope of task #{p.id} '{p.name}' is {p.status}; "
                "cannot spawn into a closing or closed scope"
            )
        child = _Task(id=self._next_id, name=name, parent=p.id, order=self._next_id)
        self._next_id += 1
        self._tasks[child.id] = child
        p.children.append(child.id)
        self._record("spawn", p, "applied", f"spawned child #{child.id} '{name}'")
        return child.id

    def finish(self, task: int, result=None) -> Event:
        """Mark a task's own work finished; waits for children, never cancels them.

        Idempotent for an equal result; a conflicting result or a
        conflicting prior outcome (fail/cancel) raises
        :class:`ResultConflictError`.
        """
        _check_result(result, "finish")
        t = self._get(task, "finish")
        if t.outcome_kind == "finished" and _results_equal(t.result, result):
            return self._record("finish", t, "no-op", "idempotent finish; same result")
        if t.outcome_kind is not None:
            raise ResultConflictError(
                f"finish: task #{t.id} '{t.name}' already has outcome "
                f"{t.outcome_kind!r}; conflicting finish(result={result!r}) rejected"
            )
        t.outcome_kind = "finished"
        t.result = result
        t.status = COMPLETING
        self._finalize(t)
        return self._record("finish", t, "applied", f"finished with result {result!r}")

    def fail(self, task: int, error: BaseException) -> Event:
        """Fail a task with a business exception.

        Requests cancellation of the task's unfinished children (the
        subtree must exit before the failure is final).  A second,
        conflicting outcome raises :class:`ResultConflictError`.
        """
        if isinstance(error, type) or not isinstance(error, BaseException):
            raise InvalidInputError(
                f"fail: error must be a BaseException instance, got "
                f"{type(error).__name__} ({error!r})"
            )
        t = self._get(task, "fail")
        if t.outcome_kind is not None:
            raise ResultConflictError(
                f"fail: task #{t.id} '{t.name}' already has outcome "
                f"{t.outcome_kind!r}; conflicting fail({error!r}) rejected"
            )
        t.outcome_kind = "failed"
        t.error = error
        t.status = COMPLETING
        for cid in t.children:
            self._request_cancel(self._tasks[cid], "parent failed")
        self._finalize(t)
        return self._record("fail", t, "applied", f"failed with {error!r}")

    def cancel(self, task: int, reason: str = "cancelled") -> Event:
        """Request cancellation of a task and its unfinished subtree.

        Inside a shield the cancellation is only recorded as pending; it
        is delivered when the outermost shield is left.  Cancelling a DONE
        task or a task that already has an outcome is a no-op.
        """
        _check_reason(reason, "cancel")
        t = self._get(task, "cancel")
        if t.status == DONE:
            return self._record("cancel", t, "no-op", "late cancel; task already done")
        if t.outcome_kind is not None and not t.children:
            return self._record(
                "cancel", t, "no-op", f"outcome {t.outcome_kind!r} already final"
            )
        first = not t.cancel_requested
        self._request_cancel(t, reason)
        if t.shield_depth > 0 and t.outcome_kind is None:
            note = f"cancel pending behind shield depth {t.shield_depth}"
        elif first:
            note = f"cancel delivered ({reason!r})"
        else:
            note = "cancel already requested"
            return self._record("cancel", t, "no-op", note)
        return self._record("cancel", t, "applied", note)

    def enter_shield(self, task: int) -> Event:
        """Enter a cancellation shield; deliveries are deferred, not lost."""
        t = self._get(task, "enter_shield")
        if t.status == DONE:
            return self._record("enter_shield", t, "no-op", "task already done")
        t.shield_depth += 1
        return self._record(
            "enter_shield", t, "applied", f"shield depth now {t.shield_depth}"
        )

    def leave_shield(self, task: int) -> Event:
        """Leave a shield; leaving the outermost one delivers pending cancels."""
        t = self._get(task, "leave_shield")
        if t.shield_depth == 0:
            raise ShieldError(
                f"leave_shield: task #{t.id} '{t.name}' is not inside a shield"
            )
        t.shield_depth -= 1
        if t.shield_depth == 0 and t.pending_cancels:
            reason = t.pending_cancels.pop(0)
            t.pending_cancels.clear()
            self._deliver_cancel(t, reason)
            return self._record(
                "leave_shield", t, "applied",
                f"outermost shield left; pending cancel delivered ({reason!r})",
            )
        return self._record(
            "leave_shield", t, "applied", f"shield depth now {t.shield_depth}"
        )

    def close_scope(self, task: int) -> Event:
        """Close a scope: cancel all unfinished children, wait for the subtree.

        Idempotent: closing an already-closing or closed scope is a no-op.
        A scope with no prior outcome completes as ``finished`` with
        result ``None``.
        """
        t = self._get(task, "close_scope")
        if t.status == DONE:
            return self._record("close_scope", t, "no-op", "scope already closed")
        if t.close_requested:
            return self._record("close_scope", t, "no-op", "scope already closing")
        t.close_requested = True
        if t.outcome_kind is None:
            t.outcome_kind = "finished"
            t.result = None
        t.status = COMPLETING
        for cid in t.children:
            self._request_cancel(self._tasks[cid], "scope closing")
        self._finalize(t)
        return self._record(
            "close_scope", t, "applied", "scope closing; unfinished children cancelled"
        )
