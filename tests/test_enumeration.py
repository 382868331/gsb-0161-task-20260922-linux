"""Trajectory enumeration over small task trees.

Breadth-first enumeration of legal event sequences with state
de-duplication (via ``TaskTree.snapshot``).  Branches are produced by
replaying the event history onto a fresh tree, so the enumerator only
uses the public API.  After every applied event the structural
invariants are checked directly against that public API:

* no task is DONE while any descendant is not DONE
  (equivalently: a non-DONE task implies no DONE ancestor);
* a rejected operation leaves the snapshot byte-identical (atomicity).

The enumerator distinguishes a fully explored space (``exhausted=True``)
from a budget cut (``exhausted=False``); the test asserts the space is
fully explored within the budget, so a silently truncated search fails.
"""

import unittest
from collections import deque

from tasktree import DONE, TaskTree, TaskTreeError

MAX_TASKS = 3  # root + at most 2 spawned tasks
MAX_EVENTS = 8
BUDGET = 200_000


def _candidate_ops(tree):
    """All syntactically plausible next operations for the current state."""
    ids = tree.task_ids()
    if len(ids) < MAX_TASKS:
        for parent in ids:
            yield ("spawn", parent)
    for task in ids:
        yield ("finish", task)
        yield ("fail", task)
        yield ("cancel", task)
        yield ("close_scope", task)
        yield ("enter_shield", task)
        yield ("leave_shield", task)


def _apply(tree, op, arg):
    if op == "spawn":
        tree.spawn(arg, f"t{len(tree.task_ids())}")
    elif op == "finish":
        tree.finish(arg)
    elif op == "fail":
        tree.fail(arg, ValueError("boom"))
    elif op == "cancel":
        tree.cancel(arg, "stop")
    elif op == "close_scope":
        tree.close_scope(arg)
    elif op == "enter_shield":
        tree.enter_shield(arg)
    elif op == "leave_shield":
        tree.leave_shield(arg)
    else:  # pragma: no cover - enumerator bug guard
        raise AssertionError(f"unknown op {op}")


def _replay(history):
    tree = TaskTree()
    for op, arg in history:
        _apply(tree, op, arg)
    return tree


def _descendants(tree, task):
    stack = list(tree.children(task))
    while stack:
        current = stack.pop()
        yield current
        stack.extend(tree.children(current))


def _check_invariants(tree):
    for task in tree.task_ids():
        if tree.status(task) == DONE:
            for desc in _descendants(tree, task):
                assert tree.status(desc) == DONE, (
                    f"task #{task} is DONE but descendant #{desc} is "
                    f"{tree.status(desc)}"
                )


def enumerate_trajectories(max_events=MAX_EVENTS, budget=BUDGET):
    """BFS over deduplicated states; returns exploration statistics."""
    seen = {_replay(()).snapshot()}
    queue = deque([()])
    stats = {"visited": 1, "applied": 0, "rejected": 0, "exhausted": False}
    while queue:
        if stats["visited"] >= budget:
            return stats  # budget exhausted: NOT a proof of full exploration
        history = queue.popleft()
        if len(history) >= max_events:
            continue
        for op, arg in _candidate_ops(_replay(history)):
            tree = _replay(history)
            before = tree.snapshot()
            try:
                _apply(tree, op, arg)
            except TaskTreeError:
                stats["rejected"] += 1
                assert tree.snapshot() == before, (
                    f"rejected {op}({arg}) left a partial state change"
                )
                continue
            stats["applied"] += 1
            _check_invariants(tree)
            snap = tree.snapshot()
            if snap not in seen:
                seen.add(snap)
                stats["visited"] += 1
                queue.append(history + ((op, arg),))
    stats["exhausted"] = True
    return stats


class TestTrajectoryEnumeration(unittest.TestCase):
    def test_no_parent_completes_while_a_child_is_active(self):
        stats = enumerate_trajectories()
        self.assertTrue(
            stats["exhausted"],
            f"search budget {BUDGET} exhausted before the space was fully "
            f"explored (visited={stats['visited']}); cannot claim completeness",
        )
        self.assertGreater(stats["applied"], 0)
        self.assertGreater(stats["rejected"], 0)  # illegal branches were exercised
        self.assertLess(stats["visited"], BUDGET)


if __name__ == "__main__":
    unittest.main()
