"""Unit tests for the tasktree core semantics."""

import unittest

from tasktree import (
    ACTIVE,
    COMPLETING,
    DONE,
    InvalidInputError,
    ResultConflictError,
    ScopeClosedError,
    ShieldError,
    TaskTree,
    TaskTreeError,
    UnknownTaskError,
)


class TestBasicLifecycle(unittest.TestCase):
    def test_spawn_finish_root_close(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        b = tree.spawn(0, "b")
        self.assertEqual((a, b), (1, 2))
        ev = tree.finish(a, result=42)
        self.assertEqual(ev.status, "applied")
        self.assertEqual(tree.status(a), DONE)
        tree.finish(b)
        ev = tree.close_scope(0)
        self.assertEqual(tree.status(0), DONE)
        self.assertEqual(ev.waiting, ())
        self.assertIsNone(tree.exception_group(0))

    def test_finish_waits_for_children_without_cancelling(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        child = tree.spawn(a, "child")
        ev = tree.finish(a)
        self.assertEqual(tree.status(a), COMPLETING)
        self.assertTrue(any("child" in r for r in ev.waiting))
        # child was NOT cancelled by the parent's finish
        self.assertEqual(tree.status(child), ACTIVE)
        tree.finish(child, result="ok")
        self.assertEqual(tree.status(a), DONE)

    def test_spawn_into_closing_or_closed_scope_rejected(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.spawn(a, "grandchild")
        tree.close_scope(a)  # closing: grandchild gets cancelled
        with self.assertRaises(ScopeClosedError):
            tree.spawn(a, "too-late")
        tree.finish(0)
        with self.assertRaises(ScopeClosedError):
            tree.spawn(0, "also-too-late")

    def test_unknown_task(self):
        tree = TaskTree()
        with self.assertRaises(UnknownTaskError):
            tree.finish(99)


class TestFinishIdempotencyAndConflicts(unittest.TestCase):
    def test_finish_idempotent_same_result(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a, result=7)
        ev = tree.finish(a, result=7)
        self.assertEqual(ev.status, "no-op")
        self.assertEqual(tree.outcome(a)["result"], 7)

    def test_finish_conflicting_result(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a, result=1)
        with self.assertRaises(ResultConflictError):
            tree.finish(a, result=2)

    def test_finish_bool_result_not_equal_to_int(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a, result=True)
        with self.assertRaises(ResultConflictError):
            tree.finish(a, result=1)

    def test_late_finish_after_cancel_conflicts(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.cancel(a, "shutdown")
        self.assertEqual(tree.outcome(a)["kind"], "cancelled")
        with self.assertRaises(ResultConflictError):
            tree.finish(a, result="too-late")

    def test_late_cancel_after_finish_is_noop(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a, result=1)
        ev = tree.cancel(a)
        self.assertEqual(ev.status, "no-op")
        self.assertEqual(tree.outcome(a)["kind"], "finished")

    def test_fail_after_finish_conflicts(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a)
        with self.assertRaises(ResultConflictError):
            tree.fail(a, ValueError("boom"))


class TestShields(unittest.TestCase):
    def test_cancel_pending_inside_shield_delivered_on_outermost_leave(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.enter_shield(a)
        ev = tree.cancel(a, "stop")
        self.assertIn("pending", ev.note)
        self.assertEqual(tree.status(a), ACTIVE)  # not delivered yet
        tree.enter_shield(a)
        tree.leave_shield(a)  # inner leave: still shielded
        self.assertEqual(tree.status(a), ACTIVE)
        ev = tree.leave_shield(a)  # outermost leave: delivery happens now
        self.assertIn("delivered", ev.note)
        self.assertEqual(tree.outcome(a)["kind"], "cancelled")
        self.assertEqual(tree.outcome(a)["cancel_reason"], "stop")

    def test_leave_shield_without_enter_raises(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        with self.assertRaises(ShieldError):
            tree.leave_shield(a)

    def test_shielded_child_survives_close_scope_until_unshielded(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.enter_shield(a)
        tree.close_scope(0)
        # cancel requested by close_scope is pending behind the shield
        self.assertEqual(tree.status(a), ACTIVE)
        self.assertEqual(tree.status(0), COMPLETING)
        self.assertTrue(any("pending" in r for r in tree.pending_reasons(a)))
        tree.leave_shield(a)
        self.assertEqual(tree.status(a), DONE)
        self.assertEqual(tree.status(0), DONE)


class TestCancelAndFailInteraction(unittest.TestCase):
    def test_cancel_then_fail_conflicts(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.cancel(a)
        with self.assertRaises(ResultConflictError):
            tree.fail(a, ValueError("boom"))

    def test_fail_then_cancel_is_noop_for_leaf(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.fail(a, ValueError("boom"))
        ev = tree.cancel(a)
        self.assertEqual(ev.status, "no-op")
        self.assertEqual(tree.outcome(a)["kind"], "failed")

    def test_fail_cancels_unfinished_children(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        child = tree.spawn(a, "child")
        tree.fail(a, ValueError("boom"))
        self.assertEqual(tree.outcome(child)["kind"], "cancelled")
        self.assertEqual(tree.status(a), DONE)

    def test_cancel_propagates_to_subtree(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        b = tree.spawn(a, "b")
        c = tree.spawn(b, "c")
        tree.cancel(a, "abort")
        for t in (a, b, c):
            self.assertEqual(tree.outcome(t)["kind"], "cancelled")
            self.assertEqual(tree.status(t), DONE)


class TestExceptionAggregation(unittest.TestCase):
    def test_aggregate_ordered_by_creation(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        b = tree.spawn(0, "b")
        tree.fail(a, ValueError("first-created"))
        tree.fail(b, KeyError("second-created"))
        tree.fail(0, RuntimeError("root-fails-last-but-created-first"))
        group = tree.exception_group(0)
        self.assertIsInstance(group, ExceptionGroup)
        kinds = [type(e) for e in group.exceptions]
        # root (order 0) precedes a (order 1) precedes b (order 2)
        self.assertEqual(kinds, [RuntimeError, ValueError, KeyError])

    def test_cancellation_is_not_a_business_exception(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.spawn(0, "b")
        tree.cancel(a, "not an error")
        tree.close_scope(0)  # cancels b as well
        self.assertIsNone(tree.exception_group(0))

    def test_nested_failures_flattened_in_creation_order(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        child = tree.spawn(a, "child")
        tree.fail(child, ValueError("child"))
        tree.fail(a, RuntimeError("parent"))
        tree.close_scope(0)
        group = tree.exception_group(0)
        self.assertEqual(
            [type(e) for e in group.exceptions], [RuntimeError, ValueError]
        )

    def test_exception_group_requires_done(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.spawn(a, "child")
        tree.fail(a, ValueError("boom"))  # child cancelled but tree root active
        with self.assertRaises(TaskTreeError):
            tree.exception_group(0)


class TestInputValidation(unittest.TestCase):
    def setUp(self):
        self.tree = TaskTree()
        self.a = self.tree.spawn(0, "a")

    def test_bool_task_id_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.finish(True)

    def test_non_int_task_id_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.cancel("a")

    def test_nan_result_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.finish(self.a, result=float("nan"))

    def test_infinity_result_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.finish(self.a, result=float("inf"))
        with self.assertRaises(InvalidInputError):
            self.tree.finish(self.a, result=complex(0, float("inf")))

    def test_bad_name_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.spawn(0, "")
        with self.assertRaises(InvalidInputError):
            self.tree.spawn(0, 123)

    def test_fail_requires_exception_instance(self):
        with self.assertRaises(InvalidInputError):
            self.tree.fail(self.a, ValueError)  # class, not instance
        with self.assertRaises(InvalidInputError):
            self.tree.fail(self.a, "boom")

    def test_non_str_reason_rejected(self):
        with self.assertRaises(InvalidInputError):
            self.tree.cancel(self.a, reason=5)


class TestAtomicity(unittest.TestCase):
    def test_rejected_operations_leave_state_unchanged(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.finish(a, result=1)
        before = tree.snapshot()
        for bad_call in (
            lambda: tree.finish(a, result=2),
            lambda: tree.fail(a, ValueError("x")),
            lambda: tree.spawn(a, "child-of-closed"),
            lambda: tree.leave_shield(a),
            lambda: tree.finish(True),
            lambda: tree.finish(999),
        ):
            with self.assertRaises(TaskTreeError):
                bad_call()
            self.assertEqual(tree.snapshot(), before)


class TestEventLog(unittest.TestCase):
    def test_events_are_ordered_and_carry_waiting_reasons(self):
        tree = TaskTree()
        a = tree.spawn(0, "a")
        tree.spawn(a, "child")
        tree.close_scope(a)
        events = tree.events
        self.assertEqual([e.seq for e in events], list(range(len(events))))
        close_ev = events[-1]
        self.assertEqual(close_ev.op, "close_scope")
        self.assertEqual(close_ev.task, a)
        # the cancelled child completes immediately, so the scope is done
        self.assertEqual(close_ev.task_status, DONE)


if __name__ == "__main__":
    unittest.main()
