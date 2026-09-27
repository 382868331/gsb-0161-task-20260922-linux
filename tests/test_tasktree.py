"""tasktree 的单元测试与合法轨迹枚举验收。

期望值均由测试脚本手工推导，或从 status() 快照直接断言结构不变量；
不调用被测核心去计算期望值。
"""

import copy
import unittest

from tasktree import (
    FinishConflictError,
    ResourceLimitError,
    ScopeClosedError,
    ShieldError,
    TaskCancelled,
    TaskRuntime,
    TaskStateError,
    TaskTreeError,
    UnknownTaskError,
)


def descendants_of(status, tid):
    """由 status() 快照的 parent 指针独立计算后代集合（测试侧参考）。"""
    children = {}
    for k, s in status.items():
        children.setdefault(s["parent"], []).append(k)
    out, stack = [], list(children.get(tid, ()))
    while stack:
        n = stack.pop()
        out.append(n)
        stack.extend(children.get(n, ()))
    return out


def check_invariants(tc, rt):
    """从 status() 快照直接断言的结构不变量。"""
    st = rt.status()
    for tid, s in st.items():
        if tid == 0:
            tc.assertEqual(s["state"], "active", st)
            tc.assertIsNone(s["outcome"], st)
            continue
        if s["outcome"] is None:
            tc.assertEqual(s["state"], "active", st)
        # 核心验收：父已退出 => 全部后代已退出（不存在父完成而子仍活跃）
        if s["state"] == "exited":
            for d in descendants_of(st, tid):
                tc.assertEqual(st[d]["state"], "exited",
                               f"task#{tid} exited but descendant #{d} alive")
        # cancelled 结局必须来自已投递的取消
        if s["outcome"] == "cancelled":
            tc.assertTrue(s["cancel_delivered"], st)
        # pending 取消只可能存在于 shield 内
        if s["pending_cancels"] > 0:
            tc.assertGreater(s["shield_depth"], 0, st)
        # scope 已关闭 => 子任务全部退出
        if s["scope"] == "closed":
            for c in s["children"]:
                tc.assertEqual(st[c]["state"], "exited", st)


class BasicLifecycleTest(unittest.TestCase):
    def test_spawn_finish_close_happy_path(self):
        rt = TaskRuntime()
        r1 = rt.spawn(0, "a")
        self.assertEqual(r1.new_task_id, 1)
        r2 = rt.spawn(0, "b")
        self.assertEqual(r2.new_task_id, 2)
        rt.finish(1, "ra")
        rt.finish(2, "rb")
        r = rt.close_scope(0)
        self.assertEqual(r.scope_state, "closed")
        self.assertTrue(rt.is_closed(0))
        self.assertEqual(rt.pending_reasons(0), [])
        st = rt.status()
        self.assertEqual(st[1]["state"], "exited")
        self.assertEqual(st[1]["outcome"], "finished")
        self.assertEqual(st[1]["value"], "ra")
        self.assertIsNone(rt.exception_group(0))

    def test_close_scope_cancels_children_and_waits(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(1, "grand")
        r = rt.close_scope(0)
        # 取消已投递但任务尚未退出 -> scope 处于 closing，等待链非空
        self.assertEqual(r.scope_state, "closing")
        self.assertFalse(rt.is_closed(0))
        reasons = rt.pending_reasons(0)
        self.assertTrue(any("awaiting exit" in x for x in reasons))
        self.assertTrue(any("scope of task#0" in x for x in reasons))
        # 取消已投递后 finish -> 结局为 cancelled，结果被取消取代
        r = rt.finish(2, "too-late")
        self.assertEqual(rt.outcome(2), ("cancelled", None))
        self.assertTrue(any("superseded" in n for n in r.notes))
        rt.finish(1, "also-late")
        self.assertEqual(rt.outcome(1)[0], "cancelled")
        self.assertTrue(rt.is_closed(0))
        # 取消不是业务异常
        self.assertIsNone(rt.exception_group(0))

    def test_finish_with_active_children_drains(self):
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "child")
        rt.finish(1, "p-done")
        st = rt.status()
        # 父记录了结局但子仍活跃 -> draining，不得 exited
        self.assertEqual(st[1]["state"], "draining")
        self.assertEqual(st[2]["state"], "active")
        rt.finish(2, "c-done")
        self.assertEqual(rt.status()[1]["state"], "exited")


class ShieldTest(unittest.TestCase):
    def test_nested_shield_pending_then_delivery(self):
        rt = TaskRuntime()
        rt.spawn(0, "s")
        rt.enter_shield(1)
        rt.enter_shield(1)
        r = rt.cancel(1)
        st = rt.status()
        self.assertEqual(st[1]["pending_cancels"], 1)
        self.assertFalse(st[1]["cancel_delivered"])
        self.assertTrue(any("pending" in n for n in r.notes))
        # 离开内层：仍未投递
        rt.leave_shield(1)
        st = rt.status()
        self.assertEqual(st[1]["pending_cancels"], 1)
        self.assertFalse(st[1]["cancel_delivered"])
        # 离开最外层：投递
        r = rt.leave_shield(1)
        st = rt.status()
        self.assertEqual(st[1]["pending_cancels"], 0)
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertTrue(any("delivered" in n for n in r.notes))

    def test_shield_protects_finish_then_discards_pending(self):
        rt = TaskRuntime()
        rt.spawn(0, "s")
        rt.enter_shield(1)
        rt.cancel(1)               # 仅记 pending
        rt.finish(1, "safe")       # 取消未投递，正常完成
        r = rt.leave_shield(1)     # pending 被丢弃
        self.assertEqual(rt.outcome(1), ("finished", "safe"))
        self.assertTrue(any("discarded" in n for n in r.notes))

    def test_shield_underflow_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        with self.assertRaises(ShieldError):
            rt.leave_shield(1)


class CancelFailInteractionTest(unittest.TestCase):
    def test_cancel_and_fail_together_business_failure_wins(self):
        rt = TaskRuntime()
        rt.spawn(0, "x")
        rt.cancel(1)
        rt.fail(1, ValueError("real bug"))
        self.assertEqual(rt.outcome(1)[0], "failed")
        group = rt.exception_group(0)
        self.assertIsNotNone(group)
        # 组里恰好一个业务异常，且不是取消
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], ValueError)
        self.assertNotIsInstance(group.exceptions[0], TaskCancelled)

    def test_late_finish_after_failure_is_ignored(self):
        rt = TaskRuntime()
        rt.spawn(0, "x")
        rt.fail(1, KeyError("k"))
        r = rt.finish(1, "late")
        self.assertTrue(any("late finish ignored" in n for n in r.notes))
        self.assertEqual(rt.outcome(1)[0], "failed")

    def test_finish_idempotent_and_conflict(self):
        rt = TaskRuntime()
        rt.spawn(0, "x")
        rt.finish(1, 42)
        r = rt.finish(1, 42)  # 幂等
        self.assertTrue(any("idempotent" in n for n in r.notes))
        with self.assertRaises(FinishConflictError):
            rt.finish(1, 43)


class ScopeAndSpawnTest(unittest.TestCase):
    def test_spawn_after_close_rejected(self):
        rt = TaskRuntime()
        rt.close_scope(0)
        with self.assertRaises(ScopeClosedError):
            rt.spawn(0)

    def test_close_scope_idempotent(self):
        rt = TaskRuntime()
        rt.close_scope(0)
        r = rt.close_scope(0)
        self.assertTrue(any("idempotent" in n for n in r.notes))
        self.assertEqual(r.scope_state, "closed")

    def test_close_waits_for_shielded_descendant(self):
        rt = TaskRuntime()
        rt.spawn(0, "worker")
        rt.enter_shield(1)
        rt.close_scope(0)          # 取消被 shield 挡住，记 pending
        st = rt.status()
        self.assertEqual(st[0]["scope"], "closing")
        self.assertEqual(st[1]["pending_cancels"], 1)
        rt.leave_shield(1)         # 投递
        rt.finish(1, None)         # -> cancelled
        self.assertTrue(rt.is_closed(0))


class ExceptionGroupOrderTest(unittest.TestCase):
    def test_aggregation_in_creation_order_parent_first(self):
        rt = TaskRuntime()
        rt.spawn(0, "t1")
        rt.spawn(0, "t2")
        rt.spawn(1, "t3")          # t1 的子任务
        e1, e2, e3 = (RuntimeError("e1"), RuntimeError("e2"),
                      RuntimeError("e3"))
        # 故意乱序失败；归并必须按创建序 t1, t2, t3
        rt.fail(3, e3)
        rt.fail(2, e2)
        rt.fail(1, e1)
        group = rt.exception_group(0)
        self.assertEqual(list(group.exceptions), [e1, e2, e3])
        # 子树视图：task#1 的组只含 e1 与 e3
        sub = rt.exception_group(1)
        self.assertEqual(list(sub.exceptions), [e1, e3])

    def test_cancelled_excluded_from_group(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.cancel(1)
        rt.finish(1, None)         # -> cancelled
        rt.fail(2, OSError("disk"))
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], OSError)


class ValidationTest(unittest.TestCase):
    def test_bool_rejected_as_int(self):
        rt = TaskRuntime()
        rt.spawn(0)
        with self.assertRaises(TypeError):
            rt.finish(True, 1)
        with self.assertRaises(TypeError):
            rt.spawn(False)
        with self.assertRaises(TypeError):
            TaskRuntime(max_tasks=True)

    def test_nan_and_infinity_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                rt.finish(1, bad)
        rt.finish(1, 3.5)  # 有限浮点合法
        self.assertEqual(rt.outcome(1), ("finished", 3.5))

    def test_fail_requires_business_exception(self):
        rt = TaskRuntime()
        rt.spawn(0)
        with self.assertRaises(TypeError):
            rt.fail(1, "not an exception")
        with self.assertRaises(ValueError):
            rt.fail(1, TaskCancelled("nope"))
        with self.assertRaises(ValueError):
            rt.fail(1, KeyboardInterrupt())

    def test_unknown_task_and_root_body_ops(self):
        rt = TaskRuntime()
        with self.assertRaises(UnknownTaskError):
            rt.finish(99)
        with self.assertRaises(TaskStateError):
            rt.finish(0, None)
        with self.assertRaises(TaskStateError):
            rt.cancel(0)
        with self.assertRaises(TaskStateError):
            rt.enter_shield(0)

    def test_limits(self):
        rt = TaskRuntime(max_tasks=2)
        rt.spawn(0)
        with self.assertRaises(ResourceLimitError):
            rt.spawn(0)


class EnumerationTest(unittest.TestCase):
    """枚举小任务树的合法轨迹，逐步断言结构不变量。

    规模上限：总任务数 < 4（含根），事件深度 <= 7，shield 深度 <= 2，
    每任务 pending 取消 <= 2。用状态快照做记忆化去重。
    """

    MAX_EVENTS = 7
    MAX_TASKS_IN_TREE = 4  # 含根

    def test_legal_traces_invariants(self):
        boom = RuntimeError("boom")
        seen = set()
        stats = {"closed_trees": 0, "checked": 0}

        def depth_of(st, tid):
            d, cur = 0, tid
            while st[cur]["parent"] is not None:
                cur = st[cur]["parent"]
                d += 1
            return d

        def key(rt):
            return tuple(
                (tid, s["parent"], s["state"], s["outcome"], repr(s["value"]),
                 s["shield_depth"], s["pending_cancels"],
                 s["cancel_delivered"], s["scope"])
                for tid, s in sorted(rt.status().items()))

        def legal_ops(rt):
            st = rt.status()
            ntasks = len(st)
            for tid, s in sorted(st.items()):
                if tid != 0:
                    if s["outcome"] is None:
                        yield "finish", (tid, f"r{tid}")
                        yield "fail", (tid, boom)
                        if (not s["cancel_delivered"]
                                and s["pending_cancels"] < 2):
                            yield "cancel", (tid,)
                        if s["shield_depth"] < 2:
                            yield "enter_shield", (tid,)
                    if s["shield_depth"] > 0:
                        yield "leave_shield", (tid,)
                if s["scope"] == "open":
                    yield "close_scope", (tid,)
                    if (ntasks < self.MAX_TASKS_IN_TREE
                            and depth_of(st, tid) < 2):
                        yield "spawn", (tid,)

        def dfs(rt, depth):
            check_invariants(self, rt)
            stats["checked"] += 1
            if rt.status()[0]["scope"] == "closed":
                stats["closed_trees"] += 1
            k = key(rt)
            if k in seen or depth == 0:
                return
            seen.add(k)
            for name, args in legal_ops(rt):
                rt2 = copy.deepcopy(rt)
                before = key(rt2)
                try:
                    getattr(rt2, name)(*args)
                except TaskTreeError:
                    # 语义拒绝必须保持状态不变（无未说明的部分变更）
                    self.assertEqual(before, key(rt2))
                    continue
                dfs(rt2, depth - 1)

        dfs(TaskRuntime(), self.MAX_EVENTS)
        # 状态空间确实被充分探索，且存在整棵树关闭的轨迹
        self.assertGreater(len(seen), 500)
        self.assertGreater(stats["closed_trees"], 0)


class DeadlineTest(unittest.TestCase):
    """显式时钟与截止时间：到期发起取消（仅请求），顺序固定。"""

    def test_expiry_requests_cancel_but_does_not_complete(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=10)
        r = rt.advance_time(10)
        st = rt.status()
        # 取消已投递，但任务未确认退出前仍是 active，不是完成
        self.assertTrue(st[1]["cancel_delivered"], st)
        self.assertTrue(st[1]["deadline_fired"], st)
        self.assertEqual(st[1]["state"], "active", st)
        self.assertIsNone(st[1]["outcome"], st)
        self.assertTrue(any("awaiting exit" in p
                            for p in rt.pending_reasons(0)))
        # 到期后 finish -> cancelled（取消取代结果）
        rt.finish(1, "too-late")
        self.assertEqual(rt.outcome(1)[0], "cancelled")
        self.assertIsNone(rt.exception_group(0))

    def test_expiry_not_reached_before_deadline(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=10)
        rt.advance_time(9)
        st = rt.status()
        self.assertFalse(st[1]["cancel_delivered"], st)
        rt.finish(1, "ok")
        self.assertEqual(rt.outcome(1), ("finished", "ok"))

    def test_completion_before_expiry_at_same_instant_wins(self):
        # 固定顺序：同一时刻，显式 finish 先于随后 advance 的到期扫描
        rt = TaskRuntime(now=5)
        rt.spawn(0, "a", deadline=10)
        rt.advance_time(10)
        rt2 = TaskRuntime(now=5)
        rt2.spawn(0, "a", deadline=10)
        rt2.finish(1, "done")          # now=5 时完成
        rt2.advance_time(10)           # 随后推进到期限
        self.assertEqual(rt2.outcome(1), ("finished", "done"))
        self.assertFalse(rt2.status()[1]["cancel_delivered"])
        # 反向顺序：先推进到到期，再 finish -> cancelled
        self.assertEqual(rt.outcome(1)[0], None)
        rt.finish(1, "done")
        self.assertEqual(rt.outcome(1)[0], "cancelled")

    def test_multiple_expiries_processed_in_creation_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=5)
        rt.spawn(0, "b", deadline=5)
        rt.spawn(0, "c", deadline=7)
        r = rt.advance_time(7)
        fired = [n for n in r.notes if "deadline" in n and "reached" in n]
        self.assertEqual(len(fired), 3, r.notes)
        self.assertIn("task#1", fired[0])
        self.assertIn("task#2", fired[1])
        self.assertIn("task#3", fired[2])
        st = rt.status()
        for tid in (1, 2, 3):
            self.assertTrue(st[tid]["cancel_delivered"], st)

    def test_child_deadline_must_not_exceed_parent(self):
        rt = TaskRuntime()
        rt.spawn(0, "p", deadline=10)
        rt.spawn(1, "early", deadline=3)          # 更早：允许
        with self.assertRaises(ValueError):
            rt.spawn(1, "late", deadline=11)      # 越过父期限：拒绝
        with self.assertRaises(ValueError):
            rt.spawn(1, "none")                   # 父有期限时子必须设期限
        rt.spawn(1, "equal", deadline=10)         # 等于父期限：允许
        # 父无期限时子自由
        rt2 = TaskRuntime()
        rt2.spawn(0, "free")
        rt2.spawn(1, "any", deadline=99)

    def test_root_deadline_cancels_all_unfinished(self):
        rt = TaskRuntime(deadline=8)
        rt.spawn(0, "a", deadline=8)
        rt.spawn(0, "b", deadline=8)
        rt.spawn(1, "a1", deadline=8)
        rt.advance_time(8)
        st = rt.status()
        for tid in (1, 2, 3):
            self.assertTrue(st[tid]["cancel_delivered"], st)
            self.assertEqual(st[tid]["state"], "active", st)
        with self.assertRaises(ValueError):
            TaskRuntime(now=5, deadline=3)        # 期限早于当前时刻

    def test_shielded_expiry_stays_pending_until_unshield(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=5)
        rt.enter_shield(1)
        rt.advance_time(5)
        st = rt.status()
        self.assertFalse(st[1]["cancel_delivered"], st)
        self.assertEqual(st[1]["pending_cancels"], 1, st)
        rt.leave_shield(1)
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_deadline_fires_only_once(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=5)
        rt.advance_time(5)
        r = rt.advance_time(9)
        self.assertFalse(any("task#1" in n and "reached" in n
                             for n in r.notes), r.notes)

    def test_expiry_cancel_then_fail_keeps_exception_group(self):
        rt = TaskRuntime()
        rt.spawn(0, "a", deadline=5)
        rt.spawn(0, "b")
        rt.advance_time(5)                        # a 到期被取消
        rt.fail(1, ValueError("boom"))            # 业务异常优先于取消
        rt.finish(2, "ok")
        eg = rt.exception_group(0)
        self.assertIsNotNone(eg)
        self.assertEqual(len(eg.exceptions), 1)
        self.assertIsInstance(eg.exceptions[0], ValueError)
        self.assertEqual(rt.outcome(1)[0], "failed")

    def test_advance_time_validation(self):
        rt = TaskRuntime()
        with self.assertRaises(TypeError):
            rt.advance_time(True)
        with self.assertRaises(ValueError):
            rt.advance_time(float("nan"))
        with self.assertRaises(ValueError):
            rt.advance_time(float("inf"))
        rt.advance_time(5)
        with self.assertRaises(ValueError):
            rt.advance_time(4)                    # 时间不可倒退
        self.assertEqual(rt.now, 5.0)
        with self.assertRaises(TypeError):
            rt.spawn(0, "x", deadline=True)
        with self.assertRaises(ValueError):
            rt.spawn(0, "x", deadline=float("nan"))
        with self.assertRaises(ValueError):
            rt.spawn(0, "x", deadline=4)          # 期限早于当前时刻


if __name__ == "__main__":
    unittest.main()
