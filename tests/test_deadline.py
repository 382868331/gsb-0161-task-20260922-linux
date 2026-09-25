"""tasktree 逻辑时钟与截止时间的单元测试。

期望值均由测试脚本手工推导；不调用被测核心去计算期望值。
覆盖：截止时刻 finish 先后顺序、恰好相等算到期、(截止时刻, 创建序) 触发
顺序、嵌套 shield、父先到期传播、已完成子任务、旧定时条目失效、root
scope 截止时间、next_deadline 逐步驱动、输入校验。
"""

import unittest

from tasktree import (
    TaskRuntime,
    TaskStateError,
)


class DeadlineBasicTest(unittest.TestCase):
    def test_finish_before_deadline_then_advance_no_cancel(self):
        # 截止时刻前 finish：到期记录失效，推进时间不产生取消
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 5)
        r = rt.finish(1, "done")
        self.assertTrue(any("invalidated" in n for n in r.notes))
        self.assertIsNone(rt.status()[1]["deadline"])
        r = rt.advance_time(10)
        self.assertTrue(any("no deadlines due" in n for n in r.notes))
        self.assertEqual(rt.outcome(1), ("finished", "done"))
        self.assertFalse(rt.status()[1]["cancel_delivered"])

    def test_advance_past_deadline_then_finish_is_cancelled(self):
        # 先推进过截止时刻：取消已投递，之后的 finish 记为 cancelled
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 5)
        r = rt.advance_time(5)
        self.assertTrue(any("expired at 5" in n for n in r.notes))
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        rt.finish(1, "too-late")
        self.assertEqual(rt.outcome(1), ("cancelled", None))

    def test_exact_equal_time_counts_as_expired(self):
        # 时间恰好相等也算到期
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 7)
        rt.advance_time(6)
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        rt.advance_time(7)  # 恰好相等
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_advance_to_same_time_is_allowed(self):
        # 非递减：推进到同一时刻合法，用于触发设为当前时刻的截止时间
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.advance_time(3)
        rt.set_deadline(1, 3)  # 等于 now，合法
        r = rt.advance_time(3)
        self.assertTrue(any("stays at 3" in n for n in r.notes))
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_expiry_order_by_deadline_then_creation_order(self):
        # 同一时刻多个到期：先按截止时刻，再按任务创建序（id 序）
        rt = TaskRuntime()
        rt.spawn(0, "t1")
        rt.spawn(0, "t2")
        rt.spawn(0, "t3")
        rt.set_deadline(3, 5)   # t3 截止 5
        rt.set_deadline(1, 5)   # t1 截止 5（创建更早，应先触发）
        rt.set_deadline(2, 3)   # t2 截止 3（时刻最早，最先触发）
        r = rt.advance_time(10)
        expired = [n for n in r.notes if "expired at" in n]
        self.assertEqual(len(expired), 3)
        self.assertIn("task#2", expired[0])   # 截止 3 最先
        self.assertIn("task#1", expired[1])   # 截止 5，创建序 1 < 3
        self.assertIn("task#3", expired[2])
        # 时间相等时 id 即创建序，堆序保证确定性
        self.assertEqual(rt.now, 10)

    def test_expiry_cancel_not_a_business_error(self):
        # 到期引发的取消不进入 exception_group；之后的业务 fail 仍进组
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.set_deadline(1, 2)
        rt.set_deadline(2, 2)
        rt.advance_time(2)
        rt.finish(1, None)                    # -> cancelled
        rt.fail(2, RuntimeError("real bug"))  # 业务异常优先于取消
        self.assertEqual(rt.outcome(1)[0], "cancelled")
        self.assertEqual(rt.outcome(2)[0], "failed")
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], RuntimeError)


class DeadlineShieldTest(unittest.TestCase):
    def test_expiry_inside_nested_shield_stays_pending(self):
        # 嵌套 shield：到期只记 pending，最外层离开才投递
        rt = TaskRuntime()
        rt.spawn(0, "s")
        rt.enter_shield(1)
        rt.enter_shield(1)
        rt.set_deadline(1, 4)
        r = rt.advance_time(4)
        st = rt.status()
        self.assertEqual(st[1]["pending_cancels"], 1)
        self.assertFalse(st[1]["cancel_delivered"])
        self.assertTrue(any("pending" in n for n in r.notes))
        rt.leave_shield(1)  # 内层离开：仍 pending
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        rt.leave_shield(1)  # 最外层离开：投递
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        rt.finish(1, None)
        self.assertEqual(rt.outcome(1)[0], "cancelled")

    def test_expiry_does_not_bypass_close_scope_waiting(self):
        # 到期只提出取消请求：shield 挡住时 scope 不能 closed，
        # 必须等既有子树退出条件满足
        rt = TaskRuntime()
        rt.spawn(0, "worker")
        rt.enter_shield(1)
        rt.set_deadline(1, 3)
        rt.close_scope(0)
        rt.advance_time(3)   # 到期 -> pending，不投递
        self.assertEqual(rt.status()[0]["scope"], "closing")
        self.assertFalse(rt.is_closed(0))
        rt.leave_shield(1)   # 投递
        rt.finish(1, None)   # -> cancelled，子树退出条件满足
        self.assertTrue(rt.is_closed(0))


class DeadlinePropagationTest(unittest.TestCase):
    def test_parent_expires_first_propagates_to_children(self):
        # 父先到期：按原路径（cancel 语义）传播到整个未结束子树
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "child")
        rt.spawn(2, "grand")
        rt.set_deadline(1, 5)
        r = rt.advance_time(5)
        st = rt.status()
        for tid in (1, 2, 3):
            self.assertTrue(st[tid]["cancel_delivered"], st)
        self.assertTrue(any("task#1" in n and "expired" in n
                            for n in r.notes))

    def test_child_does_not_copy_parent_deadline(self):
        # 子任务不直接复制父时间：子无到期记录，next_deadline 只有父
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "child")
        rt.set_deadline(1, 5)
        self.assertIsNone(rt.status()[2]["deadline"])
        self.assertEqual(rt.next_deadline(), (5, 1))
        rt.advance_time(4)   # 未到期，子无任何取消
        self.assertFalse(rt.status()[2]["cancel_delivered"])

    def test_completed_child_unaffected_by_parent_expiry(self):
        # 父到期传播时，已完成子任务保持原结局
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "fast-child")
        rt.spawn(1, "slow-child")
        rt.finish(2, "early")
        rt.set_deadline(1, 5)
        rt.advance_time(5)
        st = rt.status()
        self.assertEqual(st[2]["outcome"], "finished")
        self.assertEqual(st[2]["value"], "early")
        self.assertFalse(st[2]["cancel_delivered"])
        self.assertTrue(st[3]["cancel_delivered"])


class DeadlineGenerationTest(unittest.TestCase):
    def test_stale_timer_entry_does_not_fire(self):
        # 更新截止时间后，旧定时条目不得影响同一任务的新一代定时
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 5)
        r = rt.set_deadline(1, 20)   # 旧条目 (5, #1) 作废
        self.assertTrue(any("invalidated" in n for n in r.notes))
        r = rt.advance_time(10)      # 越过旧截止 5：不得触发
        self.assertTrue(any("no deadlines due" in n for n in r.notes))
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        rt.advance_time(20)          # 新一代到期
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_cleared_deadline_does_not_fire(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 5)
        r = rt.clear_deadline(1)
        self.assertTrue(any("cleared" in n for n in r.notes))
        rt.advance_time(9)
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        # clear 幂等
        r = rt.clear_deadline(1)
        self.assertTrue(any("idempotent" in n for n in r.notes))

    def test_completed_task_deadline_record_invalidated(self):
        # 完成后到期记录失效：next_deadline 不再返回它
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.set_deadline(1, 5)
        rt.set_deadline(2, 8)
        rt.finish(1, "done")
        self.assertEqual(rt.next_deadline(), (8, 2))
        rt.advance_time(10)
        self.assertEqual(rt.outcome(1), ("finished", "done"))
        self.assertTrue(rt.status()[2]["cancel_delivered"])
        self.assertIsNone(rt.next_deadline())


class RootDeadlineTest(unittest.TestCase):
    def test_root_scope_deadline_cancels_descendants_only(self):
        # root 可设自己的截止时间：到期 = 请求取消全部未结束后代，
        # 但不关闭 scope（不绕过 close_scope）
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.set_deadline(0, 4)
        r = rt.advance_time(4)
        self.assertTrue(any("root scope" in n for n in r.notes))
        st = rt.status()
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertTrue(st[2]["cancel_delivered"])
        self.assertEqual(st[0]["scope"], "open")  # 到期不关闭 scope
        # 仍需走 close_scope 的既有路径收尾
        rt.finish(1, None)
        rt.finish(2, None)
        self.assertFalse(rt.is_closed(0))
        r = rt.close_scope(0)
        self.assertEqual(r.scope_state, "closed")

    def test_root_deadline_invalidated_after_close(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(0, 9)
        rt.finish(1, "x")
        rt.close_scope(0)          # 子树已退出 -> 立即 closed
        self.assertTrue(rt.is_closed(0))
        self.assertIsNone(rt.status()[0]["deadline"])
        r = rt.advance_time(9)     # 旧到期记录已失效
        self.assertTrue(any("no deadlines due" in n for n in r.notes))


class NextDeadlineDrivingTest(unittest.TestCase):
    def test_simulator_step_driving(self):
        # 模拟器逐步驱动：反复取 next_deadline 并推进到该时刻
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.set_deadline(1, 6)
        rt.set_deadline(2, 3)
        fired = []
        while (nd := rt.next_deadline()) is not None:
            t, tid = nd
            rt.advance_time(t)
            fired.append((t, tid))
        self.assertEqual(fired, [(3, 2), (6, 1)])  # 按 (时刻, 创建序)
        self.assertEqual(rt.now, 6)
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        self.assertTrue(rt.status()[2]["cancel_delivered"])

    def test_receipt_carries_now(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.set_deadline(1, 2)
        r = rt.advance_time(2)
        self.assertEqual(r.now, 2)
        r = rt.finish(1, None)
        self.assertEqual(r.now, 2)   # 业务事件不推进时钟


class DeadlineValidationTest(unittest.TestCase):
    def test_time_must_be_non_decreasing(self):
        rt = TaskRuntime()
        rt.advance_time(5)
        with self.assertRaises(ValueError):
            rt.advance_time(4)
        rt.advance_time(5)  # 相等允许
        self.assertEqual(rt.now, 5)

    def test_bool_rejected_for_time_and_deadline(self):
        rt = TaskRuntime()
        rt.spawn(0)
        with self.assertRaises(TypeError):
            rt.advance_time(True)
        with self.assertRaises(TypeError):
            rt.set_deadline(1, False)
        with self.assertRaises(TypeError):
            rt.set_deadline(True, 5)

    def test_past_deadline_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.advance_time(5)
        with self.assertRaises(ValueError):
            rt.set_deadline(1, 4)
        with self.assertRaises(ValueError):
            rt.set_deadline(1, -1)

    def test_deadline_on_completed_task_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.finish(1, "done")
        with self.assertRaises(TaskStateError):
            rt.set_deadline(1, 5)

    def test_deadline_on_closed_scope_rejected(self):
        rt = TaskRuntime()
        rt.close_scope(0)
        with self.assertRaises(TaskStateError):
            rt.set_deadline(0, 5)


if __name__ == "__main__":
    unittest.main()
