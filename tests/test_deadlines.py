"""截止时间（deadline）与逻辑时钟（advance_time）的测试。

覆盖题面点名场景：
* 截止时刻 finish 的先后顺序（同刻按任务创建序）；
* 嵌套 shield：到期只提取消请求，shield 内仍记 pending，必须等子树退出
  条件满足、走 close_scope 收尾，不能绕过；
* 父先到期：取消按原路径传播，子任务不复制父截止时刻；
* 已完成子任务的到期记录失效，旧定时条目不影响同 id 的新一代定时；
* 下一待触发时刻与到期来源（next_deadline/due_deadlines/Receipt.fired）；
* 超时只产生取消，不混入业务异常聚合。

期望值均手工推导，不调用被测核心计算期望值。
"""

import copy
import unittest

from tasktree import (
    DeadlineInfo,
    NonMonotonicTimeError,
    ScopeClosedError,
    TaskRuntime,
    TaskStateError,
    TaskTreeError,
)


class ClockBasicsTest(unittest.TestCase):
    def test_clock_starts_zero_and_receipt_carries_clock(self):
        rt = TaskRuntime()
        self.assertEqual(rt.now, 0)
        r = rt.spawn(0, "a")
        self.assertEqual(r.clock, 0)
        r = rt.advance_time(7)
        self.assertEqual(rt.now, 7)
        self.assertEqual(r.clock, 7)
        self.assertEqual(r.fired, ())
        self.assertTrue(any("no deadline due" in n for n in r.notes))

    def test_start_time_argument(self):
        rt = TaskRuntime(start_time=100)
        self.assertEqual(rt.now, 100)
        rt.spawn(0)
        rt.set_deadline(1, 101)
        self.assertEqual(rt.advance_time(101).fired[0].deadline, 101)

    def test_non_decreasing_and_bool_rejected(self):
        rt = TaskRuntime()
        rt.advance_time(5)
        with self.assertRaises(NonMonotonicTimeError):
            rt.advance_time(4)
        # 相等允许（非递减），且不触发任何到期记录
        r = rt.advance_time(5)
        self.assertEqual(r.clock, 5)
        with self.assertRaises(TypeError):
            rt.advance_time(True)
        with self.assertRaises(TypeError):
            TaskRuntime(start_time=False)

    def test_deadline_must_be_strictly_future_int(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.advance_time(5)
        for bad in (5, 4):  # 必须严格晚于当前时钟
            with self.assertRaises(ValueError):
                rt.set_deadline(1, bad)
        with self.assertRaises(TypeError):
            rt.set_deadline(1, True)
        with self.assertRaises(TypeError):
            rt.set_deadline(True, 6)


class DeadlineFiringOrderTest(unittest.TestCase):
    def test_same_deadline_fires_in_creation_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "late-created-first")   # id 1
        rt.spawn(0, "early-created-second")  # id 2
        # 故意逆创建序安排，触发序仍须按创建序（id 1 先于 id 2）
        rt.set_deadline(2, 10)
        rt.set_deadline(1, 10)
        r = rt.advance_time(10)
        self.assertEqual(
            r.fired,
            (DeadlineInfo(1, 10), DeadlineInfo(2, 10)))
        self.assertEqual([d.task_id for d in r.fired], [1, 2])

    def test_distinct_deadlines_fire_in_finish_time_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "later")    # id 1, due 20
        rt.spawn(0, "sooner")   # id 2, due 10
        rt.set_deadline(1, 20)
        rt.set_deadline(2, 10)
        self.assertEqual(rt.next_deadline(), DeadlineInfo(2, 10))
        r1 = rt.advance_time(10)
        self.assertEqual([d.task_id for d in r1.fired], [2])
        self.assertEqual(rt.next_deadline(), DeadlineInfo(1, 20))
        r2 = rt.advance_time(20)
        self.assertEqual([d.task_id for d in r2.fired], [1])

    def test_equal_time_counts_as_due(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.set_deadline(1, 3)
        # 推进到 2 不触发；恰好推进到 3 触发
        self.assertEqual(rt.advance_time(2).fired, ())
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        r = rt.advance_time(3)
        self.assertEqual(r.fired, (DeadlineInfo(1, 3),))
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_jump_past_multiple_deadlines_all_fire(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.set_deadline(1, 5)
        rt.set_deadline(2, 8)
        r = rt.advance_time(20)
        self.assertEqual(
            r.fired, (DeadlineInfo(1, 5), DeadlineInfo(2, 8)))
        # 全部到期在返回前收敛，下一条业务事件看到的是收敛后状态
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        self.assertTrue(rt.status()[2]["cancel_delivered"])


class DeadlineShieldTest(unittest.TestCase):
    def test_deadline_into_nested_shield_records_pending(self):
        rt = TaskRuntime()
        rt.spawn(0, "worker")
        rt.spawn(1, "child")
        rt.enter_shield(1)
        rt.enter_shield(1)
        rt.set_deadline(1, 5)
        r = rt.advance_time(5)
        st = rt.status()
        # 到期只提取消请求：shield 内只记 pending，未投递
        self.assertEqual(r.fired, (DeadlineInfo(1, 5),))
        self.assertEqual(st[1]["pending_cancels"], 1)
        self.assertFalse(st[1]["cancel_delivered"])
        # 子任务不在自己的 shield 内：父到期沿原路径传播，取消直接投递
        self.assertTrue(st[2]["cancel_delivered"])
        # 任务仍 active：到期不能强制结束
        self.assertEqual(st[1]["state"], "active")
        rt.leave_shield(1)  # 离内层仍 pending
        self.assertEqual(rt.status()[1]["pending_cancels"], 1)
        rt.leave_shield(1)  # 离最外层才投递
        self.assertTrue(rt.status()[1]["cancel_delivered"])

    def test_expiry_cannot_bypass_close_scope_teardown(self):
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "protected")
        rt.enter_shield(2)
        rt.set_deadline(0, 5)       # 根 scope 到期 -> 请求关闭并取消后代
        rt.advance_time(5)
        st = rt.status()
        # 根 scope 已在关闭中，但被 shield 的子任务挡住，不能直接 closed
        self.assertEqual(st[0]["scope"], "closing")
        self.assertFalse(rt.is_closed(0))
        self.assertEqual(st[2]["pending_cancels"], 1)
        # 父任务自己被取消投递，但因子未退出仍不能 exited
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertNotEqual(st[1]["state"], "exited")
        # 必须走既有退出条件：离 shield -> finish -> 子树退完才 closed
        rt.leave_shield(2)
        rt.finish(2, None)          # -> cancelled
        rt.finish(1, None)          # -> cancelled
        self.assertTrue(rt.is_closed(0))
        # 超时不是业务异常
        self.assertIsNone(rt.exception_group(0))

    def test_shielded_finish_before_deadline_then_timer_invalid(self):
        rt = TaskRuntime()
        rt.spawn(0, "s")
        rt.enter_shield(1)
        rt.set_deadline(1, 10)
        rt.finish(1, "done")        # shield 内正常完成；到期记录立即失效
        r = rt.advance_time(10)
        self.assertEqual(r.fired, ())
        self.assertEqual(rt.outcome(1), ("finished", "done"))


class ParentDeadlinePropagationTest(unittest.TestCase):
    def test_child_does_not_inherit_parent_deadline(self):
        rt = TaskRuntime()
        rt.spawn(0, "parent")
        rt.spawn(1, "child")
        rt.set_deadline(1, 5)
        st = rt.status()
        self.assertEqual(st[1]["deadline"], 5)
        self.assertIsNone(st[2]["deadline"])
        self.assertEqual(
            rt.due_deadlines(), [DeadlineInfo(1, 5)])

    def test_parent_expires_first_cancels_subtree_along_normal_path(self):
        rt = TaskRuntime()
        rt.spawn(0, "parent")       # id 1, due 5
        rt.spawn(1, "child")        # id 2, 自己无截止时刻
        rt.spawn(2, "grand")        # id 3, 自己无截止时刻
        rt.set_deadline(1, 5)
        # 子任务另有更晚的自己的截止时刻：父到期不改变它
        rt.set_deadline(2, 50)
        r = rt.advance_time(5)
        self.assertEqual(r.fired, (DeadlineInfo(1, 5),))
        st = rt.status()
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertTrue(st[2]["cancel_delivered"])
        self.assertTrue(st[3]["cancel_delivered"])
        # 子的自有截止时刻仍有效
        self.assertEqual(st[2]["deadline"], 50)
        # 父不能先于子树退出
        self.assertNotEqual(st[1]["state"], "exited")
        rt.finish(3, None)
        rt.finish(2, None)
        rt.finish(1, None)
        self.assertEqual(rt.status()[1]["state"], "exited")


class DeadlineInvalidationTest(unittest.TestCase):
    def test_completed_task_deadline_is_invalidated(self):
        rt = TaskRuntime()
        rt.spawn(0, "fast")
        rt.set_deadline(1, 10)
        rt.finish(1, "early")
        self.assertIsNone(rt.status()[1]["deadline"])
        self.assertEqual(rt.due_deadlines(), [])
        r = rt.advance_time(10)
        self.assertEqual(r.fired, ())
        self.assertEqual(rt.outcome(1), ("finished", "early"))

    def test_failed_task_deadline_is_invalidated_but_failure_aggregated(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.set_deadline(1, 10)
        rt.fail(1, ValueError("real"))
        r = rt.advance_time(10)
        self.assertEqual(r.fired, ())
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], ValueError)

    def test_reset_supersedes_old_timer_generation(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.set_deadline(1, 5)
        rt.set_deadline(1, 20)       # 重设：旧条目作废
        st = rt.status()
        self.assertEqual(st[1]["deadline"], 20)
        self.assertEqual(rt.due_deadlines(), [DeadlineInfo(1, 20)])
        # 旧定时条目不影响新一代：推进到旧时刻不触发
        self.assertEqual(rt.advance_time(5).fired, ())
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        # 新时刻正常触发
        self.assertEqual(rt.advance_time(20).fired,
                         (DeadlineInfo(1, 20),))

    def test_old_entries_never_fire_after_reschedule_chain(self):
        # 多次重设产生多条堆中旧条目，全部不得误触发
        rt = TaskRuntime()
        rt.spawn(0)
        for d in (2, 4, 6, 8):
            rt.set_deadline(1, d)
        rt.advance_time(7)           # 越过 2/4/6 三个旧时刻
        self.assertFalse(rt.status()[1]["cancel_delivered"])
        r = rt.advance_time(8)       # 仅最新一代触发
        self.assertEqual(r.fired, (DeadlineInfo(1, 8),))

    def test_cancel_before_deadline_then_deadline_is_idempotent_request(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(1)
        rt.set_deadline(1, 5)
        rt.cancel(1)                 # 先手动取消并投递
        r = rt.advance_time(5)      # 到期再提一次：仅幂等备注，结局不变
        self.assertEqual(r.fired, (DeadlineInfo(1, 5),))
        self.assertTrue(any("already delivered" in n for n in r.notes))
        rt.finish(1, None)
        self.assertEqual(rt.outcome(1)[0], "cancelled")
        # 子任务也已被两路取消覆盖，且都不算业务异常
        self.assertIsNone(rt.exception_group(0))


class RootDeadlineTest(unittest.TestCase):
    def test_root_deadline_closes_scope_but_waits(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(1, "grand")
        rt.set_deadline(0, 9)
        self.assertEqual(rt.next_deadline(), DeadlineInfo(0, 9))
        r = rt.advance_time(9)
        self.assertEqual(r.fired, (DeadlineInfo(0, 9),))
        self.assertEqual(rt.status()[0]["scope"], "closing")
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        # 仍要等子树退出
        self.assertFalse(rt.is_closed(0))
        rt.finish(2, None)
        rt.finish(1, None)
        self.assertTrue(rt.is_closed(0))

    def test_root_deadline_on_empty_tree_closes_at_exact_time(self):
        rt = TaskRuntime()
        rt.set_deadline(0, 4)
        self.assertFalse(rt.is_closed(0))
        # 空树：恰好推进到截止时刻即关闭，无需任何业务事件
        r = rt.advance_time(4)
        self.assertEqual(r.fired, (DeadlineInfo(0, 4),))
        self.assertTrue(rt.is_closed(0))

    def test_arm_deadline_on_closed_scope_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.finish(1, None)
        rt.close_scope(0)
        with self.assertRaises(ScopeClosedError):
            rt.set_deadline(0, 5)
        rt2 = TaskRuntime()
        rt2.spawn(0)
        rt2.finish(1, None)
        with self.assertRaises(TaskStateError):
            rt2.set_deadline(1, 5)
        with self.assertRaises(TaskTreeError):
            rt2.set_deadline(99, 5)

    def test_manual_close_invalidates_root_deadline(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.set_deadline(0, 10)
        rt.close_scope(0)
        self.assertIsNone(rt.status()[0]["deadline"])
        r = rt.advance_time(10)
        self.assertEqual(r.fired, ())

    def test_task_deadline_independent_of_its_child_scope_state(self):
        # 非 root 任务的截止时刻管任务本体取消：关闭其子 scope 不作废它
        rt = TaskRuntime()
        rt.spawn(0, "parent")       # 1
        rt.spawn(1, "child")        # 2
        rt.close_scope(1)           # 父任务的子 scope closing，等 child 退出
        # 任务本体仍 active：仍可安排自己的截止时刻
        r = rt.set_deadline(1, 5)
        self.assertEqual(r.fired, ())
        self.assertEqual(rt.status()[1]["deadline"], 5)
        fired = rt.advance_time(5).fired
        self.assertEqual(fired, (DeadlineInfo(1, 5),))
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        rt.finish(2, None)
        rt.finish(1, None)
        self.assertTrue(rt.is_closed(1))


class DueScheduleQueryTest(unittest.TestCase):
    def test_due_deadlines_sorted_and_filtered(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.spawn(1)
        rt.set_deadline(2, 30)
        rt.set_deadline(1, 10)
        rt.set_deadline(3, 10)
        self.assertEqual(
            rt.due_deadlines(),
            [DeadlineInfo(1, 10), DeadlineInfo(3, 10), DeadlineInfo(2, 30)])
        info = rt.next_deadline()
        self.assertEqual((info.task_id, info.deadline, info.source),
                         (1, 10, "deadline"))
        # 查询是只读的：连续两次结果一致，定时不被查询消耗
        self.assertEqual(rt.due_deadlines(), rt.due_deadlines())

    def test_next_deadline_none_when_empty(self):
        rt = TaskRuntime()
        self.assertIsNone(rt.next_deadline())
        rt.spawn(0)
        rt.set_deadline(1, 5)
        rt.advance_time(5)
        self.assertIsNone(rt.next_deadline())


class BusinessErrorNotTimeoutTest(unittest.TestCase):
    def test_failure_after_expiry_is_business_error_cancel_does_not_mask(self):
        # 到期投递取消后任务以业务失败退出：failed 入组，取消不入组
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.set_deadline(1, 5)
        rt.advance_time(5)
        rt.fail(1, RuntimeError("boom"))   # 业务失败优先
        rt.finish(2, None)                 # -> cancelled
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], RuntimeError)

    def test_pure_timeouts_leave_empty_exception_group(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(1)
        rt.set_deadline(1, 3)
        rt.advance_time(3)
        rt.finish(2, None)
        rt.finish(1, None)
        self.assertIsNone(rt.exception_group(0))


class DeadlineEnumerationTest(unittest.TestCase):
    """枚举含截止时间/时钟推进的小轨迹，逐步断言不变量。

    规模：总任务 <= 3（含根），事件深度 <= 6；deadline 取自 {1, 2}，
    advance_time 目标取自 {0, 1, 2}。
    """

    MAX_EVENTS = 6
    MAX_TASKS_IN_TREE = 3

    def test_traces_with_deadlines(self):
        boom = RuntimeError("boom")
        seen = set()
        stats = {"fired_events": 0, "checked": 0}
        TIME_TARGETS = (0, 1, 2)
        DEADLINES = (1, 2)

        def key(rt):
            st = rt.status()
            return (rt.now, tuple(
                (tid, s["state"], s["outcome"], repr(s["value"]),
                 s["shield_depth"], s["pending_cancels"],
                 s["cancel_delivered"], s["scope"], s["deadline"])
                for tid, s in sorted(st.items())))

        def legal_ops(rt):
            st = rt.status()
            ntasks = len(st)
            for tid, s in sorted(st.items()):
                if tid != 0 and s["outcome"] is None:
                    yield "finish", (tid, f"r{tid}")
                    yield "fail", (tid, boom)
                    if not s["cancel_delivered"] and s["pending_cancels"] < 2:
                        yield "cancel", (tid,)
                    if s["shield_depth"] < 1:
                        yield "enter_shield", (tid,)
                    for d in DEADLINES:
                        if d > rt.now:
                            yield "set_deadline", (tid, d)
                if s["shield_depth"] > 0:
                    yield "leave_shield", (tid,)
                if s["scope"] == "open":
                    yield "close_scope", (tid,)
                    if ntasks < self.MAX_TASKS_IN_TREE:
                        yield "spawn", (tid,)
            for tg in TIME_TARGETS:
                if tg >= rt.now:
                    yield "advance_time", (tg,)

        def check(rt):
            st = rt.status()
            # 不变量1：有效截止时刻必须严格晚于当前时钟
            for tid, s in st.items():
                if s["deadline"] is not None:
                    self.assertGreater(s["deadline"], rt.now, st)
            # 不变量2：查询视图与堆中有效条目一致，且查询只读
            before = key(rt)
            due = rt.due_deadlines()
            self.assertEqual(key(rt), before)
            expect = sorted(
                ((s["deadline"], tid) for tid, s in st.items()
                 if s["deadline"] is not None))
            self.assertEqual(
                [(d.deadline, d.task_id) for d in due], expect)
            # 不变量3：pending 取消只在 shield 内；closed scope 子任务全退出
            for tid, s in st.items():
                if s["pending_cancels"] > 0:
                    self.assertGreater(s["shield_depth"], 0, st)
                if s["scope"] == "closed":
                    for c in s["children"]:
                        self.assertEqual(st[c]["state"], "exited", st)
                if s["state"] == "exited":
                    for d_id, d_s in st.items():
                        if d_s["parent"] == tid:
                            self.assertEqual(d_s["state"], "exited", st)
            # 不变量4：超时不产生业务异常（枚举中无失败结局时组必为空）
            if not any(s["outcome"] == "failed" for s in st.values()):
                self.assertIsNone(rt.exception_group(0))

        def dfs(rt, depth):
            check(rt)
            stats["checked"] += 1
            k = key(rt)
            if k in seen or depth == 0:
                return
            seen.add(k)
            for name, args in legal_ops(rt):
                rt2 = copy.deepcopy(rt)
                before = key(rt2)
                try:
                    receipt = getattr(rt2, name)(*args)
                except TaskTreeError:
                    self.assertEqual(before, key(rt2))  # 拒绝不留部分变更
                    continue
                if name == "advance_time" and receipt.fired:
                    stats["fired_events"] += 1
                dfs(rt2, depth - 1)

        dfs(TaskRuntime(), self.MAX_EVENTS)
        self.assertGreater(len(seen), 300)
        self.assertGreater(stats["fired_events"], 0)


if __name__ == "__main__":
    unittest.main()
