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
    StrategyError,
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


# --------------------------------------------------------------- 监督策略

def member_outcomes_from_log(rt, scope):
    """独立参考：直接扫描 Receipt 日志，按输入事件顺序还原 scope 直接成员
    的"首次结局落定"序列。幂等/晚到事件不产生结局，跳过；不调用 watcher
    自身的记账，用于交叉核对。"""
    st = rt.status()
    members = set(st[scope]["children"])
    order = []  # (seq, task_id, outcome)
    for r in rt.log:
        if r.op in ("finish", "fail") and r.task_id in members:
            if any("idempotent" in n or "late " in n for n in r.notes):
                continue
            kind = rt.status()[r.task_id]["outcome"]
            order.append((r.seq, r.task_id, kind))
    return order


class WatchAllStrategyTest(unittest.TestCase):
    def test_all_succeed_in_event_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rw = rt.watch(0, "all")
        self.assertEqual(rw.op, "watch")
        # 故意不按 id 顺序完成；事件顺序必须是 c, a, b
        rt.finish(3, "c")
        rt.finish(1, "a")
        self.assertEqual(rt.status()[0]["scope"], "open")  # 未达条件不清理
        r = rt.finish(2, "b")
        # 最后一个成功当场判定满足；没有未完成成员，同事件内清理确认完成
        self.assertTrue(any("satisfied" in n for n in r.notes))
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "satisfied")
        self.assertTrue(w["satisfied"])
        self.assertEqual(w["succeeded"], [3, 1, 2])  # 事件序而非 id 序
        self.assertEqual(w["failed"], [])
        self.assertEqual(w["cancelled"], [])
        self.assertEqual(w["required"], 3)
        self.assertTrue(rt.is_closed(0))
        self.assertIsNotNone(w["decided_seq"])
        self.assertIsNotNone(w["closed_seq"])
        self.assertEqual(rt.pending_reasons(0), [])
        self.assertIsNone(rt.exception_group(0))

    def test_one_failure_unsatisfied_others_cancelled(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rt.watch(0, "all")
        rt.finish(1, "a-ok")
        rt.fail(2, RuntimeError("b broke"))  # all 已不可能 -> 判定不满足
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "unsatisfied")
        self.assertFalse(w["satisfied"])
        # 取消请求已发出但 c 尚未退出：scope closing，汇总未定稿
        self.assertEqual(rt.status()[0]["scope"], "closing")
        self.assertIsNone(w["closed_seq"])
        self.assertTrue(rt.status()[3]["cancel_delivered"])
        reasons = rt.pending_reasons(0)
        self.assertTrue(any("cleanup confirmation" in x for x in reasons))
        rt.finish(3, "ignored")  # 取消已投递 -> cancelled，结果被取代
        w = rt.watcher_summary(0)
        self.assertEqual(w["succeeded"], [1])
        self.assertEqual(w["failed"], [2])
        self.assertEqual(w["cancelled"], [3])
        self.assertTrue(rt.is_closed(0))
        self.assertIsNotNone(w["closed_seq"])
        # 业务失败入异常组；cancelled 不入
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], RuntimeError)

    def test_cancel_propagates_into_member_subtree_during_cleanup(self):
        rt = TaskRuntime()
        rt.spawn(0, "m")          # 1
        rt.spawn(1, "grand")      # 2 是成员 1 的子任务，不是 scope 成员
        rt.watch(0, "all")
        rt.fail(1, ValueError("m failed"))  # 成员唯一 -> 不满足
        # 取消沿成员子树传播到 grand
        self.assertTrue(rt.status()[2]["cancel_delivered"])
        self.assertFalse(rt.is_closed(0))   # 等 grand 退出确认
        rt.finish(2, None)                  # grand -> cancelled
        self.assertTrue(rt.is_closed(0))
        w = rt.watcher_summary(0)
        self.assertEqual(w["members"], [1])
        self.assertEqual(w["failed"], [1])
        # 孙任务不是成员，不进 watcher 记账
        self.assertEqual(w["cancelled"], [])


class WatchFirstStrategyTest(unittest.TestCase):
    def test_first_success_cancels_rest(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rt.watch(0, "first")
        rt.fail(1, KeyError("a failed first"))  # 失败记录，继续等
        self.assertEqual(rt.watcher_summary(0)["state"], "pending")
        r = rt.finish(3, "c wins")              # 首个成功（按事件序）
        self.assertTrue(any("satisfied" in n for n in r.notes))
        rt.finish(2, "b ignored")               # -> cancelled
        w = rt.watcher_summary(0)
        self.assertTrue(w["satisfied"])
        self.assertEqual(w["succeeded"], [3])
        self.assertEqual(w["failed"], [1])
        self.assertEqual(w["cancelled"], [2])
        self.assertTrue(rt.is_closed(0))

    def test_all_fail_means_unsatisfied(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "first")
        rt.fail(1, RuntimeError("e1"))
        rt.fail(2, RuntimeError("e2"))  # 最后一个未决失败 -> 不可能满足
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "unsatisfied")
        self.assertFalse(w["satisfied"])
        self.assertEqual(w["failed"], [1, 2])
        self.assertTrue(rt.is_closed(0))
        self.assertEqual(len(rt.exception_group(0).exceptions), 2)


class WatchQuorumStrategyTest(unittest.TestCase):
    def test_quorum_met_on_count_success(self):
        rt = TaskRuntime()
        for _ in range(4):
            rt.spawn(0)
        rt.watch(0, "quorum", count=2)
        rt.fail(1, RuntimeError("no1"))
        rt.finish(2, "ok2")
        self.assertEqual(rt.watcher_summary(0)["state"], "pending")
        rt.finish(4, "ok4")  # 第 2 个成功 -> 满足，取消未决的 3
        w = rt.watcher_summary(0)
        self.assertTrue(w["satisfied"])
        self.assertEqual(w["succeeded"], [2, 4])
        self.assertEqual(w["failed"], [1])
        self.assertTrue(rt.status()[3]["cancel_delivered"])
        rt.finish(3, None)
        w = rt.watcher_summary(0)  # 定稿后重新取快照
        self.assertEqual(w["cancelled"], [3])
        self.assertTrue(rt.is_closed(0))

    def test_quorum_impossible_ends_unsatisfied(self):
        rt = TaskRuntime()
        for _ in range(3):
            rt.spawn(0)
        rt.watch(0, "quorum", count=2)
        rt.finish(1, "ok")
        rt.fail(2, RuntimeError("x"))
        # 1 成 1 败 1 未决：仍可能，不判定
        self.assertEqual(rt.watcher_summary(0)["state"], "pending")
        rt.fail(3, RuntimeError("y"))  # 成功上限变成 1 < 2 -> 不满足
        w = rt.watcher_summary(0)
        self.assertFalse(w["satisfied"])
        self.assertEqual(w["succeeded"], [1])
        self.assertEqual(w["failed"], [2, 3])
        self.assertTrue(rt.is_closed(0))  # 无未决成员，当场定稿

    def test_quorum_equal_to_member_count_needs_all(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "quorum", count=2)
        rt.finish(1, "a")
        # 仅 1 个成功、另一未决：不能满足也不能判死
        self.assertEqual(rt.watcher_summary(0)["state"], "pending")
        rt.finish(2, "b")
        self.assertTrue(rt.watcher_summary(0)["satisfied"])


class CleanupConfirmationTest(unittest.TestCase):
    def test_simultaneous_success_and_cleanup_failure(self):
        """题面点名场景：成功达条件的同时，清理中别的成员报业务失败。"""
        rt = TaskRuntime()
        rt.spawn(0, "winner")
        rt.spawn(0, "trap")
        rt.spawn(0, "idle")
        rt.watch(0, "first")
        rt.finish(1, "won")          # 满足；trap/idle 收到取消请求
        # 清理确认期间 trap 以业务失败退出（业务异常优先于取消）
        r = rt.fail(2, OSError("cleanup failed"))
        self.assertTrue(any("business failure takes precedence" in n
                            for n in r.notes))
        w = rt.watcher_summary(0)
        self.assertTrue(w["satisfied"])            # 结论不被清理失败推翻
        self.assertEqual(w["failed"], [2])
        self.assertEqual(w["cleanup_failures"], [2])
        # idle 未退出 -> 汇总尚不能定稿
        self.assertFalse(rt.is_closed(0))
        self.assertIsNone(w["closed_seq"])
        rt.finish(3, None)                         # idle -> cancelled
        w = rt.watcher_summary(0)
        self.assertTrue(rt.is_closed(0))
        self.assertIsNotNone(w["closed_seq"])
        self.assertEqual(w["cancelled"], [3])
        # 清理失败是真实业务异常，必须进入异常组
        group = rt.exception_group(0)
        self.assertEqual(len(group.exceptions), 1)
        self.assertIsInstance(group.exceptions[0], OSError)

    def test_failure_before_decision_is_not_counted_as_cleanup_failure(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "all")
        rt.fail(1, RuntimeError("early"))  # 决策前失败
        rt.finish(2, None)                 # 取消投递后 -> cancelled
        rt.finish(3, None)                 # -> cancelled
        w = rt.watcher_summary(0)
        self.assertFalse(w["satisfied"])
        self.assertEqual(w["failed"], [1])
        self.assertEqual(w["cleanup_failures"], [])

    def test_failure_that_triggers_unsatisfied_is_not_cleanup_failure(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "first")
        rt.fail(1, RuntimeError("first one fails"))  # 仍可能，不判定
        rt.fail(2, RuntimeError("last one fails"))   # 同事件触发不满足
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "unsatisfied")
        # 触发判定的失败发生在 scope closing 之前，不算清理失败
        self.assertEqual(w["cleanup_failures"], [])
        self.assertEqual(w["failed"], [1, 2])

    def test_shield_delays_cleanup_confirmation(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.watch(0, "first")
        rt.enter_shield(1)
        rt.finish(2, "win")  # 判定满足；对 a 的取消被 shield 拦为 pending
        self.assertEqual(rt.status()[0]["scope"], "closing")
        self.assertEqual(rt.status()[1]["pending_cancels"], 1)
        self.assertFalse(rt.is_closed(0))
        rt.leave_shield(1)
        self.assertTrue(rt.status()[1]["cancel_delivered"])
        rt.finish(1, None)  # a -> cancelled，清理确认完成
        self.assertTrue(rt.is_closed(0))
        self.assertEqual(rt.watcher_summary(0)["cancelled"], [1])

    def test_summary_settles_only_after_all_exits(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "first")
        rt.finish(1, "win")
        # 成员 2 尚未退出：结论已定、汇总未定稿
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "satisfied")
        self.assertIsNone(w["closed_seq"])
        rt.finish(2, None)
        self.assertIsNotNone(rt.watcher_summary(0)["closed_seq"])


class WatchInstallAndAbortTest(unittest.TestCase):
    def test_watch_after_some_outcomes_replays_in_event_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rt.fail(1, RuntimeError("early fail"))
        rt.finish(2, "early win")
        # 安装时已有 1 败 1 成：first 立即满足并取消未决的 c
        r = rt.watch(0, "first")
        self.assertTrue(any("end condition satisfied" in n
                            for n in r.notes))
        self.assertEqual(r.seq, rt.watcher_summary(0)["decided_seq"])
        rt.finish(3, None)
        w = rt.watcher_summary(0)
        self.assertEqual(w["succeeded"], [2])
        self.assertEqual(w["failed"], [1])
        self.assertEqual(w["cancelled"], [3])
        self.assertTrue(rt.is_closed(0))

    def test_watch_after_outcome_still_pending(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.fail(1, RuntimeError("x"))
        rt.watch(0, "first")  # 1 败 1 未决：仍有希望
        self.assertEqual(rt.watcher_summary(0)["state"], "pending")
        rt.finish(2, "ok")
        self.assertTrue(rt.watcher_summary(0)["satisfied"])

    def test_manual_close_aborts_pending_watcher(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "all")
        r = rt.close_scope(0)  # 结束条件达成前手工关闭
        self.assertTrue(any("aborted" in n for n in r.notes))
        w = rt.watcher_summary(0)
        self.assertEqual(w["state"], "aborted")
        self.assertIsNone(w["satisfied"])
        rt.finish(1, None)
        rt.finish(2, None)
        self.assertTrue(rt.is_closed(0))
        # 手工取消的成员仍如实记账，但 watcher 不给成功/失败结论
        self.assertEqual(rt.watcher_summary(0)["cancelled"], [1, 2])
        self.assertIsNone(rt.watcher_summary(0)["satisfied"])

    def test_nested_scope_independent_watcher(self):
        rt = TaskRuntime()
        rt.spawn(0, "supervisor")      # 1
        rt.spawn(1, "w1")              # 2
        rt.spawn(1, "w2")              # 3
        rt.watch(1, "first")
        self.assertIsNone(rt.watcher_summary(0))
        rt.finish(2, "w1 wins")
        rt.finish(3, None)
        self.assertTrue(rt.is_closed(1))
        self.assertFalse(rt.is_closed(0))  # 内层 watcher 不关根 scope
        rt.finish(1, "supervisor done")
        rt.close_scope(0)
        self.assertTrue(rt.is_closed(0))


class WatchValidationTest(unittest.TestCase):
    def _fresh_with_member(self):
        rt = TaskRuntime()
        rt.spawn(0, "m")
        return rt

    def test_strategy_validation(self):
        rt = self._fresh_with_member()
        with self.assertRaises(StrategyError):
            rt.watch(0, "middle")
        with self.assertRaises(StrategyError):
            rt.watch(0, 123)
        with self.assertRaises(StrategyError):
            rt.watch(0, "FIRST")  # 大小写不做静默纠正

    def test_quorum_count_validation(self):
        rt = self._fresh_with_member()
        with self.assertRaises(StrategyError):
            rt.watch(0, "quorum")
        with self.assertRaises(StrategyError):
            rt.watch(0, "quorum", count=0)
        with self.assertRaises(StrategyError):
            rt.watch(0, "quorum", count=-1)
        with self.assertRaises(StrategyError):
            rt.watch(0, "all", count=1)
        with self.assertRaises(StrategyError):
            rt.watch(0, "first", count=1)
        with self.assertRaises(TypeError):
            rt.watch(0, "quorum", count=True)
        rt.spawn(0)
        with self.assertRaises(StrategyError):
            rt.watch(0, "quorum", count=3)  # 只有 2 个成员，永不可能

    def test_watch_requires_members_open_scope_and_uniqueness(self):
        rt = TaskRuntime()
        with self.assertRaises(StrategyError):
            rt.watch(0)  # 没有成员
        rt.spawn(0)
        rt.watch(0, "first")
        with self.assertRaises(StrategyError):
            rt.watch(0, "all")  # 每个 scope 至多一个 watcher
        with self.assertRaises(ScopeClosedError):
            rt.spawn(0)        # 成员冻结后拒绝 spawn
        rt2 = TaskRuntime()
        rt2.spawn(0)
        rt2.close_scope(0)
        with self.assertRaises(StrategyError):
            rt2.watch(0, "first")  # scope 已在关闭
        with self.assertRaises(UnknownTaskError):
            rt.watch(99, "first")

    def test_watch_rejected_when_owner_ended(self):
        rt = TaskRuntime()
        rt.spawn(0, "owner")   # 1
        rt.spawn(1, "child")   # 2
        rt.finish(1, "owner done")  # draining：scope 虽 open 但属主已结局
        with self.assertRaises(StrategyError):
            rt.watch(1, "first")

    def test_invalid_watch_leaves_no_partial_state(self):
        rt = TaskRuntime()
        rt.spawn(0)
        with self.assertRaises(StrategyError):
            rt.watch(0, "quorum", count=2)  # 成员 1 个，永不可能
        # 拒绝后不得残留 watcher，scope 仍 open，spawn 仍可用
        self.assertIsNone(rt.watcher_summary(0))
        self.assertEqual(rt.status()[0]["scope"], "open")
        rt.spawn(0, "late member")
        rt.watch(0, "all")
        rt.finish(1, None)
        rt.finish(2, None)
        self.assertTrue(rt.is_closed(0))


class WatcherEnumerationTest(unittest.TestCase):
    """枚举：根下最多 2 个成员，事件深度 <= 6，覆盖三种策略。

    每个状态断言结构不变量与 watcher 解析关系（阈值、分桶互斥且按事件序、
    清理失败定义、定稿时机）；期望值从操作日志独立推导，不读 watcher 记账
    来生成期望。
    """

    MAX_EVENTS = 6

    def test_watch_traces_relations(self):
        seen = set()
        stats = {"closed": 0, "checked": 0}

        def key(rt):
            st = rt.status()
            return tuple(
                (tid, s["state"], s["outcome"], s["cancel_delivered"],
                 s["scope"],
                 (None if s["watcher"] is None
                  else (s["watcher"]["strategy"], s["watcher"]["state"],
                        tuple(s["watcher"]["succeeded"]),
                        tuple(s["watcher"]["failed"]),
                        tuple(s["watcher"]["cancelled"]),
                        tuple(s["watcher"]["cleanup_failures"]),
                        s["watcher"]["decided_seq"],
                        s["watcher"]["closed_seq"])))
                for tid, s in sorted(st.items()))

        def legal_ops(rt):
            st = rt.status()
            ntasks = len(st)
            w0 = st[0]["watcher"]
            for tid, s in sorted(st.items()):
                if tid != 0 and s["outcome"] is None:
                    yield "finish", (tid, f"r{tid}")
                    yield "fail", (tid, RuntimeError(f"e{tid}"))
                    if not s["cancel_delivered"]:
                        yield "cancel", (tid,)
                # watch 之后不再 spawn（冻结成员集，缩小枚举）
            if w0 is None and st[0]["scope"] == "open":
                if ntasks < 3:
                    yield "spawn", (0,)
                if ntasks > 1:
                    yield "watch", (0, "all")
                    yield "watch", (0, "first")
                    yield "watch", (0, "quorum", 2)
            if st[0]["scope"] == "open":
                yield "close_scope", (0,)  # 覆盖 aborted 与无 watcher 路径

        def check_relations(rt):
            st = rt.status()
            w = st[0]["watcher"]
            if w is None:
                return
            members = w["members"]
            s_set, f_set, c_set = (set(w["succeeded"]), set(w["failed"]),
                                   set(w["cancelled"]))
            # 三个分桶互斥，且只含成员
            self.assertEqual(
                len(s_set) + len(f_set) + len(c_set),
                len(s_set | f_set | c_set))
            self.assertTrue(s_set | f_set | c_set <= set(members))
            # 分桶多重集必须与日志中成员首次结局完全一致（独立来源）
            log_order = [(tid, kind)
                         for _, tid, kind in member_outcomes_from_log(rt, 0)]
            expect_pairs = (
                [(tid, "finished") for tid in w["succeeded"]]
                + [(tid, "failed") for tid in w["failed"]]
                + [(tid, "cancelled") for tid in w["cancelled"]])
            self.assertEqual(sorted(log_order), sorted(expect_pairs))
            # 每个桶内部按输入事件顺序（跨桶不要求拼接后仍保序）
            for bucket, kind in ((w["succeeded"], "finished"),
                                 (w["failed"], "failed"),
                                 (w["cancelled"], "cancelled")):
                self.assertEqual(
                    bucket,
                    [tid for tid, k in log_order if k == kind])
            n_succ, n_mem = len(s_set), len(members)
            n_unresolved = n_mem - len(s_set | f_set | c_set)
            required = w["required"]
            all_exited = all(st[m]["state"] == "exited" for m in members)
            if w["state"] == "pending":
                # 未定稿：两个结束条件都必须尚未达到
                self.assertLess(n_succ, required)
                self.assertGreaterEqual(n_succ + n_unresolved, required)
                self.assertEqual(st[0]["scope"], "open")
                self.assertIsNone(w["satisfied"])
                self.assertIsNone(w["closed_seq"])
            elif w["state"] in ("satisfied", "unsatisfied"):
                if w["state"] == "satisfied":
                    self.assertGreaterEqual(n_succ, required)
                    self.assertTrue(w["satisfied"])
                else:
                    self.assertLess(n_succ, required)
                    self.assertFalse(w["satisfied"])
                # 定稿（scope closed）当且仅当成员全部退出
                self.assertEqual(rt.is_closed(0), all_exited)
                if all_exited:
                    self.assertIsNotNone(w["closed_seq"])
                    self.assertEqual(
                        set(members), s_set | f_set | c_set)
                    stats["closed"] += 1
                else:
                    self.assertIsNone(w["closed_seq"])
                # cleanup_failures：决策事件之后才记录失败的成员
                for tid in w["cleanup_failures"]:
                    self.assertEqual(st[tid]["outcome"], "failed")
                    seq_of = next(seq for seq, m, _ in
                                  member_outcomes_from_log(rt, 0)
                                  if m == tid)
                    self.assertGreater(seq_of, w["decided_seq"])
            else:  # aborted：手工 close_scope 提前结束，不给成功/失败结论
                self.assertEqual(w["state"], "aborted")
                self.assertIsNone(w["satisfied"])
                self.assertNotEqual(st[0]["scope"], "open")
                self.assertEqual(rt.is_closed(0), all_exited)
                if all_exited:
                    stats["closed"] += 1

        def dfs(rt, depth):
            check_invariants(self, rt)
            check_relations(rt)
            stats["checked"] += 1
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
                    self.assertEqual(before, key(rt2))  # 拒绝不留部分变更
                    continue
                dfs(rt2, depth - 1)

        dfs(TaskRuntime(), self.MAX_EVENTS)
        self.assertGreater(len(seen), 100)
        self.assertGreater(stats["closed"], 0)
        self.assertGreater(stats["checked"], len(seen))


if __name__ == "__main__":
    unittest.main()
