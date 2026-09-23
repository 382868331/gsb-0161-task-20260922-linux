"""watch（成功策略：全部成功/首个成功/指定数量成功）的单元测试与枚举验收。

期望值均由测试脚本手工推导（按事件序号计算事件序），不调用被测核心生成
期望值；枚举测试中对结束条件、阶段分流、汇总一致性全部用 status()/watches()
快照在测试侧重算。
"""

import copy
import unittest

from tasktree import (
    TaskRuntime,
    TaskTreeError,
    UnknownTaskError,
    UnknownWatchError,
    WatchStateError,
)


def snapshot(rt):
    """独立于被测推进逻辑的全状态快照（用于原子性比对）。"""
    return copy.deepcopy((rt.status(), rt.watches(), rt.log, rt._seq))


def outcome_counts(rt, members):
    st = rt.status()
    return {
        "finished": [m for m in members if st[m]["outcome"] == "finished"],
        "failed": [m for m in members if st[m]["outcome"] == "failed"],
        "cancelled": [m for m in members if st[m]["outcome"] == "cancelled"],
        "open": [m for m in members if st[m]["outcome"] is None],
    }


def check_done_invariants(tc, rt, wid):
    """done 后用快照在测试侧重算汇总必须满足的全部关系。"""
    w = rt.watches()[wid]
    res = rt.watch_result(wid)
    st = rt.status()
    tc.assertEqual(w["phase"], "done")
    members = w["members"]
    # 全部成员退出（清理确认）
    tc.assertTrue(all(st[m]["state"] == "exited" for m in members))
    counts = outcome_counts(rt, members)
    # 汇总三类集合与快照一致（顺序在下面按结局事件序单独核对）
    tc.assertEqual({s["task_id"] for s in res["successes"]},
                   set(counts["finished"]))
    tc.assertEqual({f["task_id"] for f in res["failures"]},
                   set(counts["failed"]))
    tc.assertEqual(set(res["cancelled"]), set(counts["cancelled"]))
    # successes / failures / cancelled 均按结局事件序；trigger_seq <= done_seq
    fin_seq = [(m, st[m]["outcome_seq"]) for m in counts["finished"]]
    tc.assertEqual([s["task_id"] for s in res["successes"]],
                   [m for m, _ in sorted(fin_seq, key=lambda x: (x[1], x[0]))])
    fail_seq = [(m, st[m]["outcome_seq"]) for m in counts["failed"]]
    tc.assertEqual([f["task_id"] for f in res["failures"]],
                   [m for m, _ in sorted(fail_seq, key=lambda x: (x[1], x[0]))])
    can_seq = [(m, st[m]["outcome_seq"]) for m in counts["cancelled"]]
    tc.assertEqual(res["cancelled"],
                   [m for m, _ in sorted(can_seq, key=lambda x: (x[1], x[0]))])
    tc.assertLessEqual(res["trigger_seq"], res["done_seq"])
    # 失败阶段分流：结局序号 <= trigger_seq 为判定阶段，否则清理失败
    for f in res["failures"]:
        m = f["task_id"]
        tc.assertEqual(
            f["phase"],
            "trigger" if st[m]["outcome_seq"] <= res["trigger_seq"]
            else "cleanup")
    cleanup_fails = [f for f in res["failures"] if f["phase"] == "cleanup"]
    # status 与 goal_met/清理失败 的解析关系
    if not res["goal_met"]:
        tc.assertEqual(res["status"], "unsatisfiable")
    elif cleanup_fails:
        tc.assertEqual(res["status"], "satisfied_with_cleanup_errors")
    else:
        tc.assertEqual(res["status"], "satisfied")
    # winners 恒为成功成员子集
    tc.assertTrue(set(res["winners"]) <= set(counts["finished"]))
    return res


# --------------------------------------------------------------- all 策略

class StrategyAllTest(unittest.TestCase):
    def test_all_satisfied_in_event_order(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rw = rt.watch(0, "all")
        wid = rw.watch_id
        self.assertEqual(rw.watch_phase, "watching")
        rt.finish(1, "ra")
        rt.finish(3, "rc")           # 故意乱序：3 在 2 之前成功
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        r = rt.finish(2, "rb")      # 最后一个成员成功 -> 同事件清理并完成
        self.assertIn((wid, "cleaning"), r.watch_updates)
        self.assertIn((wid, "done"), r.watch_updates)
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "satisfied")
        self.assertTrue(res["goal_met"])
        self.assertEqual(res["members"], [1, 2, 3])
        self.assertEqual([s["task_id"] for s in res["successes"]],
                         [1, 3, 2])                    # 事件序，非创建序
        self.assertEqual([s["value"] for s in res["successes"]],
                         ["ra", "rc", "rb"])
        self.assertEqual(res["winners"], [1, 3, 2])
        self.assertEqual(res["failures"], [])
        self.assertEqual(res["cancelled"], [])
        self.assertEqual(res["trigger_seq"], r.seq)
        self.assertEqual(res["done_seq"], r.seq)

    def test_all_failure_triggers_cancel_with_cleanup_failure(self):
        # 手算：成员 1,2,3。事件序 fail(2) -> 触发(不可能) -> 取消 1,3
        # fail(1)（清理中业务失败优先于取消）-> 清理失败
        # finish(3)（取消已投递）-> cancelled
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        wid = rt.watch(0, "all").watch_id
        e2 = ValueError("boom-b")
        rt.fail(2, e2)
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        st = rt.status()
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertTrue(st[3]["cancel_delivered"])
        e1 = RuntimeError("boom-a-during-cleanup")
        rt.fail(1, e1)
        r = rt.finish(3, "late")
        self.assertIn((wid, "done"), r.watch_updates)
        self.assertEqual(rt.outcome(3), ("cancelled", None))
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertFalse(res["goal_met"])
        self.assertEqual([f["task_id"] for f in res["failures"]], [2, 1])
        by_task = {f["task_id"]: f for f in res["failures"]}
        self.assertEqual(by_task[2]["phase"], "trigger")
        self.assertEqual(by_task[1]["phase"], "cleanup")
        self.assertIs(by_task[2]["error"], e2)
        self.assertIs(by_task[1]["error"], e1)
        self.assertEqual(res["cancelled"], [3])
        self.assertEqual(res["successes"], [])
        self.assertEqual(res["winners"], [])
        # 取消不是业务异常：异常组只含两个业务失败（按创建序 1,2）
        group = rt.exception_group(0)
        self.assertEqual(list(group.exceptions), [e1, e2])

    def test_all_cleanup_waits_on_member_descendants(self):
        # 成员 1 有子任务 2；触发后必须等整个成员子树退出才汇总
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(1, "a-child")
        rt.spawn(0, "b")
        wid = rt.watch(0, "all").watch_id
        rt.fail(3, ValueError("b"))      # 触发：1/2 子树被取消
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        with self.assertRaises(WatchStateError):
            rt.watch_result(wid)        # 清理未确认，拒绝汇总
        rt.finish(1, None)              # -> cancelled，draining 等子任务
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        self.assertTrue(any("watch#1" in x and "cleaning" in x
                            for x in rt.pending_reasons(0)))
        rt.finish(2, None)              # 子任务退出 -> 成员 1 exited -> done
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["cancelled"], [1])       # 只统计成员本身
        # 成员的子任务同样收到取消并以 cancelled 退出（清理覆盖整棵成员子树）
        self.assertEqual(rt.outcome(2)[0], "cancelled")
        self.assertEqual([f["task_id"] for f in res["failures"]], [3])


# ------------------------------------------------------------- first 策略

class StrategyFirstTest(unittest.TestCase):
    def test_first_success_wins_by_event_order(self):
        # 创建序 1,2,3，但事件序中 3 最先成功 -> winner=3
        rt = TaskRuntime()
        for name in ("a", "b", "c"):
            rt.spawn(0, name)
        wid = rt.watch(0, "first").watch_id
        rt.finish(3, "c-first")
        st = rt.status()
        self.assertTrue(st[1]["cancel_delivered"])
        self.assertTrue(st[2]["cancel_delivered"])
        rt.fail(1, RuntimeError("a-ignored-after"))  # 清理中失败 -> cleanup
        rt.finish(2, "late")                          # -> cancelled
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"],
                         "satisfied_with_cleanup_errors")
        self.assertTrue(res["goal_met"])
        self.assertEqual(res["winners"], [3])
        self.assertEqual([s["task_id"] for s in res["successes"]], [3])
        self.assertEqual([f["task_id"] for f in res["failures"]], [1])
        self.assertEqual(res["failures"][0]["phase"], "cleanup")
        self.assertEqual(res["cancelled"], [2])

    def test_first_no_success_is_unsatisfiable(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "first").watch_id
        rt.fail(1, ValueError("a"))
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        r = rt.fail(2, KeyError("b"))   # 全部终结且无成功
        self.assertIn((wid, "done"), r.watch_updates)
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual([f["task_id"] for f in res["failures"]], [1, 2])
        self.assertTrue(all(f["phase"] == "trigger" for f in res["failures"]))
        self.assertEqual(res["winners"], [])

    def test_first_failure_then_success_still_satisfiable(self):
        # 先失败不触发；后续成功达标，失败记为判定阶段失败
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "first").watch_id
        rt.fail(1, ValueError("a"))
        r = rt.finish(2, "b")
        self.assertIn((wid, "done"), r.watch_updates)
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "satisfied")
        self.assertEqual(res["winners"], [2])
        self.assertEqual([f["task_id"] for f in res["failures"]], [1])
        self.assertEqual(res["failures"][0]["phase"], "trigger")


# --------------------------------------------------------- n_succeed 策略

class StrategyNSucceedTest(unittest.TestCase):
    def test_n_reaches_quota_then_cancels_rest(self):
        rt = TaskRuntime()
        for _ in range(4):
            rt.spawn(0)
        wid = rt.watch(0, "n_succeed", need=2).watch_id
        rt.finish(2, "r2")
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        rt.finish(4, "r4")              # 第 2 个成功 -> 触发
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        rt.finish(1, "late")            # cancelled
        rt.finish(3, "late")            # cancelled
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "satisfied")
        self.assertEqual(res["winners"], [2, 4])
        self.assertEqual(res["cancelled"], [1, 3])

    def test_n_becomes_impossible(self):
        # need=3，4 个成员：fail(1)、finish(2) 后 finished=1、open=2 仍可能；
        # fail(3) 后 finished(1)+open(1) < 3 -> 不可能，取消成员 4
        rt = TaskRuntime()
        for _ in range(4):
            rt.spawn(0)
        wid = rt.watch(0, "n_succeed", need=3).watch_id
        rt.fail(1, ValueError("x"))
        rt.finish(2, "ok")
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        rt.fail(3, ValueError("y"))
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        rt.finish(4, "late")
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual([s["task_id"] for s in res["successes"]], [2])
        self.assertEqual(res["cancelled"], [4])
        self.assertEqual(sorted(f["task_id"] for f in res["failures"]), [1, 3])

    def test_n_boundary_need_one(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "n_succeed", need=1).watch_id
        rt.fail(1, ValueError("a"))     # 0 成功 + 1 未完成 >= 1，仍可能
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        rt.finish(2, "b")               # 达标，无未完成成员，同事件 done
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "satisfied")
        self.assertEqual(res["winners"], [2])

    def test_n_need_equals_member_count_matches_all_semantics(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "n_succeed", need=2).watch_id
        rt.fail(1, ValueError("a"))     # 0 + 1 < 2 -> 不可能
        rt.finish(2, "late")
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual(res["cancelled"], [2])

    def test_n_judgement_strictly_event_ordered(self):
        # need=2：失败先发生不影响计数，之后两个成功仍达标
        rt = TaskRuntime()
        for _ in range(3):
            rt.spawn(0)
        wid = rt.watch(0, "n_succeed", need=2).watch_id
        rt.fail(1, ValueError("early"))
        rt.finish(2, "r2")
        rt.finish(3, "r3")
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "satisfied")
        self.assertEqual(res["winners"], [2, 3])
        self.assertEqual(res["failures"][0]["phase"], "trigger")


# --------------------------------------------------- 补防/回填与清理屏蔽

class WatchBackfillAndShieldTest(unittest.TestCase):
    def test_watch_armed_after_outcomes_judges_at_once(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        rt.finish(1, "ra")
        rt.finish(2, "rb")
        rt.fail(3, ValueError("c"))
        rw = rt.watch(0, "all")         # 已有失败 -> 立即判定不可能并完成
        self.assertEqual(rw.watch_phase, "done")
        self.assertIn((rw.watch_id, "cleaning"), rw.watch_updates)
        self.assertIn((rw.watch_id, "done"), rw.watch_updates)
        res = rt.watch_result(rw.watch_id)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual([s["task_id"] for s in res["successes"]], [1, 2])

        rt2 = TaskRuntime()
        rt2.spawn(0, "a")
        rt2.spawn(0, "b")
        rt2.finish(1, "ra")
        wid = rt2.watch(0, "first").watch_id   # 已有成功 -> 立即达标
        self.assertEqual(rt2.watches()[wid]["phase"], "cleaning")
        rt2.finish(2, "late")                  # 取消后退出
        res = rt2.watch_result(wid)
        self.assertEqual(res["winners"], [1])
        self.assertEqual(res["cancelled"], [2])

    def test_shield_delays_cleanup_until_left(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "first").watch_id
        rt.enter_shield(1)
        rt.finish(2, "b-win")            # 触发；1 的取消被 shield 挡为 pending
        w = rt.watches()[wid]
        self.assertEqual(w["phase"], "cleaning")
        self.assertEqual(w["open"], 1)
        with self.assertRaises(WatchStateError):
            rt.watch_result(wid)
        st = rt.status()
        self.assertEqual(st[1]["pending_cancels"], 1)
        self.assertFalse(st[1]["cancel_delivered"])
        rt.leave_shield(1)               # 投递
        rt.finish(1, "late")             # -> cancelled -> done
        check_done_invariants(self, rt, wid)

    def test_cleanup_cancel_covers_grandchildren_spawned_under_member(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(1, "a-child")           # watching 期间允许在成员 scope 内 spawn
        rt.spawn(0, "b")
        wid = rt.watch(0, "first").watch_id
        rt.finish(3, "b")                # 触发：取消 1 与 2 的整棵子树
        self.assertTrue(rt.status()[2]["cancel_delivered"])
        rt.finish(2, "x")
        rt.finish(1, "y")
        check_done_invariants(self, rt, wid)


# ---------------------------------------------------- 与 close_scope 的组合

class WatchAndScopeClosingTest(unittest.TestCase):
    def test_watch_done_and_scope_closed_in_same_event(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "first").watch_id
        rt.enter_shield(1)
        rt.finish(2, "b")               # watch cleaning，1 pending
        rc = rt.close_scope(0)          # scope closing，对 1 再记一次 pending
        self.assertEqual(rc.scope_state, "closing")
        self.assertEqual(rt.status()[1]["pending_cancels"], 2)
        rt.leave_shield(1)              # 取消投递
        rt.finish(1, "late")            # cancelled：watch done 且 scope closed
        self.assertTrue(rt.is_closed(0))
        check_done_invariants(self, rt, wid)
        self.assertEqual(rt.pending_reasons(0), [])

    def test_spawn_blocked_during_watch_allowed_after_done(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "all").watch_id
        with self.assertRaises(WatchStateError):
            rt.spawn(0, "c")             # watching 期间成员集冻结
        # 成员自己的 scope 不受影响
        rt.spawn(1, "a-child")
        rt.finish(3, "child")
        rt.finish(1, "ra")
        rt.finish(2, "rb")              # done
        check_done_invariants(self, rt, wid)
        new_id = rt.spawn(0, "after").new_task_id   # done 后根 scope 可再 spawn
        self.assertEqual(new_id, 4)

    def test_rewatch_allowed_after_done_covers_current_children(self):
        # 再布防的成员集为当时全部直接子任务（含已退出者，其历史结局按
        # 历史事件序号参与判定）
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid1 = rt.watch(0, "all").watch_id
        rt.finish(1, "ra")
        rt.finish(2, "rb")
        rt.spawn(0, "c")
        rt.spawn(0, "c2")
        wid2 = rt.watch(0, "first").watch_id
        self.assertEqual(wid2, wid1 + 1)
        self.assertEqual(rt.watches()[wid2]["members"], [1, 2, 3, 4])
        # 补防时已有成功 -> 立即达标，取消尚未退出的 3、4
        self.assertEqual(rt.watches()[wid2]["phase"], "cleaning")
        rt.finish(3, "late")
        rt.finish(4, "late")
        res = check_done_invariants(self, rt, wid2)
        self.assertEqual(res["winners"], [1])
        self.assertEqual(res["cancelled"], [3, 4])


# ------------------------------------------------------------- 空成员与外部取消

class WatchEmptyAndExternalCancelTest(unittest.TestCase):
    def test_empty_membership_judges_immediately(self):
        # 空 scope 上布防：成员集为空，立即判定
        rt = TaskRuntime()
        rw = rt.watch(0, "all")          # 全部 0 个成功 -> vacuously satisfied
        self.assertEqual(rw.watch_phase, "done")
        res = rt.watch_result(rw.watch_id)
        self.assertEqual(res["status"], "satisfied")
        self.assertTrue(res["goal_met"])
        self.assertEqual(res["members"], [])

        rt2 = TaskRuntime()
        wid = rt2.watch(0, "first").watch_id   # 无成员可成功 -> 不可能
        res = rt2.watch_result(wid)
        self.assertEqual(res["status"], "unsatisfiable")

        rt3 = TaskRuntime()
        wid = rt3.watch(0, "n_succeed", need=1).watch_id
        res = rt3.watch_result(wid)
        self.assertEqual(res["status"], "unsatisfiable")

    def test_external_cancel_during_watching_counts_as_non_success(self):
        # watching 期间外部手动 cancel(1)，1 以 finish 退出 -> cancelled：
        # cancelled 既非成功也非失败，按事件序参与计数
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        wid = rt.watch(0, "all").watch_id
        rt.cancel(1)
        rt.finish(1, None)                # -> cancelled；all 立即不可能并触发清理
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        rt.fail(2, ValueError("b"))       # 清理中业务失败 -> 清理失败
        rt.finish(3, "c")                 # -> cancelled
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual(res["cancelled"], [1, 3])
        self.assertEqual([f["task_id"] for f in res["failures"]], [2])
        self.assertEqual(res["failures"][0]["phase"], "cleanup")

    def test_external_cancel_then_business_failure_in_cleanup(self):
        # first：成员 3 先成功触发；成员 1 已被外部 cancel 且在 shield 内，
        # 离开后以业务失败退出 -> 清理失败
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rt.spawn(0, "c")
        wid = rt.watch(0, "first").watch_id
        rt.enter_shield(1)
        rt.cancel(1)                      # 外部取消，shield 内仅 pending
        rt.finish(3, "c-wins")            # 触发：w 清理也请求取消（记 pending）
        rt.leave_shield(1)
        rt.fail(1, RuntimeError("a"))     # 清理中业务失败优先
        rt.finish(2, "late")
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"],
                         "satisfied_with_cleanup_errors")
        self.assertEqual(res["winners"], [3])
        self.assertEqual(res["failures"][0]["phase"], "cleanup")
        self.assertEqual(res["cancelled"], [2])

    def test_n_need_greater_than_members_is_unsatisfiable(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        rw = rt.watch(0, "n_succeed", need=3)   # 2 成员要 3 成功 -> 立即触发
        self.assertEqual(rw.watch_phase, "cleaning")
        self.assertFalse(rt.watches()[rw.watch_id]["goal_met"])
        st = rt.status()
        self.assertTrue(st[1]["cancel_delivered"] and st[2]["cancel_delivered"])
        r1 = rt.finish(1, "late")               # -> cancelled
        r2 = rt.finish(2, "late")               # -> cancelled，清理确认
        self.assertIn((rw.watch_id, "done"), r2.watch_updates)
        res = rt.watch_result(rw.watch_id)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertFalse(res["goal_met"])
        self.assertEqual(res["cancelled"], [1, 2])

    def test_delivered_external_cancel_makes_goal_impossible(self):
        # 外部 cancel 投递（非 shield pending）后该成员注定无法 finished：
        # all / n_succeed 立即不可达；first 只要还有别人可能成功就不触发
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "n_succeed", need=2).watch_id
        rt.cancel(1)                      # 可能成功数 1 < 2 -> 触发
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        self.assertTrue(rt.status()[2]["cancel_delivered"])
        rt.finish(1, None)
        rt.finish(2, None)
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual(res["cancelled"], [1, 2])

        rt2 = TaskRuntime()
        rt2.spawn(0, "a")
        rt2.spawn(0, "b")
        wid = rt2.watch(0, "first").watch_id
        rt2.cancel(1)                     # 1 注定失败，2 仍可能成功 -> 不触发
        self.assertEqual(rt2.watches()[wid]["phase"], "watching")
        r = rt2.finish(2, "b")            # 2 成功 -> 达标
        self.assertIn((wid, "cleaning"), r.watch_updates)
        rt2.finish(1, None)
        res = check_done_invariants(self, rt2, wid)
        self.assertEqual(res["winners"], [2])
        self.assertEqual(res["cancelled"], [1])

    def test_shield_pending_cancel_not_yet_fatal(self):
        # shield 内取消仅 pending：成员仍可正常 finish，判定不提前结束
        rt = TaskRuntime()
        rt.spawn(0, "a")
        rt.spawn(0, "b")
        wid = rt.watch(0, "all").watch_id
        rt.enter_shield(1)
        rt.cancel(1)
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        rt.leave_shield(1)                # 投递 -> all 立即不可能
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        rt.finish(1, None)
        rt.finish(2, None)
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual(res["cancelled"], [1, 2])

    def test_close_scope_during_watching_triggers_impossibility(self):
        # 父 scope 在 watching 期间关闭：取消一旦投递，first 无可能成功者
        rt = TaskRuntime()
        rt.spawn(0, "p")      # 1
        rt.spawn(1, "a")      # 2
        rt.spawn(1, "b")      # 3
        wid = rt.watch(1, "first").watch_id
        rt.close_scope(0)                    # 取消整棵树并投递
        self.assertEqual(rt.watches()[wid]["phase"], "cleaning")
        rt.fail(2, ValueError("a"))          # 清理失败
        rt.finish(3, "late")                 # -> cancelled -> done
        res = check_done_invariants(self, rt, wid)
        self.assertEqual(res["status"], "unsatisfiable")
        self.assertEqual(res["failures"][0]["phase"], "cleanup")
        self.assertEqual(res["cancelled"], [3])
        rt.finish(1, "p-late")               # p 退出，整棵树关闭
        self.assertTrue(rt.is_closed(0))
        self.assertEqual(rt.pending_reasons(0), [])

    def test_watch_on_nested_scope_only_covers_direct_children(self):
        rt = TaskRuntime()
        rt.spawn(0, "parent")      # 1
        rt.spawn(1, "inner-1")     # 2（parent scope 的直接子任务）
        rt.spawn(1, "inner-2")     # 3
        rt.spawn(0, "sibling")     # 4（根 scope 的直接子任务）
        wid = rt.watch(1, "first").watch_id
        self.assertEqual(rt.watches()[wid]["members"], [2, 3])
        rt.finish(4, "sibling")    # 不影响 parent scope 上的 watch
        self.assertEqual(rt.watches()[wid]["phase"], "watching")
        rt.finish(3, "inner-2 wins")
        r = rt.finish(2, "late")   # -> cancelled（watch 清理）
        self.assertIn((wid, "done"), r.watch_updates)
        res = rt.watch_result(wid)
        self.assertEqual(res["winners"], [3])
        self.assertEqual(res["cancelled"], [2])
        # 根 scope 的两个直接子任务仍按原生命周期独立推进
        self.assertEqual(rt.outcome(1)[0], None)


# ------------------------------------------------------------- 非法输入

class WatchValidationTest(unittest.TestCase):
    def _armed(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        return rt, rt.watch(0, "all").watch_id

    def test_strategy_validation(self):
        rt = TaskRuntime()
        with self.assertRaises(ValueError):
            rt.watch(0, "most")
        with self.assertRaises(TypeError):
            rt.watch(0, 123)
        with self.assertRaises(ValueError):
            rt.watch(0, "n_succeed")                # 缺 need
        with self.assertRaises(ValueError):
            rt.watch(0, "n_succeed", need=0)
        with self.assertRaises(ValueError):
            rt.watch(0, "n_succeed", need=-1)
        with self.assertRaises(TypeError):
            rt.watch(0, "n_succeed", need=True)     # bool 不算 int
        with self.assertRaises(TypeError):
            rt.watch(0, "n_succeed", need="2")
        with self.assertRaises(ValueError):
            rt.watch(0, "all", need=2)              # need 只属于 n_succeed

    def test_scope_validation(self):
        rt = TaskRuntime()
        with self.assertRaises(UnknownTaskError):
            rt.watch(99, "all")
        rt.close_scope(0)
        with self.assertRaises(WatchStateError):
            rt.watch(0, "all")

    def test_result_lookup_validation(self):
        rt, wid = self._armed()
        with self.assertRaises(UnknownWatchError):
            rt.watch_result(999)
        with self.assertRaises(TypeError):
            rt.watch_result(True)
        with self.assertRaises(WatchStateError):
            rt.watch_result(wid)                    # watching 中拒绝

    def test_duplicate_active_watch_rejected(self):
        rt = TaskRuntime()
        rt.spawn(0)
        rt.spawn(0)
        rt.watch(0, "all")
        with self.assertRaises(WatchStateError):
            rt.watch(0, "first")
        rt.fail(1, ValueError("x"))                # cleaning（成员 2 未退出）
        with self.assertRaises(WatchStateError):
            rt.watch(0, "first")

    def test_rejected_watch_is_atomic(self):
        # 非法 watch 不得留下任何半更新状态（策略/need 为普通 ValueError，
        # 重复布防为 WatchStateError，参数类型为 TypeError——全部即时抛出）
        def attempt(make_bad):
            rt = TaskRuntime()
            rt.spawn(0, "a")
            rt.spawn(0, "b")
            rt.watch(0, "all")
            before = snapshot(rt)
            with self.assertRaises((TaskTreeError, ValueError, TypeError)):
                make_bad(rt)
            self.assertEqual(snapshot(rt), before)

        attempt(lambda rt: rt.watch(0, "most"))
        attempt(lambda rt: rt.watch(0, "n_succeed", need=0))
        attempt(lambda rt: rt.watch(0, "first"))

        # 非法参数（甚至未到语义检查）也不得创建布防
        rt = TaskRuntime()
        before = snapshot(rt)
        with self.assertRaises(TypeError):
            rt.watch(0, 42)
        self.assertEqual(rt.watches(), {})
        self.assertEqual(snapshot(rt)[2], before[2])  # log 不变

    def test_watch_result_returns_a_copy(self):
        rt = TaskRuntime()
        rt.spawn(0, "a")
        wid = rt.watch(0, "all").watch_id
        rt.finish(1, "x")
        r1 = rt.watch_result(wid)
        r1["members"].append(999)
        r1["status"] = "tampered"
        r2 = rt.watch_result(wid)
        self.assertEqual(r2["members"], [1])
        self.assertEqual(r2["status"], "satisfied")


# ------------------------------------------------------- 小规模轨迹枚举

class WatchEnumerationTest(unittest.TestCase):
    """枚举带 watch 的合法轨迹，用快照在测试侧重算全部 watch 关系。

    根下 3 个成员；事件深度 <= 10；shield 深度 <= 1；watch 一旦 done 即停枝。
    记忆化 key 把结局/触发/汇总的绝对事件序号换成相对排名（只保留先后
    关系），避免绝对序号污染导致的状态爆炸。
    """

    MAX_EVENTS = 10
    MEMBERS = 3

    def test_watch_traces_invariants(self):
        boom = RuntimeError("boom")
        seen = set()
        stats = {"traces": 0,
                 "done_by_kind": {}}  # (strategy, need) -> done 状态数

        def key(rt):
            st = rt.status()
            ws = rt.watches()
            seqs = sorted(s["outcome_seq"] for s in st.values()
                          if s["outcome_seq"] is not None)
            rank = {v: i for i, v in enumerate(seqs)}
            return (
                tuple((tid, s["state"], s["outcome"],
                       rank.get(s["outcome_seq"]),
                       s["shield_depth"], s["pending_cancels"],
                       s["cancel_delivered"], s["scope"])
                      for tid, s in sorted(st.items())),
                tuple((wid, w["phase"], w["strategy"], w["need"],
                       tuple(w["members"]), w["goal_met"],
                       rank.get(w["trigger_seq"]),
                       rank.get(w["done_seq"]))
                      for wid, w in sorted(ws.items())),
            )

        def expect_condition(rt, w):
            """测试侧独立判定结束条件是否已满足，返回 (met, goal_met)。

            已投递取消（cancel_delivered）但未结局的成员注定无法成功；
            shield pending 不算。
            """
            st = rt.status()
            n_fin = n_fail = n_cancel = n_open = n_possible = 0
            for m in w["members"]:
                kind = st[m]["outcome"]
                if kind == "finished":
                    n_fin += 1
                elif kind == "failed":
                    n_fail += 1
                elif kind == "cancelled":
                    n_cancel += 1
                else:
                    n_open += 1
                    if not st[m]["cancel_delivered"]:
                        n_possible += 1
            strat, need = w["strategy"], w["need"]
            if strat == "all":
                if n_fail or n_cancel or n_possible < n_open:
                    return True, False
                if n_open == 0:
                    return True, n_fin == len(w["members"])
            elif strat == "first":
                if n_fin:
                    return True, True
                if n_possible == 0:
                    return True, False
            else:
                if n_fin >= need:
                    return True, True
                if n_fin + n_possible < need:
                    return True, False
            return False, None

        def check_watching(tc, rt, w):
            met, _ = expect_condition(rt, w)
            tc.assertFalse(met, "watching watch already meets end condition")

        def check_cleaning(tc, rt, w):
            met, goal = expect_condition(rt, w)
            tc.assertTrue(met, "cleaning watch does not meet condition")
            tc.assertEqual(w["goal_met"], goal)
            st = rt.status()
            for m in w["members"]:
                if st[m]["state"] != "exited":
                    # 清理期间未退出成员必然已被请求取消（投递或 shield pending）
                    tc.assertTrue(st[m]["cancel_delivered"]
                                  or st[m]["pending_cancels"] > 0)

        def check_phase(tc, rt):
            for wid, w in rt.watches().items():
                if w["phase"] == "watching":
                    check_watching(tc, rt, w)
                elif w["phase"] == "cleaning":
                    check_cleaning(tc, rt, w)
                else:
                    k = (w["strategy"], w["need"])
                    stats["done_by_kind"][k] = \
                        stats["done_by_kind"].get(k, 0) + 1
                    check_done_invariants(tc, rt, wid)

        def legal_ops(rt):
            st = rt.status()
            active = [w for w in rt.watches().values()
                      if w["phase"] != "done"]
            for tid, s in sorted(st.items()):
                if tid == 0:
                    continue
                if s["outcome"] is None:
                    yield "finish", (tid, f"r{tid}")
                    yield "fail", (tid, boom)
                    if s["shield_depth"] == 0:
                        yield "enter_shield", (tid,)
                if s["shield_depth"] > 0:
                    yield "leave_shield", (tid,)
            if st[0]["scope"] == "open":
                yield "close_scope", (0,)
            if not active and len(rt.watches()) == 0:
                for strat in ("all", "first", "n_succeed"):
                    if strat == "n_succeed":
                        for need in (1, 2, 3):
                            yield "watch", (0, strat, need)
                    else:
                        yield "watch", (0, strat)

        def dfs(rt, depth):
            check_phase(self, rt)
            stats["traces"] += 1
            k = key(rt)
            if k in seen or depth == 0:
                return
            seen.add(k)
            # watch done 后停止继续扩展（汇总已定，后续只有 scope 收尾）
            if any(w["phase"] == "done" for w in rt.watches().values()):
                return
            for name, args in legal_ops(rt):
                rt2 = copy.deepcopy(rt)
                before = key(rt2)
                try:
                    getattr(rt2, name)(*args)
                except TaskTreeError:
                    self.assertEqual(before, key(rt2))  # 拒绝必须原子
                    continue
                dfs(rt2, depth - 1)

        base = TaskRuntime()
        for i in range(1, self.MEMBERS + 1):
            base.spawn(0, f"m{i}")
        dfs(base, self.MAX_EVENTS)
        self.assertGreater(len(seen), 1000)
        # 三种策略（含 need=1/2/3）都必须实际走到过 done
        for kind in (("all", None), ("first", None),
                     ("n_succeed", 1), ("n_succeed", 2),
                     ("n_succeed", 3)):
            self.assertGreater(stats["done_by_kind"].get(kind, 0), 0,
                               f"no done state reached for {kind}")


if __name__ == "__main__":
    unittest.main()
