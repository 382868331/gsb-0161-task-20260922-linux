"""容量池（acquire/release）的单元测试与随机轨迹守恒验收。

期望值由测试内的独立参考模型（RefPools，纯 dict/deque 实现，不调用
被测核心）或手工推导给出；往返一致仅作补充。
"""

import random
import unittest
from collections import deque

from tasktree import (
    CapacityError,
    PoolError,
    TaskRuntime,
    TaskStateError,
    UnknownPoolError,
)


class RefPools:
    """测试侧独立参考：FIFO 池 + 结局自动释放 + 取消交付出队。"""

    def __init__(self, capacities):
        self.cap = dict(capacities)
        self.held = {k: {} for k in capacities}
        self.queue = {k: deque() for k in capacities}

    def available(self, pool):
        return self.cap[pool] - sum(self.held[pool].values())

    def acquire(self, tid, pool, amount):
        if not self.queue[pool] and self.available(pool) >= amount:
            self.held[pool][tid] = self.held[pool].get(tid, 0) + amount
        else:
            self.queue[pool].append((tid, amount))

    def release(self, tid, pool, amount):
        left = self.held[pool][tid] - amount
        if left:
            self.held[pool][tid] = left
        else:
            del self.held[pool][tid]
        self._drain(pool)

    def _drain(self, pool):
        while self.queue[pool]:
            tid, amt = self.queue[pool][0]
            if self.available(pool) < amt:
                break
            self.queue[pool].popleft()
            self.held[pool][tid] = self.held[pool].get(tid, 0) + amt

    def dequeue(self, tid):
        for pool in self.cap:
            q = self.queue[pool]
            kept = deque(x for x in q if x[0] != tid)
            q.clear()
            q.extend(kept)

    def exit(self, tid):
        self.dequeue(tid)
        for pool in self.cap:
            if self.held[pool].pop(tid, 0):
                self._drain(pool)

    def check_conservation(self, tc):
        for pool in self.cap:
            tc.assertEqual(
                self.available(pool) + sum(self.held[pool].values()),
                self.cap[pool])


def check_pool_invariants(tc, rt):
    """逐步守恒核对：available + 持有总量 == capacity，且队列无已退出任务。"""
    st = rt.status()
    for name, p in rt.pool_status().items():
        tc.assertEqual(p["available"] + sum(p["held"].values()),
                       p["capacity"], f"pool {name} conservation violated")
        tc.assertGreaterEqual(p["available"], 0)
        for tid, _amt in p["queue"]:
            tc.assertIsNone(st[tid]["outcome"],
                            f"exited task#{tid} still queued in {name}")
            tc.assertFalse(st[tid]["cancel_delivered"],
                           f"cancelled task#{tid} still queued in {name}")
        for tid in p["held"]:
            tc.assertIsNone(st[tid]["outcome"],
                            f"exited task#{tid} still holds {name}")


class PoolBasicsTest(unittest.TestCase):
    def test_immediate_grant_and_explicit_release(self):
        rt = TaskRuntime(pools={"gpu": 2})
        a = rt.spawn(name="a").new_task_id
        r = rt.acquire(a, "gpu", 2)
        self.assertIn("granted", r.notes[0])
        self.assertEqual(rt.pool_status("gpu")["held"], {a: 2})
        rt.release(a, "gpu", 1)
        self.assertEqual(rt.pool_status("gpu")["available"], 1)
        rt.release(a, "gpu")  # amount=None 归还全部
        self.assertEqual(rt.pool_status("gpu")["available"], 2)
        check_pool_invariants(self, rt)

    def test_over_capacity_rejected_immediately(self):
        rt = TaskRuntime(pools={"gpu": 2})
        a = rt.spawn(name="a").new_task_id
        with self.assertRaises(CapacityError):
            rt.acquire(a, "gpu", 3)
        # 拒绝不改状态：未入队、未持有
        self.assertEqual(rt.pool_status("gpu")["queue"], [])
        self.assertEqual(rt.pool_status("gpu")["held"], {})

    def test_strict_fifo_no_overtaking(self):
        rt = TaskRuntime(pools={"gpu": 2})
        a = rt.spawn(name="a").new_task_id
        heavy = rt.spawn(name="heavy").new_task_id
        light = rt.spawn(name="light").new_task_id
        rt.acquire(a, "gpu", 2)          # 占满
        rt.acquire(heavy, "gpu", 2)      # 队首，需要 2
        rt.acquire(light, "gpu", 1)      # 轻量请求，排在后面
        rt.release(a, "gpu", 1)          # 只释放 1，队首仍不够
        p = rt.pool_status("gpu")
        self.assertEqual(p["queue"], [(heavy, 2), (light, 1)])
        self.assertEqual(p["held"], {a: 1})  # light 不得插队
        rt.release(a, "gpu", 1)          # 释放剩余，heavy 先拿，light 继续等
        p = rt.pool_status("gpu")
        self.assertEqual(p["held"], {heavy: 2})
        self.assertEqual(p["queue"], [(light, 1)])
        check_pool_invariants(self, rt)

    def test_release_errors(self):
        rt = TaskRuntime(pools={"gpu": 2})
        a = rt.spawn(name="a").new_task_id
        b = rt.spawn(name="b").new_task_id
        rt.acquire(a, "gpu", 1)
        with self.assertRaises(PoolError):       # 未持有不能释放
            rt.release(b, "gpu", 1)
        with self.assertRaises(PoolError):       # 超量释放
            rt.release(a, "gpu", 2)
        with self.assertRaises(UnknownPoolError):
            rt.acquire(a, "tpu", 1)
        with self.assertRaises(UnknownPoolError):
            rt.release(a, "tpu", 1)
        with self.assertRaises(TypeError):       # bool 不是 int
            rt.acquire(a, "gpu", True)
        with self.assertRaises(ValueError):
            rt.acquire(a, "gpu", 0)
        with self.assertRaises(TaskStateError):  # root 无任务体
            rt.acquire(0, "gpu", 1)
        with self.assertRaises(ValueError):
            TaskRuntime(pools={"bad": 0})

    def test_acquire_after_end_or_cancel_rejected(self):
        rt = TaskRuntime(pools={"gpu": 1})
        a = rt.spawn(name="a").new_task_id
        rt.finish(a)
        with self.assertRaises(TaskStateError):
            rt.acquire(a, "gpu", 1)
        b = rt.spawn(name="b").new_task_id
        rt.cancel(b)
        with self.assertRaises(TaskStateError):
            rt.acquire(b, "gpu", 1)


class PoolLifecycleTest(unittest.TestCase):
    def test_finish_auto_releases_once_and_grants_next(self):
        rt = TaskRuntime(pools={"gpu": 1})
        a = rt.spawn(name="a").new_task_id
        b = rt.spawn(name="b").new_task_id
        rt.acquire(a, "gpu", 1)
        rt.acquire(b, "gpu", 1)          # 排队
        rt.finish(a, "done")             # 结局 -> 自动释放 -> 授予队首
        self.assertEqual(rt.pool_status("gpu")["held"], {b: 1})
        # 已退出任务再 release 报未持有，证明只释放了一次
        with self.assertRaises(PoolError):
            rt.release(a, "gpu", 1)
        check_pool_invariants(self, rt)

    def test_fail_teardown_releases(self):
        rt = TaskRuntime(pools={"db": 3})
        a = rt.spawn(name="a").new_task_id
        rt.acquire(a, "db", 3)
        rt.fail(a, RuntimeError("boom"))  # 异常收尾同样归还
        self.assertEqual(rt.pool_status("db")["available"], 3)
        with self.assertRaises(PoolError):
            rt.release(a, "db", 1)        # 不会重复释放
        check_pool_invariants(self, rt)

    def test_queued_cancel_removed_no_late_grant(self):
        rt = TaskRuntime(pools={"gpu": 1})
        a = rt.spawn(name="a").new_task_id
        b = rt.spawn(name="b").new_task_id
        c = rt.spawn(name="c").new_task_id
        rt.acquire(a, "gpu", 1)
        rt.acquire(b, "gpu", 1)          # 排队
        rt.acquire(c, "gpu", 1)          # 排队
        rt.cancel(b)                     # 取消交付 -> 立即出队
        self.assertEqual(rt.pool_status("gpu")["queue"], [(c, 1)])
        rt.finish(a)                     # 释放后授予 c，b 不得迟到获授
        self.assertEqual(rt.pool_status("gpu")["held"], {c: 1})
        check_pool_invariants(self, rt)

    def test_grant_and_cancel_same_step(self):
        # 同一事件内：holder 退出释放许可，队首恰好是已被取消的任务，
        # 必须跳过它授予下一个合格等待者。
        rt = TaskRuntime(pools={"gpu": 1})
        a = rt.spawn(name="a").new_task_id
        b = rt.spawn(name="b").new_task_id
        c = rt.spawn(name="c").new_task_id
        rt.acquire(a, "gpu", 1)
        rt.acquire(b, "gpu", 1)
        rt.acquire(c, "gpu", 1)
        rt.cancel(b)                     # b 出队
        r = rt.finish(a)                 # 同一步：释放 + 授予
        self.assertEqual(rt.pool_status("gpu")["held"], {c: 1})
        self.assertTrue(any("granted" in n for n in r.notes))
        check_pool_invariants(self, rt)

    def test_parent_cancel_releases_whole_subtree(self):
        rt = TaskRuntime(pools={"gpu": 2, "db": 1})
        parent = rt.spawn(name="parent").new_task_id
        x = rt.spawn(parent=parent, name="x").new_task_id
        y = rt.spawn(parent=parent, name="y").new_task_id
        rt.acquire(x, "gpu", 2)
        rt.acquire(y, "db", 1)
        rt.cancel(parent)                # 取消整棵子树（仅请求）
        # 取消不直接释放：任务仍需退出
        self.assertEqual(rt.pool_status("gpu")["available"], 0)
        rt.finish(x)
        rt.finish(y)
        rt.finish(parent)
        self.assertEqual(rt.pool_status("gpu")["available"], 2)
        self.assertEqual(rt.pool_status("db")["available"], 1)
        check_pool_invariants(self, rt)

    def test_queued_descendant_dequeued_on_subtree_cancel(self):
        rt = TaskRuntime(pools={"gpu": 1})
        holder = rt.spawn(name="holder").new_task_id
        parent = rt.spawn(name="parent").new_task_id
        child = rt.spawn(parent=parent, name="child").new_task_id
        rt.acquire(holder, "gpu", 1)
        rt.acquire(child, "gpu", 1)      # 子任务排队
        rt.cancel(parent)                # 取消子树 -> child 出队
        self.assertEqual(rt.pool_status("gpu")["queue"], [])
        rt.finish(holder)                # 释放后无人可授
        self.assertEqual(rt.pool_status("gpu")["available"], 1)
        check_pool_invariants(self, rt)

    def test_shield_delays_dequeue_but_no_double_release(self):
        rt = TaskRuntime(pools={"gpu": 1})
        a = rt.spawn(name="a").new_task_id
        b = rt.spawn(name="b").new_task_id
        rt.acquire(a, "gpu", 1)
        rt.enter_shield(b)
        rt.acquire(b, "gpu", 1)          # shield 中排队
        rt.cancel(b)                     # 只记 pending，不出队
        self.assertEqual(rt.pool_status("gpu")["queue"], [(b, 1)])
        rt.leave_shield(b)               # 交付 -> 出队
        self.assertEqual(rt.pool_status("gpu")["queue"], [])
        rt.finish(a)
        self.assertEqual(rt.pool_status("gpu")["available"], 1)
        # 持有者 shield 不影响释放单次性
        c = rt.spawn(name="c").new_task_id
        rt.acquire(c, "gpu", 1)
        rt.enter_shield(c)
        rt.cancel(c)
        rt.leave_shield(c)
        rt.finish(c)                     # 结局释放一次
        self.assertEqual(rt.pool_status("gpu")["available"], 1)
        with self.assertRaises(PoolError):
            rt.release(c, "gpu", 1)
        check_pool_invariants(self, rt)

    def test_close_scope_waits_and_releases(self):
        rt = TaskRuntime(pools={"gpu": 1})
        parent = rt.spawn(name="parent").new_task_id
        child = rt.spawn(parent=parent, name="child").new_task_id
        rt.acquire(child, "gpu", 1)
        rt.close_scope(parent)
        self.assertFalse(rt.is_closed(parent))   # 等待子树退出
        rt.finish(child)
        self.assertTrue(rt.is_closed(parent))
        self.assertEqual(rt.pool_status("gpu")["available"], 1)
        check_pool_invariants(self, rt)


class PoolRandomTrajectoryTest(unittest.TestCase):
    """随机合法轨迹：逐步对照独立参考模型并核对守恒。"""

    def test_random_trajectories_match_reference(self):
        for seed in range(30):
            rng = random.Random(seed)
            caps = {"gpu": rng.randint(1, 3), "db": rng.randint(1, 4)}
            rt = TaskRuntime(pools=caps)
            ref = RefPools(caps)
            tasks = [rt.spawn(name=f"t{i}").new_task_id for i in range(5)]
            alive = list(tasks)
            for _step in range(200):
                action = rng.choice(
                    ["acquire", "release", "finish", "cancel"])
                tid = rng.choice(tasks)
                st = rt.status()[tid]
                if action == "acquire":
                    if st["outcome"] is not None or st["cancel_delivered"]:
                        continue
                    pool = rng.choice(list(caps))
                    amt = rng.randint(1, caps[pool])
                    rt.acquire(tid, pool, amt)
                    ref.acquire(tid, pool, amt)
                elif action == "release":
                    for pool in caps:
                        held = rt.pool_status(pool)["held"].get(tid, 0)
                        if held:
                            amt = rng.randint(1, held)
                            rt.release(tid, pool, amt)
                            ref.release(tid, pool, amt)
                            break
                elif action == "finish":
                    if st["outcome"] is None and tid in alive:
                        rt.finish(tid, rng.randint(0, 100))
                        ref.exit(tid)
                        alive.remove(tid)
                else:  # cancel：仅对未结局任务，参考模型只对排队/交付建模
                    if (st["outcome"] is None and not st["cancel_delivered"]
                            and st["shield_depth"] == 0):
                        rt.cancel(tid)
                        ref.dequeue(tid)
                # 逐步核对：守恒 + 与参考模型一致
                for pool in caps:
                    got = rt.pool_status(pool)
                    self.assertEqual(
                        got["available"] + sum(got["held"].values()),
                        caps[pool],
                        f"seed={seed} step={_step} pool={pool}")
                    self.assertEqual(got["held"], ref.held[pool],
                                     f"seed={seed} step={_step} pool={pool}")
                    self.assertEqual(got["queue"], list(ref.queue[pool]),
                                     f"seed={seed} step={_step} pool={pool}")
                check_pool_invariants(self, rt)


if __name__ == "__main__":
    unittest.main()
