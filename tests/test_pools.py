"""容量池测试：FIFO、守恒、取消归还、shield、异常路径。

独立参考：RefPool 是本文件内手写的最小 FIFO 模型，不调用 tasktree
的任何池实现来推导期望值；逐事件与 TaskRuntime 的 pool_status 对比。
"""

import unittest
from collections import deque

from tasktree import (
    PoolError,
    ResourceLimitError,
    TaskRuntime,
    TaskStateError,
    UnknownPoolError,
)


class RefPool:
    """独立参考模型：严格 FIFO、不插队、超容量拒绝。"""

    def __init__(self, capacity):
        self.capacity = capacity
        self.holders = {}
        self.queue = deque()

    @property
    def free(self):
        return self.capacity - sum(self.holders.values())

    def _grant(self):
        while self.queue and self.free >= self.queue[0][1]:
            tid, amt = self.queue.popleft()
            self.holders[tid] = self.holders.get(tid, 0) + amt

    def acquire(self, tid, amt):
        assert amt <= self.capacity
        if not self.queue and self.free >= amt:
            self.holders[tid] = self.holders.get(tid, 0) + amt
        else:
            self.queue.append((tid, amt))

    def release(self, tid, amt):
        left = self.holders[tid] - amt
        if left:
            self.holders[tid] = left
        else:
            del self.holders[tid]
        self._grant()

    def dequeue(self, tid):
        self.queue = deque(x for x in self.queue if x[0] != tid)
        self._grant()

    def exit(self, tid):
        self.queue = deque(x for x in self.queue if x[0] != tid)
        self.holders.pop(tid, None)
        self._grant()

    def snap(self):
        return {
            "capacity": self.capacity,
            "free": self.free,
            "holders": dict(sorted(self.holders.items())),
            "queue": list(self.queue),
        }


def assert_conservation(testcase, rt):
    for name, snap in rt.pool_status().items():
        testcase.assertEqual(
            snap["capacity"],
            snap["free"] + sum(snap["holders"].values()),
            f"permit conservation violated in pool {name!r}",
        )
        testcase.assertGreaterEqual(snap["free"], 0)


class FifoReferenceTest(unittest.TestCase):
    """同一事件脚本驱动被测运行时与独立参考，逐步对比。"""

    def test_scripted_trace_matches_reference(self):
        rt = TaskRuntime()
        rt.create_pool("gpu", 3)
        ref = RefPool(3)
        for _ in range(5):
            rt.spawn(0)

        def check():
            self.assertEqual(rt.pool_status("gpu")["gpu"], ref.snap())
            assert_conservation(self, rt)

        rt.acquire(1, "gpu", 2)
        ref.acquire(1, 2)
        check()
        rt.acquire(2, "gpu", 2)          # 队首，暂时拿不到
        ref.acquire(2, 2)
        check()
        rt.acquire(3, "gpu", 1)          # 轻量请求，不得插队
        ref.acquire(3, 1)
        check()
        rt.release(1, "gpu", 1)          # 释放 1 后 free=2，队首获授
        ref.release(1, 1)
        check()
        self.assertEqual(rt.pool_status("gpu")["gpu"]["holders"],
                         {1: 1, 2: 2})
        rt.cancel(2)                     # 队首取消 -> 出队，轻量请求获授
        ref.dequeue(2)
        check()
        rt.finish(3, "ok")               # 结局自动归还
        ref.exit(3)
        check()
        rt.fail(1, RuntimeError("boom"))  # 异常收尾同样归还且只一次
        ref.exit(1)
        check()
        rt.finish(1, "late")             # 晚到 finish 不得重复释放
        ref.snap()
        check()


class PoolSemanticsTest(unittest.TestCase):
    def setUp(self):
        self.rt = TaskRuntime()
        self.rt.create_pool("sem", 2)
        for _ in range(4):
            self.rt.spawn(0)

    def test_oversize_request_rejected_immediately(self):
        with self.assertRaises(ResourceLimitError):
            self.rt.acquire(1, "sem", 3)
        self.assertEqual(self.rt.pool_status("sem")["sem"]["queue"], [])

    def test_head_of_line_blocking_no_overtake(self):
        self.rt.acquire(1, "sem", 2)
        self.rt.acquire(2, "sem", 2)     # 队首大请求
        self.rt.acquire(3, "sem", 1)     # 轻量请求排其后
        self.rt.release(1, "sem", 1)     # 只够轻量请求，但队首未满足
        snap = self.rt.pool_status("sem")["sem"]
        self.assertEqual(snap["holders"], {1: 1})
        self.assertEqual(snap["queue"], [(2, 2), (3, 1)])

    def test_grant_and_cancel_same_step(self):
        self.rt.acquire(1, "sem", 2)
        self.rt.acquire(2, "sem", 1)     # 队首
        self.rt.acquire(3, "sem", 1)     # 次位
        self.rt.cancel(2)                # 取消队首：出队但不释放（无持有）
        self.rt.release(1, "sem", 2)     # 同一步释放应授予 task#3 而非 task#2
        snap = self.rt.pool_status("sem")["sem"]
        self.assertEqual(snap["holders"], {3: 1})
        self.assertEqual(snap["queue"], [])
        self.rt.finish(2, "cancelled exit")
        self.assertEqual(self.rt.outcome(2)[0], "cancelled")
        assert_conservation(self, self.rt)

    def test_parent_cancel_subtree_releases_and_dequeues(self):
        self.rt.spawn(1, "child-holder")
        self.rt.spawn(1, "child-waiter")
        self.rt.acquire(4, "sem", 2)     # 子任务持有
        self.rt.acquire(5, "sem", 1)     # 子任务排队
        self.rt.cancel(1)                # 父取消 -> 整棵子树
        self.assertEqual(self.rt.pool_status("sem")["sem"]["queue"], [])
        self.rt.finish(5, None)          # 排队者收尾：不得迟到获授
        self.rt.fail(4, RuntimeError("cleanup blew up"))  # 收尾抛错也归还
        snap = self.rt.pool_status("sem")["sem"]
        self.assertEqual(snap["holders"], {})
        self.assertEqual(snap["free"], 2)
        assert_conservation(self, self.rt)

    def test_release_exactly_once_on_exit(self):
        self.rt.acquire(1, "sem", 2)
        self.rt.finish(1, "done")        # 自动归还
        self.assertEqual(self.rt.pool_status("sem")["sem"]["free"], 2)
        self.rt.finish(1, "done")        # 幂等 finish 不再释放
        self.assertEqual(self.rt.pool_status("sem")["sem"]["free"], 2)
        with self.assertRaises(PoolError):
            self.rt.release(1, "sem")    # 已不持有，不能二次释放

    def test_shield_delays_cancel_but_no_double_release(self):
        self.rt.acquire(1, "sem", 2)
        self.rt.acquire(2, "sem", 1)     # 排队
        self.rt.enter_shield(2)
        self.rt.cancel(2)                # shield 内：只记 pending，不出队
        self.assertEqual(self.rt.pool_status("sem")["sem"]["queue"], [(2, 1)])
        self.rt.leave_shield(2)          # 交付取消：此刻出队
        self.assertEqual(self.rt.pool_status("sem")["sem"]["queue"], [])
        self.rt.finish(2, None)          # 收尾：无持有，无重复释放
        self.assertEqual(self.rt.pool_status("sem")["sem"]["free"], 0)
        self.rt.finish(1, "x")
        self.assertEqual(self.rt.pool_status("sem")["sem"]["free"], 2)
        assert_conservation(self, self.rt)

    def test_shielded_queued_task_may_be_granted_before_delivery(self):
        self.rt.acquire(1, "sem", 2)
        self.rt.acquire(2, "sem", 1)
        self.rt.enter_shield(2)
        self.rt.cancel(2)                # pending，不出队
        self.rt.release(1, "sem", 2)     # 队首仍可获授（取消尚未交付）
        self.assertEqual(
            self.rt.pool_status("sem")["sem"]["holders"], {2: 1})
        self.rt.leave_shield(2)
        self.rt.finish(2, None)          # 收尾归还一次
        self.assertEqual(self.rt.pool_status("sem")["sem"]["free"], 2)


class PoolErrorPathTest(unittest.TestCase):
    def setUp(self):
        self.rt = TaskRuntime()
        self.rt.create_pool("p", 1)
        self.rt.spawn(0)

    def test_duplicate_pool_rejected(self):
        with self.assertRaises(PoolError):
            self.rt.create_pool("p", 1)

    def test_unknown_pool_rejected(self):
        with self.assertRaises(UnknownPoolError):
            self.rt.acquire(1, "nope")
        with self.assertRaises(UnknownPoolError):
            self.rt.release(1, "nope")

    def test_pool_validation(self):
        with self.assertRaises(ValueError):
            self.rt.create_pool("q", 0)
        with self.assertRaises(TypeError):
            self.rt.create_pool("q", True)
        with self.assertRaises(ValueError):
            self.rt.create_pool("", 1)

    def test_amount_validation(self):
        with self.assertRaises(TypeError):
            self.rt.acquire(1, "p", True)
        with self.assertRaises(ValueError):
            self.rt.acquire(1, "p", 0)

    def test_double_queue_rejected(self):
        self.rt.acquire(1, "p", 1)
        self.rt.spawn(0)
        self.rt.acquire(2, "p", 1)       # 排队
        with self.assertRaises(PoolError):
            self.rt.acquire(2, "p", 1)   # 重复排队

    def test_release_without_holding_rejected(self):
        with self.assertRaises(PoolError):
            self.rt.release(1, "p")

    def test_release_more_than_held_rejected(self):
        self.rt.acquire(1, "p", 1)
        with self.assertRaises(ValueError):
            self.rt.release(1, "p", 2)

    def test_acquire_after_outcome_rejected(self):
        self.rt.finish(1, "done")
        with self.assertRaises(TaskStateError):
            self.rt.acquire(1, "p", 1)

    def test_acquire_after_cancel_delivered_rejected(self):
        self.rt.cancel(1)
        with self.assertRaises(TaskStateError):
            self.rt.acquire(1, "p", 1)

    def test_acquire_on_root_rejected(self):
        with self.assertRaises(TaskStateError):
            self.rt.acquire(0, "p", 1)


if __name__ == "__main__":
    unittest.main()
