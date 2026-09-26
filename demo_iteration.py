"""tasktree 容量池演示：FIFO 排队、取消归还、守恒核对与拒绝边界。

全部确定性、离线、无 sleep、现场计算；运行时间远小于 90 秒。
每个事件后打印池的持有关系与等待队列，并核对许可守恒。
"""

from tasktree import ResourceLimitError, TaskRuntime


def show(receipt):
    print(f"  event#{receipt.seq} {receipt.op}(task={receipt.task_id})")
    for n in receipt.notes:
        print(f"      {n}")


def show_pool(rt, pool):
    snap = rt.pool_status(pool)[pool]
    holders = {f"task#{k}": v for k, v in snap["holders"].items()}
    queue = [(f"task#{t}", a) for t, a in snap["queue"]]
    print(f"      pool '{pool}': free={snap['free']}/{snap['capacity']}"
          f" holders={holders} queue={queue}")
    assert snap["free"] + sum(snap["holders"].values()) == snap["capacity"], \
        "permit conservation violated"


def part1_fifo():
    print("=" * 64)
    print("第 1 部分：严格 FIFO —— 队首拿不到时轻量请求不插队")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.create_pool("gpu", 2))
    for name in ("trainer", "evaluator", "prober"):
        show(rt.spawn(0, name))
    show(rt.acquire(1, "gpu", 2))        # 占满
    show(rt.acquire(2, "gpu", 2))        # 队首大请求
    show(rt.acquire(3, "gpu", 1))        # 轻量请求排队
    show_pool(rt, "gpu")
    print("  -- trainer 只释放 1 张卡：队首要 2，prober 不得插队 --")
    show(rt.release(1, "gpu", 1))
    show_pool(rt, "gpu")
    print("  -- trainer 收尾，释放剩余 1 张：队首获授 --")
    show(rt.finish(1, "trained"))
    show_pool(rt, "gpu")


def part2_cancel():
    print("=" * 64)
    print("第 2 部分：排队者取消移出队列；父任务取消整棵子树")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.create_pool("slot", 2))
    show(rt.spawn(0, "parent"))
    show(rt.spawn(1, "worker-a"))
    show(rt.spawn(1, "worker-b"))
    show(rt.spawn(0, "outsider"))
    show(rt.acquire(2, "slot", 2))       # worker-a 占满
    show(rt.acquire(3, "slot", 1))       # worker-b 排队
    show(rt.acquire(4, "slot", 1))       # outsider 排队
    show_pool(rt, "slot")
    print("  -- 取消 parent：整棵子树取消，排队者出队 --")
    show(rt.cancel(1))
    show_pool(rt, "slot")
    print("  -- worker-b 收尾：不得迟到获授；worker-a 异常收尾归还一次 --")
    show(rt.finish(3, None))
    show(rt.fail(2, RuntimeError("cuda oom during cleanup")))
    show_pool(rt, "slot")
    print("  -- outsider 到队首，获授 --")
    show_pool(rt, "slot")


def part3_shield_and_reject():
    print("=" * 64)
    print("第 3 部分：shield 延迟取消交付 + 超容量立即拒绝")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.create_pool("mem", 1))
    show(rt.spawn(0, "holder"))
    show(rt.spawn(0, "waiter"))
    show(rt.acquire(1, "mem", 1))
    show(rt.acquire(2, "mem", 1))        # 排队
    show(rt.enter_shield(2))
    show(rt.cancel(2))                   # shield 内：只记 pending，不出队
    show_pool(rt, "mem")
    show(rt.leave_shield(2))             # 交付取消：此刻出队
    show_pool(rt, "mem")
    show(rt.finish(2, None))
    print("  -- 申请量超过总容量：立即拒绝，不入队 --")
    try:
        rt.acquire(1, "mem", 2)
    except ResourceLimitError as e:
        print(f"      ResourceLimitError: {e}")
    show_pool(rt, "mem")
    show(rt.finish(1, "done"))
    show_pool(rt, "mem")
    print("  守恒核对：所有事件后 free + 持有 == capacity，全部通过")


if __name__ == "__main__":
    part1_fifo()
    part2_cancel()
    part3_shield_and_reject()

