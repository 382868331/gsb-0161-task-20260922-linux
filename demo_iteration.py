"""容量池演示：FIFO 排队、取消出队、子树取消释放、异常收尾归还。

纯现场计算，确定性事件驱动，无 sleep、无预录结果，运行时间 < 1 秒。
"""

from tasktree import CapacityError, PoolError, TaskRuntime


def show_pools(rt, title):
    print(f"  [{title}]")
    for name, p in rt.pool_status().items():
        held = ", ".join(f"task#{t}x{n}" for t, n in p["held"].items()) or "-"
        queue = (", ".join(f"task#{t}要{n}" for t, n in p["queue"]) or "-")
        print(f"    池 {name}: 容量={p['capacity']} 可用={p['available']} "
              f"持有=[{held}] 队列=[{queue}]")
        assert p["available"] + sum(p["held"].values()) == p["capacity"]


def main():
    print("== 1. 立即授予 + 超容量立即拒绝 ==")
    rt = TaskRuntime(pools={"gpu": 2, "db": 1})
    a = rt.spawn(name="trainer").new_task_id
    rt.acquire(a, "gpu", 2)
    show_pools(rt, "trainer 独占 gpu x2")
    try:
        rt.acquire(a, "gpu", 3)
    except CapacityError as e:
        print(f"  申请 3 > 容量 2 -> 立即拒绝: {e}")

    print("\n== 2. 严格 FIFO：轻量请求不得插队 ==")
    heavy = rt.spawn(name="heavy").new_task_id
    light = rt.spawn(name="light").new_task_id
    rt.acquire(heavy, "gpu", 2)
    rt.acquire(light, "gpu", 1)
    rt.release(a, "gpu", 1)
    show_pools(rt, "释放 1 个，队首 heavy 要 2 仍不够，light 不插队")
    rt.release(a, "gpu", 1)
    show_pools(rt, "再释放 1 个，heavy 获授，light 继续等")

    print("\n== 3. 排队任务取消：移出队列，不迟到获授 ==")
    rt.cancel(light)
    show_pools(rt, "light 取消交付后立即出队")
    rt.finish(heavy)
    show_pools(rt, "heavy 退出自动归还，无人迟到获授")

    print("\n== 4. 父任务取消整棵子树 + 异常收尾只释放一次 ==")
    parent = rt.spawn(name="pipeline").new_task_id
    w1 = rt.spawn(parent=parent, name="worker-1").new_task_id
    w2 = rt.spawn(parent=parent, name="worker-2").new_task_id
    rt.acquire(w1, "gpu", 2)
    rt.acquire(w2, "db", 1)
    show_pools(rt, "worker-1 持 gpu x2，worker-2 持 db x1")
    rt.cancel(parent)
    show_pools(rt, "取消已投递但任务未退出，许可仍被占用")
    rt.fail(w1, RuntimeError("CUDA OOM"))   # 异常收尾
    show_pools(rt, "worker-1 异常收尾，gpu 自动归还一次")
    try:
        rt.release(w1, "gpu", 1)
    except PoolError as e:
        print(f"  重复释放被拒绝: {e}")
    rt.finish(w2)
    rt.finish(parent)
    show_pools(rt, "子树全部退出，许可全部归还")

    print("\n== 5. 等待链与守恒核对 ==")
    print("  待完成原因:", rt.pending_reasons(0) or "（无，整树已退出）")
    eg = rt.exception_group(0)
    print("  异常组:", repr(eg) if eg else None)
    for name, p in rt.pool_status().items():
        total = p["available"] + sum(p["held"].values())
        assert total == p["capacity"], "许可不守恒!"
        print(f"  池 {name}: {p['available']} 可用 + "
              f"{sum(p['held'].values())} 持有 = {total} 守恒 ✓")
    print("\n演示完成：全部许可守恒，无迟到授予，无重复释放。")


if __name__ == "__main__":
    main()
