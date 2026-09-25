"""tasktree 演示：正常完成轨迹 + 异常组与等待链 + 截止时间/逻辑时钟 + 拒绝边界。

全部确定性、离线、无 sleep、无线程；时钟为显式推进的逻辑整数，
运行时间远小于 90 秒。
"""

from tasktree import (
    NonMonotonicTimeError,
    ScopeClosedError,
    TaskRuntime,
)


def show(receipt):
    print(f"  event#{receipt.seq} t={receipt.clock} "
          f"{receipt.op}(task={receipt.task_id}) "
          f"-> state={receipt.state} scope={receipt.scope_state}")
    for n in receipt.notes:
        print(f"      {n}")


def show_pending(rt, scope=0):
    reasons = rt.pending_reasons(scope)
    if not reasons:
        print("  待完成原因: <无，子树已完整退出>")
    else:
        print("  待完成原因（等待链）:")
        for r in reasons:
            print(f"      - {r}")


def part1_normal():
    print("=" * 64)
    print("第 1 部分：正常结果 —— 任务树完成并正常关闭")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.spawn(0, "parser"))
    show(rt.spawn(0, "writer"))
    show(rt.spawn(2, "flusher"))          # writer 的子任务
    show(rt.finish(3, "flushed 128 rows"))
    show(rt.finish(1, "parsed 3 files"))
    show(rt.finish(2, "written"))
    show(rt.close_scope(0))
    show_pending(rt)
    st = rt.status()
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")
    print(f"  writer 的结果: {st[2]['value']!r}")
    print(f"  异常组: {rt.exception_group(0)}")


def part2_group_and_waiting_chain():
    print()
    print("=" * 64)
    print("第 2 部分：异常组与等待链 —— 取消、屏蔽与业务失败并存")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.spawn(0, "fetch"))
    show(rt.spawn(0, "compute"))
    show(rt.spawn(2, "io-helper"))
    show(rt.enter_shield(3))              # io-helper 进入屏蔽
    show(rt.close_scope(0))               # 请求取消全部未结束后代
    show_pending(rt)
    # 屏蔽内取消只记 pending；离开最外层屏蔽才投递
    show(rt.leave_shield(3))
    # io-helper 在取消投递后以业务失败退出：业务异常优先于取消
    show(rt.fail(3, TimeoutError("io timeout after cancel")))
    # compute 收到取消后以 finish 退出：结局为 cancelled，不进异常组
    show(rt.finish(2, "partial-result"))
    # fetch 以业务失败退出
    show(rt.fail(1, ConnectionError("upstream refused")))
    show_pending(rt)
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")
    group = rt.exception_group(0)
    print(f"  异常组（按任务创建序，取消不入组）: {group!r}")
    for i, exc in enumerate(group.exceptions):
        print(f"      [{i}] {type(exc).__name__}: {exc}")


def part3_deadlines_and_logical_clock():
    print()
    print("=" * 64)
    print("第 3 部分：截止时间与逻辑时钟 —— 到期只提取消，收尾仍走原路")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.spawn(0, "parent"))          # id 1
    show(rt.spawn(1, "shielded-child"))  # id 2
    show(rt.spawn(0, "sibling"))         # id 3
    # 父 scope 自设截止时刻；子任务不复制父时间，只在父到期时按原路径收取消
    show(rt.set_deadline(1, 5))
    show(rt.enter_shield(2))             # 子任务进入屏蔽
    show(rt.set_deadline(0, 9))          # 根 scope 的截止时刻更晚

    def drive():
        # 模拟器逐步驱动：取下一待触发时刻，推进到该时刻，再处理业务事件
        nxt = rt.next_deadline()
        print(f"  [驱动] 当前 t={rt.now}，下一待触发: {nxt}")
        return rt.advance_time(nxt.deadline)

    show(drive())                        # t=5：父到期，沿子树提取消
    st = rt.status()
    print(f"  父取消投递: {st[1]['cancel_delivered']}；"
          f"shield 内子任务 pending 取消数: {st[2]['pending_cancels']}")
    show_pending(rt)
    # 子任务在 shield 内先正常退出（此时它自己没有截止时间，取消仍 pending）
    show(rt.leave_shield(2))             # pending 取消此时才投递
    show(rt.finish(2, None))             # 取消已投递 -> cancelled
    show(rt.finish(1, None))             # 父随后退出
    show(drive())                        # t=9：根 scope 到期，请求关闭
    show(rt.finish(3, "sibling done"))   # -> cancelled，非业务异常
    show_pending(rt)
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")
    print(f"  异常组（超时不混入）: {rt.exception_group(0)}")

    # 截止时刻先后顺序：同刻按任务创建序触发
    print()
    rt2 = TaskRuntime()
    rt2.spawn(0, "a")                    # id 1
    rt2.spawn(0, "b")                    # id 2
    rt2.set_deadline(2, 7)               # 逆创建序安排
    rt2.set_deadline(1, 7)
    r = rt2.advance_time(7)
    print("  同一截止时刻 t=7 的触发序（按创建序）:")
    for d in r.fired:
        print(f"      -> task#{d.task_id} 的 {d.source} 到期 @{d.deadline}")


def part4_rejection_boundary():
    print()
    print("=" * 64)
    print("第 4 部分：实际触发的拒绝/异常边界")
    print("=" * 64)
    rt = TaskRuntime()
    rt.spawn(0, "edge")

    # 3a. 数值结果拒绝 NaN
    try:
        rt.finish(1, float("nan"))
    except ValueError as e:
        print(f"  [拒绝] finish(task#1, nan) -> ValueError: {e}")

    # 3b. 整数参数拒绝 bool
    try:
        rt.finish(True, 1)
    except TypeError as e:
        print(f"  [拒绝] finish(True, 1) -> TypeError: {e}")

    # 3c. 向已关闭 scope 再 spawn
    rt.finish(1, "done")
    rt.close_scope(0)
    try:
        rt.spawn(0, "too-late")
    except ScopeClosedError as e:
        print(f"  [拒绝] spawn 到已关闭 scope -> ScopeClosedError: {e}")

    # 3d. finish 幂等但结果冲突
    rt2 = TaskRuntime()
    rt2.spawn(0, "x")
    rt2.finish(1, 42)
    rt2.finish(1, 42)                     # 幂等，接受
    try:
        rt2.finish(1, 43)
    except Exception as e:
        print(f"  [拒绝] 冲突 finish -> {type(e).__name__}: {e}")

    # 3e. 逻辑时钟不得回拨；截止时刻必须严格晚于当前时钟
    rt3 = TaskRuntime()
    rt3.advance_time(10)
    try:
        rt3.advance_time(9)
    except NonMonotonicTimeError as e:
        print(f"  [拒绝] 时钟回拨 -> NonMonotonicTimeError: {e}")
    rt3.spawn(0, "y")
    try:
        rt3.set_deadline(1, 10)          # 必须严格在未来
    except ValueError as e:
        print(f"  [拒绝] 过期/当刻截止时间 -> ValueError: {e}")


if __name__ == "__main__":
    part1_normal()
    part2_group_and_waiting_chain()
    part3_deadlines_and_logical_clock()
    part4_rejection_boundary()
    print()
    print("demo 完成：以上为真实计算结果，无预录、无 sleep、无真实线程。")
