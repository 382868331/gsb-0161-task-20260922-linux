"""tasktree 演示：正常完成轨迹 + 异常组与等待链 + 实际触发的拒绝边界。

全部确定性、离线、无 sleep；运行时间远小于 8 秒。
"""

from tasktree import (
    ScopeClosedError,
    TaskRuntime,
)


def show(receipt):
    print(f"  event#{receipt.seq} {receipt.op}(task={receipt.task_id}) "
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


def part3_rejection_boundary():
    print()
    print("=" * 64)
    print("第 3 部分：实际触发的拒绝/异常边界")
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


def part4_deadlines():
    print()
    print("=" * 64)
    print("第 4 部分：截止时间 —— 显式推进时钟，到期发起取消（仅请求）")
    print("=" * 64)
    rt = TaskRuntime(deadline=30)                 # 根 scope 期限 30
    show(rt.spawn(0, "fast", deadline=10))
    show(rt.spawn(0, "slow", deadline=20))
    show(rt.spawn(2, "slow-helper", deadline=20))
    show(rt.advance_time(10))                     # fast 到期
    show(rt.finish(1, "partial"))                 # 取消已投递 -> cancelled
    show(rt.advance_time(20))                     # slow 与其子到期
    show_pending(rt)
    # 到期取消只是请求：任务仍需显式退出，不会被直接标成完成
    show(rt.fail(2, TimeoutError("slow missed its deadline")))
    show(rt.finish(3, "helper stopped"))
    show(rt.close_scope(0))
    eg = rt.exception_group(0)
    print(f"  异常组: {eg!r}")
    print(f"  fast 结局: {rt.outcome(1)[0]}（取消取代迟到结果）")
    # 同一时刻的固定顺序：显式 finish 先于随后 advance 的到期扫描
    rt2 = TaskRuntime()
    rt2.spawn(0, "edge", deadline=5)
    rt2.finish(1, "in-time")                      # now=0 时先完成
    rt2.advance_time(5)                           # 随后才推进到期限
    print(f"  同时刻完成优先: edge 结局 = {rt2.outcome(1)}")
    # 实际触发的拒绝：子期限越过父期限
    rt2.spawn(0, "parent", deadline=10)
    try:
        rt2.spawn(2, "bad", deadline=99)
    except ValueError as e:
        print(f"  [拒绝] 子期限越过父期限 -> ValueError: {e}")
    # 实际触发的拒绝：时间倒退
    try:
        rt2.advance_time(4)
    except ValueError as e:
        print(f"  [拒绝] 时间倒退 -> ValueError: {e}")


if __name__ == "__main__":
    part1_normal()
    part2_group_and_waiting_chain()
    part3_rejection_boundary()
    part4_deadlines()
    print()
    print("demo 完成：以上为真实计算结果，无预录、无 sleep。")
