"""tasktree 演示：正常完成轨迹 + 异常组与等待链 + 实际触发的拒绝边界
+ 逻辑时钟与截止时间。

全部确定性、离线、无 sleep、无线程；运行时间远小于 90 秒。
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


def part4_logical_clock_and_deadlines():
    print()
    print("=" * 64)
    print("第 4 部分：逻辑时钟与截止时间 —— 到期只提出取消请求")
    print("=" * 64)
    rt = TaskRuntime()
    show(rt.spawn(0, "parent"))
    show(rt.spawn(1, "shielded-child"))
    show(rt.spawn(1, "fast-child"))
    show(rt.enter_shield(2))                  # 子任务进入屏蔽
    show(rt.set_deadline(1, 5))               # 父 scope 自己的截止时间
    print(f"  子任务不复制父截止时间: deadline of #2 = "
          f"{rt.status()[2]['deadline']}, #3 = {rt.status()[3]['deadline']}")

    # 截止时刻前完成 fast-child：其到期记录无关（它本就没有截止时间）
    show(rt.finish(3, "early-result"))

    # 模拟器逐步驱动：next_deadline 提供下一待触发时刻与到期来源
    print(f"  下一待触发: next_deadline() = {rt.next_deadline()}")
    nd = rt.next_deadline()
    show(rt.advance_time(nd[0]))              # 推进到 5：父先到期
    # 父到期按原路径传播：shielded-child 在 shield 内只记 pending
    st = rt.status()
    print(f"  父到期后: #1 cancel_delivered={st[1]['cancel_delivered']}, "
          f"#2 pending_cancels={st[2]['pending_cancels']} "
          f"(shield 内未投递), #3 outcome={st[3]['outcome']} (已完成不受影响)")
    show_pending(rt)

    # 到期不绕过 close_scope：根 scope 仍 open，须走既有收尾路径
    print(f"  根 scope 状态（到期不关闭 scope）: {st[0]['scope']}")
    show(rt.close_scope(0))
    show(rt.leave_shield(2))                  # 离开屏蔽：pending 投递
    show(rt.finish(2, "too-late"))            # -> cancelled
    show(rt.finish(1, "parent-late"))         # -> cancelled
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")
    print(f"  异常组（到期取消不是业务异常）: {rt.exception_group(0)}")

    # 截止时刻 finish 的先后顺序：同一任务，两种事件序两种结局
    print()
    print("  -- 截止时刻 finish 先后顺序 --")
    rt_a = TaskRuntime()
    rt_a.spawn(0, "x")
    rt_a.set_deadline(1, 5)
    rt_a.finish(1, "in-time")                 # 先 finish：到期记录失效
    rt_a.advance_time(5)
    print(f"  先 finish 再推进: outcome = {rt_a.outcome(1)}")
    rt_b = TaskRuntime()
    rt_b.spawn(0, "x")
    rt_b.set_deadline(1, 5)
    rt_b.advance_time(5)                      # 先推进到恰等时刻：到期
    rt_b.finish(1, "in-time")
    print(f"  先推进（恰等也算到期）再 finish: outcome = {rt_b.outcome(1)}")

    # 旧定时条目不得影响同 ID 的新一代定时
    print()
    print("  -- 旧定时条目失效 --")
    rt_c = TaskRuntime()
    rt_c.spawn(0, "y")
    rt_c.set_deadline(1, 5)
    show(rt_c.set_deadline(1, 20))            # 更新：旧条目 (5, #1) 作废
    show(rt_c.advance_time(10))               # 越过旧截止：不触发
    print(f"  越过旧截止后 cancel_delivered = "
          f"{rt_c.status()[1]['cancel_delivered']} (应为 False)")
    print(f"  下一待触发: next_deadline() = {rt_c.next_deadline()}")


if __name__ == "__main__":
    part1_normal()
    part2_group_and_waiting_chain()
    part3_rejection_boundary()
    part4_logical_clock_and_deadlines()
    print()
    print("demo 完成：以上为真实计算结果，无预录、无 sleep、无线程。")
