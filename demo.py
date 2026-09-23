"""tasktree 演示：原有取消/异常归并轨迹 + 成功策略（watch）的正常、边界与错误。

全部确定性、离线、无 sleep；运行时间为亚秒级。
"""

from tasktree import (
    ScopeClosedError,
    TaskRuntime,
    UnknownWatchError,
    WatchStateError,
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


def show_result(res):
    print(f"  汇总: status={res['status']} goal_met={res['goal_met']}")
    print(f"      members={res['members']} winners={res['winners']}")
    print(f"      successes={[s['task_id'] for s in res['successes']]}"
          f"  cancelled={res['cancelled']}")
    for f in res["failures"]:
        print(f"      failure task#{f['task_id']} "
              f"({f['phase']}阶段): {type(f['error']).__name__}: {f['error']}")


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
    print("第 3 部分：实际触发的拒绝/异常边界（原有）")
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


def part4_watch_strategies():
    print()
    print("=" * 64)
    print("第 4 部分：成功策略正常轨迹 —— all / first / n_succeed")
    print("=" * 64)

    # 4a. all：三个子任务全部成功（故意乱序 finish，汇总按事件序）
    print("  [all] 全部成功才算达标")
    rt = TaskRuntime()
    for name in ("fetch-a", "fetch-b", "fetch-c"):
        rt.spawn(0, name)
    wid = rt.watch(0, "all").watch_id
    rt.finish(1, "a")
    rt.finish(3, "c")
    print(f"      1、3 已成功，2 未完成时阶段: {rt.watches()[wid]['phase']}")
    rt.finish(2, "b")
    show_result(rt.watch_result(wid))

    # 4b. first：事件序中最先成功的成员胜出（不是创建序）
    print("  [first] 首个成功即达标（按输入事件序，非创建序）")
    rt = TaskRuntime()
    for name in ("slow-1", "slow-2", "fast-3"):
        rt.spawn(0, name)
    wid = rt.watch(0, "first").watch_id
    rt.finish(3, "fast-result")           # task#3 最先成功 -> winner=3
    rt.finish(1, "ignored")               # 清理期 finish -> cancelled
    rt.fail(2, RuntimeError("slow-2 error"))  # 清理期业务失败 -> 清理失败
    res = rt.watch_result(wid)
    show_result(res)
    assert res["winners"] == [3]

    # 4c. n_succeed：need=2，达到 2 个成功即取消其余
    print("  [n_succeed] need=2，够 2 个成功即取消其余")
    rt = TaskRuntime()
    for i in range(1, 5):
        rt.spawn(0, f"worker-{i}")
    wid = rt.watch(0, "n_succeed", need=2).watch_id
    rt.finish(2, "ok-2")
    rt.finish(4, "ok-4")                  # 第 2 个成功 -> 触发清理
    rt.finish(1, "late")                  # -> cancelled
    rt.finish(3, "late")                  # -> cancelled
    show_result(rt.watch_result(wid))


def part5_simultaneous_success_and_cleanup_failure():
    print()
    print("=" * 64)
    print("第 5 部分：同时成功与清理失败 + 等清理确认（含 shield）")
    print("=" * 64)
    rt = TaskRuntime()
    rt.spawn(0, "primary")      # 1
    rt.spawn(0, "backup")       # 2
    rt.spawn(1, "primary-io")   # 3：primary 的子任务
    wid = rt.watch(0, "first").watch_id
    rt.enter_shield(1)                        # primary 处于屏蔽
    show(rt.finish(2, "backup-done"))         # backup 先成功 -> 达标并触发清理
    print("      触发后状态:")
    print(f"        watch 阶段 = {rt.watches()[wid]['phase']}（等清理确认）")
    print(f"        task#1 pending_cancels = "
          f"{rt.status()[1]['pending_cancels']}（shield 内取消被挂起）")
    show(rt.leave_shield(1))                  # 取消才投递到 primary 整棵子树
    # 同一时间窗内：成功已记录，而被清理任务以业务失败退出（业务失败优先于取消）
    show(rt.fail(1, RuntimeError("primary crashed during cleanup")))
    show(rt.fail(3, TimeoutError("io failed during cleanup")))
    res = rt.watch_result(wid)
    show_result(res)
    print(f"  说明：策略达标（backup 成功），但清理期出现 {sum(1 for f in res['failures'] if f['phase']=='cleanup')} "
          f"个业务失败，status 如实记为 satisfied_with_cleanup_errors；"
          f"取消与失败分别入帐，异常组保留业务异常。")
    group = rt.exception_group(0)
    print(f"  异常组（按创建序）: "
          f"{[type(e).__name__ for e in group.exceptions]}")
    show_pending(rt)


def part6_watch_boundaries_and_errors():
    print()
    print("=" * 64)
    print("第 6 部分：watch 边界与实际触发的错误")
    print("=" * 64)

    # 6a. 边界：空 scope 上布防立即判定
    rt = TaskRuntime()
    rw = rt.watch(0, "all")
    print(f"  [边界] 空 scope + all -> 立即 {rw.watch_phase}，"
          f"status={rt.watch_result(rw.watch_id)['status']}（0 个成员 vacuously satisfied）")
    rt2 = TaskRuntime()
    wid = rt2.watch(0, "first").watch_id
    print(f"  [边界] 空 scope + first -> status="
          f"{rt2.watch_result(wid)['status']}（无成员可成功）")

    # 6b. 边界：need=1，先失败后成功仍达标
    rt = TaskRuntime()
    rt.spawn(0, "a")
    rt.spawn(0, "b")
    wid = rt.watch(0, "n_succeed", need=1).watch_id
    rt.fail(1, ValueError("a failed"))
    rt.finish(2, "b ok")
    print(f"  [边界] need=1 先失败后成功 -> "
          f"status={rt.watch_result(wid)['status']}, winners="
          f"{rt.watch_result(wid)['winners']}")

    # 6c. 实际错误：非法策略 / 非法 need
    rt = TaskRuntime()
    try:
        rt.watch(0, "majority")
    except ValueError as e:
        print(f"  [拒绝] 未知策略 -> ValueError: {e}")
    try:
        rt.watch(0, "n_succeed", need=0)
    except ValueError as e:
        print(f"  [拒绝] need=0 -> ValueError: {e}")
    try:
        rt.watch(0, "all", need=2)
    except ValueError as e:
        print(f"  [拒绝] all 搭配 need -> ValueError: {e}")

    # 6d. 实际错误：watching 期间 spawn 被冻结拒绝，且不落半更新
    rt = TaskRuntime()
    rt.spawn(0, "a")
    rt.spawn(0, "b")
    rt.watch(0, "all")
    try:
        rt.spawn(0, "c")
    except WatchStateError as e:
        print(f"  [拒绝] watching 期间 spawn -> WatchStateError: {e}")
    print(f"      拒绝后 watch 仍为: {rt.watches()[1]['phase']}，"
          f"任务数仍为 {len(rt.status())}（无半更新）")

    # 6e. 实际错误：清理确认前取汇总 / 未知 watch_id
    rt = TaskRuntime()
    rt.spawn(0, "a")
    rt.spawn(0, "b")
    wid = rt.watch(0, "all").watch_id
    rt.fail(1, ValueError("boom"))        # cleaning，等成员 2 退出
    try:
        rt.watch_result(wid)
    except WatchStateError as e:
        print(f"  [拒绝] 清理未确认取汇总 -> WatchStateError: {e}")
    try:
        rt.watch_result(999)
    except UnknownWatchError as e:
        print(f"  [拒绝] 未知 watch_id -> UnknownWatchError: {e}")
    rt.finish(2, None)
    print(f"      成员 2 退出后汇总就绪: "
          f"status={rt.watch_result(wid)['status']}")


if __name__ == "__main__":
    part1_normal()
    part2_group_and_waiting_chain()
    part3_rejection_boundary()
    part4_watch_strategies()
    part5_simultaneous_success_and_cleanup_failure()
    part6_watch_boundaries_and_errors()
    print()
    print("demo 完成：以上为真实计算结果，无预录、无 sleep。")
