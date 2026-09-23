"""tasktree 演示：正常完成轨迹 + 异常组与等待链 + 监督策略 + 实际触发的拒绝边界。

全部确定性、离线、无 sleep；运行时间远小于 90 秒。
"""

from tasktree import (
    ScopeClosedError,
    StrategyError,
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


def show_summary(rt, scope=0):
    w = rt.watcher_summary(scope)
    if w is None:
        print("  watcher: <未安装>")
        return
    print(f"  watcher: strategy={w['strategy']} state={w['state']} "
          f"satisfied={w['satisfied']} required={w['required']}")
    print(f"      members={w['members']} succeeded={w['succeeded']} "
          f"failed={w['failed']} cancelled={w['cancelled']}")
    if w["cleanup_failures"]:
        print(f"      cleanup_failures={w['cleanup_failures']}")
    print(f"      decided@event={w['decided_seq']} "
          f"finalized@event={w['closed_seq']} (None=尚在等清理确认)")


def part4_watch_strategies():
    print()
    print("=" * 64)
    print("第 4 部分：监督策略 —— all / first / quorum 与清理确认")
    print("=" * 64)

    # 4a. all 正常结果：三个成员全部成功（故意乱序完成，按事件序判定）
    print("-- 4a. all：全部成功才满足，事件顺序判定 --")
    rt = TaskRuntime()
    rt.spawn(0, "probe-east")
    rt.spawn(0, "probe-west")
    rt.spawn(0, "probe-north")
    rt.watch(0, "all")
    rt.finish(3, "north ok")
    rt.finish(1, "east ok")
    print(f"  完成 2/3 后 scope: {rt.status()[0]['scope']}（未达条件，不清理）")
    show(rt.finish(2, "west ok"))
    show_summary(rt)
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")

    # 4b. first：首个成功；成功的同时，清理中另一任务业务失败
    print()
    print("-- 4b. first：首个成功即满足，取消其余；清理确认期间出现业务失败 --")
    rt = TaskRuntime()
    rt.spawn(0, "candidate-a")
    rt.spawn(0, "candidate-b")
    rt.spawn(0, "candidate-c")
    rt.watch(0, "first")
    show(rt.finish(1, "a 抢到首个成功"))   # 判定满足，b/c 收到取消请求
    show_pending(rt)                       # b/c 尚未退出 -> 等待清理确认
    show(rt.fail(2, IOError("b 清理时落盘失败")))  # 业务失败优先于取消
    print(f"  b 失败后 scope 已关闭? {rt.is_closed(0)}（c 未退出，不能定稿）")
    show(rt.finish(3, "c 的迟到结果"))     # 取消已投递 -> cancelled
    show_summary(rt)
    group = rt.exception_group(0)
    print("  异常组（清理失败是真实业务异常，取消不入组）:")
    for exc in group.exceptions:
        print(f"      - {type(exc).__name__}: {exc}")

    # 4c. quorum 边界：3 个成员需 2 个成功；恰好 1 成 2 败 -> 不满足
    print()
    print("-- 4c. quorum=2/3 边界：成功 + 未决不足时判定不满足 --")
    rt = TaskRuntime()
    rt.spawn(0, "replica-1")
    rt.spawn(0, "replica-2")
    rt.spawn(0, "replica-3")
    rt.watch(0, "quorum", count=2)
    rt.finish(1, "ack")
    rt.fail(2, TimeoutError("replica-2 timeout"))
    print(f"  1 成 1 败 1 未决 -> state={rt.watcher_summary(0)['state']}"
          f"（仍可能，不提前判定）")
    rt.fail(3, TimeoutError("replica-3 timeout"))  # 成功上限 1 < 2
    show_summary(rt)
    print(f"  根 scope 已关闭: {rt.is_closed(0)}")

    # 4d. 实际触发的参数/状态错误
    print()
    print("-- 4d. 监督策略的实际拒绝边界 --")
    rt = TaskRuntime()
    rt.spawn(0, "only-one")
    try:
        rt.watch(0, "majority")
    except StrategyError as e:
        print(f"  [拒绝] 未知策略 -> StrategyError: {e}")
    try:
        rt.watch(0, "quorum")
    except StrategyError as e:
        print(f"  [拒绝] quorum 缺 count -> StrategyError: {e}")
    try:
        rt.watch(0, "quorum", count=2)
    except StrategyError as e:
        print(f"  [拒绝] count 超过成员数 -> StrategyError: {e}")
    rt.watch(0, "first")
    try:
        rt.spawn(0, "latecomer")
    except ScopeClosedError as e:
        print(f"  [拒绝] watch 后成员冻结再 spawn -> ScopeClosedError: {e}")
    try:
        rt.watch(0, "all")
    except StrategyError as e:
        print(f"  [拒绝] 同一 scope 重复 watch -> StrategyError: {e}")


if __name__ == "__main__":
    part1_normal()
    part2_group_and_waiting_chain()
    part4_watch_strategies()
    part3_rejection_boundary()
    print()
    print("demo 完成：以上为真实计算结果，无预录、无 sleep。")
