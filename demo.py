"""Deterministic demo of the tasktree library.

Scenario (fixed input, no randomness, no I/O, no sleep):

* Part 1 (normal result): a small task tree where two tasks fail with
  business exceptions and one is cancelled.  The demo prints the waiting
  chain (why scopes have not completed yet) after each event and the
  final aggregated ExceptionGroup, ordered by task creation order.
* Part 2 (rejection boundary): real, actually-raised rejections —
  a conflicting late finish, a spawn into a closed scope, and a NaN
  result — each caught and printed with its locatable message.

Run:  python demo.py
"""

from tasktree import (
    ResultConflictError,
    ScopeClosedError,
    InvalidInputError,
    TaskTree,
)


def show_events(tree, since):
    for ev in tree.events[since:]:
        print(
            f"  event #{ev.seq}: {ev.op}(task={ev.task}) -> {ev.status} "
            f"({ev.note}); task_status={ev.task_status}"
        )
        for reason in ev.waiting:
            print(f"      waiting: {reason}")


def part1():
    print("=== Part 1: normal run — exception group and waiting chain ===")
    tree = TaskTree()
    seen = 0

    fetch = tree.spawn(0, "fetch")      # id 1
    parse = tree.spawn(0, "parse")      # id 2
    upload = tree.spawn(fetch, "upload")  # id 3, child of fetch
    show_events(tree, seen)
    seen = len(tree.events)

    # 'parse' fails: its subtree is empty, so it completes immediately.
    tree.fail(parse, ValueError("malformed header"))
    # 'upload' shields itself; then 'fetch' fails, which requests
    # cancellation of its unfinished child — pending behind the shield.
    tree.enter_shield(upload)
    tree.fail(fetch, ConnectionError("connection reset"))
    # Closing the root scope cancels remaining unfinished children and
    # waits for the whole subtree to exit.
    tree.close_scope(0)
    print("-- after fail(parse) + shield(upload) + fail(fetch) + close_scope(root):")
    show_events(tree, seen)
    seen = len(tree.events)
    print("-- root waiting chain:")
    for reason in tree.pending_reasons(0):
        print(f"      {reason}")

    # Leaving the outermost shield delivers the pending cancel; the
    # whole subtree then exits and the root scope completes.
    tree.leave_shield(upload)
    print("-- after leave_shield(upload):")
    show_events(tree, seen)

    group = tree.exception_group(0)
    print("-- aggregated business exceptions (creation order):")
    for exc in group.exceptions:
        print(f"      {type(exc).__name__}: {exc}")
    print(f"  upload outcome: {tree.outcome(upload)['kind']} "
          f"(cancellation is not part of the group)")
    print(f"  root status: {tree.status(0)}")


def part2():
    print("\n=== Part 2: actually triggered rejection boundaries ===")
    tree = TaskTree()
    a = tree.spawn(0, "worker")
    tree.finish(a, result=1)
    try:
        tree.finish(a, result=2)  # conflicting late finish
    except ResultConflictError as exc:
        print(f"  rejected conflicting finish: {exc}")
    try:
        tree.spawn(a, "child-of-closed-scope")
    except ScopeClosedError as exc:
        print(f"  rejected spawn into closed scope: {exc}")
    b = tree.spawn(0, "sensor")
    try:
        tree.finish(b, result=float("nan"))
    except InvalidInputError as exc:
        print(f"  rejected NaN result: {exc}")
    print(f"  state unchanged after rejections: "
          f"worker outcome={tree.outcome(a)['kind']}, "
          f"sensor status={tree.status(b)}")


if __name__ == "__main__":
    part1()
    part2()
