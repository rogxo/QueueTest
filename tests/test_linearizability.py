"""Check short concurrent histories against an independent sequential FIFO.

Find a legal ordering respecting every completed-before-invoked relationship.
Long stress tests detect loss/duplication; these short histories check atomicity.
"""

import random
from dataclasses import dataclass
from functools import cache
from itertools import count
from threading import Barrier
from time import sleep

import pytest

from lockfree_queue import Empty, LockFreeQueue

EMPTY = "<empty>"


@dataclass(frozen=True)
class Operation:
    kind: str
    argument: int | None
    result: object
    start: int
    end: int


def is_linearizable(history):
    predecessors = [
        sum(1 << index for index, other in enumerate(history) if other.end < operation.start)
        for operation in history
    ]
    complete = (1 << len(history)) - 1

    @cache
    def search(done, fifo):
        if done == complete:
            return True
        for index, operation in enumerate(history):
            bit = 1 << index
            if done & bit or predecessors[index] & ~done:
                continue
            next_fifo = fifo
            if operation.kind == "put":
                if operation.result is not None:
                    continue
                next_fifo = (*fifo, operation.argument)
            elif operation.kind == "empty":
                if operation.result != (not fifo):
                    continue
            else:
                expected = fifo[0] if fifo else EMPTY
                if operation.kind == "try_get":
                    expected = (True, fifo[0]) if fifo else (False, None)
                if operation.result != expected:
                    continue
                next_fifo = fifo[1:]
            if search(done | bit, next_fifo):
                return True
        return False

    return search(0, ())


@pytest.mark.parametrize(
    "history,valid",
    [
        ([Operation("get", None, 1, 0, 1), Operation("put", 1, None, 2, 3)], False),
        (
            [
                Operation("put", 1, None, 0, 1),
                Operation("put", 2, None, 2, 3),
                Operation("get", None, 2, 4, 5),
            ],
            False,
        ),
        (
            [
                Operation("put", 1, None, 0, 1),
                Operation("get", None, 1, 2, 5),
                Operation("get", None, 1, 3, 4),
            ],
            False,
        ),
        ([Operation("put", 1, None, 0, 1), Operation("empty", None, True, 2, 3)], False),
        (
            [
                Operation("put", 1, None, 0, 3),
                Operation("put", 2, None, 1, 2),
                Operation("get", None, 2, 4, 5),
                Operation("get", None, 1, 6, 7),
            ],
            True,
        ),
        ([Operation("put", None, None, 0, 1), Operation("get", None, None, 2, 3)], True),
    ],
)
def test_history_checker_examples(history, valid):
    assert is_linearizable(history) is valid


@pytest.mark.parametrize("seed", range(20))
def test_concurrent_histories_are_linearizable(spawn, seed):
    queue = LockFreeQueue()
    ticks = count()
    start = Barrier(4)

    def run(worker_id):
        rng = random.Random(seed * 10 + worker_id)
        local_history = []
        start.wait(timeout=10)
        for index in range(4):
            kind = rng.choice(["put", "put", "get", "try_get", "empty"])
            argument = worker_id * 10 + index if kind == "put" else None
            if rng.random() < 0.3:
                argument = None
            invocation = next(ticks)
            sleep(0)
            try:
                result = queue.put(argument) if kind == "put" else getattr(queue, kind)()
            except Empty:
                result = EMPTY
            completion = next(ticks)
            local_history.append(Operation(kind, argument, result, invocation, completion))
        return local_history

    workers = [spawn(run, index) for index in range(3)]
    start.wait(timeout=10)
    history = [operation for worker in workers for operation in worker.join()]
    # Include the remaining contents and a final empty result in the history,
    # so incorrect enqueues cannot hide behind a lack of later consumers.
    while True:
        invocation = next(ticks)
        try:
            result = queue.get()
        except Empty:
            history.append(Operation("get", None, EMPTY, invocation, next(ticks)))
            break
        history.append(Operation("get", None, result, invocation, next(ticks)))
    assert is_linearizable(history), f"Non-linearizable history (seed={seed}): {history}"
