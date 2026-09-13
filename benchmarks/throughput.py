"""Reproducible MPMC transfer benchmark; run after installing the project.

python benchmarks/throughput.py --producers 4 --consumers 4 --items 1000
"""

import argparse
import json
import platform
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from queue import Empty, Queue, SimpleQueue
from threading import Barrier, Event
from time import perf_counter, sleep

from lockfree_queue import LockFreeQueue


def positive_int(value):
    result = int(value)
    if result < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def measure(factory, producers, consumers, items, timeout):
    queue = factory()
    gate = Barrier(producers + consumers + 1)
    stop = Event()
    sentinel = object()

    def produce(producer_id):
        gate.wait(timeout=timeout)
        for sequence in range(items):
            if stop.is_set():
                return
            queue.put_nowait(producer_id * items + sequence)

    def consume():
        received = []
        empty_polls = 0
        gate.wait(timeout=timeout)
        while not stop.is_set():
            try:
                item = queue.get_nowait()
            except Empty:
                empty_polls += 1
                sleep(0)
                continue
            if item is sentinel:
                return received, empty_polls
            received.append(item)
        return received, empty_polls

    with ThreadPoolExecutor(max_workers=producers + consumers) as pool:
        producer_jobs = [pool.submit(produce, index) for index in range(producers)]
        consumer_jobs = [pool.submit(consume) for _ in range(consumers)]
        started = perf_counter()
        deadline = started + timeout
        try:
            gate.wait(timeout=timeout)
            for job in producer_jobs:
                job.result(timeout=max(0, deadline - perf_counter()))
            for _ in range(consumers):
                queue.put_nowait(sentinel)
            batches = [
                job.result(timeout=max(0, deadline - perf_counter())) for job in consumer_jobs
            ]
            elapsed = perf_counter() - started
        finally:
            stop.set()
            gate.abort()

    count = producers * items
    received = Counter(item for batch, _ in batches for item in batch)
    if received != Counter(range(count)):
        raise RuntimeError("Lost or duplicated messages")
    return {
        "queue": factory.__name__,
        "messages": count,
        "seconds": round(elapsed, 4),
        "messages_per_second": round(count / elapsed),
        "empty_polls": sum(polls for _, polls in batches),
        "delivery_verified": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--producers", type=positive_int, default=4)
    parser.add_argument("--consumers", type=positive_int, default=4)
    parser.add_argument("--items", type=positive_int, default=1000, help="messages per producer")
    parser.add_argument("--rounds", type=positive_int, default=3)
    parser.add_argument("--timeout", type=positive_int, default=120, help="seconds per round")
    args = parser.parse_args()
    print(
        json.dumps(
            {"python": platform.python_version(), "platform": platform.platform(), **vars(args)}
        ),
        flush=True,
    )
    for factory in (LockFreeQueue, SimpleQueue, Queue):
        for round_number in range(1, args.rounds + 1):
            result = measure(factory, args.producers, args.consumers, args.items, args.timeout)
            print(json.dumps({"round": round_number, **result}), flush=True)


if __name__ == "__main__":
    main()
