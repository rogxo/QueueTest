import os
import sys
from collections import Counter
from threading import Barrier, Event
from time import monotonic, sleep

import pytest

from lockfree_queue import Empty, LockFreeQueue
from lockfree_queue._atomic import AtomicUInt64


@pytest.mark.parametrize("producers,consumers", [(1, 1), (8, 1), (1, 8), (8, 8), (16, 16)])
@pytest.mark.stress
def test_concurrent_delivery_exactly_once_and_per_producer_fifo(spawn, producers, consumers):
    # Override for extended runs without making ordinary test runs enormous.
    items_per_producer = int(os.environ.get("QUEUE_STRESS_ITEMS", "150"))
    assert items_per_producer > 0
    queue = LockFreeQueue()
    start = Barrier(producers + consumers + 1)
    stop = Event()
    sentinel = object()

    def produce(producer_id):
        start.wait(timeout=10)
        for sequence in range(items_per_producer):
            if stop.is_set():
                return
            queue.put((producer_id, sequence))

    def consume():
        received = []
        start.wait(timeout=10)
        while not stop.is_set():
            try:
                item = queue.get()
            except Empty:
                # Waiting belongs to the application/test, not the queue.
                sleep(0)
                continue
            if item is sentinel:
                return received
            received.append(item)
        return received

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.0001)
    producer_workers = []
    consumer_workers = []
    deadline = monotonic() + float(os.environ.get("QUEUE_STRESS_TIMEOUT", "300"))
    try:
        producer_workers = [spawn(produce, index) for index in range(producers)]
        consumer_workers = [spawn(consume) for _ in range(consumers)]
        start.wait(timeout=10)
        for worker in producer_workers:
            worker.join(deadline - monotonic())
        for _ in range(consumers):
            queue.put(sentinel)
        batches = [worker.join(deadline - monotonic()) for worker in consumer_workers]
    finally:
        stop.set()
        start.abort()
        sys.setswitchinterval(old_interval)

    expected = Counter(
        (producer_id, sequence)
        for producer_id in range(producers)
        for sequence in range(items_per_producer)
    )
    assert Counter(item for batch in batches for item in batch) == expected
    # Each consumer's calls are ordered. Concatenating different consumers'
    # return logs would NOT reveal their global dequeue linearization order.
    for batch in batches:
        last_seen = {}
        for producer_id, sequence in batch:
            assert sequence > last_seen.get(producer_id, -1)
            last_seen[producer_id] = sequence
    assert queue.empty()
    assert len(queue._nodes) == 1


def test_simultaneous_consumers_of_one_item_have_one_winner(spawn):
    queue = LockFreeQueue()
    payload = object()
    queue.put(payload)
    start = Barrier(17)

    def consume_once():
        start.wait(timeout=10)
        return queue.try_get()

    workers = [spawn(consume_once) for _ in range(16)]
    start.wait(timeout=10)
    results = [worker.join() for worker in workers]
    assert sum(found for found, _ in results) == 1
    assert all(value is payload if found else value is None for found, value in results)
    assert queue.empty()


def test_native_cas_has_exactly_one_winner(spawn):
    atomic = AtomicUInt64()
    start = Barrier(17)

    def compete(value):
        start.wait(timeout=10)
        return value, atomic.compare_exchange(0, value)

    workers = [spawn(compete, value) for value in range(1, 17)]
    start.wait(timeout=10)
    winners = [value for value, won in (worker.join() for worker in workers) if won]
    assert winners == [atomic.load()]
