"""Minimal thread example; run after installing the project."""

from concurrent.futures import ThreadPoolExecutor
from time import sleep

from lockfree_queue import Empty, LockFreeQueue


def main():
    queue = LockFreeQueue()
    sentinel = object()

    def produce(producer_id):
        for sequence in range(100):
            queue.put((producer_id, sequence))

    def consume():
        count = 0
        while True:
            try:
                item = queue.get()
            except Empty:
                sleep(0.001)
                continue
            if item is sentinel:
                return count
            count += 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        consumers = [pool.submit(consume) for _ in range(4)]
        producers = [pool.submit(produce, index) for index in range(4)]
        for producer in producers:
            producer.result()
        for _ in consumers:
            queue.put(sentinel)
        delivered = sum(consumer.result() for consumer in consumers)
    assert delivered == 400
    print(f"Delivered {delivered} messages")


if __name__ == "__main__":
    main()
