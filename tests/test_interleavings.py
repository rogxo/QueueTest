"""Force critical scheduling windows instead of relying on lucky races."""

import gc
import weakref
from contextlib import contextmanager
from threading import Event, current_thread

import pytest

from lockfree_queue import LockFreeQueue


@contextmanager
def pause_atomic(monkeypatch, target, method, *, after=False):
    reached, resume = Event(), Event()
    original = getattr(type(target), method)

    def wrapped(self, *args):
        if self is target and current_thread().name == "paused" and not reached.is_set():
            if after:
                result = original(self, *args)
            reached.set()
            assert resume.wait(15), "Test did not release paused worker"
            return result if after else original(self, *args)
        return original(self, *args)

    with monkeypatch.context() as patch:
        patch.setattr(type(target), method, wrapped)
        try:
            yield reached
        finally:
            resume.set()


@pytest.mark.parametrize("helper", ["consumer", "producer"])
def test_other_threads_help_a_producer_paused_after_link(monkeypatch, spawn, helper):
    queue = LockFreeQueue()
    link = queue._nodes[queue._tail.load()].next
    with pause_atomic(monkeypatch, link, "compare_exchange", after=True) as reached:
        producer = spawn(queue.put, "first", name="paused")
        assert reached.wait(5)

        def make_progress():
            assert not queue.empty()  # head == tail does not necessarily mean empty.
            if helper == "consumer":
                assert queue.get() == "first"
                queue.put("second")
            else:
                queue.put("second")
                assert queue.get() == "first"
            assert queue.get() == "second"

        spawn(make_progress).join(5)
        assert producer.thread.is_alive()
    producer.join()
    assert queue.empty()
    assert len(queue._nodes) == 1


def test_other_threads_progress_while_consumer_is_paused_after_head_cas(monkeypatch, spawn):
    queue = LockFreeQueue()
    queue.put("first")
    queue.put("second")
    with pause_atomic(monkeypatch, queue._head, "compare_exchange", after=True) as reached:
        consumer = spawn(queue.get, name="paused")
        assert reached.wait(5)

        def make_progress():
            assert queue.get() == "second"
            queue.put("third")
            assert queue.get() == "third"

        spawn(make_progress).join(5)
        assert consumer.thread.is_alive()
    assert consumer.join() == "first"
    assert queue.empty()
    assert len(queue._nodes) == 1


def test_losing_consumer_retries_instead_of_returning_duplicate(monkeypatch, spawn):
    queue = LockFreeQueue()
    queue.put("first")
    with pause_atomic(monkeypatch, queue._head, "compare_exchange") as reached:
        consumer = spawn(queue.get, name="paused")
        assert reached.wait(5)

        def win_race():
            assert queue.get() == "first"
            queue.put("second")

        spawn(win_race).join(5)
    assert consumer.join() == "second"
    assert queue.empty()


def test_losing_producer_retries_instead_of_overwriting_a_link(monkeypatch, spawn):
    queue = LockFreeQueue()
    link = queue._nodes[queue._tail.load()].next
    with pause_atomic(monkeypatch, link, "compare_exchange") as reached:
        producer = spawn(queue.put, "paused", name="paused")
        assert reached.wait(5)
        spawn(queue.put, "winner").join(5)
    producer.join()
    assert queue.get() == "winner"
    assert queue.get() == "paused"
    assert queue.empty()


@pytest.mark.parametrize("has_node_reference", [False, True])
def test_consumer_survives_retired_head(monkeypatch, spawn, has_node_reference):
    queue = LockFreeQueue()
    for item in range(3):
        queue.put(item)
    old_head = queue._nodes[queue._head.load()]
    old_ref = weakref.ref(old_head)
    target = old_head.next if has_node_reference else queue._head
    del old_head
    with pause_atomic(monkeypatch, target, "load", after=not has_node_reference) as reached:
        consumer = spawn(queue.get, name="paused")
        assert reached.wait(5)

        def retire_nodes():
            assert queue.get() == 0
            assert queue.get() == 1

        spawn(retire_nodes).join(5)
        gc.collect()
        assert (old_ref() is not None) == has_node_reference
    assert consumer.join() == 2
    gc.collect()
    assert old_ref() is None
    assert len(queue._nodes) == 1


def test_producer_retries_after_tail_is_retired(monkeypatch, spawn):
    queue = LockFreeQueue()
    with pause_atomic(monkeypatch, queue._tail, "load", after=True) as reached:
        producer = spawn(queue.put, "paused", name="paused")
        assert reached.wait(5)

        def retire_tail():
            queue.put("other")
            assert queue.get() == "other"

        spawn(retire_tail).join(5)
    producer.join()
    assert queue.get() == "paused"
    assert queue.empty()


@pytest.mark.parametrize("has_node_reference", [False, True])
def test_empty_retries_after_snapshot_head_is_retired(monkeypatch, spawn, has_node_reference):
    queue = LockFreeQueue()
    queue.put("first")
    target = queue._nodes[queue._head.load()].next if has_node_reference else queue._head
    with pause_atomic(monkeypatch, target, "load", after=True) as reached:
        reader = spawn(queue.empty, name="paused")
        assert reached.wait(5)

        def change_queue():
            assert queue.get() == "first"
            queue.put("second")

        spawn(change_queue).join(5)
    assert reader.join() is False
    assert queue.get() == "second"
