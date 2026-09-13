import gc
import random
import sys
import weakref
from collections import deque
from itertools import count
from queue import Empty as StandardEmpty
from types import SimpleNamespace

import pytest

from lockfree_queue import Empty, LockFreeQueue
from lockfree_queue import _queue as implementation
from lockfree_queue._atomic import MAX_TOKEN, AtomicUInt64


def test_empty_queue_and_aliases():
    queue = LockFreeQueue[int]()
    assert Empty is StandardEmpty
    assert queue.empty()
    assert queue.try_get() == (False, None)
    for get in (queue.get, queue.get_nowait):
        with pytest.raises(Empty):
            get()
    assert queue.put(1) is None
    assert queue.put_nowait(2) is None
    assert not queue.empty()
    assert queue.get() == 1
    assert queue.get_nowait() == 2
    assert queue.empty()


@pytest.mark.parametrize("payload", [None, False, 0, "", b"", [], {}, object()])
def test_arbitrary_payloads_keep_identity(payload):
    queue = LockFreeQueue()
    queue.put(payload)
    found, actual = queue.try_get()
    assert found
    assert actual is payload
    assert queue.try_get() == (False, None)


def test_payload_does_not_need_hashing_or_comparison():
    class Opaque:
        def __hash__(self):
            raise AssertionError("Payload was hashed")

        def __eq__(self, other):
            raise AssertionError("Payload was compared")

    queue = LockFreeQueue()
    payload = Opaque()
    queue.put(payload)
    assert queue.get() is payload


def test_mutation_is_visible_by_reference():
    queue = LockFreeQueue[list[int]]()
    payload = [1]
    queue.put(payload)
    payload.append(2)
    assert queue.get() == [1, 2]


def test_randomized_sequential_fifo_against_deque():
    rng = random.Random(20260913)
    queue = LockFreeQueue()
    model = deque()
    for index in range(5000):
        if rng.random() < 0.55:
            item = None if index % 7 == 0 else index
            queue.put(item)
            model.append(item)
        elif model:
            assert queue.get() == model.popleft()
        else:
            with pytest.raises(Empty):
                queue.get()
        assert queue.empty() == (not model)
    while model:
        assert queue.get() == model.popleft()
    assert queue.empty()


def test_independent_instances():
    first, second = LockFreeQueue(), LockFreeQueue()
    first.put("first")
    second.put("second")
    assert first.get() == "first"
    assert first.empty()
    assert second.get() == "second"


def test_old_nodes_and_payloads_are_reclaimed():
    class Payload:
        pass

    queue = LockFreeQueue()
    node_refs = []
    payload_refs = []
    for _ in range(1000):
        payload = Payload()
        payload_refs.append(weakref.ref(payload))
        queue.put(payload)
        node_refs.append(weakref.ref(queue._nodes[queue._tail.load()]))
        assert queue.get() is payload
    del payload
    gc.collect()
    assert all(ref() is None for ref in payload_refs)
    assert sum(ref() is not None for ref in node_refs) == 1
    assert len(queue._nodes) == 1  # Only the current dummy, not queue history.


def test_node_ids_never_wrap_or_reuse():
    queue = LockFreeQueue()
    queue._tokens = count(MAX_TOKEN)
    queue.put("last ID")
    assert queue.get() == "last ID"
    for _ in range(2):
        with pytest.raises(OverflowError, match="IDs exhausted"):
            queue.put("must not wrap")
    assert queue.empty()
    assert len(queue._nodes) == 1


def test_node_allocation_failure_leaves_queue_usable(monkeypatch):
    queue = LockFreeQueue()
    queue.put("existing")

    def fail(value):
        raise MemoryError("injected allocation failure")

    with monkeypatch.context() as patch:
        patch.setattr(implementation, "_Node", fail)
        with pytest.raises(MemoryError):
            queue.put("failed")
    queue.put("later")
    assert queue.get() == "existing"
    assert queue.get() == "later"
    assert queue.empty()
    assert len(queue._nodes) == 1


@pytest.mark.parametrize("runtime", ["pypy", "free_threaded", "gil_disabled"])
def test_unsupported_runtimes_fail_explicitly(monkeypatch, runtime):
    if runtime == "pypy":
        monkeypatch.setattr(sys, "implementation", SimpleNamespace(name="pypy"))
    elif runtime == "free_threaded":
        monkeypatch.setattr(implementation.sysconfig, "get_config_var", lambda name: 1)
    else:
        monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    with pytest.raises(RuntimeError, match="GIL-enabled CPython"):
        LockFreeQueue()


def test_backend_rejects_missing_native_cas(monkeypatch):
    from lockfree_queue import _atomic

    monkeypatch.setattr(
        _atomic.atomics, "atomic", lambda **kwargs: SimpleNamespace(ops_supported=[])
    )
    with pytest.raises(RuntimeError, match="64-bit"):
        AtomicUInt64()


def test_native_cas_success_failure_and_unsigned_boundary():
    atomic = AtomicUInt64()
    assert atomic.load() == 0
    assert not atomic.compare_exchange(1, 2)
    assert atomic.load() == 0
    assert atomic.compare_exchange(0, MAX_TOKEN)
    assert atomic.load() == MAX_TOKEN
    assert not atomic.compare_exchange(0, 3)
    assert atomic.compare_exchange(MAX_TOKEN, 1)
    assert atomic.load() == 1
