"""Michael-Scott FIFO with native CAS and CPython-managed node lifetimes.

The link/head/tail algorithm is lock-free, not wait-free. The entire Python
operation is NOT strictly lock-free: it uses the GIL, Python allocation and GC.
The registry relies on individual dict operations and next(itertools.count)
being atomic in ordinary GIL-enabled CPython. Native CAS can release the GIL.
"""

from __future__ import annotations

import sys
import sysconfig
from itertools import count
from queue import Empty
from typing import Generic, TypeVar, cast

from ._atomic import MAX_TOKEN, AtomicUInt64

T = TypeVar("T")
_CLEARED = object()


class _Node(Generic[T]):
    __slots__ = ("value", "next", "__weakref__")

    def __init__(self, value: T | object) -> None:
        self.value = value
        self.next = AtomicUInt64()


def _check_runtime() -> None:
    # Refuse free-threaded builds even if a C extension happened to enable the
    # GIL at import time; that configuration has not been validated here.
    if (
        sys.implementation.name != "cpython"
        or sysconfig.get_config_var("Py_GIL_DISABLED")
        or not getattr(sys, "_is_gil_enabled", lambda: True)()
    ):
        raise RuntimeError("LockFreeQueue requires a standard, GIL-enabled CPython build")


class LockFreeQueue(Generic[T]):
    """Unbounded multi-producer, multi-consumer FIFO for threads in one process.

    put()/put_nowait() enqueue one object. get()/get_nowait() dequeue one object,
    raising queue.Empty immediately when empty; they never wait for an item.
    Objects, including None, are transferred by reference.

    There are no application mutexes or spinlocks. Native CAS provides atomic
    link/head/tail updates. See the module documentation for runtime limits.
    """

    __slots__ = ("_head", "_tail", "_nodes", "_tokens")

    def __init__(self) -> None:
        _check_runtime()
        self._tokens = count(2)
        self._nodes: dict[int, _Node[T]] = {1: _Node(_CLEARED)}
        self._head = AtomicUInt64(1)
        self._tail = AtomicUInt64(1)

    def put_nowait(self, item: T) -> None:
        """Atomically append item; contention may cause retries.

        Raises OverflowError after exhausting the lifetime's 64-bit node IDs.
        IDs are never reused, even after a node has been reclaimed.
        """
        token = next(self._tokens)
        if token > MAX_TOKEN:
            raise OverflowError("Queue node IDs exhausted; create a new queue")

        node: _Node[T] = _Node(item)
        # Publish the owning reference BEFORE making its token reachable.
        self._nodes[token] = node
        while True:
            tail = self._tail.load()
            tail_node = self._nodes.get(tail)
            if tail_node is None:
                # A consumer retired it after the load. Reload, never dereference
                # a raw address. A successful lookup holds a strong reference.
                continue
            next_token = tail_node.next.load()
            if tail != self._tail.load():
                continue
            if next_token == 0:
                if tail_node.next.compare_exchange(0, token):
                    # Enqueue linearizes at the successful link CAS above.
                    # Anyone can finish advancing tail if this thread pauses.
                    self._tail.compare_exchange(tail, token)
                    return
            else:
                self._tail.compare_exchange(tail, next_token)

    def get_nowait(self) -> T:
        """Atomically remove the oldest item, or raise Empty without waiting."""
        while True:
            head = self._head.load()
            tail = self._tail.load()
            head_node = self._nodes.get(head)
            if head_node is None:
                continue
            next_token = head_node.next.load()
            if head != self._head.load():
                continue
            if head == tail:
                if next_token == 0:
                    # Empty linearizes at the validated null-link load.
                    raise Empty
                # An enqueuer has linked an item but has not advanced tail yet.
                self._tail.compare_exchange(tail, next_token)
                continue

            next_node = self._nodes.get(next_token)
            if next_node is None:
                continue
            # Read BEFORE the head CAS: the winner clears the new dummy's
            # payload. A reader of a cleared value necessarily loses its CAS.
            value = next_node.value
            if self._head.compare_exchange(head, next_token):
                # Dequeue linearizes at the successful head CAS above.
                next_node.value = _CLEARED
                del self._nodes[head]
                return cast(T, value)

    def try_get(self) -> tuple[bool, T | None]:
        """Return (True, item) or (False, None); None itself is a valid item."""
        try:
            return True, self.get_nowait()
        except Empty:
            return False, None

    def empty(self) -> bool:
        """Return a linearizable snapshot, which can immediately become stale.

        Consumers must call get_nowait/try_get directly instead of checking
        empty() and assuming that a later dequeue will succeed.
        """
        while True:
            head = self._head.load()
            node = self._nodes.get(head)
            if node is None:
                continue
            next_token = node.next.load()
            if head == self._head.load():
                return next_token == 0

    put = put_nowait
    get = get_nowait
