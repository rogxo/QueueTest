"""Native-CAS Michael-Scott queue for threads in GIL-enabled CPython."""

from ._queue import Empty, LockFreeQueue

__all__ = ["Empty", "LockFreeQueue"]
