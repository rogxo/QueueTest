"""The atomics/patomic backend exposes hardware lock-free operations.

Use sequential consistency throughout. Never substitute a Python check followed
by assignment for compare_exchange, or fall back to a mutex on unsupported CPUs.
"""

import atomics

MAX_TOKEN = (1 << 64) - 1


class AtomicUInt64:
    """An owning, aligned unsigned 64-bit atomic; zero represents a null link."""

    __slots__ = ("_value",)

    def __init__(self, value: int = 0) -> None:
        self._value = atomics.atomic(width=8, atype=atomics.UINT)
        required = {atomics.OpType.LOAD, atomics.OpType.STORE, atomics.OpType.CMPXCHG_STRONG}
        if not required.issubset(self._value.ops_supported):
            raise RuntimeError("This platform does not support lock-free 64-bit load/store/CAS")
        self._value.store(value)

    def load(self) -> int:
        return self._value.load()

    def compare_exchange(self, expected: int, desired: int) -> bool:
        return self._value.cmpxchg_strong(expected, desired).success
