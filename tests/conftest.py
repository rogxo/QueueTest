"""Bounded thread helpers: propagate worker failures and never hang pytest."""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from threading import Thread
from time import monotonic
from typing import Any

import pytest


@dataclass
class Worker:
    thread: Thread | None = None
    result: Any = None
    error: BaseException | None = None

    def join(self, timeout: float = 30) -> Any:
        assert self.thread is not None
        self.thread.join(max(0, timeout))
        assert not self.thread.is_alive(), f"Thread {self.thread.name} did not finish"
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture
def spawn() -> Iterator[Callable[..., Worker]]:
    workers: list[Worker] = []

    def start(fn: Callable[..., Any], *args: Any, name: str | None = None) -> Worker:
        worker = Worker()

        def run() -> None:
            try:
                worker.result = fn(*args)
            except BaseException as exc:
                worker.error = exc

        worker.thread = Thread(target=run, name=name, daemon=True)
        workers.append(worker)
        worker.thread.start()
        return worker

    yield start
    deadline = monotonic() + 5
    for worker in workers:
        worker.join(deadline - monotonic())
