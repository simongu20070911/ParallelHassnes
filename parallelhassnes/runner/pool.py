from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class RunnerHandle:
    runner_id: str
    runner: Any
    capacity: int
    semaphore: threading.Semaphore


class RunnerPool:
    def __init__(self, runners: list[RunnerHandle]) -> None:
        if not runners:
            raise ValueError("RunnerPool requires at least one runner")
        self._runners = {r.runner_id: r for r in runners}
        self._order = sorted(self._runners.keys())

    def has_runner(self, runner_id: str) -> bool:
        return runner_id in self._runners

    def runner_ids(self) -> list[str]:
        return list(self._order)

    def pick_runner_id(
        self,
        preferred: str | None,
        avoid: set[str] | None,
    ) -> str:
        avoid = avoid or set()
        if preferred and preferred in self._runners and preferred not in avoid:
            return preferred
        for rid in self._order:
            if rid in avoid:
                continue
            return rid
        # Fallback: ignore avoid if it blocks everything.
        return self._order[0]

    def try_acquire(self, runner_id: str) -> bool:
        return self._runners[runner_id].semaphore.acquire(blocking=False)

    def try_acquire_any(self, preferred: str | None, avoid: set[str] | None) -> str | None:
        avoid = avoid or set()
        candidates: list[str] = []
        if preferred and preferred in self._runners and preferred not in avoid:
            candidates.append(preferred)
        for rid in self._order:
            if rid in avoid:
                continue
            if rid not in candidates:
                candidates.append(rid)
        for rid in candidates:
            if self.try_acquire(rid):
                return rid
        return None

    def release(self, runner_id: str) -> None:
        self._runners[runner_id].semaphore.release()

    def run_on(
        self,
        runner_id: str,
        fn: Callable[[Any], None],
    ) -> None:
        fn(self._runners[runner_id].runner)
