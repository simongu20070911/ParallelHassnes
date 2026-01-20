from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Event, Lock
from typing import Any, Callable


@dataclass(frozen=True)
class TickLoopConfig:
    interval_seconds: float
    concurrency_override: int | None
    use_fake_invoker: bool


class TickLoop:
    """
    Run the harness tick in a background loop.

    This is the "long-lived harness" mode implied by FUNCTIONALITY_SPEC.md:
    once batches are submitted (filesystem queue or API), the harness keeps
    ingesting/scheduling/executing without requiring an external tick driver.
    """

    def __init__(self, harness: Any, cfg: TickLoopConfig) -> None:
        self._harness = harness
        self._cfg = cfg
        self._stop = Event()
        self._tick_mu = Lock()

    def stop(self) -> None:
        self._stop.set()

    def tick_once(self) -> None:
        self.tick_once_with_overrides(
            concurrency_override=self._cfg.concurrency_override,
            use_fake_invoker=self._cfg.use_fake_invoker,
        )

    def tick_once_with_overrides(self, *, concurrency_override: int | None, use_fake_invoker: bool) -> None:
        # Serialize ticks: avoids concurrent scheduler/runners in the same process.
        with self._tick_mu:
            self._harness.tick_once(concurrency_override=concurrency_override, use_fake_invoker=use_fake_invoker)

    def run_forever(self, *, on_error: Callable[[BaseException], None] | None = None) -> None:
        interval = float(self._cfg.interval_seconds)
        if interval < 0.05:
            interval = 0.05
        while not self._stop.is_set():
            try:
                self.tick_once()
            except BaseException as e:
                if on_error is not None:
                    try:
                        on_error(e)
                    except Exception:
                        pass
                # Don't spin on persistent failures.
                time.sleep(min(1.0, interval))
            # Sleep after each tick to reduce load and provide periodic runner health updates.
            self._stop.wait(interval)
