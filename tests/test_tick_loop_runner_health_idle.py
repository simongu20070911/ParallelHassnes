from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.runtime.tick_loop import TickLoop, TickLoopConfig


class TickLoopRunnerHealthIdleTests(unittest.TestCase):
    def test_tick_loop_updates_runner_health_when_idle(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"

            # Baseline schema is required by RunnerPool creation.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            old_env = os.environ.get("PARALLELHASSNES_RUNNER_IDS")
            os.environ["PARALLELHASSNES_RUNNER_IDS"] = "runner_only"
            try:
                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                loop = TickLoop(harness, TickLoopConfig(interval_seconds=0.05, concurrency_override=None, use_fake_invoker=True))

                loop.tick_once()
                health_path = runners_root / "runner_only" / "health.json"
                self.assertTrue(health_path.exists())
                h1 = json.loads(health_path.read_text(encoding="utf-8"))
                t1 = h1.get("last_seen_at")
                self.assertIsInstance(t1, str)

                time.sleep(0.02)
                loop.tick_once()
                h2 = json.loads(health_path.read_text(encoding="utf-8"))
                t2 = h2.get("last_seen_at")
                self.assertIsInstance(t2, str)
                self.assertNotEqual(t1, t2)
            finally:
                if old_env is None:
                    os.environ.pop("PARALLELHASSNES_RUNNER_IDS", None)
                else:
                    os.environ["PARALLELHASSNES_RUNNER_IDS"] = old_env


if __name__ == "__main__":
    unittest.main()

