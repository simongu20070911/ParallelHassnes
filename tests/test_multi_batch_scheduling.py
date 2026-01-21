from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.pool import RunnerHandle, RunnerPool
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.scheduler.scheduler import Scheduler, SchedulerConfig
from parallelhassnes.storage.runs_store import RunsStore


def _test_harness_cfg(runner_ids: list[str]) -> dict:
    return {
        "harness_config_version": "hc_test",
        "written_at": "2026-01-14T00:00:00Z",
        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
        "defaults": {
            "execution_policy": {},
            "timeouts": {},
            "retries": {"max_attempts": 1},
            "retention_policy": {},
            "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
            "workspace_policy": {"mode": "shared"},
            "runner_capacity": 1,
            "runner_pool": {"shared_filesystem": True, "runner_ids": runner_ids},
            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
        },
        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
    }


class MultiBatchSchedulingTests(unittest.TestCase):
    def test_multi_batch_interleaves_across_batches(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            cfg = _test_harness_cfg(runner_ids=["runner_a", "runner_b"])

            # Two independent batches, each with a single step that sleeps long enough to observe overlap.
            def write_batch(batch_id: str) -> None:
                batch_dir = runs_root / batch_id
                batch_dir.mkdir(parents=True, exist_ok=True)
                (batch_dir / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2026-01-14T00:00:00Z",
                            "harness_config_version": cfg["harness_config_version"],
                            "batch_goal_summary": ("word " * 200).strip(),
                            "working_root": str(root),
                            "concurrency": 1,
                            "effective_defaults": {},
                            "jobs": [
                                {
                                    "job_id": "j1",
                                    "working_directory": ".",
                                    "steps": [{"step_id": "s1", "prompt": "x", "fake_sleep_seconds": 0.5}],
                                }
                            ],
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

            batch_a = "batch_a"
            batch_b = "batch_b"
            write_batch(batch_a)
            write_batch(batch_b)

            runner_a = Runner(
                store=store,
                cfg=RunnerConfig(
                    runner_id="runner_a",
                    use_fake_invoker=True,
                    baseline_output_schema_path=str((root / "run_report.schema.json").resolve()),
                ),
                harness_cfg=cfg,
            )
            runner_b = Runner(
                store=store,
                cfg=RunnerConfig(
                    runner_id="runner_b",
                    use_fake_invoker=True,
                    baseline_output_schema_path=str((root / "run_report.schema.json").resolve()),
                ),
                harness_cfg=cfg,
            )
            pool = RunnerPool(
                [
                    RunnerHandle(runner_id="runner_a", runner=runner_a, capacity=1, semaphore=threading.Semaphore(1)),
                    RunnerHandle(runner_id="runner_b", runner=runner_b, capacity=1, semaphore=threading.Semaphore(1)),
                ]
            )

            sched = Scheduler(store=store, cfg=SchedulerConfig(concurrency_override=1, multi_batch=True), harness_cfg=cfg)
            t = threading.Thread(target=lambda: sched.run_until_idle([batch_a, batch_b], runner_pool=pool))
            t.start()

            def attempts_started(batch_id: str) -> list[Path]:
                ad = store.paths.attempts_dir(batch_id, "j1", "s1")
                if not ad.exists():
                    return []
                return [p for p in ad.iterdir() if p.is_dir()]

            # Wait for batch_a to start running.
            deadline = time.monotonic() + 2.0
            a_attempt = None
            while time.monotonic() < deadline:
                started = attempts_started(batch_a)
                if started:
                    a_attempt = started[0]
                    st = store.read_attempt_state(a_attempt) or {}
                    if st.get("status") == "running":
                        break
                time.sleep(0.01)
            self.assertIsNotNone(a_attempt, "batch_a attempt did not start")

            # While batch_a is still running, batch_b should also start promptly in multi-batch mode.
            b_deadline = time.monotonic() + 0.3
            while time.monotonic() < b_deadline:
                if attempts_started(batch_b):
                    break
                time.sleep(0.01)
            self.assertTrue(attempts_started(batch_b), "batch_b attempt did not start while batch_a was running")

            t.join(timeout=5)
            self.assertFalse(t.is_alive(), "scheduler did not become idle")


if __name__ == "__main__":
    unittest.main()

