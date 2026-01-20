from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.storage.runs_store import RunsStore


class RunnerHealthPeriodicTests(unittest.TestCase):
    def test_health_updates_while_running(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            runner_id = "runner_health_test"
            harness_cfg = {
                "harness_config_version": "hc_test",
                "written_at": "2026-01-15T00:00:00Z",
                "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                "defaults": {
                    "execution_policy": {},
                    "timeouts": {},
                    "retries": {"max_attempts": 1},
                    "retention_policy": {},
                    "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
                    "workspace_policy": {"mode": "shared"},
                    "runner_capacity": 1,
                    "runner_pool": {"shared_filesystem": True, "runner_ids": [runner_id]},
                    "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                },
                "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
            }

            runner = Runner(
                store=store,
                cfg=RunnerConfig(runner_id=runner_id, use_fake_invoker=True, baseline_output_schema_path=str((root / "run_report.schema.json").resolve())),
                harness_cfg=harness_cfg,
            )

            # Seed minimal batch/job/step.
            batch_id, job_id, step_id = "b", "j", "s"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)

            health_path = store.paths.runner_health_path(runner_id)
            self.assertTrue(health_path.exists())
            mtime0 = health_path.stat().st_mtime

            def run_step() -> None:
                runner.execute_step(
                    batch={"batch_id": batch_id, "effective_defaults": {}, "jobs": []},
                    job={"job_id": job_id, "working_directory": ".", "steps": []},
                    step={"step_id": step_id, "prompt": "x", "fake_sleep_seconds": 2.2},
                    job_workdir=str(root),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(root),
                    current={"batch_id": batch_id, "job_id": job_id, "updated_at": "2026-01-15T00:00:00Z", "steps": {}},
                )

            t = threading.Thread(target=run_step)
            t.start()

            # Wait for at least one periodic heartbeat update to runner health while the step is still running.
            deadline = time.time() + 5
            saw_update = False
            while time.time() < deadline and t.is_alive():
                try:
                    m = health_path.stat().st_mtime
                    if m > mtime0:
                        saw_update = True
                        break
                except FileNotFoundError:
                    pass
                time.sleep(0.1)

            t.join(timeout=10)
            self.assertFalse(t.is_alive())
            self.assertTrue(saw_update, "runner health.json was not updated while the attempt was running")

            # Sanity: health file remains valid JSON.
            obj = json.loads(health_path.read_text(encoding="utf-8"))
            self.assertEqual(obj.get("runner_id"), runner_id)


if __name__ == "__main__":
    unittest.main()

