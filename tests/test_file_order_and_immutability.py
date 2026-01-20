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


class FileOrderAndImmutabilityTests(unittest.TestCase):
    def test_attempt_file_order_and_heartbeat_stops_after_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            harness_cfg = {
                "harness_config_version": "hc_test",
                "written_at": "2026-01-14T00:00:00Z",
                "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                "defaults": {
                    "execution_policy": {},
                    "timeouts": {},
                    "retries": {"max_attempts": 1},
                    "retention_policy": {},
                    "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
                    "workspace_policy": {"mode": "shared"},
                    "runner_capacity": 1,
                    "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"]},
                    "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                },
                "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
            }

            runner = Runner(
                store=store,
                cfg=RunnerConfig(runner_id="r1", use_fake_invoker=True, baseline_output_schema_path=str((root / "run_report.schema.json").resolve())),
                harness_cfg=harness_cfg,
            )

            batch_id, job_id, step_id = "b", "j", "s"

            def run_step() -> None:
                runner.execute_step(
                    batch={"batch_id": batch_id, "effective_defaults": {}, "jobs": []},
                    job={"job_id": job_id, "working_directory": ".", "steps": []},
                    step={"step_id": step_id, "prompt": "x", "fake_sleep_seconds": 1.2},
                    job_workdir=str(root),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(root),
                    current={"batch_id": batch_id, "job_id": job_id, "updated_at": "2026-01-14T00:00:00Z", "steps": {}},
                )

            t = threading.Thread(target=run_step)
            t.start()

            attempt_dir: Path | None = None
            deadline = time.time() + 5
            while time.time() < deadline and attempt_dir is None:
                cur = store.read_current(batch_id, job_id)
                latest = (((cur or {}).get("steps") or {}).get(step_id) or {}).get("latest") if cur else None
                if isinstance(latest, dict) and isinstance(latest.get("attempt_dir"), str):
                    attempt_dir = Path(latest["attempt_dir"])
                    break
                time.sleep(0.01)

            self.assertIsNotNone(attempt_dir)
            attempt_dir = attempt_dir or Path("unused")

            # During running: state/meta exist (meta may be momentarily missing if the runner crashed mid-creation);
            # final outputs do not yet exist.
            deadline2 = time.time() + 2
            while time.time() < deadline2 and not (attempt_dir / "meta.json").exists():
                time.sleep(0.01)
            self.assertTrue((attempt_dir / "state.json").exists())
            self.assertTrue((attempt_dir / "meta.json").exists())
            self.assertFalse((attempt_dir / "final.txt").exists())
            self.assertFalse((attempt_dir / "final.json").exists())

            meta_before = (attempt_dir / "meta.json").read_text(encoding="utf-8")
            meta_mtime_before = (attempt_dir / "meta.json").stat().st_mtime

            t.join(timeout=10)
            self.assertFalse(t.is_alive())

            # After terminalization: final outputs exist and state is terminal.
            self.assertTrue((attempt_dir / "final.txt").exists())
            self.assertTrue((attempt_dir / "final.json").exists())
            st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
            self.assertIn(st.get("status"), {"succeeded", "failed", "needs_attention", "canceled"})
            self.assertIsInstance(st.get("ended_at"), str)

            # meta.json remains write-once / unchanged.
            self.assertEqual((attempt_dir / "meta.json").read_text(encoding="utf-8"), meta_before)
            self.assertEqual((attempt_dir / "meta.json").stat().st_mtime, meta_mtime_before)

            # Heartbeat writer stops after terminalization: state.json should not revert to "running" afterwards.
            state_before = (attempt_dir / "state.json").read_text(encoding="utf-8")
            state_mtime_before = (attempt_dir / "state.json").stat().st_mtime
            time.sleep(1.2)
            self.assertEqual((attempt_dir / "state.json").read_text(encoding="utf-8"), state_before)
            self.assertEqual((attempt_dir / "state.json").stat().st_mtime, state_mtime_before)


if __name__ == "__main__":
    unittest.main()
