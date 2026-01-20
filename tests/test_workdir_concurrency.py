from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness


class WorkdirConcurrencyTests(unittest.TestCase):
    def test_per_workdir_concurrency_limits_parallel_attempts(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Only one runner, but with capacity=2; concurrency=2 would normally run both jobs in parallel.
            # per_workdir_concurrency=1 should force serial execution => max_load_observed <= 1.
            job_workdir = str(root.resolve())
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
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
                            "runner_capacity": 2,
                            "runner_pool": {"shared_filesystem": True, "runner_ids": ["runner_only"]},
                            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                        },
                        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {job_workdir: 1}},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "concurrency": 2,
                "jobs": [
                    {"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x", "fake_sleep_seconds": 0.2}]},
                    {"job_id": "j2", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "y", "fake_sleep_seconds": 0.2}]},
                ],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            health = json.loads((runners_root / "runner_only" / "health.json").read_text(encoding="utf-8"))
            self.assertLessEqual(int(health.get("max_load_observed", 0)), 1)


if __name__ == "__main__":
    unittest.main()

