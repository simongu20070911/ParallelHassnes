from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.harness.harness import Harness
from parallelhassnes.storage.runs_store import RunsStore


class SchedulerSuccessFallbackTests(unittest.TestCase):
    def test_depends_on_uses_by_run_id_success_when_latest_successful_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Minimal harness config.
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
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
                            "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"]},
                            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                        },
                        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            # Submit a Launch Table: step b depends on step a.
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "concurrency": 1,
                "jobs": [
                    {
                        "job_id": "j1",
                        "working_directory": ".",
                        "steps": [{"step_id": "a", "prompt": "x"}, {"step_id": "b", "prompt": "y", "depends_on": ["a"]}],
                    }
                ],
            }
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            h = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
            h.tick_once(concurrency_override=1, use_fake_invoker=True)

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batch_id = store.list_batches()[0]

            # Simulate a crash during pointer update: remove latest_successful but keep by_run_id succeeded entry.
            cur = store.read_current(batch_id, "j1") or {}
            step_a = (((cur.get("steps") or {}).get("a") or {}))
            self.assertTrue(isinstance(step_a, dict) and step_a.get("by_run_id"))
            step_a.pop("latest_successful", None)
            store.write_current(batch_id, "j1", cur)

            # Run another tick; scheduler should treat a as succeeded (via by_run_id) and schedule b.
            h.tick_once(concurrency_override=1, use_fake_invoker=True)

            cur2 = store.read_current(batch_id, "j1") or {}
            self.assertIn("b", (cur2.get("steps") or {}))
            self.assertTrue((((cur2.get("steps") or {}).get("b") or {}).get("latest_successful") is not None))


if __name__ == "__main__":
    unittest.main()

