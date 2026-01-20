from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard
from parallelhassnes.storage.runs_store import RunsStore


class ScoreboardStateMissingSemanticsTests(unittest.TestCase):
    def test_missing_state_is_treated_as_inflight_queued_initializing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Minimal harness config for scoreboard thresholds.
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
                            "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
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

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batch_id = "b_missing_state"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-15T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            attempt_dir = store.paths.attempt_dir(batch_id, "j1", "s1", "attempt1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            # Intentionally do NOT create state.json.

            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {"s1": {"latest": {"run_id": "r1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}}},
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["running"]), 1)
            running = sb.get("running") or []
            self.assertTrue(any(x.get("step_id") == "s1" and x.get("current_item") == "queued/initializing" for x in running))


if __name__ == "__main__":
    unittest.main()

