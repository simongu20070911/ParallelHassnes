from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard
from parallelhassnes.storage.runs_store import RunsStore


def _long_goal() -> str:
    return ("word " * 200).strip()


class ScoreboardDependencySuccessFallbackTests(unittest.TestCase):
    def test_depends_on_satisfied_by_by_run_id_success_without_latest_successful_pointer(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
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
                            "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"], "resume_base_transfer_enabled": False},
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

            batch_id = "batch_dep"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-15T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": _long_goal(),
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [
                                    {"step_id": "s1", "prompt": "x"},
                                    {"step_id": "s2", "prompt": "y", "depends_on": ["s1"]},
                                ],
                            }
                        ],
                        "effective_defaults": {},
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            # Current: s1 has a succeeded attempt recorded in by_run_id, but latest_successful pointer is missing.
            attempt_dir = runs_root / batch_id / "j1" / "steps" / "s1" / "attempts" / "20260115T000000Z_run_s1"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": "j1", "step_id": "s1", "run_id": "run_s1", "runner_id": "r1", "invocation": "exec"}) + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "state.json").write_text(
                json.dumps({"status": "succeeded", "started_at": "2026-01-15T00:00:00Z", "ended_at": "2026-01-15T00:01:00Z", "last_heartbeat_at": "2026-01-15T00:01:00Z", "exit_code": 0})
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)

            (runs_root / batch_id / "j1").mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "j1" / "current.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "job_id": "j1",
                        "updated_at": "2026-01-15T00:02:00Z",
                        "steps": {
                            "s1": {
                                "latest": {"run_id": "run_s1", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve())},
                                "by_run_id": {
                                    "run_s1": {
                                        "run_id": "run_s1",
                                        "attempt_dir": str(attempt_dir),
                                        "resume_base_dir": str((attempt_dir / "codex_home").resolve()),
                                        "status": "succeeded",
                                        "ended_at": "2026-01-15T00:01:00Z",
                                    }
                                },
                            },
                            "s2": {},
                        },
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            # s2 must be ready, not blocked, because s1 success can be inferred from by_run_id.
            blocked = [b for b in sb.get("blocked", []) if b.get("step_id") == "s2"]
            self.assertEqual(blocked, [])
            self.assertEqual(sb.get("counts", {}).get("ready"), 1)


if __name__ == "__main__":
    unittest.main()

