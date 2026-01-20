from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.ops.cancel import cancel_attempt
from parallelhassnes.recovery.recovery import recover_orphaned_running_attempts
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard
from parallelhassnes.storage.runs_store import RunsStore


def _write_harness_config(runs_root: Path) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
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


class InvariantTests(unittest.TestCase):
    def test_attempt_dir_immutable_after_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id, job_id, step_id, run_id = "b", "j", "s", "r1"
            attempt_dir = store.paths.attempt_dir(batch_id, job_id, step_id, "attempt1")
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "r1", "invocation": "exec"})
                + "\n",
                encoding="utf-8",
            )
            state_path = attempt_dir / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "status": "succeeded",
                        "started_at": "2026-01-14T00:00:00Z",
                        "ended_at": "2026-01-14T00:00:01Z",
                        "last_heartbeat_at": "2026-01-14T00:00:00Z",
                        "pid": None,
                        "errors": [],
                        "artifacts": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "final.txt").write_text("{}", encoding="utf-8")
            (attempt_dir / "final.json").write_text("{}", encoding="utf-8")

            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            store.write_current(
                batch_id,
                job_id,
                {
                    "batch_id": batch_id,
                    "job_id": job_id,
                    "updated_at": "2026-01-14T00:00:02Z",
                    "steps": {step_id: {"latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}}},
                },
            )

            before = {p: p.stat().st_mtime for p in attempt_dir.rglob("*") if p.is_file()}

            # These operations must not mutate a terminal attempt.
            res = cancel_attempt(attempt_dir)
            self.assertTrue(res.get("ok"))
            self.assertTrue(res.get("already_terminal"))
            compute_batch_scoreboard(store, batch_id=batch_id)
            recover_orphaned_running_attempts(store, heartbeat_stale_after_seconds=1800, heartbeat_interval_seconds=900)

            after = {p: p.stat().st_mtime for p in attempt_dir.rglob("*") if p.is_file()}
            self.assertEqual(before, after)

    def test_state_json_status_transitions_do_not_revive_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            attempt_dir = root / "attempt"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            state_path = attempt_dir / "state.json"
            state_path.write_text(
                json.dumps(
                    {"status": "succeeded", "started_at": "2026-01-14T00:00:00Z", "ended_at": "2026-01-14T00:00:01Z", "last_heartbeat_at": "2026-01-14T00:00:00Z", "pid": None},
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            before = state_path.read_text(encoding="utf-8")
            res = cancel_attempt(attempt_dir)
            self.assertTrue(res.get("already_terminal"))
            self.assertEqual(state_path.read_text(encoding="utf-8"), before)

    def test_current_json_missing_attempt_dir_degrades_gracefully(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id, job_id, step_id = "b", "j", "s"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            missing_attempt_dir = runs_root / "no_such_attempt"
            store.write_current(
                batch_id,
                job_id,
                {
                    "batch_id": batch_id,
                    "job_id": job_id,
                    "updated_at": "2026-01-14T00:00:00Z",
                    "steps": {step_id: {"latest": {"run_id": "r1", "attempt_dir": str(missing_attempt_dir), "resume_base_dir": str(missing_attempt_dir / "codex_home")}}},
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            # Reader semantics: treat missing state.json as queued/initializing => running.
            self.assertEqual(int(sb["counts"]["running"]), 1)


if __name__ == "__main__":
    unittest.main()

