from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard
from parallelhassnes.storage.runs_store import RunsStore
from parallelhassnes.core.paths import Paths
from parallelhassnes.recovery.recovery import recover_orphaned_running_attempts, shutdown_orphan_heartbeat_threads


def _write_harness_config(runs_root: Path) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
    cfg = {
        "harness_config_version": "hc_test",
        "written_at": "2026-01-15T00:00:00Z",
        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
        "defaults": {
            "execution_policy": {},
            "timeouts": {},
            "retries": {"max_attempts": 1},
            "retention_policy": {},
            # Global defaults: 45 minutes stale threshold.
            "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
            "workspace_policy": {"mode": "shared"},
            "runner_capacity": 1,
            "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"], "resume_base_transfer_enabled": False},
            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
        },
        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
    }
    (runs_root / "_system" / "harness_config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class BatchEffectiveDefaultsScoreboardOverrideTests(unittest.TestCase):
    def test_batch_threshold_override_affects_stuck_detection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root)

            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            batch_id = "batch_sb"
            job_id = "job_sb"
            step_id = "s1"
            run_id = "run_sb"
            attempt_dir = runs_root / batch_id / job_id / "steps" / step_id / "attempts" / f"20260115T000000Z_{run_id}"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "r1", "invocation": "exec"}) + "\n",
                encoding="utf-8",
            )

            # Running attempt with stale heartbeat (age roughly 2000s at compute time if now ~2026).
            proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
            try:
                (attempt_dir / "state.json").write_text(
                    json.dumps(
                        {
                            "status": "running",
                            "started_at": "2026-01-15T00:00:00Z",
                            "last_heartbeat_at": "2026-01-15T00:00:00Z",
                            "pid": proc.pid,
                            "exit_code": None,
                            "artifacts": [],
                            "errors": [],
                        },
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
                # Batch overrides stale threshold to 1800 (30 minutes).
                (runs_root / batch_id / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2026-01-15T00:00:00Z",
                            "harness_config_version": "hc_test",
                            "batch_goal_summary": ("word " * 200).strip(),
                            "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                            "effective_defaults": {"scoreboard": {"heartbeat_stale_after_seconds": 1800}},
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (runs_root / batch_id / job_id).mkdir(parents=True, exist_ok=True)
                (runs_root / batch_id / job_id / "current.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "job_id": job_id,
                            "updated_at": "2026-01-15T00:00:00Z",
                            "steps": {step_id: {"latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}, "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}}}},
                        },
                        ensure_ascii=False,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
                sb = compute_batch_scoreboard(store, batch_id=batch_id)
                self.assertEqual(sb.get("heartbeat_stale_after_seconds"), 1800)
                # With the override, the run should be considered stuck (since seconds_since_last_heartbeat > 1800).
                kinds = [x.get("kind") for x in sb.get("attention", [])]
                self.assertIn("stuck", kinds)
            finally:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    pass
                shutdown_orphan_heartbeat_threads()

    def test_recovery_uses_batch_override_for_stale_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)

            batch_id = "batch_rec2"
            job_id = "job_rec2"
            step_id = "s1"
            run_id = "run_rec2"
            attempt_dir = runs_root / batch_id / job_id / "steps" / step_id / "attempts" / f"20260115T000000Z_{run_id}"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "r1", "invocation": "exec"}) + "\n",
                encoding="utf-8",
            )
            # Alive pid doesn't matter; we want hb_stale logic, so choose a nonexistent pid but stale heartbeat.
            (attempt_dir / "state.json").write_text(
                json.dumps({"status": "running", "started_at": "2026-01-15T00:00:00Z", "last_heartbeat_at": "2026-01-15T00:00:00Z", "pid": 999999}, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-15T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                        "effective_defaults": {"scoreboard": {"heartbeat_stale_after_seconds": 1800}},
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            (runs_root / batch_id / job_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / job_id / "current.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "job_id": job_id,
                        "updated_at": "2026-01-15T00:00:00Z",
                        "steps": {step_id: {"by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running", "ended_at": None}}}},
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            # Pass a huge stale threshold; override should still mark needs_attention because pid is dead.
            out = recover_orphaned_running_attempts(store, heartbeat_stale_after_seconds=999999, heartbeat_interval_seconds=900)
            self.assertTrue(out.get("ok"))
            st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(st.get("status"), "needs_attention")


if __name__ == "__main__":
    unittest.main()

