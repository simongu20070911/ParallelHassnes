from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.recovery.recovery import shutdown_orphan_heartbeat_threads


def _write_harness_config(runs_root: Path, enabled: bool) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
    cfg = {
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
            "runner_pool": {"shared_filesystem": True, "runner_ids": ["runner_only"]},
            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
        },
        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
    }
    if enabled:
        cfg["defaults"]["scoreboard"]["auto_remediation"] = {"enabled": True, "kinds": ["stuck"], "action": "cancel_and_force_retry", "max_actions_per_tick": 10}  # type: ignore[index]
    (runs_root / "_system" / "harness_config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class AutoRemediationTests(unittest.TestCase):
    def test_default_disabled_does_not_cancel(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, enabled=False)

            # Provide baseline schema at the expected lookup location (required by RunnerPool even if no steps run).
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            batch_id, job_id, step_id, run_id = "batch_x", "job_x", "step1", "run_x"
            attempt_dir = runs_root / batch_id / job_id / "steps" / step_id / "attempts" / "attempt1"
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "runner_only", "invocation": "exec"})
                + "\n",
                encoding="utf-8",
            )

            proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
            try:
                (attempt_dir / "state.json").write_text(
                    json.dumps(
                        {
                            "status": "running",
                            "started_at": "2026-01-14T00:00:00Z",
                            "last_heartbeat_at": "2000-01-01T00:00:00Z",
                            "pid": proc.pid,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
                (runs_root / batch_id / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2026-01-14T00:00:00Z",
                            "harness_config_version": "hc_test",
                            "batch_goal_summary": ("word " * 200).strip(),
                            "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                            "effective_defaults": {},
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
                            "updated_at": "2026-01-14T00:00:00Z",
                            "steps": {
                                step_id: {
                                    "latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                                    "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}},
                                }
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                harness.tick_once(concurrency_override=None, use_fake_invoker=True)

                self.assertFalse((attempt_dir / "remediation.json").exists())
                self.assertIsNone(proc.poll())
            finally:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    pass
                shutdown_orphan_heartbeat_threads()

    def test_enabled_cancels_and_records_action(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, enabled=True)

            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            batch_id, job_id, step_id, run_id = "batch_y", "job_y", "step1", "run_y"
            attempt_dir = runs_root / batch_id / job_id / "steps" / step_id / "attempts" / "attempt1"
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "runner_only", "invocation": "exec"})
                + "\n",
                encoding="utf-8",
            )

            proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
            try:
                (attempt_dir / "state.json").write_text(
                    json.dumps(
                        {
                            "status": "running",
                            "started_at": "2026-01-14T00:00:00Z",
                            "last_heartbeat_at": "2000-01-01T00:00:00Z",
                            "pid": proc.pid,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
                (runs_root / batch_id / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2026-01-14T00:00:00Z",
                            "harness_config_version": "hc_test",
                            "batch_goal_summary": ("word " * 200).strip(),
                            "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                            "effective_defaults": {},
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
                            "updated_at": "2026-01-14T00:00:00Z",
                            "steps": {
                                step_id: {
                                    "latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                                    "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}},
                                }
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                harness.tick_once(concurrency_override=None, use_fake_invoker=True)

                # Remediation action recorded on the original stuck attempt.
                st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
                self.assertEqual(st.get("status"), "canceled")

                # Process should have been terminated.
                proc.wait(timeout=2)

                # Force-retry override should exist for the step.
                ov_path = runs_root / batch_id / job_id / "overrides.json"
                self.assertTrue(ov_path.exists())
                ov = json.loads(ov_path.read_text(encoding="utf-8"))
                ent = ((ov.get("steps") or {}).get(step_id) or {})
                self.assertIsInstance(ent.get("force_retry_nonce"), int)
                self.assertGreater(int(ent.get("force_retry_nonce") or 0), 0)

                # Remediation action recorded in the mutable index (current.json), not inside the attempt directory.
                cur = json.loads((runs_root / batch_id / job_id / "current.json").read_text(encoding="utf-8"))
                mark = (((cur.get("steps") or {}).get(step_id) or {}).get("auto_remediation") or {})
                self.assertEqual(mark.get("last_applied_run_id"), run_id)
                self.assertEqual(mark.get("kind"), "stuck")
                self.assertEqual(mark.get("action"), "cancel_and_force_retry")
            finally:
                try:
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=2)
                except Exception:
                    pass
                shutdown_orphan_heartbeat_threads()

    def test_legacy_boolean_enables_stuck_remediation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, enabled=False)

            # Flip legacy boolean flag (no structured auto_remediation object).
            cfg_path = runs_root / "_system" / "harness_config.json"
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            cfg["defaults"]["scoreboard"]["stuck_auto_remediation_enabled"] = True
            cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            batch_id, job_id, step_id, run_id = "batch_z", "job_z", "step1", "run_z"
            attempt_dir = runs_root / batch_id / job_id / "steps" / step_id / "attempts" / "attempt1"
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "run_id": run_id, "runner_id": "runner_only", "invocation": "exec"})
                + "\n",
                encoding="utf-8",
            )

            proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
            try:
                (attempt_dir / "state.json").write_text(
                    json.dumps(
                        {
                            "status": "running",
                            "started_at": "2026-01-14T00:00:00Z",
                            "last_heartbeat_at": "2000-01-01T00:00:00Z",
                            "pid": proc.pid,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
                (runs_root / batch_id / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2026-01-14T00:00:00Z",
                            "harness_config_version": "hc_test",
                            "batch_goal_summary": ("word " * 200).strip(),
                            "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                            "effective_defaults": {},
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
                            "updated_at": "2026-01-14T00:00:00Z",
                            "steps": {
                                step_id: {
                                    "latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                                    "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}},
                                }
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                harness.tick_once(concurrency_override=None, use_fake_invoker=True)

                st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
                self.assertEqual(st.get("status"), "canceled")
                proc.wait(timeout=2)

                cur = json.loads((runs_root / batch_id / job_id / "current.json").read_text(encoding="utf-8"))
                mark = (((cur.get("steps") or {}).get(step_id) or {}).get("auto_remediation") or {})
                self.assertEqual(mark.get("last_applied_run_id"), run_id)
                self.assertEqual(mark.get("kind"), "stuck")
                self.assertEqual(mark.get("action"), "cancel_and_force_retry")
            finally:
                try:
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=2)
                except Exception:
                    pass
                shutdown_orphan_heartbeat_threads()


if __name__ == "__main__":
    unittest.main()
