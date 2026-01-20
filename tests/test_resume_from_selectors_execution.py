from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.recovery.recovery import shutdown_orphan_heartbeat_threads


def _long_goal() -> str:
    return ("word " * 200).strip()


def _write_launch_table(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class ResumeFromSelectorsExecutionTests(unittest.TestCase):
    def test_resume_from_latest_can_resume_failed_source(self) -> None:
        """
        Spec: resume_from.selector may be `latest` (may resume from a failed attempt).
        This validates the full scheduling+runner workflow, not just selector resolution.
        """
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

            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            lt = {
                "working_root": str(root),
                "batch_goal_summary": _long_goal(),
                "concurrency": 1,
                "jobs": [
                    {
                        "job_id": "j1",
                        "working_directory": ".",
                        "steps": [
                            # step1 fails but still produces a resume base (codex_home/).
                            {"step_id": "step1", "prompt": "x", "fake_exit_code": 1, "fake_codex_home_marker": "s1.txt"},
                            {"step_id": "step2", "prompt": "y", "resume_from": {"step_id": "step1", "selector": "latest"}, "fake_codex_home_marker": "s2.txt"},
                        ],
                    }
                ],
            }
            _write_launch_table(incoming / "lt.json", lt)

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
            try:
                harness.tick_once(concurrency_override=1, use_fake_invoker=True)
            finally:
                shutdown_orphan_heartbeat_threads()

            # Locate batch id and validate both steps ran.
            batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
            cur = json.loads((runs_root / batch_id / "j1" / "current.json").read_text(encoding="utf-8"))
            self.assertIn("step1", cur.get("steps", {}))
            self.assertIn("step2", cur.get("steps", {}))

            s1_attempt = Path(cur["steps"]["step1"]["latest"]["attempt_dir"])
            s2_attempt = Path(cur["steps"]["step2"]["latest"]["attempt_dir"])
            self.assertTrue((s1_attempt / "codex_home" / "s1.txt").exists())
            self.assertTrue((s2_attempt / "codex_home" / "s2.txt").exists())
            # Ensure copy-on-resume does not mutate the source attempt's codex_home.
            self.assertFalse((s1_attempt / "codex_home" / "s2.txt").exists())

    def test_resume_from_run_id_uses_specific_attempt(self) -> None:
        """
        Spec: selector may be `run_id` to resume a specific attempt.
        We control the run_id deterministically so the Launch Table can reference it.
        """
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

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

            # Force deterministic run ids for step1 and step2.
            import parallelhassnes.runner.runner as runner_mod

            orig_new_run_id = runner_mod.new_run_id
            ids = iter(["run_known", "run_step2"])
            runner_mod.new_run_id = lambda: next(ids)  # type: ignore[assignment]
            try:
                incoming = queue_root / "incoming"
                incoming.mkdir(parents=True, exist_ok=True)
                lt = {
                    "working_root": str(root),
                    "batch_goal_summary": _long_goal(),
                    "concurrency": 1,
                    "jobs": [
                        {
                            "job_id": "j1",
                            "working_directory": ".",
                            "steps": [
                                {"step_id": "step1", "prompt": "x", "fake_codex_home_marker": "s1.txt"},
                                {
                                    "step_id": "step2",
                                    "prompt": "y",
                                    "resume_from": {"step_id": "step1", "selector": "run_id", "run_id": "run_known"},
                                    "fake_codex_home_marker": "s2.txt",
                                },
                            ],
                        }
                    ],
                }
                _write_launch_table(incoming / "lt.json", lt)

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                try:
                    harness.tick_once(concurrency_override=1, use_fake_invoker=True)
                finally:
                    shutdown_orphan_heartbeat_threads()

                batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
                cur = json.loads((runs_root / batch_id / "j1" / "current.json").read_text(encoding="utf-8"))
                step1_latest = cur["steps"]["step1"]["latest"]
                self.assertEqual(step1_latest["run_id"], "run_known")
                # step2 ran and produced its marker.
                s2_attempt = Path(cur["steps"]["step2"]["latest"]["attempt_dir"])
                self.assertTrue((s2_attempt / "codex_home" / "s2.txt").exists())
            finally:
                runner_mod.new_run_id = orig_new_run_id  # type: ignore[assignment]


if __name__ == "__main__":
    unittest.main()

