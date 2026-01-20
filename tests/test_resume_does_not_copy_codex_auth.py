from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.recovery.recovery import shutdown_orphan_heartbeat_threads


def _write_harness_config(runs_root: Path) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
    cfg = {
        "harness_config_version": "hc_test",
        "written_at": "2026-01-15T00:00:00Z",
        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
        "defaults": {
            "execution_policy": {"skip_git_repo_check": True, "capture_events_jsonl": False, "capture_codex_thread_id": False},
            "timeouts": {"step_timeout_seconds": 5},
            "retries": {"max_attempts": 1},
            "retention_policy": {},
            "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
            "workspace_policy": {"mode": "shared"},
            "runner_capacity": 1,
            "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"], "resume_base_transfer_enabled": False},
            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
        },
        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
    }
    (runs_root / "_system" / "harness_config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class ResumeDoesNotCopyCodexAuthTests(unittest.TestCase):
    def test_resume_base_copy_excludes_auth_and_reprovisions_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root)

            # Ensure baseline schema is available for RunnerPool.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Provide a runner-local auth.json and force Runner to see it via HOME.
            fake_home = root / "home"
            (fake_home / ".codex").mkdir(parents=True, exist_ok=True)
            auth_path = fake_home / ".codex" / "auth.json"
            auth_path.write_text('{"secret":"do-not-copy"}\n', encoding="utf-8")
            cfg_path = fake_home / ".codex" / "config.toml"
            cfg_path.write_text("model = \"gpt\"\n", encoding="utf-8")

            old_home = os.environ.get("HOME")
            os.environ["HOME"] = str(fake_home)
            try:
                # Prepare a batch with a resume step so Runner copies session store.
                launch_table = {
                    "batch_goal_summary": ("word " * 200).strip(),
                    "jobs": [
                        {
                            "job_id": "j1",
                            "working_directory": ".",
                            "steps": [
                                {"step_id": "s1", "prompt": "x", "fake_codex_home_marker": "s1.marker"},
                                {"step_id": "s2", "prompt": "y", "resume_from": {"step_id": "s1", "selector": "latest_successful"}},
                            ],
                        }
                    ],
                }

                # Submit through FS queue by writing directly into incoming.
                incoming = queue_root / "incoming"
                incoming.mkdir(parents=True, exist_ok=True)
                (incoming / "lt.json").write_text(json.dumps(launch_table, ensure_ascii=False), encoding="utf-8")

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
                harness.tick_once(concurrency_override=None, use_fake_invoker=True)

                # Locate attempt dirs from current.json.
                batch_id = sorted(p.name for p in runs_root.iterdir() if p.is_dir() and not p.name.startswith("_"))[0]
                current = json.loads((runs_root / batch_id / "j1" / "current.json").read_text(encoding="utf-8"))
                s1 = ((current.get("steps") or {}).get("s1") or {}).get("latest") or {}
                s2 = ((current.get("steps") or {}).get("s2") or {}).get("latest") or {}
                s1_dir = Path(s1["attempt_dir"])
                s2_dir = Path(s2["attempt_dir"])

                # Step1 codex_home contains a symlink to runner-local auth.json.
                s1_auth = s1_dir / "codex_home" / "auth.json"
                self.assertTrue(s1_auth.exists())
                self.assertTrue(s1_auth.is_symlink())

                # Step2 codex_home should not get a copied auth.json file from resume base;
                # it should be provisioned as a symlink independently.
                s2_auth = s2_dir / "codex_home" / "auth.json"
                self.assertTrue(s2_auth.exists())
                self.assertTrue(s2_auth.is_symlink())
                self.assertEqual(Path(os.readlink(s2_auth)).resolve(), auth_path.resolve())

                # Ensure resume base content (non-credential) was copied.
                self.assertTrue((s2_dir / "codex_home" / "s1.marker").exists())
            finally:
                if old_home is None:
                    os.environ.pop("HOME", None)
                else:
                    os.environ["HOME"] = old_home
                shutdown_orphan_heartbeat_threads()


if __name__ == "__main__":
    unittest.main()
