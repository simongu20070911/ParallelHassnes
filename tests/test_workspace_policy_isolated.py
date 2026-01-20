from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.storage.runs_store import RunsStore
from parallelhassnes.core.paths import Paths


class WorkspacePolicyIsolatedTests(unittest.TestCase):
    def test_isolated_workspace_persists_across_steps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Create a small git repo as the source workdir.
            repo = root / "repo"
            repo.mkdir(parents=True, exist_ok=True)
            subprocess.run(["git", "-C", str(repo), "init"], check=True, capture_output=True, text=True)
            (repo / "README.md").write_text("hello\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True, text=True)
            subprocess.run(
                ["git", "-C", str(repo), "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-m", "init"],
                check=True,
                capture_output=True,
                text=True,
            )

            # Write harness config with isolated workspace policy enabled.
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2026-01-15T00:00:00Z",
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "execution_policy": {"skip_git_repo_check": False},
                            "timeouts": {},
                            "retries": {"max_attempts": 1},
                            "retention_policy": {},
                            "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
                            "workspace_policy": {"mode": "isolated"},
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

            # Submit a Launch Table via filesystem queue mode.
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            launch_table = {
                "spec_version": "1.0.0",
                "batch_goal_summary": ("word " * 200).strip(),
                "working_root": str(root),
                "jobs": [
                    {
                        "job_id": "job1",
                        "working_directory": "repo",
                        "steps": [
                            {
                                "step_id": "step1",
                                "prompt": "x",
                                "fake_workspace_writes": [{"path": "persist.txt", "content": "ok\n"}],
                            },
                            {
                                "step_id": "step2",
                                "prompt": "y",
                                "resume_from": {"step_id": "step1", "selector": "latest_successful"},
                                "fake_workspace_assert_exists": ["persist.txt"],
                            },
                        ],
                    }
                ],
            }
            (incoming / "lt.json").write_text(json.dumps(launch_table, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            h = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
            h.tick_once(concurrency_override=1, use_fake_invoker=True)

            # Identify batch id (assigned by harness ingest).
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batches = store.list_batches()
            self.assertEqual(len(batches), 1)
            batch_id = batches[0]

            cur = store.read_current(batch_id, "job1")
            self.assertIsNotNone(cur)
            latest = (((cur or {}).get("steps") or {}).get("step2") or {}).get("latest") if cur else None
            self.assertIsInstance(latest, dict)
            attempt_dir = Path(latest["attempt_dir"])
            meta = store.read_attempt_meta(attempt_dir)
            self.assertIsInstance(meta, dict)
            env = (meta or {}).get("environment_snapshot") if isinstance(meta, dict) else None
            self.assertIsInstance(env, dict)

            workspace_root = Path(env["workspace_root"]).resolve()
            job_workdir = Path(env["job_workdir"]).resolve()
            self.assertTrue(str(workspace_root).endswith(f"runs/{batch_id}/job1/_workspace"))
            try:
                job_workdir.relative_to(workspace_root)
            except Exception as e:
                raise AssertionError(f"job_workdir is not under workspace_root: job_workdir={job_workdir} workspace_root={workspace_root}") from e
            self.assertTrue((job_workdir / "persist.txt").exists())

            # Cleanup worktree registration to avoid leaving stale references in the source repo.
            try:
                subprocess.run(["git", "-C", str(repo), "worktree", "remove", "--force", str(workspace_root)], check=False, capture_output=True, text=True)
                subprocess.run(["git", "-C", str(repo), "worktree", "prune"], check=False, capture_output=True, text=True)
            except Exception:
                pass
