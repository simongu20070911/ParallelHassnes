from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.pool import RunnerHandle, RunnerPool
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard
from parallelhassnes.storage.runs_store import RunsStore


class RunnerLocalityTests(unittest.TestCase):
    def test_resume_locality_blocks_when_shared_filesystem_false_and_source_runner_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
            runners_root.mkdir(parents=True, exist_ok=True)

            # Harness config: no shared filesystem; only runner_b is available.
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2026-01-14T00:00:00Z",
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "scoreboard": {"heartbeat_stale_after_seconds": 2700},
                            "runner_pool": {"shared_filesystem": False, "runner_ids": ["runner_b"]},
                            "execution_policy": {},
                            "timeouts": {},
                            "retries": {"max_attempts": 1},
                            "retention_policy": {},
                            "workspace_policy": {"mode": "shared"},
                        },
                        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            # Create step1 successful attempt authored by runner_a.
            batch_id = "batch_loc"
            job_id = "job_loc"
            step1_id = "step1"
            step2_id = "step2"
            attempt_dir = store.paths.attempt_dir(batch_id, job_id, step1_id, "run_1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "job_id": job_id,
                        "step_id": step1_id,
                        "run_id": "run_1",
                        "runner_id": "runner_a",
                        "invocation": "exec",
                        "prompt_sha256": "x",
                        "codex_cli_version": "unknown",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "state.json").write_text(
                json.dumps({"status": "succeeded", "started_at": "2026-01-14T00:00:00Z", "ended_at": "2026-01-14T00:00:01Z"}) + "\n",
                encoding="utf-8",
            )

            # Batch meta + current pointers.
            store.write_batch_meta(
                {
                    "spec_version": "1.0.0",
                    "batch_id": batch_id,
                    "submitted_at": "2026-01-14T00:00:00Z",
                    "launch_table_sha256": "x",
                    "batch_goal_summary": "word " * 200,
                    "harness_config_version": "hc_test",
                    "jobs": [
                        {
                            "job_id": job_id,
                            "working_directory": ".",
                            "steps": [
                                {"step_id": step1_id, "prompt": "x"},
                                {"step_id": step2_id, "resume_from": {"step_id": step1_id, "selector": "latest_successful"}, "prompt": "y"},
                            ],
                        }
                    ],
                }
            )
            store.write_current(
                batch_id,
                job_id,
                {
                    "batch_id": batch_id,
                    "job_id": job_id,
                    "updated_at": "2026-01-14T00:00:02Z",
                    "steps": {
                        step1_id: {
                            "latest_successful": {"run_id": "run_1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                            "latest": {"run_id": "run_1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                            "by_run_id": {"run_1": {"run_id": "run_1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}},
                        }
                    },
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(sb["runner_shared_filesystem"], False)
            blocked = [b for b in sb["blocked"] if b["job_id"] == job_id and b["step_id"] == step2_id]
            self.assertEqual(len(blocked), 1)
            self.assertTrue(any("resume_from locality" in r for r in blocked[0]["reasons"]))

    def test_scheduler_does_not_acquire_other_runner_when_locality_requires_missing_source_runner(self) -> None:
        # Directly validate the pool + choice logic in the strict locality case.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            store = RunsStore(Paths(runs_root=root / "runs", runners_root=root / "runners"))
            attempt_dir = root / "runs" / "b" / "j" / "steps" / "s1" / "attempts" / "run_1"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": "b",
                        "job_id": "j",
                        "step_id": "s1",
                        "run_id": "run_1",
                        "runner_id": "runner_a",
                        "invocation": "exec",
                        "prompt_sha256": "x",
                        "codex_cli_version": "unknown",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            current = {
                "steps": {
                    "s1": {
                        "latest_successful": {"attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "run_id": "run_1"},
                        "by_run_id": {"run_1": {"attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "run_id": "run_1"}},
                    }
                }
            }
            step2 = {"step_id": "s2", "resume_from": {"step_id": "s1", "selector": "latest_successful"}}

            pool = RunnerPool([RunnerHandle(runner_id="runner_b", runner=object(), capacity=1, semaphore=__import__("threading").Semaphore(1))])

            from parallelhassnes.scheduler.scheduler import _resolve_runner_choice

            desired, avoid, _ = _resolve_runner_choice(
                store=store,
                batch_id="b",
                job_id="j",
                current=current,
                step=step2,
                runner_pool=pool,
                overrides={},
                runner_affinity_cfg={},
                shared_fs=False,
                transfer_enabled=False,
            )
            self.assertEqual(desired, "runner_a")
            self.assertIn("runner_b", avoid)
            self.assertIsNone(pool.try_acquire_any(desired, avoid=avoid))


if __name__ == "__main__":
    unittest.main()
