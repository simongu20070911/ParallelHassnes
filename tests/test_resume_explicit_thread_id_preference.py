from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.storage.runs_store import RunsStore


class ResumeExplicitThreadIdPreferenceTests(unittest.TestCase):
    def test_resume_prefers_known_thread_id_when_available(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            # Baseline schema at expected location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Minimal harness config so Runner can read defaults.
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2026-01-15T00:00:00Z",
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "execution_policy": {"skip_git_repo_check": True},
                            "timeouts": {},
                            "retries": {"max_attempts": 1},
                            "runner_capacity": 1,
                            "runner_pool": {"shared_filesystem": True, "runner_ids": ["runner_only"]},
                            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                            "retention_policy": {"keep_raw_events_days": 7, "keep_git_artifacts_days": 7, "keep_resume_bases_days": 30},
                            "scoreboard": {"heartbeat_stale_after_seconds": 2700, "heartbeat_interval_seconds": 900},
                            "workspace_policy": {"mode": "shared"},
                        },
                        "limits": {"max_jobs_per_batch": 1000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            hcfg = store.read_harness_config()

            runner = Runner(
                store=store,
                cfg=RunnerConfig(runner_id="runner_only", use_fake_invoker=False, baseline_output_schema_path=str((root / "run_report.schema.json").resolve())),
                harness_cfg=hcfg,
            )

            batch_id = "b1"
            job_id = "j1"
            step1 = "step1"
            step2 = "step2"
            run1 = "r1"
            tid = "thread_abc"

            # Create a source attempt and expose it in current.json with a captured thread id.
            source_attempt_dir = store.paths.attempt_dir(batch_id, job_id, step1, store.paths.run_dir_name(run1, started_compact_utc="20260115T000000Z"))
            source_codex_home = store.paths.attempt_codex_home(source_attempt_dir)
            source_codex_home.mkdir(parents=True, exist_ok=True)

            cur = {
                "batch_id": batch_id,
                "job_id": job_id,
                "updated_at": "2026-01-15T00:00:00Z",
                "steps": {
                    step1: {
                        "latest": {"run_id": run1, "attempt_dir": str(source_attempt_dir), "resume_base_dir": str(source_codex_home), "codex_thread_id": tid},
                        "by_run_id": {run1: {"run_id": run1, "attempt_dir": str(source_attempt_dir), "resume_base_dir": str(source_codex_home), "codex_thread_id": tid, "status": "succeeded", "ended_at": "2026-01-15T00:00:01Z"}},
                        "latest_successful": {"run_id": run1, "attempt_dir": str(source_attempt_dir), "resume_base_dir": str(source_codex_home), "codex_thread_id": tid},
                    }
                },
            }
            store.write_current(batch_id, job_id, cur)

            batch = {"batch_id": batch_id, "effective_defaults": {}, "packs": {}}
            job = {"job_id": job_id, "working_directory": "."}
            step = {"step_id": step2, "prompt": "x", "resume_from": {"step_id": step1, "selector": "latest_successful"}}

            seen: dict[str, Any] = {}

            def fake_invoke_codex(*args: Any, **kwargs: Any) -> tuple[str, int]:
                seen["invocation"] = kwargs.get("invocation")
                seen["explicit_resume_thread_id"] = kwargs.get("explicit_resume_thread_id")
                hb = kwargs.get("hb")
                if hb is not None:
                    hb.mark_running(pid=123)
                return (
                    json.dumps(
                        {
                            "schema_version": "1.0.0",
                            "status": "ok",
                            "summary": "ok",
                            "files_read": [],
                            "files_written": [],
                            "artifacts": [],
                        }
                    ),
                    0,
                )

            runner._invoke_codex = fake_invoke_codex  # type: ignore[method-assign]

            runner.execute_step(
                batch=batch,
                job=job,
                step=step,
                job_workdir=str(root),
                workspace_policy={"mode": "shared"},
                workspace_root=str(root),
                current=cur,
            )

            self.assertEqual(seen.get("invocation"), "resume")
            self.assertEqual(seen.get("explicit_resume_thread_id"), tid)
