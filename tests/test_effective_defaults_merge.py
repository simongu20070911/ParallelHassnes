from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.core.paths import Paths
from parallelhassnes.storage.runs_store import RunsStore


def _write_harness_config(runs_root: Path, runner_ids: list[str]) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
    (runs_root / "_system" / "harness_config.json").write_text(
        json.dumps(
            {
                "harness_config_version": "hc_test",
                "written_at": "2026-01-14T00:00:00Z",
                "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                "defaults": {
                    "execution_policy": {"sandbox": "workspace-write", "approval_policy": "on-request"},
                    "timeouts": {"step_timeout_seconds": 3600},
                    "retries": {"max_attempts": 1},
                    "runner_capacity": 1,
                    "runner_pool": {"shared_filesystem": True, "runner_ids": runner_ids},
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


class EffectiveDefaultsMergeTests(unittest.TestCase):
    def test_defaults_overrides_merge_into_effective_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, runner_ids=["runner_only"])

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "defaults_overrides": {
                    "execution_policy": {"sandbox": "read-only", "approval_policy": "never"},
                    "timeouts": {"step_timeout_seconds": 12},
                },
                "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batch_id = store.list_batches()[0]
            batch = store.read_batch_meta(batch_id)
            eff = batch.get("effective_defaults") or {}

            self.assertEqual((eff.get("execution_policy") or {}).get("sandbox"), "read-only")
            self.assertEqual((eff.get("execution_policy") or {}).get("approval_policy"), "never")
            self.assertEqual((eff.get("timeouts") or {}).get("step_timeout_seconds"), 12)

    def test_top_level_defaults_fields_merge_into_effective_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, runner_ids=["runner_only"])

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "execution_policy": {"sandbox": "read-only"},
                "timeouts": {"step_timeout_seconds": 9},
                "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batch_id = store.list_batches()[0]
            batch = store.read_batch_meta(batch_id)
            eff = batch.get("effective_defaults") or {}

            self.assertEqual((eff.get("execution_policy") or {}).get("sandbox"), "read-only")
            self.assertEqual((eff.get("timeouts") or {}).get("step_timeout_seconds"), 9)

    def test_defaults_overrides_rejects_unknown_keys(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root, runner_ids=["runner_only"])

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "defaults_overrides": {"not_allowed": {"x": 1}},
                "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            with self.assertRaises(ValueError):
                harness.tick_once(concurrency_override=None, use_fake_invoker=True)
