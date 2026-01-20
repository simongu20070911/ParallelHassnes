from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.retention.gc import RetentionPolicy, run_gc
from parallelhassnes.util.tail import tail_last_lines


class RetentionPolicyTests(unittest.TestCase):
    def test_gc_compresses_old_raw_events_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            batch_dir = runs_root / "batch1"
            attempt_dir = batch_dir / "job1" / "steps" / "s" / "attempts" / "a1"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "state.json").write_text(
                json.dumps({"status": "succeeded", "started_at": "2000-01-01T00:00:00Z", "ended_at": "2000-01-02T00:00:00Z", "last_heartbeat_at": "2000-01-01T00:00:00Z"})
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "codex.events.jsonl").write_text("a\nb\nc\n", encoding="utf-8")
            (batch_dir / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": "batch1",
                        "submitted_at": "2000-01-01T00:00:00Z",
                        "harness_config_version": "hc",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "effective_defaults": {"retention_policy": {"compress_raw_events_after_days": 0, "keep_raw_events_days": 100000, "keep_git_artifacts_days": 999, "keep_resume_bases_days": 999}},
                        "jobs": [{"job_id": "job1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            out = run_gc(runs_root=runs_root, policy=RetentionPolicy(keep_raw_events_days=100000, keep_git_artifacts_days=999, keep_resume_bases_days=999), protected_resume_bases=set())
            self.assertTrue(out.get("ok"))
            self.assertFalse((attempt_dir / "codex.events.jsonl").exists())
            self.assertTrue((attempt_dir / "codex.events.jsonl.gz").exists())
            self.assertEqual(tail_last_lines(attempt_dir / "codex.events.jsonl.gz", n=2), ["b\n", "c\n"])

    def test_gc_uses_batch_retention_policy_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"

            def make_batch(batch_id: str, keep_raw_events_days: int) -> Path:
                batch_dir = runs_root / batch_id
                attempt_dir = batch_dir / "job1" / "steps" / "s" / "attempts" / "a1"
                attempt_dir.mkdir(parents=True, exist_ok=True)
                (attempt_dir / "state.json").write_text(
                    json.dumps({"status": "succeeded", "started_at": "2000-01-01T00:00:00Z", "ended_at": "2000-01-02T00:00:00Z", "last_heartbeat_at": "2000-01-01T00:00:00Z"})
                    + "\n",
                    encoding="utf-8",
                )
                (attempt_dir / "codex.events.jsonl").write_text("x\n", encoding="utf-8")
                (batch_dir / "batch_meta.json").write_text(
                    json.dumps(
                        {
                            "batch_id": batch_id,
                            "submitted_at": "2000-01-01T00:00:00Z",
                            "harness_config_version": "hc",
                            "batch_goal_summary": ("word " * 200).strip(),
                            "effective_defaults": {"retention_policy": {"keep_raw_events_days": keep_raw_events_days, "keep_git_artifacts_days": 999, "keep_resume_bases_days": 999}},
                            "jobs": [{"job_id": "job1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x"}]}],
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return attempt_dir

            a = make_batch("batch_delete", keep_raw_events_days=0)
            b = make_batch("batch_keep", keep_raw_events_days=100000)

            run_gc(runs_root=runs_root, policy=RetentionPolicy(keep_raw_events_days=999, keep_git_artifacts_days=999, keep_resume_bases_days=999), protected_resume_bases=set())
            self.assertFalse((a / "codex.events.jsonl").exists())
            self.assertTrue((b / "codex.events.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
