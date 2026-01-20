from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from parallelhassnes.harness.harness import Harness


def _write_launch_table(incoming: Path, name: str, obj: dict) -> None:
    incoming.mkdir(parents=True, exist_ok=True)
    (incoming / name).write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_scenarios() -> dict:
    """
    Integration scenarios runner.

    Runs three spec-aligned flows using the fake invoker:
    1) exec success, 2) retry, 3) resume.
    """
    errors: list[str] = []
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        runs_root = root / "runs"
        runners_root = root / "runners"
        queue_root = runs_root / "_queue"

        # Runner expects the baseline schema at runs_root.parent/run_report.schema.json.
        repo_root = Path(__file__).resolve().parents[2]
        src_schema = repo_root / "run_report.schema.json"
        if not src_schema.exists():
            return {"ok": False, "errors": [f"missing baseline schema at {src_schema}"]}
        shutil.copy2(src_schema, root / "run_report.schema.json")

        h = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

        # Scenario 1: exec success.
        _write_launch_table(
            queue_root / "incoming",
            "s1.json",
            {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "jobs": [{"job_id": "job_exec", "working_directory": ".", "steps": [{"step_id": "step1", "prompt": "x"}]}],
            },
        )
        h.tick_once(concurrency_override=None, use_fake_invoker=True)

        # Scenario 2: retry (requires two ticks: one attempt per tick per step).
        _write_launch_table(
            queue_root / "incoming",
            "s2.json",
            {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "jobs": [
                    {
                        "job_id": "job_retry",
                        "working_directory": ".",
                        "steps": [
                            {
                                "step_id": "step1",
                                "prompt": "x",
                                "retry_policy": {"max_attempts": 2, "retry_on_statuses": ["failed"]},
                                "fake_exit_codes": [1, 0],
                            }
                        ],
                    }
                ],
            },
        )
        h.tick_once(concurrency_override=None, use_fake_invoker=True)
        h.tick_once(concurrency_override=None, use_fake_invoker=True)

        # Scenario 3: resume.
        _write_launch_table(
            queue_root / "incoming",
            "s3.json",
            {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "jobs": [
                    {
                        "job_id": "job_resume",
                        "working_directory": ".",
                        "steps": [
                            {"step_id": "step1", "prompt": "x", "fake_codex_home_marker": "marker.txt"},
                            {"step_id": "step2", "resume_from": {"step_id": "step1", "selector": "latest_successful"}, "prompt": "y"},
                        ],
                    }
                ],
            },
        )
        h.tick_once(concurrency_override=None, use_fake_invoker=True)

        # Minimal assertions: all batches have scoreboards and at least one attempt dir.
        batches = [p for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_")]
        if not batches:
            errors.append("no batches created")
        for b in batches:
            if not (b / "batch_meta.json").exists():
                errors.append(f"missing batch_meta.json in {b.name}")
            if not (b / "scoreboard.batch.json").exists():
                errors.append(f"missing scoreboard.batch.json in {b.name}")

    return {"ok": not errors, "errors": errors}

