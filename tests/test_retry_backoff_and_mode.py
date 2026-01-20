from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness


_FAKE_CODEX_RETRY = """#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
if "--version" in args:
    print("codex 0.0.0-test")
    sys.exit(0)

def _flag(flag):
    if flag in args:
        i = args.index(flag)
        if i + 1 < len(args):
            return args[i + 1]
    return None

olm = _flag("--output-last-message")
job_workdir = "."
if "-C" in args:
    i = args.index("-C")
    if i + 1 < len(args):
        job_workdir = args[i + 1]

is_resume = "resume" in args
resume_arg = None
if is_resume:
    i = args.index("resume")
    if i + 1 < len(args):
        resume_arg = args[i + 1]
    codex_home = os.environ.get("CODEX_HOME")
    if codex_home and resume_arg:
        with open(os.path.join(codex_home, "resume_arg.txt"), "w", encoding="utf-8") as f:
            f.write(resume_arg + "\\n")

prompt = sys.stdin.read()

# Fail the first exec invocation only (per job_workdir), succeed after that and for resume.
marker = os.path.join(job_workdir, ".failed_once")
if (not is_resume) and ("fail_once" in prompt) and (not os.path.exists(marker)):
    with open(marker, "w", encoding="utf-8") as f:
        f.write("1\\n")
    sys.exit(1)

final = json.dumps(
    {
        "schema_version": "1.0.0",
        "status": "ok",
        "summary": "ok",
        "files_read": [],
        "files_written": [],
        "artifacts": [],
    },
    ensure_ascii=False,
)
if olm:
    with open(olm, "w", encoding="utf-8") as f:
        f.write(final)
print(final)
sys.exit(0)
"""


class RetryPolicyTests(unittest.TestCase):
    def test_retry_backoff_seconds_blocks_immediate_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # Baseline schema for runner lookup.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            shutil.copy2(src_schema, root / "run_report.schema.json")

            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

            lt = {
                "working_root": str(root),
                "batch_goal_summary": ("word " * 200).strip(),
                "jobs": [
                    {
                        "job_id": "job_bk",
                        "working_directory": ".",
                        "steps": [
                            {
                                "step_id": "step1",
                                "prompt": "x",
                                "retry_policy": {"max_attempts": 2, "retry_on_statuses": ["failed"], "backoff_seconds": 999999},
                                "fake_exit_codes": [1, 0],
                            }
                        ],
                    }
                ],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            harness.tick_once(concurrency_override=None, use_fake_invoker=True)
            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
            cur = json.loads((runs_root / batch_id / "job_bk" / "current.json").read_text(encoding="utf-8"))
            by = cur["steps"]["step1"]["by_run_id"]
            self.assertEqual(len(by), 1)

    def test_retry_mode_resume_last_attempt_uses_resume_invocation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            # Baseline schema for runner lookup.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            shutil.copy2(src_schema, root / "run_report.schema.json")

            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Minimal harness config; keep capture flags off so runner uses stdout for final.
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2026-01-14T00:00:00Z",
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "execution_policy": {"skip_git_repo_check": True},
                            "timeouts": {},
                            "retries": {"max_attempts": 2},
                            "retention_policy": {},
                            "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
                            "workspace_policy": {"mode": "shared"},
                            "runner_capacity": 1,
                            "runner_pool": {"shared_filesystem": True, "runner_ids": ["runner_only"]},
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

            # Fake codex on PATH.
            fakebin = root / "fakebin"
            fakebin.mkdir(parents=True, exist_ok=True)
            codex_path = fakebin / "codex"
            codex_path.write_text(_FAKE_CODEX_RETRY, encoding="utf-8")
            codex_path.chmod(codex_path.stat().st_mode | stat.S_IEXEC)

            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = str(fakebin) + os.pathsep + old_path
            try:
                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

                lt = {
                    "working_root": str(root),
                    "batch_goal_summary": ("word " * 200).strip(),
                    "jobs": [
                        {
                            "job_id": "job_rm",
                            "working_directory": ".",
                            "steps": [
                                {
                                    "step_id": "step1",
                                    "prompt": "fail_once",
                                    "retry_policy": {"max_attempts": 2, "retry_on_statuses": ["failed", "needs_attention"], "mode": "resume_last_attempt"},
                                }
                            ],
                        }
                    ],
                }
                incoming = queue_root / "incoming"
                incoming.mkdir(parents=True, exist_ok=True)
                (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

                harness.tick_once(concurrency_override=None, use_fake_invoker=False)
                harness.tick_once(concurrency_override=None, use_fake_invoker=False)

                batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
                cur = json.loads((runs_root / batch_id / "job_rm" / "current.json").read_text(encoding="utf-8"))
                step = cur["steps"]["step1"]
                self.assertEqual(len(step["by_run_id"]), 2)
                latest_attempt = Path(step["latest"]["attempt_dir"])
                meta = json.loads((latest_attempt / "meta.json").read_text(encoding="utf-8"))
                self.assertEqual(meta.get("invocation"), "resume")
                resume_arg = (latest_attempt / "codex_home" / "resume_arg.txt").read_text(encoding="utf-8").strip()
                self.assertEqual(resume_arg, "--last")
            finally:
                os.environ["PATH"] = old_path


if __name__ == "__main__":
    unittest.main()
