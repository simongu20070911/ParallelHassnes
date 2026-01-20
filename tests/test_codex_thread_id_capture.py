from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness


_FAKE_CODEX = """#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
if "--version" in args:
    print("codex 0.0.0-test")
    sys.exit(0)

def _get_flag(flag):
    if flag in args:
        i = args.index(flag)
        if i + 1 < len(args):
            return args[i + 1]
    return None

json_mode = "--json" in args
olm = _get_flag("--output-last-message")

thread_id = "tid_1"
resume_arg = None
if "resume" in args:
    i = args.index("resume")
    if i + 1 < len(args):
        resume_arg = args[i + 1]
    if resume_arg and resume_arg != "--last":
        thread_id = resume_arg

codex_home = os.environ.get("CODEX_HOME")
if codex_home and resume_arg:
    with open(os.path.join(codex_home, "resume_arg.txt"), "w", encoding="utf-8") as f:
        f.write(resume_arg + "\\n")

_ = sys.stdin.read()

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

if json_mode:
    sys.stdout.write(json.dumps({"type": "thread.started", "thread_id": thread_id}) + "\\n")
    sys.stdout.write(json.dumps({"type": "turn.completed", "usage": {"output_tokens": 1}}) + "\\n")
else:
    sys.stdout.write(final)
sys.exit(0)
"""


class CodexThreadIdCaptureTests(unittest.TestCase):
    def test_capture_thread_id_and_use_explicit_resume(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            shutil.copy2(src_schema, root / "run_report.schema.json")

            # Fake codex binary on PATH.
            fakebin = root / "fakebin"
            fakebin.mkdir(parents=True, exist_ok=True)
            codex_path = fakebin / "codex"
            codex_path.write_text(_FAKE_CODEX, encoding="utf-8")
            codex_path.chmod(codex_path.stat().st_mode | stat.S_IEXEC)

            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = str(fakebin) + os.pathsep + old_path
            try:
                # Harness config: enable thread id capture.
                (runs_root / "_system" / "harness_config.json").write_text(
                    json.dumps(
                        {
                            "harness_config_version": "hc_test",
                            "written_at": "2026-01-14T00:00:00Z",
                            "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                            "defaults": {
                                "execution_policy": {"skip_git_repo_check": True, "capture_codex_thread_id": True},
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
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)

                lt = {
                    "working_root": str(root),
                    "batch_goal_summary": ("word " * 200).strip(),
                    "jobs": [
                        {
                            "job_id": "job_t",
                            "working_directory": ".",
                            "steps": [
                                {"step_id": "step1", "prompt": "x"},
                                {
                                    "step_id": "step2",
                                    "resume_from": {"step_id": "step1", "selector": "latest_successful", "codex_thread_id": "tid_1"},
                                    "prompt": "y",
                                },
                            ],
                        }
                    ],
                }
                incoming = queue_root / "incoming"
                incoming.mkdir(parents=True, exist_ok=True)
                (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

                harness.tick_once(concurrency_override=None, use_fake_invoker=False)

                batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
                cur = json.loads((runs_root / batch_id / "job_t" / "current.json").read_text(encoding="utf-8"))
                s1 = cur["steps"]["step1"]["latest"]
                s2 = cur["steps"]["step2"]["latest"]
                self.assertEqual(s1.get("codex_thread_id"), "tid_1")

                step1_attempt = Path(s1["attempt_dir"])
                meta1 = json.loads((step1_attempt / "meta.json").read_text(encoding="utf-8"))
                # meta.json is write-once at attempt start; exec attempts may not know codex_thread_id yet.
                self.assertIsNone(meta1.get("codex_thread_id"))

                step2_attempt = Path(s2["attempt_dir"])
                meta2 = json.loads((step2_attempt / "meta.json").read_text(encoding="utf-8"))
                self.assertEqual(meta2.get("invocation"), "resume")
                self.assertEqual(meta2.get("codex_thread_id"), "tid_1")
                # Fake codex writes the resume argument used into CODEX_HOME.
                resume_arg = (step2_attempt / "codex_home" / "resume_arg.txt").read_text(encoding="utf-8").strip()
                self.assertEqual(resume_arg, "tid_1")
            finally:
                os.environ["PATH"] = old_path


if __name__ == "__main__":
    unittest.main()
