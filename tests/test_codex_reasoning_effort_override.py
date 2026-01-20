from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.runner import runner as runner_mod


_FAKE_CODEX = """#!/usr/bin/env python3
import json, os, sys

args = sys.argv[1:]
if len(args) >= 1 and args[0] == "--version":
    print("codex 0.0.0-test")
    raise SystemExit(0)

# Runner probes help once to adapt flags; keep this minimal and non-fatal.
if len(args) >= 2 and args[0] == "exec" and "--help" in args:
    print("usage: codex exec [OPTIONS] [PROMPT] [COMMAND]")
    raise SystemExit(0)

if not args or args[0] != "exec":
    sys.stderr.write("unsupported invocation\\n")
    raise SystemExit(2)

codex_home = os.environ.get("CODEX_HOME")
if codex_home:
    with open(os.path.join(codex_home, "argv.json"), "w", encoding="utf-8") as f:
        json.dump(sys.argv, f)

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
sys.stdout.write(final)
raise SystemExit(0)
"""


class CodexReasoningEffortOverrideTests(unittest.TestCase):
    def test_model_reasoning_effort_is_passed_via_codex_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Baseline schema at expected location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            shutil.copy2(src_schema, root / "run_report.schema.json")

            # Fake codex binary on PATH.
            fakebin = root / "fakebin"
            fakebin.mkdir(parents=True, exist_ok=True)
            codex_path = fakebin / "codex"
            codex_path.write_text(_FAKE_CODEX, encoding="utf-8")
            codex_path.chmod(codex_path.stat().st_mode | stat.S_IEXEC)

            old_path = os.environ.get("PATH", "")
            old_help_cache = runner_mod._CODEX_EXEC_HELP_CACHE
            os.environ["PATH"] = str(fakebin) + os.pathsep + old_path
            runner_mod._CODEX_EXEC_HELP_CACHE = ""
            try:
                # Minimal harness config.
                (runs_root / "_system" / "harness_config.json").write_text(
                    json.dumps(
                        {
                            "harness_config_version": "hc_test",
                            "written_at": "2026-01-15T00:00:00Z",
                            "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                            "defaults": {
                                "execution_policy": {"skip_git_repo_check": True},
                                "timeouts": {"step_timeout_seconds": 3600},
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
                    # Submission-time override: set reasoning effort low.
                    "execution_policy": {"skip_git_repo_check": True, "model_reasoning_effort": "low"},
                    "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
                }
                incoming = queue_root / "incoming"
                incoming.mkdir(parents=True, exist_ok=True)
                (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

                harness.tick_once(concurrency_override=None, use_fake_invoker=False)

                batch_id = next(p.name for p in runs_root.iterdir() if p.is_dir() and p.name.startswith("batch_"))
                cur = json.loads((runs_root / batch_id / "j1" / "current.json").read_text(encoding="utf-8"))
                attempt_dir = Path(cur["steps"]["s1"]["latest"]["attempt_dir"])
                argv = json.loads((attempt_dir / "codex_home" / "argv.json").read_text(encoding="utf-8"))

                self.assertIn("--config", argv)
                self.assertIn('model_reasoning_effort="low"', argv)
            finally:
                os.environ["PATH"] = old_path
                runner_mod._CODEX_EXEC_HELP_CACHE = old_help_cache


if __name__ == "__main__":
    unittest.main()

