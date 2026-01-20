from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.storage.runs_store import RunsStore


_RUN_REPORT_OK = json.dumps(
    {
        "schema_version": "1.0.0",
        "status": "ok",
        "summary": "ok",
        "files_read": [],
        "files_written": [],
        "artifacts": [],
    }
)


def _write_fake_codex(bin_dir: Path) -> None:
    """
    Create a fake `codex` executable that supports the subset used by Runner:
    - `codex exec ... --json --output-last-message <path> -`
    - `codex exec ... --output-schema <path> -`
    - `codex --version`
    Behavior is controlled by env vars:
    - FAKE_CODEX_MODE = "exit1" | "sleep"
    - FAKE_CODEX_SLEEP_SECONDS = float string
    """
    script = r"""#!/usr/bin/env python3
import os, sys, time, json

def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        sys.stdout.write("codex 0.0.0-fake\n")
        return 0

    # Runner uses: codex exec ... [resume ...] -
    args = sys.argv[1:]
    if not args or args[0] != "exec":
        sys.stderr.write("unsupported invocation\n")
        return 2

    out_path = None
    wants_json = False
    if "--output-last-message" in args:
        i = args.index("--output-last-message")
        out_path = args[i + 1]
    if "--json" in args:
        wants_json = True

    # Consume stdin (prompt) to match runner behavior.
    try:
        sys.stdin.read()
    except Exception:
        pass

    mode = os.environ.get("FAKE_CODEX_MODE", "exit1")
    if mode == "sleep":
        s = float(os.environ.get("FAKE_CODEX_SLEEP_SECONDS", "1.0"))
        time.sleep(s)
        # If we survived the sleep, write output and succeed.
        if out_path:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(os.environ.get("FAKE_CODEX_FINAL", "{}"))
        if wants_json:
            sys.stdout.write(json.dumps({"type": "thread.started", "thread_id": "t_fake"}) + "\n")
        return 0

    # Default: write output (if requested) and exit non-zero.
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(os.environ.get("FAKE_CODEX_FINAL", "{}"))
    if wants_json:
        sys.stdout.write(json.dumps({"type": "thread.started", "thread_id": "t_fake"}) + "\n")
    sys.stderr.write("simulated error\n")
    return 1

if __name__ == "__main__":
    raise SystemExit(main())
"""
    p = bin_dir / "codex"
    p.write_text(script, encoding="utf-8")
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


class CodexTimeoutVsErrorTests(unittest.TestCase):
    def test_nonzero_exit_is_not_mislabeled_as_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Fake codex in PATH.
            bin_dir = root / "bin"
            bin_dir.mkdir(parents=True, exist_ok=True)
            _write_fake_codex(bin_dir)

            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = str(bin_dir) + os.pathsep + old_path
            os.environ["FAKE_CODEX_MODE"] = "exit1"
            os.environ["FAKE_CODEX_FINAL"] = _RUN_REPORT_OK
            try:
                store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
                # Minimal harness config with non-null timeout (so we exercise the timeout logic path).
                hcfg = {
                    "harness_config_version": "hc_test",
                    "written_at": "2026-01-15T00:00:00Z",
                    "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                    "defaults": {
                        "execution_policy": {"skip_git_repo_check": True, "capture_events_jsonl": True},
                        "timeouts": {"step_timeout_seconds": 5},
                        "retries": {"max_attempts": 1},
                        "retention_policy": {},
                        "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
                        "workspace_policy": {"mode": "shared"},
                        "runner_capacity": 1,
                        "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"]},
                        "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                    },
                    "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
                }
                runner = Runner(
                    store=store,
                    cfg=RunnerConfig(runner_id="r1", use_fake_invoker=False, baseline_output_schema_path=str((root / "run_report.schema.json").resolve())),
                    harness_cfg=hcfg,
                )
                runner.execute_step(
                    batch={"batch_id": "b", "effective_defaults": {}, "packs": {}},
                    job={"job_id": "j", "working_directory": "."},
                    step={"step_id": "s", "prompt": "x", "timeout_seconds": 0.5},
                    job_workdir=str(root),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(root),
                    current={"batch_id": "b", "job_id": "j", "updated_at": "2026-01-15T00:00:00Z", "steps": {}},
                )

                # Locate attempt dir via current.json; ensure state.json errors are not a timeout.
                cur = store.read_current("b", "j") or {}
                latest = (((cur.get("steps") or {}).get("s") or {}).get("latest") or {})
                attempt_dir = Path(latest["attempt_dir"])
                st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
                errs = " ".join(st.get("errors") or [])
                self.assertNotIn("step timeout", errs)
                self.assertTrue(st.get("status") in {"failed", "needs_attention"})
            finally:
                os.environ["PATH"] = old_path

    def test_timeout_is_reported_as_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            bin_dir = root / "bin"
            bin_dir.mkdir(parents=True, exist_ok=True)
            _write_fake_codex(bin_dir)

            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = str(bin_dir) + os.pathsep + old_path
            os.environ["FAKE_CODEX_MODE"] = "sleep"
            os.environ["FAKE_CODEX_SLEEP_SECONDS"] = "2.0"
            os.environ["FAKE_CODEX_FINAL"] = _RUN_REPORT_OK
            try:
                store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
                hcfg = {
                    "harness_config_version": "hc_test",
                    "written_at": "2026-01-15T00:00:00Z",
                    "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                    "defaults": {
                        "execution_policy": {"skip_git_repo_check": True, "capture_events_jsonl": True},
                        "timeouts": {"step_timeout_seconds": 5},
                        "retries": {"max_attempts": 1},
                        "retention_policy": {},
                        "scoreboard": {"heartbeat_interval_seconds": 1, "heartbeat_stale_after_seconds": 2700},
                        "workspace_policy": {"mode": "shared"},
                        "runner_capacity": 1,
                        "runner_pool": {"shared_filesystem": True, "runner_ids": ["r1"]},
                        "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                    },
                    "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
                }
                runner = Runner(
                    store=store,
                    cfg=RunnerConfig(runner_id="r1", use_fake_invoker=False, baseline_output_schema_path=str((root / "run_report.schema.json").resolve())),
                    harness_cfg=hcfg,
                )
                runner.execute_step(
                    batch={"batch_id": "b", "effective_defaults": {}, "packs": {}},
                    job={"job_id": "j", "working_directory": "."},
                    step={"step_id": "s", "prompt": "x", "timeout_seconds": 0.2},
                    job_workdir=str(root),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(root),
                    current={"batch_id": "b", "job_id": "j", "updated_at": "2026-01-15T00:00:00Z", "steps": {}},
                )

                cur = store.read_current("b", "j") or {}
                latest = (((cur.get("steps") or {}).get("s") or {}).get("latest") or {})
                attempt_dir = Path(latest["attempt_dir"])
                st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
                errs = " ".join(st.get("errors") or [])
                self.assertIn("step timeout", errs)
                self.assertEqual(st.get("status"), "needs_attention")
            finally:
                os.environ["PATH"] = old_path


if __name__ == "__main__":
    unittest.main()

