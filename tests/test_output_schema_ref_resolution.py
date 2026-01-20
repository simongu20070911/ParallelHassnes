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
    Fake `codex` that validates `--output-schema <path>` is directly readable.

    Runner passes `-C <job_workdir>`, but this fake doesn't implement it; requiring
    a readable schema path ensures the runner resolves relative schema refs to an
    absolute path (or otherwise usable path) deterministically.
    """
    script = r"""#!/usr/bin/env python3
import sys, json, os

def main():
    if len(sys.argv) >= 2 and sys.argv[1] == "--version":
        sys.stdout.write("codex 0.0.0-fake\n")
        return 0

    args = sys.argv[1:]
    if not args or args[0] != "exec":
        sys.stderr.write("unsupported invocation\n")
        return 2

    if "--output-schema" in args:
        i = args.index("--output-schema")
        schema_path = args[i + 1]
        try:
            with open(schema_path, "rb") as f:
                f.read(1)
        except Exception as e:
            sys.stderr.write("cannot read schema: %s\n" % (e,))
            return 3

    out_path = None
    wants_json = False
    if "--output-last-message" in args:
        i = args.index("--output-last-message")
        out_path = args[i + 1]
    if "--json" in args:
        wants_json = True

    try:
        sys.stdin.read()
    except Exception:
        pass

    final = os.environ.get("FAKE_CODEX_FINAL", "{}")
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(final)
    else:
        sys.stdout.write(final)

    if wants_json:
        sys.stdout.write("\n" + json.dumps({"type": "thread.started", "thread_id": "t_fake"}) + "\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
"""
    p = bin_dir / "codex"
    p.write_text(script, encoding="utf-8")
    p.chmod(p.stat().st_mode | stat.S_IXUSR)


class OutputSchemaRefResolutionTests(unittest.TestCase):
    def test_output_schema_ref_is_relative_to_job_workdir(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Job workdir with a relative schema ref.
            job_workdir = root / "repo"
            job_workdir.mkdir(parents=True, exist_ok=True)
            schema_rel = Path("schemas") / "custom.schema.json"
            (job_workdir / schema_rel.parent).mkdir(parents=True, exist_ok=True)
            baseline_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (job_workdir / schema_rel).write_text(baseline_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Fake codex in PATH.
            bin_dir = root / "bin"
            bin_dir.mkdir(parents=True, exist_ok=True)
            _write_fake_codex(bin_dir)
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = str(bin_dir) + os.pathsep + old_path
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
                    cfg=RunnerConfig(runner_id="r1", use_fake_invoker=False, baseline_output_schema_path=str(baseline_schema.resolve())),
                    harness_cfg=hcfg,
                )
                runner.execute_step(
                    batch={"batch_id": "b", "effective_defaults": {}, "packs": {}},
                    job={"job_id": "j", "working_directory": "."},
                    step={"step_id": "s", "prompt": "x", "output_schema_ref": str(schema_rel)},
                    job_workdir=str(job_workdir),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(job_workdir),
                    current={"batch_id": "b", "job_id": "j", "updated_at": "2026-01-15T00:00:00Z", "steps": {}},
                )

                cur = store.read_current("b", "j") or {}
                latest = (((cur.get("steps") or {}).get("s") or {}).get("latest") or {})
                attempt_dir = Path(latest["attempt_dir"])
                st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
                self.assertEqual(st.get("status"), "succeeded")
                self.assertTrue((attempt_dir / "final.json").exists())
            finally:
                os.environ["PATH"] = old_path


if __name__ == "__main__":
    unittest.main()

