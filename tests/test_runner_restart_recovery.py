from __future__ import annotations

import json
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.recovery.recovery import recover_orphaned_running_attempts, shutdown_orphan_heartbeat_threads
from parallelhassnes.storage.runs_store import RunsStore


class RunnerRestartRecoveryTests(unittest.TestCase):
    def test_runner_restart_continues_heartbeats_if_pid_alive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id, job_id, step_id, run_id = "b1", "j1", "s1", "r1"

            # Minimal batch meta to satisfy list_batches().
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            attempt_dir = store.paths.attempt_dir(batch_id, job_id, step_id, "attempt1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            state_path = store.paths.attempt_state_path(attempt_dir)

            p = subprocess.Popen(["sleep", "3"])
            try:
                state_path.write_text(
                    json.dumps(
                        {
                            "status": "running",
                            "started_at": "2026-01-14T00:00:00Z",
                            "last_heartbeat_at": "2026-01-14T00:00:00Z",
                            "pid": p.pid,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )

                store.write_current(
                    batch_id,
                    job_id,
                    {
                        "batch_id": batch_id,
                        "job_id": job_id,
                        "updated_at": "2026-01-14T00:00:00Z",
                        "steps": {
                            step_id: {
                                "latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                                "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}},
                            }
                        },
                    },
                )

                recover_orphaned_running_attempts(store, heartbeat_stale_after_seconds=1, heartbeat_interval_seconds=1)
                st1 = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(st1.get("status"), "running")
                hb1 = st1.get("last_heartbeat_at")
                self.assertIsInstance(hb1, str)

                time.sleep(1.2)
                st2 = json.loads(state_path.read_text(encoding="utf-8"))
                self.assertEqual(st2.get("status"), "running")
                hb2 = st2.get("last_heartbeat_at")
                self.assertIsInstance(hb2, str)
                self.assertNotEqual(hb1, hb2)
            finally:
                try:
                    p.terminate()
                    p.wait(timeout=2)
                except Exception:
                    pass
                shutdown_orphan_heartbeat_threads()

    def test_runner_restart_finalizes_if_pid_gone(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id, job_id, step_id, run_id = "b2", "j2", "s2", "r2"

            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": job_id, "working_directory": ".", "steps": [{"step_id": step_id, "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            attempt_dir = store.paths.attempt_dir(batch_id, job_id, step_id, "attempt1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            state_path = store.paths.attempt_state_path(attempt_dir)
            state_path.write_text(
                json.dumps(
                    {
                        "status": "running",
                        "started_at": "2026-01-14T00:00:00Z",
                        "last_heartbeat_at": "2026-01-14T00:00:00Z",
                        "pid": 999999,  # expected to be non-existent
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            store.write_current(
                batch_id,
                job_id,
                {
                    "batch_id": batch_id,
                    "job_id": job_id,
                    "updated_at": "2026-01-14T00:00:00Z",
                    "steps": {
                        step_id: {
                            "latest": {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")},
                            "by_run_id": {run_id: {"run_id": run_id, "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home"), "status": "running"}},
                        }
                    },
                },
            )

            recover_orphaned_running_attempts(store, heartbeat_stale_after_seconds=1, heartbeat_interval_seconds=1)

            st = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(st.get("status"), "needs_attention")
            self.assertIsInstance(st.get("ended_at"), str)
            self.assertIn("pid not alive", st.get("current_item") or "")

            cur = store.read_current(batch_id, job_id) or {}
            ent = (((cur.get("steps") or {}).get(step_id) or {}).get("by_run_id") or {}).get(run_id) or {}
            self.assertEqual(ent.get("status"), "needs_attention")
            self.assertIsInstance(ent.get("ended_at"), str)


if __name__ == "__main__":
    unittest.main()

