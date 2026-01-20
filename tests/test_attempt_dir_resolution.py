from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.storage.runs_store import RunsStore


class AttemptDirResolutionTests(unittest.TestCase):
    def test_resolve_attempt_dir_by_suffix_scan_when_current_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b1"
            job_id = "j1"
            step_id = "s1"
            run_id = "r_suffix"
            attempts_dir = store.paths.attempts_dir(batch_id, job_id, step_id)
            attempts_dir.mkdir(parents=True, exist_ok=True)
            attempt_dir = attempts_dir / f"20260115T000000Z_{run_id}"
            attempt_dir.mkdir(parents=True, exist_ok=True)

            resolved = store.resolve_attempt_dir(batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
            self.assertEqual(resolved, attempt_dir.resolve())

    def test_resolve_attempt_dir_via_current_json_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b1"
            job_id = "j1"
            step_id = "s1"
            run_id = "r123"
            attempts_dir = store.paths.attempts_dir(batch_id, job_id, step_id)
            attempts_dir.mkdir(parents=True, exist_ok=True)
            attempt_dir = attempts_dir / f"20260115T000000Z_{run_id}"
            attempt_dir.mkdir(parents=True, exist_ok=True)

            # current.json is the job-level index that makes attempts discoverable without scanning.
            store.write_current(
                batch_id,
                job_id,
                {
                    "batch_id": batch_id,
                    "job_id": job_id,
                    "updated_at": "2026-01-15T00:00:00Z",
                    "steps": {
                        step_id: {
                            "by_run_id": {
                                run_id: {
                                    "run_id": run_id,
                                    "attempt_dir": str(attempt_dir),
                                    "resume_base_dir": str(attempt_dir / "codex_home"),
                                    "status": "succeeded",
                                    "ended_at": "2026-01-15T00:00:01Z",
                                }
                            }
                        }
                    },
                },
            )

            resolved = store.resolve_attempt_dir(batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
            self.assertEqual(resolved, attempt_dir.resolve())

    def test_resolve_attempt_dir_direct_run_id_dir_name(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b1"
            job_id = "j1"
            step_id = "s1"
            run_id = "r_direct"
            attempts_dir = store.paths.attempts_dir(batch_id, job_id, step_id)
            attempts_dir.mkdir(parents=True, exist_ok=True)
            attempt_dir = attempts_dir / run_id
            attempt_dir.mkdir(parents=True, exist_ok=True)

            resolved = store.resolve_attempt_dir(batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
            self.assertEqual(resolved, attempt_dir.resolve())
