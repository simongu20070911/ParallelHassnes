from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.resume.selectors import resolve_resume_source


class ResumeSelectorDeterminismTests(unittest.TestCase):
    def test_latest_successful_selects_max_ended_at(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(runs_root=root / "runs", runners_root=root / "runners")
            batch_id, job_id = "b", "j"
            src_step = "s1"

            base1 = root / "base1"
            base2 = root / "base2"
            base1.mkdir(parents=True, exist_ok=True)
            base2.mkdir(parents=True, exist_ok=True)

            current = {
                "batch_id": batch_id,
                "job_id": job_id,
                "updated_at": "2026-01-14T00:00:00Z",
                "steps": {
                    src_step: {
                        # Intentionally wrong pointer to ensure we don't depend on it.
                        "latest_successful": {"run_id": "r_old", "attempt_dir": "x", "resume_base_dir": str(base1)},
                        "by_run_id": {
                            "r1": {"run_id": "r1", "attempt_dir": str(root / "a1"), "resume_base_dir": str(base1), "status": "succeeded", "ended_at": "2026-01-14T00:00:01Z"},
                            "r2": {"run_id": "r2", "attempt_dir": str(root / "a2"), "resume_base_dir": str(base2), "status": "succeeded", "ended_at": "2026-01-14T00:00:02Z"},
                        },
                    }
                },
            }

            resolved = resolve_resume_source(paths, batch_id=batch_id, job_id=job_id, current=current, resume_from={"step_id": src_step, "selector": "latest_successful"})
            self.assertEqual(resolved.run_id, "r2")
            self.assertEqual(Path(resolved.resume_base_dir).resolve(), base2.resolve())

    def test_latest_selects_max_attempt_dir_basename(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(runs_root=root / "runs", runners_root=root / "runners")
            batch_id, job_id = "b", "j"
            src_step = "s1"

            base1 = root / "base1"
            base2 = root / "base2"
            base1.mkdir(parents=True, exist_ok=True)
            base2.mkdir(parents=True, exist_ok=True)

            ad1 = root / "20260114T000000Z_r1"
            ad2 = root / "20260115T000000Z_r2"

            current = {
                "batch_id": batch_id,
                "job_id": job_id,
                "updated_at": "2026-01-14T00:00:00Z",
                "steps": {
                    src_step: {
                        # Intentionally wrong pointer.
                        "latest": {"run_id": "r1", "attempt_dir": str(ad1), "resume_base_dir": str(base1)},
                        "by_run_id": {
                            "r1": {"run_id": "r1", "attempt_dir": str(ad1), "resume_base_dir": str(base1), "status": "failed", "ended_at": "2026-01-14T00:00:01Z"},
                            # latest selector must pick the most recent *terminal* attempt (not running).
                            "r2": {"run_id": "r2", "attempt_dir": str(ad2), "resume_base_dir": str(base2), "status": "failed", "ended_at": "2026-01-15T00:00:01Z"},
                        },
                    }
                },
            }

            resolved = resolve_resume_source(paths, batch_id=batch_id, job_id=job_id, current=current, resume_from={"step_id": src_step, "selector": "latest"})
            self.assertEqual(resolved.run_id, "r2")
            self.assertEqual(Path(resolved.resume_base_dir).resolve(), base2.resolve())

    def test_run_id_discoverable_by_directory_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            paths = Paths(runs_root=root / "runs", runners_root=root / "runners")
            batch_id, job_id = "b", "j"
            src_step = "s1"
            run_id = "run_20260114T000000Z_deadbeef"

            # Create attempt directory under the canonical attempts dir, ending with _<run_id>.
            attempt_dir = paths.attempt_dir(batch_id, job_id, src_step, f"20260114T000000Z_{run_id}")
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)

            current = {"batch_id": batch_id, "job_id": job_id, "updated_at": "2026-01-14T00:00:00Z", "steps": {src_step: {"by_run_id": {}}}}

            resolved = resolve_resume_source(
                paths,
                batch_id=batch_id,
                job_id=job_id,
                current=current,
                resume_from={"step_id": src_step, "selector": "run_id", "run_id": run_id},
            )
            self.assertEqual(resolved.run_id, run_id)
            self.assertEqual(Path(resolved.attempt_dir).resolve(), attempt_dir.resolve())
            self.assertEqual(Path(resolved.resume_base_dir).resolve(), (attempt_dir / "codex_home").resolve())


if __name__ == "__main__":
    unittest.main()
