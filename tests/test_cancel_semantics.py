from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.ops.cancel import cancel_attempt


class CancelSemanticsTests(unittest.TestCase):
    def test_cancel_does_not_overwrite_terminal_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            attempt_dir = Path(td) / "attempt"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": "b", "job_id": "j", "step_id": "s", "run_id": "r", "runner_id": "runner_x", "invocation": "exec", "prompt_sha256": "x", "codex_cli_version": "unknown"})
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "state.json").write_text(
                json.dumps({"status": "succeeded", "started_at": "2026-01-14T00:00:00Z", "ended_at": "2026-01-14T00:00:01Z"}) + "\n",
                encoding="utf-8",
            )

            res = cancel_attempt(attempt_dir)
            self.assertTrue(res.get("already_terminal"))
            st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(st["status"], "succeeded")

    def test_cancel_when_state_missing_preserves_known_partial_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            attempt_dir = Path(td) / "attempt"
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt_dir / "meta.json").write_text(
                json.dumps({"batch_id": "b", "job_id": "j", "step_id": "s", "run_id": "r", "runner_id": "runner_x", "invocation": "exec", "prompt_sha256": "x", "codex_cli_version": "unknown"})
                + "\n",
                encoding="utf-8",
            )
            (attempt_dir / "final.txt").write_text("partial\n", encoding="utf-8")

            res = cancel_attempt(attempt_dir)
            self.assertTrue(res.get("canceled"))
            st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(st["status"], "canceled")
            arts = st.get("artifacts") or []
            paths = {a.get("path") for a in arts if isinstance(a, dict)}
            self.assertIn(str((attempt_dir / "final.txt").resolve()), paths)
            self.assertIn(str((attempt_dir / "codex_home").resolve()), paths)


if __name__ == "__main__":
    unittest.main()

