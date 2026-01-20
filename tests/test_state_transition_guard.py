from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.state_update import read_state_best_effort, write_state_guarded


class StateTransitionGuardTests(unittest.TestCase):
    def test_allows_normal_transitions_and_freezes_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "state.json"
            self.assertTrue(
                write_state_guarded(
                    p,
                    {
                        "status": "queued",
                        "started_at": "2026-01-15T00:00:00Z",
                        "last_heartbeat_at": None,
                        "current_item": "queued/initializing",
                        "exit_code": None,
                        "pid": None,
                        "artifacts": [],
                        "errors": [],
                    },
                )
            )
            self.assertTrue(
                write_state_guarded(
                    p,
                    {
                        "status": "running",
                        "started_at": "2026-01-15T00:00:00Z",
                        "last_heartbeat_at": "2026-01-15T00:01:00Z",
                        "current_item": None,
                        "exit_code": None,
                        "pid": 123,
                        "artifacts": [],
                        "errors": [],
                    },
                )
            )
            self.assertTrue(
                write_state_guarded(
                    p,
                    {
                        "status": "succeeded",
                        "started_at": "2026-01-15T00:00:00Z",
                        "ended_at": "2026-01-15T00:02:00Z",
                        "last_heartbeat_at": "2026-01-15T00:01:00Z",
                        "current_item": None,
                        "exit_code": 0,
                        "pid": 123,
                        "artifacts": [],
                        "errors": [],
                    },
                )
            )
            before = json.dumps(read_state_best_effort(p), sort_keys=True)
            self.assertFalse(
                write_state_guarded(
                    p,
                    {
                        "status": "running",
                        "started_at": "2026-01-15T00:00:00Z",
                        "last_heartbeat_at": "2026-01-15T00:03:00Z",
                        "current_item": None,
                        "exit_code": None,
                        "pid": 123,
                        "artifacts": [],
                        "errors": [],
                    },
                )
            )
            after = json.dumps(read_state_best_effort(p), sort_keys=True)
            self.assertEqual(before, after)

    def test_rejects_illegal_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "state.json"
            write_state_guarded(
                p,
                {
                    "status": "running",
                    "started_at": "2026-01-15T00:00:00Z",
                    "last_heartbeat_at": "2026-01-15T00:01:00Z",
                    "current_item": None,
                    "exit_code": None,
                    "pid": 123,
                    "artifacts": [],
                    "errors": [],
                },
            )
            with self.assertRaises(ValueError):
                write_state_guarded(
                    p,
                    {
                        "status": "queued",
                        "started_at": "2026-01-15T00:00:00Z",
                        "last_heartbeat_at": None,
                        "current_item": "queued/initializing",
                        "exit_code": None,
                        "pid": None,
                        "artifacts": [],
                        "errors": [],
                    },
                )

