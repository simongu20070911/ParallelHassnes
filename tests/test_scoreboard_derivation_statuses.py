from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.harness.harness import Harness
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard, compute_system_scoreboard
from parallelhassnes.storage.runs_store import RunsStore


def _write_harness_config(runs_root: Path, runner_ids: list[str] | None = None) -> None:
    (runs_root / "_system").mkdir(parents=True, exist_ok=True)
    (runs_root / "_system" / "harness_config.json").write_text(
        json.dumps(
            {
                "harness_config_version": "hc_test",
                "written_at": "2026-01-14T00:00:00Z",
                "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                "defaults": {
                    "execution_policy": {},
                    "timeouts": {},
                    "retries": {"max_attempts": 1},
                    "retention_policy": {},
                    "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
                    "workspace_policy": {"mode": "shared"},
                    "runner_capacity": 1,
                    "runner_pool": {"shared_filesystem": True, "runner_ids": runner_ids or ["r1"]},
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


class ScoreboardDerivationStatusTests(unittest.TestCase):
    def test_blocked_due_to_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b1"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [
                                    {"step_id": "a", "prompt": "x"},
                                    {"step_id": "b", "prompt": "y", "depends_on": ["a"]},
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            store.write_current(batch_id, "j1", {"batch_id": batch_id, "job_id": "j1", "updated_at": utc_isoformat(), "steps": {}})

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["blocked"]), 1)
            self.assertEqual(int(sb["counts"]["ready"]), 1)
            blocked = sb.get("blocked") or []
            self.assertTrue(any(x.get("step_id") == "b" and "depends_on" in " ".join(x.get("reasons") or []) for x in blocked))

    def test_succeeded_when_any_success_exists_even_if_latest_failed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b_success_precedence"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s1", "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            run1_dir = store.paths.attempt_dir(batch_id, "j1", "s1", "20260114T000001Z_run_1")
            run2_dir = store.paths.attempt_dir(batch_id, "j1", "s1", "20260114T000002Z_run_2")
            run1_dir.mkdir(parents=True, exist_ok=True)
            run2_dir.mkdir(parents=True, exist_ok=True)
            (run1_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (run2_dir / "codex_home").mkdir(parents=True, exist_ok=True)
            (run2_dir / "state.json").write_text(
                json.dumps(
                    {
                        "status": "failed",
                        "started_at": "2026-01-14T00:00:02Z",
                        "ended_at": "2026-01-14T00:00:03Z",
                        "last_heartbeat_at": "2026-01-14T00:00:03Z",
                        "current_item": None,
                        "exit_code": 1,
                        "pid": None,
                        "artifacts": [],
                        "errors": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {
                        "s1": {
                            "latest": {"run_id": "run_2", "attempt_dir": str(run2_dir), "resume_base_dir": str(run2_dir / "codex_home")},
                            "latest_successful": {"run_id": "run_1", "attempt_dir": str(run1_dir), "resume_base_dir": str(run1_dir / "codex_home")},
                            "by_run_id": {
                                "run_1": {"run_id": "run_1", "attempt_dir": str(run1_dir), "resume_base_dir": str(run1_dir / "codex_home"), "status": "succeeded", "ended_at": "2026-01-14T00:00:01Z"},
                                "run_2": {"run_id": "run_2", "attempt_dir": str(run2_dir), "resume_base_dir": str(run2_dir / "codex_home"), "status": "failed", "ended_at": "2026-01-14T00:00:03Z"},
                            },
                        }
                    },
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["succeeded"]), 1)
            self.assertEqual(int(sb["counts"]["failed"]), 0)
            self.assertEqual(len(sb.get("failures") or []), 0)

    def test_blocked_due_to_missing_resume_base(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b2"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [
                                    {"step_id": "s1", "prompt": "x"},
                                    {"step_id": "s2", "prompt": "y", "resume_from": {"step_id": "s1"}},
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            missing = str(root / "does_not_exist")
            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {
                        "s1": {
                            "by_run_id": {
                                "r1": {
                                    "run_id": "r1",
                                    "attempt_dir": "x",
                                    "resume_base_dir": missing,
                                    "status": "succeeded",
                                    "ended_at": "2026-01-14T00:00:01Z",
                                }
                            }
                        }
                    },
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["blocked"]), 1)
            blocked = sb.get("blocked") or []
            self.assertTrue(any(x.get("step_id") == "s2" and "resume_from" in " ".join(x.get("reasons") or []) and "resume base" in " ".join(x.get("reasons") or []) for x in blocked))

    def test_resume_visibility_includes_ready_resume_steps(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b_resume_vis_ready"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [
                                    {"step_id": "s1", "prompt": "x"},
                                    {"step_id": "s2", "prompt": "y", "resume_from": {"step_id": "s1", "selector": "latest_successful"}},
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            # Source attempt exists and is successful (so s2 is "ready").
            attempt1 = store.paths.attempt_dir(batch_id, "j1", "s1", "20260114T000001Z_run_1")
            attempt1.mkdir(parents=True, exist_ok=True)
            (attempt1 / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt1 / "state.json").write_text(
                json.dumps(
                    {
                        "status": "succeeded",
                        "started_at": "2026-01-14T00:00:01Z",
                        "ended_at": "2026-01-14T00:00:02Z",
                        "last_heartbeat_at": "2026-01-14T00:00:02Z",
                        "current_item": None,
                        "exit_code": 0,
                        "pid": None,
                        "artifacts": [],
                        "errors": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {
                        "s1": {
                            "latest_successful": {"run_id": "run_1", "attempt_dir": str(attempt1), "resume_base_dir": str(attempt1 / "codex_home")},
                            "by_run_id": {
                                "run_1": {"run_id": "run_1", "attempt_dir": str(attempt1), "resume_base_dir": str(attempt1 / "codex_home"), "status": "succeeded", "ended_at": "2026-01-14T00:00:02Z"}
                            },
                        }
                    },
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["ready"]), 1)
            resume_steps = sb.get("resume_steps") or []
            self.assertTrue(any(x.get("job_id") == "j1" and x.get("step_id") == "s2" and x.get("derived_status") == "ready" and (x.get("resume_from_resolved") or {}).get("resume_from_step_id") == "s1" for x in resume_steps))

    def test_system_scoreboard_counts_respect_runner_locality_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"

            # shared_filesystem=false and no transfer => resume steps must be pinned to source runner to be runnable.
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
            (runs_root / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2026-01-14T00:00:00Z",
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "execution_policy": {},
                            "timeouts": {},
                            "retries": {"max_attempts": 1},
                            "retention_policy": {},
                            "scoreboard": {"heartbeat_interval_seconds": 900, "heartbeat_stale_after_seconds": 2700},
                            "workspace_policy": {"mode": "shared"},
                            "runner_capacity": 1,
                            "runner_pool": {"shared_filesystem": False, "runner_ids": ["runner_b"], "resume_base_transfer_enabled": False},
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

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            # Create runner health file for runner_b so it's considered available.
            (runners_root / "runner_b").mkdir(parents=True, exist_ok=True)
            (runners_root / "runner_b" / "health.json").write_text('{"runner_id":"runner_b","last_seen_at":"2026-01-14T00:00:00Z"}\n', encoding="utf-8")

            batch_id = "b_sys_locality"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [
                                    {"step_id": "s1", "prompt": "x"},
                                    {"step_id": "s2", "prompt": "y", "resume_from": {"step_id": "s1", "selector": "latest_successful"}},
                                ],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            # Source attempt succeeded on runner_a (not available).
            attempt1 = store.paths.attempt_dir(batch_id, "j1", "s1", "20260114T000001Z_run_1")
            attempt1.mkdir(parents=True, exist_ok=True)
            (attempt1 / "codex_home").mkdir(parents=True, exist_ok=True)
            (attempt1 / "meta.json").write_text('{"batch_id":"x","job_id":"x","step_id":"x","run_id":"x","runner_id":"runner_a","invocation":"exec"}\n', encoding="utf-8")
            (attempt1 / "state.json").write_text(
                json.dumps(
                    {
                        "status": "succeeded",
                        "started_at": "2026-01-14T00:00:01Z",
                        "ended_at": "2026-01-14T00:00:02Z",
                        "last_heartbeat_at": "2026-01-14T00:00:02Z",
                        "current_item": None,
                        "exit_code": 0,
                        "pid": None,
                        "artifacts": [],
                        "errors": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {
                        "s1": {
                            "latest_successful": {"run_id": "run_1", "attempt_dir": str(attempt1), "resume_base_dir": str(attempt1 / "codex_home")},
                            "by_run_id": {
                                "run_1": {"run_id": "run_1", "attempt_dir": str(attempt1), "resume_base_dir": str(attempt1 / "codex_home"), "status": "succeeded", "ended_at": "2026-01-14T00:00:02Z"}
                            },
                        }
                    },
                },
            )

            sys_sb = compute_system_scoreboard(store)
            row = next(x for x in sys_sb if x["batch_id"] == batch_id)
            self.assertEqual(int(row["counts"]["blocked"]), 1)

    def test_running_with_fresh_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b3"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            attempt_dir = store.paths.attempt_dir(batch_id, "j1", "s", "attempt1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "state.json").write_text(
                json.dumps(
                    {
                        "status": "running",
                        "started_at": utc_isoformat(),
                        "last_heartbeat_at": utc_isoformat(),
                        "pid": None,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {"s": {"latest": {"run_id": "r1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}}},
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["running"]), 1)
            # No stuck/attention derived when heartbeat is fresh.
            att = sb.get("attention") or []
            self.assertFalse(any(x.get("kind") == "stuck" for x in att))

    def test_stuck_detection_beyond_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            _write_harness_config(runs_root)
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id = "b4"
            (runs_root / batch_id).mkdir(parents=True, exist_ok=True)
            (runs_root / batch_id / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": "hc_test",
                        "batch_goal_summary": ("word " * 200).strip(),
                        "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x"}]}],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            attempt_dir = store.paths.attempt_dir(batch_id, "j1", "s", "attempt1")
            attempt_dir.mkdir(parents=True, exist_ok=True)
            (attempt_dir / "state.json").write_text(
                json.dumps(
                    {
                        "status": "running",
                        "started_at": "2026-01-14T00:00:00Z",
                        # Very stale heartbeat.
                        "last_heartbeat_at": "2026-01-14T00:00:00Z",
                        "pid": None,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            store.write_current(
                batch_id,
                "j1",
                {
                    "batch_id": batch_id,
                    "job_id": "j1",
                    "updated_at": utc_isoformat(),
                    "steps": {"s": {"latest": {"run_id": "r1", "attempt_dir": str(attempt_dir), "resume_base_dir": str(attempt_dir / "codex_home")}}},
                },
            )

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["running"]), 1)
            att = sb.get("attention") or []
            self.assertTrue(any(x.get("kind") == "stuck" and x.get("step_id") == "s" for x in att))

    def test_needs_attention_from_schema_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"
            _write_harness_config(runs_root)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            # Create a stricter schema that will fail validation for the fake invoker output.
            strict = root / "strict.schema.json"
            strict.write_text(
                json.dumps(
                    {
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["schema_version", "status", "summary", "files_read", "files_written", "artifacts", "extra_required_field"],
                        "properties": {"extra_required_field": {"type": "string"}},
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
                "concurrency": 1,
                "jobs": [{"job_id": "j1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x", "output_schema_ref": str(strict)}]}],
            }
            incoming = queue_root / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            (incoming / "lt.json").write_text(json.dumps(lt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            batch_id = store.list_batches()[0]
            cur = store.read_current(batch_id, "j1") or {}
            latest = (((cur.get("steps") or {}).get("s") or {}).get("latest") or {})
            attempt_dir = Path(latest["attempt_dir"])
            st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(st.get("status"), "needs_attention")
            self.assertTrue(any("schema validation failed" in e for e in (st.get("errors") or [])))

            sb = compute_batch_scoreboard(store, batch_id=batch_id)
            self.assertEqual(int(sb["counts"]["needs_attention"]), 1)
            att = sb.get("attention") or []
            self.assertTrue(any(x.get("kind") == "needs_attention" and x.get("step_id") == "s" for x in att))


if __name__ == "__main__":
    unittest.main()
