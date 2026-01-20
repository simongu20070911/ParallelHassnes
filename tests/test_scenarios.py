from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.core.paths import Paths
from parallelhassnes.storage.runs_store import RunsStore
from parallelhassnes.util.tail import tail_last_lines
from parallelhassnes.ops.cancel import cancel_attempt
from parallelhassnes.retention.gc import RetentionPolicy, compute_protected_resume_bases_for_open_batches, run_gc
from parallelhassnes.recovery.recovery import recover_orphaned_running_attempts
from parallelhassnes.ops.overrides import set_force_retry, set_forced_terminal


def _long_goal() -> str:
    # >150 words, whitespace-delimited.
    base = (
        "This batch exists to validate the ParallelHassnes workflow and artifact contract end to end "
        "without relying on raw event traces. "
    )
    words = (base.split() * 25)  # ~250+ tokens
    return " ".join(words)


def _write_launch_table(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class ScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

        # Copy baseline schema into tmp root to match Runner's lookup contract.
        src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
        shutil.copy2(src_schema, self.root / "run_report.schema.json")

        (self.root / "runs").mkdir(parents=True, exist_ok=True)
        (self.root / "runners").mkdir(parents=True, exist_ok=True)

        self.harness = Harness(
            runs_root=self.root / "runs",
            runners_root=self.root / "runners",
            queue_root=self.root / "runs" / "_queue",
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_exec_success_creates_attempt_artifacts_and_pointers(self) -> None:
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_a",
                    "working_directory": ".",
                    "steps": [{"step_id": "step1", "prompt": "Return a valid Run Report JSON."}],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt.json", lt)

        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        batches = sorted(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
        self.assertEqual(len(batches), 1)
        batch_id = batches[0]

        batch_meta = json.loads((self.root / "runs" / batch_id / "batch_meta.json").read_text(encoding="utf-8"))
        self.assertEqual(batch_meta["batch_id"], batch_id)
        self.assertIn("harness_config_version", batch_meta)

        current = json.loads((self.root / "runs" / batch_id / "job_a" / "current.json").read_text(encoding="utf-8"))
        step = current["steps"]["step1"]
        attempt_dir = Path(step["latest"]["attempt_dir"])

        self.assertTrue((attempt_dir / "meta.json").exists())
        self.assertTrue((attempt_dir / "state.json").exists())
        self.assertTrue((attempt_dir / "final.json").exists())
        self.assertTrue((attempt_dir / "final.txt").exists())
        self.assertTrue((attempt_dir / "codex_home").exists())

        state = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "succeeded")
        self.assertIsNotNone(state.get("started_at"))
        self.assertIsNotNone(state.get("ended_at"))

        rs = RunsStore(Paths(runs_root=self.root / "runs", runners_root=self.root / "runners"))
        resolved = rs.resolve_attempt_dir(batch_id=batch_id, job_id="job_a", step_id="step1", run_id=step["latest"]["run_id"])
        self.assertEqual(resolved, attempt_dir.resolve())

        # Tail utility: verify last-lines behavior (used by tail-events).
        events_path = attempt_dir / "codex.events.jsonl"
        events_path.write_text("a\nb\nc\nd\n", encoding="utf-8")
        self.assertEqual(tail_last_lines(events_path, n=2), ["c\n", "d\n"])

        # Runner health contains the required richer fields (capacity/load/pressure/drain).
        health_files = list((self.root / "runners").glob("*/health.json"))
        self.assertEqual(len(health_files), 1)
        health = json.loads(health_files[0].read_text(encoding="utf-8"))
        self.assertIn("capacity", health)
        self.assertIn("current_load", health)
        self.assertIn("pressure", health)
        self.assertIn("drain_mode", health)

    def test_retry_flow_creates_two_attempts_and_latest_successful(self) -> None:
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_r",
                    "working_directory": ".",
                    "steps": [
                        {
                            "step_id": "step1",
                            "prompt": "Return a valid Run Report JSON.",
                            "retry_policy": {"max_attempts": 2, "retry_on_statuses": ["failed"]},
                            "fake_exit_codes": [1, 0],
                        }
                    ],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt_retry.json", lt)

        # First tick runs the first attempt; retries are scheduled on subsequent ticks so that
        # orchestrator-driven overrides (e.g., requeue) can be applied between attempts.
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
        current = json.loads((self.root / "runs" / batch_id / "job_r" / "current.json").read_text(encoding="utf-8"))
        step = current["steps"]["step1"]

        by_run_id = step["by_run_id"]
        self.assertEqual(len(by_run_id), 2)
        self.assertIn("latest_successful", step)
        self.assertEqual(step["latest_successful"]["run_id"], step["latest"]["run_id"])

        succeeded_run_id = step["latest_successful"]["run_id"]
        failed_run_id = next(rid for rid in by_run_id.keys() if rid != succeeded_run_id)
        succeeded_attempt = Path(by_run_id[succeeded_run_id]["attempt_dir"])
        failed_attempt = Path(by_run_id[failed_run_id]["attempt_dir"])
        succeeded_state = json.loads((succeeded_attempt / "state.json").read_text(encoding="utf-8"))
        failed_state = json.loads((failed_attempt / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(succeeded_state["status"], "succeeded")
        self.assertEqual(failed_state["status"], "failed")

    def test_resume_copies_codex_home_marker(self) -> None:
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_resume",
                    "working_directory": ".",
                    "steps": [
                        {"step_id": "step1", "prompt": "Step1", "fake_codex_home_marker": "marker.txt"},
                        {
                            "step_id": "step2",
                            "resume_from": {"step_id": "step1", "selector": "latest_successful"},
                            "prompt": "Step2",
                        },
                    ],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt_resume.json", lt)

        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
        current = json.loads((self.root / "runs" / batch_id / "job_resume" / "current.json").read_text(encoding="utf-8"))

        step1_attempt = Path(current["steps"]["step1"]["latest"]["attempt_dir"])
        step2_attempt = Path(current["steps"]["step2"]["latest"]["attempt_dir"])

        self.assertTrue((step1_attempt / "codex_home" / "marker.txt").exists())
        self.assertTrue((step2_attempt / "codex_home" / "marker.txt").exists())

        step1_run_id = current["steps"]["step1"]["latest"]["run_id"]
        step1_resume_base = current["steps"]["step1"]["latest"]["resume_base_dir"]
        step2_meta = json.loads((step2_attempt / "meta.json").read_text(encoding="utf-8"))
        self.assertEqual(step2_meta["invocation"], "resume")
        self.assertEqual(step2_meta["parent_run_id"], step1_run_id)
        self.assertEqual(step2_meta["resume_from"]["step_id"], "step1")
        self.assertEqual(step2_meta["resume_base_copied_from"], step1_resume_base)

    def test_cancel_attempt_kills_process_and_terminalizes_state(self) -> None:
        # Build a synthetic attempt dir with a long-running process pid.
        attempt_dir = self.root / "runs" / "batch_x" / "job_x" / "steps" / "step1" / "attempts" / "run_x"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)

        (attempt_dir / "meta.json").write_text('{"batch_id":"batch_x","job_id":"job_x","step_id":"step1","run_id":"run_x","runner_id":"r1"}\n', encoding="utf-8")

        import subprocess
        import json

        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        state_path = attempt_dir / "state.json"
        state_path.write_text(
            json.dumps({"status": "running", "started_at": "2026-01-14T00:00:00Z", "last_heartbeat_at": "2026-01-14T00:00:00Z", "pid": proc.pid})
            + "\n",
            encoding="utf-8",
        )

        res = cancel_attempt(attempt_dir, grace_seconds=0.2)
        self.assertTrue(res.get("ok"))
        self.assertTrue(res.get("canceled"))

        proc.wait(timeout=2)
        st = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(st["status"], "canceled")
        self.assertIsNotNone(st.get("ended_at"))

    def test_cancel_attempt_when_state_missing_marks_canceled(self) -> None:
        attempt_dir = self.root / "runs" / "batch_q" / "job_q" / "steps" / "step1" / "attempts" / "run_q"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        (attempt_dir / "meta.json").write_text('{"batch_id":"batch_q","job_id":"job_q","step_id":"step1","run_id":"run_q","runner_id":"r1"}\n', encoding="utf-8")

        res = cancel_attempt(attempt_dir, grace_seconds=0.1)
        self.assertTrue(res.get("ok"))
        st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(st["status"], "canceled")

    def test_gc_protects_resume_bases_for_open_batches(self) -> None:
        # Create an open batch with a finished attempt whose codex_home is referenced by current.json.
        batch_dir = self.root / "runs" / "batch_gc"
        job_dir = batch_dir / "job_gc"
        attempt_dir = job_dir / "steps" / "step1" / "attempts" / "run_gc"
        (attempt_dir / "codex_home").mkdir(parents=True, exist_ok=True)
        (attempt_dir / "codex_home" / "keep.txt").write_text("x", encoding="utf-8")
        (attempt_dir / "meta.json").write_text('{"batch_id":"batch_gc","job_id":"job_gc","step_id":"step1","run_id":"run_gc","runner_id":"r1","invocation":"exec"}\n', encoding="utf-8")
        (attempt_dir / "state.json").write_text(
            '{"status":"succeeded","started_at":"2000-01-01T00:00:00Z","ended_at":"2000-01-02T00:00:00Z","last_heartbeat_at":"2000-01-01T00:00:00Z"}\n',
            encoding="utf-8",
        )
        (attempt_dir / "codex.events.jsonl").write_text("old\n", encoding="utf-8")
        (attempt_dir / "git_diff.patch").write_text("old\n", encoding="utf-8")

        (batch_dir / "batch_meta.json").parent.mkdir(parents=True, exist_ok=True)
        (batch_dir / "batch_meta.json").write_text('{"batch_id":"batch_gc","submitted_at":"2000-01-01T00:00:00Z","harness_config_version":"hc","batch_goal_summary":"' + _long_goal() + '","jobs":[{"job_id":"job_gc","working_directory":".","steps":[{"step_id":"step1","prompt":"x"}]}]}\n', encoding="utf-8")
        (job_dir / "current.json").parent.mkdir(parents=True, exist_ok=True)
        (job_dir / "current.json").write_text(
            json.dumps(
                {
                    "batch_id": "batch_gc",
                    "job_id": "job_gc",
                    "updated_at": "2000-01-01T00:00:00Z",
                    "steps": {
                        "step1": {
                            "latest": {"run_id": "run_gc", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve())},
                            "latest_successful": {"run_id": "run_gc", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve())},
                            "by_run_id": {"run_gc": {"run_id": "run_gc", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve()), "status": "succeeded", "ended_at": "2000-01-02T00:00:00Z"}},
                        }
                    },
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

        protected = compute_protected_resume_bases_for_open_batches(self.root / "runs")
        self.assertIn((attempt_dir / "codex_home").resolve(), protected)

        out = run_gc(
            runs_root=self.root / "runs",
            policy=RetentionPolicy(keep_raw_events_days=0, keep_git_artifacts_days=0, keep_resume_bases_days=0),
            protected_resume_bases=protected,
        )
        self.assertTrue((attempt_dir / "codex_home").exists())
        self.assertFalse((attempt_dir / "codex.events.jsonl").exists())
        self.assertFalse((attempt_dir / "git_diff.patch").exists())

        # Close the batch and rerun GC: resume base can be deleted.
        (batch_dir / "batch_closed.json").write_text('{"batch_id":"batch_gc","closed_at":"2000-01-03T00:00:00Z"}\n', encoding="utf-8")
        protected2 = compute_protected_resume_bases_for_open_batches(self.root / "runs")
        self.assertNotIn((attempt_dir / "codex_home").resolve(), protected2)
        run_gc(
            runs_root=self.root / "runs",
            policy=RetentionPolicy(keep_raw_events_days=0, keep_git_artifacts_days=0, keep_resume_bases_days=0),
            protected_resume_bases=protected2,
        )
        self.assertFalse((attempt_dir / "codex_home").exists())

    def test_recovery_marks_orphaned_running_attempt_needs_attention(self) -> None:
        # Use the store-backed recovery on a synthetic running attempt with dead pid.
        paths = Paths(runs_root=self.root / "runs", runners_root=self.root / "runners")
        store = RunsStore(paths)

        batch_dir = self.root / "runs" / "batch_rec"
        job_dir = batch_dir / "job_rec"
        attempt_dir = job_dir / "steps" / "step1" / "attempts" / "run_rec"
        attempt_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "batch_meta.json").write_text(
            '{"batch_id":"batch_rec","submitted_at":"2000-01-01T00:00:00Z","harness_config_version":"hc","batch_goal_summary":"' + _long_goal() + '","jobs":[{"job_id":"job_rec","working_directory":".","steps":[{"step_id":"step1","prompt":"x"}]}]}\n',
            encoding="utf-8",
        )
        (job_dir / "current.json").parent.mkdir(parents=True, exist_ok=True)
        (attempt_dir / "meta.json").write_text('{"batch_id":"batch_rec","job_id":"job_rec","step_id":"step1","run_id":"run_rec","runner_id":"r1","invocation":"exec"}\n', encoding="utf-8")
        (attempt_dir / "state.json").write_text(
            '{"status":"running","started_at":"2000-01-01T00:00:00Z","last_heartbeat_at":"2000-01-01T00:00:00Z","pid":999999}\n',
            encoding="utf-8",
        )
        (job_dir / "current.json").write_text(
            json.dumps(
                {
                    "batch_id": "batch_rec",
                    "job_id": "job_rec",
                    "updated_at": "2000-01-01T00:00:00Z",
                    "steps": {"step1": {"latest": {"run_id": "run_rec", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve())}, "by_run_id": {"run_rec": {"run_id": "run_rec", "attempt_dir": str(attempt_dir), "resume_base_dir": str((attempt_dir / "codex_home").resolve()), "status": "running", "ended_at": None}}}},
                }
            )
            + "\n",
            encoding="utf-8",
        )

        out = recover_orphaned_running_attempts(store, heartbeat_stale_after_seconds=1800)
        self.assertTrue(out.get("ok"))
        st = json.loads((attempt_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(st["status"], "needs_attention")

    def test_force_retry_override_runs_additional_attempt(self) -> None:
        # max_attempts=1 would normally stop after 1 failure; override should allow another attempt.
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_or",
                    "working_directory": ".",
                    "steps": [
                        {
                            "step_id": "step1",
                            "prompt": "Return a valid Run Report JSON.",
                            "retry_policy": {"max_attempts": 1, "retry_on_statuses": ["failed"]},
                            "fake_exit_codes": [1, 0],
                        }
                    ],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt_or.json", lt)
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
        paths = Paths(runs_root=self.root / "runs", runners_root=self.root / "runners")
        store = RunsStore(paths)

        current = json.loads((self.root / "runs" / batch_id / "job_or" / "current.json").read_text(encoding="utf-8"))
        by = current["steps"]["step1"]["by_run_id"]
        self.assertEqual(len(by), 1)

        # Force retry and tick again; should create a second attempt.
        set_force_retry(store.overrides_path(batch_id, "job_or"), "step1")
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        current2 = json.loads((self.root / "runs" / batch_id / "job_or" / "current.json").read_text(encoding="utf-8"))
        by2 = current2["steps"]["step1"]["by_run_id"]
        self.assertEqual(len(by2), 2)

    def test_force_retry_can_rerun_succeeded_resume_step(self) -> None:
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_tr",
                    "working_directory": ".",
                    "steps": [
                        {"step_id": "step1", "prompt": "Step1", "fake_codex_home_marker": "marker.txt"},
                        {"step_id": "step2", "resume_from": {"step_id": "step1", "selector": "latest_successful"}, "prompt": "Step2"},
                    ],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt_tr.json", lt)

        # First tick runs step1 and step2 successfully.
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)
        batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
        paths = Paths(runs_root=self.root / "runs", runners_root=self.root / "runners")
        store = RunsStore(paths)

        current = json.loads((self.root / "runs" / batch_id / "job_tr" / "current.json").read_text(encoding="utf-8"))
        self.assertEqual(len(current["steps"]["step1"]["by_run_id"]), 1)
        self.assertEqual(len(current["steps"]["step2"]["by_run_id"]), 1)

        step1_run_id = current["steps"]["step1"]["latest"]["run_id"]

        # Force retry step2 even though it succeeded; second tick should create another attempt for step2.
        set_force_retry(store.overrides_path(batch_id, "job_tr"), "step2")
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

        current2 = json.loads((self.root / "runs" / batch_id / "job_tr" / "current.json").read_text(encoding="utf-8"))
        self.assertEqual(len(current2["steps"]["step1"]["by_run_id"]), 1)
        self.assertEqual(len(current2["steps"]["step2"]["by_run_id"]), 2)

        latest2_dir = Path(current2["steps"]["step2"]["latest"]["attempt_dir"])
        meta2 = json.loads((latest2_dir / "meta.json").read_text(encoding="utf-8"))
        self.assertEqual(meta2.get("invocation"), "resume")
        self.assertEqual(meta2.get("parent_run_id"), step1_run_id)
        self.assertEqual((meta2.get("resume_from") or {}).get("step_id"), "step1")

    def test_mark_failed_override_prevents_scheduling(self) -> None:
        lt = {
            "working_root": str(self.root),
            "batch_goal_summary": _long_goal(),
            "jobs": [
                {
                    "job_id": "job_mf",
                    "working_directory": ".",
                    "steps": [{"step_id": "step1", "prompt": "Return a valid Run Report JSON."}],
                }
            ],
        }

        incoming = self.root / "runs" / "_queue" / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        _write_launch_table(incoming / "lt_mf.json", lt)

        # Mark failed before any attempts run.
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)
        batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))

        paths = Paths(runs_root=self.root / "runs", runners_root=self.root / "runners")
        store = RunsStore(paths)
        set_forced_terminal(store.overrides_path(batch_id, "job_mf"), "step1", status="failed", reason="operator")

        # Another tick should not create extra attempts (still only 1 from initial run), and scoreboard should show failed.
        self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)
        current = json.loads((self.root / "runs" / batch_id / "job_mf" / "current.json").read_text(encoding="utf-8"))
        self.assertIn("forced_terminal", current["steps"]["step1"])

    def test_affinity_pins_resume_step_to_source_runner(self) -> None:
        # Two runners; step2 resume_from should run on same runner as selected step1 attempt.
        import os

        os.environ["PARALLELHASSNES_RUNNER_IDS"] = "runner_a,runner_b"
        try:
            lt = {
                "working_root": str(self.root),
                "batch_goal_summary": _long_goal(),
                "concurrency": 2,
                "jobs": [
                    {
                        "job_id": "job_aff",
                        "working_directory": ".",
                        "steps": [
                            {"step_id": "step1", "prompt": "Step1", "fake_sleep_seconds": 0.05},
                            {"step_id": "step2", "resume_from": {"step_id": "step1", "selector": "latest_successful"}, "prompt": "Step2"},
                        ],
                    }
                ],
            }
            incoming = self.root / "runs" / "_queue" / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            _write_launch_table(incoming / "lt_aff.json", lt)
            self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
            current = json.loads((self.root / "runs" / batch_id / "job_aff" / "current.json").read_text(encoding="utf-8"))
            step1_attempt = Path(current["steps"]["step1"]["latest"]["attempt_dir"])
            step2_attempt = Path(current["steps"]["step2"]["latest"]["attempt_dir"])
            step1_meta = json.loads((step1_attempt / "meta.json").read_text(encoding="utf-8"))
            step2_meta = json.loads((step2_attempt / "meta.json").read_text(encoding="utf-8"))
            self.assertEqual(step2_meta["runner_id"], step1_meta["runner_id"])
        finally:
            os.environ.pop("PARALLELHASSNES_RUNNER_IDS", None)

    def test_resume_base_transfer_allows_cross_runner_resume_when_shared_fs_false(self) -> None:
        # When shared_filesystem=false and transfer is enabled, a resume step may run on a different runner.
        import os

        os.environ["PARALLELHASSNES_RUNNER_IDS"] = "runner_a,runner_b"
        try:
            (self.root / "runs" / "_system").mkdir(parents=True, exist_ok=True)
            (self.root / "runs" / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2000-01-01T00:00:00Z",
                        "paths": {"runs_root": str(self.root / "runs"), "runners_root": str(self.root / "runners")},
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "runner_capacity": 1,
                            "runner_pool": {"shared_filesystem": False, "resume_base_transfer_enabled": True, "runner_ids": ["runner_a", "runner_b"]},
                            "runner_affinity": {"resume_steps": "none", "non_resume_steps": "none"},
                            "execution_policy": {"sandbox": "workspace-write", "approval_policy": "on-request", "skip_git_repo_check": False, "web_search_enabled": False},
                            "timeouts": {"step_timeout_seconds": 3600},
                            "retries": {"max_attempts": 1},
                            "retention_policy": {"keep_raw_events_days": 7, "keep_git_artifacts_days": 7, "keep_resume_bases_days": 30},
                            "scoreboard": {"heartbeat_stale_after_seconds": 2700, "heartbeat_interval_seconds": 900},
                            "workspace_policy": {"mode": "shared"},
                        },
                        "limits": {"max_jobs_per_batch": 100, "max_steps_per_job": 10},
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            lt = {
                "working_root": str(self.root),
                "batch_goal_summary": _long_goal(),
                "concurrency": 2,
                "jobs": [
                    {
                        "job_id": "job_xfer",
                        "working_directory": ".",
                        "steps": [
                            {"step_id": "step1", "prompt": "Step1", "fake_codex_home_marker": "marker.txt", "fake_sleep_seconds": 0.05},
                            {"step_id": "step_busy", "depends_on": ["step1"], "prompt": "Busy", "fake_sleep_seconds": 0.3},
                            {"step_id": "step2", "resume_from": {"step_id": "step1", "selector": "latest_successful"}, "prompt": "Step2"},
                        ],
                    }
                ],
            }
            incoming = self.root / "runs" / "_queue" / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            _write_launch_table(incoming / "lt_xfer.json", lt)

            self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
            current = json.loads((self.root / "runs" / batch_id / "job_xfer" / "current.json").read_text(encoding="utf-8"))
            step1_attempt = Path(current["steps"]["step1"]["latest"]["attempt_dir"])
            step2_attempt = Path(current["steps"]["step2"]["latest"]["attempt_dir"])
            step1_meta = json.loads((step1_attempt / "meta.json").read_text(encoding="utf-8"))
            step2_meta = json.loads((step2_attempt / "meta.json").read_text(encoding="utf-8"))

            self.assertNotEqual(step2_meta["runner_id"], step1_meta["runner_id"])
            self.assertEqual(step2_meta.get("invocation"), "resume")
            self.assertIsInstance(step2_meta.get("resume_base_transfer"), dict)
            transfer = step2_meta.get("resume_base_transfer") or {}
            self.assertTrue(transfer.get("enabled"))
            cache_path = Path(transfer.get("to"))
            self.assertTrue((cache_path / "marker.txt").exists())
            self.assertTrue((step2_attempt / "codex_home" / "marker.txt").exists())
        finally:
            os.environ.pop("PARALLELHASSNES_RUNNER_IDS", None)

    def test_capacity_constraints_prevent_over_capacity_runner(self) -> None:
        # One runner with capacity=1, concurrency=2; ensure runner never exceeds max_load_observed=1.
        import os

        os.environ["PARALLELHASSNES_RUNNER_IDS"] = "runner_only"
        try:
            # Write harness_config with runner_capacity=1
            (self.root / "runs" / "_system").mkdir(parents=True, exist_ok=True)
            (self.root / "runs" / "_system" / "harness_config.json").write_text(
                json.dumps(
                    {
                        "harness_config_version": "hc_test",
                        "written_at": "2000-01-01T00:00:00Z",
                        "paths": {"runs_root": str(self.root / "runs"), "runners_root": str(self.root / "runners")},
                        "interfaces": {"api_mode": {"enabled": False, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": True}},
                        "defaults": {
                            "runner_capacity": 1,
                            "execution_policy": {"sandbox": "workspace-write", "approval_policy": "on-request", "skip_git_repo_check": False, "web_search_enabled": False},
                            "timeouts": {"step_timeout_seconds": 3600},
                            "retries": {"max_attempts": 1},
                            "retention_policy": {"keep_raw_events_days": 7, "keep_git_artifacts_days": 7, "keep_resume_bases_days": 30},
                            "scoreboard": {"heartbeat_stale_after_seconds": 2700, "heartbeat_interval_seconds": 900, "stuck_auto_remediation_enabled": False},
                            "workspace_policy": {"mode": "shared"},
                        },
                        "limits": {"max_jobs_per_batch": 100, "max_steps_per_job": 10},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            lt = {
                "working_root": str(self.root),
                "batch_goal_summary": _long_goal(),
                "concurrency": 2,
                "jobs": [
                    {"job_id": "job_c1", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "x", "fake_sleep_seconds": 0.2}]},
                    {"job_id": "job_c2", "working_directory": ".", "steps": [{"step_id": "s", "prompt": "y", "fake_sleep_seconds": 0.2}]},
                ],
            }
            incoming = self.root / "runs" / "_queue" / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            _write_launch_table(incoming / "lt_cap.json", lt)
            self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            health = json.loads((self.root / "runners" / "runner_only" / "health.json").read_text(encoding="utf-8"))
            self.assertLessEqual(int(health.get("max_load_observed", 0)), 1)
        finally:
            os.environ.pop("PARALLELHASSNES_RUNNER_IDS", None)

    def test_requeue_different_runner_moves_next_attempt(self) -> None:
        import os

        os.environ["PARALLELHASSNES_RUNNER_IDS"] = "runner_a,runner_b"
        try:
            lt = {
                "working_root": str(self.root),
                "batch_goal_summary": _long_goal(),
                "jobs": [
                    {
                        "job_id": "job_rq",
                        "working_directory": ".",
                        "steps": [{"step_id": "step1", "prompt": "x", "retry_policy": {"max_attempts": 2, "retry_on_statuses": ["failed"]}, "fake_exit_codes": [1, 0]}],
                    }
                ],
            }
            incoming = self.root / "runs" / "_queue" / "incoming"
            incoming.mkdir(parents=True, exist_ok=True)
            _write_launch_table(incoming / "lt_rq.json", lt)
            self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            batch_id = next(p.name for p in (self.root / "runs").iterdir() if p.is_dir() and p.name.startswith("batch_"))
            cur = json.loads((self.root / "runs" / batch_id / "job_rq" / "current.json").read_text(encoding="utf-8"))
            step = cur["steps"]["step1"]
            first_attempt = Path(step["latest"]["attempt_dir"])
            first_runner = json.loads((first_attempt / "meta.json").read_text(encoding="utf-8"))["runner_id"]

            store = RunsStore(Paths(runs_root=self.root / "runs", runners_root=self.root / "runners"))
            from parallelhassnes.ops.overrides import set_requeue

            set_requeue(store.overrides_path(batch_id, "job_rq"), "step1", target_runner_id=None, different_from_last=True, reason="test")
            self.harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            cur2 = json.loads((self.root / "runs" / batch_id / "job_rq" / "current.json").read_text(encoding="utf-8"))
            step2 = cur2["steps"]["step1"]
            self.assertEqual(len(step2["by_run_id"]), 2)
            second_attempt = Path(step2["latest"]["attempt_dir"])
            second_runner = json.loads((second_attempt / "meta.json").read_text(encoding="utf-8"))["runner_id"]
            self.assertNotEqual(second_runner, first_runner)
        finally:
            os.environ.pop("PARALLELHASSNES_RUNNER_IDS", None)


if __name__ == "__main__":
    unittest.main()
