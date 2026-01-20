from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from parallelhassnes.core.paths import Paths
from parallelhassnes.runner.pool import RunnerHandle, RunnerPool
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.scheduler.scheduler import Scheduler, SchedulerConfig
from parallelhassnes.storage.runs_store import RunsStore


def _test_harness_cfg(runner_ids: list[str]) -> dict:
    return {
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
            "runner_pool": {"shared_filesystem": True, "runner_ids": runner_ids},
            "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
        },
        "limits": {"max_jobs_per_batch": 10000, "max_steps_per_job": 100, "per_workdir_concurrency": {}},
    }


class ConcurrencySafetyTests(unittest.TestCase):
    def test_attempt_dir_uniqueness_retries_on_collision(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            cfg = _test_harness_cfg(runner_ids=["r1"])

            # Force a deterministic run_dir_name so we can pre-create the colliding directory.
            import parallelhassnes.core.paths as paths_mod

            orig_ts = paths_mod.utc_compact_timestamp
            paths_mod.utc_compact_timestamp = lambda: "20260114T000000Z"  # type: ignore[assignment]

            # Force first run_id to collide, then succeed.
            import parallelhassnes.runner.runner as runner_mod

            orig_new_run_id = runner_mod.new_run_id
            run_ids = iter(["run_20260114T000000Z_deadbeef", "run_20260114T000000Z_feedface"])
            runner_mod.new_run_id = lambda: next(run_ids)  # type: ignore[assignment]

            try:
                batch_id, job_id, step_id = "batch1", "j1", "s1"
                # Pre-create the colliding attempt directory for the first run_id.
                colliding_run_id = "run_20260114T000000Z_deadbeef"
                colliding_dir = store.paths.attempt_dir(batch_id, job_id, step_id, store.paths.run_dir_name(colliding_run_id))
                colliding_dir.mkdir(parents=True, exist_ok=False)

                runner = Runner(
                    store=store,
                    cfg=RunnerConfig(
                        runner_id="r1",
                        use_fake_invoker=True,
                        baseline_output_schema_path=str((root / "run_report.schema.json").resolve()),
                    ),
                    harness_cfg=cfg,
                )

                runner.execute_step(
                    batch={"batch_id": batch_id, "effective_defaults": {}, "jobs": []},
                    job={"job_id": job_id, "working_directory": ".", "steps": []},
                    step={"step_id": step_id, "prompt": "x"},
                    job_workdir=str(root),
                    workspace_policy={"mode": "shared"},
                    workspace_root=str(root),
                    current={"batch_id": batch_id, "job_id": job_id, "updated_at": "2026-01-14T00:00:00Z", "steps": {}},
                )

                cur = store.read_current(batch_id, job_id)
                self.assertIsNotNone(cur)
                by = (((cur or {}).get("steps") or {}).get(step_id) or {}).get("by_run_id") or {}
                self.assertEqual(sorted(by.keys()), ["run_20260114T000000Z_feedface"])
            finally:
                runner_mod.new_run_id = orig_new_run_id  # type: ignore[assignment]
                paths_mod.utc_compact_timestamp = orig_ts  # type: ignore[assignment]

    def test_step_lock_prevents_double_execution_across_schedulers(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            store1 = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            store2 = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            cfg = _test_harness_cfg(runner_ids=["runner_a", "runner_b"])

            batch_id = "batch_lock"
            batch_dir = runs_root / batch_id
            batch_dir.mkdir(parents=True, exist_ok=True)
            (batch_dir / "batch_meta.json").write_text(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "submitted_at": "2026-01-14T00:00:00Z",
                        "harness_config_version": cfg["harness_config_version"],
                        "batch_goal_summary": ("word " * 200).strip(),
                        "working_root": str(root),
                        "concurrency": 1,
                        "effective_defaults": {},
                        "jobs": [
                            {
                                "job_id": "j1",
                                "working_directory": ".",
                                "steps": [{"step_id": "s", "prompt": "x", "fake_sleep_seconds": 0.3}],
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )

            runner_a = Runner(
                store=store1,
                cfg=RunnerConfig(
                    runner_id="runner_a",
                    use_fake_invoker=True,
                    baseline_output_schema_path=str((root / "run_report.schema.json").resolve()),
                ),
                harness_cfg=cfg,
            )
            runner_b = Runner(
                store=store2,
                cfg=RunnerConfig(
                    runner_id="runner_b",
                    use_fake_invoker=True,
                    baseline_output_schema_path=str((root / "run_report.schema.json").resolve()),
                ),
                harness_cfg=cfg,
            )
            pool1 = RunnerPool([RunnerHandle(runner_id="runner_a", runner=runner_a, capacity=1, semaphore=threading.Semaphore(1))])
            pool2 = RunnerPool([RunnerHandle(runner_id="runner_b", runner=runner_b, capacity=1, semaphore=threading.Semaphore(1))])

            sched1 = Scheduler(store=store1, cfg=SchedulerConfig(concurrency_override=1), harness_cfg=cfg)
            sched2 = Scheduler(store=store2, cfg=SchedulerConfig(concurrency_override=1), harness_cfg=cfg)

            t1 = threading.Thread(target=lambda: sched1.run_until_idle([batch_id], runner_pool=pool1))
            t2 = threading.Thread(target=lambda: sched2.run_until_idle([batch_id], runner_pool=pool2))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            attempts_dir = runs_root / batch_id / "j1" / "steps" / "s" / "attempts"
            attempts = [p for p in attempts_dir.iterdir() if p.is_dir()] if attempts_dir.exists() else []
            self.assertEqual(len(attempts), 1)

    def test_current_json_atomic_merge_under_concurrent_updates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))

            batch_id, job_id = "b", "j"

            def update_a(cur: dict) -> None:
                steps = cur.setdefault("steps", {})
                steps.setdefault("a", {})["marker"] = "A"

            def update_b(cur: dict) -> None:
                steps = cur.setdefault("steps", {})
                steps.setdefault("b", {})["marker"] = "B"

            ta = threading.Thread(target=lambda: store.update_current_atomic(batch_id, job_id, update_a))
            tb = threading.Thread(target=lambda: store.update_current_atomic(batch_id, job_id, update_b))
            ta.start()
            tb.start()
            ta.join()
            tb.join()

            cur = store.read_current(batch_id, job_id)
            self.assertIsNotNone(cur)
            steps = (cur or {}).get("steps") or {}
            self.assertEqual(steps.get("a", {}).get("marker"), "A")
            self.assertEqual(steps.get("b", {}).get("marker"), "B")


if __name__ == "__main__":
    unittest.main()
