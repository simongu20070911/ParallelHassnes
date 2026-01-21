from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.time import parse_iso8601_z, utc_isoformat
from parallelhassnes.core.file_lock import try_acquire_lock, FileLock
from parallelhassnes.workspace.policy import WorkspaceResolution, resolve_job_workdir


@dataclass(frozen=True)
class SchedulerConfig:
    concurrency_override: int | None
    multi_batch: bool = False


class Scheduler:
    def __init__(self, store: Any, cfg: SchedulerConfig, harness_cfg: dict[str, Any]) -> None:
        self._store = store
        self._cfg = cfg
        self._hcfg = harness_cfg

    def run_until_idle(self, ingested_batches: list[str], runner_pool: Any, *, batch_id_filter: str | None = None) -> None:
        # Scan all known batches (including newly ingested) and run runnable steps until none remain.
        batches = set(self._store.list_batches())
        batches.update(ingested_batches)
        if isinstance(batch_id_filter, str) and batch_id_filter.strip():
            batches = {batch_id_filter.strip()} if batch_id_filter.strip() in batches else set()
        batch_list = sorted(batches)
        if self._cfg.multi_batch:
            self._run_multi_batch_until_idle(batch_list, runner_pool)
        else:
            for batch_id in batch_list:
                self._run_batch_until_idle(batch_id, runner_pool)

    def _run_multi_batch_until_idle(self, batch_ids: list[str], runner_pool: Any) -> None:
        """
        Multi-batch scheduler: interleave runnable steps across batches so that
        independent batches can make progress concurrently under shared runner capacity.
        """
        from collections import deque

        per_workdir_limits = (self._hcfg.get("limits", {}) or {}).get("per_workdir_concurrency") or {}
        if not isinstance(per_workdir_limits, dict):
            per_workdir_limits = {}

        workdir_inflight: dict[str, int] = {}

        @dataclass
        class _BatchState:
            batch_id: str
            batch: dict[str, Any]
            concurrency: int
            shared_fs: bool
            transfer_enabled: bool
            runner_affinity_cfg: dict[str, Any]
            job_defs: list[tuple[str, dict[str, Any], WorkspaceResolution]]
            job_by_id: dict[str, tuple[dict[str, Any], WorkspaceResolution]]
            step_by_key: dict[tuple[str, str], dict[str, Any]]
            runnable_q: deque[tuple[str, str]]
            queued: set[tuple[str, str]]
            started_steps: set[tuple[str, str]]
            job_cursor: int
            state_path: Path
            in_flight: set[Future[None]]
            future_to_step_lock: dict[Future[None], FileLock]
            future_to_workdir: dict[Future[None], str]

            def write_state(self) -> None:
                write_atomic_json(
                    self.state_path,
                    {
                        "batch_id": self.batch_id,
                        "updated_at": utc_isoformat(),
                        "queue": [{"job_id": jid, "step_id": sid} for (jid, sid) in list(self.runnable_q)],
                        "in_flight": len(self.in_flight),
                        "note": "Best-effort scheduler snapshot; source of truth remains attempt dirs + current.json.",
                    },
                )

            def enqueue(self, job_id: str, step_id: str) -> None:
                k = (job_id, step_id)
                if k in self.queued or k in self.started_steps:
                    return
                self.runnable_q.append(k)
                self.queued.add(k)

            def restore_queue_snapshot(self) -> None:
                if not self.state_path.exists():
                    return
                try:
                    import json

                    raw = json.loads(self.state_path.read_text(encoding="utf-8"))
                    q = raw.get("queue") if isinstance(raw, dict) else None
                    if isinstance(q, list):
                        for ent in q:
                            if not isinstance(ent, dict):
                                continue
                            jid = ent.get("job_id")
                            sid = ent.get("step_id")
                            if isinstance(jid, str) and isinstance(sid, str) and (jid, sid) in self.step_by_key:
                                self.enqueue(jid, sid)
                except Exception:
                    return

            def refresh_queue(self, store: Any) -> None:
                if not self.job_defs:
                    return
                n = len(self.job_defs)
                for off in range(n):
                    jidx = (self.job_cursor + off) % n
                    job_id, job, _workspace = self.job_defs[jidx]
                    current = store.read_current(self.batch_id, job_id) or _empty_current(self.batch_id, job_id)
                    overrides = store.read_overrides(self.batch_id, job_id)
                    for step in job.get("steps", []):
                        step_id = step.get("step_id")
                        if not isinstance(step_id, str) or not step_id:
                            continue
                        if (job_id, step_id) in self.started_steps:
                            continue
                        if _forced_terminal(current, overrides, step_id):
                            _apply_forced_terminal_marker(store, current, self.batch_id, job_id, step_id, overrides)
                            continue
                        if _step_succeeded(current, step_id) and not _force_retry_requested(current, overrides, step_id):
                            continue
                        if _step_running(store, current, step_id):
                            continue
                        if not _retry_allowed(self.batch, current, step, overrides):
                            continue
                        if not _deps_satisfied(store, self.batch_id, job_id, current, step, job.get("steps", [])):
                            continue
                        self.enqueue(job_id, step_id)
                self.job_cursor = (self.job_cursor + 1) % n

        batch_states: list[_BatchState] = []
        for batch_id in batch_ids:
            # Operator control: a closed batch is finalized and should not accept new scheduling.
            if (self._store.paths.batch_dir(batch_id) / "batch_closed.json").exists():
                continue
            batch = self._store.read_batch_meta(batch_id)
            concurrency = int(self._cfg.concurrency_override or batch.get("concurrency") or 1)
            if concurrency <= 0:
                concurrency = 1

            defaults = batch.get("effective_defaults", {}) or {}
            runner_pool_cfg = defaults.get("runner_pool") or (self._hcfg.get("defaults", {}) or {}).get("runner_pool") or {}
            shared_fs = bool(runner_pool_cfg.get("shared_filesystem", True))
            transfer_enabled = bool(runner_pool_cfg.get("resume_base_transfer_enabled", False))
            runner_affinity_cfg = defaults.get("runner_affinity") or (self._hcfg.get("defaults", {}) or {}).get("runner_affinity") or {}

            job_defs: list[tuple[str, dict[str, Any], WorkspaceResolution]] = []
            job_by_id: dict[str, tuple[dict[str, Any], WorkspaceResolution]] = {}
            step_by_key: dict[tuple[str, str], dict[str, Any]] = {}
            for job in batch["jobs"]:
                job_id = job["job_id"]
                workspace = resolve_job_workdir(paths=self._store.paths, harness_cfg=self._hcfg, batch=batch, job=job)
                job_defs.append((job_id, job, workspace))
                job_by_id[job_id] = (job, workspace)
                for step in job.get("steps", []):
                    step_id = step.get("step_id")
                    if isinstance(step_id, str) and step_id:
                        step_by_key[(job_id, step_id)] = step

            bs = _BatchState(
                batch_id=batch_id,
                batch=batch,
                concurrency=concurrency,
                shared_fs=shared_fs,
                transfer_enabled=transfer_enabled,
                runner_affinity_cfg=runner_affinity_cfg if isinstance(runner_affinity_cfg, dict) else {},
                job_defs=job_defs,
                job_by_id=job_by_id,
                step_by_key=step_by_key,
                runnable_q=deque(),
                queued=set(),
                started_steps=set(),
                job_cursor=0,
                state_path=self._store.paths.batch_scheduler_state_path(batch_id),
                in_flight=set(),
                future_to_step_lock={},
                future_to_workdir={},
            )
            bs.restore_queue_snapshot()
            bs.refresh_queue(self._store)
            bs.write_state()
            batch_states.append(bs)

        # Run steps across all batches using a single worker pool to avoid thread explosion.
        try:
            total_cap = max(1, int(runner_pool.total_capacity()))
        except Exception:
            try:
                total_cap = max(1, len(runner_pool.runner_ids()))
            except Exception:
                total_cap = 1

        all_in_flight: set[Future[None]] = set()
        future_to_batch: dict[Future[None], _BatchState] = {}

        def _dispatch_from_batch(ex: ThreadPoolExecutor, bs: _BatchState) -> bool:
            if not bs.runnable_q:
                bs.refresh_queue(self._store)
                if not bs.runnable_q:
                    return False
            made_progress = False
            attempts_without_schedule = 0
            max_attempts = len(bs.runnable_q) if bs.runnable_q else 0
            while bs.runnable_q and len(bs.in_flight) < bs.concurrency and (max_attempts == 0 or attempts_without_schedule < max_attempts):
                job_id, step_id = bs.runnable_q.popleft()
                bs.queued.discard((job_id, step_id))
                attempts_without_schedule += 1

                if (job_id, step_id) in bs.started_steps:
                    continue

                jw = bs.job_by_id.get(job_id)
                if jw is None:
                    continue
                job, workspace = jw
                step = bs.step_by_key.get((job_id, step_id))
                if not isinstance(step, dict):
                    continue

                current = self._store.read_current(bs.batch_id, job_id) or _empty_current(bs.batch_id, job_id)
                overrides = self._store.read_overrides(bs.batch_id, job_id)
                job_workdir = workspace.job_workdir

                if _forced_terminal(current, overrides, step_id):
                    _apply_forced_terminal_marker(self._store, current, bs.batch_id, job_id, step_id, overrides)
                    continue
                if _step_succeeded(current, step_id) and not _force_retry_requested(current, overrides, step_id):
                    continue
                if _step_running(self._store, current, step_id):
                    continue
                if not _retry_allowed(bs.batch, current, step, overrides):
                    continue
                if not _deps_satisfied(self._store, bs.batch_id, job_id, current, step, job.get("steps", [])):
                    continue

                step_lock_path = self._store.paths.step_dir(bs.batch_id, job_id, step_id) / "step.lock"
                step_lock = try_acquire_lock(step_lock_path)
                if step_lock is None:
                    bs.enqueue(job_id, step_id)
                    continue

                if per_workdir_limits:
                    lim = per_workdir_limits.get(job_workdir)
                    if isinstance(lim, int) and lim > 0:
                        if workdir_inflight.get(job_workdir, 0) >= lim:
                            step_lock.release()
                            bs.enqueue(job_id, step_id)
                            continue

                desired, avoid, requeue_nonce = _resolve_runner_choice(
                    self._store,
                    bs.batch_id,
                    job_id,
                    current,
                    step,
                    runner_pool,
                    overrides,
                    bs.runner_affinity_cfg,
                    shared_fs=bs.shared_fs,
                    transfer_enabled=bs.transfer_enabled,
                )
                acquired = runner_pool.try_acquire_any(desired, avoid=avoid)
                if acquired is None:
                    step_lock.release()
                    bs.enqueue(job_id, step_id)
                    continue
                runner_id = acquired

                if _force_retry_requested(current, overrides, step_id):
                    _mark_force_retry_applied(self._store, current, bs.batch_id, job_id, step_id, overrides)

                if requeue_nonce is not None:
                    _mark_requeue_applied(self._store, current, bs.batch_id, job_id, step_id, overrides, requeue_nonce)

                fut = ex.submit(_run_step_on_pool, runner_pool, runner_id, bs.batch, job, step, workspace, current)
                fut.add_done_callback(lambda f, rid=runner_id: runner_pool.release(rid))
                bs.in_flight.add(fut)
                bs.future_to_step_lock[fut] = step_lock
                if per_workdir_limits and job_workdir in per_workdir_limits:
                    bs.future_to_workdir[fut] = job_workdir
                    workdir_inflight[job_workdir] = workdir_inflight.get(job_workdir, 0) + 1
                bs.started_steps.add((job_id, step_id))

                all_in_flight.add(fut)
                future_to_batch[fut] = bs
                made_progress = True
                bs.write_state()

            return made_progress

        with ThreadPoolExecutor(max_workers=total_cap) as ex:
            while True:
                made_progress = False

                for bs in batch_states:
                    made_progress = _dispatch_from_batch(ex, bs) or made_progress

                done, _ = wait(all_in_flight, timeout=0.05)
                if done:
                    made_progress = True
                for f in done:
                    all_in_flight.discard(f)
                    bs = future_to_batch.pop(f, None)
                    if bs is None:
                        continue
                    bs.in_flight.discard(f)
                    wd = bs.future_to_workdir.pop(f, None)
                    if wd is not None:
                        cur = workdir_inflight.get(wd, 0) - 1
                        if cur <= 0:
                            workdir_inflight.pop(wd, None)
                        else:
                            workdir_inflight[wd] = cur
                    lk = bs.future_to_step_lock.pop(f, None)
                    if lk is not None:
                        lk.release()
                    bs.refresh_queue(self._store)
                    bs.write_state()

                if all_in_flight:
                    continue

                # No futures running: only continue if new runnable work appears.
                if not made_progress:
                    any_runnable = False
                    for bs in batch_states:
                        bs.refresh_queue(self._store)
                        bs.write_state()
                        if bs.runnable_q:
                            any_runnable = True
                    if not any_runnable:
                        break

    def _run_batch_until_idle(self, batch_id: str, runner_pool: Any) -> None:
        # Operator control: a closed batch is finalized and should not accept new scheduling.
        # (Retention GC treats closed batches specially; scheduler should too.)
        if (self._store.paths.batch_dir(batch_id) / "batch_closed.json").exists():
            return
        batch = self._store.read_batch_meta(batch_id)
        concurrency = int(self._cfg.concurrency_override or batch.get("concurrency") or 1)
        if concurrency <= 0:
            concurrency = 1

        defaults = batch.get("effective_defaults", {}) or {}
        runner_pool_cfg = defaults.get("runner_pool") or (self._hcfg.get("defaults", {}) or {}).get("runner_pool") or {}
        shared_fs = bool(runner_pool_cfg.get("shared_filesystem", True))
        transfer_enabled = bool(runner_pool_cfg.get("resume_base_transfer_enabled", False))
        runner_affinity_cfg = defaults.get("runner_affinity") or (self._hcfg.get("defaults", {}) or {}).get("runner_affinity") or {}
        per_workdir_limits = (self._hcfg.get("limits", {}) or {}).get("per_workdir_concurrency") or {}
        if not isinstance(per_workdir_limits, dict):
            per_workdir_limits = {}

        max_workers = max(1, concurrency)
        in_flight: set[Future[None]] = set()
        future_to_workdir: dict[Future[None], str] = {}
        workdir_inflight: dict[str, int] = {}
        future_to_step_lock: dict[Future[None], FileLock] = {}
        # Within a single scheduler run (one harness tick), run at most one attempt per (job_id, step_id).
        # This makes retries/requeues orchestrator-controllable without racing the scheduler loop.
        started_steps: set[tuple[str, str]] = set()

        # Maintain a queue of runnable steps (fair round-robin scan) and dispatch from it.
        from collections import deque

        job_defs: list[tuple[str, dict[str, Any], WorkspaceResolution]] = []
        step_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for job in batch["jobs"]:
            job_id = job["job_id"]
            workspace = resolve_job_workdir(paths=self._store.paths, harness_cfg=self._hcfg, batch=batch, job=job)
            job_defs.append((job_id, job, workspace))
            for step in job.get("steps", []):
                step_id = step.get("step_id")
                if isinstance(step_id, str) and step_id:
                    step_by_key[(job_id, step_id)] = step

        runnable_q: deque[tuple[str, str]] = deque()
        queued: set[tuple[str, str]] = set()
        job_cursor = 0
        state_path = self._store.paths.batch_scheduler_state_path(batch_id)

        def write_state(in_flight_count: int) -> None:
            write_atomic_json(
                state_path,
                {
                    "batch_id": batch_id,
                    "updated_at": utc_isoformat(),
                    "queue": [{"job_id": jid, "step_id": sid} for (jid, sid) in list(runnable_q)],
                    "in_flight": in_flight_count,
                    "note": "Best-effort scheduler snapshot; source of truth remains attempt dirs + current.json.",
                },
            )

        def enqueue(job_id: str, step_id: str) -> None:
            k = (job_id, step_id)
            if k in queued or k in started_steps:
                return
            runnable_q.append(k)
            queued.add(k)

        def refresh_queue() -> None:
            nonlocal job_cursor
            if not job_defs:
                return
            n = len(job_defs)
            for off in range(n):
                jidx = (job_cursor + off) % n
                job_id, job, _workspace = job_defs[jidx]
                current = self._store.read_current(batch_id, job_id) or _empty_current(batch_id, job_id)
                overrides = self._store.read_overrides(batch_id, job_id)
                for step in job.get("steps", []):
                    step_id = step.get("step_id")
                    if not isinstance(step_id, str) or not step_id:
                        continue
                    if (job_id, step_id) in started_steps:
                        continue
                    if _forced_terminal(current, overrides, step_id):
                        _apply_forced_terminal_marker(self._store, current, batch_id, job_id, step_id, overrides)
                        continue
                    if _step_succeeded(current, step_id) and not _force_retry_requested(current, overrides, step_id):
                        continue
                    if _step_running(self._store, current, step_id):
                        continue
                    if not _retry_allowed(batch, current, step, overrides):
                        continue
                    if not _deps_satisfied(self._store, batch_id, job_id, current, step, job.get("steps", [])):
                        continue
                    enqueue(job_id, step_id)
            job_cursor = (job_cursor + 1) % n

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            # Restore any persisted queue snapshot (best-effort), then refresh from live state.
            if state_path.exists():
                try:
                    import json

                    raw = json.loads(state_path.read_text(encoding="utf-8"))
                    q = raw.get("queue") if isinstance(raw, dict) else None
                    if isinstance(q, list):
                        for ent in q:
                            if not isinstance(ent, dict):
                                continue
                            jid = ent.get("job_id")
                            sid = ent.get("step_id")
                            if isinstance(jid, str) and isinstance(sid, str) and (jid, sid) in step_by_key:
                                enqueue(jid, sid)
                except Exception:
                    pass

            refresh_queue()
            write_state(in_flight_count=len(in_flight))
            while runnable_q or in_flight:
                made_progress = False

                # Dispatch runnable steps up to the concurrency limit.
                attempts_without_schedule = 0
                max_attempts = len(runnable_q) if runnable_q else 0
                while runnable_q and len(in_flight) < concurrency and (max_attempts == 0 or attempts_without_schedule < max_attempts):
                    job_id, step_id = runnable_q.popleft()
                    queued.discard((job_id, step_id))
                    attempts_without_schedule += 1

                    if (job_id, step_id) in started_steps:
                        continue

                    job = None
                    workspace = None
                    for jid, j, wd in job_defs:
                        if jid == job_id:
                            job = j
                            workspace = wd
                            break
                    if job is None or workspace is None:
                        continue
                    step = step_by_key.get((job_id, step_id))
                    if not isinstance(step, dict):
                        continue

                    current = self._store.read_current(batch_id, job_id) or _empty_current(batch_id, job_id)
                    overrides = self._store.read_overrides(batch_id, job_id)
                    job_workdir = workspace.job_workdir

                    if _forced_terminal(current, overrides, step_id):
                        _apply_forced_terminal_marker(self._store, current, batch_id, job_id, step_id, overrides)
                        continue
                    if _step_succeeded(current, step_id) and not _force_retry_requested(current, overrides, step_id):
                        continue
                    if _step_running(self._store, current, step_id):
                        continue
                    if not _retry_allowed(batch, current, step, overrides):
                        continue
                    if not _deps_satisfied(self._store, batch_id, job_id, current, step, job.get("steps", [])):
                        continue

                    # Prevent two runners (or two concurrent harness instances) from executing the same step concurrently.
                    step_lock_path = self._store.paths.step_dir(batch_id, job_id, step_id) / "step.lock"
                    step_lock = try_acquire_lock(step_lock_path)
                    if step_lock is None:
                        enqueue(job_id, step_id)
                        continue

                    # Optional per-workdir concurrency limit.
                    if per_workdir_limits:
                        lim = per_workdir_limits.get(job_workdir)
                        if isinstance(lim, int) and lim > 0:
                            if workdir_inflight.get(job_workdir, 0) >= lim:
                                step_lock.release()
                                enqueue(job_id, step_id)
                                continue

                    # Choose runner based on requeue/affinity/capacity.
                    desired, avoid, requeue_nonce = _resolve_runner_choice(
                        self._store,
                        batch_id,
                        job_id,
                        current,
                        step,
                        runner_pool,
                        overrides,
                        runner_affinity_cfg,
                        shared_fs=shared_fs,
                        transfer_enabled=transfer_enabled,
                    )
                    acquired = runner_pool.try_acquire_any(desired, avoid=avoid)
                    if acquired is None:
                        step_lock.release()
                        enqueue(job_id, step_id)
                        continue
                    runner_id = acquired

                    if _force_retry_requested(current, overrides, step_id):
                        _mark_force_retry_applied(self._store, current, batch_id, job_id, step_id, overrides)

                    if requeue_nonce is not None:
                        _mark_requeue_applied(self._store, current, batch_id, job_id, step_id, overrides, requeue_nonce)

                    fut = ex.submit(_run_step_on_pool, runner_pool, runner_id, batch, job, step, workspace, current)
                    fut.add_done_callback(lambda f, rid=runner_id: runner_pool.release(rid))
                    in_flight.add(fut)
                    future_to_step_lock[fut] = step_lock
                    if per_workdir_limits and job_workdir in per_workdir_limits:
                        future_to_workdir[fut] = job_workdir
                        workdir_inflight[job_workdir] = workdir_inflight.get(job_workdir, 0) + 1
                    made_progress = True
                    started_steps.add((job_id, step_id))
                    write_state(in_flight_count=len(in_flight))

                # Reap any completed futures.
                done, _ = wait(in_flight, timeout=0.05)
                in_flight -= set(done)
                for f in done:
                    wd = future_to_workdir.pop(f, None)
                    if wd is not None:
                        cur = workdir_inflight.get(wd, 0) - 1
                        if cur <= 0:
                            workdir_inflight.pop(wd, None)
                        else:
                            workdir_inflight[wd] = cur
                    lk = future_to_step_lock.pop(f, None)
                    if lk is not None:
                        lk.release()
                if done:
                    refresh_queue()
                    made_progress = True
                    write_state(in_flight_count=len(in_flight))

                if not made_progress and not in_flight:
                    # Nothing dispatched and nothing running: try one more refresh, then stop.
                    refresh_queue()
                    if not runnable_q:
                        write_state(in_flight_count=0)
                        break
                    write_state(in_flight_count=0)

    # Scheduler persistence: best-effort `scheduler_state.json` is written under runs/<batch_id>/.


def _empty_current(batch_id: str, job_id: str) -> dict[str, Any]:
    return {"batch_id": batch_id, "job_id": job_id, "updated_at": utc_isoformat(), "steps": {}}


def _step_succeeded(current: dict[str, Any], step_id: str) -> bool:
    s = current.get("steps", {}).get(step_id, {})
    if "latest_successful" in s and s["latest_successful"] is not None:
        return True
    # Best-effort fallback: treat any succeeded entry in by_run_id as success,
    # even if the latest_successful pointer is missing (e.g., crash during pointer update).
    by = s.get("by_run_id")
    if isinstance(by, dict):
        for _, ent in by.items():
            if isinstance(ent, dict) and ent.get("status") == "succeeded":
                return True
    return False


def _step_running(store: Any, current: dict[str, Any], step_id: str) -> bool:
    s = current.get("steps", {}).get(step_id, {})
    latest = s.get("latest")
    if not latest:
        return False
    attempt_dir = Path(latest["attempt_dir"])
    st = store.read_attempt_state(attempt_dir)
    if st is None:
        return True
    return bool(st.get("status") == "running")


def _deps_satisfied(store: Any, batch_id: str, job_id: str, current: dict[str, Any], step: dict[str, Any], all_steps: list[dict[str, Any]]) -> bool:
    step_id = step["step_id"]
    deps = step.get("depends_on") or []

    for d in deps:
        if not _step_succeeded(current, d):
            return False

    # `resume_from` implies ordering dependency on the source step, but the
    # satisfaction criteria depends on the selector:
    # - latest_successful => require a successful source attempt
    # - latest/run_id => require a terminal source attempt
    #
    # This is enforced by the resume selector itself.
    rf = step.get("resume_from")
    if rf:
        from parallelhassnes.resume.selectors import resolve_resume_source

        try:
            resolved = resolve_resume_source(store.paths, batch_id=batch_id, job_id=job_id, current=current, resume_from=rf)
        except Exception:
            return False
        if not Path(resolved.resume_base_dir).exists():
            return False

    return True


def _run_step_on_pool(
    runner_pool: Any,
    runner_id: str,
    batch: dict[str, Any],
    job: dict[str, Any],
    step: dict[str, Any],
    workspace: WorkspaceResolution,
    current: dict[str, Any],
) -> None:
    runner_pool.run_on(
        runner_id,
        lambda r: r.execute_step(
            batch=batch,
            job=job,
            step=step,
            job_workdir=workspace.job_workdir,
            workspace_policy=workspace.workspace_policy,
            workspace_root=workspace.workspace_root,
            current=current,
        ),
    )


def _resolve_runner_choice(
    store: Any,
    batch_id: str,
    job_id: str,
    current: dict[str, Any],
    step: dict[str, Any],
    runner_pool: Any,
    overrides: dict[str, Any],
    runner_affinity_cfg: dict[str, Any],
    shared_fs: bool,
    transfer_enabled: bool,
) -> tuple[str | None, set[str], int | None]:
    """
    Choose runner id with priority:
    1) requeue override (target runner or "different" semantics)
    2) runner_affinity policy (step-level or defaults)
    3) default: pool order
    Returns (preferred_runner_id, avoid_set, requeue_nonce_to_apply).
    """
    preferred: str | None = None
    avoid: set[str] = set()
    requeue_nonce: int | None = None

    step_id = step["step_id"]
    osteps = overrides.get("steps") if isinstance(overrides, dict) else None
    if isinstance(osteps, dict):
        ent = osteps.get(step_id)
        if isinstance(ent, dict):
            rq, nonce = _requeue_requested(current, ent, step_id)
            if rq is not None and nonce is not None:
                tgt = rq.get("target_runner_id")
                if isinstance(tgt, str) and tgt:
                    preferred = tgt
                    requeue_nonce = nonce
                elif rq.get("different_from_last") is True:
                    last_rid = _latest_runner_id_for_step(store, current, step_id)
                    if last_rid:
                        # Deterministically pick a different runner when possible.
                        preferred = _next_runner_id(runner_pool, last_rid)
                        requeue_nonce = nonce

    if preferred is not None:
        return preferred, avoid, requeue_nonce

    # runner_affinity policy.
    affinity = step.get("runner_affinity")
    if not isinstance(affinity, str) or not affinity:
        # Defaults: resume steps vs non-resume steps.
        key = "resume_steps" if step.get("resume_from") else "non_resume_steps"
        dv = runner_affinity_cfg.get(key) if isinstance(runner_affinity_cfg, dict) else None
        affinity = dv if isinstance(dv, str) else None

    # Locality constraint: if resume_from exists and shared filesystem is disabled,
    # the step must run on the same runner as the selected resume source attempt (no transfer in MVP).
    rf = step.get("resume_from")
    if rf:
        src_runner = _resume_source_runner_id(store, batch_id, job_id, current, rf)
        if src_runner and not shared_fs and not transfer_enabled:
            # Enforce strict pin when transfer is unavailable.
            preferred = src_runner
            if not runner_pool.has_runner(src_runner):
                # Avoid all others to prevent silent scheduling elsewhere. Scheduler will see no capacity.
                avoid = set(runner_pool.runner_ids())
            return preferred, avoid, None

        if affinity == "none":
            return None, avoid, None

        # Default: pin resume to source runner when possible.
        if affinity in (None, "pin_resume_source", "prefer_resume_source"):
            if src_runner:
                preferred = src_runner

    return preferred, avoid, None


def _resume_source_runner_id(store: Any, batch_id: str, job_id: str, current: dict[str, Any], rf: dict[str, Any]) -> str | None:
    from parallelhassnes.resume.selectors import resolve_resume_source

    try:
        resolved = resolve_resume_source(store.paths, batch_id=batch_id, job_id=job_id, current=current, resume_from=rf)
    except Exception:
        return None
    meta = store.read_attempt_meta(Path(resolved.attempt_dir))
    rid = meta.get("runner_id") if isinstance(meta, dict) else None
    return rid if isinstance(rid, str) and rid else None


def _latest_runner_id_for_step(store: Any, current: dict[str, Any], step_id: str) -> str | None:
    latest = ((current.get("steps") or {}).get(step_id) or {}).get("latest")
    if not isinstance(latest, dict):
        return None
    ad = latest.get("attempt_dir")
    if not isinstance(ad, str):
        return None
    meta = store.read_attempt_meta(Path(ad))
    rid = meta.get("runner_id") if isinstance(meta, dict) else None
    return rid if isinstance(rid, str) else None


def _next_runner_id(runner_pool: Any, last_rid: str) -> str | None:
    try:
        rids = runner_pool.runner_ids()
    except Exception:
        return None
    if not isinstance(rids, list) or not rids:
        return None
    if last_rid not in rids:
        return None
    if len(rids) == 1:
        return last_rid
    i = rids.index(last_rid)
    return rids[(i + 1) % len(rids)]


def _requeue_requested(current: dict[str, Any], ent: dict[str, Any], step_id: str) -> tuple[dict[str, Any] | None, int | None]:
    rq = ent.get("requeue")
    if not isinstance(rq, dict):
        return None, None
    nonce = ent.get("requeue_nonce")
    if not isinstance(nonce, int) or nonce <= 0:
        # Back-compat: fall back to embedded nonce if present.
        nonce = rq.get("nonce")
    if not isinstance(nonce, int) or nonce <= 0:
        return None, None
    applied = ((current.get("steps") or {}).get(step_id) or {}).get("requeue_applied_nonce")
    applied_i = int(applied) if isinstance(applied, int) else 0
    if nonce <= applied_i:
        return None, None
    return rq, nonce


def _retry_allowed(batch: dict[str, Any], current: dict[str, Any], step: dict[str, Any], overrides: dict[str, Any]) -> bool:
    """
    Return True if the step is eligible to run another attempt under retry policy.
    If the step has never attempted, returns True.
    """
    step_id = step["step_id"]
    s = (current.get("steps") or {}).get(step_id) or {}
    by = s.get("by_run_id") or {}
    attempts = len(by)
    latest = s.get("latest") or {}
    latest_status = None
    latest_run_id = latest.get("run_id")
    if latest_run_id and latest_run_id in by:
        latest_status = by[latest_run_id].get("status")

    # If no attempts yet, allowed.
    if attempts == 0:
        return True

    # Manual override: force a retry regardless of policy.
    if _force_retry_requested(current, overrides, step_id):
        return True

    # If latest attempt is running, not eligible.
    if latest_status == "running":
        return False

    # If latest succeeded, not eligible.
    if s.get("latest_successful"):
        return False

    policy = step.get("retry_policy") or batch.get("effective_defaults", {}).get("retries") or {}
    max_attempts = int(policy.get("max_attempts", 1))
    retry_on = policy.get("retry_on_statuses") or ["failed"]
    backoff_seconds = policy.get("backoff_seconds", 0)
    try:
        backoff_seconds = int(backoff_seconds)
    except Exception:
        backoff_seconds = 0
    if backoff_seconds < 0:
        backoff_seconds = 0

    if attempts >= max_attempts:
        return False
    if latest_status is None:
        # If unknown, be conservative: allow only if attempts < max_attempts.
        return True

    if backoff_seconds > 0 and latest_status in set(retry_on):
        ended_at = None
        if latest_run_id and latest_run_id in by:
            ended_at = by[latest_run_id].get("ended_at")
        if isinstance(ended_at, str) and ended_at:
            try:
                age = int((parse_iso8601_z(utc_isoformat()) - parse_iso8601_z(ended_at)).total_seconds())
                if age < backoff_seconds:
                    return False
            except Exception:
                # If time parsing fails, be conservative and allow.
                pass
    return latest_status in set(retry_on)


def _force_retry_requested(current: dict[str, Any], overrides: dict[str, Any], step_id: str) -> bool:
    steps = overrides.get("steps") if isinstance(overrides, dict) else None
    if not isinstance(steps, dict):
        return False
    ent = steps.get(step_id)
    if not isinstance(ent, dict):
        return False
    nonce = ent.get("force_retry_nonce")
    if not isinstance(nonce, int) or nonce <= 0:
        return False
    applied = ((current.get("steps") or {}).get(step_id) or {}).get("force_retry_applied_nonce")
    applied_i = int(applied) if isinstance(applied, int) else 0
    return nonce > applied_i


def _forced_terminal(current: dict[str, Any], overrides: dict[str, Any], step_id: str) -> bool:
    steps = overrides.get("steps") if isinstance(overrides, dict) else None
    if not isinstance(steps, dict):
        return False
    ent = steps.get(step_id)
    if not isinstance(ent, dict):
        return False
    ft = ent.get("forced_terminal")
    return isinstance(ft, dict) and isinstance(ft.get("status"), str)


def _apply_forced_terminal_marker(store: Any, current: dict[str, Any], batch_id: str, job_id: str, step_id: str, overrides: dict[str, Any]) -> None:
    """
    Record forced terminal status in current.json for observability and stable scoreboard derivation.
    """
    steps = (overrides.get("steps") or {}) if isinstance(overrides, dict) else {}
    ent = steps.get(step_id) if isinstance(steps, dict) else None
    ft = ent.get("forced_terminal") if isinstance(ent, dict) else None
    if not isinstance(ft, dict):
        return

    def apply(cur: dict[str, Any]) -> None:
        cur_steps = cur.setdefault("steps", {})
        s = cur_steps.setdefault(step_id, {})
        if s.get("forced_terminal") == ft:
            return
        s["forced_terminal"] = ft

    store.update_current_atomic(batch_id, job_id, apply)


def _mark_force_retry_applied(store: Any, current: dict[str, Any], batch_id: str, job_id: str, step_id: str, overrides: dict[str, Any]) -> None:
    steps = overrides.get("steps") if isinstance(overrides, dict) else None
    if not isinstance(steps, dict):
        return
    ent = steps.get(step_id)
    if not isinstance(ent, dict):
        return
    nonce = ent.get("force_retry_nonce")
    if not isinstance(nonce, int):
        return

    def apply(cur: dict[str, Any]) -> None:
        cur_steps = cur.setdefault("steps", {})
        s = cur_steps.setdefault(step_id, {})
        s["force_retry_applied_nonce"] = nonce

    store.update_current_atomic(batch_id, job_id, apply)


def _mark_requeue_applied(
    store: Any,
    current: dict[str, Any],
    batch_id: str,
    job_id: str,
    step_id: str,
    overrides: dict[str, Any],
    nonce: int,
) -> None:
    if not isinstance(nonce, int) or nonce <= 0:
        return

    def apply(cur: dict[str, Any]) -> None:
        cur_steps = cur.setdefault("steps", {})
        s = cur_steps.setdefault(step_id, {})
        s["requeue_applied_nonce"] = nonce

    store.update_current_atomic(batch_id, job_id, apply)
