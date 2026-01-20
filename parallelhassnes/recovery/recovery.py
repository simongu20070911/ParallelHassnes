from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.state_update import write_state_guarded
from parallelhassnes.core.time import parse_iso8601_z, utc_isoformat


_orphan_hb_mu = threading.Lock()
_orphan_hb_threads: dict[str, tuple[threading.Thread, threading.Event]] = {}


def shutdown_orphan_heartbeat_threads() -> None:
    """
    Stop any orphan-heartbeat threads started by this process.

    Intended for tests and clean shutdown; normal harness operation can rely on process exit.
    """
    with _orphan_hb_mu:
        items = list(_orphan_hb_threads.items())
        _orphan_hb_threads.clear()
    for _, (t, stop) in items:
        stop.set()
        t.join(timeout=1)


def recover_orphaned_running_attempts(
    store: Any,
    heartbeat_stale_after_seconds: int,
    heartbeat_interval_seconds: int = 900,
    batch_id_filter: str | None = None,
) -> dict[str, Any]:
    """
    Crash-safety helper:
    - If an attempt is `running` but its recorded pid is not alive, mark it terminal `needs_attention`.
    - Sync `current.json.steps[*].by_run_id[*].status` and `.ended_at` for the affected run.
    """
    updated: list[str] = []
    now_dt = parse_iso8601_z(utc_isoformat())
    hb_interval = int(heartbeat_interval_seconds) if isinstance(heartbeat_interval_seconds, int) else 900
    if hb_interval <= 0:
        hb_interval = 900
    batches = list(store.list_batches())
    if isinstance(batch_id_filter, str) and batch_id_filter.strip():
        bid = batch_id_filter.strip()
        batches = [bid] if bid in set(batches) else []
    for batch_id in batches:
        try:
            if (store.paths.batch_dir(batch_id) / "batch_unreadable.json").exists():
                continue
        except Exception:
            pass
        try:
            batch = store.read_batch_meta(batch_id)
        except Exception:
            # If the batch metadata is unreadable (e.g., filesystem/iCloud stub issues),
            # do not wedge the harness tick. Skip this batch; operator can intervene.
            continue
        sb = (batch.get("effective_defaults", {}) or {}).get("scoreboard") if isinstance(batch, dict) else None
        batch_stale_after = heartbeat_stale_after_seconds
        batch_hb_interval = hb_interval
        if isinstance(sb, dict):
            try:
                v = int(sb.get("heartbeat_stale_after_seconds", batch_stale_after))
                if v >= 1800:
                    batch_stale_after = v
            except Exception:
                pass
            try:
                v = int(sb.get("heartbeat_interval_seconds", batch_hb_interval))
                if v > 0:
                    batch_hb_interval = v
            except Exception:
                pass
        for job in batch.get("jobs", []):
            job_id = job["job_id"]
            try:
                cur = store.read_current(batch_id, job_id)
            except Exception:
                # Never wedge crash recovery on unreadable control files.
                continue
            if not cur:
                continue
            to_apply: list[tuple[str, str, str]] = []  # (step_id, run_id, ended_at)
            steps = (cur.get("steps") or {}) if isinstance(cur, dict) else {}
            if not isinstance(steps, dict):
                continue
            for step_id, entry in steps.items():
                if not isinstance(entry, dict):
                    continue
                by = entry.get("by_run_id")
                if not isinstance(by, dict):
                    continue
                for run_id, run_ent in list(by.items()):
                    if not isinstance(run_ent, dict):
                        continue
                    attempt_dir_str = run_ent.get("attempt_dir")
                    if not isinstance(attempt_dir_str, str):
                        continue
                    attempt_dir = Path(attempt_dir_str)
                    try:
                        st = store.read_attempt_state(attempt_dir)
                    except Exception:
                        continue
                    if not st or st.get("status") != "running":
                        continue
                    pid = st.get("pid")
                    hb = st.get("last_heartbeat_at")
                    hb_stale = True
                    if isinstance(hb, str) and hb:
                        try:
                            age = (now_dt - parse_iso8601_z(hb)).total_seconds()
                            hb_stale = age > batch_stale_after
                        except Exception:
                            hb_stale = True

                    pid_alive = isinstance(pid, int) and pid > 0 and _pid_exists(pid)
                    if pid_alive:
                        # Runner restart case: if the Codex process is still running, we can keep the attempt
                        # from being incorrectly finalized just because the heartbeat writer died.
                        #
                        # However, if the recorded heartbeat timestamp predates started_at, treat it as corrupt
                        # (it cannot be a heartbeat for this attempt) and let the scoreboard surface it as stuck.
                        started_at = st.get("started_at")
                        hb_invalid = False
                        if isinstance(hb, str) and hb and isinstance(started_at, str) and started_at:
                            try:
                                hb_dt = parse_iso8601_z(hb)
                                st_dt = parse_iso8601_z(started_at)
                                hb_invalid = hb_dt < st_dt
                            except Exception:
                                hb_invalid = False
                        if hb_invalid:
                            continue

                        _ensure_orphan_heartbeat(attempt_dir / "state.json", pid=pid, interval_seconds=batch_hb_interval)
                        if hb_stale:
                            _refresh_running_heartbeat(attempt_dir / "state.json", pid=pid)
                        continue

                    now = utc_isoformat()
                    new_state = dict(st)
                    reason = "orphaned running attempt"
                    if not pid_alive:
                        reason = "orphaned running attempt (pid not alive)"
                    elif hb_stale:
                        reason = "stale heartbeat for running attempt"
                    new_state.update(
                        {
                            "status": "needs_attention",
                            "ended_at": now,
                            "current_item": reason,
                            "errors": (st.get("errors") or []) + [reason],
                        }
                    )
                    if not new_state.get("started_at"):
                        new_state["started_at"] = now
                    # Guard state writes to avoid clobbering concurrent writers and
                    # to preserve terminal immutability semantics.
                    write_state_guarded(attempt_dir / "state.json", new_state)
                    to_apply.append((step_id, run_id, now))
                    updated.append(str(attempt_dir))

            if to_apply:
                # Avoid clobbering concurrent writers: update only the affected run entries under lock.
                def apply(cur2: dict[str, Any]) -> None:
                    steps2 = cur2.setdefault("steps", {})
                    for sid, rid, ended_at in to_apply:
                        s2 = steps2.get(sid)
                        if not isinstance(s2, dict):
                            continue
                        by2 = s2.get("by_run_id")
                        if not isinstance(by2, dict):
                            continue
                        ent2 = by2.get(rid)
                        if not isinstance(ent2, dict):
                            continue
                        ent2 = dict(ent2)
                        ent2["status"] = "needs_attention"
                        ent2["ended_at"] = ended_at
                        by2[rid] = ent2

                store.update_current_atomic(batch_id, job_id, apply)
    return {"ok": True, "updated_attempts": updated}


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _ensure_orphan_heartbeat(state_path: Path, pid: int, interval_seconds: int) -> None:
    """
    Ensure a background heartbeat thread is running for an orphaned running attempt.

    This is used after a harness restart when the Codex process continues to run but the
    original heartbeat thread is gone.
    """
    key = str(state_path.resolve())
    with _orphan_hb_mu:
        existing = _orphan_hb_threads.get(key)
        if existing is not None and existing[0].is_alive():
            return
        stop = threading.Event()

        def run() -> None:
            try:
                # Best-effort: keep heartbeating until pid is gone, attempt becomes terminal, or shutdown.
                while not stop.wait(interval_seconds):
                    if not _pid_exists(pid):
                        return
                    _refresh_running_heartbeat(state_path, pid=pid)
            finally:
                with _orphan_hb_mu:
                    cur = _orphan_hb_threads.get(key)
                    if cur is not None and cur[0] is t:
                        _orphan_hb_threads.pop(key, None)

        t = threading.Thread(target=run, daemon=True)
        _orphan_hb_threads[key] = (t, stop)
        t.start()


def _refresh_running_heartbeat(state_path: Path, pid: int) -> None:
    now = utc_isoformat()
    obj: dict[str, Any] = {}
    if state_path.exists():
        try:
            raw = json.loads(state_path.read_text(encoding="utf-8"))
            obj = raw if isinstance(raw, dict) else {}
        except Exception:
            obj = {}

    # Only heartbeat running attempts; do not "revive" terminal ones.
    if obj.get("status") in {"succeeded", "failed", "canceled", "needs_attention"}:
        return

    obj.setdefault("status", "running")
    obj.setdefault("started_at", now)
    obj["last_heartbeat_at"] = now
    obj["pid"] = pid
    if not obj.get("current_item"):
        obj["current_item"] = "runner restart: heartbeat resumed"
    write_state_guarded(state_path, obj)
