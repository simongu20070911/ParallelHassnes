from __future__ import annotations

import os
import signal
import time
from pathlib import Path
from typing import Any

from parallelhassnes.core.state_update import write_state_guarded
from parallelhassnes.core.time import utc_isoformat


TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "needs_attention"}


def cancel_attempt(attempt_dir: Path, grace_seconds: float = 3.0) -> dict[str, Any]:
    """
    Best-effort cancel:
    - If state is terminal => no-op.
    - If state missing => treat as queued/initializing and write terminal canceled state.
    - If state running and pid present => SIGTERM process group, then SIGKILL, then write canceled state.
    """
    attempt_dir = attempt_dir.resolve()
    state_path = attempt_dir / "state.json"
    meta_path = attempt_dir / "meta.json"
    now = utc_isoformat()

    # meta.json should exist for well-formed attempts, but tolerate it being momentarily missing
    # if a producer crashed mid-creation (filesystem mode favors robustness over strictness here).

    state: dict[str, Any] | None = None
    if state_path.exists():
        state = _read_json(state_path)
        status = state.get("status")
        if status in TERMINAL_STATUSES:
            return {"ok": True, "already_terminal": True, "status": status, "attempt_dir": str(attempt_dir)}

    # queued/initializing: no state file yet
    if state is None:
        partial = _discover_partial_artifacts(attempt_dir)
        canceled = {
            "status": "canceled",
            "started_at": now,
            "ended_at": now,
            "last_heartbeat_at": None,
            "current_item": "canceled before start",
            "exit_code": None,
            "pid": None,
            "artifacts": partial,
            "errors": ["canceled before state.json existed"],
        }
        write_state_guarded(state_path, canceled)
        return {"ok": True, "canceled": True, "attempt_dir": str(attempt_dir)}

    pid = state.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        # No PID to kill; still mark canceled to unblock orchestration.
        partial = state.get("artifacts") if isinstance(state.get("artifacts"), list) else _discover_partial_artifacts(attempt_dir)
        canceled = dict(state)
        canceled.update({"status": "canceled", "ended_at": now, "current_item": "canceled (no pid)", "errors": (state.get("errors") or []) + ["canceled but pid missing"]})
        if not canceled.get("started_at"):
            canceled["started_at"] = now
        canceled["artifacts"] = partial
        write_state_guarded(state_path, canceled)
        return {"ok": True, "canceled": True, "attempt_dir": str(attempt_dir), "killed": False}

    killed = _kill_process_group(pid, grace_seconds=grace_seconds)
    partial = state.get("artifacts") if isinstance(state.get("artifacts"), list) else _discover_partial_artifacts(attempt_dir)
    canceled = dict(state)
    canceled.update({"status": "canceled", "ended_at": now, "current_item": "canceled", "errors": (state.get("errors") or []) + (["process killed"] if killed else ["process not found"])})
    if not canceled.get("started_at"):
        canceled["started_at"] = now
    canceled["artifacts"] = partial
    write_state_guarded(state_path, canceled)
    return {"ok": True, "canceled": True, "attempt_dir": str(attempt_dir), "killed": killed}


def _kill_process_group(pid: int, grace_seconds: float) -> bool:
    # We start Codex with start_new_session=True, so pid is the process group id too.
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Try direct kill as fallback
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            return False

    deadline = time.time() + grace_seconds
    while time.time() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.05)

    try:
        os.killpg(pid, signal.SIGKILL)
    except Exception:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass
    time.sleep(0.1)
    return not _pid_exists(pid)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _discover_partial_artifacts(attempt_dir: Path) -> list[dict[str, Any]]:
    """
    Best-effort artifact preservation for canceled attempts.

    This is intentionally conservative: it records only the small, well-known
    files/dirs the harness/runner write so the orchestrator can inspect them.
    """
    known = [
        ("meta.json", "log", "attempt metadata"),
        ("state.json", "log", "attempt state snapshot"),
        ("final.json", "report", "agent Run Report (if written)"),
        ("final.txt", "log", "raw final output (if written)"),
        ("codex.events.jsonl", "log", "optional codex JSONL events stream"),
        ("git_diff.patch", "patch", "optional git diff at attempt end"),
        ("git_status.txt", "log", "optional git status at attempt end"),
        ("git_head.txt", "log", "optional git HEAD at attempt end"),
        ("files_touched.json", "report", "optional best-effort files touched"),
        ("codex_home", "directory", "attempt-local CODEX_HOME (resume base)"),
    ]
    out: list[dict[str, Any]] = []
    for name, kind, desc in known:
        p = attempt_dir / name
        if p.exists():
            out.append({"path": str(p), "kind": kind, "description": desc})
    return out
