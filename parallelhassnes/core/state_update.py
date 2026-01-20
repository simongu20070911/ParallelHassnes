from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.file_lock import acquire_lock_blocking


TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "needs_attention"}
NON_TERMINAL_STATUSES = {"queued", "running"}


def _allowed_transition(prev: str | None, nxt: str) -> bool:
    # Creation: allow any valid status.
    if prev is None:
        return nxt in (TERMINAL_STATUSES | NON_TERMINAL_STATUSES)

    if prev in TERMINAL_STATUSES:
        return False

    if prev == "queued":
        return nxt in {"queued", "running", "canceled"}

    if prev == "running":
        return nxt in {"running", "succeeded", "failed", "canceled", "needs_attention"}

    # Unknown/invalid previous status: be strict.
    return False


def read_state_best_effort(state_path: Path) -> dict[str, Any] | None:
    if not state_path.exists():
        return None
    try:
        obj = json.loads(state_path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def write_state_guarded(state_path: Path, obj: dict[str, Any]) -> bool:
    """
    Write `state.json` as an atomic snapshot, while preserving the invariant:
    once terminal, never change again.

    Returns True if a write occurred, False if it was suppressed due to terminal state.
    """
    lock = acquire_lock_blocking(state_path.with_suffix(".lock"))
    try:
        existing = read_state_best_effort(state_path)
        prev_status = existing.get("status") if isinstance(existing, dict) else None
        next_status = obj.get("status")
        if not isinstance(next_status, str) or not next_status:
            raise ValueError("state.status must be a non-empty string")

        if isinstance(prev_status, str) and prev_status in TERMINAL_STATUSES:
            # Terminal is frozen forever.
            return False

        if isinstance(prev_status, str) or prev_status is None:
            if not _allowed_transition(prev_status if isinstance(prev_status, str) else None, next_status):
                raise ValueError(f"illegal state transition: {prev_status!r} -> {next_status!r}")
        write_atomic_json(state_path, obj)
        return True
    finally:
        lock.release()
