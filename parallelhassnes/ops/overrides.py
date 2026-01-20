from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.time import utc_isoformat


@dataclass(frozen=True)
class StepOverrides:
    # Monotonic nonce; increment to request another run regardless of retry policy.
    force_retry_nonce: int | None = None

    # If set, the scheduler treats the step as terminal and will not schedule it.
    forced_terminal_status: str | None = None  # failed|canceled|needs_attention
    forced_terminal_reason: str | None = None


def load_overrides(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "updated_at": None, "steps": {}}
    import json

    obj = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        return {"version": 1, "updated_at": None, "steps": {}}
    obj.setdefault("version", 1)
    obj.setdefault("steps", {})
    return obj


def save_overrides(path: Path, obj: dict[str, Any]) -> None:
    obj["updated_at"] = utc_isoformat()
    write_atomic_json(path, obj)


def set_force_retry(path: Path, step_id: str) -> dict[str, Any]:
    obj = load_overrides(path)
    steps = obj.setdefault("steps", {})
    ent = steps.setdefault(step_id, {})
    nonce = ent.get("force_retry_nonce")
    nonce_i = int(nonce) if isinstance(nonce, int) else 0
    ent["force_retry_nonce"] = nonce_i + 1
    steps[step_id] = ent
    save_overrides(path, obj)
    return obj


def set_forced_terminal(path: Path, step_id: str, status: str, reason: str | None) -> dict[str, Any]:
    if status not in {"failed", "canceled", "needs_attention"}:
        raise ValueError("forced terminal status must be failed|canceled|needs_attention")
    obj = load_overrides(path)
    steps = obj.setdefault("steps", {})
    ent = steps.setdefault(step_id, {})
    ent["forced_terminal"] = {"status": status, "reason": reason, "set_at": utc_isoformat()}
    steps[step_id] = ent
    save_overrides(path, obj)
    return obj


def set_requeue(path: Path, step_id: str, target_runner_id: str | None, different_from_last: bool, reason: str | None) -> dict[str, Any]:
    obj = load_overrides(path)
    steps = obj.setdefault("steps", {})
    ent = steps.setdefault(step_id, {})
    nonce = ent.get("requeue_nonce")
    nonce_i = int(nonce) if isinstance(nonce, int) else 0
    ent["requeue_nonce"] = nonce_i + 1
    ent["requeue"] = {
        "nonce": nonce_i + 1,
        "target_runner_id": target_runner_id,
        "different_from_last": bool(different_from_last),
        "reason": reason,
        "requested_at": utc_isoformat(),
    }
    steps[step_id] = ent
    save_overrides(path, obj)
    return obj
