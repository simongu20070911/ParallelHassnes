from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.file_lock import acquire_lock_blocking
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.validation.contracts import validate_current


@dataclass(frozen=True)
class CurrentUpdate:
    batch_id: str
    job_id: str
    current_path: Path
    lock_path: Path


def update_current_atomic(
    info: CurrentUpdate,
    update_fn: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    lock = acquire_lock_blocking(info.lock_path)
    try:
        if info.current_path.exists():
            import json

            cur = json.loads(info.current_path.read_text(encoding="utf-8"))
            if not isinstance(cur, dict):
                cur = {"batch_id": info.batch_id, "job_id": info.job_id, "updated_at": utc_isoformat(), "steps": {}}
        else:
            cur = {"batch_id": info.batch_id, "job_id": info.job_id, "updated_at": utc_isoformat(), "steps": {}}

        update_fn(cur)
        cur["batch_id"] = info.batch_id
        cur["job_id"] = info.job_id
        cur["updated_at"] = utc_isoformat()
        cur.setdefault("steps", {})

        errs = validate_current(cur)
        if errs:
            raise ValueError("Invalid current.json: " + "; ".join(errs))

        write_atomic_json(info.current_path, cur)
        return cur
    finally:
        lock.release()

