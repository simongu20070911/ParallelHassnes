from __future__ import annotations

from typing import Any, Iterable

ATTEMPT_CORRELATION_REQUIRED_FIELDS: tuple[str, ...] = (
    "batch_id",
    "job_id",
    "step_id",
    "run_id",
    "runner_id",
)

ATTEMPT_CORRELATION_OPTIONAL_FIELDS: tuple[str, ...] = ("codex_thread_id",)


def require_nonempty_str_fields(obj: dict[str, Any], fields: Iterable[str], errors: list[str], prefix: str = "") -> None:
    for k in fields:
        if not isinstance(obj.get(k), str) or not obj[k]:
            errors.append(f"{prefix}{k} is required")

