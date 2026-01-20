from __future__ import annotations

import secrets
from dataclasses import dataclass

from parallelhassnes.core.time import utc_compact_timestamp


def _suffix() -> str:
    return secrets.token_hex(4)


def new_batch_id() -> str:
    return f"batch_{utc_compact_timestamp()}_{_suffix()}"


def new_job_id(i: int) -> str:
    return f"job_{i:04d}"


def new_run_id() -> str:
    return f"run_{utc_compact_timestamp()}_{_suffix()}"


@dataclass(frozen=True)
class CorrelationIds:
    batch_id: str
    job_id: str
    step_id: str
    run_id: str
    runner_id: str
    codex_thread_id: str | None = None

