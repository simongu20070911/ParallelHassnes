from __future__ import annotations

from typing import Any

from parallelhassnes.core.contracts import (
    ATTEMPT_CORRELATION_REQUIRED_FIELDS,
    require_nonempty_str_fields,
)

import re


_SECRET_KEY_RE = re.compile(r"(secret|token|api[_-]?key|private[_-]?key|password)", re.IGNORECASE)


def _deny_secrets(obj: Any, path: str = "$") -> list[str]:
    problems: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            kp = f"{path}.{k}"
            if isinstance(k, str) and _SECRET_KEY_RE.search(k):
                problems.append(f"Secret-like key not allowed in harness_config: {kp}")
            problems.extend(_deny_secrets(v, kp))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            problems.extend(_deny_secrets(v, f"{path}[{i}]"))
    elif isinstance(obj, str):
        if "-----BEGIN" in obj and "PRIVATE KEY-----" in obj:
            problems.append(f"Private key material not allowed in harness_config at {path}")
    return problems


def validate_harness_config(cfg: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if not isinstance(cfg.get("harness_config_version"), str) or not cfg["harness_config_version"]:
        errors.append("harness_config_version is required")
    if not isinstance(cfg.get("written_at"), str) or not cfg["written_at"]:
        errors.append("written_at is required")
    interfaces = cfg.get("interfaces")
    if not isinstance(interfaces, dict):
        errors.append("interfaces must be an object")
    defaults = cfg.get("defaults")
    if not isinstance(defaults, dict):
        errors.append("defaults must be an object")
    limits = cfg.get("limits")
    if not isinstance(limits, dict):
        errors.append("limits must be an object")

    pwc = (limits or {}).get("per_workdir_concurrency")
    if pwc is not None:
        if not isinstance(pwc, dict):
            errors.append("limits.per_workdir_concurrency must be an object")
        else:
            for k, v in pwc.items():
                if not isinstance(k, str) or not k.strip():
                    errors.append("limits.per_workdir_concurrency keys must be non-empty strings")
                    break
                if not isinstance(v, int) or v <= 0:
                    errors.append(f"limits.per_workdir_concurrency[{k!r}] must be an integer >= 1")
                    break

    # Optional disk-bloat guardrails (bytes; null disables).
    for key in ("max_isolated_git_root_du_bytes", "max_isolated_git_root_tracked_bytes"):
        v = (limits or {}).get(key)
        if v is None:
            continue
        if not isinstance(v, int) or v < 0:
            errors.append(f"limits.{key} must be an integer >= 0 (bytes) or null")

    stale_after = None
    try:
        stale_after = int(((defaults or {}).get("scoreboard") or {}).get("heartbeat_stale_after_seconds", 2700))
    except Exception:
        errors.append("defaults.scoreboard.heartbeat_stale_after_seconds must be an integer")
    if stale_after is not None and stale_after < 1800:
        errors.append("defaults.scoreboard.heartbeat_stale_after_seconds must be >= 1800")

    hb_interval = None
    try:
        hb_interval = int(((defaults or {}).get("scoreboard") or {}).get("heartbeat_interval_seconds", 900))
    except Exception:
        errors.append("defaults.scoreboard.heartbeat_interval_seconds must be an integer")
    if hb_interval is not None:
        if hb_interval <= 0:
            errors.append("defaults.scoreboard.heartbeat_interval_seconds must be >= 1")
        if hb_interval > 900:
            errors.append("defaults.scoreboard.heartbeat_interval_seconds must be <= 900")

    secret_problems = _deny_secrets(cfg)
    if secret_problems:
        errors.extend(secret_problems)

    return errors


def validate_batch_meta(batch: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    require_nonempty_str_fields(batch, ("batch_id",), errors)
    if not isinstance(batch.get("submitted_at"), str) or not batch["submitted_at"]:
        errors.append("submitted_at is required")
    if not isinstance(batch.get("harness_config_version"), str) or not batch["harness_config_version"]:
        errors.append("harness_config_version is required")
    goal = batch.get("batch_goal_summary")
    if not isinstance(goal, str) or not goal.strip():
        errors.append("batch_goal_summary is required")
    else:
        # Spec rule: whitespace-delimited token count, must be more than 150 words.
        wc = len([t for t in goal.strip().split() if t])
        if wc <= 150:
            errors.append("batch_goal_summary must be more than 150 words (whitespace-delimited)")
    jobs = batch.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        errors.append("jobs[] is required")
    return errors


def validate_current(current: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    require_nonempty_str_fields(current, ("batch_id", "job_id"), errors)
    if not isinstance(current.get("updated_at"), str) or not current["updated_at"]:
        errors.append("updated_at is required")
    steps = current.get("steps")
    if not isinstance(steps, dict):
        errors.append("steps must be an object")
        return errors
    for step_id, entry in steps.items():
        if not isinstance(entry, dict):
            errors.append(f"steps[{step_id}] must be an object")
            continue
        latest = entry.get("latest")
        if latest is not None and not isinstance(latest, dict):
            errors.append(f"steps[{step_id}].latest must be an object")
        by = entry.get("by_run_id")
        if by is not None and not isinstance(by, dict):
            errors.append(f"steps[{step_id}].by_run_id must be an object")
    return errors


def validate_attempt_meta(meta: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    require_nonempty_str_fields(meta, ATTEMPT_CORRELATION_REQUIRED_FIELDS, errors)
    inv = meta.get("invocation")
    if inv not in {"exec", "resume"}:
        errors.append("invocation must be exec|resume")
    return errors


def validate_attempt_state(state: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    status = state.get("status")
    if status not in {"queued", "running", "succeeded", "failed", "canceled", "needs_attention"}:
        errors.append("status must be queued|running|succeeded|failed|canceled|needs_attention")
        return errors
    if status in {"running", "succeeded", "failed", "canceled", "needs_attention"}:
        if not isinstance(state.get("started_at"), str) or not state["started_at"]:
            errors.append("started_at required for running/terminal state")
    if status in {"succeeded", "failed", "canceled", "needs_attention"}:
        if not isinstance(state.get("ended_at"), str) or not state["ended_at"]:
            errors.append("ended_at required for terminal state")
    return errors
