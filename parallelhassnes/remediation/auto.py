from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.ops.cancel import cancel_attempt
from parallelhassnes.ops.overrides import set_force_retry
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard


_ORPHAN_REASONS = (
    "orphaned running attempt",
    "pid not alive",
    "stale heartbeat for running attempt",
)


@dataclass(frozen=True)
class AutoRemediationResult:
    enabled: bool
    actions: list[dict[str, Any]]


def maybe_auto_remediate(store: Any, cfg: dict[str, Any], *, batch_id_filter: str | None = None) -> AutoRemediationResult:
    """
    Optional policy-driven auto-remediation executor.

    Default: disabled (no actions; scoreboards still surface stuck/failed).
    If enabled, can take actions for stuck/failed/needs_attention steps.
    """
    scoreboard = ((cfg.get("defaults") or {}).get("scoreboard") or {}) if isinstance(cfg, dict) else {}
    ar = (scoreboard.get("auto_remediation") or {}) if isinstance(scoreboard, dict) else {}

    # Compatibility: spec only requires “stuck auto-remediation enablement” to be present conceptually.
    # Support both the structured config (`auto_remediation`) and a simple boolean flag.
    enabled = bool(ar.get("enabled", False))
    enabled_via_legacy_flag = False
    if not enabled and isinstance(scoreboard, dict) and bool(scoreboard.get("stuck_auto_remediation_enabled", False)):
        enabled = True
        enabled_via_legacy_flag = True
        if not isinstance(ar, dict):
            ar = {}
        # Provide conservative defaults for the legacy boolean flag.
        # Include `needs_attention` so orphaned attempts (pid not alive / stale heartbeat) can be auto-retried.
        # `_needs_attention_is_orphaned` ensures we don't blindly retry all needs_attention failures.
        ar = {"enabled": True, "kinds": ["stuck", "needs_attention"], "action": "cancel_and_force_retry", **ar}
    if not enabled:
        return AutoRemediationResult(enabled=False, actions=[])

    kinds = ar.get("kinds") or ["stuck"]
    if not isinstance(kinds, list):
        kinds = ["stuck"]
    kinds_set = {k for k in kinds if isinstance(k, str) and k}
    if not kinds_set:
        kinds_set = {"stuck"}

    action = ar.get("action") if isinstance(ar.get("action"), str) else "cancel_and_force_retry"
    max_actions = int(ar.get("max_actions_per_tick", 10))
    if max_actions <= 0:
        max_actions = 10
    grace = float(ar.get("cancel_grace_seconds", 1.0))
    max_applies_per_step = int(ar.get("max_applies_per_step", 1 if enabled_via_legacy_flag else 5))
    if max_applies_per_step <= 0:
        max_applies_per_step = 1

    actions: list[dict[str, Any]] = []
    batches = list(store.list_batches())
    if isinstance(batch_id_filter, str) and batch_id_filter.strip():
        b = batch_id_filter.strip()
        batches = [b] if b in set(batches) else []
    for batch_id in batches:
        if len(actions) >= max_actions:
            break
        # Operator control: closed batches are finalized and should not be auto-remediated.
        try:
            if (store.paths.batch_dir(batch_id) / "batch_closed.json").exists():
                continue
        except Exception:
            pass
        try:
            sb = compute_batch_scoreboard(store, batch_id=batch_id)
        except Exception:
            continue
        for item in sb.get("attention", []) if isinstance(sb, dict) else []:
            if len(actions) >= max_actions:
                break
            if not isinstance(item, dict):
                continue
            kind = item.get("kind")
            if not isinstance(kind, str) or kind not in kinds_set:
                continue
            job_id = item.get("job_id")
            step_id = item.get("step_id")
            if not isinstance(job_id, str) or not isinstance(step_id, str):
                continue

            if _auto_remediation_count(store, batch_id, job_id, step_id) >= max_applies_per_step:
                continue

            # Resolve attempt_dir when present (e.g., for stuck).
            attempt_dir = item.get("attempt_dir")
            attempt_path = Path(attempt_dir).resolve() if isinstance(attempt_dir, str) and attempt_dir else None

            if kind == "stuck" and attempt_path is not None:
                # Idempotency: avoid repeatedly canceling the same latest attempt.
                if _already_auto_remediated_latest(store, batch_id, job_id, step_id):
                    continue
                if action != "cancel_and_force_retry":
                    continue
                cancel_res = cancel_attempt(attempt_path, grace_seconds=grace)
                _sync_current_on_cancel(store, attempt_path)
                # Trigger a new attempt via force-retry override.
                try:
                    ov_path = store.overrides_path(batch_id, job_id)
                    ov = set_force_retry(ov_path, step_id)
                except Exception:
                    ov = None
                # Record the remediation action in the mutable index (current.json),
                # not inside the attempt directory (which becomes immutable at attempt end).
                try:
                    _mark_auto_remediated_latest(store, batch_id, job_id, step_id, kind=kind, action="cancel_and_force_retry")
                except Exception:
                    pass
                actions.append(
                    {
                        "batch_id": batch_id,
                        "job_id": job_id,
                        "step_id": step_id,
                        "kind": kind,
                        "action": "cancel_and_force_retry",
                        "attempt_dir": str(attempt_path),
                        "cancel": cancel_res,
                        "overrides": ov,
                    }
                )
                continue

            if kind in {"failed", "needs_attention"}:
                # Do not write into terminal attempt dirs; record the action in current.json.
                if _already_auto_remediated_latest(store, batch_id, job_id, step_id):
                    continue

                # Safety default: only auto-retry needs_attention steps when we can attribute them to
                # harness/runner disruption (orphaned pid / stale heartbeat), unless the operator
                # explicitly configures broader retries.
                if kind == "needs_attention":
                    orphaned = _needs_attention_is_orphaned(store, batch_id, job_id, step_id)
                    allow = orphaned or (action == "force_retry" and not enabled_via_legacy_flag)
                    if not allow:
                        continue

                if action not in {"force_retry", "cancel_and_force_retry"}:
                    continue
                try:
                    ov_path = store.overrides_path(batch_id, job_id)
                    ov = set_force_retry(ov_path, step_id)
                except Exception:
                    continue
                _mark_auto_remediated_latest(store, batch_id, job_id, step_id, kind=kind, action="force_retry")
                actions.append({"batch_id": batch_id, "job_id": job_id, "step_id": step_id, "kind": kind, "action": "force_retry", "overrides": ov})

    return AutoRemediationResult(enabled=True, actions=actions)


def _sync_current_on_cancel(store: Any, attempt_dir: Path) -> None:
    """
    Best-effort: when we cancel an attempt, sync `current.json.steps[*].by_run_id[*].status` so
    scheduling logic (which reads `current.json`) can proceed.
    """
    meta_path = attempt_dir / "meta.json"
    state_path = attempt_dir / "state.json"
    if not meta_path.exists() or not state_path.exists():
        return
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        st = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(meta, dict) or not isinstance(st, dict):
        return
    batch_id = meta.get("batch_id")
    job_id = meta.get("job_id")
    step_id = meta.get("step_id")
    run_id = meta.get("run_id")
    if not all(isinstance(x, str) and x for x in (batch_id, job_id, step_id, run_id)):
        return
    ended_at = st.get("ended_at")
    if not isinstance(ended_at, str) or not ended_at:
        ended_at = utc_isoformat()

    def apply(cur: dict[str, Any]) -> None:
        s = (cur.setdefault("steps", {}) or {}).setdefault(step_id, {})
        by = s.setdefault("by_run_id", {})
        ent = by.get(run_id)
        if not isinstance(ent, dict):
            return
        ent = dict(ent)
        ent["status"] = "canceled"
        ent["ended_at"] = ended_at
        by[run_id] = ent

    try:
        store.update_current_atomic(batch_id, job_id, apply)
    except Exception:
        return


def _already_auto_remediated_latest(store: Any, batch_id: str, job_id: str, step_id: str) -> bool:
    cur = store.read_current(batch_id, job_id) or {}
    s = ((cur.get("steps") or {}).get(step_id) or {})
    if not isinstance(s, dict):
        return False
    latest = s.get("latest")
    latest_run_id = latest.get("run_id") if isinstance(latest, dict) else None
    mark = s.get("auto_remediation")
    if isinstance(mark, dict) and isinstance(latest_run_id, str):
        return mark.get("last_applied_run_id") == latest_run_id
    return False


def _mark_auto_remediated_latest(store: Any, batch_id: str, job_id: str, step_id: str, *, kind: str | None = None, action: str | None = None) -> None:
    cur = store.read_current(batch_id, job_id) or {}
    s = ((cur.get("steps") or {}).get(step_id) or {})
    latest = s.get("latest") if isinstance(s, dict) else None
    latest_run_id = latest.get("run_id") if isinstance(latest, dict) else None
    if not isinstance(latest_run_id, str) or not latest_run_id:
        return

    def apply(cur2: dict[str, Any]) -> None:
        s2 = (cur2.setdefault("steps", {}) or {}).setdefault(step_id, {})
        prev = s2.get("auto_remediation") if isinstance(s2.get("auto_remediation"), dict) else {}
        prev_count = int(prev.get("applied_count_total", 0)) if isinstance(prev, dict) else 0
        payload: dict[str, Any] = {
            "last_applied_at": utc_isoformat(),
            "last_applied_run_id": latest_run_id,
            "applied_count_total": prev_count + 1,
        }
        if isinstance(kind, str) and kind:
            payload["kind"] = kind
        if isinstance(action, str) and action:
            payload["action"] = action
        s2["auto_remediation"] = payload

    store.update_current_atomic(batch_id, job_id, apply)


def _auto_remediation_count(store: Any, batch_id: str, job_id: str, step_id: str) -> int:
    cur = store.read_current(batch_id, job_id) or {}
    s = ((cur.get("steps") or {}).get(step_id) or {})
    if not isinstance(s, dict):
        return 0
    ar = s.get("auto_remediation")
    if not isinstance(ar, dict):
        return 0
    v = ar.get("applied_count_total", 0)
    try:
        return int(v)
    except Exception:
        return 0


def _needs_attention_is_orphaned(store: Any, batch_id: str, job_id: str, step_id: str) -> bool:
    cur = store.read_current(batch_id, job_id) or {}
    steps = cur.get("steps") if isinstance(cur, dict) else None
    if not isinstance(steps, dict):
        return False
    s = steps.get(step_id)
    if not isinstance(s, dict):
        return False
    latest = s.get("latest")
    attempt_dir = latest.get("attempt_dir") if isinstance(latest, dict) else None
    if not isinstance(attempt_dir, str) or not attempt_dir:
        return False
    st = store.read_attempt_state(Path(attempt_dir)) or {}
    if not isinstance(st, dict) or st.get("status") != "needs_attention":
        return False
    reason = str(st.get("current_item") or "")
    errs = st.get("errors") or []
    parts = [reason]
    if isinstance(errs, list):
        parts.extend([str(x) for x in errs])
    blob = " | ".join([p for p in parts if p]).lower()
    return any(r in blob for r in _ORPHAN_REASONS)
