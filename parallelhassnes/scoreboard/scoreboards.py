from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from parallelhassnes.core.time import parse_iso8601_z, utc_isoformat
from parallelhassnes.resume.selectors import resolve_resume_source


def compute_system_scoreboard(store: Any) -> list[dict[str, Any]]:
    cfg = store.read_harness_config()
    available_runner_ids = _available_runner_ids(store, cfg)

    batches = []
    for batch_id in store.list_batches():
        batch = store.read_batch_meta(batch_id)
        runner_pool_cfg = (
            (batch.get("effective_defaults", {}) or {}).get("runner_pool")
            or (cfg.get("defaults", {}) or {}).get("runner_pool")
            or {}
        )
        shared_fs = bool(runner_pool_cfg.get("shared_filesystem", True))
        transfer_enabled = bool(runner_pool_cfg.get("resume_base_transfer_enabled", False))
        sb_cfg = (
            (batch.get("effective_defaults", {}) or {}).get("scoreboard")
            or (cfg.get("defaults", {}) or {}).get("scoreboard")
            or {}
        )
        threshold = int((sb_cfg or {}).get("heartbeat_stale_after_seconds", 2700))
        if threshold < 1800:
            threshold = 1800
        counts = _count_steps_for_batch(
            store,
            batch,
            shared_fs=shared_fs,
            transfer_enabled=transfer_enabled,
            available_runner_ids=available_runner_ids,
            heartbeat_stale_after_seconds=threshold,
        )
        preview = _goal_preview(batch.get("batch_goal_summary", ""))
        batches.append(
            {
                "batch_id": batch_id,
                "submitted_at": batch.get("submitted_at"),
                "batch_goal_summary_preview": preview,
                "jobs_total": counts["jobs_total"],
                "steps_total": counts["steps_total"],
                "counts": counts["counts"],
                "running_steps": counts["counts"]["running"],
                "attention_steps": counts["counts"]["needs_attention"] + counts["stuck_count"] + counts["counts"]["failed"],
            }
        )

    def key(b: dict[str, Any]) -> tuple[int, int, int]:
        attention = 1 if b.get("attention_steps", 0) > 0 else 0
        running = 1 if b.get("running_steps", 0) > 0 else 0
        sub = b.get("submitted_at") or ""
        ts = 0
        if isinstance(sub, str) and sub:
            try:
                ts = int(parse_iso8601_z(sub).timestamp())
            except Exception:
                ts = 0
        # attention first, then running, then submitted_at desc
        return (-attention, -running, -ts)

    return sorted(batches, key=key)


def compute_batch_scoreboard(store: Any, batch_id: str) -> dict[str, Any]:
    batch = store.read_batch_meta(batch_id)
    cfg = store.read_harness_config()
    sb_cfg = (batch.get("effective_defaults", {}) or {}).get("scoreboard") or (cfg.get("defaults", {}) or {}).get("scoreboard") or {}
    threshold = int((sb_cfg or {}).get("heartbeat_stale_after_seconds", 2700))
    if threshold < 1800:
        threshold = 1800

    runner_pool_cfg = (
        (batch.get("effective_defaults", {}) or {}).get("runner_pool")
        or (cfg.get("defaults", {}) or {}).get("runner_pool")
        or {}
    )
    shared_fs = bool(runner_pool_cfg.get("shared_filesystem", True))
    transfer_enabled = bool(runner_pool_cfg.get("resume_base_transfer_enabled", False))
    available_runner_ids = _available_runner_ids(store, cfg)

    jobs_total = len(batch["jobs"])
    steps_total = sum(len(j["steps"]) for j in batch["jobs"])

    counts = {"blocked": 0, "ready": 0, "running": 0, "succeeded": 0, "failed": 0, "needs_attention": 0, "canceled": 0}
    running_list: list[dict[str, Any]] = []
    blocked_list: list[dict[str, Any]] = []
    failures_list: list[dict[str, Any]] = []
    attention: list[dict[str, Any]] = []
    resume_steps: list[dict[str, Any]] = []

    now = parse_iso8601_z(utc_isoformat())
    for job in batch["jobs"]:
        job_id = job["job_id"]
        current = store.read_current(batch_id, job_id) or {"steps": {}}
        for step in job["steps"]:
            step_id = step["step_id"]
            resume_vis = _resume_visibility(store, batch_id, job_id, current, step)
            derived, reasons, latest_attempt_dir = _derive_step_status(store, batch_id, current, job, step, shared_fs, transfer_enabled, available_runner_ids)
            counts[derived] += 1
            if resume_vis is not None:
                resume_steps.append({"job_id": job_id, "step_id": step_id, "derived_status": derived, "resume_from_resolved": resume_vis})
            if derived == "running" and latest_attempt_dir is not None:
                st = store.read_attempt_state(latest_attempt_dir) or {}
                hb = st.get("last_heartbeat_at")
                started_at = st.get("started_at")
                seconds_since = None
                duration = None
                if hb:
                    seconds_since = int((now - parse_iso8601_z(hb)).total_seconds())
                if started_at:
                    duration = int((now - parse_iso8601_z(started_at)).total_seconds())
                item = {
                    "job_id": job_id,
                    "step_id": step_id,
                    "run_id": (current.get("steps", {}).get(step_id, {}).get("latest") or {}).get("run_id"),
                    "runner_id": (store.read_attempt_meta(latest_attempt_dir) or {}).get("runner_id"),
                    "attempt_dir": str(latest_attempt_dir),
                    "current_item": (st.get("current_item") or ("queued/initializing" if (not st or st.get("status") == "queued") else None)),
                    "last_heartbeat_at": hb,
                    "seconds_since_last_heartbeat": seconds_since,
                    "started_at": started_at,
                    "run_duration_seconds": duration,
                }
                if resume_vis:
                    item["resume_from_resolved"] = resume_vis
                running_list.append(item)
                # "Stuck" = running with no heartbeat update for the threshold.
                # If `last_heartbeat_at` is missing, fall back to started_at duration.
                stale_age_seconds = seconds_since if seconds_since is not None else duration
                if stale_age_seconds is not None and stale_age_seconds > threshold:
                    attention.append({**item, "kind": "stuck"})

            if derived == "blocked":
                blocked_item = {"job_id": job_id, "step_id": step_id, "reasons": reasons}
                if resume_vis:
                    blocked_item["resume_from_resolved"] = resume_vis
                blocked_list.append(blocked_item)

            if derived in {"failed", "needs_attention"} and latest_attempt_dir is not None:
                st = store.read_attempt_state(latest_attempt_dir) or {}
                final = None
                fp = latest_attempt_dir / "final.json"
                if fp.exists():
                    try:
                        final = json.loads(fp.read_text(encoding="utf-8"))
                    except Exception:
                        final = None
                failures_list.append(
                    {
                        "job_id": job_id,
                        "step_id": step_id,
                        "run_id": (current.get("steps", {}).get(step_id, {}).get("latest") or {}).get("run_id"),
                        "attempt_dir": str(latest_attempt_dir),
                        "state_status": st.get("status"),
                        "exit_code": st.get("exit_code"),
                        "final_status": (final or {}).get("status") if final else None,
                        "final_summary": (final or {}).get("summary") if final else None,
                    }
                )
                if resume_vis:
                    failures_list[-1]["resume_from_resolved"] = resume_vis
                attention.append({"job_id": job_id, "step_id": step_id, "kind": derived})

    attention_sorted = sorted(attention, key=lambda x: {"stuck": 0, "needs_attention": 1, "failed": 2}.get(x.get("kind", ""), 9))

    return {
        "batch_id": batch_id,
        "submitted_at": batch.get("submitted_at"),
        "computed_at": utc_isoformat(),
        "batch_goal_summary": batch.get("batch_goal_summary"),
        "jobs_total": jobs_total,
        "steps_total": steps_total,
        "heartbeat_stale_after_seconds": threshold,
        "runner_shared_filesystem": shared_fs,
        "counts": counts,
        "attention": attention_sorted,
        "running": running_list,
        "blocked": blocked_list,
        "failures": failures_list,
        "resume_steps": resume_steps,
    }


def format_scoreboards(obj: dict[str, Any]) -> str:
    if "system" in obj:
        lines = ["SYSTEM SCOREBOARD"]
        for b in obj["system"]:
            lines.append(
                f"- {b['batch_id']} attention={b['attention_steps']} running={b['running_steps']} jobs={b['jobs_total']} steps={b['steps_total']} :: {b['batch_goal_summary_preview']}"
            )
        return "\n".join(lines)
    if "batch" in obj:
        b = obj["batch"]
        lines = [f"BATCH {b['batch_id']}"]
        lines.append(f"submitted_at={b['submitted_at']} computed_at={b['computed_at']}")
        lines.append(f"heartbeat_stale_after_seconds={b['heartbeat_stale_after_seconds']}")
        lines.append("counts=" + json.dumps(b["counts"], ensure_ascii=False))
        if b["attention"]:
            lines.append("attention:")
            for a in b["attention"][:20]:
                lines.append("  - " + json.dumps(a, ensure_ascii=False))
        if b["blocked"]:
            lines.append("blocked:")
            for bl in b["blocked"][:20]:
                lines.append("  - " + json.dumps(bl, ensure_ascii=False))
        return "\n".join(lines)
    return json.dumps(obj, ensure_ascii=False, indent=2)


def _goal_preview(goal: str) -> str:
    g = (goal or "").strip().replace("\n", " ")
    return (g[:120] + "…") if len(g) > 120 else g


def _count_steps_for_batch(
    store: Any,
    batch: dict[str, Any],
    *,
    shared_fs: bool,
    transfer_enabled: bool,
    available_runner_ids: list[str],
    heartbeat_stale_after_seconds: int,
) -> dict[str, Any]:
    jobs_total = len(batch["jobs"])
    steps_total = sum(len(j["steps"]) for j in batch["jobs"])
    counts = {"blocked": 0, "ready": 0, "running": 0, "succeeded": 0, "failed": 0, "needs_attention": 0, "canceled": 0}
    stuck_count = 0
    threshold = int(heartbeat_stale_after_seconds)
    if threshold < 1800:
        threshold = 1800
    now = parse_iso8601_z(utc_isoformat())

    for job in batch["jobs"]:
        current = store.read_current(batch["batch_id"], job["job_id"]) or {"steps": {}}
        for step in job["steps"]:
            status, _, latest_attempt_dir = _derive_step_status(
                store,
                batch["batch_id"],
                current,
                job,
                step,
                shared_fs=shared_fs,
                transfer_enabled=transfer_enabled,
                available_runner_ids=available_runner_ids,
            )
            counts[status] += 1
            if status == "running" and latest_attempt_dir is not None:
                st = store.read_attempt_state(latest_attempt_dir) or {}
                hb = st.get("last_heartbeat_at")
                started_at = st.get("started_at")
                if hb:
                    age = int((now - parse_iso8601_z(hb)).total_seconds())
                    if age > threshold:
                        stuck_count += 1
                elif started_at:
                    age = int((now - parse_iso8601_z(started_at)).total_seconds())
                    if age > threshold:
                        stuck_count += 1
    return {"jobs_total": jobs_total, "steps_total": steps_total, "counts": counts, "stuck_count": stuck_count}


def _derive_step_status(
    store: Any,
    batch_id: str,
    current: dict[str, Any],
    job: dict[str, Any],
    step: dict[str, Any],
    shared_fs: bool,
    transfer_enabled: bool,
    available_runner_ids: list[str],
) -> tuple[str, list[str], Path | None]:
    step_id = step["step_id"]
    s = (current.get("steps") or {}).get(step_id) or {}
    forced = s.get("forced_terminal")
    if isinstance(forced, dict) and isinstance(forced.get("status"), str):
        status = forced["status"]
        if status in {"failed", "canceled", "needs_attention"}:
            return status, [f"forced_terminal: {status}"], None

    # Precedence is spec-driven:
    # 1) running (active attempt)
    # 2) succeeded (any successful attempt exists)
    # 3) latest terminal failure/cancel (only when no successful attempt exists)
    by = s.get("by_run_id") if isinstance(s, dict) else None
    latest = s.get("latest")
    latest_attempt_dir = Path(latest["attempt_dir"]) if latest and latest.get("attempt_dir") else None
    if latest_attempt_dir is not None:
        st = store.read_attempt_state(latest_attempt_dir)
        if st is None:
            # Missing is treated as queued/initializing (in-flight).
            return "running", [], latest_attempt_dir
        if st and st.get("status") in {"queued", "running"}:
            return "running", [], latest_attempt_dir

    if s.get("latest_successful"):
        return "succeeded", [], latest_attempt_dir
    if isinstance(by, dict):
        # Fallback: derive success if any succeeded attempt is recorded.
        for _, ent in by.items():
            if isinstance(ent, dict) and ent.get("status") == "succeeded":
                return "succeeded", [], latest_attempt_dir

    if latest_attempt_dir is not None:
        st = store.read_attempt_state(latest_attempt_dir) or {}
        if st and st.get("status") in {"failed", "needs_attention", "canceled"}:
            return st["status"], [], latest_attempt_dir
        # Best-effort: if host state is terminal succeeded but pointers weren't updated, treat as succeeded.
        if st and st.get("status") == "succeeded":
            return "succeeded", [], latest_attempt_dir

    # Fallback: if `latest` pointer is missing but `by_run_id` exists, infer the latest terminal attempt
    # using only `current.json` (no filesystem scanning).
    if latest_attempt_dir is None and isinstance(by, dict) and by:
        candidates: list[tuple[str, str, dict[str, Any]]] = []  # (ended_at, run_id, ent)
        for rid, ent in by.items():
            if not isinstance(rid, str) or not rid:
                continue
            if not isinstance(ent, dict):
                continue
            status = ent.get("status")
            if status not in {"succeeded", "failed", "canceled", "needs_attention"}:
                continue
            ended_at = ent.get("ended_at")
            if not isinstance(ended_at, str) or not ended_at:
                continue
            candidates.append((ended_at, rid, ent))
        if candidates:
            candidates.sort()
            _, rid, ent = candidates[-1]
            status = ent.get("status")
            if status in {"failed", "needs_attention", "canceled"}:
                # No successful attempt exists (checked above), so this is the derived terminal outcome.
                return str(status), [], None

    reasons: list[str] = []
    def dep_succeeded(dep_step_id: str) -> bool:
        dep_entry = ((current.get("steps") or {}).get(dep_step_id) or {})
        if not isinstance(dep_entry, dict):
            return False
        if dep_entry.get("latest_successful") is not None:
            return True
        dep_by = dep_entry.get("by_run_id")
        if isinstance(dep_by, dict):
            for _, ent in dep_by.items():
                if isinstance(ent, dict) and ent.get("status") == "succeeded":
                    return True
        return False

    # Dependencies from batch spec
    deps = step.get("depends_on") or []
    for d in deps:
        if not dep_succeeded(d):
            reasons.append(f"depends_on: {job['job_id']}.{d} not succeeded")
    if step.get("resume_from"):
        rf = step["resume_from"]
        try:
            resolved = resolve_resume_source(store.paths, batch_id=batch_id, job_id=job["job_id"], current=current, resume_from=rf)
        except Exception as e:
            reasons.append(f"resume_from: cannot resolve source: {e}")
        else:
            rb = resolved.resume_base_dir
            if not rb or not Path(rb).exists():
                reasons.append(f"resume_from: resume base missing: {rb}")
            elif not shared_fs and not transfer_enabled:
                ad = resolved.attempt_dir
                src_rid = None
                if isinstance(ad, str) and ad:
                    meta = store.read_attempt_meta(Path(ad)) or {}
                    src_rid = meta.get("runner_id") if isinstance(meta, dict) else None
                if not isinstance(src_rid, str) or not src_rid:
                    reasons.append(f"resume_from locality: cannot determine source runner_id for {job['job_id']}.{resolved.step_id}")
                elif available_runner_ids and src_rid not in set(available_runner_ids):
                    reasons.append(f"resume_from locality: source runner {src_rid} not available (shared_filesystem=false)")

    if reasons:
        return "blocked", reasons, latest_attempt_dir

    return "ready", [], latest_attempt_dir


def _resume_visibility(store: Any, batch_id: str, job_id: str, current: dict[str, Any], step: dict[str, Any]) -> dict[str, Any] | None:
    rf = step.get("resume_from")
    if not rf:
        return None
    try:
        resolved = resolve_resume_source(store.paths, batch_id=batch_id, job_id=job_id, current=current, resume_from=rf)
        return {
            "resume_from_step_id": resolved.step_id,
            "selector": resolved.selector,
            "selected_run_id": resolved.run_id,
            "resume_base_dir": resolved.resume_base_dir,
        }
    except Exception as e:
        src_step_id = rf.get("step_id")
        selector = rf.get("selector") or ("run_id" if rf.get("run_id") else "latest_successful")
        return {"resume_from_step_id": src_step_id, "selector": selector, "selected_run_id": None, "resume_base_dir": None, "error": str(e)}


def _available_runner_ids(store: Any, cfg: dict[str, Any]) -> list[str]:
    pool = (cfg.get("defaults", {}) or {}).get("runner_pool")
    if isinstance(pool, dict):
        rids = pool.get("runner_ids")
        if isinstance(rids, list):
            out = [x.strip() for x in rids if isinstance(x, str) and x.strip()]
            if out:
                return sorted(out)
    try:
        root = store.paths.runners_root
    except Exception:
        return []
    if not root.exists():
        return []
    out: list[str] = []
    for p in root.iterdir():
        if not p.is_dir():
            continue
        if (p / "health.json").exists():
            out.append(p.name)
    return sorted(out)
