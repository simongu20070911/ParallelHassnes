from __future__ import annotations

import gzip
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.time import now_utc, parse_iso8601_z


@dataclass(frozen=True)
class RetentionPolicy:
    keep_raw_events_days: int
    keep_git_artifacts_days: int
    keep_resume_bases_days: int
    compress_raw_events_after_days: int | None = None
    keep_reduced_state_days: int = 3650
    keep_final_outputs_days: int = 3650
    allow_delete_success_final_outputs: bool = False


def run_gc(
    runs_root: Path,
    policy: RetentionPolicy,
    protected_resume_bases: set[Path],
) -> dict[str, Any]:
    """
    Filesystem GC:
    - Never delete `final.*` or `handoff.json`.
    - Never delete any resume base dir in `protected_resume_bases`.
    - Optionally delete old raw events and git artifacts by ended_at age.
    - Optionally delete old `codex_home/` for closed batches only (see batch_closed.json marker).
    """
    runs_root = runs_root.resolve()
    now = now_utc()

    deleted: list[str] = []
    kept: list[str] = []

    for batch_dir in _iter_batches(runs_root):
        batch_meta = _read_json(batch_dir / "batch_meta.json")
        job_overrides: dict[str, dict[str, Any]] = {}
        jobs = batch_meta.get("jobs") if isinstance(batch_meta, dict) else None
        if isinstance(jobs, list):
            for j in jobs:
                if isinstance(j, dict) and isinstance(j.get("job_id"), str):
                    job_overrides[j["job_id"]] = j

        closed = (batch_dir / "batch_closed.json").exists()
        for attempt_dir in batch_dir.glob("**/attempts/*"):
            if not attempt_dir.is_dir():
                continue
            rel = None
            try:
                rel = attempt_dir.relative_to(batch_dir)
            except Exception:
                rel = None
            job_id = rel.parts[0] if rel and rel.parts else None
            eff_policy = _effective_policy(policy, batch_meta, job_id=job_id, job_entry=job_overrides.get(job_id or ""))

            state_path = attempt_dir / "state.json"
            if not state_path.exists():
                continue
            st = _read_json(state_path)
            ended_at = st.get("ended_at")
            if not isinstance(ended_at, str) or not ended_at:
                continue
            age_days = (now - parse_iso8601_z(ended_at)).total_seconds() / 86400.0
            status = st.get("status")

            # Raw events
            events = attempt_dir / "codex.events.jsonl"
            events_gz = attempt_dir / "codex.events.jsonl.gz"
            if events.exists():
                if eff_policy.compress_raw_events_after_days is not None and age_days > eff_policy.compress_raw_events_after_days:
                    if not events_gz.exists():
                        _gzip_file(events, events_gz)
                        deleted.append(str(events))
                        events.unlink(missing_ok=True)
                        # Keep the compressed copy as the raw-events artifact.
                        kept.append(str(events_gz))
                if age_days > eff_policy.keep_raw_events_days:
                    deleted.append(str(events))
                    events.unlink(missing_ok=True)
            if events_gz.exists() and age_days > eff_policy.keep_raw_events_days:
                deleted.append(str(events_gz))
                events_gz.unlink(missing_ok=True)

            # Raw logs (stdout/stderr)
            for name in ("codex.stdout.txt", "codex.stderr.txt"):
                p = attempt_dir / name
                gz = attempt_dir / f"{name}.gz"
                if p.exists():
                    if eff_policy.compress_raw_events_after_days is not None and age_days > eff_policy.compress_raw_events_after_days:
                        if not gz.exists():
                            _gzip_file(p, gz)
                            deleted.append(str(p))
                            p.unlink(missing_ok=True)
                            kept.append(str(gz))
                    if age_days > eff_policy.keep_raw_events_days:
                        deleted.append(str(p))
                        p.unlink(missing_ok=True)
                if gz.exists() and age_days > eff_policy.keep_raw_events_days:
                    deleted.append(str(gz))
                    gz.unlink(missing_ok=True)

            # Git artifacts
            if age_days > eff_policy.keep_git_artifacts_days:
                for name in ("git_head.txt", "git_status.txt", "git_diff.patch", "files_touched.json"):
                    p = attempt_dir / name
                    if p.exists():
                        deleted.append(str(p))
                        p.unlink(missing_ok=True)

            # Reduced state / final outputs: keep longer (default: very long), delete only for closed batches.
            if closed and age_days > eff_policy.keep_reduced_state_days:
                for name in ("state.json",):
                    p = attempt_dir / name
                    if p.exists():
                        deleted.append(str(p))
                        p.unlink(missing_ok=True)
            if closed and age_days > eff_policy.keep_final_outputs_days:
                # Never delete successful outputs unless explicitly configured.
                if not (status == "succeeded" and not eff_policy.allow_delete_success_final_outputs):
                    for name in ("final.json", "final.txt"):
                        p = attempt_dir / name
                        if p.exists():
                            deleted.append(str(p))
                            p.unlink(missing_ok=True)

            # Resume bases (`codex_home/`) only for closed batches and only after keep_resume_bases_days.
            codex_home = attempt_dir / "codex_home"
            if codex_home.exists():
                if codex_home.resolve() in protected_resume_bases:
                    kept.append(str(codex_home))
                elif closed and age_days > eff_policy.keep_resume_bases_days:
                    deleted.append(str(codex_home))
                    shutil.rmtree(codex_home, ignore_errors=True)
                else:
                    kept.append(str(codex_home))

    return {"ok": True, "deleted": deleted, "kept_resume_bases": kept}


def compute_protected_resume_bases_for_open_batches(runs_root: Path) -> set[Path]:
    protected: set[Path] = set()
    for batch_dir in _iter_batches(runs_root.resolve()):
        if (batch_dir / "batch_closed.json").exists():
            continue
        for current_path in batch_dir.glob("*/current.json"):
            try:
                cur = _read_json(current_path)
            except Exception:
                continue
            steps = (cur.get("steps") or {}) if isinstance(cur, dict) else {}
            if not isinstance(steps, dict):
                continue
            for _, s in steps.items():
                if not isinstance(s, dict):
                    continue
                for key in ("latest", "latest_successful"):
                    ent = s.get(key)
                    if isinstance(ent, dict) and isinstance(ent.get("resume_base_dir"), str):
                        protected.add(Path(ent["resume_base_dir"]).resolve())
                by = s.get("by_run_id")
                if isinstance(by, dict):
                    for _, ent in by.items():
                        if isinstance(ent, dict) and isinstance(ent.get("resume_base_dir"), str):
                            protected.add(Path(ent["resume_base_dir"]).resolve())
    return protected


def _iter_batches(runs_root: Path) -> list[Path]:
    out: list[Path] = []
    if not runs_root.exists():
        return out
    for p in runs_root.iterdir():
        if not p.is_dir():
            continue
        if p.name.startswith("_"):
            continue
        if (p / "batch_meta.json").exists():
            out.append(p)
    return sorted(out)


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _effective_policy(base: RetentionPolicy, batch_meta: dict[str, Any], job_id: str | None, job_entry: dict[str, Any] | None) -> RetentionPolicy:
    """
    Resolve per-batch/job overrides.

    Precedence (lowest -> highest):
    - `base` (typically current harness defaults)
    - `batch_meta.effective_defaults.retention_policy` snapshot (submission-time)
    - `batch_meta.retention_policy` (optional user override)
    - `job.retention_policy` (optional per-job override)
    """
    merged: dict[str, Any] = {
        "keep_raw_events_days": base.keep_raw_events_days,
        "keep_git_artifacts_days": base.keep_git_artifacts_days,
        "keep_resume_bases_days": base.keep_resume_bases_days,
        "compress_raw_events_after_days": base.compress_raw_events_after_days,
        "keep_reduced_state_days": base.keep_reduced_state_days,
        "keep_final_outputs_days": base.keep_final_outputs_days,
        "allow_delete_success_final_outputs": base.allow_delete_success_final_outputs,
    }

    def merge_from(obj: Any) -> None:
        if not isinstance(obj, dict):
            return
        for k in list(merged.keys()):
            if k in obj:
                merged[k] = obj[k]

    merge_from(((batch_meta.get("effective_defaults") or {}).get("retention_policy") or {}) if isinstance(batch_meta, dict) else {})
    merge_from(batch_meta.get("retention_policy") if isinstance(batch_meta, dict) else None)
    if job_entry is not None:
        merge_from(job_entry.get("retention_policy"))

    def as_int(x: Any, default: int) -> int:
        try:
            v = int(x)
            return v if v >= 0 else default
        except Exception:
            return default

    keep_raw = as_int(merged.get("keep_raw_events_days"), base.keep_raw_events_days)
    keep_git = as_int(merged.get("keep_git_artifacts_days"), base.keep_git_artifacts_days)
    keep_resume = as_int(merged.get("keep_resume_bases_days"), base.keep_resume_bases_days)
    compress_after = merged.get("compress_raw_events_after_days")
    compress_after_i: int | None = None
    if compress_after is not None:
        try:
            compress_after_i = int(compress_after)
        except Exception:
            compress_after_i = None
    keep_reduced = as_int(merged.get("keep_reduced_state_days"), base.keep_reduced_state_days)
    keep_final = as_int(merged.get("keep_final_outputs_days"), base.keep_final_outputs_days)
    allow_del_success = bool(merged.get("allow_delete_success_final_outputs", base.allow_delete_success_final_outputs))

    return RetentionPolicy(
        keep_raw_events_days=keep_raw,
        keep_git_artifacts_days=keep_git,
        keep_resume_bases_days=keep_resume,
        compress_raw_events_after_days=compress_after_i,
        keep_reduced_state_days=keep_reduced,
        keep_final_outputs_days=keep_final,
        allow_delete_success_final_outputs=allow_del_success,
    )


def _gzip_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    # Write to temp + rename to avoid partial .gz files.
    tmp = dst.with_name(dst.name + ".tmp")
    with open(src, "rb") as f_in:
        with gzip.open(tmp, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.replace(tmp, dst)
