from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.paths import Paths
from parallelhassnes.core.time import parse_iso8601_z


@dataclass(frozen=True)
class ResolvedResumeSource:
    step_id: str
    selector: str
    run_id: str
    attempt_dir: str
    resume_base_dir: str
    codex_thread_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "step_id": self.step_id,
            "selector": self.selector,
            "run_id": self.run_id,
            "attempt_dir": self.attempt_dir,
            "resume_base_dir": self.resume_base_dir,
        }
        if self.codex_thread_id:
            out["codex_thread_id"] = self.codex_thread_id
        return out


_TERMINAL_ATTEMPT_STATUSES = {"succeeded", "failed", "canceled", "needs_attention"}


def resolve_resume_source(
    paths: Paths,
    batch_id: str,
    job_id: str,
    current: dict[str, Any],
    resume_from: dict[str, Any],
) -> ResolvedResumeSource:
    """
    Deterministically resolve a `resume_from` selector to a concrete source attempt.

    Rules:
    - `latest_successful`: choose the successful attempt with the most recent `ended_at`.
    - `latest`: choose the most recent *terminal* attempt by attempt_dir basename (lex order; timestamp prefix).
    - `run_id`: resolve a specific terminal attempt; use `by_run_id[run_id]` if present, otherwise discover the attempt directory by suffix match.
    """
    src_step_id = resume_from.get("step_id")
    if not isinstance(src_step_id, str) or not src_step_id:
        raise ValueError("resume_from.step_id is required")

    selector = resume_from.get("selector")
    if not isinstance(selector, str) or not selector:
        selector = "latest_successful"

    src_entry = ((current.get("steps") or {}).get(src_step_id) or {})
    if not isinstance(src_entry, dict):
        src_entry = {}
    by = src_entry.get("by_run_id")
    if not isinstance(by, dict):
        by = {}

    if selector == "latest_successful":
        sel = _select_latest_successful(src_step_id, src_entry, by)
        if sel is not None:
            return sel
        raise RuntimeError(f"resume_from cannot resolve source attempt: {src_step_id} selector=latest_successful")

    if selector == "latest":
        sel = _select_latest(src_step_id, src_entry, by)
        if sel is not None:
            return sel
        raise RuntimeError(f"resume_from cannot resolve source attempt: {src_step_id} selector=latest")

    if selector == "run_id":
        rid = resume_from.get("run_id")
        if not isinstance(rid, str) or not rid:
            raise ValueError("resume_from.run_id is required when selector=run_id")
        return _select_run_id(paths, batch_id, job_id, src_step_id, rid, by)

    # Treat any other selector string as an explicit run_id.
    return _select_run_id(paths, batch_id, job_id, src_step_id, selector, by)


def _select_latest_successful(step_id: str, src_entry: dict[str, Any], by: dict[str, Any]) -> ResolvedResumeSource | None:
    best: tuple[Any, str, dict[str, Any]] | None = None  # (ended_dt, run_id, ent)
    for rid, ent in by.items():
        if not isinstance(rid, str) or not rid:
            continue
        if not isinstance(ent, dict):
            continue
        if ent.get("status") != "succeeded":
            continue
        ended_at = ent.get("ended_at")
        if not isinstance(ended_at, str) or not ended_at:
            continue
        try:
            ended_dt = parse_iso8601_z(ended_at)
        except Exception:
            continue
        if best is None or ended_dt > best[0]:
            best = (ended_dt, rid, ent)

    if best is not None:
        _, rid, ent = best
        return _ent_to_resolved(step_id, "latest_successful", rid, ent)

    # Fallback: accept current.json latest_successful pointer if present.
    ptr = src_entry.get("latest_successful")
    if isinstance(ptr, dict):
        rid = ptr.get("run_id")
        if isinstance(rid, str) and rid:
            return _ent_to_resolved(step_id, "latest_successful", rid, ptr)
    return None


def _select_latest(step_id: str, src_entry: dict[str, Any], by: dict[str, Any]) -> ResolvedResumeSource | None:
    candidates: list[tuple[str, str, dict[str, Any]]] = []  # (dir_name, run_id, ent)
    for rid, ent in by.items():
        if not isinstance(rid, str) or not rid:
            continue
        if not isinstance(ent, dict):
            continue
        if ent.get("status") not in _TERMINAL_ATTEMPT_STATUSES:
            continue
        ended_at = ent.get("ended_at")
        if not isinstance(ended_at, str) or not ended_at:
            continue
        ad = ent.get("attempt_dir")
        if not isinstance(ad, str) or not ad:
            continue
        candidates.append((Path(ad).name, rid, ent))

    if candidates:
        candidates.sort()
        _, rid, ent = candidates[-1]
        return _ent_to_resolved(step_id, "latest", rid, ent)

    # Fallback: accept current.json latest pointer if present.
    ptr = src_entry.get("latest")
    if isinstance(ptr, dict):
        rid = ptr.get("run_id")
        if isinstance(rid, str) and rid:
            ent = by.get(rid)
            if isinstance(ent, dict) and ent.get("status") in _TERMINAL_ATTEMPT_STATUSES:
                return _ent_to_resolved(step_id, "latest", rid, ent)
    return None


def _select_run_id(paths: Paths, batch_id: str, job_id: str, step_id: str, run_id: str, by: dict[str, Any]) -> ResolvedResumeSource:
    ent = by.get(run_id)
    if isinstance(ent, dict):
        if ent.get("status") not in _TERMINAL_ATTEMPT_STATUSES:
            raise RuntimeError(f"resume_from source attempt not terminal: {step_id} run_id={run_id} status={ent.get('status')!r}")
        ended_at = ent.get("ended_at")
        if not isinstance(ended_at, str) or not ended_at:
            raise RuntimeError(f"resume_from source attempt missing ended_at: {step_id} run_id={run_id}")
        return _ent_to_resolved(step_id, "run_id", run_id, ent)

    # Discover attempt directory by path derivation rules: directory name ends with "_<run_id>".
    attempts_dir = paths.attempts_dir(batch_id, job_id, step_id)
    if attempts_dir.exists():
        matches: list[Path] = []
        suf = "_" + run_id
        for p in attempts_dir.iterdir():
            if p.is_dir() and p.name.endswith(suf):
                matches.append(p)
        if matches:
            matches.sort(key=lambda p: p.name)
            attempt_dir = matches[-1].resolve()
            resume_base_dir = attempt_dir / "codex_home"
            if not resume_base_dir.exists():
                raise RuntimeError(f"resume_from source resume_base_dir missing: {resume_base_dir}")
            st = attempt_dir / "state.json"
            if st.exists():
                try:
                    state = json.loads(st.read_text(encoding="utf-8"))
                    status = state.get("status") if isinstance(state, dict) else None
                    if status not in _TERMINAL_ATTEMPT_STATUSES:
                        raise RuntimeError(f"resume_from source attempt not terminal: {step_id} run_id={run_id} status={status!r}")
                except Exception:
                    # If state is unreadable, be conservative and refuse to resume.
                    raise RuntimeError(f"resume_from source attempt state unreadable: {st}")
            tid = _read_thread_id_from_meta(attempt_dir / "meta.json")
            return ResolvedResumeSource(
                step_id=step_id,
                selector="run_id",
                run_id=run_id,
                attempt_dir=str(attempt_dir),
                resume_base_dir=str(resume_base_dir),
                codex_thread_id=tid,
            )

    raise RuntimeError(f"resume_from cannot resolve source attempt: {step_id} selector=run_id run_id={run_id}")


def _read_thread_id_from_meta(meta_path: Path) -> str | None:
    if not meta_path.exists():
        return None
    try:
        raw = json.loads(meta_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            tid = raw.get("codex_thread_id")
            if isinstance(tid, str) and tid:
                return tid
    except Exception:
        return None
    return None


def _ent_to_resolved(step_id: str, selector: str, run_id: str, ent: dict[str, Any]) -> ResolvedResumeSource:
    resume_base_dir = ent.get("resume_base_dir")
    attempt_dir = ent.get("attempt_dir")
    if not isinstance(resume_base_dir, str) or not resume_base_dir:
        raise RuntimeError(f"resume_from source entry missing resume_base_dir: {step_id}/{run_id}")
    if not isinstance(attempt_dir, str) or not attempt_dir:
        raise RuntimeError(f"resume_from source entry missing attempt_dir: {step_id}/{run_id}")
    tid = ent.get("codex_thread_id")
    return ResolvedResumeSource(
        step_id=step_id,
        selector=selector,
        run_id=run_id,
        attempt_dir=attempt_dir,
        resume_base_dir=resume_base_dir,
        codex_thread_id=tid if isinstance(tid, str) and tid else None,
    )
