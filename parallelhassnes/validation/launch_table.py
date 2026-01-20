from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from parallelhassnes.core.ids import new_job_id
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.validation.versioning import parse_semver_major

DEFAULT_LAUNCH_TABLE_SPEC_VERSION = "1.0.0"
SUPPORTED_LAUNCH_TABLE_MAJOR = 1


def _word_count(text: str) -> int:
    return len([t for t in text.strip().split() if t])


def normalize_launch_table(raw: dict[str, Any], assigned_batch_id: str, launch_table_sha256: str) -> dict[str, Any]:
    lt = copy.deepcopy(raw)
    lt["batch_id"] = assigned_batch_id
    lt.setdefault("submitted_at", utc_isoformat())
    lt.setdefault("spec_version", DEFAULT_LAUNCH_TABLE_SPEC_VERSION)

    # Snapshot/ref handling: for FS queue we embed normalized LT inline.
    lt["launch_table_sha256"] = launch_table_sha256

    jobs = lt.get("jobs") or []
    for i, job in enumerate(jobs):
        job.setdefault("job_id", new_job_id(i + 1))
        steps = job.get("steps") or []
        for si, step in enumerate(steps):
            step.setdefault("step_id", f"step{si+1}")
            if "resume_from" in step and step["resume_from"] is not None:
                rf = step["resume_from"]
                rf.setdefault("selector", "latest_successful")
    lt["jobs"] = jobs

    # Materialize effective defaults placeholder (harness will fill; keep minimal).
    lt.setdefault("effective_defaults", {})
    return lt


def validate_launch_table(lt: dict[str, Any], store: Any) -> None:
    # Versioning: accept any supported major; reject unknown major.
    spec_version = lt.get("spec_version")
    if not isinstance(spec_version, str) or not spec_version.strip():
        raise ValueError("spec_version is required (string)")
    major = parse_semver_major(spec_version)
    if major is None:
        raise ValueError("spec_version must be semver-ish (e.g., 1.0.0)")
    if major != SUPPORTED_LAUNCH_TABLE_MAJOR:
        raise ValueError(f"unsupported spec_version major={major}; supported={SUPPORTED_LAUNCH_TABLE_MAJOR}.x")

    # Required: batch_goal_summary >150 words.
    goal = lt.get("batch_goal_summary")
    if not isinstance(goal, str) or not goal.strip():
        raise ValueError("batch_goal_summary is required")
    if _word_count(goal) <= 150:
        raise ValueError("batch_goal_summary must be more than 150 words (whitespace-delimited)")

    jobs = lt.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("jobs[] is required and must be non-empty")

    # Validate prompt refs exist if provided.
    working_root = lt.get("working_root")
    if working_root is not None and not isinstance(working_root, str):
        raise ValueError("working_root must be a string when provided")

    for job in jobs:
        if "job_id" not in job:
            raise ValueError("job.job_id is required after normalization")
        wd = job.get("working_directory")
        if wd is None or not isinstance(wd, str):
            raise ValueError(f"job {job['job_id']}: working_directory is required")
        steps = job.get("steps")
        if not isinstance(steps, list) or not steps:
            raise ValueError(f"job {job['job_id']}: steps[] is required")

        # Step graph cycle check (per-job).
        step_ids = [s.get("step_id") for s in steps]
        if any(not isinstance(sid, str) or not sid for sid in step_ids):
            raise ValueError(f"job {job['job_id']}: all steps must have step_id")
        edges: dict[str, set[str]] = {sid: set() for sid in step_ids}  # sid -> deps
        for step in steps:
            sid = step["step_id"]
            deps = step.get("depends_on") or []
            if not isinstance(deps, list):
                raise ValueError(f"job {job['job_id']} step {sid}: depends_on must be a list")
            for d in deps:
                if d not in edges:
                    raise ValueError(f"job {job['job_id']} step {sid}: depends_on unknown step_id: {d}")
                edges[sid].add(d)

            rf = step.get("resume_from")
            if rf:
                src = rf.get("step_id")
                if not isinstance(src, str) or not src:
                    raise ValueError(f"job {job['job_id']} step {sid}: resume_from.step_id is required")
                if src not in edges:
                    raise ValueError(f"job {job['job_id']} step {sid}: resume_from.step_id unknown: {src}")
                edges[sid].add(src)

            # Pack references must be resolvable if used.
            pack_id = step.get("pack_id") or job.get("pack_id") or lt.get("default_pack_id")
            if pack_id:
                packs = lt.get("packs") or {}
                if pack_id not in packs:
                    raise ValueError(f"job {job['job_id']} step {sid}: pack_id not found in packs: {pack_id}")

            prompt_ref = step.get("prompt_ref")
            if prompt_ref is not None:
                if not isinstance(prompt_ref, str) or not prompt_ref:
                    raise ValueError(f"job {job['job_id']} step {sid}: prompt_ref must be a non-empty string")
                if not _resolve_ref_exists(working_root, wd, prompt_ref):
                    raise FileNotFoundError(f"job {job['job_id']} step {sid}: prompt_ref not found: {prompt_ref}")

            schema_ref = step.get("output_schema_ref")
            if schema_ref is not None:
                if not isinstance(schema_ref, str) or not schema_ref:
                    raise ValueError(f"job {job['job_id']} step {sid}: output_schema_ref must be a non-empty string")
                if not _resolve_ref_exists(working_root, wd, schema_ref) and not Path(schema_ref).exists():
                    raise FileNotFoundError(f"job {job['job_id']} step {sid}: output_schema_ref not found: {schema_ref}")

        _assert_acyclic(edges, f"job {job['job_id']}")


def _resolve_ref_exists(working_root: str | None, job_working_directory: str, ref: str) -> bool:
    # Interpret refs as filesystem paths relative to job working directory (simple MVP).
    base = Path(working_root) if working_root else Path.cwd()
    job_root = (base / job_working_directory).resolve()
    return (job_root / ref).exists()


def _assert_acyclic(deps: dict[str, set[str]], context: str) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(n: str) -> None:
        if n in visited:
            return
        if n in visiting:
            raise ValueError(f"{context}: step DAG has a cycle at {n}")
        visiting.add(n)
        for d in deps.get(n, set()):
            dfs(d)
        visiting.remove(n)
        visited.add(n)

    for k in deps:
        dfs(k)
