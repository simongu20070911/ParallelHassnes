from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.ids import new_batch_id
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.validation.launch_table import normalize_launch_table, validate_launch_table
from parallelhassnes.validation.contracts import validate_batch_meta


@dataclass(frozen=True)
class FsQueue:
    root: Path

    def incoming_dir(self) -> Path:
        return self.root / "incoming"

    def processed_dir(self) -> Path:
        return self.root / "processed"

    def index_dir(self) -> Path:
        return self.root / "index"

    def by_sha_dir(self) -> Path:
        return self.index_dir() / "by_launch_table_sha256"

    def ingest_all(self, store: Any) -> list[str]:
        self.incoming_dir().mkdir(parents=True, exist_ok=True)
        self.processed_dir().mkdir(parents=True, exist_ok=True)
        self.by_sha_dir().mkdir(parents=True, exist_ok=True)

        batch_ids: list[str] = []
        for p in sorted(self.incoming_dir().glob("*.json")):
            batch_id = self._ingest_one(store, p)
            batch_ids.append(batch_id)
            p.rename(self.processed_dir() / p.name)
        return batch_ids

    def _ingest_one(self, store: Any, launch_table_path: Path) -> str:
        raw = json.loads(launch_table_path.read_text(encoding="utf-8"))
        sha = hashlib.sha256(launch_table_path.read_bytes()).hexdigest()
        existing = self.by_sha_dir() / f"{sha}.json"
        if existing.exists():
            prev = json.loads(existing.read_text(encoding="utf-8"))
            return prev["batch_id"]

        def deep_merge(base: Any, override: Any) -> Any:
            if isinstance(base, dict) and isinstance(override, dict):
                out = dict(base)
                for k, v in override.items():
                    out[k] = deep_merge(out.get(k), v)
                return out
            return override

        # Submission-time allowlist: prohibit caller from overriding harness-derived fields.
        forbidden = {"effective_defaults", "harness_config_version", "launch_table_sha256"}
        if isinstance(raw, dict):
            bad = sorted(k for k in raw.keys() if k in forbidden)
            if bad:
                raise ValueError(f"Launch Table must not include override fields: {', '.join(bad)}")

        assigned_batch_id = raw.get("batch_id") or new_batch_id()
        normalized = normalize_launch_table(raw, assigned_batch_id=assigned_batch_id, launch_table_sha256=sha)

        harness_cfg = store.read_harness_config()
        normalized["harness_config_version"] = harness_cfg["harness_config_version"]

        # Enforce harness safety limits (maximum batch/job/step sizes).
        limits = harness_cfg.get("limits") if isinstance(harness_cfg, dict) else None
        if not isinstance(limits, dict):
            limits = {}
        try:
            max_jobs = int(limits.get("max_jobs_per_batch", 10000))
        except Exception:
            max_jobs = 10000
        try:
            max_steps_per_job = int(limits.get("max_steps_per_job", 100))
        except Exception:
            max_steps_per_job = 100
        jobs = normalized.get("jobs") or []
        if isinstance(jobs, list) and max_jobs > 0 and len(jobs) > max_jobs:
            raise ValueError(f"batch exceeds max_jobs_per_batch={max_jobs}")
        if isinstance(jobs, list) and max_steps_per_job > 0:
            for j in jobs:
                if not isinstance(j, dict):
                    continue
                steps = j.get("steps") or []
                if isinstance(steps, list) and len(steps) > max_steps_per_job:
                    jid = j.get("job_id") if isinstance(j.get("job_id"), str) else "<unknown>"
                    raise ValueError(f"job {jid} exceeds max_steps_per_job={max_steps_per_job}")

        # Policy merge: compute effective defaults from harness defaults plus submission-time overrides.
        #
        # The spec is intentionally "conceptual" about override shapes, but it does require:
        # - harness computes `effective_defaults`
        # - Launch Table overrides are limited to an explicitly allowed subset
        allowed_default_keys = {"execution_policy", "timeouts", "retries", "retention_policy", "scoreboard", "workspace_policy"}
        overrides = None
        if isinstance(raw, dict):
            for k in ("defaults_overrides", "effective_defaults_overrides"):
                if raw.get(k) is not None:
                    overrides = raw.get(k)
                    break
        if overrides is not None and not isinstance(overrides, dict):
            raise ValueError("defaults_overrides must be an object when provided")
        if isinstance(overrides, dict):
            unknown = sorted(k for k in overrides.keys() if k not in allowed_default_keys)
            if unknown:
                raise ValueError("defaults_overrides contains unsupported keys: " + ", ".join(unknown))

        base_defaults = harness_cfg.get("defaults", {}) if isinstance(harness_cfg, dict) else {}
        effective: dict[str, Any] = {}
        for k in sorted(allowed_default_keys):
            v = (base_defaults.get(k) if isinstance(base_defaults, dict) else None) or {}
            if not isinstance(v, dict):
                v = {}
            effective[k] = v

        # Allow the Launch Table to specify defaults directly (conceptual spec field shape),
        # in addition to the explicit `defaults_overrides` object.
        top_level_overrides: dict[str, Any] = {}
        if isinstance(raw, dict):
            for k in sorted(allowed_default_keys):
                if raw.get(k) is None:
                    continue
                v = raw.get(k)
                if not isinstance(v, dict):
                    raise ValueError(f"Launch Table field {k} must be an object when provided")
                top_level_overrides[k] = v
        for k, v in top_level_overrides.items():
            effective[k] = deep_merge(effective.get(k), v)

        if isinstance(overrides, dict):
            for k, v in overrides.items():
                effective[k] = deep_merge(effective.get(k), v)

        # Validate scoreboard-related defaults that affect monitoring correctness.
        sb = effective.get("scoreboard")
        if isinstance(sb, dict):
            if "heartbeat_stale_after_seconds" in sb:
                try:
                    stale = int(sb.get("heartbeat_stale_after_seconds"))
                except Exception:
                    raise ValueError("defaults.scoreboard.heartbeat_stale_after_seconds must be an integer")
                if stale < 1800:
                    raise ValueError("defaults.scoreboard.heartbeat_stale_after_seconds must be >= 1800 (30 minutes)")
            if "heartbeat_interval_seconds" in sb:
                try:
                    iv = int(sb.get("heartbeat_interval_seconds"))
                except Exception:
                    raise ValueError("defaults.scoreboard.heartbeat_interval_seconds must be an integer")
                if iv <= 0:
                    raise ValueError("defaults.scoreboard.heartbeat_interval_seconds must be >= 1")
                if iv > 900:
                    raise ValueError("defaults.scoreboard.heartbeat_interval_seconds must be <= 900 (15 minutes)")
        normalized["effective_defaults"] = effective

        validate_launch_table(normalized, store)
        errs = validate_batch_meta(normalized)
        if errs:
            raise ValueError("Invalid batch_meta.json: " + "; ".join(errs))

        store.write_batch_meta(normalized)

        write_atomic_json(existing, {"batch_id": assigned_batch_id, "queued_at": utc_isoformat(), "source": str(launch_table_path)})
        ack = self.processed_dir() / f"{launch_table_path.stem}.ack.json"
        write_atomic_json(ack, {"batch_id": assigned_batch_id, "launch_table_sha256": sha, "accepted_job_ids": [j["job_id"] for j in normalized["jobs"]]})
        return assigned_batch_id
