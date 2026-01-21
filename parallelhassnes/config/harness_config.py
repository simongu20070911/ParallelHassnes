from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.atomic_io import write_atomic_json, write_once_json
from parallelhassnes.core.time import utc_isoformat


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


@dataclass(frozen=True)
class HarnessConfig:
    runs_root: Path
    runners_root: Path
    harness_config_version: str
    written_at: str
    interfaces: dict[str, Any]
    defaults: dict[str, Any]
    limits: dict[str, Any]
    queue_root: Path | None = None

    @staticmethod
    def default(
        runs_root: Path,
        runners_root: Path,
        queue_root: Path | None = None,
        harness_config_version: str | None = None,
    ) -> "HarnessConfig":
        from parallelhassnes.core.ids import new_run_id

        version = harness_config_version or f"hc_{new_run_id()}"
        return HarnessConfig(
            runs_root=runs_root,
            runners_root=runners_root,
            queue_root=queue_root,
            harness_config_version=version,
            written_at=utc_isoformat(),
            interfaces={
                "api_mode": {"enabled": False, "auth_mode": "none"},
                "filesystem_queue_mode": {"enabled": True},
            },
            defaults={
                "execution_policy": {
                    "sandbox": "workspace-write",
                "approval_policy": "on-request",
                "skip_git_repo_check": False,
                "web_search_enabled": False,
                "capture_git_artifacts": False,
                # Default to keeping enough artifacts to debug/reproduce runs.
                "capture_events_jsonl": True,
                "capture_codex_thread_id": True,
            },
                "timeouts": {"step_timeout_seconds": 3600},
                "retries": {"max_attempts": 1},
                "runner_capacity": 1,
                "runner_pool": {"shared_filesystem": True, "runner_ids": [], "resume_base_transfer_enabled": False},
                "runner_affinity": {"resume_steps": "pin_resume_source", "non_resume_steps": "none"},
                "scheduler": {"multi_batch": True},
                "retention_policy": {
                    "keep_raw_events_days": 30,
                    "keep_git_artifacts_days": 7,
                    "keep_resume_bases_days": 30,
                    "compress_raw_events_after_days": 2,
                    "keep_reduced_state_days": 3650,
                    "keep_final_outputs_days": 3650,
                    "allow_delete_success_final_outputs": False,
                },
                "scoreboard": {
                    "heartbeat_stale_after_seconds": 2700,
                    "heartbeat_interval_seconds": 900,
                    # Auto-remediate stale/orphaned attempts (cancel+retry for stuck running; force-retry for orphaned needs_attention).
                    "stuck_auto_remediation_enabled": True,
                },
                "workspace_policy": {"mode": "shared"},
            },
            limits={
                "max_jobs_per_batch": 10000,
                "max_steps_per_job": 100,
                "per_workdir_concurrency": {},
                # Optional disk-bloat guardrails for workspace isolation.
                # If set, submit-time preflight blocks isolated worktrees whose git root exceeds these sizes.
                # Values are bytes; null disables.
                "max_isolated_git_root_du_bytes": None,
                "max_isolated_git_root_tracked_bytes": None,
            },
        )

    def to_json_obj(self) -> dict[str, Any]:
        paths: dict[str, Any] = {"runs_root": str(self.runs_root), "runners_root": str(self.runners_root)}
        if self.queue_root is not None:
            paths["queue_root"] = str(self.queue_root)
        return {
            "harness_config_version": self.harness_config_version,
            "written_at": self.written_at,
            "paths": paths,
            "interfaces": self.interfaces,
            "defaults": self.defaults,
            "limits": self.limits,
        }


def write_harness_config_snapshots(cfg: HarnessConfig) -> None:
    from parallelhassnes.core.paths import Paths

    paths = Paths(runs_root=cfg.runs_root, runners_root=cfg.runners_root)
    obj = cfg.to_json_obj()

    problems = _deny_secrets(obj)
    if problems:
        raise ValueError("Invalid harness_config.json (contains secrets):\n" + "\n".join(problems))

    stale_after = int(obj.get("defaults", {}).get("scoreboard", {}).get("heartbeat_stale_after_seconds", 2700))
    if stale_after < 1800:
        raise ValueError("heartbeat_stale_after_seconds must be >= 1800 (30 minutes)")

    hb_interval = int(obj.get("defaults", {}).get("scoreboard", {}).get("heartbeat_interval_seconds", 900))
    if hb_interval <= 0:
        raise ValueError("heartbeat_interval_seconds must be >= 1")
    if hb_interval > 900:
        raise ValueError("heartbeat_interval_seconds must be <= 900 (15 minutes)")

    cfg.runs_root.mkdir(parents=True, exist_ok=True)
    cfg.runners_root.mkdir(parents=True, exist_ok=True)

    write_atomic_json(paths.harness_config_path(), obj)
    versions_dir = paths.harness_config_versions_dir()
    versions_dir.mkdir(parents=True, exist_ok=True)
    write_once_json(versions_dir / f"{cfg.harness_config_version}.json", obj)
