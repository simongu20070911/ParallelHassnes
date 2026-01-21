from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.config.harness_config import HarnessConfig, write_harness_config_snapshots
from parallelhassnes.core.atomic_io import write_once_json
from parallelhassnes.core.paths import Paths
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.interfaces.fs_queue import FsQueue
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard, compute_system_scoreboard, format_scoreboards
from parallelhassnes.storage.runs_store import RunsStore


@dataclass(frozen=True)
class Harness:
    runs_root: Path
    runners_root: Path
    queue_root: Path

    def __post_init__(self) -> None:
        self.runs_root.mkdir(parents=True, exist_ok=True)
        self.runners_root.mkdir(parents=True, exist_ok=True)
        self.queue_root.mkdir(parents=True, exist_ok=True)

    def _paths(self) -> Paths:
        return Paths(runs_root=self.runs_root, runners_root=self.runners_root)

    def tick_once(
        self,
        concurrency_override: int | None,
        use_fake_invoker: bool,
        batch_id_filter: str | None = None,
        *,
        multi_batch: bool | None = None,
    ) -> None:
        store = RunsStore(self._paths())
        cfg_path = self._paths().harness_config_path()
        if not cfg_path.exists():
            cfg = HarnessConfig.default(runs_root=self.runs_root, runners_root=self.runners_root, queue_root=self.queue_root)
            write_harness_config_snapshots(cfg)
        cfg = store.read_harness_config()
        # Ensure the current config version is preserved as a write-once snapshot.
        try:
            versions_dir = self._paths().harness_config_versions_dir()
            versions_dir.mkdir(parents=True, exist_ok=True)
            version = cfg.get("harness_config_version")
            if isinstance(version, str) and version:
                write_once_json(versions_dir / f"{version}.json", cfg)
        except FileExistsError:
            pass

        from parallelhassnes.remediation.auto import maybe_auto_remediate

        from parallelhassnes.recovery.recovery import recover_orphaned_running_attempts

        hb_interval = int(cfg.get("defaults", {}).get("scoreboard", {}).get("heartbeat_interval_seconds", 900))
        if hb_interval <= 0:
            hb_interval = 900
        stale_after = int(cfg.get("defaults", {}).get("scoreboard", {}).get("heartbeat_stale_after_seconds", 2700))
        if stale_after < 1800:
            stale_after = 1800
        recover_orphaned_running_attempts(
            store,
            heartbeat_stale_after_seconds=stale_after,
            heartbeat_interval_seconds=hb_interval,
            batch_id_filter=batch_id_filter,
        )

        # Optional remediation runs after crash recovery so orphaned attempts can be auto-retried
        # (if configured), instead of staying in `needs_attention` until an operator intervenes.
        maybe_auto_remediate(store, cfg, batch_id_filter=batch_id_filter)

        queue = FsQueue(self.queue_root)
        ingested = queue.ingest_all(store)

        mb = multi_batch
        if mb is None:
            dv = ((cfg.get("defaults", {}) or {}).get("scheduler") or {}) if isinstance(cfg, dict) else {}
            mb = bool(dv.get("multi_batch", False)) if isinstance(dv, dict) else False
        scheduler = store.build_scheduler(cfg, concurrency_override=concurrency_override, multi_batch=bool(mb))
        runner_ids = _runner_ids_from_env_or_config(cfg)
        pool = store.build_runner_pool(cfg, use_fake_invoker=use_fake_invoker, runner_ids=runner_ids)
        scheduler.run_until_idle(ingested_batches=ingested, runner_pool=pool, batch_id_filter=batch_id_filter)

        for batch_id in store.list_batches():
            try:
                try:
                    if (store.paths.batch_dir(batch_id) / "batch_unreadable.json").exists():
                        continue
                except Exception:
                    pass
                store.write_batch_scoreboard(batch_id)
            except Exception:
                # Monitoring must never wedge the harness tick loop.
                continue
        try:
            store.write_system_scoreboard()
        except Exception:
            pass

    def compute_scoreboards(self, batch_id: str | None) -> dict[str, Any]:
        store = RunsStore(self._paths())
        if batch_id:
            return {"batch": compute_batch_scoreboard(store, batch_id=batch_id)}
        return {"system": compute_system_scoreboard(store)}

    def format_scoreboards(self, obj: dict[str, Any]) -> str:
        return format_scoreboards(obj)


def _runner_ids_from_env_or_config(cfg: dict[str, Any]) -> list[str]:
    env = os.environ.get("PARALLELHASSNES_RUNNER_IDS")
    if env:
        out = [x.strip() for x in env.split(",") if x.strip()]
        if out:
            return out
    pool = (cfg.get("defaults", {}) or {}).get("runner_pool")
    if isinstance(pool, dict):
        rids = pool.get("runner_ids")
        if isinstance(rids, list):
            out = [x for x in rids if isinstance(x, str) and x.strip()]
            if out:
                return out
    # Default: single local runner id.
    import platform

    return [f"runner_{platform.node()}_{platform.system()}".replace(" ", "_")]
