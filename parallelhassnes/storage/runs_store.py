from __future__ import annotations

import json
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import threading

from parallelhassnes.config.harness_config import HarnessConfig
from parallelhassnes.core.atomic_io import write_atomic_json, write_once_json
from parallelhassnes.core.paths import Paths
from parallelhassnes.core.safe_read import SafeReadError, SafeReadTimeout, read_json_via_cat
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.runner.runner import Runner, RunnerConfig
from parallelhassnes.scheduler.scheduler import Scheduler, SchedulerConfig
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard, compute_system_scoreboard
from parallelhassnes.validation.contracts import validate_current, validate_harness_config
from parallelhassnes.core.current_update import CurrentUpdate, update_current_atomic as _update_current_atomic_file


@dataclass(frozen=True)
class RunsStore:
    paths: Paths

    def _read_json_safely(self, path: Path) -> dict[str, Any]:
        """
        Read small JSON control files without risking an indefinite block.

        Default to a short timeout to keep harness ticks responsive. Operators can
        inspect the offending file manually if a timeout occurs.
        """
        # NOTE: We intentionally do not use Path.read_text here due to macOS/iCloud
        # placeholder/stub edge cases that can wedge a long-lived harness process.
        obj = read_json_via_cat(path, timeout_seconds=2.0)
        if not isinstance(obj, dict):
            raise ValueError(f"expected JSON object in {path}")
        return obj

    def read_harness_config(self) -> dict[str, Any]:
        p = self.paths.harness_config_path()
        cfg = self._read_json_safely(p)
        errs = validate_harness_config(cfg)
        if errs:
            raise ValueError("Invalid harness_config.json: " + "; ".join(errs))
        return cfg

    def write_batch_meta(self, batch_meta: dict[str, Any]) -> None:
        batch_id = batch_meta["batch_id"]
        write_once_json(self.paths.batch_meta_path(batch_id), batch_meta)

    def list_batches(self) -> list[str]:
        if not self.paths.runs_root.exists():
            return []
        out: list[str] = []
        for p in self.paths.runs_root.iterdir():
            if not p.is_dir():
                continue
            if p.name.startswith("_"):
                continue
            if (p / "batch_meta.json").exists():
                out.append(p.name)
        return sorted(out)

    def read_batch_meta(self, batch_id: str) -> dict[str, Any]:
        p = self.paths.batch_meta_path(batch_id)
        if not p.exists():
            raise FileNotFoundError(str(p))
        try:
            return self._read_json_safely(p)
        except (SafeReadTimeout, SafeReadError) as e:
            # Persist a marker so long-lived harness loops can skip quickly on subsequent ticks.
            try:
                marker = self.paths.batch_dir(batch_id) / "batch_unreadable.json"
                if not marker.exists():
                    write_atomic_json(
                        marker,
                        {
                            "kind": "batch_meta_unreadable",
                            "path": str(p),
                            "error": str(e),
                            "written_at": utc_isoformat(),
                        },
                    )
            except Exception:
                pass
            raise

    def job_ids_for_batch(self, batch_id: str) -> list[str]:
        batch_dir = self.paths.batch_dir(batch_id)
        if not batch_dir.exists():
            return []
        out: list[str] = []
        for p in batch_dir.iterdir():
            if p.is_dir() and p.name not in {"_system"} and p.name != "batch_meta.json":
                if (p / "current.json").exists() or (p / "steps").exists():
                    out.append(p.name)
        return sorted(out)

    def read_current(self, batch_id: str, job_id: str) -> dict[str, Any] | None:
        p = self.paths.current_path(batch_id, job_id)
        if not p.exists():
            return None
        try:
            return self._read_json_safely(p)
        except Exception:
            return None

    def write_current(self, batch_id: str, job_id: str, current: dict[str, Any]) -> None:
        errs = validate_current(current)
        if errs:
            raise ValueError("Invalid current.json: " + "; ".join(errs))
        # Serialize writes across multiple harness/runner processes to avoid clobber.
        info = CurrentUpdate(
            batch_id=batch_id,
            job_id=job_id,
            current_path=self.paths.current_path(batch_id, job_id),
            lock_path=self.paths.job_dir(batch_id, job_id) / "current.lock",
        )

        def apply(cur: dict[str, Any]) -> None:
            cur.clear()
            cur.update(current)

        _update_current_atomic_file(info, apply)

    def update_current_atomic(self, batch_id: str, job_id: str, update_fn: Any) -> dict[str, Any]:
        info = CurrentUpdate(
            batch_id=batch_id,
            job_id=job_id,
            current_path=self.paths.current_path(batch_id, job_id),
            lock_path=self.paths.job_dir(batch_id, job_id) / "current.lock",
        )
        return _update_current_atomic_file(info, update_fn)

    def resolve_attempt_dir(self, batch_id: str | None, job_id: str | None, step_id: str | None, run_id: str | None) -> Path:
        if not batch_id or not job_id or not step_id or not run_id:
            raise ValueError("batch_id, job_id, step_id, and run_id are required when attempt_dir is not provided")
        attempts_dir = self.paths.attempts_dir(batch_id, job_id, step_id)
        if not attempts_dir.exists():
            raise FileNotFoundError(f"attempts dir not found: {attempts_dir}")

        # Preferred: consult `current.json` as the job-level index (no directory scanning).
        cur = self.read_current(batch_id, job_id)
        if isinstance(cur, dict):
            step_ent = ((cur.get("steps") or {}).get(step_id) or {})
            if isinstance(step_ent, dict):
                by = step_ent.get("by_run_id")
                if isinstance(by, dict):
                    ent = by.get(run_id)
                    if isinstance(ent, dict):
                        ad = ent.get("attempt_dir")
                        if isinstance(ad, str) and ad:
                            p = Path(ad)
                            if p.exists() and p.is_dir():
                                return p.resolve()
                # Fallback pointers (still no scanning).
                for key in ("latest", "latest_successful"):
                    ent = step_ent.get(key)
                    if isinstance(ent, dict) and ent.get("run_id") == run_id:
                        ad = ent.get("attempt_dir")
                        if isinstance(ad, str) and ad:
                            p = Path(ad)
                            if p.exists() and p.is_dir():
                                return p.resolve()

        # Preferred: direct directory name = run_id (per spec option A).
        direct = attempts_dir / run_id
        if direct.exists() and direct.is_dir():
            return direct.resolve()

        # Fallback: discover attempt directory by name convention (no run-file content reads).
        suffix = "_" + run_id
        candidates: list[Path] = []
        for p in attempts_dir.iterdir():
            if p.is_dir() and p.name.endswith(suffix):
                candidates.append(p)
        if len(candidates) == 1:
            return candidates[0].resolve()
        if len(candidates) > 1:
            names = ", ".join(sorted(p.name for p in candidates)[:5])
            raise RuntimeError(f"multiple attempt dirs match run_id={run_id} under {attempts_dir}: {names}")

        raise FileNotFoundError(f"attempt dir not found for run_id={run_id} under {attempts_dir}")

    def overrides_path(self, batch_id: str, job_id: str) -> Path:
        return self.paths.job_dir(batch_id, job_id) / "overrides.json"

    def read_overrides(self, batch_id: str, job_id: str) -> dict[str, Any]:
        from parallelhassnes.ops.overrides import load_overrides

        return load_overrides(self.overrides_path(batch_id, job_id))

    def read_attempt_state(self, attempt_dir: Path) -> dict[str, Any] | None:
        p = self.paths.attempt_state_path(attempt_dir)
        if not p.exists():
            return None
        try:
            return self._read_json_safely(p)
        except Exception:
            return None

    def read_attempt_meta(self, attempt_dir: Path) -> dict[str, Any] | None:
        p = self.paths.attempt_meta_path(attempt_dir)
        if not p.exists():
            return None
        try:
            return self._read_json_safely(p)
        except Exception:
            return None

    def build_scheduler(self, cfg: dict[str, Any], concurrency_override: int | None, *, multi_batch: bool = False) -> Scheduler:
        scfg = SchedulerConfig(
            concurrency_override=concurrency_override,
            multi_batch=bool(multi_batch),
        )
        return Scheduler(store=self, cfg=scfg, harness_cfg=cfg)

    def build_runner(self, cfg: dict[str, Any], use_fake_invoker: bool) -> Runner:
        runner_id = f"runner_{platform.node()}_{platform.system()}".replace(" ", "_")
        rcfg = RunnerConfig(
            runner_id=runner_id,
            use_fake_invoker=use_fake_invoker,
            baseline_output_schema_path=str((self.paths.runs_root.parent / "run_report.schema.json").resolve()),
        )
        return Runner(store=self, cfg=rcfg, harness_cfg=cfg)

    def build_runner_pool(self, cfg: dict[str, Any], use_fake_invoker: bool, runner_ids: list[str]) -> Any:
        from parallelhassnes.runner.pool import RunnerHandle, RunnerPool

        cap = int((cfg.get("defaults", {}) or {}).get("runner_capacity", 1))
        if cap <= 0:
            cap = 1
        handles: list[RunnerHandle] = []
        for rid in runner_ids:
            # Drain mode: runner may be marked as not accepting new work.
            eff_cap = cap
            drain_path = self.paths.runners_root / rid / "drain.json"
            if drain_path.exists():
                try:
                    raw = json.loads(drain_path.read_text(encoding="utf-8"))
                    drain = bool(raw.get("drain", False)) if isinstance(raw, dict) else True
                except Exception:
                    drain = True
                if drain:
                    eff_cap = 0
            rcfg = RunnerConfig(
                runner_id=rid,
                use_fake_invoker=use_fake_invoker,
                baseline_output_schema_path=str((self.paths.runs_root.parent / "run_report.schema.json").resolve()),
            )
            runner = Runner(store=self, cfg=rcfg, harness_cfg=cfg)
            handles.append(RunnerHandle(runner_id=rid, runner=runner, capacity=eff_cap, semaphore=threading.Semaphore(eff_cap)))
        return RunnerPool(handles)

    def write_runner_health(self, runner_id: str, health: dict[str, Any]) -> None:
        write_atomic_json(self.paths.runner_health_path(runner_id), health)

    def write_system_scoreboard(self) -> None:
        out = compute_system_scoreboard(self)
        path = self.paths.runs_root / "_system" / "scoreboard.system.json"
        write_atomic_json(path, out)

    def write_batch_scoreboard(self, batch_id: str) -> None:
        out = compute_batch_scoreboard(self, batch_id=batch_id)
        path = self.paths.batch_dir(batch_id) / "scoreboard.batch.json"
        write_atomic_json(path, out)
