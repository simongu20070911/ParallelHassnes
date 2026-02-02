from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Any

try:  # FastAPI/Starlette are optional dependencies; keep import-time safe.
    from starlette.requests import Request
except Exception:  # pragma: no cover
    Request = Any  # type: ignore[misc,assignment]

from parallelhassnes.config.harness_config import HarnessConfig, write_harness_config_snapshots
from parallelhassnes.core.atomic_io import write_atomic_json
from parallelhassnes.core.paths import Paths
from parallelhassnes.harness.harness import Harness
from parallelhassnes.interfaces.fs_queue import FsQueue
from parallelhassnes.ops.cancel import cancel_attempt
from parallelhassnes.ops.overrides import set_force_retry, set_forced_terminal, set_requeue
from parallelhassnes.scoreboard.scoreboards import compute_batch_scoreboard, compute_system_scoreboard
from parallelhassnes.storage.runs_store import RunsStore
from parallelhassnes.runtime.tick_loop import TickLoop, TickLoopConfig

from parallelhassnes.api.auth import AuthConfig, require_api_auth


def create_app(
    *,
    root: Path,
    runs_root: Path | None = None,
    runners_root: Path | None = None,
    queue_root: Path | None = None,
) -> Any:
    """
    FastAPI “API mode” façade for the filesystem harness.

    This is intentionally thin: each operation maps to the same underlying
    filesystem artifacts described in FUNCTIONALITY_SPEC.md.

    Note: FastAPI/uvicorn are optional dependencies; this function imports
    FastAPI only when called.
    """
    try:
        from fastapi import Body, FastAPI, HTTPException, Response
        from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
    except Exception as e:  # pragma: no cover
        raise RuntimeError("FastAPI is not installed. Install with: pip install -e .[api]") from e

    root = root.resolve()
    runs_root = (runs_root or (root / "runs")).resolve()
    runners_root = (runners_root or (root / "runners")).resolve()
    queue_root = (queue_root or (runs_root / "_queue")).resolve()

    paths = Paths(runs_root=runs_root, runners_root=runners_root)
    store = RunsStore(paths)
    harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
    tick_loop: TickLoop | None = None
    tick_thread: threading.Thread | None = None

    # Ensure baseline harness config exists (filesystem MVP behavior).
    cfg_path = paths.harness_config_path()
    if not cfg_path.exists():
        cfg = HarnessConfig.default(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
        write_harness_config_snapshots(cfg)

    app = FastAPI(title="ParallelHassnes API (filesystem façade)", version="0.1.0")

    def _read_cfg() -> dict[str, Any]:
        return store.read_harness_config()

    def _auth_from_cfg(cfg: dict[str, Any]) -> AuthConfig:
        api = ((cfg.get("interfaces") or {}).get("api_mode") or {}) if isinstance(cfg, dict) else {}
        enabled = bool(api.get("enabled", False))
        auth_mode = str(api.get("auth_mode", "none"))
        return AuthConfig(enabled=enabled, auth_mode=auth_mode)

    def _tick_cfg_from_cfg(cfg: dict[str, Any]) -> tuple[bool, float]:
        api = ((cfg.get("interfaces") or {}).get("api_mode") or {}) if isinstance(cfg, dict) else {}
        enabled = bool(api.get("enabled", False))
        # API mode implies a long-lived harness; default to auto-ticking when enabled.
        auto_tick = bool(api.get("auto_tick", True))
        interval = api.get("tick_interval_seconds", 0.5)
        try:
            interval_f = float(interval)
        except Exception:
            interval_f = 0.5
        if interval_f < 0.05:
            interval_f = 0.05
        return (enabled and auto_tick), interval_f

    def _multi_batch_from_cfg(cfg: dict[str, Any]) -> bool:
        dv = ((cfg.get("defaults", {}) or {}).get("scheduler") or {}) if isinstance(cfg, dict) else {}
        return bool(dv.get("multi_batch", False)) if isinstance(dv, dict) else False

    def _enforce(request: Request) -> None:
        cfg = _read_cfg()
        auth = _auth_from_cfg(cfg)
        try:
            require_api_auth(auth, client_host=getattr(getattr(request, "client", None), "host", None), authorization=request.headers.get("authorization"))
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=str(e))

    def _resolve_attempt_dir(
        *,
        attempt_dir: str | None,
        batch_id: str | None,
        job_id: str | None,
        step_id: str | None,
        run_id: str | None,
    ) -> Path:
        if attempt_dir:
            return Path(attempt_dir).resolve()
        return store.resolve_attempt_dir(batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True}

    @app.on_event("startup")
    def _startup() -> None:  # pragma: no cover
        nonlocal tick_loop, tick_thread
        cfg = _read_cfg()
        enabled, interval = _tick_cfg_from_cfg(cfg)
        if not enabled:
            return
        # Use the same lock for background and on-demand tick calls.
        tick_loop = TickLoop(
            harness,
            TickLoopConfig(interval_seconds=interval, concurrency_override=None, use_fake_invoker=False, multi_batch=_multi_batch_from_cfg(cfg)),
        )
        tick_thread = threading.Thread(target=lambda: tick_loop.run_forever(on_error=None), daemon=True)
        tick_thread.start()

    @app.on_event("shutdown")
    def _shutdown() -> None:  # pragma: no cover
        nonlocal tick_loop, tick_thread
        if tick_loop is not None:
            tick_loop.stop()
        if tick_thread is not None:
            tick_thread.join(timeout=1)

    @app.post("/v1/tick")
    def tick(request: Request, concurrency: int | None = None, no_codex: bool = False, multi_batch: bool | None = None) -> dict[str, Any]:
        _enforce(request)
        # Share the same tick serialization mutex when auto-tick is enabled.
        if tick_loop is not None:
            tick_loop.tick_once_with_overrides(concurrency_override=concurrency, use_fake_invoker=bool(no_codex), multi_batch=multi_batch)
        else:
            harness.tick_once(concurrency_override=concurrency, use_fake_invoker=bool(no_codex), multi_batch=multi_batch)
        return {"ok": True}

    @app.post("/v1/batches")
    def submit_batch(request: Request, launch_table: dict[str, Any] = Body(...)) -> dict[str, Any]:
        _enforce(request)
        payload = json.dumps(launch_table, ensure_ascii=False, sort_keys=True).encode("utf-8")
        sha = hashlib.sha256(payload).hexdigest()
        name = f"launch_table_{sha[:12]}.json"
        incoming = queue_root / "incoming"
        incoming.mkdir(parents=True, exist_ok=True)
        p = incoming / name
        if not p.exists():
            p.write_bytes(payload)

        queue = FsQueue(queue_root)
        batch_id = queue._ingest_one(store, p)  # type: ignore[attr-defined]
        # Mirror ingest_all() behavior: move source into processed/ for a stable audit trail.
        processed = queue.processed_dir()
        processed.mkdir(parents=True, exist_ok=True)
        if p.exists():
            p.rename(processed / p.name)

        ack = processed / f"{Path(name).stem}.ack.json"
        accepted_job_ids: list[str] = []
        if ack.exists():
            try:
                obj = json.loads(ack.read_text(encoding="utf-8"))
                accepted_job_ids = obj.get("accepted_job_ids") or []
            except Exception:
                accepted_job_ids = []

        return {"batch_id": batch_id, "accepted_job_ids": accepted_job_ids}

    @app.get("/v1/scoreboards/system")
    def scoreboard_system(request: Request) -> Any:
        _enforce(request)
        out = compute_system_scoreboard(store)
        # Best-effort: keep filesystem scoreboard artifacts fresh for operators who read
        # runs/_system/scoreboard.system.json directly while long-running batches are active.
        try:
            write_atomic_json(paths.runs_root / "_system" / "scoreboard.system.json", out)
        except Exception:
            pass
        return JSONResponse(out)

    @app.get("/v1/batches/{batch_id}/scoreboard")
    def scoreboard_batch(request: Request, batch_id: str) -> Any:
        _enforce(request)
        out = compute_batch_scoreboard(store, batch_id=batch_id)
        # Best-effort: keep runs/<batch_id>/scoreboard.batch.json fresh for filesystem operators.
        try:
            write_atomic_json(paths.batch_dir(batch_id) / "scoreboard.batch.json", out)
        except Exception:
            pass
        return JSONResponse(out)

    @app.get("/v1/batches/{batch_id}/jobs/{job_id}/current")
    def get_current(request: Request, batch_id: str, job_id: str) -> Any:
        _enforce(request)
        cur = store.read_current(batch_id, job_id)
        if cur is None:
            raise HTTPException(status_code=404, detail="current.json not found")
        return JSONResponse(cur)

    @app.get("/v1/attempts/resolve")
    def resolve_attempt(
        request: Request,
        batch_id: str,
        job_id: str,
        step_id: str,
        run_id: str,
    ) -> Any:
        _enforce(request)
        d = store.resolve_attempt_dir(batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
        return {"attempt_dir": str(d)}

    @app.get("/v1/attempts/artifact")
    def get_artifact(
        request: Request,
        name: str,
        attempt_dir: str | None = None,
        batch_id: str | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
        run_id: str | None = None,
    ) -> Any:
        _enforce(request)
        ad = _resolve_attempt_dir(attempt_dir=attempt_dir, batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
        p = (ad / name).resolve()
        if not p.exists():
            raise HTTPException(status_code=404, detail="artifact not found")
        if p.suffix in {".json"}:
            return JSONResponse(json.loads(p.read_text(encoding="utf-8")))
        return Response(content=p.read_bytes(), media_type="application/octet-stream")

    @app.get("/v1/attempts/events")
    def tail_events(
        request: Request,
        attempt_dir: str | None = None,
        batch_id: str | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
        run_id: str | None = None,
        lines: int = 50,
        follow: bool = False,
    ) -> Any:
        _enforce(request)
        ad = _resolve_attempt_dir(attempt_dir=attempt_dir, batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
        p = (ad / "codex.events.jsonl").resolve()
        if not p.exists():
            gz = p.with_suffix(p.suffix + ".gz")
            if gz.exists():
                p = gz
            else:
                raise HTTPException(status_code=404, detail="codex.events.jsonl not found")

        from parallelhassnes.util.tail import tail_last_lines

        if not follow:
            return PlainTextResponse("".join(tail_last_lines(p, n=int(lines))))

        if p.name.endswith(".gz"):
            raise HTTPException(status_code=400, detail="follow is not supported for .gz files")

        def gen() -> Any:
            for line in tail_last_lines(p, n=int(lines)):
                yield line
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        yield line
                    else:
                        import time

                        time.sleep(0.2)

        return StreamingResponse(gen(), media_type="text/plain")

    @app.post("/v1/attempts/cancel")
    def cancel(
        request: Request,
        grace_seconds: float = 3.0,
        attempt_dir: str | None = None,
        batch_id: str | None = None,
        job_id: str | None = None,
        step_id: str | None = None,
        run_id: str | None = None,
    ) -> Any:
        _enforce(request)
        ad = _resolve_attempt_dir(attempt_dir=attempt_dir, batch_id=batch_id, job_id=job_id, step_id=step_id, run_id=run_id)
        return JSONResponse(cancel_attempt(ad, grace_seconds=float(grace_seconds)))

    @app.post("/v1/batches/{batch_id}/jobs/{job_id}/steps/{step_id}/force-retry")
    def force_retry(request: Request, batch_id: str, job_id: str, step_id: str) -> Any:
        _enforce(request)
        path = store.overrides_path(batch_id, job_id)
        obj = set_force_retry(path, step_id)
        return JSONResponse({"ok": True, "overrides_path": str(path), "overrides": obj})

    @app.post("/v1/batches/{batch_id}/jobs/{job_id}/steps/{step_id}/mark-failed")
    def mark_failed(request: Request, batch_id: str, job_id: str, step_id: str, reason: str | None = None) -> Any:
        _enforce(request)
        path = store.overrides_path(batch_id, job_id)
        obj = set_forced_terminal(path, step_id, status="failed", reason=reason)
        return JSONResponse({"ok": True, "overrides_path": str(path), "overrides": obj})

    @app.post("/v1/batches/{batch_id}/jobs/{job_id}/steps/{step_id}/requeue")
    def requeue(
        request: Request,
        batch_id: str,
        job_id: str,
        step_id: str,
        runner_id: str | None = None,
        different: bool = False,
        reason: str | None = None,
    ) -> Any:
        _enforce(request)
        path = store.overrides_path(batch_id, job_id)
        obj = set_requeue(path, step_id, target_runner_id=runner_id, different_from_last=bool(different), reason=reason)
        return JSONResponse({"ok": True, "overrides_path": str(path), "overrides": obj})

    return app
