from __future__ import annotations

from pathlib import Path

from parallelhassnes.api.app import create_app


def run_api_server(
    *,
    root: Path,
    runs_root: Path | None,
    runners_root: Path | None,
    queue_root: Path | None,
    host: str,
    port: int,
) -> None:
    try:
        import uvicorn
    except Exception as e:  # pragma: no cover
        raise RuntimeError("uvicorn is not installed. Install with: pip install -e .[api]") from e

    app = create_app(root=root, runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
    uvicorn.run(app, host=host, port=int(port), log_level="info")

