from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any


class SafeReadTimeout(RuntimeError):
    pass


class SafeReadError(RuntimeError):
    pass


def read_bytes_via_cat(path: Path, *, timeout_seconds: float) -> bytes:
    """
    Read file bytes via a subprocess with a timeout.

    Why this exists:
    - On some macOS setups (notably iCloud-synced Desktop/Documents), direct Python reads can
      block indefinitely on certain placeholder/stub states.
    - A subprocess with a timeout can be killed, preventing the harness tick loop from wedging.
    """
    try:
        proc = subprocess.run(
            ["/bin/cat", str(path)],
            capture_output=True,
            timeout=float(timeout_seconds),
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise SafeReadTimeout(f"timeout reading {path} after {timeout_seconds}s") from e
    except Exception as e:
        raise SafeReadError(f"failed to read {path}: {e}") from e

    if proc.returncode != 0:
        stderr = (proc.stderr or b"")[:200].decode("utf-8", errors="replace")
        raise SafeReadError(f"cat failed for {path} rc={proc.returncode} stderr={stderr!r}")
    return proc.stdout or b""


def read_text_via_cat(path: Path, *, timeout_seconds: float, encoding: str = "utf-8") -> str:
    return read_bytes_via_cat(path, timeout_seconds=timeout_seconds).decode(encoding, errors="strict")


def read_json_via_cat(path: Path, *, timeout_seconds: float, encoding: str = "utf-8") -> Any:
    import json

    return json.loads(read_text_via_cat(path, timeout_seconds=timeout_seconds, encoding=encoding))

