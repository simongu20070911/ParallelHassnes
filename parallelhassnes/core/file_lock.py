from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any


@dataclass(frozen=True)
class FileLock:
    path: Path
    handle: IO[str]

    def release(self) -> None:
        try:
            self.handle.close()
        except Exception:
            pass


def try_acquire_lock(path: Path) -> FileLock | None:
    """
    Try to acquire an exclusive non-blocking lock on `path`.

    Holds the lock for as long as the returned FileLock is kept alive (file handle open).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, "a+", encoding="utf-8")
    try:
        import fcntl

        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return FileLock(path=path, handle=f)
    except Exception:
        try:
            f.close()
        except Exception:
            pass
        return None


def acquire_lock_blocking(path: Path) -> FileLock:
    """
    Acquire an exclusive lock on `path`, blocking until it becomes available.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    f = open(path, "a+", encoding="utf-8")
    import fcntl

    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
    return FileLock(path=path, handle=f)

