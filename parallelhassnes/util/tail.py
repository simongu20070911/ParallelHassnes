from __future__ import annotations

import os
import time
from collections import deque
from pathlib import Path
from typing import Iterable, TextIO


def tail_last_lines(path: Path, n: int) -> list[str]:
    if n <= 0:
        return []
    dq: deque[str] = deque(maxlen=n)
    if path.name.endswith(".gz"):
        import gzip

        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                dq.append(line)
    else:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                dq.append(line)
    return list(dq)


def tail_follow(path: Path, out: TextIO, last_lines: int = 50, poll_seconds: float = 0.2) -> None:
    """
    Minimal tail -f implementation (read text lines, no partial-line streaming).
    """
    if path.name.endswith(".gz"):
        raise ValueError("tail_follow does not support .gz files")
    for line in tail_last_lines(path, n=last_lines):
        out.write(line)
    out.flush()

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if line:
                out.write(line)
                out.flush()
                continue
            time.sleep(poll_seconds)
