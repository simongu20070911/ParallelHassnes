from __future__ import annotations

from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_isoformat() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


def utc_compact_timestamp() -> str:
    return now_utc().strftime("%Y%m%dT%H%M%SZ")


def parse_iso8601_z(ts: str) -> datetime:
    if ts.endswith("Z"):
        return datetime.fromisoformat(ts[:-1] + "+00:00")
    return datetime.fromisoformat(ts)

