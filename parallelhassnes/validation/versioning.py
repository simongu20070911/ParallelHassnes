from __future__ import annotations


def parse_semver_major(version: str) -> int | None:
    """
    Parse a semver-ish string and return major version.
    Accepts forms like "1", "1.0", "1.0.0". Returns None if unparseable.
    """
    if not isinstance(version, str):
        return None
    v = version.strip()
    if not v:
        return None
    head = v.split(".", 1)[0]
    try:
        return int(head)
    except Exception:
        return None

