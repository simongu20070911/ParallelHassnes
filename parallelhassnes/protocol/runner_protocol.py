from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RunnerProtocolVersion:
    """
    Placeholder for a future runner<->harness protocol boundary.

    MVP runs runners in-process, but the plan calls out a stable module location
    for when runners become out-of-process or remote.
    """

    major: int = 1
    minor: int = 0

