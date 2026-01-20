from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ApiInterfaceConfig:
    """
    Placeholder for a future HTTP API mode.

    MVP is filesystem-first (FsQueue). This module exists so API mode can be
    added without reshaping package structure.
    """

    enabled: bool = False

