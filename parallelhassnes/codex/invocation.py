from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CodexInvocation:
    """
    Minimal invocation contract for spawning Codex.

    The current MVP invokes Codex directly inside Runner; this module exists to
    keep a stable place for codex-specific flags/env rules as the system grows.
    """

    args: list[str]
    env: dict[str, str]


def build_invocation(args: list[str], env: dict[str, str]) -> CodexInvocation:
    return CodexInvocation(args=list(args), env=dict(env))

