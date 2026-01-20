from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AuthConfig:
    enabled: bool
    auth_mode: str  # none|local_trust|token


def require_api_auth(auth: AuthConfig, client_host: str | None, authorization: str | None) -> None:
    """
    Enforce API auth as defined by FUNCTIONALITY_SPEC.md §12.2 (conceptual).

    Secrets are not stored in runs/; token mode reads the expected bearer token
    from environment variables on the server host.
    """
    if not auth.enabled:
        raise PermissionError("api_mode is disabled")

    mode = (auth.auth_mode or "none").strip()
    if mode == "none":
        return

    if mode == "local_trust":
        host = (client_host or "").strip()
        if host in {"127.0.0.1", "::1"}:
            return
        raise PermissionError("local_trust requires localhost client")

    if mode == "token":
        expected = os.environ.get("PARALLELHASSNES_API_TOKEN")
        if not expected:
            raise PermissionError("token auth enabled but PARALLELHASSNES_API_TOKEN is not set on server")
        hdr = (authorization or "").strip()
        if hdr.lower().startswith("bearer "):
            got = hdr[7:].strip()
            if got == expected:
                return
        raise PermissionError("invalid bearer token")

    raise PermissionError(f"unknown auth_mode: {mode}")

