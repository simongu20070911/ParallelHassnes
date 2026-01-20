from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from parallelhassnes.core.file_lock import acquire_lock_blocking


@dataclass(frozen=True)
class WorkspaceResolution:
    workspace_policy: dict[str, Any]
    workspace_root: str
    job_workdir: str
    kind: str  # shared | isolated_git_worktree | isolated_copy
    source_workdir: str


def resolve_job_workdir(
    *,
    paths: Any,
    harness_cfg: dict[str, Any],
    batch: dict[str, Any],
    job: dict[str, Any],
) -> WorkspaceResolution:
    """
    Resolve the job working directory according to workspace policy.

    Spec requirement (11.1): resume affects conversation state, not filesystem rollback.
    So for resume steps, we always run against the current workspace state at execution time.
    """
    batch_id = batch["batch_id"]
    job_id = job["job_id"]

    # Base workdir from Launch Table.
    working_root = batch.get("working_root")
    if working_root:
        source_workdir = str((Path(working_root) / job["working_directory"]).resolve())
    else:
        source_workdir = str(Path(job["working_directory"]).resolve())

    # Policy precedence: job override -> batch effective defaults -> harness defaults.
    policy = (
        job.get("workspace_policy")
        or (batch.get("effective_defaults", {}) or {}).get("workspace_policy")
        or (harness_cfg.get("defaults", {}) or {}).get("workspace_policy")
        or {"mode": "shared"}
    )
    if not isinstance(policy, dict):
        policy = {"mode": "shared"}

    mode = str(policy.get("mode", "shared"))
    if mode != "isolated":
        return WorkspaceResolution(
            workspace_policy=policy,
            workspace_root=source_workdir,
            job_workdir=source_workdir,
            kind="shared",
            source_workdir=source_workdir,
        )

    # Isolated workspace is per job, persistent across steps within a batch.
    job_dir = paths.job_dir(batch_id, job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    workspace_root = job_dir / "_workspace"
    lock = acquire_lock_blocking(job_dir / "_workspace.lock")
    try:
        # Reuse if already initialized.
        if (workspace_root / ".git").exists():
            kind = "isolated_git_worktree"
            job_workdir = _map_to_workspace(source_workdir=Path(source_workdir), workspace_root=workspace_root)
            return WorkspaceResolution(
                workspace_policy=policy,
                workspace_root=str(workspace_root),
                job_workdir=str(job_workdir),
                kind=kind,
                source_workdir=source_workdir,
            )
        if workspace_root.exists() and any(workspace_root.iterdir()):
            kind = "isolated_copy"
            return WorkspaceResolution(
                workspace_policy=policy,
                workspace_root=str(workspace_root),
                job_workdir=str(workspace_root),
                kind=kind,
                source_workdir=source_workdir,
            )

        # Try git worktree when possible (preferred institutional behavior).
        src = Path(source_workdir)
        git_root = _find_git_root(src)
        if git_root is not None:
            _ensure_git_worktree(git_root=git_root, workspace_root=workspace_root)
            job_workdir = _map_to_workspace(source_workdir=src, workspace_root=workspace_root)
            return WorkspaceResolution(
                workspace_policy=policy,
                workspace_root=str(workspace_root),
                job_workdir=str(job_workdir),
                kind="isolated_git_worktree",
                source_workdir=source_workdir,
            )

        # Policy: avoid full-copy fallback unless explicitly allowed.
        #
        # Rationale: copying a non-git working directory per job is a predictable failure mode
        # (massive disk bloat, slow launches, and accidental lookahead via copied caches).
        allow_copy_fallback = policy.get("allow_isolated_copy_fallback", False)
        if allow_copy_fallback is not True:
            raise RuntimeError(
                "workspace_policy.mode='isolated' requires a git repo so we can use a git worktree. "
                f"No git root found for working_directory={source_workdir}. "
                "Fix: `git init` + initial commit (and .gitignore for data), or set workspace_policy.mode='shared'. "
                "If you *really* want full copies, set workspace_policy.allow_isolated_copy_fallback=true."
            )

        # Fallback: copy the directory (best-effort, operator-approved).
        workspace_root.mkdir(parents=True, exist_ok=True)
        exclude_names = policy.get("copy_exclude_names")
        if not isinstance(exclude_names, list):
            exclude_names = None
        _copy_dir_contents(src, workspace_root, exclude_names=exclude_names)
        return WorkspaceResolution(
            workspace_policy=policy,
            workspace_root=str(workspace_root),
            job_workdir=str(workspace_root),
            kind="isolated_copy",
            source_workdir=source_workdir,
        )
    finally:
        lock.release()


def _find_git_root(path: Path) -> Path | None:
    p = path if path.is_dir() else path.parent
    try:
        p = p.resolve()
    except Exception:
        return None
    for parent in [p, *p.parents]:
        if (parent / ".git").exists():
            return parent
    return None


def _ensure_git_worktree(*, git_root: Path, workspace_root: Path) -> None:
    workspace_root.parent.mkdir(parents=True, exist_ok=True)
    if workspace_root.exists() and any(workspace_root.iterdir()):
        # If it exists and is non-empty but not a worktree, do not overwrite.
        if not (workspace_root / ".git").exists():
            raise RuntimeError(f"isolated workspace exists but is not a git worktree: {workspace_root}")
        return
    workspace_root.mkdir(parents=True, exist_ok=True)

    # `git worktree add` requires an empty directory.
    if any(workspace_root.iterdir()):
        return

    head = subprocess.run(["git", "-C", str(git_root), "rev-parse", "HEAD"], capture_output=True, text=True)
    if head.returncode != 0:
        raise RuntimeError("failed to resolve git HEAD for worktree creation")
    rev = (head.stdout or "").strip() or "HEAD"

    # Avoid interactive prompts.
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    add = subprocess.run(["git", "-C", str(git_root), "worktree", "add", "--detach", str(workspace_root), rev], capture_output=True, text=True, env=env)
    if add.returncode != 0:
        raise RuntimeError(f"git worktree add failed: {(add.stderr or '').strip()[:500]}")


def _map_to_workspace(*, source_workdir: Path, workspace_root: Path) -> Path:
    git_root = _find_git_root(source_workdir)
    if git_root is None:
        return workspace_root
    try:
        rel = source_workdir.resolve().relative_to(git_root.resolve())
    except Exception:
        return workspace_root
    return (workspace_root / rel).resolve()


def _copy_dir_contents(src: Path, dst: Path, *, exclude_names: list[str] | None = None) -> None:
    if not src.exists():
        raise FileNotFoundError(str(src))
    if not src.is_dir():
        raise ValueError(f"source_workdir must be a directory for copy isolation: {src}")
    dst.mkdir(parents=True, exist_ok=True)
    exclude_set: set[str] = set()
    if exclude_names:
        exclude_set = {x.strip() for x in exclude_names if isinstance(x, str) and x.strip()}

    # Default excludes: these are frequently huge and almost never required for agent execution.
    # Keep this conservative; operators can always override with an explicit policy.
    exclude_set |= {
        ".DS_Store",
        "__pycache__",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".ipynb_checkpoints",
        ".venv",
        ".direnv",
        "node_modules",
    }
    for item in src.iterdir():
        if item.name in exclude_set:
            continue
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True, ignore=shutil.ignore_patterns(*sorted(exclude_set)))
        else:
            shutil.copy2(item, target)
