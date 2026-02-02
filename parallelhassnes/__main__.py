from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from parallelhassnes.config.harness_config import (
    HarnessConfig,
    write_harness_config_snapshots,
)
from parallelhassnes.core.time import utc_isoformat
from parallelhassnes.harness.harness import Harness


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="parallelhassnes")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Initialize runs/ and write harness_config.json snapshots")
    p_init.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_init.add_argument(
        "--runs-root",
        default=None,
        help="Runs store root (default: <root>/runs)",
    )
    p_init.add_argument(
        "--runners-root",
        default=None,
        help="Runners root (default: <root>/runners)",
    )
    p_init.add_argument(
        "--harness-config-version",
        default=None,
        help="Optional explicit harness_config_version (default: generated)",
    )

    p_once = sub.add_parser("tick", help="One-shot: ingest queue, schedule, run until idle, write scoreboards")
    p_once.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_once.add_argument("--runs-root", default=None)
    p_once.add_argument("--runners-root", default=None)
    p_once.add_argument(
        "--queue-root",
        default=None,
        help="Filesystem queue root (default: <runs_root>/_queue)",
    )
    p_once.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Override batch-level concurrency cap (optional)",
    )
    p_once.add_argument(
        "--no-codex",
        action="store_true",
        help="Do not spawn Codex; use a deterministic fake invoker (for local dry runs/tests)",
    )
    p_once.add_argument(
        "--batch-id",
        default=None,
        help="If provided, run scheduling only for this batch_id (still ingests filesystem queue first).",
    )
    p_once.add_argument(
        "--multi-batch",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Interleave runnable steps across batches (default: from harness_config.json).",
    )

    p_sb = sub.add_parser("scoreboard", help="Compute scoreboards from on-disk state")
    p_sb.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_sb.add_argument("--runs-root", default=None)
    p_sb.add_argument("--batch-id", default=None, help="If provided, compute per-batch view only")
    p_sb.add_argument(
        "--write",
        action="store_true",
        help="Write scoreboard JSON files to disk (updates runs/_system/scoreboard.system.json and/or runs/<batch_id>/scoreboard.batch.json).",
    )
    p_sb.add_argument("--json", action="store_true", help="Emit JSON to stdout")

    p_submit = sub.add_parser("submit", help="Submit a Launch Table file into the filesystem queue")
    p_submit.add_argument("launch_table_path", help="Path to Launch Table JSON")
    p_submit.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_submit.add_argument("--runs-root", default=None)
    p_submit.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip workspace safety preflight (not recommended).",
    )

    p_art = sub.add_parser("artifact", help="Fetch a run attempt artifact by name")
    p_art.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_art.add_argument("--runs-root", default=None)
    p_art.add_argument("--attempt-dir", default=None, help="Explicit attempt directory path")
    p_art.add_argument("--batch-id", default=None)
    p_art.add_argument("--job-id", default=None)
    p_art.add_argument("--step-id", default=None)
    p_art.add_argument("--run-id", default=None)
    p_art.add_argument("--name", required=True, help="Artifact filename (e.g., final.json, meta.json, codex.events.jsonl)")
    p_art.add_argument("--out", default=None, help="Write artifact bytes to this path (default: stdout)")

    p_tail = sub.add_parser("tail-events", help="Tail codex.events.jsonl for an attempt (when enabled)")
    p_tail.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_tail.add_argument("--runs-root", default=None)
    p_tail.add_argument("--attempt-dir", default=None, help="Explicit attempt directory path")
    p_tail.add_argument("--batch-id", default=None)
    p_tail.add_argument("--job-id", default=None)
    p_tail.add_argument("--step-id", default=None)
    p_tail.add_argument("--run-id", default=None)
    p_tail.add_argument("--lines", type=int, default=50, help="Number of last lines to print")
    p_tail.add_argument("--follow", action="store_true", help="Follow (like tail -f)")

    p_cancel = sub.add_parser("cancel", help="Cancel a running attempt (best-effort)")
    p_cancel.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_cancel.add_argument("--runs-root", default=None)
    p_cancel.add_argument("--attempt-dir", default=None, help="Explicit attempt directory path")
    p_cancel.add_argument("--batch-id", default=None)
    p_cancel.add_argument("--job-id", default=None)
    p_cancel.add_argument("--step-id", default=None)
    p_cancel.add_argument("--run-id", default=None)
    p_cancel.add_argument("--grace-seconds", type=float, default=3.0)

    p_close = sub.add_parser("close-batch", help="Mark a batch as closed/finalized (enables more aggressive GC)")
    p_close.add_argument("batch_id")
    p_close.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_close.add_argument("--runs-root", default=None)
    p_close.add_argument("--reason", default=None)

    p_gc = sub.add_parser("gc", help="Run garbage collection (retention) over runs store")
    p_gc.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_gc.add_argument("--runs-root", default=None)
    p_gc.add_argument("--dry-run", action="store_true")

    p_fr = sub.add_parser("force-retry", help="Force retry a step regardless of retry policy")
    p_fr.add_argument("batch_id")
    p_fr.add_argument("job_id")
    p_fr.add_argument("step_id")
    p_fr.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_fr.add_argument("--runs-root", default=None)

    p_mf = sub.add_parser("mark-failed", help="Mark a step as failed permanently (scheduler will stop retrying it)")
    p_mf.add_argument("batch_id")
    p_mf.add_argument("job_id")
    p_mf.add_argument("step_id")
    p_mf.add_argument("--reason", default=None)
    p_mf.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_mf.add_argument("--runs-root", default=None)

    p_rq = sub.add_parser("requeue", help="Requeue a step on same/different runner")
    p_rq.add_argument("batch_id")
    p_rq.add_argument("job_id")
    p_rq.add_argument("step_id")
    p_rq.add_argument("--runner-id", default=None, help="If provided, target this runner_id")
    p_rq.add_argument("--different", action="store_true", help="Try to schedule on a different runner than the last attempt")
    p_rq.add_argument("--reason", default=None)
    p_rq.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_rq.add_argument("--runs-root", default=None)

    p_lint = sub.add_parser("lint-terminology", help="Docs-level terminology drift check (no ambiguous phrases)")
    p_lint.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_lint.add_argument("--json", action="store_true", help="Emit JSON to stdout")

    p_api = sub.add_parser("api", help="Run API mode server (FastAPI; optional dependency)")
    p_api.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_api.add_argument("--runs-root", default=None)
    p_api.add_argument("--runners-root", default=None)
    p_api.add_argument("--queue-root", default=None)
    p_api.add_argument("--host", default="127.0.0.1")
    p_api.add_argument("--port", type=int, default=8000)

    p_serve = sub.add_parser("serve", help="Run the harness in a long-lived tick loop (filesystem-first)")
    p_serve.add_argument("--root", default=str(Path.cwd()), help="Project root (default: cwd)")
    p_serve.add_argument("--runs-root", default=None)
    p_serve.add_argument("--runners-root", default=None)
    p_serve.add_argument("--queue-root", default=None)
    p_serve.add_argument(
        "--interval-seconds",
        type=float,
        default=0.5,
        help="Tick loop interval in seconds (default: 0.5)",
    )
    p_serve.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Override batch-level concurrency cap (optional)",
    )
    p_serve.add_argument(
        "--no-codex",
        action="store_true",
        help="Do not spawn Codex; use a deterministic fake invoker (for local dry runs/tests)",
    )
    p_serve.add_argument(
        "--multi-batch",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Interleave runnable steps across batches (default: from harness_config.json).",
    )

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    args = _parse_args(argv)

    root = Path(args.root).resolve()
    runs_root = Path(getattr(args, "runs_root", None)).resolve() if getattr(args, "runs_root", None) else (root / "runs")
    runners_root = Path(args.runners_root).resolve() if getattr(args, "runners_root", None) else (root / "runners")

    if args.cmd == "init":
        cfg = HarnessConfig.default(
            runs_root=runs_root,
            runners_root=runners_root,
            harness_config_version=args.harness_config_version,
        )
        write_harness_config_snapshots(cfg)
        print(json.dumps({"ok": True, "runs_root": str(runs_root), "runners_root": str(runners_root)}))
        return 0

    if args.cmd == "submit":
        lt_path = Path(args.launch_table_path).resolve()
        if not getattr(args, "skip_preflight", False):
            _preflight_launch_table_workspaces(lt_path, runs_root=runs_root)
        queue_dir = runs_root / "_queue" / "incoming"
        queue_dir.mkdir(parents=True, exist_ok=True)
        dest = queue_dir / lt_path.name
        if dest.exists():
            raise SystemExit(f"Launch Table already exists in queue: {dest}")
        dest.write_bytes(lt_path.read_bytes())
        print(json.dumps({"ok": True, "queued_path": str(dest)}))
        return 0

    if args.cmd == "lint-terminology":
        from parallelhassnes.lint.terminology import lint_no_synonym_drift

        errs = lint_no_synonym_drift(root)
        out = {"ok": not errs, "errors": errs}
        if args.json:
            print(json.dumps(out, ensure_ascii=False))
        else:
            if errs:
                for e in errs:
                    print(e)
            else:
                print("OK")
        return 0 if not errs else 2

    if args.cmd == "api":
        from parallelhassnes.api.server import run_api_server

        run_api_server(
            root=root,
            runs_root=Path(getattr(args, "runs_root", None)).resolve() if getattr(args, "runs_root", None) else None,
            runners_root=Path(getattr(args, "runners_root", None)).resolve() if getattr(args, "runners_root", None) else None,
            queue_root=Path(getattr(args, "queue_root", None)).resolve() if getattr(args, "queue_root", None) else None,
            host=str(getattr(args, "host", "127.0.0.1")),
            port=int(getattr(args, "port", 8000)),
        )
        return 0

    if args.cmd == "serve":
        from parallelhassnes.runtime.tick_loop import TickLoop, TickLoopConfig

        harness = Harness(
            runs_root=runs_root,
            runners_root=runners_root,
            queue_root=Path(getattr(args, "queue_root", None)).resolve()
            if getattr(args, "queue_root", None)
            else (runs_root / "_queue"),
        )
        loop = TickLoop(
            harness,
            TickLoopConfig(
                interval_seconds=float(getattr(args, "interval_seconds", 0.5)),
                concurrency_override=int(getattr(args, "concurrency")) if getattr(args, "concurrency", None) is not None else None,
                use_fake_invoker=bool(getattr(args, "no_codex", False)),
                multi_batch=getattr(args, "multi_batch", None),
            ),
        )
        try:
            loop.run_forever(on_error=lambda e: print(f"[serve] tick error: {e}", file=sys.stderr))
        except KeyboardInterrupt:
            return 0
        return 0

    harness = Harness(
        runs_root=runs_root,
        runners_root=runners_root,
        queue_root=Path(getattr(args, "queue_root", None)).resolve()
        if getattr(args, "queue_root", None)
        else (runs_root / "_queue"),
    )

    if args.cmd == "artifact":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        if args.attempt_dir:
            attempt_dir = Path(args.attempt_dir).resolve()
        else:
            attempt_dir = rs.resolve_attempt_dir(batch_id=args.batch_id, job_id=args.job_id, step_id=args.step_id, run_id=args.run_id)
        artifact_path = (attempt_dir / args.name).resolve()
        if not artifact_path.exists():
            raise SystemExit(f"artifact not found: {artifact_path}")
        if args.out:
            Path(args.out).write_bytes(artifact_path.read_bytes())
        else:
            sys.stdout.buffer.write(artifact_path.read_bytes())
        return 0

    if args.cmd == "tail-events":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.storage.runs_store import RunsStore
        from parallelhassnes.util.tail import tail_follow, tail_last_lines

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        if args.attempt_dir:
            attempt_dir = Path(args.attempt_dir).resolve()
        else:
            attempt_dir = rs.resolve_attempt_dir(batch_id=args.batch_id, job_id=args.job_id, step_id=args.step_id, run_id=args.run_id)
        events_path = attempt_dir / "codex.events.jsonl"
        if not events_path.exists():
            gz = attempt_dir / "codex.events.jsonl.gz"
            if gz.exists():
                events_path = gz
            else:
                raise SystemExit(f"events file missing (enable capture first): {events_path}")
        if args.follow:
            tail_follow(events_path, out=sys.stdout, last_lines=args.lines)
        else:
            for line in tail_last_lines(events_path, n=args.lines):
                sys.stdout.write(line)
        return 0

    if args.cmd == "cancel":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.ops.cancel import cancel_attempt
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        if args.attempt_dir:
            attempt_dir = Path(args.attempt_dir).resolve()
        else:
            attempt_dir = rs.resolve_attempt_dir(batch_id=args.batch_id, job_id=args.job_id, step_id=args.step_id, run_id=args.run_id)
        result = cancel_attempt(attempt_dir, grace_seconds=float(args.grace_seconds))
        print(json.dumps(result, ensure_ascii=False))
        return 0

    if args.cmd == "close-batch":
        from parallelhassnes.core.atomic_io import write_once_json

        batch_dir = runs_root / args.batch_id
        if not (batch_dir / "batch_meta.json").exists():
            raise SystemExit(f"batch not found: {args.batch_id}")
        closed_path = batch_dir / "batch_closed.json"
        write_once_json(closed_path, {"batch_id": args.batch_id, "closed_at": utc_isoformat(), "reason": args.reason})
        print(json.dumps({"ok": True, "batch_id": args.batch_id, "closed_path": str(closed_path)}, ensure_ascii=False))
        return 0

    if args.cmd == "gc":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.retention.gc import RetentionPolicy, compute_protected_resume_bases_for_open_batches, run_gc
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        cfg = rs.read_harness_config()
        rp = (cfg.get("defaults", {}) or {}).get("retention_policy", {}) or {}
        compress_after = None
        if "compress_raw_events_after_days" in rp and rp.get("compress_raw_events_after_days") is not None:
            try:
                compress_after = int(rp.get("compress_raw_events_after_days"))
            except Exception:
                compress_after = None
        policy = RetentionPolicy(
            keep_raw_events_days=int(rp.get("keep_raw_events_days", 7)),
            keep_git_artifacts_days=int(rp.get("keep_git_artifacts_days", 7)),
            keep_resume_bases_days=int(rp.get("keep_resume_bases_days", 30)),
            compress_raw_events_after_days=compress_after,
            keep_reduced_state_days=int(rp.get("keep_reduced_state_days", 3650)),
            keep_final_outputs_days=int(rp.get("keep_final_outputs_days", 3650)),
            allow_delete_success_final_outputs=bool(rp.get("allow_delete_success_final_outputs", False)),
        )
        protected = compute_protected_resume_bases_for_open_batches(runs_root)
        if args.dry_run:
            print(json.dumps({"ok": True, "policy": policy.__dict__, "protected_resume_bases": [str(p) for p in sorted(protected)]}, ensure_ascii=False))
            return 0
        out = run_gc(runs_root=runs_root, policy=policy, protected_resume_bases=protected)
        print(json.dumps(out, ensure_ascii=False))
        return 0

    if args.cmd == "force-retry":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.ops.overrides import set_force_retry
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        path = rs.overrides_path(args.batch_id, args.job_id)
        obj = set_force_retry(path, args.step_id)
        print(json.dumps({"ok": True, "overrides_path": str(path), "overrides": obj}, ensure_ascii=False))
        return 0

    if args.cmd == "mark-failed":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.ops.overrides import set_forced_terminal
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        path = rs.overrides_path(args.batch_id, args.job_id)
        obj = set_forced_terminal(path, args.step_id, status="failed", reason=args.reason)
        print(json.dumps({"ok": True, "overrides_path": str(path), "overrides": obj}, ensure_ascii=False))
        return 0

    if args.cmd == "requeue":
        from parallelhassnes.core.paths import Paths
        from parallelhassnes.ops.overrides import set_requeue
        from parallelhassnes.storage.runs_store import RunsStore

        rs = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
        path = rs.overrides_path(args.batch_id, args.job_id)
        obj = set_requeue(path, args.step_id, target_runner_id=args.runner_id, different_from_last=bool(args.different), reason=args.reason)
        print(json.dumps({"ok": True, "overrides_path": str(path), "overrides": obj}, ensure_ascii=False))
        return 0

    if args.cmd == "tick":
        harness.tick_once(
            concurrency_override=getattr(args, "concurrency", None),
            use_fake_invoker=bool(getattr(args, "no_codex", False)),
            batch_id_filter=getattr(args, "batch_id", None),
            multi_batch=getattr(args, "multi_batch", None),
        )
        return 0

    if args.cmd == "scoreboard":
        if getattr(args, "write", False):
            from parallelhassnes.core.paths import Paths
            from parallelhassnes.storage.runs_store import RunsStore

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            # Always refresh the system scoreboard (cheap, operator-useful).
            try:
                store.write_system_scoreboard()
            except Exception:
                pass
            bid = getattr(args, "batch_id", None)
            if isinstance(bid, str) and bid.strip():
                try:
                    store.write_batch_scoreboard(bid.strip())
                except Exception:
                    pass
        out = harness.compute_scoreboards(batch_id=getattr(args, "batch_id", None))
        if args.json:
            print(json.dumps(out, ensure_ascii=False, indent=2))
        else:
            print(harness.format_scoreboards(out))
        return 0

    raise SystemExit(f"Unknown cmd: {args.cmd}")


def _preflight_launch_table_workspaces(launch_table_path: Path, *, runs_root: Path) -> None:
    """
    Operator safety check to prevent predictable disk bloat.

    Policy: any job that requests workspace_policy.mode='isolated' must be a git repo
    (so the harness uses a git worktree). If the working_directory is not in a git repo,
    fail fast before enqueueing the batch.
    """
    try:
        lt: dict[str, Any] = json.loads(launch_table_path.read_text())
    except Exception as e:
        raise SystemExit(f"preflight failed: could not parse Launch Table JSON: {launch_table_path} ({e})")

    batch_policy = lt.get("workspace_policy") if isinstance(lt, dict) else None
    jobs = lt.get("jobs") if isinstance(lt, dict) else None
    if not isinstance(jobs, list) or not jobs:
        return

    def effective_mode(job: dict[str, Any]) -> str:
        policy = job.get("workspace_policy") or batch_policy or {}
        if not isinstance(policy, dict):
            policy = {}
        return str(policy.get("mode", "shared"))

    # Resolve all workdirs that will be asked to be isolated.
    isolated_jobs: list[tuple[str, Path]] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        if effective_mode(j) != "isolated":
            continue
        wd = j.get("working_directory")
        if not isinstance(wd, str) or not wd.strip():
            continue
        isolated_jobs.append((str(j.get("job_id", "<unknown>")), Path(wd).expanduser().resolve()))

    if not isolated_jobs:
        return

    # Dedupe by git_root for reporting.
    missing_git: list[tuple[str, Path]] = []
    roots: dict[Path, list[str]] = {}
    for job_id, wd in isolated_jobs:
        root = _find_git_root(wd)
        if root is None:
            missing_git.append((job_id, wd))
            continue
        roots.setdefault(root, []).append(job_id)

    if missing_git:
        lines = [
            "preflight blocked submission: workspace_policy.mode='isolated' requires a git repo (git worktree isolation).",
            "Jobs with non-git working_directory:",
        ]
        for job_id, wd in missing_git:
            lines.append(f"- {job_id}: {wd}")
        lines.append("Fix: `git init` + initial commit in the project root (and ensure large data dirs are in .gitignore),")
        lines.append("or set workspace_policy.mode='shared' for this batch.")
        raise SystemExit("\n".join(lines))

    # Verify each root has a valid HEAD (worktree creation requires a commit).
    no_head: list[Path] = []
    for git_root in roots:
        chk = subprocess.run(
            ["git", "-C", str(git_root), "rev-parse", "--verify", "HEAD"],
            capture_output=True,
            text=True,
            env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
        )
        if chk.returncode != 0:
            no_head.append(git_root)
    if no_head:
        msg = [
            "preflight blocked submission: git repo exists but has no commit (HEAD missing), so git worktrees cannot be created.",
        ]
        for r in no_head:
            msg.append(f"- {r}")
        msg.append("Fix: make an initial commit in each repo (even a minimal commit; keep data ignored).")
        raise SystemExit("\n".join(msg))

    # Optional bloat guardrails from harness config.
    max_du_bytes = None
    max_tracked_bytes = None
    try:
        hc_path = (Path(runs_root).resolve() / "_system" / "harness_config.json").resolve()
        if hc_path.exists():
            hc = json.loads(hc_path.read_text(encoding="utf-8"))
            lim = hc.get("limits") if isinstance(hc, dict) else None
            if isinstance(lim, dict):
                if lim.get("max_isolated_git_root_du_bytes") is not None:
                    max_du_bytes = int(lim.get("max_isolated_git_root_du_bytes"))
                if lim.get("max_isolated_git_root_tracked_bytes") is not None:
                    max_tracked_bytes = int(lim.get("max_isolated_git_root_tracked_bytes"))
    except Exception:
        max_du_bytes = None
        max_tracked_bytes = None

    # Report sizes to stderr (operator visibility). This does not block.
    try:
        sys.stderr.write("preflight: isolated workspaces will use git worktrees (no full directory copies).\n")
        too_large: list[str] = []
        for git_root, job_ids in sorted(roots.items(), key=lambda kv: str(kv[0])):
            du_total = _du_bytes(git_root)
            tracked = _git_tracked_bytes(git_root)
            sys.stderr.write(
                f"- git_root={git_root} jobs={len(job_ids)} du_total={_fmt_bytes(du_total)} tracked_files={_fmt_bytes(tracked)}\n"
            )
            if isinstance(max_du_bytes, int) and max_du_bytes >= 0 and du_total > max_du_bytes:
                too_large.append(
                    f"- git_root={git_root} du_total={_fmt_bytes(du_total)} exceeds limits.max_isolated_git_root_du_bytes={_fmt_bytes(max_du_bytes)}"
                )
            if isinstance(max_tracked_bytes, int) and max_tracked_bytes >= 0 and tracked > max_tracked_bytes:
                too_large.append(
                    f"- git_root={git_root} tracked_files={_fmt_bytes(tracked)} exceeds limits.max_isolated_git_root_tracked_bytes={_fmt_bytes(max_tracked_bytes)}"
                )
        if too_large:
            msg = [
                "preflight blocked submission: isolated git worktrees exceed configured disk-bloat limits.",
                "Violations:",
                *too_large,
                "Fix options:",
                "- Move large data out of the git root (keep it in .gitignore and fetch/download during the run).",
                "- Use workspace_policy.mode='shared' for this batch/job.",
                "- Or raise/disable the limits in runs/_system/harness_config.json (set to null to disable).",
                "Override (not recommended): submit with --skip-preflight.",
            ]
            raise SystemExit("\n".join(msg))
    except Exception:
        # Never fail submission due to reporting issues.
        return


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


def _du_bytes(path: Path) -> int:
    # `du -sk` returns kibibytes; use disk usage to match what fills the drive.
    out = subprocess.run(["du", "-sk", str(path)], capture_output=True, text=True)
    if out.returncode != 0:
        return 0
    parts = (out.stdout or "").strip().split()
    if not parts:
        return 0
    try:
        kib = int(parts[0])
    except Exception:
        return 0
    return kib * 1024


def _git_tracked_bytes(git_root: Path) -> int:
    ls = subprocess.run(["git", "-C", str(git_root), "ls-files", "-z"], capture_output=True)
    if ls.returncode != 0:
        return 0
    data = ls.stdout or b""
    total = 0
    for raw in data.split(b"\x00"):
        if not raw:
            continue
        try:
            rel = raw.decode("utf-8", errors="strict")
        except Exception:
            continue
        p = (git_root / rel).resolve()
        try:
            st = p.stat()
        except Exception:
            continue
        total += int(getattr(st, "st_size", 0) or 0)
    return total


def _fmt_bytes(n: int) -> str:
    if n <= 0:
        return "0B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    v = float(n)
    i = 0
    while v >= 1024.0 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    if i == 0:
        return f"{int(v)}{units[i]}"
    return f"{v:.2f}{units[i]}"


if __name__ == "__main__":
    raise SystemExit(main())
