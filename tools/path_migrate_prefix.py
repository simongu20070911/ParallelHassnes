from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ScanHit:
    path: str
    kind: str
    size_bytes: int


@dataclass(frozen=True)
class ScanSkip:
    path: str
    reason: str


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="path_migrate_prefix")
    p.add_argument("--runs-root", required=True, help="Runs root (e.g., /Users/simongu/ParallelHassnes/runs)")
    p.add_argument("--old", required=True, help="Old prefix to replace")
    p.add_argument("--new", required=True, help="New prefix")
    p.add_argument("--apply", action="store_true", help="Apply in-place rewrites (default: dry-run)")
    p.add_argument("--timeout-seconds", type=float, default=1.5, help="Per-file read timeout (default: 1.5s)")
    p.add_argument(
        "--target-name",
        action="append",
        default=[],
        help="Filename to scan (repeatable). If omitted, scans common control JSON files.",
    )
    p.add_argument("--out-json", required=True, help="Write report JSON to this path")
    p.add_argument("--out-md", required=False, help="Write report Markdown to this path (optional)")
    return p.parse_args(argv)


def _read_bytes(path: Path, timeout_seconds: float) -> bytes:
    try:
        p = subprocess.run(["/bin/cat", str(path)], capture_output=True, timeout=float(timeout_seconds), check=False)
    except subprocess.TimeoutExpired:
        raise TimeoutError(str(path))
    if p.returncode != 0:
        return b""
    return p.stdout or b""


def _deep_replace(obj: Any, old: str, new: str) -> Any:
    if isinstance(obj, str):
        return obj.replace(old, new)
    if isinstance(obj, list):
        return [_deep_replace(x, old, new) for x in obj]
    if isinstance(obj, dict):
        return {k: _deep_replace(v, old, new) for k, v in obj.items()}
    return obj


def _write_atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    runs_root = Path(args.runs_root).expanduser().resolve()
    old = str(args.old)
    new = str(args.new)
    apply = bool(args.apply)
    timeout_s = float(args.timeout_seconds)

    default_targets = {
        "batch_meta.json",
        "scheduler_state.json",
        "current.json",
        "meta.json",
        "state.json",
        "overrides.json",
        "health.json",
        "harness_config.json",
        "batch_closed.json",
        "batch_unreadable.json",
        "scoreboard.batch.json",
        "scoreboard.system.json",
    }
    targets = set(args.target_name) if args.target_name else default_targets

    candidates: list[Path] = []
    for name in sorted(targets):
        candidates.extend(runs_root.rglob(name))
    candidates = sorted({p.resolve() for p in candidates if p.is_file()})

    hits: list[ScanHit] = []
    skipped: list[ScanSkip] = []
    changed: list[str] = []
    changed_by_kind: Counter[str] = Counter()

    for path in candidates:
        try:
            b = _read_bytes(path, timeout_seconds=timeout_s)
        except TimeoutError:
            skipped.append(ScanSkip(path=str(path), reason="read_timeout"))
            continue
        if not b:
            continue
        if old.encode("utf-8") not in b:
            continue
        kind = path.name
        hits.append(ScanHit(path=str(path), kind=kind, size_bytes=path.stat().st_size))

        if not apply:
            continue

        # Best-effort: only rewrite valid JSON objects/arrays.
        try:
            obj = json.loads(b.decode("utf-8"))
        except Exception:
            skipped.append(ScanSkip(path=str(path), reason="invalid_json"))
            continue

        # Safety: do not touch live running attempt state.
        if kind == "state.json" and isinstance(obj, dict) and obj.get("status") == "running":
            skipped.append(ScanSkip(path=str(path), reason="skip_running_state"))
            continue

        new_obj = _deep_replace(obj, old=old, new=new)
        if new_obj == obj:
            continue
        _write_atomic_json(path, new_obj)
        changed.append(str(path))
        changed_by_kind[kind] += 1

    report: dict[str, Any] = {
        "runs_root": str(runs_root),
        "old_prefix": old,
        "new_prefix": new,
        "apply": apply,
        "targets": sorted(targets),
        "candidates_scanned": len(candidates),
        "hits_count": len(hits),
        "hits_by_kind": dict(Counter([h.kind for h in hits])),
        "skipped_count": len(skipped),
        "skipped": [s.__dict__ for s in skipped],
        "changed_count": len(changed),
        "changed_by_kind": dict(changed_by_kind),
        "changed": changed[:5000],
        "notes": [
            "This tool performs a literal prefix replacement inside JSON string fields only (deep traversal).",
            "It does not rewrite binary/log files (events, stdout/stderr).",
            "It skips state.json where status=='running' to avoid interfering with live attempts.",
        ],
    }

    out_json = Path(args.out_json).expanduser().resolve()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    if args.out_md:
        out_md = Path(args.out_md).expanduser().resolve()
        out_md.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        lines.append("# ParallelHassnes Path Migration Audit\n")
        lines.append(f"- runs_root: `{runs_root}`")
        lines.append(f"- old_prefix: `{old}`")
        lines.append(f"- new_prefix: `{new}`")
        lines.append(f"- apply: `{apply}`")
        lines.append(f"- candidates_scanned: `{len(candidates)}`")
        lines.append(f"- hits_count: `{len(hits)}`")
        lines.append(f"- skipped_count: `{len(skipped)}`")
        lines.append(f"- changed_count: `{len(changed)}`\n")
        if hits:
            lines.append("## Hits By Kind")
            for k, v in Counter([h.kind for h in hits]).most_common():
                lines.append(f"- `{k}`: `{v}`")
            lines.append("")
        if apply and changed_by_kind:
            lines.append("## Changed By Kind")
            for k, v in changed_by_kind.most_common():
                lines.append(f"- `{k}`: `{v}`")
            lines.append("")
        if skipped:
            lines.append("## Skipped (First 50)")
            for s in skipped[:50]:
                lines.append(f"- `{s.path}` ({s.reason})")
            lines.append("")
        lines.append("## Next Steps")
        lines.append("- You can remove the Desktop symlink after rewriting if no remaining files reference the old prefix.")
        lines.append("- If there are still hits in `state.json`/`meta.json`, that is harmless; operationally the important one is `current.json`.")
        out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({"ok": True, "report_json": str(out_json), "apply": apply, "hits": len(hits), "changed": len(changed)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

