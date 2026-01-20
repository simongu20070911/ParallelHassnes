from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = REPO_ROOT / "FUNCTIONALITY_SPEC.md"


@dataclass(frozen=True)
class Requirement:
    line: int
    kind: str  # MUST | MUST_NOT | REQUIRED | SHOULD
    section: str
    text: str


@dataclass(frozen=True)
class Alignment:
    status: str  # PASS | N/A | GAP | PARTIAL
    evidence: tuple[str, ...]
    verification: tuple[str, ...]
    notes: str | None = None


def _find_line(path: Path, needle: str) -> int | None:
    try:
        for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if needle in line:
                return i
    except FileNotFoundError:
        return None
    return None


def _ref(path_rel: str, needle: str | None = None) -> str:
    path = REPO_ROOT / path_rel
    if needle is None:
        return str(path_rel)
    ln = _find_line(path, needle)
    return f"{path_rel}:{ln or 1}"


def extract_spec_requirements(spec_path: Path) -> list[Requirement]:
    """
    Extract “normative” requirements from the spec:
    - lines containing MUST/REQUIRED/SHOULD keywords
    - bullets under “Must include / Must not include / Scoreboard must include” blocks

    This is intentionally deterministic and conservative: it yields a finite list
    that we can cover exhaustively with an alignment map.
    """
    lines = spec_path.read_text(encoding="utf-8").splitlines()
    heading_stack: list[str] = []
    reqs: list[Requirement] = []
    mode: str | None = None

    def current_section() -> str:
        return " > ".join(heading_stack)

    for i, line in enumerate(lines, 1):
        h = re.match(r"^(#{1,6})\s+(.*)$", line)
        if h:
            level = len(h.group(1))
            title = h.group(2).strip()
            heading_stack[:] = heading_stack[: level - 1]
            heading_stack.append(title)
            mode = None
            continue

        stripped = line.strip()
        if stripped.startswith("**Must include") or stripped.startswith("Must include") or stripped.startswith("**Scoreboard must include") or stripped.startswith("Scoreboard must include"):
            mode = "MUST"
            continue
        if stripped.startswith("**Must not include") or stripped.startswith("Must not include"):
            mode = "MUST_NOT"
            continue

        bullet = re.match(r"^(?:[-*]|\d+\.)\s+(.*)$", stripped)
        if bullet:
            text = bullet.group(1).strip()
            if mode in {"MUST", "MUST_NOT"}:
                reqs.append(Requirement(line=i, kind=mode, section=current_section(), text=text))
                continue

            if re.search(r"\b(must not|must|required|should)\b", text, re.IGNORECASE):
                if re.search(r"\bmust not\b", text, re.IGNORECASE):
                    kind = "MUST_NOT"
                elif re.search(r"\brequired\b", text, re.IGNORECASE):
                    kind = "REQUIRED"
                elif re.search(r"\bshould\b", text, re.IGNORECASE):
                    kind = "SHOULD"
                else:
                    kind = "MUST"
                reqs.append(Requirement(line=i, kind=kind, section=current_section(), text=text))
                continue

        if stripped == "":
            mode = None

    return reqs


def build_alignment_map() -> dict[int, Alignment]:
    """
    Exhaustive mapping from spec requirement line -> implementation evidence.

    This must cover every extracted requirement line in FUNCTIONALITY_SPEC.md.
    """
    out: dict[int, Alignment] = {}

    # 4) Roles
    out[28] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "class Runner:"),
            _ref("parallelhassnes/runner/runner.py", 'env["CODEX_HOME"]'),
            _ref("parallelhassnes/runner/runner.py", 'base = ["codex", "exec"'),
        ),
        verification=(
            "tests/test_scenarios.py",
            "tests/test_file_order_and_immutability.py",
        ),
    )

    # 6.2 Launch table: batch goal + resume_from invariants
    out[93] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/validation/launch_table.py", "batch_goal_summary must be more than 150 words"),
        ),
        verification=("tests/test_scenarios.py",),
    )
    for ln in (120, 121, 126, 127):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/validation/launch_table.py", "resume_from"),
                _ref("parallelhassnes/scheduler/scheduler.py", "resolve_resume_source"),
            ),
            verification=("tests/test_resume_selector_determinism.py", "tests/test_scenarios.py"),
        )

    # 6.4 Harness config snapshots and effective defaults
    for ln in (141, 142, 143):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/config/harness_config.py", "def write_harness_config_snapshots"),
                _ref("parallelhassnes/harness/harness.py", "write_harness_config_snapshots"),
            ),
            verification=("tests/test_scenarios.py",),
        )
    out[152] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/config/harness_config.py", "heartbeat_stale_after_seconds"),
            _ref("parallelhassnes/validation/contracts.py", "heartbeat_stale_after_seconds"),
        ),
        verification=("tests/test_scoreboard_derivation_statuses.py",),
    )
    for ln in (153, 154):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/interfaces/fs_queue.py", "Policy merge: compute effective defaults"),
            ),
            verification=("tests/test_effective_defaults_merge.py",),
        )

    # 6.4.1 harness_config contents / not include secrets
    for ln in range(160, 173):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/config/harness_config.py", "return {"),
                _ref("parallelhassnes/validation/contracts.py", "def validate_harness_config"),
            ),
            verification=("tests/test_scenarios.py",),
        )
    out[175] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/config/harness_config.py", "_deny_secrets"),
        ),
        verification=("tests/test_harness_config_secret_denylist.py",),
    )

    # 7.1 Batch submission validation
    out[181] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/validation/launch_table.py", "def validate_launch_table"),
            _ref("parallelhassnes/interfaces/fs_queue.py", "validate_launch_table"),
        ),
        verification=("tests/test_scenarios.py",),
    )

    # 7.1.1 batch_meta.json fields + exclusions
    for ln in range(193, 203):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/validation/launch_table.py", "normalize_launch_table"),
                _ref("schemas/launch_table.normalized.schema.json", "\"batch_goal_summary\""),
            ),
            verification=("tests/test_scenarios.py", "tests/test_effective_defaults_merge.py"),
        )
    out[205] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/interfaces/fs_queue.py", "forbidden = {\"effective_defaults\""),
        ),
        verification=("tests/test_effective_defaults_merge.py",),
        notes="batch_meta is write-once; runtime attempt results live under attempt dirs + current.json.",
    )
    out[206] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "_update_current_thread_id"),
        ),
        verification=("tests/test_codex_thread_id_capture.py",),
        notes="Thread id is not in batch_meta at submit time; captured later into current.json when enabled.",
    )
    out[230] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "capture_events_jsonl"),
        ),
        verification=("tests/test_scenarios.py",),
        notes="Raw event streams are optional and never used for orchestration decisions.",
    )

    # 7.4 Resume execution: explicit thread id when required (advanced)
    out[241] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "Prefer an explicit thread id when known"),
            _ref("parallelhassnes/runner/runner.py", "cmd = base + [\"resume\", explicit_resume_thread_id.strip()]"),
        ),
        verification=("tests/test_resume_explicit_thread_id_preference.py",),
    )

    # 7.6 Cancellation (SHOULD)
    out[255] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/ops/cancel.py", "def cancel_attempt"),
        ),
        verification=("tests/test_cancel_semantics.py",),
        notes="Spec uses SHOULD; implementation supports cancel-before-start and cancel-while-running with terminalization.",
    )

    # 8.1.1 Run report schema validation
    out[280] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "schema_errors = validate_json_against_schema"),
        ),
        verification=("tests/test_schema_files.py", "tests/test_file_order_and_immutability.py"),
    )

    # 8.2.1 attempt dir naming + state/codex_home invariants
    out[322] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/core/paths.py", "def run_dir_name"),
            _ref("parallelhassnes/runner/runner.py", "failed to allocate a unique attempt directory"),
        ),
        verification=("tests/test_concurrency_safety.py",),
    )
    for ln in (323, 324):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/storage/runs_store.py", "def resolve_attempt_dir"),
            ),
            verification=("tests/test_attempt_dir_resolution.py",),
        )
    out[343] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/core/atomic_io.py", "def write_atomic_json"),
            _ref("parallelhassnes/core/state_update.py", "write_atomic_json"),
        ),
        verification=("tests/test_state_transition_guard.py",),
    )
    out[344] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "hb.write_queued()"),
            _ref("parallelhassnes/runner/runner.py", "\"queued/initializing\""),
        ),
        verification=("tests/test_scenarios.py",),
        notes="Runner writes an initial queued snapshot immediately; readers also tolerate missing state as best-effort.",
    )
    out[349] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/core/state_update.py", "TERMINAL_STATUSES"),
        ),
        verification=("tests/test_state_transition_guard.py", "tests/test_cancel_semantics.py"),
    )
    for ln in (351, 352):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/validation/contracts.py", "ended_at required for terminal state"),
            ),
            verification=("tests/test_file_order_and_immutability.py",),
        )
    out[362] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "env[\"CODEX_HOME\"]"),
            _ref("parallelhassnes/runner/runner.py", "Attempt directory uniqueness under parallel starts"),
        ),
        verification=("tests/test_concurrency_safety.py",),
    )
    out[365] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "def _provision_codex_credentials"),
        ),
        verification=("tests/test_scenarios.py",),
        notes="Best-effort: uses env-injected API key when present, else symlinks runner-local ~/.codex auth/config into attempt-local CODEX_HOME.",
    )

    # 8.3 pointers (SHOULD) + 8.3.1 current.json requirements
    out[417] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/storage/runs_store.py", "def resolve_attempt_dir"),
            _ref("parallelhassnes/runner/runner.py", "\"resume_base_dir\""),
        ),
        verification=("tests/test_attempt_dir_resolution.py",),
        notes="Spec uses SHOULD; attempt dirs are discoverable by IDs and current.json includes resume_base_dir pointers.",
    )
    out[418] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/scoreboard/scoreboards.py", "reasons.append"),
        ),
        verification=("tests/test_scoreboard_derivation_statuses.py",),
        notes="Blocked reasons are derived in scoreboards; current.json remains a minimal mutable index.",
    )
    for ln in range(435, 443):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/core/current_update.py", "steps ="),
                _ref("parallelhassnes/runner/runner.py", "def _update_current_on_start"),
            ),
            verification=("tests/test_scenarios.py", "tests/test_codex_thread_id_capture.py"),
        )

    # 8.5 Retention: protect resume bases referenced by current.json
    out[462] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/retention/gc.py", "def compute_protected_resume_bases_for_open_batches"),
        ),
        verification=("tests/test_scenarios.py", "tests/test_retention_policy.py"),
    )

    # 8.6 scenario: current.json must be updated by completion
    out[472] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "self._update_current_on_end"),
        ),
        verification=("tests/test_scenarios.py",),
    )

    # 9.1 stuck definition + scoreboard contract
    out[500] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/scoreboard/scoreboards.py", "hb_age"),
            _ref("parallelhassnes/validation/contracts.py", "heartbeat_stale_after_seconds must be >= 1800"),
        ),
        verification=("tests/test_scoreboard_derivation_statuses.py",),
    )
    for ln in range(519, 539):
        out[ln] = Alignment(
            status="PASS",
            evidence=(
                _ref("parallelhassnes/scoreboard/scoreboards.py", "def compute_batch_scoreboard"),
            ),
            verification=("tests/test_scoreboard_derivation_statuses.py",),
        )

    # 10.1 resume semantics
    out[671] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "codex_home ="),
            _ref("parallelhassnes/runner/runner.py", "Copy-on-resume"),
        ),
        verification=("tests/test_scenarios.py",),
    )
    out[672] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "_copy_dir(source_for_copy, dest_codex_home)"),
        ),
        verification=("tests/test_scenarios.py",),
    )
    out[676] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "resume_base_transfer_enabled"),
        ),
        verification=("tests/test_runner_locality.py",),
    )
    out[677] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "shared_filesystem"),
        ),
        verification=("tests/test_runner_locality.py",),
    )
    out[681] = Alignment(
        status="PASS",
        evidence=(
            _ref("parallelhassnes/runner/runner.py", "Prefer an explicit thread id when known"),
        ),
        verification=("tests/test_resume_explicit_thread_id_preference.py",),
        notes="If a thread id is known (captured or provided), resume uses it; otherwise it falls back to resume --last inside an isolated session store.",
    )

    # Appendix A/B: non-runtime doc guidance; N/A for code alignment checks.
    out[757] = Alignment(status="N/A", evidence=(), verification=(), notes="Spec misread clarification; not a code requirement.")
    out[763] = Alignment(status="N/A", evidence=(), verification=(), notes="Spec editing checklist item; not a runtime requirement.")
    out[766] = Alignment(status="N/A", evidence=(), verification=(), notes="Spec editing checklist item; not a runtime requirement.")

    return out


def render_matrix(requirements: list[Requirement], align: dict[int, Alignment]) -> str:
    lines: list[str] = []
    lines.append("# Spec Alignment Matrix (FUNCTIONALITY_SPEC.md → Implementation)\n")
    lines.append("This file is generated/maintained by `tools/spec_alignment_audit.py`.\n")
    lines.append("Legend: PASS = implemented and testable; N/A = not a runtime requirement; PARTIAL/GAP = misalignment.\n")
    lines.append("| Spec line | Kind | Requirement | Status | Evidence | Verification | Notes |")
    lines.append("|---:|---|---|---|---|---|---|")

    for r in sorted(requirements, key=lambda x: x.line):
        a = align.get(r.line)
        if a is None:
            status = "GAP"
            evidence = ""
            verification = ""
            notes = "no mapping"
        else:
            status = a.status
            evidence = "<br>".join(a.evidence) if a.evidence else ""
            verification = "<br>".join(a.verification) if a.verification else ""
            notes = a.notes or ""
        req = r.text.replace("|", "\\|")
        lines.append(f"| {r.line} | {r.kind} | {req} | {status} | {evidence} | {verification} | {notes} |")

    lines.append("")
    return "\n".join(lines)


def audit(*, write_matrix_path: Path | None) -> dict[str, Any]:
    requirements = extract_spec_requirements(SPEC_PATH)
    align = build_alignment_map()
    missing = sorted({r.line for r in requirements} - set(align.keys()))
    extra = sorted(set(align.keys()) - {r.line for r in requirements})

    gaps: list[int] = []
    partial: list[int] = []
    for r in requirements:
        a = align.get(r.line)
        if a is None:
            gaps.append(r.line)
            continue
        if a.status == "GAP":
            gaps.append(r.line)
        if a.status == "PARTIAL":
            partial.append(r.line)

    # Evidence paths should exist (best-effort): only validate the file component.
    bad_evidence: list[str] = []
    for a in align.values():
        for ref in a.evidence:
            path_part = ref.split(":", 1)[0]
            p = REPO_ROOT / path_part
            if not p.exists():
                bad_evidence.append(ref)

    report = {
        "spec_path": str(SPEC_PATH),
        "requirements_count": len(requirements),
        "missing_mappings": missing,
        "extra_mappings": extra,
        "gaps": gaps,
        "partial": partial,
        "bad_evidence": bad_evidence,
    }

    if write_matrix_path is not None:
        write_matrix_path.write_text(render_matrix(requirements, align), encoding="utf-8")

    return report


def main(argv: list[str]) -> int:
    write = None
    if "--write" in argv:
        write = REPO_ROOT / "SPEC_ALIGNMENT_MATRIX.md"

    report = audit(write_matrix_path=write)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["missing_mappings"] or report["gaps"] or report["partial"] or report["bad_evidence"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__("sys").argv[1:]))

