# Manual Spec Trace (authoritative)

This file is a **manual**, human-maintained trace from `FUNCTIONALITY_SPEC.md` requirements to implementation and tests.

It exists because “institutional grade” alignment means:
- every **MUST/REQUIRED/MUST NOT** constraint is backed by either a deterministic invariant in code *and* at least one regression test, or
- a clear explanation of why the spec labels it “conceptual / not required for filesystem MVP”.

This trace is **not** generated and is not an “auditor output”. It is intended as the owner’s checklist.

## Conventions
- Spec references are by the line numbers produced by: `rg -n "must|required|must not" FUNCTIONALITY_SPEC.md`.
- “Evidence” points to the concrete code path that enforces the behavior.
- “Test” points to a regression test that would fail if the behavior regresses.

## 1) Roles / IDs / submission contract

- Spec: `FUNCTIONALITY_SPEC.md:28` (Runner executes jobs, sets `CODEX_HOME`, spawns `codex exec`, writes artifacts)
  - Evidence: `parallelhassnes/runner/runner.py` (attempt lifecycle + `CODEX_HOME` + `codex exec`)
  - Test: `tests/test_scenarios.py`

- Spec: `FUNCTIONALITY_SPEC.md:93` (required `batch_goal_summary` >150 words)
  - Evidence: `parallelhassnes/validation/launch_table.py` (`_word_count` + validation)
  - Test: `tests/test_scenarios.py`

- Spec: `FUNCTIONALITY_SPEC.md:120`/`:121`/`:126`/`:127` (resume_from step_id/selector, stable resume base, implies dependency)
  - Evidence:
    - `parallelhassnes/validation/launch_table.py` (wiring `resume_from` into DAG)
    - `parallelhassnes/resume/selectors.py` (selectors: `latest_successful|latest|run_id`)
    - `parallelhassnes/runner/runner.py` (copy-on-resume)
  - Tests:
    - `tests/test_resume_from_selectors_execution.py`
    - `tests/test_resume_explicit_thread_id_preference.py`

## 2) Centralized harness config

- Spec: `FUNCTIONALITY_SPEC.md:141`/`:142`/`:143` (write `runs/_system/harness_config.json`, atomic snapshot, version snapshots write-once)
  - Evidence: `parallelhassnes/config/harness_config.py` + `parallelhassnes/harness/harness.py`
  - Test: `tests/test_scenarios.py`

- Spec: `FUNCTIONALITY_SPEC.md:174`/`:175` (must not include secrets in harness_config)
  - Evidence: `parallelhassnes/validation/contracts.py` + `parallelhassnes/config/harness_config.py` secret-key/PEM denial
  - Test: `tests/test_invariants.py` (config validation scenarios)

- Spec: `FUNCTIONALITY_SPEC.md:150`/`:172` (operator-level maximum batch/job/step sizes; safety caps)
  - Evidence: `parallelhassnes/interfaces/fs_queue.py` (enforces `limits.max_jobs_per_batch` and `limits.max_steps_per_job` at ingestion)
  - Tests: `tests/test_limits_enforced.py`

## 3) Effective defaults

- Spec: `FUNCTIONALITY_SPEC.md:153`/`:154` (allowed overrides; compute+record `effective_defaults` in `batch_meta.json`)
  - Evidence: `parallelhassnes/interfaces/fs_queue.py` (allowlist merge into `effective_defaults`)
  - Tests:
    - `tests/test_effective_defaults_merge.py`
    - `tests/test_batch_effective_defaults_scoreboard_override.py`

## 4) Attempt directory naming + discoverability

- Spec: `FUNCTIONALITY_SPEC.md:322`/`:323`/`:324` (unique attempt dirs; discoverable from IDs without reading file contents; include run_id)
  - Evidence:
    - `parallelhassnes/core/paths.py` (`run_dir_name` includes run_id)
    - `parallelhassnes/storage/runs_store.py` (`resolve_attempt_dir` supports direct `<run_id>/` and suffix match `_<run_id>` without opening attempt files)
  - Test: `tests/test_attempt_dir_resolution.py`

## 5) `state.json` semantics (atomic snapshots + immutability)

- Spec: `FUNCTIONALITY_SPEC.md:343`/`:344`/`:349`/`:351`/`:352`
  - Evidence:
    - `parallelhassnes/core/atomic_io.py` (write temp + rename)
    - `parallelhassnes/core/state_update.py` (allowed transitions; terminal freeze)
    - `parallelhassnes/ops/cancel.py` (missing state treated queued/initializing; writes terminal canceled)
    - `parallelhassnes/recovery/recovery.py` (recovery uses guarded writer; no clobber)
  - Tests:
    - `tests/test_file_order_and_immutability.py`
    - `tests/test_cancel_semantics.py`
    - `tests/test_runner_restart_recovery.py`

## 6) `CODEX_HOME` + auth behavior (no API key required)

- Spec: `FUNCTIONALITY_SPEC.md:365` (if no API key, symlink existing `auth.json` into attempt-local CODEX_HOME)
  - Evidence: `parallelhassnes/runner/runner.py` `_provision_codex_credentials`
  - Tests:
    - `tests/test_resume_does_not_copy_codex_auth.py` (also ensures resume copy does not duplicate auth material)

## 7) Run Report (`final.json`) schema validation

- Spec: `FUNCTIONALITY_SPEC.md:280` (validate `final.json` against expected schema; failures -> needs_attention)
  - Evidence: `parallelhassnes/runner/runner.py` (schema validation + terminal `needs_attention`)
  - Tests:
    - `tests/test_scoreboard_derivation_statuses.py` (schema-failure surfaced)
    - `tests/test_codex_timeout_vs_error.py` (timeout vs non-zero exit handling)

## 8) `current.json` pointers

- Spec: `FUNCTIONALITY_SPEC.md:472` (`current.json` updated to point at attempts; required by completion)
  - Evidence: `parallelhassnes/runner/runner.py` `_update_current_on_start` + `_update_current_on_end`
  - Tests: `tests/test_scenarios.py`

## 9) Scoreboards (batch + system)

- Spec: `FUNCTIONALITY_SPEC.md:500`/`:523` (stuck threshold default 45m; must be >=30m; surface in scoreboard)
  - Evidence: `parallelhassnes/scoreboard/scoreboards.py` (threshold from batch effective defaults, clamped >=1800)
  - Test: `tests/test_batch_effective_defaults_scoreboard_override.py`

- Spec: `FUNCTIONALITY_SPEC.md:540` (step status derivation precedence rules)
  - Evidence: `parallelhassnes/scoreboard/scoreboards.py` `_derive_step_status`
  - Tests:
    - `tests/test_scoreboard_derivation_statuses.py`
    - `tests/test_scoreboard_dependency_success_fallback.py` (dependency success inferred from `by_run_id` when pointer missing)

- Spec: `FUNCTIONALITY_SPEC.md:549` (resume visibility for steps with `resume_from`)
  - Evidence: `parallelhassnes/scoreboard/scoreboards.py` `_resume_visibility` + `resume_steps[]`
  - Tests: `tests/test_scoreboard_derivation_statuses.py`

- Spec: `FUNCTIONALITY_SPEC.md:555`/`:569` (multi-batch system view + drill-down)
  - Evidence: `parallelhassnes/scoreboard/scoreboards.py` `compute_system_scoreboard` + `compute_batch_scoreboard`
  - Tests: `tests/test_scenarios.py`

## 9.2 Runner health (pressure + drain)

- Spec: `FUNCTIONALITY_SPEC.md:651`–`:656` (runner health includes pressure signals and drain mode)
  - Evidence:
    - `parallelhassnes/runner/runner.py` `_write_runner_health` (disk + loadavg + memory, and drain_mode)
    - `parallelhassnes/storage/runs_store.py` `build_runner_pool` (drain-aware capacity=0 for new work)
  - Tests:
    - `tests/test_runner_drain_mode.py`

## 10) Resume semantics / locality / copy-on-resume

- Spec: `FUNCTIONALITY_SPEC.md:671`/`:672`/`:676`/`:677`/`:681`
  - Evidence: `parallelhassnes/runner/runner.py` `_prepare_resume_base` (copy base; optional transfer cache; explicit thread id preference)
  - Tests:
    - `tests/test_runner_locality.py`
    - `tests/test_resume_from_selectors_execution.py`
    - `tests/test_resume_explicit_thread_id_preference.py`

## 11) Workspace policy

- Spec: `FUNCTIONALITY_SPEC.md:702`/`:706`/`:711` (shared vs isolated per job; recorded in meta; resume uses latest workspace state)
  - Evidence:
    - `parallelhassnes/workspace/policy.py` (per-job workspace selection)
    - `parallelhassnes/runner/runner.py` (records workspace info in meta)
  - Tests:
    - `tests/test_workspace_policy_isolated.py`

## 12) Interfaces (filesystem + API)

- Spec: `FUNCTIONALITY_SPEC.md:728` (API mode optional; when present maps to filesystem artifacts)
  - Evidence:
    - filesystem: `parallelhassnes/interfaces/fs_queue.py`
    - API: `parallelhassnes/api/app.py` + `parallelhassnes/api/auth.py`
  - Tests:
    - filesystem flows: `tests/test_scenarios.py`

## 13) Retention / GC protection for resume bases

- Spec: `FUNCTIONALITY_SPEC.md:462` (GC must not delete resume bases referenced by current.json for open batches)
  - Evidence: `parallelhassnes/retention/gc.py` `compute_protected_resume_bases_for_open_batches`
  - Test: `tests/test_scenarios.py` (GC protection + closed-batch deletion)

## 14) Long-lived harness control plane

- Spec: implied by `FUNCTIONALITY_SPEC.md:6.4` (“long-lived control-plane component”) and operational monitoring needs.
  - Evidence:
    - CLI: `parallelhassnes/__main__.py` `serve`
    - loop: `parallelhassnes/runtime/tick_loop.py`
  - Tests:
    - `tests/test_tick_loop_runner_health_idle.py` (periodic health update even when no jobs)

## Complete requirement index (exhaustive)

This section exists only to ensure **every** line that matches `must|required|must not` in `FUNCTIONALITY_SPEC.md`
has an explicit disposition (PASS or N/A) with concrete evidence.

Legend:
- PASS = enforced in code and covered by at least one regression test.
- N/A = not a runtime requirement (e.g., spec headings, spec-editing checklist items, clarifications).

| Spec line | Disposition | Evidence (code) | Tests | Notes |
|---:|---|---|---|---|
| `FUNCTIONALITY_SPEC.md:28` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_scenarios.py` | Runner sets `CODEX_HOME`, spawns `codex exec`, writes artifacts. |
| `FUNCTIONALITY_SPEC.md:60` | PASS | `parallelhassnes/validation/launch_table.py`, `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scenarios.py` | Batch goal summary validated (>150 words) and previewed in system scoreboard. |
| `FUNCTIONALITY_SPEC.md:64` | N/A | — | — | Spec section heading (IDs list). |
| `FUNCTIONALITY_SPEC.md:93` | PASS | `parallelhassnes/validation/launch_table.py` | `tests/test_scenarios.py` | Word-count validation uses whitespace-delimited tokens. |
| `FUNCTIONALITY_SPEC.md:120` | PASS | `parallelhassnes/validation/launch_table.py` | `tests/test_scenarios.py` | `resume_from.step_id` required and validated. |
| `FUNCTIONALITY_SPEC.md:121` | PASS | `parallelhassnes/validation/launch_table.py` | `tests/test_scenarios.py` | Default selector (`latest_successful`) normalized at submit time. |
| `FUNCTIONALITY_SPEC.md:126` | PASS | `parallelhassnes/resume/selectors.py`, `parallelhassnes/runner/runner.py` | `tests/test_resume_from_selectors_execution.py` | Resume base is a frozen per-attempt `codex_home/` and is copied into a new attempt. |
| `FUNCTIONALITY_SPEC.md:127` | PASS | `parallelhassnes/validation/launch_table.py`, `parallelhassnes/scheduler/scheduler.py` | `tests/test_resume_selector_determinism.py` | `resume_from` is treated as an ordering dependency by scheduler and validation. |
| `FUNCTIONALITY_SPEC.md:141` | PASS | `parallelhassnes/config/harness_config.py`, `parallelhassnes/harness/harness.py` | `tests/test_scenarios.py` | Central config materialized at `runs/_system/harness_config.json`. |
| `FUNCTIONALITY_SPEC.md:142` | PASS | `parallelhassnes/core/atomic_io.py`, `parallelhassnes/config/harness_config.py` | `tests/test_scenarios.py` | Atomic snapshot includes stable `harness_config_version`. |
| `FUNCTIONALITY_SPEC.md:143` | PASS | `parallelhassnes/config/harness_config.py`, `parallelhassnes/harness/harness.py` | `tests/test_scenarios.py` | Version snapshots are write-once under `runs/_system/harness_config_versions/`. |
| `FUNCTIONALITY_SPEC.md:152` | PASS | `parallelhassnes/config/harness_config.py`, `parallelhassnes/validation/contracts.py` | `tests/test_invariants.py` | Scoreboard defaults validated; stale threshold clamped/validated `>=1800`. |
| `FUNCTIONALITY_SPEC.md:153` | PASS | `parallelhassnes/interfaces/fs_queue.py` | `tests/test_effective_defaults_merge.py` | Only allowlisted override keys permitted; effective defaults computed. |
| `FUNCTIONALITY_SPEC.md:154` | PASS | `parallelhassnes/interfaces/fs_queue.py` | `tests/test_effective_defaults_merge.py` | `batch_meta.json` includes `harness_config_version` and `effective_defaults`. |
| `FUNCTIONALITY_SPEC.md:156` | N/A | — | — | Spec subsection heading (“contents required, conceptual”). |
| `FUNCTIONALITY_SPEC.md:159` | N/A | — | — | Spec “Must include (minimum)” heading for harness_config. |
| `FUNCTIONALITY_SPEC.md:170` | PASS | `parallelhassnes/config/harness_config.py` | `tests/test_invariants.py` | Harness config includes scoreboard defaults + remediation enablement flag. |
| `FUNCTIONALITY_SPEC.md:174` | N/A | — | — | Spec “Must not include” heading for harness_config. |
| `FUNCTIONALITY_SPEC.md:175` | PASS | `parallelhassnes/config/harness_config.py`, `parallelhassnes/validation/contracts.py` | `tests/test_harness_config_secret_denylist.py` | Secrets denied by key-pattern + PEM detection. |
| `FUNCTIONALITY_SPEC.md:181` | PASS | `parallelhassnes/validation/launch_table.py`, `parallelhassnes/interfaces/fs_queue.py` | `tests/test_scenarios.py` | Submit-time validation of required fields/DAG/refs. |
| `FUNCTIONALITY_SPEC.md:189` | N/A | — | — | Spec subsection heading (`batch_meta.json` contents). |
| `FUNCTIONALITY_SPEC.md:192` | N/A | — | — | Spec “Must include (minimum)” heading for batch_meta. |
| `FUNCTIONALITY_SPEC.md:197` | PASS | `parallelhassnes/validation/launch_table.py`, `parallelhassnes/validation/contracts.py` | `tests/test_scenarios.py` | `batch_goal_summary` required and word-count validated. |
| `FUNCTIONALITY_SPEC.md:204` | N/A | — | — | Spec “Must not include (by design)” heading for batch_meta. |
| `FUNCTIONALITY_SPEC.md:230` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_scenarios.py` | Raw events are optional; scheduling uses `batch_meta.json`/`current.json`/`state.json`. |
| `FUNCTIONALITY_SPEC.md:241` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_resume_explicit_thread_id_preference.py` | When a thread ID is known/required, resume uses explicit `resume <id>`. |
| `FUNCTIONALITY_SPEC.md:280` | PASS | `parallelhassnes/runner/runner.py`, `parallelhassnes/core/schema_validation.py` | `tests/test_schema_files.py` | Final output validated against schema; schema errors produce `needs_attention`. |
| `FUNCTIONALITY_SPEC.md:286` | PASS | `parallelhassnes/core/paths.py`, `parallelhassnes/runner/runner.py` | `tests/test_file_order_and_immutability.py` | Run attempt directory layout (meta/state/final/handoff/codex_home). |
| `FUNCTIONALITY_SPEC.md:321` | N/A | — | — | Spec subsection heading (“naming required”). |
| `FUNCTIONALITY_SPEC.md:322` | PASS | `parallelhassnes/core/paths.py` | `tests/test_attempt_dir_resolution.py` | Attempt directories are uniquely named per run attempt. |
| `FUNCTIONALITY_SPEC.md:323` | PASS | `parallelhassnes/storage/runs_store.py` | `tests/test_attempt_dir_resolution.py` | Attempt dir resolvable from IDs via `current.json`, direct `<run_id>/`, or suffix `_<run_id>` (no attempt-file reads). |
| `FUNCTIONALITY_SPEC.md:324` | PASS | `parallelhassnes/core/paths.py` | `tests/test_attempt_dir_resolution.py` | `<run_dir_name>` includes `run_id` (suffix). |
| `FUNCTIONALITY_SPEC.md:343` | PASS | `parallelhassnes/core/atomic_io.py` | `tests/test_file_order_and_immutability.py` | Atomic snapshot updates use write-temp + rename. |
| `FUNCTIONALITY_SPEC.md:344` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scoreboard_state_missing_semantics.py` | Missing `state.json` treated as queued/initializing for display and in-flight for status. |
| `FUNCTIONALITY_SPEC.md:349` | PASS | `parallelhassnes/core/state_update.py` | `tests/test_state_transition_guard.py` | Terminal status is frozen; illegal transitions rejected. |
| `FUNCTIONALITY_SPEC.md:351` | PASS | `parallelhassnes/validation/contracts.py` | `tests/test_invariants.py` | `started_at` required when running/terminal. |
| `FUNCTIONALITY_SPEC.md:352` | PASS | `parallelhassnes/validation/contracts.py` | `tests/test_invariants.py` | `ended_at` required when terminal. |
| `FUNCTIONALITY_SPEC.md:362` | PASS | `parallelhassnes/core/paths.py`, `parallelhassnes/runner/runner.py` | `tests/test_file_order_and_immutability.py` | Attempt-local `codex_home/` is per run attempt. |
| `FUNCTIONALITY_SPEC.md:365` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_resume_does_not_copy_codex_auth.py` | No API key needed: `auth.json`/`config.toml` are symlinked into attempt-local `CODEX_HOME`. |
| `FUNCTIONALITY_SPEC.md:381` | N/A | — | — | Spec lifecycle/mutability audit section (spec documentation). |
| `FUNCTIONALITY_SPEC.md:421` | PASS | `parallelhassnes/storage/runs_store.py`, `parallelhassnes/runner/runner.py` | `tests/test_scenarios.py` | `current.json` exists and is updated from lifecycle events without raw trace parsing. |
| `FUNCTIONALITY_SPEC.md:434` | N/A | — | — | Spec “Must include (minimum)” heading for current.json fields. |
| `FUNCTIONALITY_SPEC.md:462` | PASS | `parallelhassnes/retention/gc.py` | `tests/test_retention_policy.py` | GC protects any `resume_base_dir` referenced by open-batch `current.json`. |
| `FUNCTIONALITY_SPEC.md:472` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_scenarios.py` | `current.json` updated on attempt start/end, at least by completion. |
| `FUNCTIONALITY_SPEC.md:495` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scoreboard_derivation_statuses.py` | Scoreboard derivable without raw traces (uses pointers/state). |
| `FUNCTIONALITY_SPEC.md:500` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_batch_effective_defaults_scoreboard_override.py` | Stuck threshold default 45m and clamped/validated `>=30m`. |
| `FUNCTIONALITY_SPEC.md:523` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_batch_effective_defaults_scoreboard_override.py` | Batch scoreboard surfaces `heartbeat_stale_after_seconds` (clamped `>=1800`). |
| `FUNCTIONALITY_SPEC.md:503` | N/A | — | — | Spec subsection heading (scoreboard contract). |
| `FUNCTIONALITY_SPEC.md:518` | N/A | — | — | Spec “Scoreboard must include (minimum)” heading. |
| `FUNCTIONALITY_SPEC.md:540` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scoreboard_derivation_statuses.py` | Step status derivation rules implemented in `_derive_step_status`. |
| `FUNCTIONALITY_SPEC.md:549` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scoreboard_derivation_statuses.py` | Resume visibility included via `resume_steps[]` and `resume_from_resolved`. |
| `FUNCTIONALITY_SPEC.md:552` | PASS | `parallelhassnes/scoreboard/scoreboards.py` | `tests/test_scenarios.py` | Multi-batch scoreboard computed and written to `runs/_system/scoreboard.system.json`. |
| `FUNCTIONALITY_SPEC.md:555` | N/A | — | — | Spec “System view must include (minimum)” heading. |
| `FUNCTIONALITY_SPEC.md:569` | PASS | `parallelhassnes/api/app.py`, `parallelhassnes/harness/harness.py` | `tests/test_scenarios.py` | Drill-down: system scoreboard links to per-batch scoreboard (API + on-disk artifacts). |
| `FUNCTIONALITY_SPEC.md:659` | PASS | `parallelhassnes/scoreboard/scoreboards.py`, `parallelhassnes/remediation/auto.py` | `tests/test_auto_remediation.py` | “Stuck” condition uses stale heartbeat threshold. |
| `FUNCTIONALITY_SPEC.md:671` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_resume_from_selectors_execution.py` | Every attempt has a resume base (`codex_home/`) once finished. |
| `FUNCTIONALITY_SPEC.md:672` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_resume_does_not_copy_codex_auth.py` | Resume copies session store; source resume base is never mutated. |
| `FUNCTIONALITY_SPEC.md:676` | PASS | `parallelhassnes/scheduler/scheduler.py`, `parallelhassnes/runner/runner.py` | `tests/test_runner_locality.py` | Resume requires source attempt readability or transfer cache. |
| `FUNCTIONALITY_SPEC.md:677` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_runner_locality.py` | Same-runner is sufficient; shared FS may allow cross-runner. |
| `FUNCTIONALITY_SPEC.md:681` | PASS | `parallelhassnes/runner/runner.py` | `tests/test_resume_explicit_thread_id_preference.py` | If multiple threads possible and a thread ID is known, prefer explicit resume. |
| `FUNCTIONALITY_SPEC.md:695` | PASS | `parallelhassnes/runner/runner.py`, `parallelhassnes/resume/selectors.py` | `tests/test_resume_from_selectors_execution.py` | Step2 resume uses `current.json` + resume bases, not raw events. |
| `FUNCTIONALITY_SPEC.md:702` | PASS | `parallelhassnes/workspace/policy.py` | `tests/test_workspace_policy_isolated.py` | Workspace policy is selectable per job (shared vs isolated). |
| `FUNCTIONALITY_SPEC.md:708` | N/A | — | — | Spec subsection heading (workspace vs resume continuity). |
| `FUNCTIONALITY_SPEC.md:711` | PASS | `parallelhassnes/workspace/policy.py` | `tests/test_workspace_policy_isolated.py` | Resume affects conversation state only; filesystem is “latest workspace”. |
| `FUNCTIONALITY_SPEC.md:728` | PASS | `parallelhassnes/api/app.py`, `parallelhassnes/api/auth.py` | `tests/test_scenarios.py` | API mode is optional; when enabled it exposes submit/query/tail/cancel/trigger operations. |
| `FUNCTIONALITY_SPEC.md:757` | N/A | — | — | Spec clarification (“common misread”), not a runtime requirement. |
| `FUNCTIONALITY_SPEC.md:763` | N/A | — | — | Spec editing checklist item. |
| `FUNCTIONALITY_SPEC.md:766` | N/A | — | — | Spec editing checklist item. |
