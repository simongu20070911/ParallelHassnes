# ParallelHassnes — Full Implementation Plan (Step-by-step with tickboxes)

Checkbox legend (implementation tracking)

* [ ] Not implemented

* [~] Scaffolded (interfaces/files exist, not end-to-end)

* [x] Implemented

---

## Coverage map (spec → plan) (notes)

- Spec §§1–5 (purpose/roles/IDs/invariants): Phase 0 + enforcement across Phases 1–2, 7
- Spec §6 (Launch Table) + §6.4 (harness config): Phases 3–4
- Spec §7 (schedule/execute): Phases 6–10
- Spec §8 (logging + lifecycle): Phases 1–2, 5, 7
- Spec §9 (scoreboards + stuck): Phase 11 (+ defaults in Phase 3)
- Spec §10 (resume): Phase 8 (+ locality in Phase 6)
- Spec §11 (workspace policy): Phase 7 must record selected policy in `meta.json`
- Spec §12 (interfaces): Phase 12
- Spec §13 (versioning/compat): Phases 0.2 + 2
- Spec §14 (open choices): treat as explicit decision gates (see notes in relevant phases)

---

## Phase 0 — Repo scaffolding, terminology lock, and “source of truth” contracts

### 0.1 Lock canonical terms + IDs everywhere

* [x] Create a **Terminology constants doc** that mirrors the spec’s canonical nouns (Batch/Job/Step/Run Attempt/etc.)

* [x] Define the **minimum correlation set** (`batch_id`, `job_id`, `step_id`, `run_id`, `runner_id`, optional `codex_thread_id`) as a shared “required fields” contract

* [x] Add a “no synonym drift” lint/check (docs-level) so code/docs use the canonical terms consistently

### 0.2 Define spec + schema versioning strategy

* [x] Establish `spec_version` for Launch Table normalization

* [x] Establish `schema_version` for `final.json` Run Report (baseline schema + optional per-job schema)

* [x] Define compatibility rules (reader must accept older minor versions; reject unknown major versions)

### 0.3 Create the codebase module skeleton (no framework/language assumptions)

* [x] Scaffold modules (interfaces + folders) for: config, storage, scheduler, runner protocol, codex invocation, scoreboard, GC/retention, interfaces (API/fs-queue)

* [x] Add a single “integration test harness” entrypoint module that can run scenario simulations (exec success / retry / resume)

---

## Phase 1 — Directory layout (runtime) + path derivation utilities

### 1.1 Runtime directory layout (exactly matches the spec)

* [~] Implement the required on-disk tree (created lazily as needed):

```text

runners/

<runner_id>/

health.json

runs/

_system/

harness_config.json

harness_config_versions/

<harness_config_version>.json

<batch_id>/

batch_meta.json

<job_id>/

current.json

steps/

<step_id>/

attempts/

<run_dir_name>/

codex_home/

meta.json

state.json

final.json

final.txt

handoff.json

codex.events.jsonl

git_head.txt

git_status.txt

git_diff.patch

files_touched.json

```

### 1.2 Deterministic path derivation (no directory scanning needed)

* [x] Create a “Path Resolver” module that deterministically computes:

* [x] batch folder: `runs/<batch_id>/`

* [x] job folder: `runs/<batch_id>/<job_id>/`

* [x] step folder: `runs/<batch_id>/<job_id>/steps/<step_id>/`

* [x] attempts folder: `.../attempts/`

* [x] attempt dir: `.../attempts/<run_dir_name>/` (must include `run_id`)

* [x] attempt’s `codex_home`: `<attempt_dir>/codex_home/`

* [x] Enforce `<run_dir_name>` format:

* [x] lexicographically sortable by start time (recommended): `YYYYMMDDTHHMMSSZ_<run_id>`

* [x] includes `run_id` always

Notes:
- Given only a `run_id`, locating the attempt directory must not require filesystem scanning. Achieve this by (a) using `<run_id>/` as the directory name, or (b) maintaining a required `current.json.steps[*].by_run_id` mapping (see Phase 5).

### 1.3 Atomic write primitives (critical for `state.json` + config snapshots)

* [x] Implement `write_atomic_json(path, obj)` = write temp + fsync + rename

* [x] Implement `write_once_json(path, obj)` = fail if exists

* [x] Implement `append_only(path, line)` = append with flush semantics

* [x] Implement reader tolerances:

* [x] scoreboard readers treat missing `state.json` as `queued/initializing`

* [x] readers never assume `current.json` is strongly consistent

---

## Phase 2 — Data contracts (schemas) and validators

### 2.1 Define JSON schemas (or equivalent validation contracts)

* [x] Launch Table normalized form schema (conceptual fields in spec)

* [x] `runs/_system/harness_config.json` schema

* [x] `runs/<batch_id>/batch_meta.json` schema

* [x] `runs/<batch_id>/<job_id>/current.json` schema

* [x] attempt `meta.json` schema

* [x] attempt `state.json` schema (status transitions + required timestamps)

* [x] Run Report baseline schema (`final.json`)

* [x] `status: ok|failed|needs_attention`

* [x] `summary`

* [x] `files_read[]`, `files_written[]`

* [x] `artifacts[]`

### 2.2 Implement validators (must match spec rules)

* [x] `batch_goal_summary` validation:

* [x] required

* [x] **whitespace-delimited token count > 150**

* [x] Step DAG validation:

* [x] detect cycles in `depends_on[]` and implied dependencies from `resume_from`

* [x] Reference validation:

* [x] prompts/schemas exist or are retrievable by the chosen interface mode

* [x] Run Report validation at attempt end:

* [x] if schema validation fails ⇒ mark attempt `needs_attention`, preserve raw `final.txt`

### 2.3 Mutability/lifecycle audit enforcement

* [x] Enforce file class behaviors:

* [x] `meta.json` write-once at attempt start

* [x] `final.json/final.txt/handoff.json/git_*` write-once at attempt end

* [x] `state.json` atomic snapshot during run; terminal state frozen forever

* [x] `codex.events.jsonl` append-only (optional)

* [x] `current.json` mutable index (updated by harness only)

---

## Phase 3 — Harness configuration snapshots (`runs/_system/harness_config.json`)

### 3.1 Central config snapshot writer + versioned archive

* [x] On harness startup (and on config changes), write:

* [x] `runs/_system/harness_config.json` (atomic snapshot)

* [x] `runs/_system/harness_config_versions/<harness_config_version>.json` (write-once)

* [x] Ensure `harness_config.json` includes required minimum fields:

* [x] `harness_config_version`, `written_at`

* [x] interfaces: `api_mode{enabled,auth_mode}`, `filesystem_queue_mode{enabled,...}`

* [x] defaults: `execution_policy`, `timeouts`, `retries`, `retention_policy`

* [x] scoreboard defaults: `heartbeat_stale_after_seconds` (default 2700; must be >= 1800)

* [~] runner pool/capabilities (conceptual)

* [x] limits: max batch/job/step sizes

* [x] Enforce “must not include secrets”:

* [x] validate/denylist obvious secret-like keys/values in `harness_config.json` and refuse to write invalid config

### 3.2 Effective defaults computation (per batch)

* [x] Implement “policy merge”:

* [x] batch submission overrides only allowed fields

* [x] compute “effective defaults” for execution behavior

* [x] Record the `harness_config_version` + effective defaults inside `batch_meta.json`

---

## Phase 4 — Batch submission (Launch Table → `batch_meta.json`)

### 4.1 Submit flow (core happy path)

* [x] Accept Launch Table (from API or filesystem queue)

* [x] Validate required fields and limits

* [x] Assign IDs if missing:

* [x] `batch_id` (if absent)

* [x] `job_id` per job (stable within batch)

* [x] ensure each step has stable `step_id`

* [x] Normalize the Launch Table into an immutable snapshot

* [x] Instruction packs (if used in Launch Table):

* [x] either expand packs into step prompt text during normalization, or explicitly reject packs as out-of-scope for MVP (do not silently ignore)

* [x] Write `runs/<batch_id>/batch_meta.json` (write-once)

* [x] Return acknowledgement: `batch_id`, accepted `job_id`s

### 4.2 Batch metadata contents (must match spec)

* [x] `batch_meta.json` includes:

* [x] `batch_id`, `submitted_at`, `harness_config_version`

* [x] `batch_goal_summary` (full text)

* [x] normalized jobs/steps including dependency wiring + `resume_from`

* [x] effective defaults/policies

---

## Phase 5 — Job/step state indexing (`current.json`) and attempt lifecycle events

### 5.1 Create and maintain `current.json` per job

* [x] Create `runs/<batch_id>/<job_id>/current.json` on first lifecycle event

* [x] Update `current.json` on:

* [x] attempt start (set `latest`)

* [x] attempt end (update `latest`, update `latest_successful` if succeeded)

* [x] Include required fields:

* [x] `batch_id`, `job_id`, `updated_at`

* [x] `steps[step_id].latest{run_id, attempt_dir, resume_base_dir, codex_thread_id?}`

* [x] `steps[step_id].latest_successful{...}` if different

* [x] Required (to support `resume_from.run_id` selectors without scanning):

* [x] `steps[step_id].by_run_id[run_id] -> {attempt_dir, resume_base_dir, status, ended_at}`

### 5.2 Authoritative attempt artifacts are leaf files

* [x] Treat `meta.json` + `state.json` + `final.*` as authoritative per attempt

* [x] Treat `current.json` as a pointer/index that may be briefly stale

---

## Phase 6 — Scheduler (dependency resolution + concurrency + affinity)

### 6.1 Build the scheduler’s internal model from `batch_meta.json`

* [x] Parse normalized jobs/steps into a DAG

* [x] Compute “runnable” steps:

* [x] all `depends_on[]` satisfied (succeeded)

* [x] if `resume_from` exists: source attempt resolved and resume base available

* [x] Track step-level derived statuses: blocked/ready/running/succeeded/failed/needs_attention/canceled

### 6.2 Concurrency controls

* [~] Enforce global `concurrency` cap per batch (and/or system-wide if configured)

* [x] Enforce per-runner capacity constraints (from runner health snapshots or config)

* [x] Optional: per-repo/per-directory concurrency limits

### 6.3 Runner selection + affinity

* [x] Implement `runner_affinity` policy, especially:

* [x] “pin to runner that ran step1” for resume stability

* [x] Ensure locality requirement:

* [x] runner can read the source attempt dir (`runs/...`) needed for resume base copying

* [x] (if not) implement a resume-base transfer mechanism (copy to destination runner)

### 6.4 Queue semantics

* [x] Maintain a queue of runnable steps

* [x] Dispatch to runners based on capacity + affinity + fairness

* [x] Persist enough scheduling state so harness can restart and continue (no lost work)

---

## Phase 7 — Runner (worker) implementation: attempt directories + Codex CLI execution

### 7.1 Runner identity + health reporting

* [x] Each runner has a stable `runner_id`

* [x] Periodically write `runners/<runner_id>/health.json` (atomic snapshot) including:

* [x] `runner_id`, `last_seen_at`

* [x] capacity + current load

* [x] pressure signals (disk/memory if available)

* [x] drain mode (accepting new work or not)

### 7.2 Attempt directory creation (single source of truth)

For each scheduled run:

* [x] Create attempt dir: `runs/<batch_id>/<job_id>/steps/<step_id>/attempts/<run_dir_name>/`

* [x] Create `<attempt_dir>/codex_home/` directory

* [x] Write `meta.json` (write-once at attempt start) including:

* [x] IDs: `batch_id`, `job_id`, `step_id`, `run_id`, `runner_id`

* [x] invocation type: `exec` vs `resume`

* [x] prompt reference (`prompt_ref` or `prompt_sha256`)

* [x] policy snapshot + environment snapshot (conceptual)

* [x] resume linkage fields when applicable (see Phase 8)

* [x] Record workspace continuity choices (per spec §11):

* [x] `workspace_policy` (shared vs isolated vs job-persistent, etc.)

* [x] `workspace_root` (or equivalent path)

* [x] Record Codex CLI version in `meta.json` (e.g., `codex_cli_version`)

### 7.3 Minimal real-time status (`state.json`)

* [x] Initialize `state.json` as `running` with:

* [x] `status: running`, `started_at`, `last_heartbeat_at`

* [x] optional `current_item`

* [x] Heartbeat updates:

* [x] update `state.json` at least every 15 minutes while running

* [x] default stuck threshold compatibility: scoreboard stale threshold default 45 min (2700s)

### 7.4 Codex invocation (non-interactive)

* [x] Spawn Codex CLI non-interactively:

* [x] `codex exec ... --output-schema <schema_ref>` (or equivalent) to produce Run Report

* [x] Set `CODEX_HOME=<attempt_dir>/codex_home/`

* [x] Credential provisioning for attempt-local `CODEX_HOME`:

* [x] prefer env-injected credential when available (avoid writing secrets into runs store)

* [x] otherwise make existing runner-local CLI auth available to the attempt-local `CODEX_HOME` (recommended: symlink a runner-local credential store; do not copy secrets into the run attempt directory)

* [x] Ensure repo context discovery (AGENTS.md) comes from the job working directory as intended

* [x] Capture outputs:

* [x] `final.json` (schema output) at attempt end

* [x] `final.txt` (raw final text) optional but recommended

* [x] optional `codex.events.jsonl` append-only if auditing enabled

### 7.5 Repo state artifacts (optional but recommended)

At attempt end (especially when code changes are expected):

* [x] Write `git_head.txt`, `git_status.txt`, `git_diff.patch` (write-once)

* [x] Write `files_touched.json` best-effort

### 7.6 Attempt finalization (terminal state)

* [x] On process exit, finalize `state.json` with:

* [x] terminal `status` ∈ {succeeded, failed, canceled, needs_attention}

* [x] `ended_at`, `exit_code` (if available)

* [x] Enforce attempt-scoped state transitions:

* [x] `queued -> running -> terminal` (and optional `queued -> canceled`)

* [x] once terminal, status must never change again for that run attempt

* [x] `errors[]` summary strings

* [x] `artifacts[]` manifest entries

---

## Phase 8 — Resume workflows (copy-on-resume + linkage metadata)

### 8.1 Resume resolution in harness (before dispatch)

For a step with `resume_from`:

* [x] Treat `resume_from` as an implied dependency (must not run early)

* [x] Resolve source attempt selector:

* [x] default `latest_successful`

* [x] allow `latest` or explicit `run_id`

* [x] Resolve resume base reference:

* [x] source attempt’s `<source_attempt_dir>/codex_home/`

* [~] optional `codex_thread_id` if known/captured

Notes:
- Primary resume mechanism is `resume --last` with isolated per-attempt `CODEX_HOME`; `codex_thread_id` is optional and mainly for correlation or explicit `resume <id>` when needed.

### 8.2 Runner behavior for resume (must not mutate source)

* [x] Copy source resume base dir into new attempt’s `<attempt_dir>/codex_home/`

* [x] Execute resume via one of:

* [x] `codex exec resume --last` (recommended when per-attempt isolated stores are used)

* [x] `codex exec resume <codex_thread_id>` when explicit thread is required

### 8.3 Resume linkage in `meta.json`

* [x] Record:

* [x] `invocation: resume`

* [x] `parent_run_id`

* [x] `resume_from.step_id`

* [x] resolved source `run_id`

* [x] resolved resume base reference (path copied from)

* [~] `codex_thread_id` if known

* [x] If `codex_thread_id` capture is enabled:

* [x] parse it from Codex execution output when available (e.g., by consuming a JSON event stream) and propagate into attempt `meta.json` + job `current.json`

---

## Phase 9 — Retries (new run attempts, policy-driven)

### 9.1 Retry policy evaluation

* [x] Per-step retry policy:

* [x] max attempts

* [x] backoff strategy (conceptual)

* [x] retry mode: fresh thread vs resume same thread (if enabled/appropriate)

### 9.2 Retry execution semantics

* [x] A retry always creates a **new** attempt directory and **new** `run_id`

* [x] Preserve history (never overwrite an attempt dir)

* [x] Update `current.json`:

* [x] `latest` points to retry attempt

* [x] `latest_successful` updates only when a retry succeeds

### 9.3 Manual overrides (orchestrator-driven)

* [x] Provide operations to:

* [x] force retry now

* [x] requeue on same/different runner (respecting locality constraints)

* [x] mark failed permanently (batch continues where possible)

---

## Phase 10 — Cancellation (safe stop + correct terminal state)

### 10.1 Cancel request handling

* [x] Expose cancel operation through the interface(s)

* [x] Runner cancels:

* [x] terminate Codex process (and spawned subprocesses if applicable)

* [x] finalize `state.json.status = canceled`

* [x] preserve partial artifacts/logs

### 10.2 Cancel race conditions

* [x] If cancel occurs before start:

* [x] allow `queued -> canceled`

* [x] If cancel occurs during running:

* [x] ensure terminalization happens exactly once

---

## Phase 11 — Scoreboards (system view + batch view) without trace parsing

### 11.1 Per-batch scoreboard computation (contract-driven)

Inputs allowed:

* `batch_meta.json`, `current.json`, attempt `state.json`

* optional: attempt `final.json` for display only

* optional: runner `health.json`

Implementation tasks:

* [x] Compute batch header:

* [x] `batch_id`, `submitted_at`, `computed_at`

* [x] show full `batch_goal_summary`

* [x] totals: `jobs_total`, `steps_total`

* [x] include `heartbeat_stale_after_seconds`

* [x] Compute derived step statuses using the spec rules

* [x] Produce lists:

* [x] Attention list: stuck → needs_attention → failed

* [x] Running list: include `current_item`, `seconds_since_last_heartbeat`, duration

* [x] Blocked list: include unmet dependencies + missing resume base reasons

* [x] Failures list: include host outcome + optional agent outcome

### 11.2 “Stuck” detection

* [x] Mark as stuck when:

* [x] `state.status == running` and `now - last_heartbeat_at > threshold`

* [x] threshold default 2700 seconds; must be >= 1800

### 11.3 Multi-batch “system view”

* [x] One record per batch:

* [x] `batch_id`, `submitted_at`, `batch_goal_summary_preview`

* [x] totals + counts by derived status

* [x] `running_steps` count

* [x] `attention_steps` count (stuck + needs_attention + failed)

* [x] Ordering:

* [x] attention first, then running, then submitted_at desc

### 11.4 Optional stuck auto-remediation executor (default disabled)

* [x] Add a policy-driven executor that can (optionally) take actions for stuck/failed runs:

* [x] default: disabled; only surface `stuck` in the scoreboard

* [x] if enabled: execute one of {cancel+retry, recovery resume, requeue} and record the action taken in artifacts/state

---

## Phase 12 — Interfaces (API mode + filesystem queue mode)

### 12.1 Minimal operations (must exist regardless of interface style)

* [x] Submit batch

* [x] List/query batch status (system scoreboard)

* [x] Fetch per-run artifacts by name (given attempt_dir or IDs)

* [x] Stream/tail per-run events (when enabled)

* [x] Cancel a run

* [x] Trigger retry/resume steps

### 12.2 Filesystem queue mode (MVP-friendly)

* [x] Define input drop location(s) for Launch Tables

* [x] Define output expectations (IDs returned via ack file or written into `batch_meta.json`)

* [x] Ensure idempotency (re-dropping same Launch Table doesn’t create duplicates unintentionally)

### 12.3 API mode (optional but first-class per spec)

* [x] Define endpoints/operations conceptually:

* [x] submit

* [x] get scoreboard (system + batch)

* [x] get job pointers (`current.json`)

* [x] get attempt state/meta/final

* [x] tail events

* [x] cancel

* [x] trigger retry/resume

* [x] Implement auth mode per `harness_config.json` (`none|local_trust|token` conceptually)

---

## Phase 13 — Retention & garbage collection (GC)

### 13.1 Retention policy model

* [x] Support per batch/job retention settings:

* [x] keep raw events N days

* [x] keep reduced state + final outputs longer

* [x] optional compression for old raw logs

### 13.2 Safety rules (must not break resume)

* [x] GC must never delete:

* [x] `final.*` and `handoff.json` for successful runs (unless explicitly configured)

* [x] any `codex_home/` that is still a legal resume base for a not-yet-terminal dependent step

* [x] any `resume_base_dir` currently referenced by `current.json` in an open batch

### 13.3 Batch finalization concept

* [x] Define “batch closed/finalized” state (no new retries/resumes will be scheduled)

* [x] Only after batch finalized may GC apply more aggressive policies (still respecting configured invariants)

---

## Phase 14 — Recovery & correctness hardening

### 14.1 Crash safety

* [x] Harness restart:

* [x] rehydrate batch state from `batch_meta.json` + `current.json` + attempt `state.json`

* [x] detect orphaned “running” attempts (no heartbeat) and surface as stuck

* [x] Runner restart:

* [x] continue heartbeats if process is still running

* [x] if process is gone, finalize attempt as failed/needs_attention with a clear reason

### 14.2 Concurrency safety

* [x] Ensure attempt directory uniqueness under parallel starts

* [x] Prevent two runners from executing the same step concurrently unless explicitly allowed

* [x] Ensure `current.json` updates are atomic and last-writer-wins safely (with `updated_at`)

### 14.3 Determinism around resume selection

* [x] Implement selector resolution deterministically:

* [x] `latest_successful` chooses most recent *successful* attempt by ended_at

* [x] `latest` chooses most recent attempt by started_at/dir sort

* [x] explicit `run_id` must exist in `by_run_id` or be discoverable by path derivation rules

---

## Phase 15 — Test plan (must include the spec’s scenario simulations)

### 15.1 Scenario simulation tests (required)

* [x] Exec success flow (8.6.1)

* [x] verify file creation order + immutability rules

* [x] Retry flow (8.6.2)

* [x] attempt1 failed, attempt2 succeeded; pointers correct

* [x] Resume flow (8.6.3)

* [x] step2 copies step1 resume base; source remains unchanged; linkage correct

### 15.2 Scoreboard derivation tests

* [x] Verify derived statuses for:

* [x] blocked due to dependencies

* [x] blocked due to missing resume base

* [x] running with fresh heartbeat

* [x] stuck detection beyond threshold

* [x] needs_attention from schema validation failure

### 15.3 Property/invariant tests

* [x] Attempt dir immutability after terminal state

* [x] Allowed `state.json` status transitions only

* [x] `current.json` always points to existing attempt dirs (or degrades gracefully)

---

# Modularization blueprint (code layout)

This is a **conceptual** codebase tree to keep responsibilities crisp (no language/framework implied):

```text

parallelhassnes/

docs/

spec.md # your spec (source of truth)

run_report.schema.json # baseline schema for final.json

examples/

launch_table.example.json

batch_meta.example.json

current.example.json

state.example.json

meta.example.json

core/

ids/ # batch_id/job_id/run_id generation + parsing

paths/ # runs/ directory resolver + canonical naming

atomic_io/ # atomic JSON snapshot writes, write-once helpers, append-only helpers

time/ # timestamp helpers (UTC ISO8601, sortable strings)

validation/ # schema + invariant validators

config/

harness_config/ # load/validate/write harness_config snapshots + versioning

policy_merge/ # compute “effective defaults” per batch

storage/

runs_store/ # create folders, list attempts, read/write batch_meta/current/meta/state/final

artifact_store/ # artifact lookup and streaming/tailing helpers

scheduling/

dag/ # dependency graph builder + cycle detection

queue/ # runnable queue + fairness

affinity/ # runner affinity rules

dispatcher/ # assign runnable work to runner(s)

runner/

runtime/ # runner main loop + job execution lifecycle

health/ # health.json writer

codex/

invoke_exec/ # spawn codex exec, capture outputs, set CODEX_HOME

invoke_resume/ # resume --last or resume <id>

events/ # optional codex.events.jsonl capture

workspace/ # shared vs isolated workspace policy

repo_state/ # git_head/status/diff capture

cancellation/ # process tree termination + cleanup

postprocess/ # validate final.json, mark needs_attention on schema failure

orchestration/

submission/ # accept Launch Tables, normalize, write batch_meta.json

pointers/ # current.json updates on lifecycle events

retries/ # retry policy evaluation

resume_resolution/ # resolve resume_from selectors and locate resume bases

scoreboard/

batch_view/ # compute per-batch view (9.1.1)

system_view/ # compute multi-batch view (9.1.2)

stuck_detection/ # heartbeat stale logic

interfaces/

api_mode/ # submit/query/cancel/fetch/tail operations

fs_queue_mode/ # filesystem input queue + acknowledgements

retention/

gc/ # retention policy + protected resume bases

compression/ # optional raw log compression

tests/

scenarios/ # exec success, retry, resume

invariants/ # lifecycle/mutability rules

scoreboard/ # derived status correctness

```

---

# Implementation order recommendation (to stay sane while building “full”)

If you want the most reliable path to “working end-to-end” quickly (without skipping anything important), implement in this order:

1. [x] Atomic IO + path resolver + schemas

2. [x] Harness config snapshotting (`runs/_system/...`)

3. [x] Batch submission → `batch_meta.json`

4. [x] Runner exec happy-path (attempt dir, meta/state, codex exec, final.json)

5. [x] `current.json` pointers

6. [x] Scheduler: dependency + concurrency + dispatch

7. [x] Scoreboards (system + batch)

8. [x] Resume (copy-on-resume)

9. [x] Retries + cancellation

10. [x] Retention/GC + recovery hardening

---
