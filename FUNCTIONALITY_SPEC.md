# ParallelHassnes — Functionality Spec (Workflow + Logging)

## 1) Purpose
ParallelHassnes is a batch execution harness for running many Codex CLI jobs concurrently (“agents”), capturing their outputs and artifacts (optionally including full event traces), and supporting multi-step workflows (including `resume`) with reliable monitoring and recovery.

This spec describes **what the system does** (behavior and state), not how it is implemented (no framework/language choices).


## 2) Goals
- Accept large batch submissions (“Launch Tables”) that describe many jobs, optionally multi-step.
- Schedule jobs across one or more runners with concurrency controls and job affinity.
- Execute Codex CLI non-interactively (`codex exec`) while capturing a **structured final Run Report** (via `--output-schema`) plus minimal run metadata for monitoring and resume.
- Provide strong observability:
  - “Scoreboard” status without reading full traces
  - Deep-dive trace replay/tailing on demand (when enabled)
- Support **resume** workflows safely and deterministically (e.g., 2-step jobs: step2 resumes step1’s thread).
- Provide a stable artifact contract for downstream automation.

## 3) Non-Goals (MVP boundary)
- No requirement for a GUI (text-first monitoring is acceptable).
- No requirement for multi-tenant auth/quotas (single operator is acceptable).
- No requirement to minimize token/cost (throughput and control preferred).
- No requirement to infer “intent” from raw traces; orchestration uses explicit job/step contracts.

## 4) System Roles
- **Orchestrator**: constructs Launch Tables, submits batches, decides follow-ups (retries, resume steps), and consumes reduced state + artifacts.
- **Harness**: accepts batch specs, maintains job/run records, schedules work to runners, and serves status/events/artifacts.
- **Runner (worker code)**: a software process/service (written by us) that executes jobs on a host that has Codex CLI and required tools; creates attempt directories, sets `CODEX_HOME`, spawns `codex exec`, and writes logs/artifacts to disk in real time.
- **Codex Agent (LLM)**: the model-driven agent running inside the Codex CLI process. It proposes actions and produces `final.json` output, but it does not control runner-level concerns like process spawning, filesystem layout, or `CODEX_HOME`.
- **Postprocessor (optional)**: validates and normalizes the agent’s final Run Report (schema output) and can add host-derived metadata (e.g., exit code, timestamps) without changing the agent’s narrative content.

## 5) Core Concepts / Identifiers
### 5.1 Entities
- **Batch**: a collection of jobs submitted together.
- **Job**: a logical unit of work (may contain one or more steps).
- **Step**: an ordered stage within a job (e.g., step1 diagnose, step2 implement).
- **Run** (a.k.a. Run Attempt): one invocation of Codex for a particular step (initial or resumed), including retries.
- **Session / Thread**: Codex conversation state identified by a UUID. In `codex exec --json` this is emitted as `thread.started` with field `thread_id`; in `codex exec resume` this is the `SESSION_ID`.
- **Artifact**: a file or structured output produced by a run (final response, schema output, diffs, reports, etc.).
- **Runner**: an execution node. A job may be pinned to a runner for resume stability.

### 5.1.1 Terminology (canonical)
This spec uses the following terms consistently; avoid synonyms and overloaded nouns.

| Term                                      | Meaning                                                                                                                                                                                                                                |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `runs store` (`runs/`)                    | Top-level directory tree where ParallelHassnes writes batch/job/step/run attempt artifacts.                                                                                                                                            |
| job folder                                | `runs/<batch_id>/<job_id>/` (contains `current.json` + per-step folders).                                                                                                                                                              |
| step folder                               | `runs/<batch_id>/<job_id>/steps/<step_id>/` (contains `attempts/`).                                                                                                                                                                    |
| run attempt                               | One Codex invocation for one step (identified by `run_id`). Retries and resumes create new run attempts.                                                                                                                               |
| run attempt directory                     | Leaf folder for one run attempt: `.../attempts/<run_dir_name>/` (contains `meta.json`, `state.json`, `final.json`, etc.).                                                                                                              |
| session store (`CODEX_HOME`)              | Directory where Codex persists conversation state used by `codex exec resume` / `resume --last`. In this spec, each run attempt uses an isolated session store at `.../codex_home/` (by setting `CODEX_HOME`).                         |
| resume base (session snapshot)            | Frozen snapshot of a session store at the end of a run attempt (the completed attempt’s `codex_home/`), used as the source for future `resume_from` steps (copied into the new run’s session store).                                   |
| Codex thread ID (`codex_thread_id`)       | UUID used by `codex exec resume <SESSION_ID>` to resume a conversation. In `codex exec --json`, this appears in `thread.started.thread_id`. Optional in this spec when using isolated per-attempt session stores with `resume --last`. |
| Run Report (`final.json`)                 | Schema-conformant JSON output produced by the agent; primary step-to-step handoff.                                                                                                                                                     |
| reduced state (`state.json`)              | Host-derived run status written by the runner for real-time monitoring/scoreboard.                                                                                                                                                     |
| pointer (`current.json`)                  | Mutable index file summarizing “latest run(s)” for a job so the orchestrator can avoid scanning directories.                                                                                                                           |
| harness config (`harness_config.json`)    | Centralized harness configuration snapshot written at a well-known path (`runs/_system/harness_config.json`) so the orchestrator can read operator-level defaults/limits without contacting the harness.                               |
| batch meta (`batch_meta.json`)            | Write-once batch submission record (Launch Table snapshot + assigned IDs + defaults); does not change for retries/resumes.                                                                                                             |
| batch goal summary (`batch_goal_summary`) | Required submit-time human description (more than 150 words) describing the batch goal; stored in `batch_meta.json` and shown as a preview in the multi-batch scoreboard.                                                              |
| scoreboard (batch view)                   | Orchestrator-facing view for a single batch, derived from `batch_meta.json` + `current.json` + attempt `state.json` (no raw trace parsing).                                                                                            |
| scoreboard (system view)                  | Orchestrator-facing multi-batch triage view (one row per batch), derived from each batch’s `batch_meta.json` + per-job `current.json`.                                                                                                 |

### 5.2 Required IDs (minimum correlation set)
- `batch_id`: unique ID for a submitted batch.
- `job_id`: unique ID within the batch (stable across retries).
- `step_id`: stable identifier within the job (e.g., `"step1"`, `"step2"`).
- `run_id`: unique per run attempt (changes on retry/resume invocation).
- `codex_thread_id` (a.k.a. `thread_id`, CLI `SESSION_ID`): UUID used to resume the same Codex conversation state (optional when using isolated per-attempt session stores with `resume --last`).
- `runner_id`: identifies which runner executed the run.

### 5.3 Invariants
- A **run attempt directory** (the leaf folder for a specific `run_id`, e.g. `.../steps/<step_id>/attempts/.../`) becomes **immutable once finished**. During execution the runner may atomically update snapshot files (e.g., `state.json`) and append to log files (e.g., `codex.events.jsonl` when enabled).
- Parent “index” folders and pointers (e.g., adding new attempt directories, updating `current.json`) are allowed to change over time and are not considered immutable.
- Retries create a **new** `run_id` (history preserved).
- Resumes are always represented as new runs linked back to a parent run.

## 6) Launch Table (Batch Specification)
### 6.1 Intent
The Launch Table describes:
- shared defaults (policies, environment, working root),
- jobs to run,
- step sequencing/dependencies,
- prompts (inline or referenced),
- expected outputs (schema + artifacts),
- operational constraints (timeouts, retries, affinity).

### 6.2 Functional fields (conceptual)
The following are conceptual fields; exact serialization is not prescribed by this spec.

- **Batch metadata**
  - `batch_id` (optional; may be assigned by harness)
  - `batch_goal_summary` (required; more than 150 words; human-readable statement of what this batch is trying to achieve)
    - word counting rule (for validation): whitespace-delimited tokens
  - `labels` / `tags`
  - `concurrency` (global cap)
  - `retention_policy` (log/artifact retention window)

- **Defaults**
  - `working_root` (where jobs operate)
  - `execution_policy` (sandbox/approvals, web search enabled/disabled)
  - `timeouts` (default step timeout)
  - `retries` (default retry policy)

- **Instruction packs (optional)**
  - Named “packs” of shared instructions to reduce per-job duplication.
  - Jobs/steps may reference a `pack_id` plus local overrides.

- **Jobs**
  - `job_id` (or generated)
  - `labels`/`tags`
  - `working_directory` (relative to working_root)
  - `steps[]` with:
    - `step_id`
    - `prompt` (inline) or `prompt_ref` (file/blob reference)
    - `append_prompt` (for resumes / follow-ups)
    - `output_schema_ref` (optional)
    - `depends_on[]` (step dependencies)
    - `resume_from` (resume a prior Codex session as input to this step)
      - must identify a source step (at minimum): `resume_from.step_id`
      - must define a source attempt selector (default: `latest_successful`):
        - `latest_successful` (recommended default)
        - `latest` (may resume from a failed attempt)
        - `run_id` (explicit attempt)
      - may optionally supply an explicit `codex_thread_id` override (advanced)
      - must have a stable resume base for the selected source attempt (a session snapshot) so later steps can resume from any earlier step without mutating the source
      - `resume_from` implies a dependency: the harness must treat it as if the step also `depends_on` the source step
    - `timeout_seconds`, `retry_policy`
    - `runner_affinity` (e.g., “pin to runner that ran step1”)
    - `artifacts_expected[]` (names/types)

### 6.3 “Two-step wide” pattern
Common workflow:
- `step1`: initial `codex exec` that produces handoff artifact(s) (and implicitly creates a resumable session).
- `step2`: `codex exec resume --last` in an isolated per-attempt session store seeded from step1’s resume base (or `resume <SESSION_ID>` when using explicit IDs), with an appended prompt that consumes the handoff artifact(s).

### 6.4 Harness configuration (centralized)
The harness is a long-lived control-plane component; it needs centralized configuration so the orchestrator does not have to repeat operator-level defaults in every batch submission.

Functional requirements:
- Harness must materialize its centralized configuration as a well-known on-disk file that the orchestrator can read: `runs/_system/harness_config.json`.
- `runs/_system/harness_config.json` is an atomic snapshot (readable without contacting the harness) and must include a stable `harness_config_version`.
- The harness must preserve each `harness_config_version` as a write-once snapshot at `runs/_system/harness_config_versions/<harness_config_version>.json` for as long as any batch artifacts reference it.
- Centralized harness configuration defines operator-level defaults and limits, such as:
  - runs store root and runners root locations (or equivalents)
  - runner pool membership / runner capability flags (conceptual)
  - interfaces enabled (`api mode`, `filesystem queue mode`, or both)
  - API access policy / auth mode (if API mode is enabled)
  - default `execution_policy`, `timeouts`, and `retries`
  - maximum batch/job/step sizes (to prevent accidental overload)
  - retention defaults (how long to keep raw logs / diffs / run outputs)
  - scoreboard defaults such as `heartbeat_stale_after_seconds` (default 2700; must be >= 1800) and any stuck auto-remediation enablement
- Launch Tables may override only the subset explicitly allowed by harness policy; the harness must compute and record “effective defaults” for each batch in `batch_meta.json`.
- For reproducibility, `batch_meta.json` must include a `harness_config_version` identifier and the “effective defaults” sufficient to interpret the batch behavior later (conceptual; see 7.1.1).

### 6.4.1 `runs/_system/harness_config.json` contents (required, conceptual)
The harness config is intended to answer: “What modes is the harness running in, what are the defaults/limits, and how do I (the orchestrator) talk to it?”

**Must include (minimum):**
- `harness_config_version`
- `written_at`
- `interfaces`:
  - `api_mode`:
    - `enabled` (boolean)
    - `auth_mode` (conceptual; e.g., `none | local_trust | token`)
  - `filesystem_queue_mode`:
    - `enabled` (boolean)
    - queue roots/paths may be included as opaque strings (optional; conceptual)
- default policies (conceptual): `execution_policy`, `timeouts`, `retries`, `retention_policy`
- scoreboard defaults: `heartbeat_stale_after_seconds` (default 2700; must be >= 1800) and stuck auto-remediation enablement
- runner pool / capabilities (conceptual): enough to explain scheduling decisions (e.g., which runners exist and whether they are accepting work)
- limits (conceptual): maximum batch/job/step sizes and any other safety caps

**Must not include:**
- secrets (API tokens, private keys). The orchestrator and runners must obtain secrets from their own credential stores.

## 7) Scheduling & Execution Workflow
### 7.1 Batch submission
1. Orchestrator submits Launch Table to Harness.
2. Harness validates:
   - required fields present
   - `batch_goal_summary` is present and more than 150 words (whitespace-delimited token count)
   - step DAG is acyclic
   - referenced prompts/schemas exist (or are retrievable)
3. Harness assigns IDs (if missing) and creates initial records:
   - write `runs/<batch_id>/batch_meta.json` (write-once)
4. Harness returns an acknowledgement including `batch_id` and accepted `job_id`s.

### 7.1.1 `batch_meta.json` contents (required, conceptual)
`batch_meta.json` is an immutable snapshot of the batch submission after harness normalization (IDs filled, defaults applied).

**Must include (minimum):**
- `batch_id`
- `spec_version` (if used)
- submission timestamp (`submitted_at`)
- `harness_config_version` (the config version effective at submit time; referencable via `runs/_system/harness_config_versions/<harness_config_version>.json`)
- `batch_goal_summary` (required; more than 150 words)
- Launch Table snapshot or reference:
  - either inline normalized Launch Table, or
  - `launch_table_ref` + `launch_table_sha256`
- normalized `jobs[]` including each job’s `job_id`, `working_directory`, and `steps[]` with each step’s `step_id` and dependency wiring (`depends_on[]`, `resume_from` if present).
- resolved defaults/policies that materially affect execution (e.g., sandbox/approvals/search flags, timeouts, retry policy) as selected by the harness for this batch.

**Must not include (by design):**
- runtime attempt results, `run_id` lists, or per-attempt status (those live in attempt `meta.json` / `state.json` and job pointers like `current.json`)
- the step1-produced `codex_thread_id` for future resumes (unknown at submission time; may be recorded later if captured, e.g. via raw events, via attempt `meta.json` and propagated via `current.json`)

**Resume relationship:**
- A resume is normally a new run attempt within the same `batch_id` (no `batch_meta.json` change). A new batch (new `batch_id`) only exists if the orchestrator submits a new Launch Table.

### 7.2 Job queueing
- Jobs/steps enter a queue when all dependencies are satisfied.
- Scheduler selects runnable steps based on:
  - global concurrency
  - per-runner capacity
  - runner affinity (especially for resume)
  - optional per-repo/per-directory limits

### 7.3 Runner execution (single run)
For each scheduled run:
1. Runner creates a new run attempt directory (see Logging Layout).
2. Runner records `meta.json` (run identifiers, working directory, policy snapshot).
3. Runner starts Codex in non-interactive mode for the step, requesting a schema-conformant final output (“Run Report”).
   - `AGENTS.md` instructions are discovered from the job’s `working_directory` (and its parents) as part of Codex’s normal repo-context loading; this is independent of `CODEX_HOME`.
4. Runner maintains minimal real-time run status (process started/running/ended) suitable for a scoreboard.
5. On completion:
   - write the schema output (`final.json`) and/or the raw final text (`final.txt`) as configured
   - capture repo state artifacts (diff/status) as configured
   - finalize `state.json` with end time + exit status
6. (Optional) If deep auditing is enabled, the runner may also persist raw event streams, but orchestration must not depend on them.

### 7.4 Resume execution
Resume is treated as **a new run**:
1. Harness schedules a new run for `step2`.
2. Harness resolves `resume_from` to a concrete resume base:
   - select the source run attempt (e.g., `latest_successful` for that `resume_from.step_id`)
   - obtain a resume base reference: the source attempt’s session snapshot directory (e.g., `<source_attempt_dir>/codex_home/`), plus optional `codex_thread_id` if captured
3. Runner prepares a fresh session store for this run attempt by copying the resume base into the new attempt’s session store directory.
4. Runner resumes the prior session using one of:
   - `resume --last` when the session store is isolated per run attempt (recommended), or
   - an explicit `codex_thread_id` when required (advanced / non-isolated stores).
5. Runner executes the resume invocation, producing a new Run Report and new artifacts.
6. Linkage is recorded via `parent_run_id`, `resume_from.step_id`, and the resolved resume base reference (and `codex_thread_id` if known) in `meta.json`.

### 7.5 Retries
Retry policy is step-scoped:
- A failure creates a new run attempt (`run_id` changes).
- Retry behavior is configurable:
  - “fresh” (new thread) or “resume same thread” (if appropriate)
  - backoff strategy (conceptually)
  - max attempts
- Orchestrator can override: manual retry, requeue, or mark failed.

### 7.6 Cancellation
- Cancel requests should:
  - stop the Codex process (and any spawned subprocesses)
  - finalize run state as `canceled`
  - preserve partial logs/artifacts with clear status

## 8) Logging, State, and Artifact Contract
### 8.1 Principles
- **Final Run Report is the primary output**: orchestration reads a schema-conformant `final.json` produced by the agent.
- **Run status is minimal and host-derived**: orchestration can rely on process lifecycle signals (running/ended/exit code) without parsing full traces.
- **LLM self-reporting is not fully authoritative**: file read/write lists are defined as “explicitly inspected/modified by the agent” and may be incomplete; the orchestrator may verify via workspace inspection when necessary.
- **Everything is correlatable** via immutable IDs and linkage metadata.

### 8.1.1 Run Report contract (definition)
Each run produces a single schema-conformant JSON object (“Run Report”) as its final response. This Run Report is used as the primary handoff between steps and as the canonical summary for orchestration.

The system may use a standard baseline schema (e.g., `run_report.schema.json`) and optionally allow per-job schemas when richer structure is needed.

**Run Report fields (minimum):**
- `status`: `ok | failed | needs_attention`
- `summary`: short human-readable summary
- `files_read`: list of repo-relative paths the agent explicitly inspected (best-effort)
- `files_written`: list of repo-relative paths the agent explicitly modified/created (best-effort)
- `artifacts`: list of additional outputs (paths + descriptions) produced by the run

**Schema validation:**
- The system must validate `final.json` against the expected schema.
- If validation fails, mark the run `needs_attention` (or `failed`) and preserve the raw final output for debugging.

**Best-effort scope note:**
- `files_read` and `files_written` are intended to capture explicit interactions, not implicit/transitive reads performed by compilers/build tools.

### 8.2 Run attempt directory layout (required)
Each run attempt writes to a dedicated attempt directory:

### 8.2.0 Directory tree (informative)
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

### 8.2.1 Run attempt directory naming (required)
- Run attempt directories must be uniquely named per run attempt to avoid collisions under parallelism.
- Run attempt directory locations must be discoverable from IDs without reading run-file contents (at minimum: `batch_id`, `job_id`, `step_id`, `run_id`).
- `<run_dir_name>` must include `run_id` (e.g., as a suffix like `..._<run_id>` or as the full directory name).
- Recommended canonical layout:
  - `runs/<batch_id>/<job_id>/steps/<step_id>/attempts/<run_dir_name>/`
  - where `<run_dir_name>` is lexicographically sortable by start time (recommended: `YYYYMMDDTHHMMSSZ_<run_id>`).
- Alternative (allowed): use `<run_id>/` as the directory name when `started_at` is reliably recorded in `state.json` and sortable listing by mtime is acceptable.

- `meta.json` (write-once; writer: runner; created at attempt start)
  - identifiers: `batch_id`, `job_id`, `step_id`, `run_id`, `runner_id`
  - invocation: `exec` vs `resume`
  - linkage:
    - `parent_run_id` (for resume)
    - `resume_from.step_id` and resolved source `run_id` (for resume)
    - resolved resume base reference (for resume; e.g., source attempt `codex_home/` path that was copied)
    - `codex_thread_id` (if known)
  - prompt reference: `prompt_ref` or `prompt_sha256`
  - policy snapshot: sandbox/approval/search flags (conceptual)
  - environment snapshot (conceptual): relevant versions, working directory, etc.

- `state.json` (atomic snapshot; writer: runner; updated periodically while `running`; frozen at attempt end)
  - write semantics: updates must be performed via “write temp + rename” so readers never see partial JSON.
  - reader semantics: readers must tolerate `state.json` being momentarily missing (e.g., right at attempt start) and treat it as `queued` / `initializing`.
  - `status`: `queued | running | succeeded | failed | canceled | needs_attention`
  - allowed status transitions (run attempt scoped):
    - `queued -> running -> {succeeded | failed | canceled | needs_attention}`
    - `queued -> canceled` (optional; canceled before start)
  - terminality: once `status` is one of `{succeeded | failed | canceled | needs_attention}`, the run attempt is ended and `status` must never change again for that run attempt.
  - `started_at`, `ended_at`
    - `started_at` must be present when `status` is `running` or terminal
    - `ended_at` must be present when `status` is terminal
  - `last_heartbeat_at` (host-derived heartbeat time; `state.json` is updated at least every 15 minutes while `running`)
  - `current_item` (optional; short status string suitable for a scoreboard)
  - `exit_code` / termination reason (if available)
  - `artifacts[]` (manifest entries written so far)
  - `errors[]` (summary strings; not full traces)

- `codex_home/` (session store; writer: runner/Codex; updated while `running`; frozen at attempt end)
  - contains the Codex session state for this run attempt (the attempt’s “resume base” snapshot once finished)
  - runner (worker code) sets `CODEX_HOME` to this directory for the Codex process for this attempt
  - unique per run attempt (`run_id`); must not be shared between concurrent attempts
  - authentication note: Codex CLI auth/config is normally stored under `CODEX_HOME` (e.g., `auth.json` in the default `~/.codex` on the runner host).
    - If an API key is available, the runner may inject credentials via environment (e.g., `CODEX_API_KEY`) to avoid writing secrets into the run attempt directory.
    - If no API key is available, the runner must make the existing `auth.json` available to the attempt-local `CODEX_HOME` (recommended: symlink from a runner-local credential store).
  - `CODEX_HOME` affects Codex persisted state (sessions/config/auth) but does **not** change how Codex discovers repo context like `AGENTS.md` (which comes from the job’s `working_directory` and its parents).

- `final.txt` (write-once; writer: runner; created at attempt end, if configured)
- `final.json` (write-once; writer: runner; created at attempt end, if configured)
- `handoff.json` (write-once; writer: runner; created at attempt end, optional)

- Optional raw event logs (debug/audit only)
  - `codex.events.jsonl` (append-only; writer: runner; written while `running`; frozen at attempt end) if enabled

- Optional repo state artifacts (recommended when jobs modify code)
  - `git_head.txt` (write-once; writer: runner; created at attempt end)
  - `git_status.txt` (write-once; writer: runner; created at attempt end)
  - `git_diff.patch` (write-once; writer: runner; created at attempt end)
  - `files_touched.json` (write-once; writer: runner; created at attempt end; best-effort list)

### 8.2.2 Lifecycle / mutability audit (required)
Each listed object is classified as exactly one of: `write-once`, `append-only`, `atomic snapshot`, `mutable index`.

| Object | Path pattern | Class | Writer | When it changes | When it becomes immutable |
| --- | --- | --- | --- | --- | --- |
| runs store root | `runs/` | `mutable index` | harness | when new batches/jobs/steps/attempts are created | never (by design) |
| batch folder | `runs/<batch_id>/` | `mutable index` | harness | when new jobs are created under the batch | never (by design) |
| job folder | `runs/<batch_id>/<job_id>/` | `mutable index` | harness | when new step folders/attempt folders appear | never (by design) |
| steps folder | `runs/<batch_id>/<job_id>/steps/` | `mutable index` | harness | when new step folders appear | never (by design) |
| step folder | `runs/<batch_id>/<job_id>/steps/<step_id>/` | `mutable index` | harness | when new attempts are created for the step | never (by design) |
| attempts folder | `runs/<batch_id>/<job_id>/steps/<step_id>/attempts/` | `mutable index` | harness | when new attempt directories are created | never (by design) |
| run attempt directory | `.../attempts/<run_dir_name>/` | `mutable index` | runner | created at attempt start; files inside may change per their class | attempt end |
| `codex_home/` | `.../attempts/<run_dir_name>/codex_home/` | `mutable index` | runner (Codex) | while `running` | attempt end |
| `meta.json` | `.../attempts/<run_dir_name>/meta.json` | `write-once` | runner | attempt start | immediately after write |
| `state.json` | `.../attempts/<run_dir_name>/state.json` | `atomic snapshot` | runner | at least every 15 minutes while `running` | attempt end |
| `final.json` | `.../attempts/<run_dir_name>/final.json` | `write-once` | runner | attempt end | immediately after write |
| `final.txt` | `.../attempts/<run_dir_name>/final.txt` | `write-once` | runner | attempt end | immediately after write |
| `handoff.json` | `.../attempts/<run_dir_name>/handoff.json` | `write-once` | runner | attempt end (optional) | immediately after write |
| `codex.events.jsonl` | `.../attempts/<run_dir_name>/codex.events.jsonl` | `append-only` | runner | while `running` (optional) | attempt end |
| `git_*.txt` / `git_diff.patch` | `.../attempts/<run_dir_name>/git_*` | `write-once` | runner | attempt end (optional) | immediately after write |
| `files_touched.json` | `.../attempts/<run_dir_name>/files_touched.json` | `write-once` | runner | attempt end (optional) | immediately after write |
| `current.json` | `runs/<batch_id>/<job_id>/current.json` | `mutable index` | harness | when a new attempt starts/ends | never (by design) |
| `batch_meta.json` | `runs/<batch_id>/batch_meta.json` | `write-once` | harness | batch submission | immediately after write |
| system folder | `runs/_system/` | `mutable index` | harness | created/updated as harness config snapshots are written | never (by design) |
| `harness_config.json` | `runs/_system/harness_config.json` | `atomic snapshot` | harness | when harness config changes | never (by design) |
| `harness_config_versions/<version>.json` | `runs/_system/harness_config_versions/<harness_config_version>.json` | `write-once` | harness | when a new config version is created | immediately after write |
| runners folder | `runners/` | `mutable index` | runner | when new runners register/are removed | never (by design) |
| runner health snapshot | `runners/<runner_id>/health.json` | `atomic snapshot` | runner | periodically while runner is online | never (by design) |

### 8.3 Batch/job “pointers” (recommended)
To avoid scanning directories:
- `runs/<batch_id>/<job_id>/current.json` (mutable index; writer: harness; updated on attempt start/end) points to:
  - latest run per step
  - latest successful run per step (if different)
  - per-step resume bases (minimum): the session snapshot directory to copy (e.g., the attempt’s `codex_home/`), plus optional `codex_thread_id` for correlation
- `current.json` is a **pointer file**, not a “scheduler truth” file:
  - it should contain enough information to locate attempts and resume bases deterministically
  - it should not be relied on to explain why something is blocked; blocked/ready reasons are derived in the scoreboard from `batch_meta.json` dependency wiring + attempt `state.json` / `current.json` pointers
  - it is intentionally **minimal**: diagnosing failures/stalls requires opening the pointed run attempt directory (`attempt_dir`) and reading that attempt’s `state.json` (and optionally `final.json` / `meta.json`)

### 8.3.1 `current.json` contents (required, conceptual)
`current.json` exists so the orchestrator can answer “which attempt/resume base should I use?” without opening raw event logs.

Notes:
- Source of information: the harness updates `current.json` from run lifecycle events (attempt start/end) and the authoritative leaf attempt artifacts (`state.json` / `meta.json`), without parsing raw traces.
- Level of detail: `current.json` provides step-level pointers (latest/latest_successful/by_run_id). It is not expected to include blocked reasons, error messages, or “what is it doing right now”.
- Debug/triage flow (recommended):
  - use the scoreboard to find the step of interest (running/stuck/failed/blocked)
  - follow `current.json.steps[step_id].latest.attempt_dir`
  - read `<attempt_dir>/state.json` (heartbeat, status, `errors[]`, `exit_code`)
  - optionally read `<attempt_dir>/final.json` (agent status/summary) and `<attempt_dir>/meta.json` (resume linkage)
  - only if needed, tail `<attempt_dir>/codex.events.jsonl` (when enabled)

**Must include (minimum):**
- `batch_id`, `job_id`, `updated_at`
- `steps` map keyed by `step_id`, where each value includes:
  - `latest`:
    - `run_id`
    - `attempt_dir` (path to the run attempt directory)
    - `resume_base_dir` (path to that attempt’s `codex_home/`)
    - optional `codex_thread_id`
  - `latest_successful` (same fields as `latest`) when different

**Optional (recommended for explicit `resume_from.run_id` selectors):**
- `steps[step_id].by_run_id` mapping `run_id -> {attempt_dir, resume_base_dir, status, ended_at}`.

### 8.4 How orchestration consumes logs
- Normal operation:
  - orchestrator reads `state.json` / `current.json` only
- Debug operation:
  - orchestrator tails `codex.events.jsonl` for a specific run
  - orchestrator opens `meta.json` to interpret linkage and policies

### 8.5 Retention & garbage collection (functional)
- System supports retention policies per batch/job:
  - keep raw event logs for N days
  - keep reduced state and final outputs longer
  - optionally compress archived raw logs
- GC never deletes the only copy of:
  - `final.*` and `handoff.json` for successful runs (unless explicitly configured)
  - `codex_home/` for any run attempt that could still be legally used as a resume base by any not-yet-terminal dependent step in the same batch (or until the batch is finalized/closed — meaning no new runs/resumes/retries will be scheduled for the batch), unless the system has an explicit, tested way to reconstruct the resume base.
    - In practice: in an open batch, any `resume_base_dir` currently referenced by that batch’s `current.json` must be treated as protected from GC.

### 8.6 Scenario simulation (concrete flows)
This section enumerates concrete sequences so “what changes on disk, and when” is unambiguous.

#### 8.6.1 Exec success (single attempt)
1. Runner creates a new run attempt directory (including `codex_home/`).
2. Runner writes `meta.json` (write-once).
3. Runner initializes `state.json` with `status: running`, `started_at`, `last_heartbeat_at`.
4. Runner launches Codex with `CODEX_HOME` set to the attempt-local `codex_home/`.
5. Harness updates `current.json` to point at this attempt/run (optional at start, required by completion).
6. While running, runner periodically refreshes `state.json.last_heartbeat_at` (and optionally `current_item` / partial `artifacts[]`).
   - Default: refresh at least every 15 minutes.
7. On success, runner writes `final.json` (and optionally `final.txt`, `handoff.json`, `git_*`, `files_touched.json`).
8. Runner finalizes `state.json` with `status: succeeded`, `ended_at`, `exit_code`.
9. Harness updates `current.json` to reflect “latest attempt” and “latest successful attempt” for the step.

#### 8.6.2 Retry (attempt 1 fails, attempt 2 succeeds)
1. Attempt 1 proceeds as in 8.6.1 through writing `meta.json` + `state.json` updates.
2. Attempt 1 ends with `state.json.status: failed` and an `exit_code` (and may still write `final.*` if available).
3. Harness leaves attempt 1 directory intact (frozen) and schedules a new run attempt (`run_id` changes).
4. Attempt 2 creates a new run attempt directory and repeats 8.6.1.
5. `current.json` is updated to point to attempt 2 as “latest”; “latest successful” points to attempt 2 only after it succeeds.

#### 8.6.3 Resume (step2 resumes step1 session)
1. Step1 runs as an exec attempt (8.6.1) and produces `final.json` (and any handoff artifacts).
2. Step2 is scheduled (often with runner affinity to step1’s runner).
3. Runner seeds step2’s attempt-local session store by copying step1’s resume base (step1 attempt’s `codex_home/`) into step2 attempt’s `codex_home/`.
4. Runner runs `codex exec resume --last` (or `resume <codex_thread_id>`) as a **new** run attempt directory under step2.
5. Step2 writes its own `meta.json` including `invocation: resume` + `parent_run_id` (and `codex_thread_id` if known).
6. Step2 produces its own `final.json` and finalizes `state.json`; `current.json` is updated to include step2’s latest attempt pointers.

## 9) Monitoring & Introspection
### 9.1 “Scoreboard” view (must be derivable without traces)
For each batch:
- total jobs/steps
- counts by status
- currently running steps with `current_item`
- “stuck” steps: `running` with no `state.json.last_heartbeat_at` heartbeat update for a threshold (default 45 minutes; must be >= 30 minutes)
- failure list: steps with `failed` / `needs_attention`

### 9.1.1 Scoreboard contract (required, conceptual)
The scoreboard is an orchestrator-facing control-plane view. It should answer, at a glance:
- Is the batch making progress (or stalled)?
- What needs attention right now (stuck/fail/needs_attention)?
- What is currently running (and where)?
- What is blocked (and why)?
- For resume steps, what resume base will be used?

**Inputs (allowed):**
- `batch_meta.json` (planned jobs/steps/dependencies)
- `current.json` (per-step pointers: latest/latest_successful/by_run_id, including `attempt_dir` + `resume_base_dir`)
- attempt `state.json` (live heartbeat + status + `current_item`)
- attempt `final.json` (optional: agent summary/status for display only; do not treat as authoritative host state)
- runner health snapshot(s) (optional)

**Scoreboard must include (minimum):**
- **Batch header**
  - `batch_id`, `submitted_at` (from `batch_meta.json`), `computed_at` (time this scoreboard view is computed)
  - `batch_goal_summary` (from `batch_meta.json`)
  - totals: `jobs_total`, `steps_total`
  - `heartbeat_stale_after_seconds` (default: 2700 / 45 minutes; must be >= 1800 / 30 minutes)
- **Counts by step status** (step-level, derived; see below): `blocked`, `ready`, `running`, `succeeded`, `failed`, `needs_attention`, `canceled`
- **Attention list** (ordered by severity): stuck steps, then `needs_attention`, then failed steps
- **Running list**: each running step includes:
  - identifiers: `job_id`, `step_id`, `run_id`, `runner_id`
  - `attempt_dir`
  - `current_item` (if present)
  - `last_heartbeat_at` and derived `seconds_since_last_heartbeat`
  - `started_at` and derived `run_duration_seconds`
- **Blocked list**: each blocked step includes:
  - `job_id`, `step_id`
  - blocking reasons: unsatisfied `depends_on[]` (and/or missing resume base for `resume_from`)
- **Failure list**: each failed/needs_attention step includes:
  - `job_id`, `step_id`, `run_id`, `attempt_dir`
  - host outcome: `state.json.status`, `exit_code` (if present)
  - optional agent outcome: `final.json.status` + `final.json.summary` (display only)

**Step status derivation (required):**
- `running`: step has an active attempt whose `state.json.status` is `running`
- `succeeded`: step has a `latest_successful` pointer in `current.json`
- `failed`: step has at least one attempt and has no successful attempt; latest attempt is `failed`
- `needs_attention`: latest attempt is `needs_attention` (or schema validation failed)
- `canceled`: latest attempt is `canceled` and no successful attempt exists
- `ready`: dependencies satisfied but no attempt is currently running and no successful attempt exists
- `blocked`: dependencies not satisfied (including `resume_from` source attempt missing a usable resume base)

**Resume visibility (required for steps with `resume_from`):**
- show the resolved resume source: `resume_from.step_id`, selected source `run_id`, and `resume_base_dir` that will be copied.

### 9.1.2 Multi-batch scoreboard (required, conceptual)
When multiple batches exist concurrently, the orchestrator needs a “system view” to triage without drilling into each batch.

**System view must include (minimum):**
- one line (or record) per `batch_id`
- `submitted_at` (from `batch_meta.json`)
- `batch_goal_summary_preview` (derived from `batch_meta.json.batch_goal_summary`; e.g., first ~120 characters or first sentence)
  - totals: `jobs_total`, `steps_total`
  - counts by derived step status (same categories as 9.1.1)
  - `running_steps` count
  - `attention_steps` count (stuck + needs_attention + failed)

**System view ordering (recommended):**
- batches with any `attention_steps > 0` first
- then batches with `running_steps > 0`
- then the rest, sorted by `submitted_at` descending

**Drill-down behavior (required):**
- selecting a batch renders the per-batch scoreboard described in 9.1.1

### 9.1.3 Scoreboard examples (informative)
These are **examples** of what the orchestrator would like to see; the exact presentation (JSON, table, TUI) is not prescribed as long as the 9.1.1/9.1.2 content is available.

**Example: multi-batch “system view” (two batches)**
```json
[
  {
    "batch_id": "batch_20260114_173000Z_001",
    "submitted_at": "2026-01-14T17:30:00Z",
    "batch_goal_summary_preview": "Backfill and audit ParallelHassnes spec semantics for resume-from-any-step, CODEX_HOME, and scoreboard…",
    "jobs_total": 12,
    "steps_total": 24,
    "counts": { "blocked": 2, "ready": 4, "running": 6, "succeeded": 12, "failed": 0, "needs_attention": 0, "canceled": 0 },
    "running_steps": 6,
    "attention_steps": 0
  },
  {
    "batch_id": "batch_20260114_120000Z_000",
    "submitted_at": "2026-01-14T12:00:00Z",
    "batch_goal_summary_preview": "Generate summaries for 200 repos and open PRs with fixes; prioritize failing CI and flaky tests…",
    "jobs_total": 200,
    "steps_total": 400,
    "counts": { "blocked": 0, "ready": 0, "running": 0, "succeeded": 358, "failed": 22, "needs_attention": 20, "canceled": 0 },
    "running_steps": 0,
    "attention_steps": 42
  }
]
```

**Example: per-batch “batch view” (excerpt)**
```json
{
  "batch_id": "batch_20260114_173000Z_001",
  "submitted_at": "2026-01-14T17:30:00Z",
  "computed_at": "2026-01-14T17:52:00Z",
  "batch_goal_summary": "<more than 150 words omitted here for brevity>",
  "jobs_total": 12,
  "steps_total": 24,
  "heartbeat_stale_after_seconds": 2700,
  "counts": { "blocked": 2, "ready": 4, "running": 6, "succeeded": 12, "failed": 0, "needs_attention": 0, "canceled": 0 },
  "running": [
    {
      "job_id": "job_03",
      "step_id": "step2",
      "run_id": "run_9f7b…",
      "runner_id": "runner_mac_01",
      "attempt_dir": "runs/batch_20260114_173000Z_001/job_03/steps/step2/attempts/20260114T175000Z_run_9f7b…/",
      "current_item": "running tests",
      "last_heartbeat_at": "2026-01-14T17:51:00Z",
      "seconds_since_last_heartbeat": 60,
      "started_at": "2026-01-14T17:45:00Z",
      "run_duration_seconds": 420
    }
  ],
  "blocked": [
    {
      "job_id": "job_09",
      "step_id": "step2",
      "reasons": [
        "depends_on: job_09.step1 not succeeded",
        "resume_from: no resume base available yet for job_09.step1"
      ]
    }
  ],
  "failures": [
    {
      "job_id": "job_12",
      "step_id": "step1",
      "run_id": "run_1ab2…",
      "attempt_dir": "runs/batch_20260114_173000Z_001/job_12/steps/step1/attempts/20260114T174000Z_run_1ab2…/",
      "state_status": "failed",
      "exit_code": 1,
      "final_status": "failed",
      "final_summary": "Test suite failed on macOS due to missing dependency; suggested fix in README."
    }
  ]
}
```

### 9.2 Runner health
Each runner exposes a periodically updated snapshot (e.g., `runners/<runner_id>/health.json` in filesystem mode):
- `runner_id`, `last_seen_at`
- capacity and current load (running steps)
- system pressure signals (disk free, memory pressure) as available
- drain mode (accepting new work or not)

### 9.3 Stuck detection and auto-remediation (functional)
When `running` and `now - state.json.last_heartbeat_at > threshold` (default 45 minutes; must be >= 30 minutes):
- treat the run as `stuck` (attention), without changing attempt terminal status purely due to staleness
- optionally attempt:
  - graceful cancel (then mark the attempt `canceled` or `needs_attention` once ended) + retry
  - resume with a recovery prompt
  - requeue on same runner

## 10) Resume Semantics (Functional Rules)
### 10.1 Thread locality
Resume requires access to a stable “resume base” for the source step.

Functional requirements:
- Every run attempt that may be resumed from must produce a **resume base** (session snapshot), stored as an immutable directory snapshot (the completed attempt-local `codex_home/` once frozen at attempt end).
- A resume run must not mutate the source attempt’s resume base. Instead it must **copy** the source resume base into the new run attempt’s session store before invoking `codex exec resume`.
- This copy-on-resume design is what makes `resume_from` able to point to **any earlier step** (including branching), because each step’s end-state remains preserved.

Locality / portability:
- A resume run must execute on a runner that can read the source attempt directory (and thus its `codex_home/`), or the system must first copy the resume base to the destination runner.
- In filesystem-only MVP mode, “same runner” is sufficient but not strictly required if the `runs/` store (or the resume base) is accessible from the destination runner.

`resume --last` vs explicit `codex_thread_id`:
- `resume --last` is safe when the session store is isolated per run attempt (fresh `codex_home/` per attempt, seeded only via copy).
- If the session store may contain multiple threads, resuming must use an explicit `codex_thread_id`.

Note on “where logs go”:
- Per-run reduced logs and outputs (e.g., `state.json`, `final.json`, optional `codex.events.jsonl`) live in the run attempt directory under the `runs/` store.
- Codex’s own persisted session material (including rollouts) lives under the configured `CODEX_HOME`.
- In this spec, `CODEX_HOME` is set to the attempt-local `.../codex_home/` directory.

### 10.2 Linking runs
Every resume run records:
- `codex_thread_id` (same as parent, when known)
- `parent_run_id` (the run that produced the thread or immediate prior run in chain)
This allows reconstructing the full chain of turns for a job.

### 10.3 Handoff contract
Step2 must not require reading raw event logs.
Instead, step1 produces:
- `final.json` (the schema-conformant Run Report; primary handoff)
- optionally `handoff.json` (even smaller “just what step2 needs” summary)
Step2 consumes `final.json` (or `handoff.json`) by reference.

## 11) Workspace and Concurrency Safety (Functional Expectations)
The system must define one of these policies per job (selectable):
- **Shared workspace**: fastest, but jobs may collide (operator accepts risk).
- **Isolated workspace**: each job runs in an isolated checkout/worktree/sandboxed directory to avoid file conflicts.

This spec does not require a specific policy, but requires the chosen policy to be explicit and recorded in `meta.json`.

### 11.1 Workspace continuity vs resume continuity (required)
`resume_from` changes the Codex **conversation state** by seeding the new attempt’s session store (`codex_home/`) from an earlier step’s resume base. It does **not** imply filesystem rollback.

Required invariant:
- When a step resumes from an earlier step, it runs against the **current workspace state** available in the job’s `working_directory` at the time the step executes (the “latest job workspace”), even if the resumed conversation context is older than the current workspace state.

## 12) Interfaces (Functional)
ParallelHassnes may be driven by:
- **API mode**: submit/query/stream/cancel/fetch artifacts.
- **Filesystem queue mode**: drop Launch Tables into an input directory; outputs are written to a predictable output directory.

Minimum operations the system supports (regardless of interface style):
- Submit batch
- List/query batch and job status
- Fetch per-run artifacts by name
- Stream or tail per-run events
- Cancel a run
- Trigger resume/retry steps

### 12.1 API mode (conceptual)
API mode is not required for the filesystem MVP, but when present it should expose operations equivalent to the filesystem artifacts:
- **submit**: accept a Launch Table (inline or by reference) and return `{batch_id, accepted_job_ids[]}`
- **get scoreboard (system + batch)**: return the same data as `runs/_system/scoreboard.system.json` and `runs/<batch_id>/scoreboard.batch.json`
- **get job pointers (`current.json`)**: return `runs/<batch_id>/<job_id>/current.json` (or equivalent structured response)
- **get attempt state/meta/final**: fetch `meta.json`, `state.json`, `final.json`, `final.txt` for a specified attempt (by ID or by `attempt_dir`)
- **tail events**: stream/tail `codex.events.jsonl` when enabled
- **cancel**: request best-effort cancellation of a running attempt (by ID or `attempt_dir`), producing `state.json.status="canceled"` when successful
- **trigger retry/resume**: request a new attempt for a step (including resume steps); in filesystem mode this maps to writing an override (e.g., bumping a monotonic nonce) plus running `tick`

### 12.2 API auth (conceptual)
If API mode is enabled, it should support an explicit `auth_mode` configured in `harness_config.json`:
- `none`: no authentication (localhost/dev only)
- `local_trust`: rely on local OS-user/process boundary (e.g., Unix socket/localhost)
- `token`: require a bearer token (suitable for remote access)

## 13) Versioning & Compatibility
- Launch Table supports a `spec_version`.
- `handoff.json` and `final.json` (schema outputs) support explicit `schema_version`.
- Runner records Codex CLI version in `meta.json` for reproducibility.

## 14) Open Design Choices (to finalize)
- Default workspace policy: shared vs isolated (and under what conditions).
- Default retry semantics: resume-thread vs fresh-thread per failure class.
- Retention defaults: how long to keep raw event logs and diffs.
- Artifact indexing: manifest format and searchable fields (tags, repo, step type).

## Appendix A) Common misreads (and clarifications)
1. Misread: “run attempt directory is immutable” means no files change while running. Clarification: `state.json` is an atomic snapshot updated while `running`, and `codex.events.jsonl` may be append-only; immutability begins at attempt end.
2. Misread: `runs/` (run artifacts) and `CODEX_HOME` (session store) are the same thing. Clarification: `runs/` is the harness’s artifact store; `CODEX_HOME` is the session store used for resume and is stored under each attempt directory as `codex_home/` in this spec.
3. Misread: you can “resume from any earlier step” just by recording a `codex_thread_id`. Clarification: resuming mutates the conversation state; to support jumping back/branching, you must preserve a per-attempt resume base (session snapshot) and copy it into a new attempt’s session store before resuming.
4. Misread: `final.json.files_read` / `files_written` is an authoritative filesystem trace. Clarification: it is a best-effort self-report of explicit interactions; the orchestrator may verify by inspecting the workspace or diffs.
5. Misread: `current.json` is immutable or strongly consistent. Clarification: it is a mutable index that may be temporarily stale; the source of truth for a specific attempt is that attempt’s `meta.json` + `state.json` + `final.*`.

## Appendix B) Spec review checklist (for future edits)
- Lock terminology: maintain the Terminology table and avoid synonyms/overloaded nouns; ensure any new term used is defined once.
- Reference completeness check: any referenced field/path must be defined once (where it lives, who writes it, and when it changes).
- Lifecycle/mutability audit: classify each file/dir as `write-once`, `append-only`, `atomic snapshot`, or `mutable index` and specify the transition to immutability.
- Scenario simulation: walk 3 concrete flows (exec success, retry, resume) and list the on-disk changes in order; if hand-wavy, tighten the spec.
- Contradiction scan: search for “immutable”, “append-only”, “must”, and cross-check against any “updated” language nearby.
- Misread list: list top plausible misinterpretations and patch wording to eliminate them.
