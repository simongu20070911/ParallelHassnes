# ParallelHassnes Terminology (Canonical)

This document mirrors the canonical nouns in `FUNCTIONALITY_SPEC.md` and exists to reduce synonym drift across code + docs.

## Canonical nouns

| Term | Canonical meaning |
| --- | --- |
| batch | A submitted unit of work containing one or more jobs (`batch_id`). |
| job | A logical task inside a batch (`job_id`) with a working directory and ordered/linked steps. |
| step | A single unit of execution within a job (`step_id`), producing one or more run attempts over time. |
| run attempt | One invocation of Codex for one `(job_id, step_id)` (creates a fresh `run_id`). Retries and resumes are new run attempts. |
| run attempt directory | The leaf directory for a run attempt: `runs/<batch_id>/<job_id>/steps/<step_id>/attempts/<run_dir_name>/`. |
| runner | The host-side executor process (not the LLM) that spawns `codex exec` and writes attempt artifacts (`runner_id`). |
| orchestrator | The control-plane system that submits batches, triggers ticks, and consumes scoreboards/artifacts. |
| harness | The scheduling/control logic that ingests Launch Tables, dispatches steps to runners, and writes minimal indices. |
| Codex thread ID (`codex_thread_id`) | The UUID used by `codex exec resume <SESSION_ID>` to resume conversation state (optional when using isolated per-attempt `CODEX_HOME` + `resume --last`). |

## Minimum correlation set (required IDs)

Every run attempt `meta.json` must include:

- `batch_id`, `job_id`, `step_id`, `run_id`, `runner_id`
- optional: `codex_thread_id`

## Forbidden / ambiguous phrases (linted)

These phrases are treated as ambiguous and should not appear in specs/plans:

- `run directory` (use `run attempt directory` or name the exact path)
- `run folder` (use `run attempt directory`)
- `hb age` / `hb_age` (use `seconds_since_last_heartbeat`)

