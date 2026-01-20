# ParallelHassnes

Filesystem-first batch harness for running many Codex CLI jobs with an auditable on-disk runs store.

## What it is

- A small Python CLI (`python -m parallelhassnes ...`) that:
  - ingests “Launch Table” JSON into a filesystem queue
  - schedules runnable steps with per-step locking
  - spawns `codex exec` to run agents and records attempt artifacts under `runs/`
  - provides a thin FastAPI façade for submit/monitor/tick

## Repo layout

- `run_report.schema.json`: output schema passed to `codex exec --output-schema`
- `runs/`: source-of-truth audit store (batches/jobs/steps/attempts + artifacts)
- `runners/`: runner registry + health + drain control

`runs/` and `runners/` are intentionally **not** tracked in git (see `.gitignore`).

## Quickstart (filesystem mode)

From the repo root:

```bash
python3 -m parallelhassnes init --root .
python3 -m parallelhassnes submit examples/launch_table.example.json --root .
python3 -m parallelhassnes tick --root . --concurrency 4
python3 -m parallelhassnes scoreboard --root . --json
```

For a dry-run without spawning Codex:

```bash
python3 -m parallelhassnes tick --root . --no-codex
```

## API mode (optional)

Install API deps:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install "fastapi>=0.110" "uvicorn>=0.25"
```

Run the server:

```bash
python -m parallelhassnes api --root . --host 127.0.0.1 --port 8765
```

Example usage:

```bash
curl -s http://127.0.0.1:8765/health
curl -s -X POST http://127.0.0.1:8765/v1/batches -H 'Content-Type: application/json' -d @examples/launch_table.example.json
curl -s -X POST 'http://127.0.0.1:8765/v1/tick?concurrency=4'
```

## Notes

- The harness serializes ticks (no two `tick` runs in the same process at once).
- Batches are currently scheduled one-at-a-time within a tick; for throughput, prefer one batch with many jobs.
- Runner attempts write `runs/<batch>/<job>/<step>/attempts/<...>/state.json` with periodic heartbeats. “Stuck” is detected via heartbeat age.
- Credentials: the runner avoids copying secrets into `runs/`; it can symlink runner-local `~/.codex/auth.json` into each attempt-local `codex_home/` when needed.

