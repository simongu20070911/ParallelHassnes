from __future__ import annotations

import json
import os
import signal
import shutil
import shlex
import subprocess
import threading
import time
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from parallelhassnes.core.atomic_io import write_atomic_json, write_once_json, write_once_text
from parallelhassnes.core.ids import new_run_id
from parallelhassnes.core.paths import Paths
from parallelhassnes.core.state_update import read_state_best_effort, write_state_guarded
from parallelhassnes.core.time import utc_compact_timestamp, utc_isoformat
from parallelhassnes.validation.json_schema import validate_json_against_schema
from parallelhassnes.validation.contracts import validate_attempt_meta, validate_attempt_state


_CODEX_EXEC_HELP_CACHE: str | None = None


@dataclass(frozen=True)
class RunnerConfig:
    runner_id: str
    use_fake_invoker: bool
    baseline_output_schema_path: str


class Runner:
    def __init__(self, store: Any, cfg: RunnerConfig, harness_cfg: dict[str, Any]) -> None:
        self._store = store
        self._cfg = cfg
        self._hcfg = harness_cfg
        self._paths: Paths = store.paths
        self._running_attempts = 0
        self._max_running_attempts_observed = 0
        # Provide a fresh runner health snapshot even when idle (filesystem MVP runs in-process).
        try:
            self._write_runner_health()
        except Exception:
            pass

    def execute_step(
        self,
        batch: dict[str, Any],
        job: dict[str, Any],
        step: dict[str, Any],
        job_workdir: str,
        workspace_policy: dict[str, Any] | None,
        workspace_root: str | None,
        current: dict[str, Any],
    ) -> None:
        batch_id = batch["batch_id"]
        job_id = job["job_id"]
        step_id = step["step_id"]

        # Attempt directory uniqueness under parallel starts: if a collision occurs, retry with a new run_id.
        attempt_dir = None
        codex_home = None
        run_id = None
        for _ in range(5):
            rid = new_run_id()
            run_dir_name = self._paths.run_dir_name(rid)
            ad = self._paths.attempt_dir(batch_id, job_id, step_id, run_dir_name)
            try:
                ad.mkdir(parents=True, exist_ok=False)
                ch = self._paths.attempt_codex_home(ad)
                ch.mkdir(parents=True, exist_ok=False)
                attempt_dir = ad
                codex_home = ch
                run_id = rid
                break
            except FileExistsError:
                continue
        if attempt_dir is None or codex_home is None or run_id is None:
            raise RuntimeError("failed to allocate a unique attempt directory after retries")

        full_prompt = _load_step_prompt(batch, job, step, job_workdir)
        prompt_sha256 = hashlib.sha256(full_prompt.encode("utf-8")).hexdigest()

        exec_policy = batch.get("effective_defaults", {}).get("execution_policy") or self._hcfg.get("defaults", {}).get("execution_policy", {})
        capture_events = bool(exec_policy.get("capture_events_jsonl", False))
        capture_thread_id = bool(exec_policy.get("capture_codex_thread_id", False))
        timeouts = batch.get("effective_defaults", {}).get("timeouts") or self._hcfg.get("defaults", {}).get("timeouts", {})
        default_timeout = None
        if isinstance(timeouts, dict) and timeouts.get("step_timeout_seconds") is not None:
            try:
                default_timeout = float(timeouts.get("step_timeout_seconds"))
            except Exception:
                default_timeout = None
        step_timeout = step.get("timeout_seconds", default_timeout)
        timeout_seconds: float | None
        try:
            timeout_seconds = float(step_timeout) if step_timeout is not None else None
        except Exception:
            timeout_seconds = None
        if timeout_seconds is not None and timeout_seconds <= 0:
            timeout_seconds = None

        schema_path = _resolve_output_schema_path(
            job_workdir=job_workdir,
            schema_ref=step.get("output_schema_ref") or self._cfg.baseline_output_schema_path,
        )

        # If resume_from: copy resume base into this codex_home before running.
        resume_linkage: dict[str, Any] | None = None
        if step.get("resume_from"):
            resume_linkage = self._prepare_resume_base(batch_id, job_id, step, batch, current, codex_home)
        else:
            resume_linkage = self._maybe_prepare_retry_resume_base(batch, job_id, step, current, codex_home)

        # Provision auth/config into attempt-local CODEX_HOME.
        self._provision_codex_credentials(codex_home)

        invocation = "resume" if resume_linkage else "exec"

        meta = {
            "batch_id": batch_id,
            "job_id": job_id,
            "step_id": step_id,
            "run_id": run_id,
            "runner_id": self._cfg.runner_id,
            "invocation": invocation,
            "prompt_ref": step.get("prompt_ref"),
            "prompt_sha256": prompt_sha256,
            "policy_snapshot": exec_policy,
            "environment_snapshot": {
                "job_workdir": job_workdir,
                "workspace_policy": workspace_policy or batch.get("effective_defaults", {}).get("workspace_policy") or self._hcfg.get("defaults", {}).get("workspace_policy"),
                "workspace_root": workspace_root or job_workdir,
            },
            "codex_cli_version": _codex_version(),
        }
        if resume_linkage:
            meta.update(resume_linkage)

        # Heartbeat interval is a scoreboard default; prefer batch-level effective defaults when provided.
        hb_cfg = (batch.get("effective_defaults", {}) or {}).get("scoreboard") or (self._hcfg.get("defaults", {}) or {}).get("scoreboard") or {}
        try:
            hb_interval = int(hb_cfg.get("heartbeat_interval_seconds", 900)) if isinstance(hb_cfg, dict) else 900
        except Exception:
            hb_interval = 900
        if hb_interval <= 0:
            hb_interval = 900
        state_path = self._paths.attempt_state_path(attempt_dir)
        started_at = utc_isoformat()
        errors: list[str] = []
        artifacts: list[dict[str, Any]] = []
        files_touched: set[str] = set()

        meta_errs = validate_attempt_meta(meta)
        if meta_errs:
            raise ValueError("Invalid attempt meta.json: " + "; ".join(meta_errs))
        write_once_json(self._paths.attempt_meta_path(attempt_dir), meta)

        hb = _HeartbeatWriter(
            state_path=state_path,
            interval_seconds=hb_interval,
            started_at=started_at,
            artifacts=artifacts,
            errors=errors,
            on_heartbeat=self._write_runner_health,
        )
        hb.write_queued()

        # Update current.json pointers at attempt start.
        self._update_current_on_start(batch_id, job_id, step_id, run_id, attempt_dir, codex_home, meta)

        # If an orchestrator cancels immediately after the attempt directory is created
        # (before Codex is spawned), do not start the process. This is critical because
        # cancel-before-start would otherwise be unable to kill a future PID.
        pre = read_state_best_effort(state_path) or {}
        if pre.get("status") == "canceled":
            self._update_current_on_end(batch_id, job_id, step_id, run_id, attempt_dir, codex_home, pre, meta)
            return

        self._running_attempts += 1
        self._max_running_attempts_observed = max(self._max_running_attempts_observed, self._running_attempts)
        self._write_runner_health()

        exit_code = None
        try:
            if self._cfg.use_fake_invoker:
                hb.mark_running(pid=None)
                prior = ((current.get("steps") or {}).get(step_id) or {}).get("by_run_id") or {}
                attempt_index = len(prior)
                exit_code = _fake_exit_code(step, attempt_index)
                _apply_fake_workspace_writes(job_workdir, step)
                marker = step.get("fake_codex_home_marker")
                if isinstance(marker, str) and marker:
                    (codex_home / marker).write_text(f"marker for {batch_id}/{job_id}/{step_id}\n", encoding="utf-8")
                sleep_s = step.get("fake_sleep_seconds")
                if isinstance(sleep_s, (int, float)) and sleep_s > 0:
                    time.sleep(float(sleep_s))
                out_text = _fake_run_report_text(batch_id, job_id, step_id, run_id, exit_code=exit_code)
            else:
                def on_thread_id(thread_id: str) -> None:
                    self._update_current_thread_id(batch_id, job_id, step_id, run_id, thread_id)

                explicit_resume_thread_id = None
                if step.get("resume_from"):
                    rf = step.get("resume_from") or {}
                    if isinstance(rf, dict):
                        tid = rf.get("codex_thread_id")
                        if isinstance(tid, str) and tid.strip():
                            explicit_resume_thread_id = tid.strip()
                # Prefer an explicit thread id when known (e.g., when resuming from a resume base
                # that may contain multiple threads). This may come from:
                # - the Launch Table override (`resume_from.codex_thread_id`)
                # - the resolved source attempt (`current.json` captured thread id)
                if explicit_resume_thread_id is None and invocation == "resume":
                    tid = meta.get("codex_thread_id")
                    if isinstance(tid, str) and tid.strip():
                        explicit_resume_thread_id = tid.strip()

                out_text, exit_code = self._invoke_codex(
                    batch,
                    job,
                    step,
                    job_workdir,
                    codex_home,
                    full_prompt,
                    attempt_dir=attempt_dir,
                    capture_events_jsonl=capture_events,
                    capture_thread_id=capture_thread_id,
                    on_thread_id=on_thread_id if capture_thread_id else None,
                    invocation=invocation,
                    explicit_resume_thread_id=explicit_resume_thread_id,
                    schema_path=schema_path,
                    timeout_seconds=timeout_seconds,
                    hb=hb,
                )

            final_txt_path = self._paths.attempt_final_txt_path(attempt_dir)
            try:
                write_once_text(final_txt_path, out_text)
            except FileExistsError:
                pass
            artifacts.append({"path": str(final_txt_path), "kind": "log", "description": "Raw Codex final output (schema JSON as text)"})

            parsed = None
            try:
                parsed = json.loads(out_text)
            except json.JSONDecodeError as e:
                errors.append(f"final output is not valid JSON: {e}")

            schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
            if parsed is not None:
                schema_errors = validate_json_against_schema(schema, parsed)
                if schema_errors:
                    errors.append("schema validation failed: " + "; ".join(schema_errors[:5]))
                else:
                    final_json_path = self._paths.attempt_final_json_path(attempt_dir)
                    write_once_json(final_json_path, parsed)
                    artifacts.append({"path": str(final_json_path), "kind": "report", "description": "Parsed Run Report (schema-conformant)"})
                    for p in parsed.get("files_written", []) if isinstance(parsed, dict) else []:
                        if isinstance(p, str) and p:
                            files_touched.add(p)
            else:
                # Preserve raw; no final.json.
                pass

            # Optional: write handoff.json as a smaller stable summary for downstream steps.
            try:
                handoff_path = self._paths.attempt_handoff_path(attempt_dir)
                if not handoff_path.exists():
                    handoff = {
                        "schema_version": (parsed.get("schema_version") if isinstance(parsed, dict) else None) or "unknown",
                        "status": (parsed.get("status") if isinstance(parsed, dict) else None),
                        "summary": (parsed.get("summary") if isinstance(parsed, dict) else None),
                        "files_read": (parsed.get("files_read") if isinstance(parsed, dict) else None) or [],
                        "files_written": (parsed.get("files_written") if isinstance(parsed, dict) else None) or [],
                        "artifacts": (parsed.get("artifacts") if isinstance(parsed, dict) else None) or [],
                        "final_json_path": str(self._paths.attempt_final_json_path(attempt_dir)) if self._paths.attempt_final_json_path(attempt_dir).exists() else None,
                        "final_txt_path": str(final_txt_path),
                    }
                    write_once_json(handoff_path, handoff)
                    artifacts.append({"path": str(handoff_path), "kind": "report", "description": "Optional handoff summary (small)"} )
            except Exception:
                pass

            # Optional: validate artifacts_expected[] by filename within the attempt directory.
            expected = step.get("artifacts_expected")
            if isinstance(expected, list):
                missing_expected: list[str] = []
                for ent in expected:
                    if not isinstance(ent, dict):
                        continue
                    name = ent.get("name")
                    if isinstance(name, str) and name:
                        if not (attempt_dir / name).exists():
                            missing_expected.append(name)
                if missing_expected:
                    errors.append("missing expected artifacts: " + ", ".join(missing_expected))

            self._maybe_capture_git_artifacts(job_workdir, attempt_dir, artifacts, files_touched)

        except Exception as e:
            errors.append(str(e))
            if exit_code is None:
                exit_code = 1
        finally:
            hb.stop()
            self._running_attempts = max(0, self._running_attempts - 1)
            self._write_runner_health()

        terminal_status = "succeeded"
        if exit_code != 0:
            terminal_status = "failed"
        if errors:
            terminal_status = "needs_attention"

        # If an external canceller already terminalized this attempt, do not overwrite.
        existing = read_state_best_effort(state_path)
        if isinstance(existing, dict) and existing.get("status") in {"canceled"}:
            terminal_status = "canceled"

        end_state = {
            "status": terminal_status,
            "started_at": hb.started_at,
            "ended_at": utc_isoformat(),
            "last_heartbeat_at": hb.last_heartbeat_at,
            "current_item": hb.current_item,
            "exit_code": exit_code,
            "pid": hb.pid,
            "artifacts": artifacts,
            "errors": errors,
        }
        state_errs = validate_attempt_state(end_state)
        if state_errs:
            # If we can't validate the host-derived state, force needs_attention and write anyway.
            end_state["status"] = "needs_attention"
        write_state_guarded(state_path, end_state)

        self._update_current_on_end(batch_id, job_id, step_id, run_id, attempt_dir, codex_home, end_state, meta)

        self._write_runner_health()

    def _maybe_capture_git_artifacts(
        self,
        job_workdir: str,
        attempt_dir: Path,
        artifacts: list[dict[str, Any]],
        files_touched: set[str],
    ) -> None:
        exec_policy = (
            (self._hcfg.get("defaults", {}) or {}).get("execution_policy", {})
            if isinstance(self._hcfg, dict)
            else {}
        )
        capture = bool(exec_policy.get("capture_git_artifacts", False))
        if not capture:
            return

        try:
            head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=job_workdir, capture_output=True, text=True)
            if head.returncode == 0:
                p = self._paths.attempt_git_head_path(attempt_dir)
                try:
                    write_once_text(p, head.stdout)
                except FileExistsError:
                    pass
                artifacts.append({"path": str(p), "kind": "log", "description": "git HEAD at attempt end"})

            status = subprocess.run(["git", "status", "--porcelain=v1"], cwd=job_workdir, capture_output=True, text=True)
            if status.returncode == 0:
                p = self._paths.attempt_git_status_path(attempt_dir)
                try:
                    write_once_text(p, status.stdout)
                except FileExistsError:
                    pass
                artifacts.append({"path": str(p), "kind": "log", "description": "git status at attempt end"})

            diff = subprocess.run(["git", "diff"], cwd=job_workdir, capture_output=True, text=True)
            if diff.returncode == 0:
                p = self._paths.attempt_git_diff_path(attempt_dir)
                try:
                    write_once_text(p, diff.stdout)
                except FileExistsError:
                    pass
                artifacts.append({"path": str(p), "kind": "patch", "description": "git diff at attempt end"})

            names = subprocess.run(["git", "diff", "--name-only"], cwd=job_workdir, capture_output=True, text=True)
            if names.returncode == 0:
                for line in names.stdout.splitlines():
                    line = line.strip()
                    if line:
                        files_touched.add(line)

            if files_touched:
                p = self._paths.attempt_files_touched_path(attempt_dir)
                write_once_json(p, {"files_touched": sorted(files_touched)})
                artifacts.append({"path": str(p), "kind": "report", "description": "Best-effort list of files touched (git + agent report)"})
        except Exception:
            return

    def _write_runner_health(self) -> None:
        drain_path = self._paths.runners_root / self._cfg.runner_id / "drain.json"
        drain_mode = False
        if drain_path.exists():
            try:
                raw = json.loads(drain_path.read_text(encoding="utf-8"))
                drain_mode = bool(raw.get("drain", False)) if isinstance(raw, dict) else True
            except Exception:
                drain_mode = True

        capacity = 1
        try:
            cfg_cap = (self._hcfg.get("defaults", {}) or {}).get("runner_capacity")
            if isinstance(cfg_cap, int) and cfg_cap > 0:
                capacity = cfg_cap
        except Exception:
            pass

        pressure: dict[str, Any] = {}
        try:
            du = shutil.disk_usage(self._paths.runs_root)
            pressure["disk_runs_root"] = {"total_bytes": du.total, "free_bytes": du.free}
        except Exception:
            pass
        try:
            page_size = os.sysconf("SC_PAGE_SIZE") if hasattr(os, "sysconf") else None
            total_pages = os.sysconf("SC_PHYS_PAGES") if hasattr(os, "sysconf") else None
            avail_pages = os.sysconf("SC_AVPHYS_PAGES") if hasattr(os, "sysconf") else None
            if isinstance(page_size, int) and isinstance(total_pages, int) and page_size > 0 and total_pages > 0:
                mem: dict[str, Any] = {"total_bytes": int(total_pages) * int(page_size)}
                if isinstance(avail_pages, int) and avail_pages >= 0:
                    mem["available_bytes"] = int(avail_pages) * int(page_size)
                pressure["memory"] = mem
        except Exception:
            pass
        try:
            if hasattr(os, "getloadavg"):
                la = os.getloadavg()
                pressure["loadavg"] = {"1m": la[0], "5m": la[1], "15m": la[2]}
        except Exception:
            pass

        self._store.write_runner_health(
            self._cfg.runner_id,
            {
                "runner_id": self._cfg.runner_id,
                "last_seen_at": utc_isoformat(),
                "capacity": capacity,
                "current_load": self._running_attempts,
                "max_load_observed": self._max_running_attempts_observed,
                "pressure": pressure,
                "drain_mode": drain_mode,
            },
        )

    def _update_current_on_start(
        self,
        batch_id: str,
        job_id: str,
        step_id: str,
        run_id: str,
        attempt_dir: Path,
        codex_home: Path,
        meta: dict[str, Any],
    ) -> None:
        thread_id = meta.get("codex_thread_id")

        def apply(cur: dict[str, Any]) -> None:
            steps = cur.setdefault("steps", {})
            s = steps.setdefault(step_id, {})
            latest = {
                "run_id": run_id,
                "attempt_dir": str(attempt_dir),
                "resume_base_dir": str(codex_home),
            }
            if isinstance(thread_id, str) and thread_id:
                latest["codex_thread_id"] = thread_id
            s["latest"] = latest
            by = s.setdefault("by_run_id", {})
            ent = by.get(run_id) if isinstance(by, dict) else None
            run_ent = ent if isinstance(ent, dict) else {}
            run_ent.update(
                {
                    "run_id": run_id,
                    "attempt_dir": str(attempt_dir),
                    "resume_base_dir": str(codex_home),
                    "status": "running",
                    "ended_at": None,
                }
            )
            if isinstance(thread_id, str) and thread_id:
                run_ent["codex_thread_id"] = thread_id
            by[run_id] = run_ent

        self._store.update_current_atomic(batch_id, job_id, apply)

    def _update_current_thread_id(self, batch_id: str, job_id: str, step_id: str, run_id: str, thread_id: str) -> None:
        def apply(cur: dict[str, Any]) -> None:
            steps = cur.setdefault("steps", {})
            s = steps.setdefault(step_id, {})
            latest = s.get("latest") if isinstance(s.get("latest"), dict) else {}
            if latest.get("run_id") == run_id:
                latest["codex_thread_id"] = thread_id
                s["latest"] = latest
            by = s.get("by_run_id") if isinstance(s.get("by_run_id"), dict) else {}
            ent = by.get(run_id)
            if isinstance(ent, dict):
                ent["codex_thread_id"] = thread_id
                by[run_id] = ent
                s["by_run_id"] = by

        self._store.update_current_atomic(batch_id, job_id, apply)

    def _update_current_on_end(
        self,
        batch_id: str,
        job_id: str,
        step_id: str,
        run_id: str,
        attempt_dir: Path,
        codex_home: Path,
        end_state: dict[str, Any],
        meta: dict[str, Any],
    ) -> None:
        def apply(cur: dict[str, Any]) -> None:
            s = cur.setdefault("steps", {}).setdefault(step_id, {})
            by = s.setdefault("by_run_id", {})
            ent = by.get(run_id) if isinstance(by, dict) else None
            run_ent = ent if isinstance(ent, dict) else {}
            run_ent.update(
                {
                    "run_id": run_id,
                    "attempt_dir": str(attempt_dir),
                    "resume_base_dir": str(codex_home),
                    "status": end_state["status"],
                    "ended_at": end_state["ended_at"],
                }
            )
            if meta.get("codex_thread_id"):
                run_ent["codex_thread_id"] = meta["codex_thread_id"]
            by[run_id] = run_ent

            if end_state["status"] == "succeeded":
                latest = s.get("latest")
                if isinstance(latest, dict):
                    s["latest_successful"] = latest

        self._store.update_current_atomic(batch_id, job_id, apply)

    def _prepare_resume_base(
        self,
        batch_id: str,
        job_id: str,
        step: dict[str, Any],
        batch: dict[str, Any],
        current: dict[str, Any],
        dest_codex_home: Path,
    ) -> dict[str, Any]:
        from parallelhassnes.resume.selectors import resolve_resume_source

        rf = step["resume_from"]
        resolved = resolve_resume_source(self._paths, batch_id=batch_id, job_id=job_id, current=current, resume_from=rf)
        src_step_id = resolved.step_id
        selector = resolved.selector
        source_resume_base = Path(resolved.resume_base_dir)

        pool_cfg = (
            (batch.get("effective_defaults", {}) or {}).get("runner_pool")
            or (self._hcfg.get("defaults", {}) or {}).get("runner_pool")
            or {}
        )
        shared_fs = bool(pool_cfg.get("shared_filesystem", True))
        transfer_enabled = bool(pool_cfg.get("resume_base_transfer_enabled", False))

        source_for_copy = source_resume_base
        transfer_info: dict[str, Any] | None = None
        if not shared_fs and transfer_enabled:
            # Transfer resume base into runner-local storage first (models "copy to destination runner").
            cache = (
                self._paths.runners_root
                / self._cfg.runner_id
                / "resume_bases"
                / batch_id
                / job_id
                / src_step_id
                / resolved.run_id
                / "codex_home"
            )
            if not cache.exists():
                cache.mkdir(parents=True, exist_ok=True)
                if source_resume_base.exists():
                    _copy_session_store(source_resume_base, cache)
            source_for_copy = cache
            transfer_info = {"enabled": True, "from": str(source_resume_base), "to": str(cache)}

        if not source_for_copy.exists():
            raise RuntimeError(f"resume_from source resume_base_dir missing: {source_for_copy}")

        # Copy-on-resume: seed dest codex_home from source codex_home.
        _copy_session_store(source_for_copy, dest_codex_home)

        explicit_tid = None
        if isinstance(rf, dict):
            tid = rf.get("codex_thread_id")
            if isinstance(tid, str) and tid.strip():
                explicit_tid = tid.strip()

        return {
            "parent_run_id": resolved.run_id,
            "resume_from": {"step_id": src_step_id, "selected_run_id": resolved.run_id, "selector": selector},
            "resume_base_copied_from": str(source_for_copy),
            "resume_base_transfer": transfer_info,
            "codex_thread_id": explicit_tid or resolved.codex_thread_id,
        }

    def _provision_codex_credentials(self, codex_home: Path) -> None:
        # Prefer env-injected credentials (avoid writing/symlinking secrets into the runs store).
        if os.environ.get("CODEX_API_KEY") or os.environ.get("OPENAI_API_KEY"):
            return

        # Otherwise, make existing runner-local CLI auth available inside attempt-local CODEX_HOME.
        runner_auth = Path.home() / ".codex" / "auth.json"
        if runner_auth.exists():
            dest = codex_home / "auth.json"
            if not dest.exists():
                dest.symlink_to(runner_auth)

        runner_cfg = Path.home() / ".codex" / "config.toml"
        if runner_cfg.exists():
            dest = codex_home / "config.toml"
            if not dest.exists():
                dest.symlink_to(runner_cfg)

    def _maybe_prepare_retry_resume_base(
        self,
        batch: dict[str, Any],
        job_id: str,
        step: dict[str, Any],
        current: dict[str, Any],
        dest_codex_home: Path,
    ) -> dict[str, Any] | None:
        """
        Retry mode: optionally resume from the previous attempt of the same step.

        This is distinct from user-specified `resume_from` (which points to a different step).
        """
        policy = step.get("retry_policy") or (batch.get("effective_defaults", {}) or {}).get("retries") or {}
        mode = policy.get("mode")
        if mode != "resume_last_attempt":
            return None
        step_id = step.get("step_id")
        if not isinstance(step_id, str) or not step_id:
            return None
        prev = ((current.get("steps") or {}).get(step_id) or {}).get("latest")
        if not isinstance(prev, dict):
            return None
        base = prev.get("resume_base_dir")
        if not isinstance(base, str) or not base:
            return None
        src = Path(base)
        if not src.exists():
            return None
        _copy_session_store(src, dest_codex_home)
        return {
            "parent_run_id": prev.get("run_id"),
            "retry_resume_from": {"run_id": prev.get("run_id"), "job_id": job_id, "step_id": step_id},
            "resume_base_copied_from": str(src),
            "codex_thread_id": prev.get("codex_thread_id"),
        }

    def _invoke_codex(
        self,
        batch: dict[str, Any],
        job: dict[str, Any],
        step: dict[str, Any],
        job_workdir: str,
        codex_home: Path,
        full_prompt: str,
        attempt_dir: Path,
        capture_events_jsonl: bool,
        capture_thread_id: bool,
        on_thread_id: Any,
        invocation: str,
        explicit_resume_thread_id: str | None,
        schema_path: str,
        timeout_seconds: float | None,
        hb: "_HeartbeatWriter",
    ) -> tuple[str, int]:
        # Codex CLI flags have evolved across versions. Cache help text so we can
        # adapt to the installed CLI without hard failing on unknown flags.
        #
        # Example: newer Codex dropped `--ask-for-approval` in favor of
        # `--dangerously-bypass-approvals-and-sandbox` (and trust-based defaults).
        global _CODEX_EXEC_HELP_CACHE
        if _CODEX_EXEC_HELP_CACHE is None:
            try:
                proc = subprocess.run(
                    ["codex", "exec", "--help"],
                    capture_output=True,
                    text=True,
                    stdin=subprocess.DEVNULL,
                    timeout=5,
                )
                _CODEX_EXEC_HELP_CACHE = (proc.stdout or "") + (proc.stderr or "")
            except Exception:
                _CODEX_EXEC_HELP_CACHE = ""
        help_text = _CODEX_EXEC_HELP_CACHE or ""

        env = os.environ.copy()
        env["CODEX_HOME"] = str(codex_home)

        exec_policy = batch.get("effective_defaults", {}).get("execution_policy") or self._hcfg.get("defaults", {}).get("execution_policy", {})
        skip_git = bool(exec_policy.get("skip_git_repo_check", False))
        web_search = bool(exec_policy.get("web_search_enabled", False))
        sandbox = exec_policy.get("sandbox")
        approval = exec_policy.get("approval_policy")
        reasoning_effort = exec_policy.get("model_reasoning_effort")

        approval_s = approval.strip() if isinstance(approval, str) else ""
        supports_ask_for_approval = "--ask-for-approval" in help_text
        supports_bypass = "--dangerously-bypass-approvals-and-sandbox" in help_text
        # `--dangerously-bypass-approvals-and-sandbox` disables sandboxing entirely. Never use it when
        # the harness asked for a sandboxed run (e.g., read-only) — that would violate the policy.
        use_bypass = bool(
            approval_s == "never"
            and supports_bypass
            and not supports_ask_for_approval
            and (not isinstance(sandbox, str) or not sandbox.strip())
        )

        base = ["codex", "exec", "-C", job_workdir, "--color", "never", "--output-schema", schema_path]
        if isinstance(reasoning_effort, str) and reasoning_effort.strip():
            # Codex CLI config key (from ~/.codex/config.toml): model_reasoning_effort = "low"|"medium"|"high"|...
            # Use TOML string quoting via json.dumps for safe escaping.
            base.extend(["--config", f"model_reasoning_effort={json.dumps(reasoning_effort.strip())}"])
        if isinstance(sandbox, str) and sandbox.strip():
            base.extend(["--sandbox", sandbox.strip()])
        if approval_s:
            if supports_ask_for_approval:
                base.extend(["--ask-for-approval", approval_s])
            elif use_bypass:
                base.append("--dangerously-bypass-approvals-and-sandbox")
            else:
                # Best-effort: Codex exec does not always expose `--ask-for-approval`, but it does support
                # `--config`. Pass both plausible config keys; unknown keys are ignored by Codex.
                base.extend(["--config", f"approval_policy={json.dumps(approval_s)}"])
                base.extend(["--config", f"ask_for_approval={json.dumps(approval_s)}"])
        if skip_git:
            base.append("--skip-git-repo-check")
        if web_search:
            base.extend(["--enable", "web_search_request"])

        wants_json = bool(capture_events_jsonl or capture_thread_id)
        events_path = self._paths.attempt_events_path(attempt_dir) if capture_events_jsonl else None
        stdout_path = self._paths.attempt_stdout_path(attempt_dir)
        stderr_path = self._paths.attempt_stderr_path(attempt_dir)
        cmd_path = self._paths.attempt_command_path(attempt_dir)
        final_txt_path = self._paths.attempt_final_txt_path(attempt_dir)
        if wants_json:
            base = base + ["--json", "--output-last-message", str(final_txt_path)]

        cmd = base
        if invocation == "resume":
            if isinstance(explicit_resume_thread_id, str) and explicit_resume_thread_id.strip():
                cmd = base + ["resume", explicit_resume_thread_id.strip()]
            else:
                cmd = base + ["resume", "--last"]

        # Feed prompt via stdin to avoid argv limits.
        if wants_json:
            # Append-only semantics: this is an audit log and must not be truncated if re-opened.
            events_f = open(events_path, "a", encoding="utf-8") if events_path else None
            stdout_f = open(stdout_path, "w", encoding="utf-8")
            stderr_f = open(stderr_path, "w", encoding="utf-8")
            write_once_text(cmd_path, " ".join(shlex.quote(x) for x in (cmd + ["-"])))
            stop_timeout = threading.Event()
            did_timeout = threading.Event()
            killer: threading.Thread | None = None
            stderr_thread: threading.Thread | None = None
            try:
                proc = subprocess.Popen(
                    cmd + ["-"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    start_new_session=True,
                )
                hb.pid = proc.pid
                hb.mark_running(pid=proc.pid)
                if timeout_seconds is not None:
                    def _kill_after_timeout() -> None:
                        if stop_timeout.wait(timeout_seconds):
                            return
                        did_timeout.set()
                        _kill_process_group(proc.pid)

                    killer = threading.Thread(target=_kill_after_timeout, daemon=True)
                    killer.start()
                assert proc.stdin is not None
                proc.stdin.write(full_prompt)
                proc.stdin.close()

                thread_seen: str | None = None
                def _drain_stderr() -> None:
                    assert proc.stderr is not None
                    for line in proc.stderr:
                        stderr_f.write(line)
                        stderr_f.flush()

                assert proc.stderr is not None
                stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
                stderr_thread.start()

                assert proc.stdout is not None
                for line in proc.stdout:
                    stdout_f.write(line)
                    stdout_f.flush()
                    if events_f is not None:
                        events_f.write(line)
                        events_f.flush()
                    if capture_thread_id and thread_seen is None:
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, dict) and obj.get("type") == "thread.started":
                                tid = obj.get("thread_id")
                                if isinstance(tid, str) and tid:
                                    thread_seen = tid
                                    if on_thread_id is not None:
                                        on_thread_id(tid)
                        except Exception:
                            pass
                proc.stdout.close()

                rc = proc.wait() or 0
            finally:
                stop_timeout.set()
                if killer is not None:
                    killer.join(timeout=0.2)
                if events_f is not None:
                    events_f.close()
                if stderr_thread is not None:
                    stderr_thread.join()
                stdout_f.close()
                stderr_f.close()

            if timeout_seconds is not None and did_timeout.is_set():
                raise TimeoutError(f"step timeout after {timeout_seconds}s")
            if rc != 0:
                err_preview = ""
                try:
                    err_preview = (stderr_path.read_text(encoding="utf-8") or "").strip()[:500]
                except Exception:
                    err_preview = ""
                raise RuntimeError(f"codex failed exit={rc} stderr={err_preview}")
            return final_txt_path.read_text(encoding="utf-8"), rc

        proc = subprocess.Popen(
            cmd + ["-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            start_new_session=True,
        )
        hb.pid = proc.pid
        hb.mark_running(pid=proc.pid)
        try:
            out, err = proc.communicate(input=full_prompt, timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _kill_process_group(proc.pid)
            try:
                out, err = proc.communicate(timeout=1)
            except Exception:
                out, err = "", ""
            raise TimeoutError(f"step timeout after {timeout_seconds}s")
        rc = proc.returncode or 0
        try:
            write_once_text(cmd_path, " ".join(shlex.quote(x) for x in (cmd + ["-"])))
            stdout_path.write_text(out or "", encoding="utf-8")
            stderr_path.write_text(err or "", encoding="utf-8")
        except Exception:
            pass
        if rc != 0:
            raise RuntimeError(f"codex failed exit={rc} stderr={(err or '').strip()[:500]}")
        return out or "", rc


def _resolve_output_schema_path(*, job_workdir: str, schema_ref: Any) -> str:
    if not isinstance(schema_ref, str) or not schema_ref.strip():
        raise ValueError("output_schema_ref must be a non-empty string when provided")
    ref = schema_ref.strip()
    p = Path(ref)
    if p.is_absolute():
        return str(p)
    # For consistency with prompt_ref resolution and Launch Table validation, interpret schema refs
    # as paths relative to the job working directory.
    return str((Path(job_workdir) / ref).resolve())


def _copy_dir(src: Path, dst: Path, *, exclude_names: set[str] | None = None) -> None:
    """
    Copy contents of src into existing dst directory.

    Institutional constraint: preserve symlinks as symlinks (do not dereference),
    so we don't accidentally copy credential material into the runs store when
    session stores contain symlinks.
    """
    exclude_names = exclude_names or set()
    for item in src.iterdir():
        if item.name in exclude_names:
            continue
        target = dst / item.name
        if item.is_symlink():
            # Recreate the symlink itself (not its contents).
            try:
                link = os.readlink(item)
            except OSError:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                target.unlink()
            except FileNotFoundError:
                pass
            os.symlink(link, target)
            continue
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True, symlinks=True)
        else:
            shutil.copy2(item, target, follow_symlinks=False)


def _copy_session_store(src: Path, dst: Path) -> None:
    """
    Copy a Codex session store (attempt-local `codex_home/`) while excluding
    credential/config files.

    The runner provisions auth separately into each attempt-local CODEX_HOME
    (e.g., via env or symlink), and secrets must not be duplicated into the runs store.
    """
    _copy_dir(src, dst, exclude_names={"auth.json", "config.toml"})


def _codex_version() -> str:
    try:
        proc = subprocess.run(["codex", "--version"], capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _kill_process_group(pid: int) -> None:
    # We start Codex with start_new_session=True, so pid is the process group id too.
    try:
        os.killpg(pid, signal.SIGKILL)
        return
    except Exception:
        pass
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception:
        pass


def _load_step_prompt(batch: dict[str, Any], job: dict[str, Any], step: dict[str, Any], job_workdir: str) -> str:
    # Packs: prepend pack text when referenced.
    packs = batch.get("packs") or {}
    pack_text = ""
    pack_id = step.get("pack_id") or job.get("pack_id") or batch.get("default_pack_id")
    if pack_id:
        p = packs.get(pack_id)
        if isinstance(p, dict) and isinstance(p.get("text"), str):
            pack_text = p["text"].strip() + "\n\n"

    if "prompt" in step and isinstance(step["prompt"], str):
        base = step["prompt"]
    elif "prompt_ref" in step and isinstance(step["prompt_ref"], str):
        base = (Path(job_workdir) / step["prompt_ref"]).read_text(encoding="utf-8")
    else:
        raise ValueError(f"step {step.get('step_id')} must have prompt or prompt_ref")

    append = ""
    if step.get("append_prompt") and isinstance(step["append_prompt"], str):
        append = "\n\n" + step["append_prompt"]

    return pack_text + base + append


def _apply_fake_workspace_writes(job_workdir: str, step: dict[str, Any]) -> None:
    """
    Test-only hook: when using the fake invoker, optionally write files into the job workspace.

    This exists to validate workspace continuity/invariants in unit tests without spawning Codex.

    Supported fields on step:
    - fake_workspace_writes: [{ "path": "relative/path.txt", "content": "..." }, ...]
    - fake_workspace_assert_exists: ["relative/path.txt", ...]
    """
    root = Path(job_workdir).resolve()

    def safe_path(rel: str) -> Path:
        p = (root / rel).resolve()
        try:
            p.relative_to(root)
        except Exception as e:
            raise ValueError(f"fake workspace write escapes job_workdir: {rel}") from e
        return p

    writes = step.get("fake_workspace_writes")
    if isinstance(writes, list):
        for ent in writes:
            if not isinstance(ent, dict):
                continue
            rel = ent.get("path")
            if not isinstance(rel, str) or not rel.strip():
                continue
            content = ent.get("content")
            if content is None:
                content = ""
            if not isinstance(content, str):
                content = str(content)
            p = safe_path(rel)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8")

    asserts = step.get("fake_workspace_assert_exists")
    if isinstance(asserts, list):
        missing: list[str] = []
        for rel in asserts:
            if not isinstance(rel, str) or not rel.strip():
                continue
            p = safe_path(rel)
            if not p.exists():
                missing.append(rel)
        if missing:
            raise FileNotFoundError("fake workspace missing required file(s): " + ", ".join(missing))


def _fake_run_report_text(batch_id: str, job_id: str, step_id: str, run_id: str, exit_code: int) -> str:
    return json.dumps(
        {
            "schema_version": "1.0.0",
            "status": "ok" if exit_code == 0 else "failed",
            "summary": f"fake run report for {batch_id}/{job_id}/{step_id}/{run_id}",
            "files_read": [],
            "files_written": [],
            "artifacts": [],
        }
    )


def _fake_exit_code(step: dict[str, Any], attempt_index: int) -> int:
    # Test hook: allow deterministic failures/successes without spawning Codex.
    if "fake_exit_codes" in step and isinstance(step["fake_exit_codes"], list):
        codes = [c for c in step["fake_exit_codes"] if isinstance(c, int)]
        if attempt_index < len(codes):
            return int(codes[attempt_index])
        return int(codes[-1]) if codes else 0
    if "fake_exit_code" in step and isinstance(step["fake_exit_code"], int):
        return int(step["fake_exit_code"])
    return 0


class _HeartbeatWriter:
    def __init__(
        self,
        state_path: Path,
        interval_seconds: int,
        started_at: str,
        artifacts: list[dict[str, Any]],
        errors: list[str],
        on_heartbeat: Callable[[], None] | None = None,
    ) -> None:
        self._state_path = state_path
        self._interval = interval_seconds
        self._stop = threading.Event()
        self._status: str = "queued"
        self.started_at: str = started_at
        self.last_heartbeat_at: str | None = None
        self.current_item: str | None = None
        self.pid: int | None = None
        self._artifacts = artifacts
        self._errors = errors
        self._on_heartbeat = on_heartbeat
        self._thread: threading.Thread | None = None

    def write_queued(self) -> None:
        self._status = "queued"
        write_state_guarded(
            self._state_path,
            {
                "status": "queued",
                "started_at": self.started_at,
                "last_heartbeat_at": self.last_heartbeat_at,
                "current_item": self.current_item or "queued/initializing",
                "pid": self.pid,
                "exit_code": None,
                "artifacts": list(self._artifacts),
                "errors": list(self._errors),
            },
        )

    def mark_running(self, pid: int | None) -> None:
        self.pid = pid
        self._status = "running"
        self.last_heartbeat_at = utc_isoformat()
        write_state_guarded(
            self._state_path,
            {
                "status": "running",
                "started_at": self.started_at,
                "last_heartbeat_at": self.last_heartbeat_at,
                "current_item": self.current_item,
                "pid": self.pid,
                "exit_code": None,
                "artifacts": list(self._artifacts),
                "errors": list(self._errors),
            },
        )
        if self._on_heartbeat is not None:
            try:
                self._on_heartbeat()
            except Exception:
                pass
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            if self._status != "running":
                continue
            self.last_heartbeat_at = utc_isoformat()
            write_state_guarded(
                self._state_path,
                {
                    "status": "running",
                    "started_at": self.started_at,
                    "last_heartbeat_at": self.last_heartbeat_at,
                    "current_item": self.current_item,
                    "pid": self.pid,
                    "exit_code": None,
                    "artifacts": list(self._artifacts),
                    "errors": list(self._errors),
                },
            )
            if self._on_heartbeat is not None:
                try:
                    self._on_heartbeat()
                except Exception:
                    pass
