from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from parallelhassnes.core.time import utc_compact_timestamp


@dataclass(frozen=True)
class Paths:
    runs_root: Path
    runners_root: Path

    def system_dir(self) -> Path:
        return self.runs_root / "_system"

    def harness_config_path(self) -> Path:
        return self.system_dir() / "harness_config.json"

    def harness_config_versions_dir(self) -> Path:
        return self.system_dir() / "harness_config_versions"

    def batch_dir(self, batch_id: str) -> Path:
        return self.runs_root / batch_id

    def batch_meta_path(self, batch_id: str) -> Path:
        return self.batch_dir(batch_id) / "batch_meta.json"

    def batch_scheduler_state_path(self, batch_id: str) -> Path:
        return self.batch_dir(batch_id) / "scheduler_state.json"

    def job_dir(self, batch_id: str, job_id: str) -> Path:
        return self.batch_dir(batch_id) / job_id

    def current_path(self, batch_id: str, job_id: str) -> Path:
        return self.job_dir(batch_id, job_id) / "current.json"

    def step_dir(self, batch_id: str, job_id: str, step_id: str) -> Path:
        return self.job_dir(batch_id, job_id) / "steps" / step_id

    def attempts_dir(self, batch_id: str, job_id: str, step_id: str) -> Path:
        return self.step_dir(batch_id, job_id, step_id) / "attempts"

    def run_dir_name(self, run_id: str, started_compact_utc: str | None = None) -> str:
        ts = started_compact_utc or utc_compact_timestamp()
        return f"{ts}_{run_id}"

    def attempt_dir(self, batch_id: str, job_id: str, step_id: str, run_dir_name: str) -> Path:
        return self.attempts_dir(batch_id, job_id, step_id) / run_dir_name

    def attempt_codex_home(self, attempt_dir: Path) -> Path:
        return attempt_dir / "codex_home"

    def attempt_meta_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "meta.json"

    def attempt_state_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "state.json"

    def attempt_final_json_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "final.json"

    def attempt_final_txt_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "final.txt"

    def attempt_handoff_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "handoff.json"

    def attempt_events_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "codex.events.jsonl"

    def attempt_stderr_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "codex.stderr.txt"

    def attempt_stdout_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "codex.stdout.txt"

    def attempt_command_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "codex.command.txt"

    def attempt_git_head_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "git_head.txt"

    def attempt_git_status_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "git_status.txt"

    def attempt_git_diff_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "git_diff.patch"

    def attempt_files_touched_path(self, attempt_dir: Path) -> Path:
        return attempt_dir / "files_touched.json"

    def runner_health_path(self, runner_id: str) -> Path:
        return self.runners_root / runner_id / "health.json"
