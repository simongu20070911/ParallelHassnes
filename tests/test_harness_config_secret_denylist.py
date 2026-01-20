from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from parallelhassnes.config.harness_config import HarnessConfig, write_harness_config_snapshots
from parallelhassnes.core.paths import Paths
from parallelhassnes.storage.runs_store import RunsStore


class HarnessConfigSecretDenylistTests(unittest.TestCase):
    def test_rejects_secret_like_key_names(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = HarnessConfig.default(runs_root=root / "runs", runners_root=root / "runners")
            cfg2 = HarnessConfig(
                runs_root=cfg.runs_root,
                runners_root=cfg.runners_root,
                harness_config_version=cfg.harness_config_version,
                written_at=cfg.written_at,
                interfaces=cfg.interfaces,
                defaults={**cfg.defaults, "api_token": "abc"},
                limits=cfg.limits,
            )
            with self.assertRaises(ValueError):
                write_harness_config_snapshots(cfg2)

    def test_rejects_private_key_material(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = HarnessConfig.default(runs_root=root / "runs", runners_root=root / "runners")
            cfg2 = HarnessConfig(
                runs_root=cfg.runs_root,
                runners_root=cfg.runners_root,
                harness_config_version=cfg.harness_config_version,
                written_at=cfg.written_at,
                interfaces=cfg.interfaces,
                defaults={**cfg.defaults, "note": "-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----"},
                limits=cfg.limits,
            )
            with self.assertRaises(ValueError):
                write_harness_config_snapshots(cfg2)

    def test_store_read_rejects_secrets_in_existing_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            runs_root.mkdir(parents=True, exist_ok=True)
            (runs_root / "_system").mkdir(parents=True, exist_ok=True)
            # Write a config that includes a forbidden key; store.read_harness_config must reject it.
            (runs_root / "_system" / "harness_config.json").write_text(
                '{\n'
                '  "harness_config_version": "hc_bad",\n'
                '  "written_at": "2026-01-15T00:00:00Z",\n'
                '  "interfaces": {"api_mode": {"enabled": false, "auth_mode": "none"}, "filesystem_queue_mode": {"enabled": true}},\n'
                '  "defaults": {"scoreboard": {"heartbeat_stale_after_seconds": 2700, "heartbeat_interval_seconds": 900}, "api_token": "abc"},\n'
                '  "limits": {"max_jobs_per_batch": 1, "max_steps_per_job": 1, "per_workdir_concurrency": {}}\n'
                '}\n',
                encoding="utf-8",
            )
            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            with self.assertRaises(ValueError):
                store.read_harness_config()
