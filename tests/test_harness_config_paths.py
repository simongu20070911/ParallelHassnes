from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from parallelhassnes.harness.harness import Harness
from parallelhassnes.core.paths import Paths
from parallelhassnes.storage.runs_store import RunsStore


class HarnessConfigPathsTests(unittest.TestCase):
    def test_harness_config_includes_queue_root_path_when_materialized(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            runs_root = root / "runs"
            runners_root = root / "runners"
            queue_root = runs_root / "_queue"

            # Provide baseline schema at the expected lookup location.
            src_schema = Path(__file__).resolve().parents[1] / "run_report.schema.json"
            (root / "run_report.schema.json").write_text(src_schema.read_text(encoding="utf-8"), encoding="utf-8")

            harness = Harness(runs_root=runs_root, runners_root=runners_root, queue_root=queue_root)
            # Tick once to materialize default harness_config.json.
            harness.tick_once(concurrency_override=None, use_fake_invoker=True)

            store = RunsStore(Paths(runs_root=runs_root, runners_root=runners_root))
            cfg = store.read_harness_config()
            paths = cfg.get("paths") or {}
            # On macOS, /var/... may resolve to /private/var/....
            self.assertEqual(Path(paths.get("queue_root")).resolve(), queue_root.resolve())
