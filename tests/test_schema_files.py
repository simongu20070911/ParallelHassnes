from __future__ import annotations

import json
import unittest
from pathlib import Path


class SchemaFilesTests(unittest.TestCase):
    def test_schema_files_are_valid_json(self) -> None:
        root = Path(__file__).resolve().parents[1]
        paths = [
            root / "run_report.schema.json",
            root / "schemas" / "launch_table.normalized.schema.json",
            root / "schemas" / "attempt_meta.schema.json",
            root / "schemas" / "attempt_state.schema.json",
        ]
        for p in paths:
            self.assertTrue(p.exists(), str(p))
            obj = json.loads(p.read_text(encoding="utf-8"))
            self.assertIsInstance(obj, dict)
            self.assertIn("$schema", obj)


if __name__ == "__main__":
    unittest.main()

