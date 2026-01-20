from __future__ import annotations

import unittest
from pathlib import Path

from parallelhassnes.lint.terminology import lint_no_synonym_drift


class TerminologyLintTests(unittest.TestCase):
    def test_docs_have_no_forbidden_phrases(self) -> None:
        root = Path(__file__).resolve().parents[1]
        errs = lint_no_synonym_drift(root)
        self.assertEqual(errs, [])


if __name__ == "__main__":
    unittest.main()

