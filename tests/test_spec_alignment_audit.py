from __future__ import annotations

import unittest
from pathlib import Path

from tools.spec_alignment_audit import audit


class SpecAlignmentAuditTests(unittest.TestCase):
    def test_no_missing_mappings_or_gaps(self) -> None:
        report = audit(write_matrix_path=None)
        self.assertEqual(report["missing_mappings"], [])
        self.assertEqual(report["extra_mappings"], [])
        self.assertEqual(report["gaps"], [])
        self.assertEqual(report["partial"], [])
        self.assertEqual(report["bad_evidence"], [])

