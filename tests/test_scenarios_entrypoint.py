from __future__ import annotations

import unittest

from parallelhassnes.scenarios.runner import run_scenarios


class ScenariosEntrypointTests(unittest.TestCase):
    def test_run_scenarios_smoke(self) -> None:
        res = run_scenarios()
        self.assertTrue(res.get("ok"), res)


if __name__ == "__main__":
    unittest.main()

