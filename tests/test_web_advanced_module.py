from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from advanced_module import (
    build_test_case_rows,
    component_pattern,
    discover_test_ids,
    summarize_metrics,
    technology_stack,
    update_test_case_status,
)


class WebAdvancedModuleTests(unittest.TestCase):
    def test_component_pattern_for_web(self) -> None:
        self.assertEqual(component_pattern("web"), "test_web*.py")
        self.assertEqual(component_pattern("frontend"), "test_web*.py")

    def test_summarize_metrics_reports_pass_rate_and_performance(self) -> None:
        metrics = summarize_metrics(
            total=10,
            completed=8,
            passed=6,
            failed=1,
            errors=1,
            skipped=0,
            duration_sec=4.0,
        )
        self.assertEqual(metrics["remaining"], 2)
        self.assertEqual(metrics["progress_pct"], 80.0)
        self.assertEqual(metrics["pass_rate_pct"], 75.0)
        self.assertEqual(metrics["failure_rate_pct"], 25.0)
        self.assertEqual(metrics["tests_per_sec"], 2.0)
        self.assertEqual(metrics["avg_test_ms"], 500.0)

    def test_discover_test_ids_filters_by_pattern(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "test_web_sample.py").write_text(
                "import unittest\n\n"
                "class T(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        self.assertTrue(True)\n",
                encoding="utf-8",
            )
            (root / "test_tg_sample.py").write_text(
                "import unittest\n\n"
                "class T2(unittest.TestCase):\n"
                "    def test_ok(self):\n"
                "        self.assertTrue(True)\n",
                encoding="utf-8",
            )
            ids = discover_test_ids(root, "test_web*.py")
            self.assertEqual(len(ids), 1)
            self.assertIn("test_web_sample.T.test_ok", ids[0])

    def test_technology_stack_contains_web_entries(self) -> None:
        stack = technology_stack()
        self.assertIn("web", stack)
        web_items = stack["web"]
        self.assertTrue(isinstance(web_items, list) and len(web_items) > 0)
        names = {str(item.get("name") or "") for item in web_items}
        self.assertIn("JavaScript ES Modules", names)

    def test_build_and_update_case_rows(self) -> None:
        rows = build_test_case_rows(["tests.test_web_mod.Case.test_a", "tests.test_web_mod.Case.test_b"])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["status"], "pending")
        self.assertEqual(rows[0]["label"], "Case.test_a")
        changed = update_test_case_status(rows, "tests.test_web_mod.Case.test_a", "passed")
        self.assertTrue(changed)
        self.assertEqual(rows[0]["status"], "passed")


if __name__ == "__main__":
    unittest.main()
