from __future__ import annotations

import unittest

from run_interleaved_backtest_2y import resolve_entry_exec_mode


class EntryExecModeTests(unittest.TestCase):
    def test_non_auto_modes_passthrough(self) -> None:
        self.assertEqual(resolve_entry_exec_mode("market", 1, 3, 0), "market")
        self.assertEqual(resolve_entry_exec_mode("limit", 2, 3, 0), "limit")

    def test_auto_min_threshold_mode(self) -> None:
        self.assertEqual(resolve_entry_exec_mode("auto", 1, 3, 0), "limit")
        self.assertEqual(resolve_entry_exec_mode("auto", 2, 3, 0), "limit")
        self.assertEqual(resolve_entry_exec_mode("auto", 3, 3, 0), "market")

    def test_auto_max_threshold_mode(self) -> None:
        # L1/L2 market, L3 limit
        self.assertEqual(resolve_entry_exec_mode("auto", 1, 3, 2), "market")
        self.assertEqual(resolve_entry_exec_mode("auto", 2, 3, 2), "market")
        self.assertEqual(resolve_entry_exec_mode("auto", 3, 3, 2), "limit")

    def test_auto_max_has_higher_priority_than_min(self) -> None:
        # max>0 should override min behavior
        self.assertEqual(resolve_entry_exec_mode("auto", 3, 3, 2), "limit")


if __name__ == "__main__":
    unittest.main()
