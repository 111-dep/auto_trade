from __future__ import annotations

import unittest
from types import SimpleNamespace

from okx_trader.entry_exec_policy import (
    resolve_entry_exec_mode,
    resolve_entry_exec_mode_for_params,
    resolve_entry_limit_fallback_mode_for_params,
    resolve_entry_limit_ttl_sec_for_params,
)


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


class EntryExecPolicyPerLevelTests(unittest.TestCase):
    def test_level_specific_exec_mode_override_wins(self) -> None:
        params = SimpleNamespace(
            entry_exec_mode="auto",
            entry_auto_market_level_min=3,
            entry_auto_market_level_max=2,
            entry_exec_mode_l1="",
            entry_exec_mode_l2="market",
            entry_exec_mode_l3="limit",
        )
        self.assertEqual(resolve_entry_exec_mode_for_params(params, 1), "market")
        self.assertEqual(resolve_entry_exec_mode_for_params(params, 2), "market")
        self.assertEqual(resolve_entry_exec_mode_for_params(params, 3), "limit")

    def test_level_specific_limit_ttl_override(self) -> None:
        params = SimpleNamespace(
            entry_limit_ttl_sec=5,
            entry_limit_ttl_sec_l1=0,
            entry_limit_ttl_sec_l2=2,
            entry_limit_ttl_sec_l3=4,
        )
        self.assertEqual(resolve_entry_limit_ttl_sec_for_params(params, 1), 0)
        self.assertEqual(resolve_entry_limit_ttl_sec_for_params(params, 2), 2)
        self.assertEqual(resolve_entry_limit_ttl_sec_for_params(params, 3), 4)

    def test_level_specific_fallback_override(self) -> None:
        params = SimpleNamespace(
            entry_limit_fallback_mode="market",
            entry_limit_fallback_mode_l1="market",
            entry_limit_fallback_mode_l2="market",
            entry_limit_fallback_mode_l3="skip",
        )
        self.assertEqual(resolve_entry_limit_fallback_mode_for_params(params, 1), "market")
        self.assertEqual(resolve_entry_limit_fallback_mode_for_params(params, 2), "market")
        self.assertEqual(resolve_entry_limit_fallback_mode_for_params(params, 3), "skip")


if __name__ == "__main__":
    unittest.main()
