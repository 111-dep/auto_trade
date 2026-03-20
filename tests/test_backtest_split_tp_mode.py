from __future__ import annotations

import unittest

from okx_trader.backtest import resolve_backtest_split_tp_enabled


class BacktestSplitTpModeTests(unittest.TestCase):
    def test_enabled_when_config_allows_native_split(self) -> None:
        self.assertTrue(
            resolve_backtest_split_tp_enabled(
                attach_tpsl_on_entry=True,
                enable_close=True,
                split_tp_on_entry=True,
                tp1_close_pct=0.5,
            )
        )

    def test_force_managed_tp_fallback_disables_split(self) -> None:
        self.assertFalse(
            resolve_backtest_split_tp_enabled(
                attach_tpsl_on_entry=True,
                enable_close=True,
                split_tp_on_entry=True,
                tp1_close_pct=0.5,
                force_managed_tp_fallback=True,
            )
        )

    def test_invalid_tp1_pct_disables_split(self) -> None:
        self.assertFalse(
            resolve_backtest_split_tp_enabled(
                attach_tpsl_on_entry=True,
                enable_close=True,
                split_tp_on_entry=True,
                tp1_close_pct=0.0,
            )
        )


if __name__ == "__main__":
    unittest.main()
