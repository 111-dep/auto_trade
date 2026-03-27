from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.models import PositionState
from okx_trader.runtime_execute_decision import execute_decision


class _GuardClient:
    def __init__(self) -> None:
        self.request_calls = 0

    def _request(self, *args, **kwargs):
        self.request_calls += 1
        raise AssertionError("binance should not call OKX positions-history lookup")

    def get_instrument(self, inst_id: str) -> dict:
        return {"lotSz": "1", "minSz": "1", "ctVal": "1", "ctValCcy": "USDT"}


class BinancePositionsHistoryGuardTests(unittest.TestCase):
    def _cfg(self) -> SimpleNamespace:
        return SimpleNamespace(
            exchange_provider="binance",
            alert_only=False,
            trade_journal_enabled=False,
            td_mode="isolated",
            params=SimpleNamespace(
                exec_l3_inst_ids=[],
                exec_max_level=2,
                tp1_r_mult=1.5,
                tp2_r_mult=2.5,
                open_window_hours=24,
                stop_streak_freeze_count=0,
                stop_streak_freeze_hours=0,
                stop_streak_l2_only=False,
            ),
        )

    def _sig(self) -> dict:
        return {
            "bias": "LONG",
            "close": 101.0,
            "ema": 100.0,
            "rsi": 55.0,
            "macd_hist": 0.1,
            "bb_width": 0.02,
            "bb_width_avg": 0.01,
            "htf_close": 100.0,
            "htf_ema_fast": 99.0,
            "htf_ema_slow": 98.0,
            "htf_rsi": 60.0,
            "strategy_variant": "pa_oral_baseline_v1",
            "location_long_ok": True,
            "location_short_ok": False,
            "fib_touch_long": True,
            "fib_touch_short": False,
            "retest_long": True,
            "retest_short": False,
            "smc_sweep_long": False,
            "smc_sweep_short": False,
            "smc_bullish_fvg": False,
            "smc_bearish_fvg": False,
            "long_entry": False,
            "short_entry": False,
            "long_entry_l2": False,
            "short_entry_l2": False,
            "long_entry_l3": False,
            "short_entry_l3": False,
            "long_level": 0,
            "short_level": 0,
            "long_exit": False,
            "short_exit": False,
            "long_stop": 95.0,
            "short_stop": 105.0,
            "atr": 2.0,
            "signal_ts_ms": 1_700_000_000_000,
        }

    def test_binance_skips_okx_positions_history_lookup(self) -> None:
        client = _GuardClient()
        state = {
            "trade": {
                "side": "long",
                "managed_by": "script",
                "entry_price": 100.0,
                "remaining_size": 1.0,
                "open_size": 1.0,
                "hard_stop": 95.0,
                "created_ts_ms": 1_699_999_999_000,
            }
        }

        with patch("okx_trader.runtime_execute_decision.resolve_entry_decision", return_value=None), patch(
            "okx_trader.runtime_execute_decision.cancel_managed_tp1_order", return_value=None
        ), patch("okx_trader.runtime_execute_decision.cancel_managed_tp2_order", return_value=None), patch(
            "okx_trader.runtime_execute_decision.log"
        ) as mock_log:
            execute_decision(
                client=client,
                cfg=self._cfg(),
                inst_id="BTC-USDT-SWAP",
                sig=self._sig(),
                pos=PositionState("flat", 0.0),
                state=state,
                root_state={},
                profile_id="DEFAULT",
            )

        self.assertEqual(client.request_calls, 0)
        self.assertNotIn("trade", state)
        warn_lines = [str(call.args[0]) for call in mock_log.call_args_list if call.args]
        self.assertFalse(any("Positions-history lookup failed" in line for line in warn_lines))


if __name__ == "__main__":
    unittest.main()
