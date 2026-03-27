from __future__ import annotations

import unittest

from okx_trader.backtest import eval_signal_outcome
from okx_trader.models import Candle


def _c(ts: int, o: float, h: float, l: float, c: float) -> Candle:
    return Candle(ts_ms=ts, open=o, high=h, low=l, close=c, confirm=True, volume=0.0)


class ManagedExitTests(unittest.TestCase):
    def test_live_style_close_based_tp1_and_tightened_stop(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 108, 100.2, 108),
            _c(2, 108, 108, 100.8, 101),
        ]
        signal_map = {
            1: {"close": 108.0, "high": 108.0, "low": 100.2, "atr": 1.0, "long_stop": 95.0},
            2: {"close": 101.0, "high": 108.0, "low": 100.8, "atr": 1.0, "long_stop": 102.0},
        }
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=3,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
            signal_lookup=lambda idx: signal_map.get(idx),
            trail_after_tp1=True,
            trail_atr_mult=0.0,
            signal_exit_enabled=False,
        )
        self.assertEqual(outcome, "TP1")
        self.assertAlmostEqual(r_value, 0.9, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 2)

    def test_live_style_disable_auto_tighten_keeps_initial_stop_until_tp1(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 101, 99.5, 101),
            _c(2, 101, 101, 93.5, 94),
        ]
        signal_map = {
            1: {"close": 101.0, "high": 101.0, "low": 99.5, "atr": 1.0, "long_stop": 102.0},
            2: {"close": 94.0, "high": 101.0, "low": 93.5, "atr": 1.0, "long_stop": 93.0},
        }
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=3,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
            signal_lookup=lambda idx: signal_map.get(idx),
            trail_after_tp1=True,
            auto_tighten_stop=False,
            trail_atr_mult=0.0,
            signal_exit_enabled=False,
        )
        self.assertEqual(outcome, "STOP")
        self.assertAlmostEqual(r_value, -1.0, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 2)

    def test_live_style_without_split_ignores_intrabar_tp1_wick(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 108, 100.0, 106),
            _c(2, 106, 106, 93.5, 94),
        ]
        signal_map = {
            1: {"close": 106.0, "high": 108.0, "low": 100.0, "atr": 1.0, "long_stop": 95.0},
            2: {"close": 94.0, "high": 106.0, "low": 93.5, "atr": 1.0, "long_stop": 95.0},
        }
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=3,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
            signal_lookup=lambda idx: signal_map.get(idx),
            trail_after_tp1=True,
            trail_atr_mult=0.0,
            signal_exit_enabled=False,
        )
        self.assertEqual(outcome, "STOP")
        self.assertAlmostEqual(r_value, 0.0, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 2)

    def test_live_style_split_tp_keeps_intrabar_stop_first(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 108, 94, 100),
        ]
        signal_map = {
            1: {"close": 100.0, "high": 108.0, "low": 94.0, "atr": 1.0, "long_stop": 95.0},
        }
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=2,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
            signal_lookup=lambda idx: signal_map.get(idx),
            trail_after_tp1=True,
            trail_atr_mult=0.0,
            signal_exit_enabled=False,
            split_tp_enabled=True,
        )
        self.assertEqual(outcome, "STOP")
        self.assertAlmostEqual(r_value, -1.0, places=6)
        self.assertEqual(held, 1)
        self.assertEqual(exit_idx, 1)

    def test_long_tp1_then_be_stop(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 108, 100.2, 107),  # hit TP1 without touching BE in same bar
            _c(2, 107, 101, 99, 100),  # pullback hit BE
        ]
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=3,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
        )
        self.assertEqual(outcome, "TP1")
        self.assertAlmostEqual(r_value, 0.75, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 2)

    def test_long_tp1_then_tp2(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 108, 100.2, 107),  # hit TP1 without touching BE in same bar
            _c(2, 107, 113, 106, 112),  # then TP2
        ]
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=107.5,
            tp2=112.5,
            ltf_candles=candles,
            start_idx=0,
            horizon_bars=3,
            managed_exit=True,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
        )
        self.assertEqual(outcome, "TP2")
        self.assertAlmostEqual(r_value, 2.0, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 2)

    def test_next_open_entry_can_stop_on_entry_bar(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 101, 94.5, 96),
            _c(2, 96, 97, 95, 96),
        ]
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=110.0,
            tp2=110.0,
            ltf_candles=candles,
            start_idx=1,
            horizon_bars=2,
            managed_exit=False,
            include_start_bar=True,
        )
        self.assertEqual(outcome, "STOP")
        self.assertAlmostEqual(r_value, -1.0, places=6)
        self.assertEqual(held, 0)
        self.assertEqual(exit_idx, 1)

    def test_time_exit_closes_at_bar_close_after_limit(self) -> None:
        candles = [
            _c(0, 100, 100, 100, 100),
            _c(1, 100, 101, 99.5, 100.2),
            _c(2, 100.2, 101.0, 99.8, 100.4),
            _c(3, 100.4, 101.2, 100.0, 100.6),
            _c(4, 100.6, 101.1, 100.1, 100.8),
        ]
        outcome, r_value, held, exit_idx = eval_signal_outcome(
            side="LONG",
            entry=100.0,
            stop=95.0,
            tp1=120.0,
            tp2=120.0,
            ltf_candles=candles,
            start_idx=1,
            horizon_bars=4,
            managed_exit=False,
            include_start_bar=True,
            max_hold_bars=2,
        )
        self.assertEqual(outcome, "TIME")
        self.assertAlmostEqual(r_value, (100.6 - 100.0) / 5.0, places=6)
        self.assertEqual(held, 2)
        self.assertEqual(exit_idx, 3)


if __name__ == "__main__":
    unittest.main()
