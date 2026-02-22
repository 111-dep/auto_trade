from __future__ import annotations

import unittest

from okx_trader.backtest import eval_signal_outcome
from okx_trader.models import Candle


def _c(ts: int, o: float, h: float, l: float, c: float) -> Candle:
    return Candle(ts_ms=ts, open=o, high=h, low=l, close=c, confirm=True, volume=0.0)


class ManagedExitTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
