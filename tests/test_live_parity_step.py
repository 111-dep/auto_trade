from __future__ import annotations

import unittest
from types import SimpleNamespace

from okx_trader.decision_core import EntryDecision
from run_interleaved_backtest_2y import _new_sim_position, _simulate_live_position_step


def _params() -> SimpleNamespace:
    return SimpleNamespace(
        tp1_close_pct=0.5,
        tp1_r_mult=1.5,
        tp2_r_mult=2.5,
        tp2_close_rest=True,
        be_trigger_r_mult=1.0,
        be_offset_pct=0.0,
        be_fee_buffer_pct=0.0,
        trail_after_tp1=True,
        trail_atr_mult=1.5,
    )


class LiveParityStepTests(unittest.TestCase):
    def test_long_exit_with_reverse_signal(self) -> None:
        params = _params()
        long_decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)
        reverse_decision = EntryDecision(
            side="SHORT",
            level=2,
            entry=99.0,
            stop=103.0,
            risk=4.0,
            tp1=93.0,
            tp2=89.0,
        )
        sig = {
            "close": 99.0,
            "atr": 1.0,
            "long_stop": 95.0,
            "short_stop": 103.0,
            "long_exit": True,
            "short_exit": False,
        }

        res = _simulate_live_position_step(
            pos=pos,
            sig=sig,
            params=params,
            decision=reverse_decision,
            allow_reverse=True,
            managed_exit=True,
        )
        self.assertTrue(res["closed"])
        self.assertEqual(res["outcome"], "EXIT")
        self.assertFalse(res["is_stop"])
        self.assertIsNotNone(res["reverse_decision"])

    def test_tp1_partial_then_long_exit_marks_tp1(self) -> None:
        params = _params()
        long_decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)

        hit_tp1 = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 108.0,
                "atr": 1.0,
                "long_stop": 95.0,
                "short_stop": 103.0,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertFalse(hit_tp1["closed"])
        self.assertTrue(pos["tp1_done"])

        close_by_exit = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 106.8,
                "atr": 1.0,
                "long_stop": 95.0,
                "short_stop": 103.0,
                "long_exit": True,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertTrue(close_by_exit["closed"])
        self.assertEqual(close_by_exit["outcome"], "TP1")
        self.assertFalse(close_by_exit["is_stop"])
        self.assertGreater(close_by_exit["r_raw"], 1.0)

    def test_tp1_partial_then_short_stop_keeps_stop_flag(self) -> None:
        params = _params()
        short_decision = EntryDecision(
            side="SHORT",
            level=2,
            entry=100.0,
            stop=105.0,
            risk=5.0,
            tp1=92.5,
            tp2=87.5,
        )
        pos = _new_sim_position(decision=short_decision, entry_ts=1, entry_i=1, risk_amt=10.0)

        hit_tp1 = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 92.0,
                "atr": 1.0,
                "long_stop": 97.0,
                "short_stop": 105.0,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertFalse(hit_tp1["closed"])
        self.assertTrue(pos["tp1_done"])

        stop_out = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 101.0,
                "atr": 1.0,
                "long_stop": 97.0,
                "short_stop": 104.5,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertTrue(stop_out["closed"])
        self.assertEqual(stop_out["outcome"], "TP1")
        self.assertTrue(stop_out["is_stop"])

    def test_split_tp_same_bar_stop_first_is_conservative(self) -> None:
        params = _params()
        long_decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)
        pos["exchange_split_tp_enabled"] = True

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 100.0,
                "high": 108.0,
                "low": 94.0,
                "atr": 1.0,
                "long_stop": 95.0,
                "short_stop": 103.0,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertTrue(res["closed"])
        self.assertEqual(res["outcome"], "STOP")
        self.assertTrue(res["is_stop"])
        self.assertAlmostEqual(res["r_raw"], -1.0, places=6)

    def test_split_tp_intrabar_then_close_below_be_closes_remainder(self) -> None:
        params = _params()
        long_decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)
        pos["exchange_split_tp_enabled"] = True

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 99.0,
                "high": 108.0,
                "low": 96.0,
                "atr": 1.0,
                "long_stop": 95.0,
                "short_stop": 103.0,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertTrue(res["closed"])
        self.assertEqual(res["outcome"], "TP1")
        self.assertTrue(res["is_stop"])
        self.assertAlmostEqual(res["r_raw"], 0.65, places=6)


if __name__ == "__main__":
    unittest.main()
