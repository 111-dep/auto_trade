from __future__ import annotations

import unittest
from types import SimpleNamespace

from okx_trader.decision_core import EntryDecision
from run_interleaved_backtest_2y import (
    _apply_cached_trade_trajectory_step,
    _build_interleaved_trade_trajectory,
    _invert_signal_symmetric,
    _new_sim_position,
    _simulate_live_position_step,
)


def _params() -> SimpleNamespace:
    return SimpleNamespace(
        tp1_close_pct=0.5,
        tp1_r_mult=1.5,
        tp2_r_mult=2.5,
        tp2_close_rest=True,
        be_trigger_r_mult=1.0,
        be_offset_pct=0.0,
        be_fee_buffer_pct=0.0,
        auto_tighten_stop=True,
        trail_after_tp1=True,
        trail_atr_mult=1.5,
    )


class LiveParityStepTests(unittest.TestCase):
    def test_invert_signal_swaps_direction_and_mirrors_stop(self) -> None:
        sig = {
            "close": 100.0,
            "long_entry": True,
            "short_entry": False,
            "long_entry_l2": True,
            "short_entry_l2": False,
            "long_entry_l3": True,
            "short_entry_l3": False,
            "long_level": 2,
            "short_level": 0,
            "long_stop": 95.0,
            "short_stop": 104.0,
            "long_exit": False,
            "short_exit": True,
            "bias": "long",
            "vote_winner": "LONG",
        }
        out = _invert_signal_symmetric(sig)
        self.assertFalse(out["long_entry"])
        self.assertTrue(out["short_entry"])
        self.assertEqual(out["short_level"], 2)
        self.assertEqual(out["bias"], "short")
        self.assertEqual(out["vote_winner"], "SHORT")
        self.assertAlmostEqual(out["long_stop"], 96.0, places=6)
        self.assertAlmostEqual(out["short_stop"], 105.0, places=6)
        self.assertTrue(out["short_exit"] is False)
        self.assertTrue(out["long_exit"] is True)

    def test_disable_auto_tighten_stop_keeps_live_position_open_before_tp1(self) -> None:
        params = _params()
        params.auto_tighten_stop = False
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

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 101.0,
                "high": 101.0,
                "low": 99.5,
                "atr": 1.0,
                "long_stop": 102.0,
                "short_stop": 98.0,
                "long_exit": False,
                "short_exit": False,
            },
            params=params,
            decision=None,
            allow_reverse=False,
            managed_exit=True,
        )
        self.assertFalse(res["closed"])
        self.assertAlmostEqual(pos["hard_stop"], 95.0, places=6)

    def test_swapped_reverse_long_take_hits_one_r(self) -> None:
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
        pos = _new_sim_position(
            decision=long_decision,
            entry_ts=1,
            entry_i=1,
            risk_amt=10.0,
            exit_model="swapped_reverse",
            swap_stop_r_mult=1.5,
        )

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 102.0,
                "high": 105.1,
                "low": 99.0,
                "atr": 1.0,
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
        self.assertFalse(res["is_stop"])
        self.assertAlmostEqual(res["r_raw"], 1.0, places=6)

    def test_swapped_reverse_same_bar_stop_wins_conservatively(self) -> None:
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
        pos = _new_sim_position(
            decision=long_decision,
            entry_ts=1,
            entry_i=1,
            risk_amt=10.0,
            exit_model="swapped_reverse",
            swap_stop_r_mult=1.5,
        )

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 100.0,
                "high": 105.5,
                "low": 92.4,
                "atr": 1.0,
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
        self.assertAlmostEqual(res["r_raw"], -1.5, places=6)

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

    def test_tp1_partial_then_short_tp2_on_intrabar_low(self) -> None:
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
                "high": 100.0,
                "low": 91.8,
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

        hit_tp2 = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 88.0,
                "high": 90.0,
                "low": 87.4,
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
        self.assertTrue(hit_tp2["closed"])
        self.assertEqual(hit_tp2["outcome"], "TP2")
        self.assertFalse(hit_tp2["is_stop"])

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

    def test_split_tp_intrabar_then_close_below_be_arms_next_bar_stop(self) -> None:
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
        self.assertFalse(res["closed"])
        self.assertTrue(pos["tp1_done"])
        self.assertTrue(pos["be_armed"])
        self.assertAlmostEqual(pos["realized_r"], 0.75, places=6)
        self.assertAlmostEqual(pos["hard_stop"], 100.0, places=6)

    def test_managed_exit_without_split_stops_on_intrabar_touch(self) -> None:
        params = _params()
        params.auto_tighten_stop = False
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

        res = _simulate_live_position_step(
            pos=pos,
            sig={
                "close": 108.0,
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

    def test_cached_trade_trajectory_replays_managed_exit_path(self) -> None:
        params = _params()
        params.auto_tighten_stop = False
        long_decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        ltf_ts = [0, 1, 2, 3, 4]
        signal_table = [
            None,
            {
                "close": 100.0,
                "high": 100.5,
                "low": 99.5,
                "atr": 1.0,
                "long_exit": False,
                "short_exit": False,
            },
            {
                "close": 108.0,
                "high": 108.2,
                "low": 99.8,
                "atr": 1.0,
                "long_exit": False,
                "short_exit": False,
            },
            {
                "close": 99.6,
                "high": 100.2,
                "low": 99.4,
                "atr": 1.0,
                "long_exit": False,
                "short_exit": False,
            },
            None,
        ]
        decision_table = [None] * len(signal_table)
        trajectory = _build_interleaved_trade_trajectory(
            ltf_ts=ltf_ts,
            signal_table=signal_table,
            decision_table=decision_table,
            params=params,
            entry_i=1,
            decision=long_decision,
            managed_exit=True,
            use_signal_exit=True,
            exit_model="standard",
            split_tp_enabled=False,
        )

        live_pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)
        live_pos["exchange_split_tp_enabled"] = False
        replay_pos = _new_sim_position(decision=long_decision, entry_ts=1, entry_i=1, risk_amt=10.0)
        replay_pos["exchange_split_tp_enabled"] = False
        replay_pos["_trajectory_events"] = trajectory
        replay_pos["_trajectory_ptr"] = 0

        for idx in (2, 3):
            sig = signal_table[idx]
            direct = _simulate_live_position_step(
                pos=live_pos,
                sig=sig,
                params=params,
                decision=None,
                allow_reverse=False,
                managed_exit=True,
                use_signal_exit=True,
            )
            replay = _apply_cached_trade_trajectory_step(
                pos=replay_pos,
                current_i=idx,
                decision=None,
                allow_reverse=False,
            )
            self.assertAlmostEqual(replay_pos["qty_rem"], live_pos["qty_rem"], places=6)
            self.assertAlmostEqual(replay_pos["realized_r"], live_pos["realized_r"], places=6)
            self.assertEqual(replay_pos["tp1_done"], live_pos["tp1_done"])
            self.assertEqual(replay_pos["be_armed"], live_pos["be_armed"])
            self.assertAlmostEqual(replay_pos["hard_stop"], live_pos["hard_stop"], places=6)
            self.assertEqual(replay["closed"], direct["closed"])
            self.assertEqual(replay["outcome"], direct["outcome"])
            self.assertEqual(replay["is_stop"], direct["is_stop"])
            self.assertAlmostEqual(replay["r_raw"], direct["r_raw"], places=6)
            if direct["closed"]:
                break

        self.assertTrue(direct["closed"])
        self.assertEqual(replay_pos["_trajectory_ptr"], len(trajectory))


if __name__ == "__main__":
    unittest.main()
