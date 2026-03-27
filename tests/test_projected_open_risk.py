from __future__ import annotations

import unittest

from okx_trader.decision_core import EntryDecision
from run_interleaved_backtest_2y import (
    _clip_entry_decision_to_window,
    _count_open_l3_side_positions,
    _open_risk_cap_allows,
    _position_potential_loss_usdt,
    _resolve_interleaved_window_bounds,
    _sum_open_positions_potential_loss,
)


class ProjectedOpenRiskTests(unittest.TestCase):
    def test_long_initial_stop_full_size(self) -> None:
        pos = {
            "side": "LONG",
            "entry": 100.0,
            "risk": 5.0,
            "stop": 95.0,
            "hard_stop": 95.0,
            "qty_rem": 1.0,
            "risk_amt": 20.0,
        }
        self.assertAlmostEqual(_position_potential_loss_usdt(pos), 20.0, places=6)

    def test_long_tp1_after_be_has_zero_potential_loss(self) -> None:
        pos = {
            "side": "LONG",
            "entry": 100.0,
            "risk": 5.0,
            "stop": 95.0,
            "hard_stop": 100.2,
            "qty_rem": 0.5,
            "risk_amt": 20.0,
        }
        self.assertAlmostEqual(_position_potential_loss_usdt(pos), 0.0, places=6)

    def test_partial_position_scales_loss(self) -> None:
        pos = {
            "side": "SHORT",
            "entry": 100.0,
            "risk": 4.0,
            "stop": 104.0,
            "hard_stop": 104.0,
            "qty_rem": 0.25,
            "risk_amt": 40.0,
        }
        # Remaining 25% at full 1R risk => 40 * 0.25 = 10
        self.assertAlmostEqual(_position_potential_loss_usdt(pos), 10.0, places=6)

    def test_sum_open_positions(self) -> None:
        open_positions = {
            "BTC-USDT-SWAP": {
                "side": "LONG",
                "entry": 100.0,
                "risk": 5.0,
                "hard_stop": 95.0,
                "qty_rem": 1.0,
                "risk_amt": 10.0,
            },
            "ETH-USDT-SWAP": {
                "side": "LONG",
                "entry": 200.0,
                "risk": 10.0,
                "hard_stop": 200.5,
                "qty_rem": 0.5,
                "risk_amt": 20.0,
            },
        }
        # BTC contributes 10, ETH contributes 0 (hard_stop above entry on long).
        self.assertAlmostEqual(_sum_open_positions_potential_loss(open_positions), 10.0, places=6)

    def test_open_risk_cap_allows_exact_boundary(self) -> None:
        open_positions = {
            "BTC-USDT-SWAP": {
                "side": "LONG",
                "entry": 100.0,
                "risk": 5.0,
                "hard_stop": 95.0,
                "qty_rem": 1.0,
                "risk_amt": 10.0,
            }
        }
        self.assertTrue(
            _open_risk_cap_allows(
                open_positions,
                equity=1000.0,
                candidate_loss_usdt=50.0,
                max_open_risk_frac=0.06,
            )
        )
        self.assertFalse(
            _open_risk_cap_allows(
                open_positions,
                equity=1000.0,
                candidate_loss_usdt=50.0001,
                max_open_risk_frac=0.06,
            )
        )

    def test_resolve_interleaved_window_bounds_with_explicit_range(self) -> None:
        ltf_ts = [0, 900_000, 1_800_000, 2_700_000, 3_600_000, 4_500_000]
        start_idx, timeline_end_exclusive, final_close_i = _resolve_interleaved_window_bounds(
            ltf_ts,
            bars=6,
            start_ts_ms=900_000,
            end_ts_ms=3_600_000,
        )
        self.assertEqual(start_idx, 1)
        self.assertEqual(timeline_end_exclusive, 4)
        self.assertEqual(final_close_i, 4)

    def test_clip_entry_decision_to_window(self) -> None:
        decision = EntryDecision(
            side="LONG",
            level=1,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=110.0,
            tp2=120.0,
            entry_idx=5,
        )
        self.assertIsNone(_clip_entry_decision_to_window(decision, max_entry_i=4))
        self.assertIs(_clip_entry_decision_to_window(decision, max_entry_i=5), decision)

    def test_count_open_l3_same_side_positions(self) -> None:
        open_positions = {
            "BTC-USDT-SWAP": {"side": "LONG", "level": 3, "qty_rem": 1.0},
            "ETH-USDT-SWAP": {"side": "LONG", "level": 3, "qty_rem": 0.4},
            "SOL-USDT-SWAP": {"side": "LONG", "level": 2, "qty_rem": 1.0},
            "DOGE-USDT-SWAP": {"side": "SHORT", "level": 3, "qty_rem": 1.0},
            "SUI-USDT-SWAP": {"side": "LONG", "level": 3, "qty_rem": 0.0},
        }
        self.assertEqual(_count_open_l3_side_positions(open_positions, "LONG"), 2)
        self.assertEqual(_count_open_l3_side_positions(open_positions, "SHORT"), 1)
        self.assertEqual(_count_open_l3_side_positions(open_positions, "flat"), 0)


if __name__ == "__main__":
    unittest.main()
