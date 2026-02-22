from __future__ import annotations

import unittest

from run_interleaved_backtest_2y import _position_potential_loss_usdt, _sum_open_positions_potential_loss


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


if __name__ == "__main__":
    unittest.main()
