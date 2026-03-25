from __future__ import annotations

import unittest

from run_interleaved_backtest_2y import (
    _build_interleaved_runtime_slots,
    _new_sim_position,
)
from okx_trader.decision_core import EntryDecision


class InterleavedProfileSlotTests(unittest.TestCase):
    def test_default_mode_keeps_single_slot_per_instrument(self) -> None:
        slots = _build_interleaved_runtime_slots(
            inst_ids=["XAU-USDT-SWAP"],
            profile_ids_by_inst={"XAU-USDT-SWAP": ["XAUV3", "XAUV2", "XAUCLASSIC"]},
            profile_by_inst={"XAU-USDT-SWAP": "XAUV3"},
            independent_profile_positions=False,
        )
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["inst"], "XAU-USDT-SWAP")
        self.assertEqual(slots[0]["profile_id"], "XAUV3")
        self.assertEqual(slots[0]["profile_ids"], ["XAUV3", "XAUV2", "XAUCLASSIC"])
        self.assertEqual(slots[0]["position_key"], "XAU-USDT-SWAP")

    def test_independent_mode_expands_to_one_slot_per_profile(self) -> None:
        slots = _build_interleaved_runtime_slots(
            inst_ids=["XAU-USDT-SWAP"],
            profile_ids_by_inst={"XAU-USDT-SWAP": ["XAUV3", "XAUV2", "XAUCLASSIC"]},
            profile_by_inst={"XAU-USDT-SWAP": "XAUV3"},
            independent_profile_positions=True,
        )
        self.assertEqual(
            [slot["position_key"] for slot in slots],
            [
                "XAU-USDT-SWAP::XAUV3",
                "XAU-USDT-SWAP::XAUV2",
                "XAU-USDT-SWAP::XAUCLASSIC",
            ],
        )
        self.assertEqual([slot["profile_ids"] for slot in slots], [["XAUV3"], ["XAUV2"], ["XAUCLASSIC"]])

    def test_new_sim_position_can_carry_profile_metadata(self) -> None:
        decision = EntryDecision(
            side="LONG",
            level=2,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )
        pos = _new_sim_position(
            decision=decision,
            entry_ts=1,
            entry_i=2,
            risk_amt=10.0,
            profile_id="XAUV3",
            position_key="XAU-USDT-SWAP::XAUV3",
        )
        self.assertEqual(pos["profile_id"], "XAUV3")
        self.assertEqual(pos["position_key"], "XAU-USDT-SWAP::XAUV3")


if __name__ == "__main__":
    unittest.main()
