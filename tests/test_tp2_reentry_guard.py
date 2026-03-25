from __future__ import annotations

import unittest

from okx_trader.tp2_reentry_guard import (
    arm_tp2_reentry_bucket,
    get_tp2_reentry_gate,
    normalize_tp2_reentry_windows,
)


class Tp2ReentryGuardTests(unittest.TestCase):
    def test_normalize_clamps_partial_window_not_below_block(self) -> None:
        block, partial_until, max_level = normalize_tp2_reentry_windows(2.0, 1.0, 2)
        self.assertEqual(block, 2.0)
        self.assertEqual(partial_until, 2.0)
        self.assertEqual(max_level, 2)

    def test_block_then_partial_then_allow(self) -> None:
        bucket = {}
        arm_tp2_reentry_bucket(
            bucket,
            event_ts_ms=0,
            block_hours=1.0,
            partial_until_hours=4.0,
        )

        gate = get_tp2_reentry_gate(
            bucket,
            now_ts_ms=30 * 60 * 1000,
            planned_level=3,
            block_hours=1.0,
            partial_until_hours=4.0,
            partial_max_level=2,
        )
        self.assertEqual(gate["status"], "block")

        gate = get_tp2_reentry_gate(
            bucket,
            now_ts_ms=2 * 3600 * 1000,
            planned_level=3,
            block_hours=1.0,
            partial_until_hours=4.0,
            partial_max_level=2,
        )
        self.assertEqual(gate["status"], "partial_level_cap")
        self.assertEqual(gate["required_level"], 2)

        gate = get_tp2_reentry_gate(
            bucket,
            now_ts_ms=5 * 3600 * 1000,
            planned_level=3,
            block_hours=1.0,
            partial_until_hours=4.0,
            partial_max_level=2,
        )
        self.assertEqual(gate["status"], "allow")

    def test_l2_passes_during_partial_window(self) -> None:
        bucket = {}
        arm_tp2_reentry_bucket(
            bucket,
            event_ts_ms=0,
            block_hours=1.0,
            partial_until_hours=4.0,
        )
        gate = get_tp2_reentry_gate(
            bucket,
            now_ts_ms=2 * 3600 * 1000,
            planned_level=2,
            block_hours=1.0,
            partial_until_hours=4.0,
            partial_max_level=2,
        )
        self.assertEqual(gate["status"], "allow")


if __name__ == "__main__":
    unittest.main()
