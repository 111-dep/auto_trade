from __future__ import annotations

import unittest

from okx_trader.decision_core import resolve_entry_decision
from okx_trader.pa_oral_baseline import PA_ORAL_BASELINE_V1
from okx_trader.strategy_variant import list_variant_resolvers, normalize_strategy_variant


class StrategyDispatchTests(unittest.TestCase):
    def test_only_oral_variant_is_registered(self) -> None:
        self.assertEqual(
            list_variant_resolvers(),
            {PA_ORAL_BASELINE_V1: "dedicated_pa_oral_engine"},
        )

    def test_oral_aliases_normalize_to_single_variant(self) -> None:
        for raw in (
            "",
            "pa_oral_baseline_v1",
            "pa_oral_baseline",
            "pa_oral",
            "oral_pa",
            "oral_price_action",
            "price_action_oral_v1",
        ):
            self.assertEqual(normalize_strategy_variant(raw), PA_ORAL_BASELINE_V1, msg=raw)

    def test_legacy_variant_names_are_rejected(self) -> None:
        for raw in ("classic", "ema20_reclaim_1h_pa_v2", "right_reversal_v1"):
            with self.assertRaisesRegex(ValueError, "Unsupported strategy variant"):
                normalize_strategy_variant(raw)

    def test_entry_decision_honors_absolute_entry_and_target_overrides(self) -> None:
        decision = resolve_entry_decision(
            {
                "close": 100.0,
                "long_level": 1,
                "short_level": 0,
                "long_stop": 95.0,
                "short_stop": 105.0,
                "entry_price_override": 101.25,
                "tp1_price_override": 104.0,
                "tp2_price_override": 108.5,
                "entry_idx": 17,
                "entry_include_start_bar": True,
            },
            max_level=1,
            tp1_r=2.0,
            tp2_r=3.0,
        )
        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertEqual(decision.side, "LONG")
        self.assertAlmostEqual(decision.entry, 101.25, places=6)
        self.assertAlmostEqual(decision.stop, 95.0, places=6)
        self.assertAlmostEqual(decision.tp1, 104.0, places=6)
        self.assertAlmostEqual(decision.tp2, 108.5, places=6)
        self.assertEqual(decision.entry_idx, 17)
        self.assertTrue(decision.include_start_bar)


if __name__ == "__main__":
    unittest.main()
