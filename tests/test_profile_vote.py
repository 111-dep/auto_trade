from __future__ import annotations

import unittest

from okx_trader.decision_core import EntryDecision
from okx_trader.profile_vote import merge_entry_votes


class ProfileVoteTests(unittest.TestCase):
    def test_weighted_vote_prefers_higher_score(self) -> None:
        base = {"close": 100.0, "long_stop": 95.0, "short_stop": 105.0}
        ids = ["A", "B"]
        decisions = {
            "A": EntryDecision(side="LONG", level=2, entry=100.0, stop=95.0, risk=5.0, tp1=107.5, tp2=112.5),
            "B": EntryDecision(side="SHORT", level=2, entry=100.0, stop=105.0, risk=5.0, tp1=92.5, tp2=87.5),
        }
        sig, meta = merge_entry_votes(
            base_signal=base,
            profile_ids=ids,
            signals_by_profile={},
            decisions_by_profile=decisions,
            mode="any",
            min_agree=1,
            enforce_max_level=3,
            profile_score_map={"A": 0.20, "B": 0.10},
            level_weight=0.0,
        )
        self.assertEqual(meta["winner_side"], "LONG")
        self.assertTrue(sig["long_entry"])
        self.assertFalse(sig["short_entry"])

    def test_tied_vote_same_level_stays_flat(self) -> None:
        base = {"close": 100.0, "long_stop": 95.0, "short_stop": 105.0}
        ids = ["A", "B"]
        decisions = {
            "A": EntryDecision(side="LONG", level=2, entry=100.0, stop=95.0, risk=5.0, tp1=107.5, tp2=112.5),
            "B": EntryDecision(side="SHORT", level=2, entry=100.0, stop=105.0, risk=5.0, tp1=92.5, tp2=87.5),
        }
        sig, meta = merge_entry_votes(
            base_signal=base,
            profile_ids=ids,
            signals_by_profile={},
            decisions_by_profile=decisions,
            mode="any",
            min_agree=1,
            enforce_max_level=3,
            profile_score_map={},
            level_weight=0.0,
        )
        self.assertEqual(meta["winner_side"], "NONE")
        self.assertFalse(sig["long_entry"])
        self.assertFalse(sig["short_entry"])

    def test_fallback_profile_only_used_when_primary_silent(self) -> None:
        base = {"close": 100.0, "long_stop": 95.0, "short_stop": 105.0}
        ids = ["DEFAULT", "RIGHTREV"]
        primary_short = EntryDecision(side="SHORT", level=2, entry=100.0, stop=105.0, risk=5.0, tp1=92.5, tp2=87.5)
        fallback_long = EntryDecision(side="LONG", level=2, entry=100.0, stop=95.0, risk=5.0, tp1=107.5, tp2=112.5)

        sig1, meta1 = merge_entry_votes(
            base_signal=base,
            profile_ids=ids,
            signals_by_profile={},
            decisions_by_profile={"DEFAULT": primary_short, "RIGHTREV": fallback_long},
            mode="any",
            min_agree=1,
            enforce_max_level=3,
            profile_score_map={},
            level_weight=0.0,
            fallback_profile_ids=["RIGHTREV"],
        )
        self.assertEqual(meta1["fallback_mode"], "suppressed")
        self.assertEqual(meta1["winner_side"], "SHORT")
        self.assertTrue(sig1["short_entry"])
        self.assertFalse(sig1["long_entry"])

        sig2, meta2 = merge_entry_votes(
            base_signal=base,
            profile_ids=ids,
            signals_by_profile={},
            decisions_by_profile={"DEFAULT": None, "RIGHTREV": fallback_long},
            mode="any",
            min_agree=1,
            enforce_max_level=3,
            profile_score_map={},
            level_weight=0.0,
            fallback_profile_ids=["RIGHTREV"],
        )
        self.assertEqual(meta2["fallback_mode"], "activated")
        self.assertEqual(meta2["winner_side"], "LONG")
        self.assertTrue(sig2["long_entry"])
        self.assertFalse(sig2["short_entry"])


if __name__ == "__main__":
    unittest.main()
