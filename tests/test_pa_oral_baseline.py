from __future__ import annotations

import unittest

from okx_trader.decision_core import resolve_entry_decision
from okx_trader.models import Candle
from okx_trader.pa_oral_baseline import build_pa_oral_signal_table


def _candle(ts_h: int, o: float, h: float, l: float, c: float) -> Candle:
    return Candle(ts_ms=ts_h * 3_600_000, open=o, high=h, low=l, close=c, confirm=True, volume=0.0)


def _neutral_htf() -> list[Candle]:
    return [_candle(i * 4, 100.0, 101.0, 99.0, 100.0) for i in range(10)]


def _mirror_ltf(candles: list[Candle], pivot: float = 200.0) -> list[Candle]:
    out: list[Candle] = []
    for candle in candles:
        o = float(pivot - candle.open)
        h = float(pivot - candle.high)
        l = float(pivot - candle.low)
        c = float(pivot - candle.close)
        out.append(
            Candle(
                ts_ms=candle.ts_ms,
                open=o,
                high=max(o, h, l, c),
                low=min(o, h, l, c),
                close=c,
                confirm=True,
                volume=0.0,
            )
        )
    return out


def _range_current_bar_sweep_ltf() -> list[Candle]:
    vals = [
        (100.0, 101.5, 99.2, 100.8),
        (100.8, 102.2, 99.8, 101.9),
        (101.9, 102.8, 100.8, 102.1),
        (102.1, 102.4, 100.3, 101.0),
        (101.0, 101.2, 98.8, 99.4),
        (99.4, 100.0, 98.1, 98.9),
        (98.9, 99.8, 98.0, 99.2),
        (99.2, 100.8, 98.6, 100.2),
        (100.2, 102.0, 99.6, 101.4),
        (101.4, 102.5, 100.7, 101.8),
        (101.8, 102.1, 100.0, 100.6),
        (100.6, 100.9, 98.9, 99.6),
        (99.6, 100.2, 98.3, 99.0),
        (99.0, 99.7, 98.0, 98.8),
        (98.8, 100.0, 98.4, 99.5),
        (99.5, 101.0, 99.0, 100.4),
        (100.4, 102.1, 99.8, 101.5),
        (101.5, 102.4, 100.9, 101.9),
        (101.9, 102.0, 100.6, 101.0),
        (101.0, 101.1, 98.7, 99.0),
        (98.1, 98.2, 97.85, 98.0),
        (98.0, 98.45, 97.6, 98.4),
        (98.4, 98.9, 98.3, 98.75),
        (98.75, 100.2, 98.7, 99.8),
    ]
    return [_candle(i, *v) for i, v in enumerate(vals)]


def _range_previous_bar_sweep_ltf() -> list[Candle]:
    vals = [
        (100.0, 101.5, 99.2, 100.8),
        (100.8, 102.2, 99.8, 101.9),
        (101.9, 102.8, 100.8, 102.1),
        (102.1, 102.4, 100.3, 101.0),
        (101.0, 101.2, 98.8, 99.4),
        (99.4, 100.0, 98.1, 98.9),
        (98.9, 99.8, 98.0, 99.2),
        (99.2, 100.8, 98.6, 100.2),
        (100.2, 102.0, 99.6, 101.4),
        (101.4, 102.5, 100.7, 101.8),
        (101.8, 102.1, 100.0, 100.6),
        (100.6, 100.9, 98.9, 99.6),
        (99.6, 100.2, 98.3, 99.0),
        (99.0, 99.7, 98.0, 98.8),
        (98.8, 100.0, 98.4, 99.5),
        (99.5, 101.0, 99.0, 100.4),
        (100.4, 102.1, 99.8, 101.5),
        (101.5, 102.4, 100.9, 101.9),
        (101.9, 102.0, 100.6, 101.0),
        (101.0, 101.1, 98.7, 99.0),
        (98.1, 98.2, 97.6, 98.0),
        (98.0, 98.45, 97.9, 98.4),
        (98.4, 98.9, 98.3, 98.75),
        (98.75, 100.2, 98.7, 99.8),
    ]
    return [_candle(i, *v) for i, v in enumerate(vals)]


def _range_current_bar_sweep_delayed_breakout_ltf() -> list[Candle]:
    ltf = _range_current_bar_sweep_ltf()
    ltf[22] = _candle(22, 98.40, 98.50, 98.30, 98.45)
    ltf[23] = _candle(23, 98.45, 98.50, 98.35, 98.48)
    ltf.append(_candle(24, 98.20, 98.40, 98.05, 98.30))
    ltf.append(_candle(25, 98.30, 98.60, 98.20, 98.55))
    return ltf


class PaOralBaselineTests(unittest.TestCase):
    def test_range_current_bar_sweep_long_entry_uses_absolute_targets(self) -> None:
        table = build_pa_oral_signal_table(
            htf_candles=_neutral_htf(),
            ltf_candles=_range_current_bar_sweep_ltf(),
        )
        row = table[22]
        self.assertEqual(row["market_state"], "range")
        self.assertEqual(row["setup_type"], "setup_B")
        self.assertTrue(bool(row["long_entry"]))
        self.assertFalse(bool(row["short_entry"]))
        self.assertTrue(bool(row["entry_include_start_bar"]))
        self.assertAlmostEqual(float(row["entry_price_override"]), 98.54486875, places=6)
        self.assertAlmostEqual(float(row["tp1_price_override"]), 102.8, places=6)
        self.assertAlmostEqual(float(row["tp2_price_override"]), 102.8, places=6)

        decision = resolve_entry_decision(
            row,
            max_level=1,
            tp1_r=2.0,
            tp2_r=3.0,
        )
        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertAlmostEqual(decision.entry, 98.54486875, places=6)
        self.assertAlmostEqual(decision.tp1, 102.8, places=6)
        self.assertAlmostEqual(decision.tp2, 102.8, places=6)
        self.assertTrue(decision.include_start_bar)

    def test_range_previous_bar_sweep_long_entry_is_supported(self) -> None:
        table = build_pa_oral_signal_table(
            htf_candles=_neutral_htf(),
            ltf_candles=_range_previous_bar_sweep_ltf(),
        )
        row = table[22]
        self.assertEqual(row["market_state"], "range")
        self.assertEqual(row["setup_type"], "setup_B")
        self.assertTrue(bool(row["long_entry"]))
        self.assertFalse(bool(row["short_entry"]))
        self.assertAlmostEqual(float(row["entry_price_override"]), 98.5447125, places=6)
        self.assertAlmostEqual(float(row["tp1_price_override"]), 102.8, places=6)
        self.assertAlmostEqual(float(row["tp2_price_override"]), 102.8, places=6)
        self.assertAlmostEqual(float(row["protected_swing_price"]), 97.6, places=6)

    def test_range_long_signal_survives_longer_validity_window(self) -> None:
        table = build_pa_oral_signal_table(
            htf_candles=_neutral_htf(),
            ltf_candles=_range_current_bar_sweep_delayed_breakout_ltf(),
        )
        self.assertFalse(bool(table[22]["long_entry"]))
        self.assertFalse(bool(table[23]["long_entry"]))
        self.assertFalse(bool(table[24]["long_entry"]))
        row = table[25]
        self.assertEqual(row["market_state"], "range")
        self.assertEqual(row["setup_type"], "setup_B")
        self.assertTrue(bool(row["long_entry"]))
        self.assertAlmostEqual(float(row["entry_price_override"]), 98.54486875, places=6)

    def test_range_current_bar_sweep_short_entry_uses_absolute_targets(self) -> None:
        table = build_pa_oral_signal_table(
            htf_candles=_neutral_htf(),
            ltf_candles=_mirror_ltf(_range_current_bar_sweep_ltf()),
        )
        row = table[22]
        self.assertEqual(row["market_state"], "range")
        self.assertEqual(row["setup_type"], "setup_B")
        self.assertFalse(bool(row["long_entry"]))
        self.assertTrue(bool(row["short_entry"]))
        self.assertTrue(bool(row["entry_include_start_bar"]))
        self.assertAlmostEqual(float(row["entry_price_override"]), 101.45513125, places=6)
        self.assertAlmostEqual(float(row["tp1_price_override"]), 97.2, places=6)
        self.assertAlmostEqual(float(row["tp2_price_override"]), 97.2, places=6)

        decision = resolve_entry_decision(
            row,
            max_level=1,
            tp1_r=2.0,
            tp2_r=3.0,
        )
        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertAlmostEqual(decision.entry, 101.45513125, places=6)
        self.assertAlmostEqual(decision.tp1, 97.2, places=6)
        self.assertAlmostEqual(decision.tp2, 97.2, places=6)
        self.assertTrue(decision.include_start_bar)

    def test_range_short_requires_stronger_body_in_round_two(self) -> None:
        ltf = _mirror_ltf(_range_current_bar_sweep_ltf())
        signal_bar = ltf[21]
        ltf[21] = Candle(
            ts_ms=signal_bar.ts_ms,
            open=102.0,
            high=102.4,
            low=101.55,
            close=101.65,
            confirm=True,
            volume=0.0,
        )
        table = build_pa_oral_signal_table(
            htf_candles=_neutral_htf(),
            ltf_candles=ltf,
        )
        row = table[22]
        self.assertEqual(row["market_state"], "range")
        self.assertEqual(row["setup_type"], "")
        self.assertFalse(bool(row["long_entry"]))
        self.assertFalse(bool(row["short_entry"]))

    def test_prefix_replay_matches_full_table_for_previous_bar_sweep_case(self) -> None:
        htf = _neutral_htf()
        ltf = _range_previous_bar_sweep_ltf()
        full = build_pa_oral_signal_table(htf_candles=htf, ltf_candles=ltf)

        for end in (21, 22, 23):
            partial = build_pa_oral_signal_table(htf_candles=htf, ltf_candles=ltf[: end + 1])
            self.assertEqual(
                partial[-1],
                full[end],
                msg=f"prefix mismatch at bar {end}",
            )


if __name__ == "__main__":
    unittest.main()
