from __future__ import annotations

import os
import unittest
from dataclasses import replace
from pathlib import Path

from okx_trader.config import read_config
from okx_trader.strategy_contract import VariantSignalInputs
from okx_trader.strategy_variant import (
    list_variant_resolvers,
    resolve_variant_signal_state,
    resolve_variant_signal_state_from_inputs,
)


class StrategyDispatchTests(unittest.TestCase):
    @staticmethod
    def _load_env_file() -> None:
        env_path = Path(__file__).resolve().parents[1] / "okx_auto_trader.env"
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            os.environ[k.strip()] = v.strip()

    def test_kwargs_and_input_object_resolve_same(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params
        variants = [
            "classic",
            "btceth_smc_a2",
            "elder_tss_v1",
            "r_breaker_v1",
            "range_reversion_v1",
            "range_reversion_v2",
            "range_reversion_v3",
            "right_reversal_v1",
            "xau_sibi_v1",
            "ema_pullback_4h_v1",
            "mtf_ema_trend_v1",
            "mtf_ema_trend_v2",
            "mtf_ema_trend_v3",
            "mtf_ema_trend_v4",
            "daily_sr_zones_v1",
            "daily_sr_zones_v2",
            "mtf_resonance_v2",
            "mtf_resonance_v3",
            "mtf_resonance_v4",
            "daily_ema_5813_v1",
            "turtle_donchian_v1",
            "rsi2_reversion_v1",
            "bollinger_trend_reversion_v1",
        ]
        payload = dict(
            bias="long",
            close=100.0,
            ema_value=99.0,
            rsi_value=55.0,
            macd_hist_value=0.2,
            atr_value=1.5,
            hhv=101.0,
            llv=98.0,
            exl=97.5,
            exh=102.0,
            pb_low=98.8,
            pb_high=100.8,
            h_close=100.5,
            h_ema_fast=99.8,
            h_ema_slow=98.7,
            width=0.02,
            width_avg=0.015,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            prev_hhv=100.5,
            prev_llv=98.5,
            prev_exl=98.8,
            prev_exh=101.2,
            current_high=100.8,
            current_low=99.2,
            prev_high=100.6,
            prev_low=99.1,
            prev2_high=100.4,
            prev2_low=98.9,
            current_open=99.7,
            prev_open=99.3,
            prev_close=99.6,
            upper_band=101.2,
            lower_band=98.8,
            mid_band=100.0,
            prev_macd_hist=0.1,
            volume=1200.0,
            volume_avg=900.0,
            prev_day_high=101.5,
            prev_day_low=97.5,
            prev_day_close=100.2,
            day_high_so_far=100.9,
            day_low_so_far=99.0,
        )

        for variant in variants:
            p = replace(base, strategy_variant=variant)
            kwargs = dict(payload)
            kwargs["p"] = p
            out_old = resolve_variant_signal_state(**kwargs)
            out_new = resolve_variant_signal_state_from_inputs(VariantSignalInputs(**kwargs))
            self.assertEqual(out_old, out_new, msg=f"variant mismatch: {variant}")

    def test_plugin_loader_entrypoint_is_safe(self) -> None:
        # Should not raise and should expose plugin-registered resolver entries.
        m = list_variant_resolvers()
        self.assertIsInstance(m, dict)
        self.assertIn("elder_tss_v1#inputs", m)
        self.assertIn("range_reversion_v1#inputs", m)
        self.assertIn("range_reversion_v2#inputs", m)
        self.assertIn("range_reversion_v3#inputs", m)
        self.assertIn("right_reversal_v1#inputs", m)
        self.assertIn("xau_sibi_v1#inputs", m)
        self.assertIn("ema_pullback_4h_v1#inputs", m)
        self.assertIn("mtf_ema_trend_v1#inputs", m)
        self.assertIn("mtf_ema_trend_v2#inputs", m)
        self.assertIn("mtf_ema_trend_v3#inputs", m)
        self.assertIn("mtf_ema_trend_v4#inputs", m)
        self.assertIn("daily_sr_zones_v1#inputs", m)
        self.assertIn("daily_sr_zones_v2#inputs", m)
        self.assertIn("mtf_resonance_v2#inputs", m)
        self.assertIn("mtf_resonance_v3#inputs", m)
        self.assertIn("mtf_resonance_v4#inputs", m)
        self.assertIn("daily_ema_5813_v1#inputs", m)
        self.assertIn("turtle_donchian_v1#inputs", m)
        self.assertIn("rsi2_reversion_v1#inputs", m)
        self.assertIn("bollinger_trend_reversion_v1#inputs", m)

    def test_turtle_donchian_v1_triggers_and_exits(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="turtle_donchian_v1"),
            bias="neutral",
            close=105.0,
            ema_value=101.0,
            rsi_value=55.0,
            macd_hist_value=0.1,
            atr_value=2.0,
            hhv=104.0,
            llv=96.0,
            exl=99.0,
            exh=106.0,
            pb_low=100.0,
            pb_high=104.0,
            h_close=105.0,
            h_ema_fast=103.0,
            h_ema_slow=100.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=False,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            prev_hhv=104.0,
            prev_llv=96.0,
            prev_exl=99.0,
            prev_exh=106.0,
            current_high=105.5,
            current_low=103.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertFalse(long_out["short_entry"])
        self.assertFalse(long_out["long_exit"])
        self.assertAlmostEqual(long_out["long_stop"], 101.0, places=6)

        exit_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="turtle_donchian_v1"),
            bias="neutral",
            close=98.0,
            ema_value=101.0,
            rsi_value=45.0,
            macd_hist_value=-0.1,
            atr_value=2.0,
            hhv=104.0,
            llv=96.0,
            exl=99.0,
            exh=106.0,
            pb_low=97.0,
            pb_high=100.0,
            h_close=98.0,
            h_ema_fast=100.0,
            h_ema_slow=101.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=False,
            not_chasing_long=False,
            not_chasing_short=True,
            prev_hhv=104.0,
            prev_llv=96.0,
            prev_exl=99.0,
            prev_exh=106.0,
            current_high=99.0,
            current_low=97.5,
        )
        exit_out = resolve_variant_signal_state_from_inputs(exit_inputs)
        self.assertTrue(exit_out["long_exit"])
        self.assertFalse(exit_out["short_exit"])

    def test_ema_pullback_4h_v1_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="ema_pullback_4h_v1"),
            bias="neutral",
            close=102.0,
            ema_value=101.0,
            rsi_value=52.0,
            macd_hist_value=0.1,
            atr_value=1.0,
            hhv=103.0,
            llv=97.0,
            exl=98.0,
            exh=103.0,
            pb_low=99.2,
            pb_high=101.2,
            h_close=102.0,
            h_ema_fast=101.0,
            h_ema_slow=99.8,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=102.5,
            current_low=100.1,
            current_open=100.8,
            prev_close=100.4,
            prev_h_ema_fast=100.7,
            prev_h_ema_slow=99.6,
            recent_rsi_min=46.0,
            recent_rsi_max=55.0,
            prev_ema_value=100.6,
            ema_slow_value=99.8,
            prev_ema_slow_value=99.5,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertEqual(long_out["long_level"], 1)
        self.assertFalse(long_out["short_entry"])
        self.assertAlmostEqual(long_out["long_stop"], 98.2, places=6)

        blocked_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="ema_pullback_4h_v1"),
            bias="neutral",
            close=102.0,
            ema_value=101.0,
            rsi_value=52.0,
            macd_hist_value=0.1,
            atr_value=1.0,
            hhv=103.0,
            llv=97.0,
            exl=98.0,
            exh=103.0,
            pb_low=99.2,
            pb_high=101.2,
            h_close=99.5,
            h_ema_fast=100.0,
            h_ema_slow=100.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=102.5,
            current_low=100.1,
            current_open=100.8,
            prev_close=100.4,
            prev_h_ema_fast=100.2,
            prev_h_ema_slow=100.2,
            recent_rsi_min=46.0,
            recent_rsi_max=55.0,
            prev_ema_value=100.6,
            ema_slow_value=99.8,
            prev_ema_slow_value=99.5,
        )
        blocked_out = resolve_variant_signal_state_from_inputs(blocked_inputs)
        self.assertFalse(blocked_out["long_entry"])
        self.assertFalse(blocked_out["short_entry"])

    def test_mtf_resonance_v2_long_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params
        inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_resonance_v2"),
            bias="neutral",
            close=101.5,
            ema_value=100.7,
            rsi_value=55.0,
            macd_hist_value=0.08,
            atr_value=0.6,
            hhv=102.0,
            llv=97.0,
            exl=99.0,
            exh=102.2,
            pb_low=99.4,
            pb_high=101.4,
            h_close=101.5,
            h_ema_fast=100.0,
            h_ema_slow=100.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.8,
            current_low=100.5,
            prev_high=101.1,
            prev_low=100.2,
            current_open=100.8,
            prev_close=100.9,
            prev_macd_hist=-0.02,
            prev_h_ema_fast=99.7,
            prev_h_ema_slow=100.0,
            loc_close=100.9,
            loc_ema_fast=100.4,
            loc_ema_slow=99.9,
            prev_loc_ema_fast=100.1,
            prev_loc_ema_slow=99.6,
            loc_rsi_value=55.0,
            loc_atr_value=0.8,
            loc_current_high=101.2,
            loc_current_low=100.0,
            hour_open=100.2,
            hour_high=101.0,
            hour_low=100.1,
            hour_close=100.8,
            hour_prev_close=100.5,
            hour_rsi_value=45.0,
        )
        out = resolve_variant_signal_state_from_inputs(inputs)
        self.assertTrue(out["long_entry"])
        self.assertEqual(out["long_level"], 1)
        self.assertFalse(out["short_entry"])

    def test_mtf_ema_trend_v3_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v3"),
            bias="neutral",
            close=101.15,
            ema_value=100.95,
            rsi_value=53.0,
            macd_hist_value=0.06,
            atr_value=0.7,
            hhv=102.2,
            llv=97.0,
            exl=99.0,
            exh=102.3,
            pb_low=100.05,
            pb_high=101.50,
            h_close=101.8,
            h_ema_fast=100.8,
            h_ema_slow=100.1,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.3,
            current_low=100.9,
            prev_high=101.2,
            prev_low=100.4,
            current_open=100.85,
            prev_close=101.0,
            prev_macd_hist=0.02,
            prev_h_ema_fast=100.5,
            prev_h_ema_slow=99.9,
            loc_close=101.15,
            loc_ema_fast=100.85,
            loc_ema_slow=100.45,
            prev_loc_ema_fast=100.6,
            prev_loc_ema_slow=100.3,
            loc_rsi_value=54.0,
            loc_atr_value=0.9,
            loc_current_high=101.3,
            loc_current_low=100.2,
            hour_open=100.6,
            hour_high=101.2,
            hour_low=100.5,
            hour_close=100.95,
            hour_prev_close=100.7,
            hour_rsi_value=47.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertGreaterEqual(long_out["long_level"], 1)
        self.assertFalse(long_out["short_entry"])

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v3"),
            bias="neutral",
            close=98.58,
            ema_value=98.70,
            rsi_value=44.0,
            macd_hist_value=-0.06,
            atr_value=0.7,
            hhv=103.0,
            llv=97.4,
            exl=97.8,
            exh=99.9,
            pb_low=98.2,
            pb_high=99.35,
            h_close=98.0,
            h_ema_fast=99.0,
            h_ema_slow=99.8,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=99.15,
            current_low=98.50,
            prev_high=98.9,
            prev_low=98.4,
            current_open=98.95,
            prev_close=98.8,
            prev_macd_hist=-0.02,
            prev_h_ema_fast=99.4,
            prev_h_ema_slow=100.0,
            loc_close=98.35,
            loc_ema_fast=98.8,
            loc_ema_slow=99.1,
            prev_loc_ema_fast=99.0,
            prev_loc_ema_slow=99.25,
            loc_rsi_value=45.0,
            loc_atr_value=0.85,
            loc_current_high=99.2,
            loc_current_low=98.1,
            hour_open=98.95,
            hour_high=99.05,
            hour_low=98.7,
            hour_close=98.75,
            hour_prev_close=98.9,
            hour_rsi_value=57.0,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertGreaterEqual(short_out["short_level"], 1)
        self.assertFalse(short_out["long_entry"])

    def test_mtf_ema_trend_v4_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v4"),
            bias="neutral",
            close=101.26,
            ema_value=101.00,
            rsi_value=56.0,
            macd_hist_value=0.08,
            atr_value=0.7,
            hhv=102.4,
            llv=97.0,
            exl=99.0,
            exh=102.4,
            pb_low=100.20,
            pb_high=101.45,
            h_close=101.9,
            h_ema_fast=100.9,
            h_ema_slow=100.1,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.35,
            current_low=100.92,
            prev_high=101.20,
            prev_low=100.45,
            current_open=100.88,
            prev_close=101.02,
            prev_macd_hist=0.03,
            prev_h_ema_fast=100.5,
            prev_h_ema_slow=99.9,
            loc_close=101.16,
            loc_ema_fast=100.95,
            loc_ema_slow=100.48,
            prev_loc_ema_fast=100.66,
            prev_loc_ema_slow=100.30,
            loc_rsi_value=55.0,
            loc_atr_value=0.9,
            loc_current_high=101.35,
            loc_current_low=100.18,
            hour_open=100.70,
            hour_high=101.18,
            hour_low=100.50,
            hour_close=100.98,
            hour_prev_close=100.72,
            hour_rsi_value=50.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertEqual(long_out["long_level"], 3)
        self.assertFalse(long_out["short_entry"])

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v4"),
            bias="neutral",
            close=98.56,
            ema_value=98.70,
            rsi_value=43.0,
            macd_hist_value=-0.07,
            atr_value=0.7,
            hhv=103.0,
            llv=97.3,
            exl=97.7,
            exh=99.9,
            pb_low=98.15,
            pb_high=99.34,
            h_close=97.95,
            h_ema_fast=98.95,
            h_ema_slow=99.85,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=99.10,
            current_low=98.45,
            prev_high=98.88,
            prev_low=98.60,
            current_open=98.94,
            prev_close=98.82,
            prev_macd_hist=-0.03,
            prev_h_ema_fast=99.35,
            prev_h_ema_slow=99.98,
            loc_close=98.34,
            loc_ema_fast=98.78,
            loc_ema_slow=99.10,
            prev_loc_ema_fast=98.98,
            prev_loc_ema_slow=99.24,
            loc_rsi_value=45.0,
            loc_atr_value=0.85,
            loc_current_high=99.18,
            loc_current_low=98.08,
            hour_open=98.96,
            hour_high=99.02,
            hour_low=98.70,
            hour_close=98.74,
            hour_prev_close=98.90,
            hour_rsi_value=57.0,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertEqual(short_out["short_level"], 3)
        self.assertFalse(short_out["long_entry"])

    def test_range_reversion_v2_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="range_reversion_v2"),
            bias="neutral",
            close=99.55,
            ema_value=99.35,
            rsi_value=38.0,
            macd_hist_value=-0.03,
            atr_value=0.45,
            hhv=101.2,
            llv=98.9,
            exl=98.9,
            exh=101.1,
            pb_low=99.0,
            pb_high=100.7,
            h_close=100.0,
            h_ema_fast=100.2,
            h_ema_slow=100.0,
            width=0.010,
            width_avg=0.009,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=99.7,
            current_low=99.0,
            prev_high=100.0,
            prev_low=99.1,
            current_open=99.38,
            prev_close=99.2,
            upper_band=100.5,
            lower_band=99.1,
            mid_band=99.8,
            prev_macd_hist=-0.05,
            loc_close=99.85,
            loc_ema_fast=100.0,
            loc_ema_slow=99.7,
            prev_loc_ema_fast=100.02,
            prev_loc_ema_slow=99.68,
            loc_rsi_value=46.0,
            loc_atr_value=0.8,
            hour_open=99.1,
            hour_high=99.45,
            hour_low=99.08,
            hour_close=99.3,
            hour_prev_close=99.18,
            hour_rsi_value=42.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertFalse(long_out["short_entry"])
        self.assertEqual(long_out["long_level"], 1)

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="range_reversion_v2"),
            bias="neutral",
            close=100.45,
            ema_value=100.65,
            rsi_value=62.0,
            macd_hist_value=0.03,
            atr_value=0.45,
            hhv=101.3,
            llv=98.9,
            exl=99.0,
            exh=101.2,
            pb_low=99.2,
            pb_high=100.9,
            h_close=100.0,
            h_ema_fast=99.8,
            h_ema_slow=100.0,
            width=0.010,
            width_avg=0.009,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=100.92,
            current_low=100.3,
            prev_high=100.88,
            prev_low=100.1,
            current_open=100.58,
            prev_close=100.7,
            upper_band=100.9,
            lower_band=99.5,
            mid_band=100.2,
            prev_macd_hist=0.05,
            loc_close=100.12,
            loc_ema_fast=100.2,
            loc_ema_slow=99.9,
            prev_loc_ema_fast=100.22,
            prev_loc_ema_slow=99.88,
            loc_rsi_value=54.0,
            loc_atr_value=0.8,
            hour_open=100.82,
            hour_high=100.91,
            hour_low=100.58,
            hour_close=100.7,
            hour_prev_close=100.78,
            hour_rsi_value=58.0,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertFalse(short_out["long_entry"])
        self.assertEqual(short_out["short_level"], 1)

    def test_range_reversion_v3_long_trigger_and_short_blocked(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="range_reversion_v3"),
            bias="neutral",
            close=99.62,
            ema_value=99.45,
            rsi_value=41.0,
            macd_hist_value=-0.01,
            atr_value=0.42,
            hhv=101.0,
            llv=99.0,
            exl=99.0,
            exh=100.9,
            pb_low=99.05,
            pb_high=100.55,
            h_close=100.1,
            h_ema_fast=100.15,
            h_ema_slow=99.95,
            width=0.010,
            width_avg=0.008,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=99.74,
            current_low=99.02,
            prev_high=99.90,
            prev_low=99.10,
            current_open=99.34,
            prev_close=99.28,
            upper_band=100.35,
            lower_band=99.10,
            mid_band=99.72,
            prev_macd_hist=-0.03,
            recent_rsi_min=35.0,
            loc_close=99.92,
            loc_ema_fast=100.02,
            loc_ema_slow=99.82,
            prev_loc_ema_fast=100.00,
            prev_loc_ema_slow=99.80,
            loc_rsi_value=45.0,
            loc_atr_value=0.75,
            loc_current_high=100.0,
            loc_current_low=99.00,
            hour_open=99.16,
            hour_high=99.60,
            hour_low=99.05,
            hour_close=99.38,
            hour_prev_close=99.22,
            hour_rsi_value=40.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertFalse(long_out["short_entry"])
        self.assertGreaterEqual(long_out["long_level"], 1)

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="range_reversion_v3"),
            bias="neutral",
            close=100.40,
            ema_value=100.55,
            rsi_value=60.0,
            macd_hist_value=0.01,
            atr_value=0.42,
            hhv=101.1,
            llv=99.0,
            exl=99.1,
            exh=100.95,
            pb_low=99.35,
            pb_high=100.86,
            h_close=99.8,
            h_ema_fast=99.75,
            h_ema_slow=99.95,
            width=0.010,
            width_avg=0.008,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=100.90,
            current_low=100.32,
            prev_high=100.82,
            prev_low=100.15,
            current_open=100.62,
            prev_close=100.70,
            upper_band=100.86,
            lower_band=99.60,
            mid_band=100.22,
            prev_macd_hist=0.03,
            recent_rsi_max=65.0,
            loc_close=100.05,
            loc_ema_fast=100.00,
            loc_ema_slow=100.20,
            prev_loc_ema_fast=100.02,
            prev_loc_ema_slow=100.22,
            loc_rsi_value=55.0,
            loc_atr_value=0.75,
            loc_current_high=100.95,
            loc_current_low=100.10,
            hour_open=100.82,
            hour_high=100.88,
            hour_low=100.44,
            hour_close=100.64,
            hour_prev_close=100.74,
            hour_rsi_value=60.0,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertFalse(short_out["short_entry"])
        self.assertFalse(short_out["long_entry"])
        self.assertEqual(short_out["short_level"], 0)

    def test_mtf_resonance_v3_long_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params
        inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_resonance_v3"),
            bias="neutral",
            close=101.45,
            ema_value=101.2,
            rsi_value=54.0,
            macd_hist_value=0.08,
            atr_value=0.6,
            hhv=102.0,
            llv=97.0,
            exl=99.0,
            exh=102.2,
            pb_low=99.4,
            pb_high=101.4,
            h_close=101.5,
            h_ema_fast=100.0,
            h_ema_slow=100.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.55,
            current_low=100.9,
            prev_high=101.1,
            prev_low=100.2,
            current_open=101.0,
            prev_close=101.0,
            prev_macd_hist=-0.02,
            prev_h_ema_fast=99.7,
            prev_h_ema_slow=100.0,
            loc_close=100.95,
            loc_ema_fast=100.4,
            loc_ema_slow=99.9,
            prev_loc_ema_fast=100.1,
            prev_loc_ema_slow=99.6,
            loc_rsi_value=55.0,
            loc_atr_value=0.8,
            loc_current_high=101.2,
            loc_current_low=100.0,
            hour_open=100.25,
            hour_high=101.0,
            hour_low=100.15,
            hour_close=100.9,
            hour_prev_close=100.5,
            hour_rsi_value=46.0,
        )
        out = resolve_variant_signal_state_from_inputs(inputs)
        self.assertTrue(out["long_entry"])
        self.assertEqual(out["long_level"], 1)
        self.assertFalse(out["short_entry"])

    def test_mtf_resonance_v4_long_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params
        inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_resonance_v4"),
            bias="neutral",
            close=101.35,
            ema_value=101.0,
            rsi_value=52.0,
            macd_hist_value=0.05,
            atr_value=0.6,
            hhv=102.0,
            llv=97.0,
            exl=99.0,
            exh=102.2,
            pb_low=99.5,
            pb_high=101.4,
            h_close=101.5,
            h_ema_fast=100.0,
            h_ema_slow=100.0,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.45,
            current_low=100.85,
            prev_high=101.1,
            prev_low=100.2,
            current_open=100.95,
            prev_close=101.0,
            prev_macd_hist=-0.01,
            prev_h_ema_fast=99.7,
            prev_h_ema_slow=100.0,
            loc_close=100.65,
            loc_ema_fast=100.4,
            loc_ema_slow=99.95,
            prev_loc_ema_fast=100.1,
            prev_loc_ema_slow=99.7,
            loc_rsi_value=52.0,
            loc_atr_value=0.8,
            loc_current_high=101.0,
            loc_current_low=100.0,
            hour_open=100.2,
            hour_high=100.95,
            hour_low=100.18,
            hour_close=100.55,
            hour_prev_close=100.5,
            hour_rsi_value=49.0,
        )
        out = resolve_variant_signal_state_from_inputs(inputs)
        self.assertTrue(out["long_entry"])
        self.assertEqual(out["long_level"], 1)
        self.assertFalse(out["short_entry"])

    def test_daily_ema_5813_v1_long_entry_and_exit(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params
        inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="daily_ema_5813_v1"),
            bias="neutral",
            close=104.0,
            ema_value=101.0,
            rsi_value=55.0,
            macd_hist_value=0.1,
            atr_value=1.5,
            hhv=105.0,
            llv=95.0,
            exl=99.0,
            exh=105.0,
            pb_low=100.0,
            pb_high=104.5,
            h_close=104.0,
            h_ema_fast=102.5,
            h_ema_slow=101.8,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=104.8,
            current_low=103.2,
            prev_high=103.5,
            prev_low=100.8,
            current_open=103.4,
            prev_close=101.4,
            prev_h_ema_fast=101.5,
            prev_h_ema_slow=101.6,
            prev_ema_value=101.2,
        )
        out = resolve_variant_signal_state_from_inputs(inputs)
        self.assertTrue(out["long_entry"])
        self.assertFalse(out["short_entry"])
        self.assertFalse(out["long_exit"])

        exit_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="daily_ema_5813_v1"),
            bias="neutral",
            close=99.5,
            ema_value=100.8,
            rsi_value=45.0,
            macd_hist_value=-0.1,
            atr_value=1.5,
            hhv=105.0,
            llv=95.0,
            exl=99.0,
            exh=105.0,
            pb_low=99.0,
            pb_high=103.0,
            h_close=99.5,
            h_ema_fast=100.6,
            h_ema_slow=100.9,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=100.5,
            current_low=98.8,
            prev_high=103.0,
            prev_low=100.5,
            current_open=100.2,
            prev_close=102.8,
            prev_h_ema_fast=101.2,
            prev_h_ema_slow=101.0,
            prev_ema_value=100.7,
        )
        exit_out = resolve_variant_signal_state_from_inputs(exit_inputs)
        self.assertTrue(exit_out["long_exit"])

    def test_mtf_ema_trend_v1_long_and_short_paths(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v1"),
            bias="long",
            close=101.3,
            ema_value=101.2,
            rsi_value=56.0,
            macd_hist_value=0.08,
            atr_value=0.9,
            hhv=102.5,
            llv=97.0,
            exl=99.0,
            exh=103.0,
            pb_low=100.2,
            pb_high=101.9,
            h_close=102.0,
            h_ema_fast=100.5,
            h_ema_slow=99.6,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.5,
            current_low=101.0,
            prev_high=101.4,
            prev_low=100.6,
            current_open=101.0,
            prev_close=100.9,
            prev_h_ema_fast=100.1,
            prev_h_ema_slow=99.4,
            loc_close=101.25,
            loc_ema_fast=101.1,
            loc_ema_slow=100.9,
            prev_loc_ema_fast=100.7,
            prev_loc_ema_slow=100.8,
            loc_rsi_value=54.0,
            loc_atr_value=0.6,
            loc_current_high=101.9,
            loc_current_low=100.9,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertFalse(long_out["short_entry"])
        self.assertEqual(long_out["long_level"], 1)
        self.assertFalse(long_out["long_exit"])

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v1"),
            bias="short",
            close=97.6,
            ema_value=97.8,
            rsi_value=42.0,
            macd_hist_value=-0.09,
            atr_value=0.9,
            hhv=103.0,
            llv=96.5,
            exl=96.8,
            exh=100.0,
            pb_low=97.0,
            pb_high=99.2,
            h_close=97.0,
            h_ema_fast=98.5,
            h_ema_slow=99.4,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=98.0,
            current_low=97.4,
            prev_high=98.5,
            prev_low=97.4,
            current_open=97.9,
            prev_close=98.1,
            prev_h_ema_fast=98.9,
            prev_h_ema_slow=99.5,
            loc_close=97.6,
            loc_ema_fast=97.8,
            loc_ema_slow=98.0,
            prev_loc_ema_fast=98.1,
            prev_loc_ema_slow=98.0,
            loc_rsi_value=43.0,
            loc_atr_value=0.55,
            loc_current_high=98.1,
            loc_current_low=97.0,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertFalse(short_out["long_entry"])
        self.assertEqual(short_out["short_level"], 1)

        long_exit_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v1"),
            bias="neutral",
            close=99.4,
            ema_value=100.1,
            rsi_value=45.0,
            macd_hist_value=-0.05,
            atr_value=0.9,
            hhv=103.0,
            llv=96.5,
            exl=98.8,
            exh=101.5,
            pb_low=99.0,
            pb_high=100.8,
            h_close=99.8,
            h_ema_fast=100.2,
            h_ema_slow=99.9,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=False,
            pullback_long=False,
            pullback_short=False,
            not_chasing_long=False,
            not_chasing_short=False,
            current_high=100.0,
            current_low=99.2,
            prev_high=100.8,
            prev_low=99.6,
            current_open=100.0,
            prev_close=100.6,
            prev_h_ema_fast=100.4,
            prev_h_ema_slow=99.9,
            loc_close=99.5,
            loc_ema_fast=99.7,
            loc_ema_slow=99.8,
            prev_loc_ema_fast=100.0,
            prev_loc_ema_slow=99.9,
            loc_rsi_value=47.0,
            loc_atr_value=0.5,
            loc_current_high=100.1,
            loc_current_low=99.2,
        )
        exit_out = resolve_variant_signal_state_from_inputs(long_exit_inputs)
        self.assertTrue(exit_out["long_exit"])

    def test_mtf_ema_trend_v2_long_and_short_paths(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v2"),
            bias="long",
            close=101.45,
            ema_value=101.2,
            rsi_value=54.0,
            macd_hist_value=0.03,
            atr_value=0.9,
            hhv=102.5,
            llv=97.0,
            exl=99.0,
            exh=103.0,
            pb_low=100.7,
            pb_high=101.9,
            h_close=102.0,
            h_ema_fast=100.5,
            h_ema_slow=99.6,
            width=0.02,
            width_avg=0.01,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=101.6,
            current_low=100.95,
            prev_high=101.4,
            prev_low=100.7,
            current_open=101.05,
            prev_close=101.0,
            prev_h_ema_fast=100.1,
            prev_h_ema_slow=99.4,
            prev_macd_hist=-0.01,
            loc_close=101.35,
            loc_ema_fast=101.15,
            loc_ema_slow=100.95,
            prev_loc_ema_fast=101.05,
            prev_loc_ema_slow=100.9,
            loc_rsi_value=53.0,
            loc_atr_value=0.55,
            loc_current_high=101.55,
            loc_current_low=100.95,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertFalse(long_out["short_entry"])
        self.assertEqual(long_out["long_level"], 1)

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="mtf_ema_trend_v2"),
            bias="short",
            close=97.45,
            ema_value=97.7,
            rsi_value=45.0,
            macd_hist_value=-0.04,
            atr_value=0.9,
            hhv=103.0,
            llv=96.5,
            exl=96.8,
            exh=100.0,
            pb_low=97.0,
            pb_high=98.3,
            h_close=97.0,
            h_ema_fast=98.5,
            h_ema_slow=99.4,
            width=0.02,
            width_avg=0.01,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=97.95,
            current_low=97.35,
            prev_high=98.1,
            prev_low=97.5,
            current_open=97.9,
            prev_close=97.8,
            prev_h_ema_fast=98.9,
            prev_h_ema_slow=99.5,
            prev_macd_hist=0.01,
            loc_close=97.55,
            loc_ema_fast=97.75,
            loc_ema_slow=97.95,
            prev_loc_ema_fast=97.82,
            prev_loc_ema_slow=97.98,
            loc_rsi_value=46.0,
            loc_atr_value=0.5,
            loc_current_high=97.95,
            loc_current_low=97.35,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertFalse(short_out["long_entry"])
        self.assertEqual(short_out["short_level"], 1)

    def test_daily_sr_zones_v1_reversal_and_breakout(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        reversal_long = VariantSignalInputs(
            p=replace(base, strategy_variant="daily_sr_zones_v1"),
            bias="neutral",
            close=101.4,
            ema_value=100.8,
            rsi_value=41.0,
            macd_hist_value=0.05,
            atr_value=1.2,
            hhv=110.0,
            llv=99.0,
            exl=100.2,
            exh=108.5,
            pb_low=99.6,
            pb_high=104.0,
            h_close=105.0,
            h_ema_fast=102.0,
            h_ema_slow=99.0,
            width=0.03,
            width_avg=0.02,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            prev_hhv=109.0,
            prev_llv=100.0,
            current_high=102.0,
            current_low=99.3,
            prev_high=101.8,
            prev_low=99.8,
            current_open=100.1,
            prev_open=101.2,
            prev_close=100.4,
            volume=1800.0,
            volume_avg=1000.0,
            prev_h_ema_fast=101.2,
            prev_h_ema_slow=98.8,
        )
        reversal_out = resolve_variant_signal_state_from_inputs(reversal_long)
        self.assertTrue(reversal_out["long_entry"])
        self.assertEqual(reversal_out["long_level"], 1)
        self.assertFalse(reversal_out["short_entry"])

        breakout_short = VariantSignalInputs(
            p=replace(base, strategy_variant="daily_sr_zones_v1"),
            bias="neutral",
            close=94.0,
            ema_value=96.0,
            rsi_value=52.0,
            macd_hist_value=-0.2,
            atr_value=1.5,
            hhv=110.0,
            llv=95.0,
            exl=94.5,
            exh=100.0,
            pb_low=94.4,
            pb_high=99.5,
            h_close=92.0,
            h_ema_fast=95.0,
            h_ema_slow=98.0,
            width=0.04,
            width_avg=0.02,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            prev_hhv=108.0,
            prev_llv=96.0,
            current_high=97.0,
            current_low=93.5,
            prev_high=98.5,
            prev_low=96.2,
            current_open=96.6,
            prev_open=95.5,
            prev_close=96.0,
            volume=2200.0,
            volume_avg=1200.0,
            prev_h_ema_fast=96.0,
            prev_h_ema_slow=98.5,
        )
        breakout_out = resolve_variant_signal_state_from_inputs(breakout_short)
        self.assertTrue(breakout_out["short_entry"])
        self.assertEqual(breakout_out["short_level"], 1)

    def test_daily_sr_zones_v2_is_more_permissive_than_v1(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        relaxed_only = VariantSignalInputs(
            p=replace(base, strategy_variant="daily_sr_zones_v2"),
            bias="neutral",
            close=101.0,
            ema_value=100.8,
            rsi_value=47.0,
            macd_hist_value=0.03,
            atr_value=1.2,
            hhv=109.5,
            llv=99.0,
            exl=100.1,
            exh=108.0,
            pb_low=99.4,
            pb_high=103.5,
            h_close=104.5,
            h_ema_fast=101.8,
            h_ema_slow=99.2,
            width=0.03,
            width_avg=0.02,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            prev_hhv=109.0,
            prev_llv=99.2,
            current_high=101.2,
            current_low=99.5,
            prev_high=101.3,
            prev_low=99.7,
            current_open=100.1,
            prev_open=100.8,
            prev_close=100.2,
            volume=1180.0,
            volume_avg=1000.0,
            prev_h_ema_fast=101.0,
            prev_h_ema_slow=99.0,
        )
        strict_out = resolve_variant_signal_state_from_inputs(
            replace(relaxed_only, p=replace(base, strategy_variant="daily_sr_zones_v1"))
        )
        relaxed_out = resolve_variant_signal_state_from_inputs(relaxed_only)
        self.assertFalse(strict_out["long_entry"])
        self.assertTrue(relaxed_out["long_entry"])
        self.assertEqual(relaxed_out["long_level"], 1)

    def test_rsi2_reversion_v1_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="rsi2_reversion_v1"),
            bias="neutral",
            close=97.5,
            ema_value=99.0,
            rsi_value=4.5,
            macd_hist_value=-0.1,
            atr_value=1.2,
            hhv=103.0,
            llv=96.0,
            exl=97.0,
            exh=103.0,
            pb_low=97.1,
            pb_high=101.2,
            h_close=105.0,
            h_ema_fast=103.0,
            h_ema_slow=100.0,
            width=0.03,
            width_avg=0.02,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            prev_hhv=102.5,
            prev_llv=96.5,
            current_high=98.2,
            current_low=97.0,
            prev_high=100.1,
            prev_low=97.3,
            current_open=98.4,
            prev_open=99.8,
            prev_close=98.8,
            upper_band=101.0,
            lower_band=97.2,
            mid_band=99.1,
            prev_macd_hist=-0.2,
            prev_h_ema_fast=102.4,
            prev_h_ema_slow=99.7,
            recent_rsi_min=3.8,
            recent_rsi_max=18.0,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertEqual(long_out["long_level"], 1)
        self.assertFalse(long_out["short_entry"])
        self.assertFalse(long_out["long_exit"])

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="rsi2_reversion_v1"),
            bias="neutral",
            close=103.5,
            ema_value=101.0,
            rsi_value=96.0,
            macd_hist_value=0.2,
            atr_value=1.1,
            hhv=104.0,
            llv=97.0,
            exl=98.0,
            exh=103.2,
            pb_low=99.0,
            pb_high=103.4,
            h_close=95.0,
            h_ema_fast=96.5,
            h_ema_slow=99.0,
            width=0.03,
            width_avg=0.02,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            prev_hhv=103.8,
            prev_llv=97.5,
            current_high=103.8,
            current_low=102.8,
            prev_high=103.1,
            prev_low=100.8,
            current_open=102.7,
            prev_open=101.2,
            prev_close=102.4,
            upper_band=103.6,
            lower_band=99.2,
            mid_band=101.4,
            prev_macd_hist=0.1,
            prev_h_ema_fast=97.0,
            prev_h_ema_slow=99.4,
            recent_rsi_min=80.0,
            recent_rsi_max=97.5,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertEqual(short_out["short_level"], 1)
        self.assertFalse(short_out["long_entry"])
        self.assertFalse(short_out["short_exit"])

    def test_bollinger_trend_reversion_v1_long_and_short_trigger(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        base = cfg.params

        long_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="bollinger_trend_reversion_v1"),
            bias="neutral",
            close=98.6,
            ema_value=99.4,
            rsi_value=34.0,
            macd_hist_value=-0.05,
            atr_value=1.0,
            hhv=103.0,
            llv=96.0,
            exl=97.5,
            exh=102.8,
            pb_low=97.2,
            pb_high=100.4,
            h_close=105.0,
            h_ema_fast=103.0,
            h_ema_slow=100.0,
            width=0.028,
            width_avg=0.020,
            long_location_ok=True,
            short_location_ok=False,
            pullback_long=True,
            pullback_short=False,
            not_chasing_long=True,
            not_chasing_short=False,
            current_high=98.8,
            current_low=97.1,
            prev_high=100.2,
            prev_low=97.4,
            current_open=97.6,
            prev_open=99.3,
            prev_close=97.7,
            upper_band=101.2,
            lower_band=97.4,
            mid_band=99.3,
            prev_h_ema_fast=102.4,
            prev_h_ema_slow=99.8,
            loc_rsi_value=39.0,
            loc_atr_value=1.2,
        )
        long_out = resolve_variant_signal_state_from_inputs(long_inputs)
        self.assertTrue(long_out["long_entry"])
        self.assertEqual(long_out["long_level"], 1)
        self.assertFalse(long_out["short_entry"])
        self.assertFalse(long_out["long_exit"])

        short_inputs = VariantSignalInputs(
            p=replace(base, strategy_variant="bollinger_trend_reversion_v1"),
            bias="neutral",
            close=101.4,
            ema_value=100.5,
            rsi_value=66.0,
            macd_hist_value=0.06,
            atr_value=0.9,
            hhv=104.0,
            llv=97.0,
            exl=98.5,
            exh=103.0,
            pb_low=99.1,
            pb_high=103.1,
            h_close=94.5,
            h_ema_fast=96.0,
            h_ema_slow=99.0,
            width=0.026,
            width_avg=0.018,
            long_location_ok=False,
            short_location_ok=True,
            pullback_long=False,
            pullback_short=True,
            not_chasing_long=False,
            not_chasing_short=True,
            current_high=103.1,
            current_low=101.1,
            prev_high=102.7,
            prev_low=100.3,
            current_open=102.3,
            prev_open=101.2,
            prev_close=102.2,
            upper_band=102.8,
            lower_band=98.4,
            mid_band=100.6,
            prev_h_ema_fast=96.5,
            prev_h_ema_slow=99.4,
            loc_rsi_value=61.0,
            loc_atr_value=1.1,
        )
        short_out = resolve_variant_signal_state_from_inputs(short_inputs)
        self.assertTrue(short_out["short_entry"])
        self.assertEqual(short_out["short_level"], 1)
        self.assertFalse(short_out["long_entry"])
        self.assertFalse(short_out["short_exit"])


if __name__ == "__main__":
    unittest.main()
