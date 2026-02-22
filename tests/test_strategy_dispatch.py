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
        variants = ["classic", "btceth_smc_a2", "elder_tss_v1", "r_breaker_v1", "range_reversion_v1"]
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


if __name__ == "__main__":
    unittest.main()
