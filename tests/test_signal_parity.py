from __future__ import annotations

import math
import os
import unittest
from dataclasses import replace
from unittest.mock import patch

from okx_trader.backtest import _build_backtest_signal_decision_tables
from okx_trader.config import read_config
from okx_trader.models import Candle
from okx_trader.signal_parity import build_signal_parity_report


def _mk_candle(ts_ms: int, value: float) -> Candle:
    wiggle = math.sin(ts_ms / 86_400_000.0) * 0.4
    close = float(value + wiggle)
    return Candle(
        ts_ms=int(ts_ms),
        open=float(close - 0.15),
        high=float(close + 0.35),
        low=float(close - 0.35),
        close=close,
        confirm=True,
        volume=float(1000.0 + (ts_ms % 17) * 10.0),
    )


class SignalParityTests(unittest.TestCase):
    def test_runtime_like_signal_matches_live_window_table(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP",
            "OKX_CANDLE_LIMIT": "300",
            "STRAT_VARIANT": "pa_oral_baseline_v1",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)

        base_ts = 1_700_000_000_000
        htf = [_mk_candle(base_ts + i * 4 * 3600 * 1000, 100.0 + i * 0.08) for i in range(520)]
        loc = [_mk_candle(base_ts + i * 3600 * 1000, 100.0 + i * 0.03) for i in range(2200)]
        ltf = [_mk_candle(base_ts + i * 15 * 60 * 1000, 100.0 + i * 0.01) for i in range(9000)]

        report = build_signal_parity_report(
            cfg=cfg,
            inst_id="BTC-USDT-SWAP",
            htf_candles=htf,
            loc_candles=loc,
            ltf_candles=ltf,
            bars=180,
            compare_fast=False,
        )
        self.assertEqual(report["runtime_live_mismatch_count"], 0)
        self.assertGreater(report["bars_compared"], 0)

    def test_backtest_live_window_vote_respects_fallback_profiles(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP",
            "OKX_CANDLE_LIMIT": "300",
            "STRAT_PROFILE_VOTE_MODE": "any",
            "STRAT_PROFILE_VOTE_MIN_AGREE": "1",
            "STRAT_PROFILE_VOTE_FALLBACK_PROFILES": "FALLBACK",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)

        cfg.strategy_profile_vote_fallback_profiles = ["FALLBACK"]
        cfg.strategy_profile_vote_mode = "any"
        cfg.strategy_profile_vote_min_agree = 1

        base = cfg.params
        params_default = replace(base, strategy_variant="pa_oral_baseline_v1", be_trigger_r_mult=1.0)
        params_fallback = replace(base, strategy_variant="pa_oral_baseline_v1", be_trigger_r_mult=9.0)

        base_ts = 1_700_000_000_000
        htf = [_mk_candle(base_ts + i * 4 * 3600 * 1000, 100.0 + i * 0.08) for i in range(10)]
        loc = [_mk_candle(base_ts + i * 3600 * 1000, 100.0 + i * 0.03) for i in range(20)]
        ltf = [_mk_candle(base_ts + i * 15 * 60 * 1000, 100.0 + i * 0.01) for i in range(40)]
        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]

        def _fake_build_signals(_htf, _loc, _ltf, p):
            side = "fallback" if float(getattr(p, "be_trigger_r_mult", 0.0) or 0.0) > 5.0 else "default"
            signal = {
                "signal_ts_ms": int(_ltf[-1].ts_ms),
                "signal_confirm": True,
                "htf_ts_ms": int(_htf[-1].ts_ms),
                "loc_ts_ms": int(_loc[-1].ts_ms),
                "close": 100.0,
                "high": 101.0,
                "low": 99.0,
                "ema": 100.0,
                "atr": 1.0,
                "macd_hist": 0.1,
                "bias": "long",
                "strategy_variant": p.strategy_variant,
                "long_entry": side == "fallback",
                "short_entry": side == "default",
                "long_entry_l2": False,
                "short_entry_l2": False,
                "long_entry_l3": False,
                "short_entry_l3": False,
                "long_level": 1 if side == "fallback" else 0,
                "short_level": 1 if side == "default" else 0,
                "long_stop": 98.0,
                "short_stop": 102.0,
                "long_exit": False,
                "short_exit": False,
            }
            return signal

        with patch("okx_trader.backtest.build_signals", side_effect=_fake_build_signals):
            tables = _build_backtest_signal_decision_tables(
                cfg=cfg,
                inst_id="BTC-USDT-SWAP",
                profile_id="DEFAULT",
                inst_profile_ids=["DEFAULT", "FALLBACK"],
                params_by_profile={"DEFAULT": params_default, "FALLBACK": params_fallback},
                pre_by_profile={},
                htf_candles=htf,
                loc_candles=loc,
                ltf_candles=ltf,
                htf_ts=htf_ts,
                loc_ts=loc_ts,
                ltf_ts=ltf_ts,
                max_level=3,
                min_level=1,
                exact_level=0,
                tp1_only=False,
                start_idx=0,
                live_signal_window_limit=cfg.candle_limit,
            )

        signal = next(sig for sig in tables["signal_table"] if sig is not None)
        self.assertEqual(signal["vote_winner"], "SHORT")
        self.assertEqual(signal["short_level"], 1)
        self.assertEqual(signal["long_level"], 0)
