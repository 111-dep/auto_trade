from __future__ import annotations

import bisect
import math
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from okx_trader.backtest import (
    _build_backtest_alignment_counts,
    _build_backtest_precalc,
    _build_backtest_signal_decision_tables,
)
from okx_trader.config import read_config
from okx_trader.models import Candle
from okx_trader.signals import build_signals


class BacktestTableTests(unittest.TestCase):
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

    def test_alignment_counts_match_bisect_right(self) -> None:
        htf_ts = [1000, 4000, 7000]
        loc_ts = [500, 2500, 4500, 6500]
        ltf_ts = [0, 500, 1500, 2500, 3500, 4500, 5500, 7500]

        got_htf, got_loc = _build_backtest_alignment_counts(htf_ts, loc_ts, ltf_ts, start_idx=2)
        want_htf = [0] * len(ltf_ts)
        want_loc = [0] * len(ltf_ts)
        for i, ts in enumerate(ltf_ts):
            if i < 2:
                continue
            want_htf[i] = bisect.bisect_right(htf_ts, ts)
            want_loc[i] = bisect.bisect_right(loc_ts, ts)

        self.assertEqual(got_htf, want_htf)
        self.assertEqual(got_loc, want_loc)

    def test_live_window_signal_mode_matches_direct_build_signals(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        params = cfg.params

        base_ts = 1_700_000_000_000

        def _mk(ts_ms: int, value: float) -> Candle:
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

        htf = [_mk(base_ts + i * 4 * 3600 * 1000, 100.0 + i * 0.08) for i in range(520)]
        loc = [_mk(base_ts + i * 3600 * 1000, 100.0 + i * 0.03) for i in range(2200)]
        ltf = [_mk(base_ts + i * 15 * 60 * 1000, 100.0 + i * 0.01) for i in range(9000)]

        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]
        pre = {"DEFAULT": _build_backtest_precalc(htf, loc, ltf, params)}
        target_i = 7000
        tables = _build_backtest_signal_decision_tables(
            cfg=cfg,
            inst_id="BTC-USDT-SWAP",
            profile_id="DEFAULT",
            inst_profile_ids=["DEFAULT"],
            params_by_profile={"DEFAULT": params},
            pre_by_profile=pre,
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
            start_idx=target_i,
            live_signal_window_limit=cfg.candle_limit,
        )
        got = tables["signal_table"][target_i]
        self.assertIsNotNone(got)

        hi = bisect.bisect_right(htf_ts, ltf_ts[target_i])
        li = bisect.bisect_right(loc_ts, ltf_ts[target_i])
        want = build_signals(
            htf[max(0, hi - cfg.candle_limit) : hi],
            loc[max(0, li - cfg.candle_limit) : li],
            ltf[max(0, target_i + 1 - cfg.candle_limit) : target_i + 1],
            params,
        )

        for key in [
            "signal_ts_ms",
            "htf_ts_ms",
            "loc_ts_ms",
            "bias",
            "long_level",
            "short_level",
            "long_stop",
            "short_stop",
            "long_exit",
            "short_exit",
        ]:
            self.assertEqual(got.get(key), want.get(key), msg=key)

    def test_live_window_signal_tables_use_disk_cache_on_repeat(self) -> None:
        self._load_env_file()
        cfg = read_config(None)
        params = cfg.params

        base_ts = 1_700_000_000_000

        def _mk(ts_ms: int, value: float) -> Candle:
            close = float(value + math.sin(ts_ms / 86_400_000.0) * 0.25)
            return Candle(
                ts_ms=int(ts_ms),
                open=float(close - 0.1),
                high=float(close + 0.2),
                low=float(close - 0.2),
                close=close,
                confirm=True,
                volume=float(500.0 + (ts_ms % 11) * 5.0),
            )

        htf = [_mk(base_ts + i * 4 * 3600 * 1000, 100.0 + i * 0.05) for i in range(520)]
        loc = [_mk(base_ts + i * 3600 * 1000, 100.0 + i * 0.02) for i in range(2200)]
        ltf = [_mk(base_ts + i * 15 * 60 * 1000, 100.0 + i * 0.01) for i in range(5000)]

        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]
        start_idx = 4200

        with tempfile.TemporaryDirectory() as td, patch.dict(
            os.environ,
            {
                'OKX_BACKTEST_LIVE_TABLE_CACHE_DIR': td,
                'OKX_BACKTEST_LIVE_TABLE_CACHE_ENABLED': '1',
            },
            clear=False,
        ):
            first = _build_backtest_signal_decision_tables(
                cfg=cfg,
                inst_id='BTC-USDT-SWAP',
                profile_id='DEFAULT',
                inst_profile_ids=['DEFAULT'],
                params_by_profile={'DEFAULT': params},
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
                start_idx=start_idx,
                live_signal_window_limit=cfg.candle_limit,
            )
            self.assertTrue(any(Path(td).iterdir()))

            with patch('okx_trader.backtest.build_signals', side_effect=AssertionError('cache expected')):
                second = _build_backtest_signal_decision_tables(
                    cfg=cfg,
                    inst_id='BTC-USDT-SWAP',
                    profile_id='DEFAULT',
                    inst_profile_ids=['DEFAULT'],
                    params_by_profile={'DEFAULT': params},
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
                    start_idx=start_idx,
                    live_signal_window_limit=cfg.candle_limit,
                )

            target_i = start_idx + 10
            self.assertEqual(first['signal_table'][target_i], second['signal_table'][target_i])
            self.assertEqual(first['decision_table'][target_i], second['decision_table'][target_i])



if __name__ == "__main__":
    unittest.main()
