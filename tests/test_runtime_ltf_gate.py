from __future__ import annotations

import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.models import Candle
from okx_trader.runtime_run_once_for_inst import run_once_for_inst


class _FakeClient:
    def __init__(self, candles_by_bar):
        self._candles_by_bar = candles_by_bar
        self.calls = []

    def get_candles(self, inst_id, bar, limit, include_unconfirmed=False):
        self.calls.append((inst_id, bar, int(limit), bool(include_unconfirmed)))
        return list(self._candles_by_bar.get(bar, []))


def _build_cfg(*, fast_ltf_gate: bool):
    params = SimpleNamespace(
        leverage=10.0,
        tp1_r_mult=1.5,
        tp2_r_mult=2.5,
        exec_max_level=3,
        exec_l3_inst_ids=[],
        skip_on_foreign_mgnmode_pos=False,
    )
    return SimpleNamespace(
        strategy_profile_vote_map={},
        strategy_profile_map={},
        strategy_profiles={"DEFAULT": params},
        params=params,
        alert_only=True,
        alert_intrabar_enabled=False,
        htf_bar="4H",
        loc_bar="1H",
        ltf_bar="15m",
        candle_limit=300,
        fast_ltf_gate=bool(fast_ltf_gate),
        dry_run=False,
        api_key="",
        secret_key="",
        passphrase="",
        td_mode="isolated",
        pos_mode="net",
    )


class RuntimeLtfGateTests(unittest.TestCase):
    def test_fast_ltf_gate_skips_htf_loc_when_no_new_ltf_close(self) -> None:
        now_ms = int(time.time() * 1000)
        ltf = [Candle(ts_ms=now_ms, open=1.0, high=1.1, low=0.9, close=1.0, confirm=True, volume=1.0)]
        client = _FakeClient({"15m": ltf})
        cfg = _build_cfg(fast_ltf_gate=True)
        inst_state = {"last_processed_ts_ms": now_ms}

        with patch("okx_trader.runtime_run_once_for_inst.build_signals", side_effect=AssertionError("unexpected")):
            processed, status = run_once_for_inst(client, cfg, "BTC-USDT-SWAP", inst_state, root_state={})

        self.assertFalse(processed)
        self.assertEqual(status, "no_new")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(client.calls[0][1], "15m")
        self.assertEqual(client.calls[0][2], 2)

    def test_fast_ltf_gate_runs_full_path_on_new_ltf_close(self) -> None:
        now_ms = int(time.time() * 1000)
        ltf = [Candle(ts_ms=now_ms, open=1.0, high=1.1, low=0.9, close=1.0, confirm=True, volume=1.0)]
        htf = [Candle(ts_ms=now_ms, open=1.0, high=1.1, low=0.9, close=1.0, confirm=True, volume=1.0)]
        loc = [Candle(ts_ms=now_ms, open=1.0, high=1.1, low=0.9, close=1.0, confirm=True, volume=1.0)]
        client = _FakeClient({"15m": ltf, "4H": htf, "1H": loc})
        cfg = _build_cfg(fast_ltf_gate=True)
        inst_state = {"last_processed_ts_ms": now_ms - 60_000}
        sig = {
            "signal_ts_ms": now_ms,
            "signal_confirm": True,
            "long_level": 0,
            "short_level": 0,
        }

        with (
            patch("okx_trader.runtime_run_once_for_inst.build_signals", return_value=sig) as mock_build,
            patch("okx_trader.runtime_run_once_for_inst._execute_decision_with_params") as mock_exec,
        ):
            processed, status = run_once_for_inst(client, cfg, "BTC-USDT-SWAP", inst_state, root_state={})

        self.assertTrue(processed)
        self.assertEqual(status, "processed")
        self.assertEqual([c[1] for c in client.calls], ["15m", "15m", "4H", "1H"])
        self.assertEqual(client.calls[0][2], 2)
        self.assertEqual(client.calls[1][2], 300)
        self.assertEqual(mock_build.call_count, 1)
        self.assertEqual(mock_exec.call_count, 1)


if __name__ == "__main__":
    unittest.main()
