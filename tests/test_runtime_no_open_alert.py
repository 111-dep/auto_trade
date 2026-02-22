from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from okx_trader.config import read_config
from okx_trader.runtime import _maybe_notify_no_open_timeout


class RuntimeNoOpenAlertTests(unittest.TestCase):
    def _build_cfg(self):
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP",
            "ALERT_NO_OPEN_HOURS": "24",
            "ALERT_NO_OPEN_COOLDOWN_HOURS": "24",
        }
        with patch.dict(os.environ, env, clear=True):
            return read_config(None)

    def test_first_tick_only_sets_monitor_start(self) -> None:
        cfg = self._build_cfg()
        state = {}
        with patch("okx_trader.runtime.notify_no_open_timeout") as notify_mock, patch(
            "okx_trader.runtime.time.time", return_value=1_000.0
        ):
            _maybe_notify_no_open_timeout(cfg, state)
        self.assertEqual(int(state.get("monitor_start_ts_ms", 0)), 1_000_000)
        notify_mock.assert_not_called()

    def test_threshold_hit_alert_once_then_cooldown(self) -> None:
        cfg = self._build_cfg()
        now_s = 200_000.0
        state = {
            "monitor_start_ts_ms": int((now_s - 48 * 3600) * 1000),
            "last_open_signal_ts_ms": int((now_s - 25 * 3600) * 1000),
            "last_open_inst_id": "BTC-USDT-SWAP",
        }
        with patch("okx_trader.runtime.notify_no_open_timeout") as notify_mock, patch(
            "okx_trader.runtime.time.time", side_effect=[now_s, now_s + 60]
        ):
            _maybe_notify_no_open_timeout(cfg, state)
            _maybe_notify_no_open_timeout(cfg, state)
        self.assertEqual(notify_mock.call_count, 1)
        self.assertGreater(int(state.get("last_no_open_alert_ts_ms", 0)), 0)
        self.assertGreater(int(state.get("last_no_open_alert_ref_open_ts_ms", 0)), 0)


if __name__ == "__main__":
    unittest.main()
