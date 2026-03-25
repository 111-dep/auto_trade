from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from okx_trader.common import apply_backtest_env_overrides


class BacktestEnvOverrideTests(unittest.TestCase):
    def test_backtest_history_cache_ttl_override_applies(self) -> None:
        with patch.dict(
            os.environ,
            {
                "OKX_HISTORY_CACHE_TTL_SECONDS": "21600",
                "OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS": "315360000",
            },
            clear=False,
        ):
            changes = apply_backtest_env_overrides()
            self.assertEqual(os.environ.get("OKX_HISTORY_CACHE_TTL_SECONDS"), "315360000")
            self.assertEqual(changes, ["history_cache_ttl=21600->315360000"])

    def test_backtest_history_cache_ttl_override_is_optional(self) -> None:
        with patch.dict(os.environ, {"OKX_HISTORY_CACHE_TTL_SECONDS": "21600"}, clear=False):
            os.environ.pop("OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS", None)
            changes = apply_backtest_env_overrides()
            self.assertEqual(os.environ.get("OKX_HISTORY_CACHE_TTL_SECONDS"), "21600")
            self.assertEqual(changes, [])


if __name__ == "__main__":
    unittest.main()
