from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from okx_trader.config import read_config


class ConfigProfileGroupsTests(unittest.TestCase):
    def test_profile_inst_groups_maps_instruments(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP,SOL-USDT-SWAP,DOGE-USDT-SWAP",
            "STRAT_PROFILE_INST_GROUPS": "BTCETH:BTC-USDT-SWAP;ELDERALT:SOL-USDT-SWAP,DOGE-USDT-SWAP",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertEqual(cfg.strategy_profile_map.get("BTC-USDT-SWAP"), "BTCETH")
        self.assertEqual(cfg.strategy_profile_map.get("SOL-USDT-SWAP"), "ELDERALT")
        self.assertEqual(cfg.strategy_profile_map.get("DOGE-USDT-SWAP"), "ELDERALT")

    def test_explicit_profile_map_overrides_group_map(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP,SOL-USDT-SWAP",
            "STRAT_PROFILE_INST_GROUPS": "BTCETH:BTC-USDT-SWAP,SOL-USDT-SWAP",
            "STRAT_PROFILE_MAP": "BTC-USDT-SWAP:DEFAULT",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertEqual(cfg.strategy_profile_map.get("BTC-USDT-SWAP"), "DEFAULT")
        self.assertEqual(cfg.strategy_profile_map.get("SOL-USDT-SWAP"), "BTCETH")

    def test_explicit_vote_map_overrides_vote_group_map(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP,SOL-USDT-SWAP",
            "STRAT_PROFILE_VOTE_INST_GROUPS": (
                "BTCETH+ELDER:BTC-USDT-SWAP;"
                "DEFAULT+ELDERALT:SOL-USDT-SWAP"
            ),
            "STRAT_PROFILE_VOTE_MAP": "BTC-USDT-SWAP:DEFAULT+ELDER",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertEqual(cfg.strategy_profile_vote_map.get("BTC-USDT-SWAP"), ["DEFAULT", "ELDER"])
        self.assertEqual(cfg.strategy_profile_vote_map.get("SOL-USDT-SWAP"), ["DEFAULT", "ELDERALT"])

    def test_profile_leverage_override_is_independent_from_global(self) -> None:
        env = {
            "OKX_INST_IDS": "XAU-USDT-SWAP,BTC-USDT-SWAP",
            "OKX_LEVERAGE": "10",
            "STRAT_PROFILE_INST_GROUPS": "XAU:XAU-USDT-SWAP",
            "STRAT_PROFILE_XAU_LEVERAGE": "25",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertEqual(cfg.leverage, 10.0)
        self.assertEqual(cfg.strategy_profiles["XAU"].leverage, 25.0)

    def test_signal_exit_default_off_and_profile_override(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP,SOL-USDT-SWAP",
            "STRAT_PROFILE_INST_GROUPS": "BTCETH:BTC-USDT-SWAP",
            "STRAT_SIGNAL_EXIT_ENABLED": "0",
            "STRAT_PROFILE_BTCETH_SIGNAL_EXIT_ENABLED": "1",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertFalse(cfg.params.signal_exit_enabled)
        self.assertTrue(cfg.strategy_profiles["BTCETH"].signal_exit_enabled)
        self.assertFalse(cfg.strategy_profiles["DEFAULT"].signal_exit_enabled)

    def test_split_tp_default_off_and_profile_override(self) -> None:
        env = {
            "OKX_INST_IDS": "BTC-USDT-SWAP,SOL-USDT-SWAP",
            "STRAT_PROFILE_INST_GROUPS": "BTCETH:BTC-USDT-SWAP",
            "STRAT_SPLIT_TP_ON_ENTRY": "0",
            "STRAT_PROFILE_BTCETH_SPLIT_TP_ON_ENTRY": "1",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = read_config(None)
        self.assertFalse(cfg.params.split_tp_on_entry)
        self.assertTrue(cfg.strategy_profiles["BTCETH"].split_tp_on_entry)
        self.assertFalse(cfg.strategy_profiles["DEFAULT"].split_tp_on_entry)


if __name__ == "__main__":
    unittest.main()
