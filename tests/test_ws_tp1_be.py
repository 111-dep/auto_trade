from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from okx_trader.config import read_config
from okx_trader.ws_tp1_be import handle_tp1_fill_from_position


class _FakeClient:
    def __init__(self) -> None:
        self.algo_calls = []
        self.order_calls = []

    def amend_algo_sl(self, **kwargs):
        self.algo_calls.append(dict(kwargs))
        return {"code": "0", "data": []}

    def amend_order_attached_sl(self, **kwargs):
        self.order_calls.append(dict(kwargs))
        return {"code": "0", "data": []}


def _cfg():
    env = {
        "OKX_INST_IDS": "SUI-USDT-SWAP",
        "OKX_PAPER": "0",
        "OKX_DRY_RUN": "0",
        "OKX_ATTACH_TPSL_ON_ENTRY": "1",
        "STRAT_SPLIT_TP_ON_ENTRY": "1",
    }
    with patch.dict(os.environ, env, clear=True):
        return read_config(None)


class WsTp1BeTests(unittest.TestCase):
    def test_long_tp1_fill_triggers_be_sync_via_algo(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        inst_state = {
            "trade": {
                "side": "long",
                "entry_price": 100.0,
                "hard_stop": 95.0,
                "open_size": 10.0,
                "remaining_size": 10.0,
                "exchange_split_tp_enabled": True,
                "exchange_tp2_size": 5.0,
                "attach_algo_id": "A-1",
                "attach_algo_cl_ord_id": "",
                "entry_ord_id": "O-1",
                "entry_cl_ord_id": "C-1",
            }
        }
        changed = handle_tp1_fill_from_position(
            cfg=cfg,
            client=client,  # type: ignore[arg-type]
            inst_id="SUI-USDT-SWAP",
            inst_state=inst_state,
            pos_side="long",
            pos_size=5.0,
            event_ts_ms=1_700_000_000_000,
        )
        self.assertTrue(changed)
        trade = inst_state["trade"]
        self.assertTrue(bool(trade.get("tp1_done")))
        self.assertTrue(bool(trade.get("be_armed")))
        self.assertEqual(len(client.algo_calls), 1)
        self.assertEqual(len(client.order_calls), 0)
        expected_be = 100.0 * (1.0 + float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
        self.assertAlmostEqual(float(trade.get("hard_stop", 0.0)), expected_be, places=6)
        self.assertEqual(str(trade.get("exchange_sl_last_reason", "")), "tp1_be_ws")

    def test_no_transition_no_sync(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        inst_state = {
            "trade": {
                "side": "short",
                "entry_price": 10.0,
                "hard_stop": 10.5,
                "open_size": 20.0,
                "remaining_size": 20.0,
                "exchange_split_tp_enabled": True,
                "exchange_tp2_size": 10.0,
                "attach_algo_id": "A-2",
                "entry_ord_id": "O-2",
            }
        }
        changed = handle_tp1_fill_from_position(
            cfg=cfg,
            client=client,  # type: ignore[arg-type]
            inst_id="SUI-USDT-SWAP",
            inst_state=inst_state,
            pos_side="short",
            pos_size=15.0,
            event_ts_ms=1_700_000_000_000,
        )
        self.assertFalse(changed)
        self.assertEqual(len(client.algo_calls), 0)
        self.assertEqual(len(client.order_calls), 0)
        self.assertFalse(bool(inst_state["trade"].get("tp1_done")))

    def test_algo_cl_id_only_still_uses_amend_algos(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        inst_state = {
            "trade": {
                "side": "short",
                "entry_price": 10.0,
                "hard_stop": 10.5,
                "open_size": 8.0,
                "remaining_size": 8.0,
                "exchange_split_tp_enabled": True,
                "exchange_tp2_size": 4.0,
                "attach_algo_id": "",
                "attach_algo_cl_ord_id": "ALGCL-ONLY-1",
                "entry_ord_id": "O-2",
                "entry_cl_ord_id": "C-2",
            }
        }
        changed = handle_tp1_fill_from_position(
            cfg=cfg,
            client=client,  # type: ignore[arg-type]
            inst_id="SUI-USDT-SWAP",
            inst_state=inst_state,
            pos_side="short",
            pos_size=4.0,
            event_ts_ms=1_700_000_000_000,
        )
        self.assertTrue(changed)
        self.assertEqual(len(client.algo_calls), 1)
        self.assertEqual(len(client.order_calls), 0)
        self.assertEqual(client.algo_calls[0].get("algo_cl_ord_id"), "ALGCL-ONLY-1")

    def test_fallback_to_amend_order_without_algo_id(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        inst_state = {
            "trade": {
                "side": "long",
                "entry_price": 50.0,
                "hard_stop": 47.0,
                "open_size": 6.0,
                "remaining_size": 6.0,
                "exchange_split_tp_enabled": True,
                "exchange_tp2_size": 3.0,
                "attach_algo_id": "",
                "attach_algo_cl_ord_id": "",
                "entry_ord_id": "O-3",
                "entry_cl_ord_id": "C-3",
            }
        }
        changed = handle_tp1_fill_from_position(
            cfg=cfg,
            client=client,  # type: ignore[arg-type]
            inst_id="SUI-USDT-SWAP",
            inst_state=inst_state,
            pos_side="long",
            pos_size=3.0,
            event_ts_ms=1_700_000_000_000,
        )
        self.assertTrue(changed)
        self.assertEqual(len(client.algo_calls), 0)
        self.assertEqual(len(client.order_calls), 1)


if __name__ == "__main__":
    unittest.main()
