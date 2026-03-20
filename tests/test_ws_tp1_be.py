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
        self.place_calls = []
        self.stop_place_calls = []
        self.cancel_calls = []
        self.order_snapshots = {}
        self.raise_on_amend_algo = None
        self.raise_on_amend_order = None

    def amend_algo_sl(self, **kwargs):
        self.algo_calls.append(dict(kwargs))
        if self.raise_on_amend_algo is not None:
            raise self.raise_on_amend_algo
        return {"code": "0", "data": []}

    def amend_order_attached_sl(self, **kwargs):
        self.order_calls.append(dict(kwargs))
        if self.raise_on_amend_order is not None:
            raise self.raise_on_amend_order
        return {"code": "0", "data": []}

    def place_stop_loss_order(
        self,
        *,
        inst_id: str,
        side: str,
        stop_price: float,
        cl_ord_id: str = "",
        size: float = 0.0,
    ):
        call = {
            "inst_id": inst_id,
            "side": side,
            "stop_price": float(stop_price),
            "cl_ord_id": cl_ord_id,
            "size": float(size),
        }
        self.stop_place_calls.append(call)
        row = {
            "algoId": f"SL-{len(self.stop_place_calls)}",
            "algoClOrdId": cl_ord_id or f"SLCL-{len(self.stop_place_calls)}",
            "state": "live",
            "slTriggerPx": str(stop_price),
        }
        return {"code": "0", "data": [row]}

    def get_order(self, *, inst_id: str, ord_id: str = "", cl_ord_id: str = ""):
        key = str(cl_ord_id or ord_id or "")
        return dict(self.order_snapshots.get(key, {}))

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side: str | None = None,
        reduce_only: bool = False,
        attach_algo_ords=None,
        cl_ord_id: str = "",
        ord_type: str = "market",
        px: float = 0.0,
        post_only: bool = False,
    ):
        call = {
            "inst_id": inst_id,
            "side": side,
            "sz": float(sz),
            "pos_side": pos_side,
            "reduce_only": bool(reduce_only),
            "cl_ord_id": cl_ord_id,
            "ord_type": ord_type,
            "px": float(px),
        }
        self.place_calls.append(call)
        row = {"ordId": f"TP2-{len(self.place_calls)}", "clOrdId": cl_ord_id, "state": "live"}
        self.order_snapshots[str(cl_ord_id or row["ordId"])] = dict(row)
        return {"code": "0", "data": [row]}

    def cancel_order(self, *, inst_id: str, ord_id: str = "", cl_ord_id: str = ""):
        self.cancel_calls.append({"inst_id": inst_id, "ord_id": ord_id, "cl_ord_id": cl_ord_id})
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


    def test_prefers_sl_attach_algo_cl_id_when_present(self) -> None:
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
                "attach_algo_id": "ALG-TP1-IGNORED",
                "attach_algo_cl_ord_id": "ALGCL-TP1-IGNORED",
                "exchange_sl_attach_algo_cl_ord_id": "ALGCL-SL-1",
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
        self.assertEqual(client.algo_calls[0].get("algo_cl_ord_id"), "ALGCL-SL-1")

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

    def test_managed_tp2_mode_places_reduce_only_tp2_after_tp1(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        inst_state = {
            "trade": {
                "side": "long",
                "entry_price": 100.0,
                "hard_stop": 95.0,
                "open_size": 10.0,
                "remaining_size": 10.0,
                "managed_tp1_enabled": True,
                "managed_tp1_ord_id": "TP1-1",
                "managed_tp1_cl_ord_id": "TP1-CL-1",
                "managed_tp1_order_state": "live",
                "managed_tp2_enabled": True,
                "exchange_tp2_size": 5.0,
                "exchange_tp2_px": 112.5,
                "managed_tp2_target_px": 112.5,
                "exchange_sl_attach_algo_cl_ord_id": "ALGCL-SL-1",
                "entry_ord_id": "O-4",
                "entry_cl_ord_id": "C-4",
                "entry_level": 3,
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
        self.assertEqual(len(client.place_calls), 1)
        self.assertEqual(client.place_calls[0].get("ord_type"), "limit")
        self.assertTrue(bool(client.place_calls[0].get("reduce_only")))
        self.assertEqual(client.place_calls[0].get("side"), "sell")
        self.assertAlmostEqual(float(client.place_calls[0].get("px", 0.0)), 112.5, places=6)
        self.assertEqual(str(trade.get("managed_tp2_order_state", "")), "live")
        self.assertTrue(str(trade.get("managed_tp2_cl_ord_id", "")))
        self.assertFalse(str(trade.get("managed_tp1_cl_ord_id", "")))

    def test_ws_sync_failure_falls_back_to_independent_stop(self) -> None:
        cfg = _cfg()
        client = _FakeClient()
        client.raise_on_amend_algo = RuntimeError("algo missing")
        client.raise_on_amend_order = RuntimeError("order filled")
        inst_state = {
            "trade": {
                "side": "short",
                "entry_price": 10.0,
                "hard_stop": 10.5,
                "open_size": 8.0,
                "remaining_size": 8.0,
                "exchange_split_tp_enabled": True,
                "exchange_tp2_size": 4.0,
                "attach_algo_id": "ALG-1",
                "attach_algo_cl_ord_id": "ALGCL-1",
                "entry_ord_id": "O-5",
                "entry_cl_ord_id": "C-5",
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
        trade = inst_state["trade"]
        self.assertEqual(len(client.algo_calls), 1)
        self.assertEqual(len(client.order_calls), 1)
        self.assertEqual(len(client.stop_place_calls), 1)
        self.assertTrue(bool(trade.get("exchange_sl_independent")))
        self.assertEqual(str(trade.get("exchange_sl_last_reason", "")), "tp1_be_ws_fallback")
        expected_be = 10.0 * (1.0 - float(cfg.params.be_offset_pct) - float(cfg.params.be_fee_buffer_pct))
        self.assertAlmostEqual(float(client.stop_place_calls[0]["stop_price"]), expected_be, places=6)



if __name__ == "__main__":
    unittest.main()
