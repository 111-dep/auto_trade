from __future__ import annotations

import unittest
from types import SimpleNamespace

from okx_trader.managed_tp2 import ensure_managed_tp1_limit_order, ensure_managed_tp2_limit_order


class _FakeClient:
    def __init__(self) -> None:
        self.placed_orders: list[dict[str, object]] = []

    def get_instrument(self, inst_id: str) -> dict[str, str]:
        return {"lotSz": "1", "minSz": "1"}

    def normalize_order_size(self, inst_id: str, sz: float, reduce_only: bool = False):
        qty = int(float(sz))
        return float(qty), str(qty)

    def get_order(self, inst_id: str, ord_id: str = "", cl_ord_id: str = "") -> dict[str, object]:
        return {}

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side=None,
        reduce_only: bool = False,
        cl_ord_id: str = "",
        ord_type: str = "market",
        px: float = 0.0,
    ) -> dict[str, object]:
        self.placed_orders.append(
            {
                "inst_id": inst_id,
                "side": side,
                "sz": float(sz),
                "pos_side": pos_side,
                "reduce_only": reduce_only,
                "cl_ord_id": cl_ord_id,
                "ord_type": ord_type,
                "px": float(px),
            }
        )
        return {"data": [{"ordId": "ord-1", "clOrdId": cl_ord_id}]}


class ManagedTpOrderTests(unittest.TestCase):
    def _build_cfg(self):
        return SimpleNamespace(pos_mode="net", params=SimpleNamespace(tp1_close_pct=0.5))

    def test_tp1_size_rebases_to_actual_live_position(self) -> None:
        client = _FakeClient()
        cfg = self._build_cfg()
        trade = {
            "side": "long",
            "open_size": 11.0,
            "remaining_size": 11.0,
            "managed_tp1_enabled": True,
            "managed_tp2_enabled": True,
            "managed_tp1_target_px": 1.377,
            "managed_tp1_target_size": 55.0,
            "managed_tp2_target_px": 1.403,
            "managed_tp2_target_size": 55.0,
            "exchange_tp1_px": 1.377,
            "exchange_tp1_size": 55.0,
            "exchange_tp2_px": 1.403,
            "exchange_tp2_size": 55.0,
            "tp1_done": False,
        }

        ok, reason = ensure_managed_tp1_limit_order(
            cfg=cfg,
            client=client,
            inst_id="NEAR-USDT-SWAP",
            trade=trade,
            signal_ts_ms=1,
            level=3,
            reason="test",
        )

        self.assertTrue(ok, reason)
        self.assertEqual(len(client.placed_orders), 1)
        self.assertEqual(client.placed_orders[0]["side"], "sell")
        self.assertEqual(client.placed_orders[0]["sz"], 5.0)
        self.assertEqual(trade["managed_tp1_target_size"], 5.0)
        self.assertEqual(trade["exchange_tp1_size"], 5.0)
        self.assertEqual(trade["managed_tp2_target_size"], 6.0)
        self.assertEqual(trade["exchange_tp2_size"], 6.0)

    def test_tp2_size_caps_to_remaining_position(self) -> None:
        client = _FakeClient()
        cfg = self._build_cfg()
        trade = {
            "side": "long",
            "open_size": 11.0,
            "remaining_size": 6.0,
            "managed_tp1_enabled": True,
            "managed_tp2_enabled": True,
            "managed_tp2_target_px": 1.403,
            "managed_tp2_target_size": 55.0,
            "exchange_tp2_px": 1.403,
            "exchange_tp2_size": 55.0,
            "tp1_done": True,
        }

        ok, reason = ensure_managed_tp2_limit_order(
            cfg=cfg,
            client=client,
            inst_id="NEAR-USDT-SWAP",
            trade=trade,
            signal_ts_ms=1,
            level=3,
            reason="test",
        )

        self.assertTrue(ok, reason)
        self.assertEqual(len(client.placed_orders), 1)
        self.assertEqual(client.placed_orders[0]["side"], "sell")
        self.assertEqual(client.placed_orders[0]["sz"], 6.0)
        self.assertEqual(trade["managed_tp2_target_size"], 6.0)
        self.assertEqual(trade["exchange_tp2_size"], 6.0)


if __name__ == "__main__":
    unittest.main()
