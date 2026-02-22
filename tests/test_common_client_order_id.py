from __future__ import annotations

import unittest

from okx_trader.common import build_client_order_id


class ClientOrderIdTests(unittest.TestCase):
    def test_client_order_id_is_deterministic(self) -> None:
        a = build_client_order_id(
            prefix="AT",
            inst_id="BTC-USDT-SWAP",
            side="long",
            signal_ts_ms=1700000000000,
            salt="open|lv=2",
        )
        b = build_client_order_id(
            prefix="AT",
            inst_id="BTC-USDT-SWAP",
            side="long",
            signal_ts_ms=1700000000000,
            salt="open|lv=2",
        )
        self.assertEqual(a, b)

    def test_client_order_id_changes_with_inputs(self) -> None:
        base = build_client_order_id(
            prefix="AT",
            inst_id="BTC-USDT-SWAP",
            side="long",
            signal_ts_ms=1700000000000,
            salt="open|lv=2",
        )
        c = build_client_order_id(
            prefix="AT",
            inst_id="BTC-USDT-SWAP",
            side="short",
            signal_ts_ms=1700000000000,
            salt="open|lv=2",
        )
        d = build_client_order_id(
            prefix="AT",
            inst_id="BTC-USDT-SWAP",
            side="long",
            signal_ts_ms=1700000000000,
            salt="open|lv=3",
        )
        self.assertNotEqual(base, c)
        self.assertNotEqual(base, d)

    def test_client_order_id_length_and_charset(self) -> None:
        oid = build_client_order_id(
            prefix="AUTO",
            inst_id="UNI-USDT-SWAP",
            side="short",
            signal_ts_ms=1701234567890,
            salt="tp1_partial",
        )
        self.assertLessEqual(len(oid), 32)
        self.assertTrue(oid.isalnum(), msg=oid)


if __name__ == "__main__":
    unittest.main()
