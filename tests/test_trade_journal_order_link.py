from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from okx_trader.trade_journal import append_trade_order_link


def _base_row() -> dict:
    return {
        "event_ts_ms": 1_700_000_000_000,
        "signal_ts_ms": 1_700_000_000_000,
        "event_type": "CLOSE",
        "trade_id": "BTC-USDT-SWAP:123",
        "inst_id": "BTC-USDT-SWAP",
        "side": "long",
        "size": 1.0,
        "reason": "tp2_full",
        "entry_ord_id": "111",
        "entry_cl_ord_id": "ATLBTC...",
        "event_ord_id": "222",
        "event_cl_ord_id": "ATLBTC...CLOSE",
        "profile_id": "BTCETH",
        "strategy_variant": "btceth_smc_a2",
    }


class TradeOrderLinkJournalTests(unittest.TestCase):
    def test_default_path_derived_from_trade_journal_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "trade_journal.csv"
            cfg = SimpleNamespace(
                trade_journal_enabled=True,
                trade_journal_path=str(journal_path),
                trade_order_link_enabled=True,
                trade_order_link_path="",
            )
            ok = append_trade_order_link(cfg, _base_row())
            self.assertTrue(ok)
            link_path = Path(td) / "trade_journal_order_links.csv"
            self.assertTrue(link_path.exists())
            with link_path.open("r", encoding="utf-8", newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_ord_id"], "222")
            self.assertEqual(rows[0]["trade_id"], "BTC-USDT-SWAP:123")

    def test_custom_path_honored(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            journal_path = Path(td) / "trade_journal.csv"
            custom_path = Path(td) / "links" / "orders.csv"
            cfg = SimpleNamespace(
                trade_journal_enabled=True,
                trade_journal_path=str(journal_path),
                trade_order_link_enabled=True,
                trade_order_link_path=str(custom_path),
            )
            ok = append_trade_order_link(cfg, _base_row())
            self.assertTrue(ok)
            self.assertTrue(custom_path.exists())

    def test_disabled_returns_false(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = SimpleNamespace(
                trade_journal_enabled=True,
                trade_journal_path=str(Path(td) / "trade_journal.csv"),
                trade_order_link_enabled=False,
                trade_order_link_path=str(Path(td) / "orders.csv"),
            )
            ok = append_trade_order_link(cfg, _base_row())
            self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
