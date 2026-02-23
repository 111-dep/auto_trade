from __future__ import annotations

import unittest
from decimal import Decimal

from reconcile_okx_bills import summarize_selected_trade_rows_by_trade_id


class ReconcileTradeIdReportTests(unittest.TestCase):
    def test_trade_id_mapping_and_delta(self) -> None:
        rows = [
            {"ordId": "100", "clOrdId": "AT1", "instId": "BTC-USDT-SWAP", "pnl": "10", "fee": "-1"},
            {"ordId": "101", "clOrdId": "AT2", "instId": "ETH-USDT-SWAP", "pnl": "-3", "fee": "-0.5"},
            {"ordId": "999", "clOrdId": "ATX", "instId": "SOL-USDT-SWAP", "pnl": "2", "fee": "-0.2"},
        ]
        link_index = {
            "ord_to_trade": {"100": {"T1"}, "101": {"T2"}},
            "cl_ord_to_trade": {"AT1": {"T1"}, "AT2": {"T2"}},
            "trade_to_inst": {"T1": {"BTC-USDT-SWAP"}, "T2": {"ETH-USDT-SWAP"}},
        }
        journal = {
            "T1": {"rows": 1, "pnl": Decimal("8"), "inst_ids": {"BTC-USDT-SWAP"}},
            "T2": {"rows": 1, "pnl": Decimal("-4"), "inst_ids": {"ETH-USDT-SWAP"}},
        }
        report = summarize_selected_trade_rows_by_trade_id(
            rows,
            link_index=link_index,
            journal_by_trade_id=journal,
        )

        self.assertEqual(int(report["mapped_rows"]), 2)
        self.assertEqual(int(report["unmapped_rows"]), 1)
        self.assertEqual(int(report["ambiguous_rows"]), 0)
        self.assertEqual(report["mapped_net"], Decimal("5.5"))
        self.assertEqual(report["unmapped_net"], Decimal("1.8"))

        per_trade = report["per_trade"]
        self.assertEqual(per_trade["T1"]["bill_net"], Decimal("9"))
        self.assertEqual(per_trade["T1"]["journal_pnl"], Decimal("8"))
        self.assertEqual(per_trade["T1"]["delta_bill_minus_journal"], Decimal("1"))

        self.assertEqual(per_trade["T2"]["bill_net"], Decimal("-3.5"))
        self.assertEqual(per_trade["T2"]["journal_pnl"], Decimal("-4"))
        self.assertEqual(per_trade["T2"]["delta_bill_minus_journal"], Decimal("0.5"))

    def test_ambiguous_rows_are_tracked(self) -> None:
        rows = [
            {"ordId": "200", "clOrdId": "", "instId": "BTC-USDT-SWAP", "pnl": "1", "fee": "-0.1"},
        ]
        link_index = {
            "ord_to_trade": {"200": {"TA", "TB"}},
            "cl_ord_to_trade": {},
            "trade_to_inst": {},
        }
        report = summarize_selected_trade_rows_by_trade_id(
            rows,
            link_index=link_index,
            journal_by_trade_id={},
        )
        self.assertEqual(int(report["mapped_rows"]), 0)
        self.assertEqual(int(report["ambiguous_rows"]), 1)
        self.assertEqual(report["ambiguous_net"], Decimal("0.9"))


if __name__ == "__main__":
    unittest.main()
