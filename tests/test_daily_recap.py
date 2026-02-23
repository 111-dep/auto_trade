from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from daily_recap import summarize_runtime_log, summarize_trade_journal


class DailyRecapTests(unittest.TestCase):
    def test_summarize_trade_journal_basic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "trade_journal.csv"
            fieldnames = [
                "event_ts_ms",
                "event_type",
                "trade_id",
                "inst_id",
                "side",
                "reason",
                "pnl_usdt",
                "entry_ord_id",
            ]
            rows = [
                {
                    "event_ts_ms": "1000",
                    "event_type": "CLOSE",
                    "trade_id": "T1",
                    "inst_id": "BTC-USDT-SWAP",
                    "side": "short",
                    "reason": "short_exit",
                    "pnl_usdt": "3.0",
                    "entry_ord_id": "E1",
                },
                {
                    "event_ts_ms": "2000",
                    "event_type": "PARTIAL_CLOSE",
                    "trade_id": "T2",
                    "inst_id": "ETH-USDT-SWAP",
                    "side": "short",
                    "reason": "short_stop",
                    "pnl_usdt": "-1.0",
                    "entry_ord_id": "E2",
                },
                {
                    "event_ts_ms": "3000",
                    "event_type": "EXTERNAL_CLOSE",
                    "trade_id": "T2",
                    "inst_id": "ETH-USDT-SWAP",
                    "side": "short",
                    "reason": "short_stop",
                    "pnl_usdt": "-2.0",
                    "entry_ord_id": "E2",
                },
                {
                    "event_ts_ms": "4000",
                    "event_type": "OPEN",
                    "trade_id": "T3",
                    "inst_id": "SOL-USDT-SWAP",
                    "side": "short",
                    "reason": "open_short",
                    "pnl_usdt": "",
                    "entry_ord_id": "E3",
                },
            ]
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

            out = summarize_trade_journal(path, start_ms=0, end_ms=10_000)
            self.assertEqual(int(out["close_row_count"]), 3)
            self.assertEqual(str(out["realized_pnl"]), "0.0")
            self.assertEqual(int(out["current_loss_streak"]), 1)
            self.assertEqual(int(out["current_stop_like_streak"]), 2)
            self.assertEqual(int(out["max_loss_streak"]), 1)
            self.assertEqual(int(out["max_win_streak"]), 1)

    def test_summarize_runtime_log_basic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "runtime.log"
            lines = [
                "[2026-02-23 10:00:00] Heartbeat | loops=1 insts=2 processed=2 no_new=0 stale=0 safety_skip=0 no_data=0 error=0",
                "[2026-02-23 10:05:00] [WARN] [DOGE-USDT-SWAP] Exchange SL sync failed (short, reason=dynamic_short)",
                "[2026-02-23 10:10:00] [ERROR] [BTC-USDT-SWAP] Instrument loop error: timeout",
            ]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")

            start_ms = int(datetime(2026, 2, 23, 9, 59, 0, tzinfo=timezone.utc).timestamp() * 1000)
            end_ms = int(datetime(2026, 2, 23, 10, 20, 0, tzinfo=timezone.utc).timestamp() * 1000)
            out = summarize_runtime_log(path, start_ms=start_ms, end_ms=end_ms, tz=timezone.utc)
            self.assertEqual(int(out["lines"]), 3)
            self.assertEqual(int(out["heartbeat"]), 1)
            self.assertEqual(int(out["warn"]), 1)
            self.assertEqual(int(out["error"]), 1)
            self.assertEqual(int(out["sl_sync_failed"]), 1)
            self.assertEqual(int(out["instrument_loop_error"]), 1)
            self.assertEqual(int(out["heartbeat_totals"]["processed"]), 2)


if __name__ == "__main__":
    unittest.main()
