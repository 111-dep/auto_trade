from __future__ import annotations

import csv
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from daily_recap import (
    _entry_exec_stats,
    _build_bills_mapping_quality,
    _resolve_net_pnl,
    _summarize_equity_delta,
    summarize_runtime_log,
    summarize_trade_journal,
)


class DailyRecapTests(unittest.TestCase):
    def test_entry_exec_stats_basic(self) -> None:
        runtime = {
            "entry_exec_counter": {
                "market": 10,
                "limit": 30,
                "limit_fallback_market": 5,
            },
            "entry_exec_open_actions": 22,
            "entry_exec_legs": 45,
        }
        out = _entry_exec_stats(runtime)
        self.assertEqual(int(out.get("total", 0)), 45)
        self.assertEqual(int(out.get("actions", 0)), 22)
        self.assertEqual(int(out.get("limit_attempts", 0)), 35)
        self.assertAlmostEqual(float(out.get("limit_fill_ratio", 0.0)), 30 / 35, places=9)
        self.assertAlmostEqual(float(out.get("fallback_ratio", 0.0)), 5 / 35, places=9)

    def test_entry_exec_stats_handles_empty(self) -> None:
        out = _entry_exec_stats({})
        self.assertEqual(int(out.get("total", 0)), 0)
        self.assertEqual(int(out.get("limit_attempts", 0)), 0)
        self.assertEqual(float(out.get("limit_fill_ratio", -1.0)), 0.0)
        self.assertEqual(float(out.get("fallback_ratio", -1.0)), 0.0)

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

    def test_summarize_trade_journal_batch_stats_and_side_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "trade_journal.csv"
            fieldnames = [
                "event_ts_ms",
                "signal_ts_ms",
                "event_type",
                "trade_id",
                "inst_id",
                "side",
                "reason",
                "pnl_usdt",
                "entry_ord_id",
            ]
            rows = [
                {"event_ts_ms": "1000", "signal_ts_ms": "1000", "event_type": "OPEN", "trade_id": "T1", "inst_id": "BTC-USDT-SWAP", "side": "short", "reason": "open_short", "pnl_usdt": "", "entry_ord_id": "E1"},
                {"event_ts_ms": "1010", "signal_ts_ms": "1000", "event_type": "OPEN", "trade_id": "T2", "inst_id": "ETH-USDT-SWAP", "side": "short", "reason": "open_short", "pnl_usdt": "", "entry_ord_id": "E2"},
                {"event_ts_ms": "2000", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "T1", "inst_id": "BTC-USDT-SWAP", "side": "short", "reason": "short_stop", "pnl_usdt": "-1.0", "entry_ord_id": "E1"},
                {"event_ts_ms": "2100", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "T2", "inst_id": "ETH-USDT-SWAP", "side": "short", "reason": "short_stop", "pnl_usdt": "-2.0", "entry_ord_id": "E2"},
                {"event_ts_ms": "3000", "signal_ts_ms": "3000", "event_type": "OPEN", "trade_id": "T3", "inst_id": "SOL-USDT-SWAP", "side": "short", "reason": "open_short", "pnl_usdt": "", "entry_ord_id": "E3"},
                {"event_ts_ms": "3500", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "T3", "inst_id": "SOL-USDT-SWAP", "side": "short", "reason": "short_stop", "pnl_usdt": "-0.5", "entry_ord_id": "E3"},
                {"event_ts_ms": "4000", "signal_ts_ms": "4000", "event_type": "OPEN", "trade_id": "T4", "inst_id": "BCH-USDT-SWAP", "side": "long", "reason": "open_long", "pnl_usdt": "", "entry_ord_id": "E4"},
                {"event_ts_ms": "4500", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "T4", "inst_id": "BCH-USDT-SWAP", "side": "long", "reason": "long_exit", "pnl_usdt": "2.0", "entry_ord_id": "E4"},
            ]
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

            out = summarize_trade_journal(path, start_ms=0, end_ms=10_000)
            batch = out.get("batch_stats") or {}
            conc = out.get("side_concurrency") or {}
            self.assertEqual(int(batch.get("batch_count", 0)), 3)
            self.assertEqual(int(batch.get("closed_batch_count", 0)), 3)
            self.assertEqual(int(batch.get("loss_batch_count", 0)), 2)
            self.assertEqual(int(batch.get("win_batch_count", 0)), 1)
            self.assertEqual(int(batch.get("max_loss_streak", 0)), 2)
            self.assertEqual(int(batch.get("current_win_streak", 0)), 1)
            self.assertEqual(int(conc.get("max_short", 0)), 2)
            self.assertEqual(int(conc.get("max_same_side", 0)), 2)

    def test_side_concurrency_counts_positions_opened_before_window(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "trade_journal.csv"
            fieldnames = [
                "event_ts_ms",
                "signal_ts_ms",
                "event_type",
                "trade_id",
                "inst_id",
                "side",
                "reason",
                "pnl_usdt",
                "entry_ord_id",
            ]
            rows = [
                {"event_ts_ms": "500", "signal_ts_ms": "500", "event_type": "OPEN", "trade_id": "P0", "inst_id": "BTC-USDT-SWAP", "side": "short", "reason": "open_short", "pnl_usdt": "", "entry_ord_id": "EP0"},
                {"event_ts_ms": "1200", "signal_ts_ms": "1200", "event_type": "OPEN", "trade_id": "P1", "inst_id": "ETH-USDT-SWAP", "side": "short", "reason": "open_short", "pnl_usdt": "", "entry_ord_id": "EP1"},
                {"event_ts_ms": "1300", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "P0", "inst_id": "BTC-USDT-SWAP", "side": "short", "reason": "short_stop", "pnl_usdt": "-1.0", "entry_ord_id": "EP0"},
                {"event_ts_ms": "1400", "signal_ts_ms": "", "event_type": "CLOSE", "trade_id": "P1", "inst_id": "ETH-USDT-SWAP", "side": "short", "reason": "short_stop", "pnl_usdt": "-1.0", "entry_ord_id": "EP1"},
            ]
            with path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

            out = summarize_trade_journal(path, start_ms=1000, end_ms=2000)
            conc = out.get("side_concurrency") or {}
            self.assertEqual(int(conc.get("window_start_active_short", 0)), 1)
            self.assertEqual(int(conc.get("max_short", 0)), 2)

    def test_resolve_net_pnl_fallback_to_journal_when_bills_unmapped_high(self) -> None:
        report = {
            "journal": {"realized_pnl": "123.45"},
            "bills": {
                "recommended_net": "-9.99",
                "selected_trade_rows": 100,
                "mapped_rows": 30,
                "unmapped_rows": 70,
                "ambiguous_rows": 0,
            },
        }
        net, src, note = _resolve_net_pnl(report)
        self.assertEqual(str(net), "123.45")
        self.assertEqual(src, "journal")
        self.assertIn("bills_unmapped_ratio_high", note)

    def test_resolve_net_pnl_uses_bills_when_mapping_quality_ok(self) -> None:
        report = {
            "journal": {"realized_pnl": "123.45"},
            "bills": {
                "recommended_net": "88.88",
                "selected_trade_rows": 100,
                "mapped_rows": 80,
                "unmapped_rows": 20,
                "ambiguous_rows": 0,
            },
        }
        net, src, note = _resolve_net_pnl(report)
        self.assertEqual(str(net), "88.88")
        self.assertEqual(src, "bills")
        self.assertEqual(note, "ok")

    def test_build_bills_mapping_quality_hard_alert_when_unmapped_too_high(self) -> None:
        report = {
            "bills": {
                "selected_trade_rows": 100,
                "mapped_rows": 20,
                "unmapped_rows": 80,
                "ambiguous_rows": 0,
            }
        }
        q = _build_bills_mapping_quality(report)
        self.assertEqual(str(q.get("status")), "alert")
        self.assertTrue(bool(q.get("hard_alert")))
        self.assertIn("bills_unmapped_ratio_high", str(q.get("net_note")))

    def test_build_bills_mapping_quality_warn_when_sample_too_small_for_alert(self) -> None:
        report = {
            "bills": {
                "selected_trade_rows": 10,
                "mapped_rows": 4,
                "unmapped_rows": 6,
                "ambiguous_rows": 0,
            }
        }
        q = _build_bills_mapping_quality(report)
        self.assertEqual(str(q.get("status")), "warn")
        self.assertFalse(bool(q.get("hard_alert")))

    def test_summarize_equity_delta_uses_snapshot_and_appends_current(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "equity_snapshots.csv"
            snap.write_text("ts_ms,equity\n1000,1000\n4000,1200\n", encoding="utf-8")
            out = _summarize_equity_delta(
                start_ms=4500,
                end_ms=9000,
                current_equity=1300,
                snapshot_path=snap,
            )
            self.assertTrue(bool(out.get("available")))
            self.assertEqual(str(out.get("start_equity")), "1200")
            self.assertEqual(str(out.get("delta_usdt")), "100")
            self.assertIn("snapshot_le_start", str(out.get("start_source")))
            txt = snap.read_text(encoding="utf-8")
            self.assertIn("9000,1300", txt.replace(".0", ""))

    def test_summarize_equity_delta_handles_missing_start_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            snap = Path(td) / "equity_snapshots.csv"
            out = _summarize_equity_delta(
                start_ms=4500,
                end_ms=9000,
                current_equity=1300,
                snapshot_path=snap,
            )
            self.assertFalse(bool(out.get("available")))
            self.assertEqual(str(out.get("reason")), "no_start_snapshot")


if __name__ == "__main__":
    unittest.main()
