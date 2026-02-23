from __future__ import annotations

import csv
import datetime as dt
import os
from typing import Any, Dict

from .common import log
from .models import Config

_JOURNAL_FIELDS = [
    "event_ts_ms",
    "event_ts_utc",
    "signal_ts_ms",
    "signal_ts_utc",
    "event_type",
    "trade_id",
    "inst_id",
    "side",
    "size",
    "entry_price",
    "exit_price",
    "stop_price",
    "tp1_price",
    "tp2_price",
    "entry_level",
    "reason",
    "pnl_usdt",
    "entry_ord_id",
    "entry_cl_ord_id",
    "profile_id",
    "strategy_variant",
    "vote_enabled",
    "vote_mode",
    "vote_winner",
    "vote_winner_profile",
    "vote_winner_level",
]

_ORDER_LINK_FIELDS = [
    "event_ts_ms",
    "event_ts_utc",
    "signal_ts_ms",
    "signal_ts_utc",
    "event_type",
    "trade_id",
    "inst_id",
    "side",
    "size",
    "reason",
    "entry_ord_id",
    "entry_cl_ord_id",
    "event_ord_id",
    "event_cl_ord_id",
    "profile_id",
    "strategy_variant",
]


def _fmt_ts_ms(ts_ms: Any) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return ""


def _append_csv_row(path: str, fieldnames: list[str], row_data: Dict[str, Any]) -> bool:
    if not str(path or "").strip():
        return False

    row: Dict[str, Any] = {}
    for k in fieldnames:
        row[k] = row_data.get(k, "")

    if not row.get("event_ts_utc"):
        row["event_ts_utc"] = _fmt_ts_ms(row.get("event_ts_ms"))
    if not row.get("signal_ts_utc"):
        row["signal_ts_utc"] = _fmt_ts_ms(row.get("signal_ts_ms"))

    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    file_exists = os.path.exists(path)
    try:
        with open(path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if (not file_exists) or os.path.getsize(path) <= 0:
                writer.writeheader()
            writer.writerow(row)
        return True
    except Exception as e:
        log(f"[Journal] write failed: {e}", level="WARN")
        return False


def append_trade_journal(cfg: Config, row_data: Dict[str, Any]) -> bool:
    if not bool(getattr(cfg, "trade_journal_enabled", False)):
        return False
    path = str(getattr(cfg, "trade_journal_path", "") or "").strip()
    if not path:
        return False

    return _append_csv_row(path, _JOURNAL_FIELDS, row_data)


def _resolve_trade_order_link_path(cfg: Config) -> str:
    custom_path = str(getattr(cfg, "trade_order_link_path", "") or "").strip()
    if custom_path:
        return custom_path
    journal_path = str(getattr(cfg, "trade_journal_path", "") or "").strip()
    if not journal_path:
        return ""
    base, ext = os.path.splitext(journal_path)
    if ext.lower() == ".csv":
        return f"{base}_order_links.csv"
    return f"{journal_path}.order_links.csv"


def append_trade_order_link(cfg: Config, row_data: Dict[str, Any]) -> bool:
    if not bool(getattr(cfg, "trade_order_link_enabled", True)):
        return False
    if not bool(getattr(cfg, "trade_journal_enabled", False)):
        return False
    path = _resolve_trade_order_link_path(cfg)
    if not path:
        return False
    return _append_csv_row(path, _ORDER_LINK_FIELDS, row_data)
