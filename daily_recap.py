#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from okx_trader.alerts import send_telegram
from okx_trader.common import load_dotenv, truncate_text
from okx_trader.config import read_config

try:
    from reconcile_okx_bills import (
        fetch_bills,
        load_trade_journal_close_by_trade_id,
        load_trade_order_link_index,
        summarize_bills,
        summarize_selected_trade_rows_by_trade_id,
    )
except Exception:  # pragma: no cover - optional path
    fetch_bills = None
    load_trade_journal_close_by_trade_id = None
    load_trade_order_link_index = None
    summarize_bills = None
    summarize_selected_trade_rows_by_trade_id = None


UTC = timezone.utc
_CLOSE_TYPES = {"CLOSE", "EXTERNAL_CLOSE", "PARTIAL_CLOSE"}
_LOG_LINE_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s*(?:\[(?P<level>DEBUG|INFO|WARN|ERROR)\]\s*)?(?P<msg>.*)$"
)
_BILLS_UNMAPPED_MAX_RATIO_DEFAULT = 0.35
_BILLS_UNMAPPED_ALERT_RATIO_DEFAULT = 0.50
_BILLS_ALERT_MIN_SELECTED_DEFAULT = 20


@dataclass
class TradeSummary:
    trade_id: str
    inst_id: str
    side: str
    pnl: Decimal
    close_count: int
    first_ts_ms: int
    last_ts_ms: int
    reasons: Counter


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0").strip())
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _fmt_decimal(v: Decimal, places: str = "0.0000") -> str:
    return f"{v.quantize(Decimal(places))}"


def _parse_tz_offset(text: str) -> timezone:
    raw = str(text or "").strip().upper().replace("UTC", "")
    if raw in {"", "Z", "+00:00", "+0000", "+00"}:
        return UTC
    m = re.fullmatch(r"([+-])(\d{1,2})(?::?(\d{2}))?", raw)
    if not m:
        raise ValueError(f"invalid tz offset: {text}")
    sign = 1 if m.group(1) == "+" else -1
    hh = int(m.group(2))
    mm = int(m.group(3) or "0")
    if hh > 23 or mm > 59:
        raise ValueError(f"invalid tz offset: {text}")
    delta = timedelta(hours=hh, minutes=mm) * sign
    return timezone(delta)


def _resolve_day_window(date_text: str, tz: timezone) -> Tuple[int, int, str, str]:
    if date_text:
        local_day = datetime.strptime(date_text, "%Y-%m-%d").date()
    else:
        local_day = datetime.now(tz).date()
    local_start = datetime(local_day.year, local_day.month, local_day.day, tzinfo=tz)
    local_end = local_start + timedelta(days=1)
    start_ms = int(local_start.astimezone(UTC).timestamp() * 1000)
    end_ms = int(local_end.astimezone(UTC).timestamp() * 1000)
    return start_ms, end_ms, local_start.strftime("%Y-%m-%d"), str(local_start.tzinfo)


def _resolve_rolling_window(hours: float, tz: timezone) -> Tuple[int, int, str, str]:
    h = max(0.0, float(hours))
    now_local = datetime.now(tz)
    start_local = now_local - timedelta(hours=h)
    start_ms = int(start_local.astimezone(UTC).timestamp() * 1000)
    end_ms = int(now_local.astimezone(UTC).timestamp() * 1000)
    return start_ms, end_ms, now_local.strftime("%Y-%m-%d"), str(now_local.tzinfo)


def _is_stop_like_reason(reason: str) -> bool:
    r = str(reason or "").strip().lower()
    if not r:
        return False
    if "tp" in r:
        return False
    if "stop" in r:
        return True
    if "sl" in r:
        return True
    if r.endswith("_exit") and "external" not in r:
        return False
    return False


def _extract_entry_exec_modes(msg: str) -> List[str]:
    m = re.search(r"entry_exec=([^\s]+)", str(msg or ""))
    if not m:
        return []
    raw = str(m.group(1) or "").strip().lower()
    if not raw:
        return []
    out: List[str] = []
    for item in raw.split(","):
        seg = str(item or "").strip().lower()
        if not seg:
            continue
        if ":" in seg:
            seg = seg.split(":", 1)[1].strip().lower()
        if seg not in {"market", "limit", "limit_fallback_market"}:
            seg = "other"
        out.append(seg)
    return out


def _entry_exec_stats(runtime: Dict[str, Any]) -> Dict[str, Any]:
    entry_exec = runtime.get("entry_exec_counter") or {}
    total = int(runtime.get("entry_exec_legs", 0) or 0)
    actions = int(runtime.get("entry_exec_open_actions", 0) or 0)
    market = int(entry_exec.get("market", 0) or 0)
    limit_fill = int(entry_exec.get("limit", 0) or 0)
    fallback_market = int(entry_exec.get("limit_fallback_market", 0) or 0)
    other = int(entry_exec.get("other", 0) or 0)
    limit_attempts = int(limit_fill + fallback_market)
    limit_fill_ratio = float(limit_fill / limit_attempts) if limit_attempts > 0 else 0.0
    fallback_ratio = float(fallback_market / limit_attempts) if limit_attempts > 0 else 0.0
    return {
        "total": total,
        "actions": actions,
        "market": market,
        "limit_fill": limit_fill,
        "fallback_market": fallback_market,
        "other": other,
        "limit_attempts": limit_attempts,
        "limit_fill_ratio": limit_fill_ratio,
        "fallback_ratio": fallback_ratio,
    }


def _ts_ms_to_utc_text(ts_ms: int) -> str:
    if int(ts_ms) <= 0:
        return ""
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _summarize_side_concurrency(
    events: List[Tuple[int, int, str, str, str, str]],
    *,
    start_ms: int,
    end_ms: int,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "max_long": 0,
        "max_short": 0,
        "max_same_side": 0,
        "max_total": 0,
        "max_long_ts_ms": 0,
        "max_short_ts_ms": 0,
        "max_total_ts_ms": 0,
        "max_long_insts": [],
        "max_short_insts": [],
        "max_total_insts": [],
        "window_start_active_long": 0,
        "window_start_active_short": 0,
        "window_start_active_total": 0,
        "events_in_window": 0,
    }
    if not events:
        return out

    active: Dict[str, Tuple[str, str]] = {}
    active_long = 0
    active_short = 0

    def _apply_event(event_type: str, trade_id: str, side: str, inst_id: str) -> None:
        nonlocal active_long, active_short
        side_n = str(side or "").strip().lower()
        if event_type == "OPEN":
            old = active.get(trade_id)
            if old is not None:
                old_side = old[0]
                if old_side == "long":
                    active_long = max(0, active_long - 1)
                elif old_side == "short":
                    active_short = max(0, active_short - 1)
            active[trade_id] = (side_n, str(inst_id or "").strip().upper())
            if side_n == "long":
                active_long += 1
            elif side_n == "short":
                active_short += 1
            return

        old = active.pop(trade_id, None)
        if old is None:
            return
        old_side = old[0]
        if old_side == "long":
            active_long = max(0, active_long - 1)
        elif old_side == "short":
            active_short = max(0, active_short - 1)

    def _snapshot(ts_ms: int) -> None:
        total = active_long + active_short
        if active_long > int(out["max_long"]):
            out["max_long"] = int(active_long)
            out["max_long_ts_ms"] = int(ts_ms)
            out["max_long_insts"] = sorted({inst for side, inst in active.values() if side == "long" and inst})
        if active_short > int(out["max_short"]):
            out["max_short"] = int(active_short)
            out["max_short_ts_ms"] = int(ts_ms)
            out["max_short_insts"] = sorted({inst for side, inst in active.values() if side == "short" and inst})
        if total > int(out["max_total"]):
            out["max_total"] = int(total)
            out["max_total_ts_ms"] = int(ts_ms)
            out["max_total_insts"] = sorted({inst for _, inst in active.values() if inst})
        out["max_same_side"] = max(int(out["max_long"]), int(out["max_short"]))

    ev_sorted = sorted(events, key=lambda x: (x[0], x[1]))
    idx = 0
    n = len(ev_sorted)
    while idx < n and int(ev_sorted[idx][0]) < int(start_ms):
        ts, _, et, tid, side, inst = ev_sorted[idx]
        _apply_event(str(et), str(tid), str(side), str(inst))
        idx += 1

    out["window_start_active_long"] = int(active_long)
    out["window_start_active_short"] = int(active_short)
    out["window_start_active_total"] = int(active_long + active_short)
    _snapshot(int(start_ms))

    while idx < n:
        ts, _, et, tid, side, inst = ev_sorted[idx]
        if int(ts) >= int(end_ms):
            break
        out["events_in_window"] = int(out["events_in_window"]) + 1
        _apply_event(str(et), str(tid), str(side), str(inst))
        _snapshot(int(ts))
        idx += 1

    return out


def _build_batch_stats(
    batch_map: Dict[str, Dict[str, Any]],
    per_trade: Dict[str, TradeSummary],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "batch_count": 0,
        "closed_batch_count": 0,
        "unresolved_batch_count": 0,
        "win_batch_count": 0,
        "loss_batch_count": 0,
        "breakeven_batch_count": 0,
        "batch_realized_pnl": Decimal("0"),
        "max_loss_streak": 0,
        "max_win_streak": 0,
        "current_loss_streak": 0,
        "current_win_streak": 0,
        "max_open_count_in_batch": 0,
        "largest_win_batch": None,
        "largest_loss_batch": None,
    }
    if not batch_map:
        return out

    batches: List[Dict[str, Any]] = []
    for key, b in batch_map.items():
        trade_ids = sorted(list(b.get("trade_ids", set()) or set()))
        if not trade_ids:
            continue
        closed = [per_trade[tid] for tid in trade_ids if tid in per_trade]
        pnl = sum((t.pnl for t in closed), Decimal("0"))
        closed_count = len(closed)
        unresolved_count = max(0, len(trade_ids) - closed_count)
        row: Dict[str, Any] = {
            "batch_key": key,
            "signal_ts_ms": int(b.get("signal_ts_ms") or 0),
            "signal_ts_utc": _ts_ms_to_utc_text(int(b.get("signal_ts_ms") or 0)),
            "side": str(b.get("side") or ""),
            "open_count": int(b.get("open_count") or len(trade_ids)),
            "closed_count": int(closed_count),
            "unresolved_count": int(unresolved_count),
            "pnl": pnl,
            "inst_ids": sorted(list(b.get("inst_ids", set()) or set())),
            "last_close_ts_ms": max((int(t.last_ts_ms) for t in closed), default=0),
        }
        batches.append(row)

    if not batches:
        return out

    out["batch_count"] = len(batches)
    out["closed_batch_count"] = sum(1 for b in batches if int(b["closed_count"]) > 0)
    out["unresolved_batch_count"] = sum(1 for b in batches if int(b["closed_count"]) == 0)
    out["max_open_count_in_batch"] = max(int(b["open_count"]) for b in batches)

    closed_batches = [b for b in batches if int(b["closed_count"]) > 0]
    if not closed_batches:
        return out

    out["batch_realized_pnl"] = sum((b["pnl"] for b in closed_batches), Decimal("0"))
    out["win_batch_count"] = sum(1 for b in closed_batches if b["pnl"] > 0)
    out["loss_batch_count"] = sum(1 for b in closed_batches if b["pnl"] < 0)
    out["breakeven_batch_count"] = sum(1 for b in closed_batches if b["pnl"] == 0)

    sorted_closed = sorted(closed_batches, key=lambda x: (int(x["last_close_ts_ms"]), int(x["signal_ts_ms"])))
    max_loss = 0
    max_win = 0
    cur_loss = 0
    cur_win = 0
    for b in sorted_closed:
        pnl = b["pnl"]
        if pnl < 0:
            cur_loss += 1
            cur_win = 0
        elif pnl > 0:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss = 0
            cur_win = 0
        max_loss = max(max_loss, cur_loss)
        max_win = max(max_win, cur_win)
    out["max_loss_streak"] = int(max_loss)
    out["max_win_streak"] = int(max_win)

    loss_streak = 0
    for b in reversed(sorted_closed):
        if b["pnl"] < 0:
            loss_streak += 1
        else:
            break
    out["current_loss_streak"] = int(loss_streak)

    win_streak = 0
    for b in reversed(sorted_closed):
        if b["pnl"] > 0:
            win_streak += 1
        else:
            break
    out["current_win_streak"] = int(win_streak)

    best = max(closed_batches, key=lambda x: x["pnl"])
    worst = min(closed_batches, key=lambda x: x["pnl"])
    out["largest_win_batch"] = {
        "signal_ts_utc": str(best["signal_ts_utc"]),
        "side": str(best["side"]),
        "pnl": best["pnl"],
        "open_count": int(best["open_count"]),
        "closed_count": int(best["closed_count"]),
        "inst_ids": list(best["inst_ids"]),
    }
    out["largest_loss_batch"] = {
        "signal_ts_utc": str(worst["signal_ts_utc"]),
        "side": str(worst["side"]),
        "pnl": worst["pnl"],
        "open_count": int(worst["open_count"]),
        "closed_count": int(worst["closed_count"]),
        "inst_ids": list(worst["inst_ids"]),
    }
    return out


def summarize_trade_journal(
    path: Path,
    *,
    start_ms: int,
    end_ms: int,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "exists": path.exists(),
        "path": str(path),
        "row_count": 0,
        "close_row_count": 0,
        "realized_pnl": Decimal("0"),
        "reason_counter": Counter(),
        "inst_counter": Counter(),
        "event_counter": Counter(),
        "trade_summaries": [],
        "current_loss_streak": 0,
        "current_win_streak": 0,
        "current_stop_like_streak": 0,
        "max_loss_streak": 0,
        "max_win_streak": 0,
        "batch_stats": _build_batch_stats({}, {}),
        "side_concurrency": _summarize_side_concurrency([], start_ms=start_ms, end_ms=end_ms),
    }
    if not path.exists():
        return out

    per_trade: Dict[str, TradeSummary] = {}
    close_events: List[Tuple[int, Decimal, str, str]] = []
    batch_map: Dict[str, Dict[str, Any]] = {}
    concurrency_events: List[Tuple[int, int, str, str, str, str]] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = _to_int(row.get("event_ts_ms"))
            if ts <= 0:
                continue
            event_type = str(row.get("event_type", "")).strip().upper()
            trade_id = str(row.get("trade_id", "")).strip() or str(row.get("entry_ord_id", "")).strip()
            inst_id = str(row.get("inst_id", "")).strip().upper()
            side = str(row.get("side", "")).strip().lower()

            if trade_id and ts < end_ms and event_type in {"OPEN", "CLOSE", "EXTERNAL_CLOSE"}:
                # Close first at same ts to avoid transient over-count in same-bar close+open sequences.
                order = 0 if event_type in {"CLOSE", "EXTERNAL_CLOSE"} else 1
                concurrency_events.append((int(ts), int(order), event_type, trade_id, side, inst_id))

            if ts < start_ms or ts >= end_ms:
                continue

            out["row_count"] += 1
            out["event_counter"][event_type] += 1

            if event_type == "OPEN" and trade_id:
                signal_ts = _to_int(row.get("signal_ts_ms"), default=0)
                if signal_ts <= 0:
                    signal_ts = int(ts)
                key_side = side or "unknown"
                batch_key = f"{int(signal_ts)}|{key_side}"
                b = batch_map.get(batch_key)
                if b is None:
                    b = {
                        "signal_ts_ms": int(signal_ts),
                        "side": key_side,
                        "open_count": 0,
                        "trade_ids": set(),
                        "inst_ids": set(),
                    }
                    batch_map[batch_key] = b
                b["open_count"] = int(b["open_count"]) + 1
                b["trade_ids"].add(trade_id)
                if inst_id:
                    b["inst_ids"].add(inst_id)

            if event_type not in _CLOSE_TYPES:
                continue

            out["close_row_count"] += 1
            reason = str(row.get("reason", "") or "unknown").strip() or "unknown"
            pnl = _to_decimal(row.get("pnl_usdt"))

            out["realized_pnl"] += pnl
            out["reason_counter"][reason] += 1
            if inst_id:
                out["inst_counter"][inst_id] += 1

            if trade_id:
                old = per_trade.get(trade_id)
                if old is None:
                    old = TradeSummary(
                        trade_id=trade_id,
                        inst_id=inst_id,
                        side=side,
                        pnl=Decimal("0"),
                        close_count=0,
                        first_ts_ms=ts,
                        last_ts_ms=ts,
                        reasons=Counter(),
                    )
                    per_trade[trade_id] = old
                old.pnl += pnl
                old.close_count += 1
                old.last_ts_ms = max(old.last_ts_ms, ts)
                old.first_ts_ms = min(old.first_ts_ms, ts)
                if inst_id and not old.inst_id:
                    old.inst_id = inst_id
                if side and not old.side:
                    old.side = side
                old.reasons[reason] += 1

            close_events.append((ts, pnl, reason, trade_id))

    trades = sorted(per_trade.values(), key=lambda x: x.last_ts_ms)
    out["trade_summaries"] = trades

    max_loss = 0
    max_win = 0
    cur_loss = 0
    cur_win = 0
    for t in trades:
        if t.pnl < 0:
            cur_loss += 1
            cur_win = 0
        elif t.pnl > 0:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss = 0
            cur_win = 0
        if cur_loss > max_loss:
            max_loss = cur_loss
        if cur_win > max_win:
            max_win = cur_win
    out["max_loss_streak"] = max_loss
    out["max_win_streak"] = max_win

    rev = list(reversed(trades))
    loss_streak = 0
    for t in rev:
        if t.pnl < 0:
            loss_streak += 1
        else:
            break
    out["current_loss_streak"] = loss_streak

    win_streak = 0
    for t in rev:
        if t.pnl > 0:
            win_streak += 1
        else:
            break
    out["current_win_streak"] = win_streak

    close_events.sort(key=lambda x: x[0])
    stop_streak = 0
    for _, _, reason, _ in reversed(close_events):
        if _is_stop_like_reason(reason):
            stop_streak += 1
        else:
            break
    out["current_stop_like_streak"] = stop_streak
    out["batch_stats"] = _build_batch_stats(batch_map, per_trade)
    out["side_concurrency"] = _summarize_side_concurrency(concurrency_events, start_ms=start_ms, end_ms=end_ms)
    return out


def summarize_runtime_log(
    path: Path,
    *,
    start_ms: int,
    end_ms: int,
    tz: timezone,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "exists": path.exists(),
        "path": str(path),
        "lines": 0,
        "warn": 0,
        "error": 0,
        "heartbeat": 0,
        "instrument_loop_error": 0,
        "sl_sync_failed": 0,
        "risk_guard_block": 0,
        "no_open_alert": 0,
        "entry_exec_counter": Counter(),
        "entry_exec_open_actions": 0,
        "entry_exec_legs": 0,
        "heartbeat_totals": {"processed": 0, "no_new": 0, "stale": 0, "safety_skip": 0, "no_data": 0, "error": 0},
        "last_heartbeat": "",
    }
    if not path.exists():
        return out

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")
            m = _LOG_LINE_RE.match(line)
            if not m:
                continue
            ts_text = m.group("ts")
            level = str(m.group("level") or "INFO").upper()
            msg = str(m.group("msg") or "")
            try:
                ts_local = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
                ts_ms = int(ts_local.astimezone(UTC).timestamp() * 1000)
            except Exception:
                continue
            if ts_ms < start_ms or ts_ms >= end_ms:
                continue

            out["lines"] += 1
            if level == "WARN":
                out["warn"] += 1
            elif level == "ERROR":
                out["error"] += 1

            if "Heartbeat |" in msg:
                out["heartbeat"] += 1
                out["last_heartbeat"] = msg
                for key in out["heartbeat_totals"].keys():
                    mm = re.search(rf"{key}=([0-9]+)", msg)
                    if mm:
                        out["heartbeat_totals"][key] += int(mm.group(1))
            if "Instrument loop error" in msg:
                out["instrument_loop_error"] += 1
            if "Exchange SL sync failed" in msg:
                out["sl_sync_failed"] += 1
            if "Risk guard blocked order" in msg:
                out["risk_guard_block"] += 1
            if "No-open timeout alert sent" in msg or "无开仓超时" in msg:
                out["no_open_alert"] += 1
            if "Action: OPEN " in msg:
                modes = _extract_entry_exec_modes(msg)
                if modes:
                    out["entry_exec_open_actions"] += 1
                    out["entry_exec_legs"] += len(modes)
                    for mode in modes:
                        out["entry_exec_counter"][str(mode)] += 1
    return out


def _trade_outcome_counts(trades: Iterable[TradeSummary]) -> Dict[str, int]:
    win = 0
    loss = 0
    breakeven = 0
    for t in trades:
        if t.pnl > 0:
            win += 1
        elif t.pnl < 0:
            loss += 1
        else:
            breakeven += 1
    total = win + loss + breakeven
    return {"total": total, "win": win, "loss": loss, "breakeven": breakeven}


def _top_trades(trades: Iterable[TradeSummary], n: int = 5) -> Tuple[List[TradeSummary], List[TradeSummary]]:
    arr = list(trades)
    winners = [x for x in sorted(arr, key=lambda t: t.pnl, reverse=True) if x.pnl > 0][:n]
    losers = [x for x in sorted(arr, key=lambda t: t.pnl) if x.pnl < 0][:n]
    return winners, losers


def _summarize_bills(
    cfg: Any,
    *,
    start_ms: int,
    end_ms: int,
    inst_ids: List[str],
    trade_clord_prefix: str,
    bills_max_pages: int,
) -> Dict[str, Any]:
    if (
        fetch_bills is None
        or summarize_bills is None
        or load_trade_order_link_index is None
        or load_trade_journal_close_by_trade_id is None
        or summarize_selected_trade_rows_by_trade_id is None
    ):
        raise RuntimeError("reconcile_okx_bills helpers not available")

    from okx_trader.okx_client import OKXClient

    rows = fetch_bills(
        OKXClient(cfg),
        inst_type="SWAP",
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids,
        endpoint="/api/v5/account/bills",
        limit=100,
        max_pages=max(1, int(bills_max_pages)),
    )
    link_index = load_trade_order_link_index(
        cfg.trade_order_link_path,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids,
    )
    s = summarize_bills(
        rows,
        trade_clord_prefix=trade_clord_prefix,
        trade_filter_mode="merge",
        allowed_ord_ids=link_index.get("ord_ids", set()),
        allowed_clord_ids=link_index.get("cl_ord_ids", set()),
        inst_ids_scope=inst_ids,
        funding_scope="matched-trade-inst",
    )
    journal_by_trade_id = load_trade_journal_close_by_trade_id(
        cfg.trade_journal_path,
        start_ms=start_ms,
        end_ms=end_ms,
        inst_ids=inst_ids,
    )
    by_trade = summarize_selected_trade_rows_by_trade_id(
        s.get("selected_trade_rows_data", []),
        link_index=link_index,
        journal_by_trade_id=journal_by_trade_id,
    )
    return {
        "raw_rows": int(s.get("raw_rows", 0)),
        "selected_trade_rows": int(s.get("selected_trade_rows", 0)),
        "selected_funding_rows": int(s.get("selected_funding_rows", 0)),
        "trade_net": _to_decimal(s.get("trade_net")),
        "funding_bal": _to_decimal(s.get("funding_bal")),
        "recommended_net": _to_decimal(s.get("recommended_net")),
        "mapped_rows": int(by_trade.get("mapped_rows", 0)),
        "ambiguous_rows": int(by_trade.get("ambiguous_rows", 0)),
        "unmapped_rows": int(by_trade.get("unmapped_rows", 0)),
        "mapped_net": _to_decimal(by_trade.get("mapped_net")),
        "journal_only_trade_ids": len(by_trade.get("journal_only_trade_ids", set()) or set()),
    }


def _summarize_exchange_positions_history(
    cfg: Any,
    *,
    start_ms: int,
    end_ms: int,
    inst_ids: List[str],
) -> Dict[str, Any]:
    from okx_trader.okx_client import OKXClient

    want = {x.upper() for x in (inst_ids or [])}
    rows = (
        OKXClient(cfg)
        ._request(
            "GET",
            "/api/v5/account/positions-history",
            params={"instType": "SWAP", "state": "filled", "limit": "100"},
            private=True,
        )
        .get("data", [])
        or []
    )
    picked: List[Dict[str, Any]] = []
    for row in rows:
        inst = str(row.get("instId", "")).strip().upper()
        if want and inst and inst not in want:
            continue
        u_time = _to_int(row.get("uTime"))
        if u_time <= 0 or u_time < start_ms or u_time >= end_ms:
            continue
        picked.append(
            {
                "inst_id": inst,
                "side": str(row.get("direction", "")).strip().lower(),
                "mgn_mode": str(row.get("mgnMode", "")).strip().lower(),
                "lever": str(row.get("lever", "")).strip(),
                "open_avg_px": _to_decimal(row.get("openAvgPx")),
                "close_avg_px": _to_decimal(row.get("closeAvgPx")),
                "open_max_pos": _to_decimal(row.get("openMaxPos")),
                "close_total_pos": _to_decimal(row.get("closeTotalPos")),
                "realized_pnl": _to_decimal(row.get("realizedPnl")),
                "pnl_ratio": _to_decimal(row.get("pnlRatio")),
                "u_time": u_time,
                "c_time": _to_int(row.get("cTime")),
                "pos_id": str(row.get("posId", "") or "").strip(),
            }
        )

    picked.sort(key=lambda x: int(x.get("u_time", 0)))
    win = 0
    loss = 0
    breakeven = 0
    cur_loss = 0
    max_loss = 0
    pnl_sum = Decimal("0")
    for row in picked:
        pnl = _to_decimal(row.get("realized_pnl"))
        pnl_sum += pnl
        if pnl > 0:
            win += 1
            cur_loss = 0
        elif pnl < 0:
            loss += 1
            cur_loss += 1
            if cur_loss > max_loss:
                max_loss = cur_loss
        else:
            breakeven += 1
            cur_loss = 0
    tail_loss = 0
    for row in reversed(picked):
        if _to_decimal(row.get("realized_pnl")) < 0:
            tail_loss += 1
        else:
            break

    top_losses = sorted([x for x in picked if _to_decimal(x.get("realized_pnl")) < 0], key=lambda x: x["realized_pnl"])[:5]
    return {
        "rows": len(picked),
        "win": win,
        "loss": loss,
        "breakeven": breakeven,
        "realized_pnl_sum": pnl_sum,
        "current_loss_streak": tail_loss,
        "max_loss_streak": max_loss,
        "top_losses": top_losses,
    }


def _summarize_equity(cfg: Any) -> Dict[str, Any]:
    from okx_trader.okx_client import OKXClient

    out: Dict[str, Any] = {
        "ok": False,
        "equity": None,
        "base_equity": _to_decimal(getattr(cfg, "compound_base_equity", 0.0)),
    }
    try:
        equity = OKXClient(cfg).get_account_equity(force_refresh=True)
    except Exception as e:  # pragma: no cover - defensive
        out["error"] = str(e)
        return out
    if equity is None or float(equity) <= 0:
        out["error"] = "equity_unavailable"
        return out
    out["ok"] = True
    out["equity"] = _to_decimal(equity)
    return out


def _build_bills_mapping_quality(
    report: Dict[str, Any],
    *,
    max_unmapped_ratio: float = _BILLS_UNMAPPED_MAX_RATIO_DEFAULT,
    alert_unmapped_ratio: float = _BILLS_UNMAPPED_ALERT_RATIO_DEFAULT,
    alert_min_selected_rows: int = _BILLS_ALERT_MIN_SELECTED_DEFAULT,
) -> Dict[str, Any]:
    bills = report.get("bills")
    if not bills:
        return {
            "enabled": False,
            "ok": True,
            "status": "disabled",
            "hard_alert": False,
            "selected_rows": 0,
            "mapped_rows": 0,
            "unmapped_rows": 0,
            "ambiguous_rows": 0,
            "mapped_ratio": 0.0,
            "unmapped_ratio": 0.0,
            "max_unmapped_ratio": float(max_unmapped_ratio),
            "alert_unmapped_ratio": float(alert_unmapped_ratio),
            "alert_min_selected_rows": max(0, int(alert_min_selected_rows)),
            "net_note": "bills_not_enabled",
        }

    selected = max(0, int(bills.get("selected_trade_rows", 0) or 0))
    mapped = max(0, int(bills.get("mapped_rows", 0) or 0))
    unmapped = max(0, int(bills.get("unmapped_rows", 0) or 0))
    ambiguous = max(0, int(bills.get("ambiguous_rows", 0) or 0))
    max_unmapped_ratio = max(0.0, min(1.0, float(max_unmapped_ratio)))
    alert_unmapped_ratio = max(0.0, min(1.0, float(alert_unmapped_ratio)))
    alert_min_selected_rows = max(0, int(alert_min_selected_rows))

    ratio_base = float(max(1, selected))
    mapped_ratio = float(mapped) / ratio_base
    unmapped_ratio = float(unmapped) / ratio_base

    net_note = "ok"
    if selected <= 0:
        net_note = "bills_no_selected_rows"
    elif unmapped_ratio > max_unmapped_ratio:
        net_note = f"bills_unmapped_ratio_high({unmapped_ratio:.2%})"
    elif mapped <= 0:
        net_note = "bills_no_mapped_rows"
    elif ambiguous > 0:
        net_note = f"bills_ambiguous_rows({ambiguous})"

    ok = net_note == "ok"
    hard_alert = False
    if selected >= alert_min_selected_rows:
        if unmapped_ratio >= alert_unmapped_ratio or mapped <= 0 or ambiguous > 0:
            hard_alert = True

    status = "ok"
    if not ok:
        status = "alert" if hard_alert else "warn"

    return {
        "enabled": True,
        "ok": ok,
        "status": status,
        "hard_alert": hard_alert,
        "selected_rows": selected,
        "mapped_rows": mapped,
        "unmapped_rows": unmapped,
        "ambiguous_rows": ambiguous,
        "mapped_ratio": mapped_ratio,
        "unmapped_ratio": unmapped_ratio,
        "max_unmapped_ratio": max_unmapped_ratio,
        "alert_unmapped_ratio": alert_unmapped_ratio,
        "alert_min_selected_rows": alert_min_selected_rows,
        "net_note": net_note,
    }


def _normalize_primary_source(raw: Any) -> str:
    mode = str(raw or "bills_auto").strip().lower()
    if mode not in {"bills_auto", "journal", "exchange_first"}:
        mode = "bills_auto"
    return mode


def _resolve_outcomes(report: Dict[str, Any]) -> Tuple[Dict[str, int], str]:
    mode = _normalize_primary_source(report.get("primary_source", "bills_auto"))
    exch = report.get("exchange_positions")
    if mode == "exchange_first" and isinstance(exch, dict) and int(exch.get("rows", 0)) > 0:
        return (
            {
                "total": int(exch.get("rows", 0)),
                "win": int(exch.get("win", 0)),
                "loss": int(exch.get("loss", 0)),
                "breakeven": int(exch.get("breakeven", 0)),
            },
            "exchange",
        )
    journal = report.get("journal") or {}
    trades = journal.get("trade_summaries") or []
    return _trade_outcome_counts(trades), "journal"


def _resolve_streak_summary(report: Dict[str, Any]) -> Dict[str, Any]:
    mode = _normalize_primary_source(report.get("primary_source", "bills_auto"))
    journal = report.get("journal") or {}
    exch = report.get("exchange_positions") or {}
    if mode == "exchange_first" and int(exch.get("rows", 0)) > 0:
        return {
            "source": "exchange",
            "current_loss_streak": int(exch.get("current_loss_streak", 0)),
            "max_loss_streak": int(exch.get("max_loss_streak", 0)),
            "current_win_streak": None,
            "max_win_streak": None,
        }
    return {
        "source": "journal",
        "current_loss_streak": int(journal.get("current_loss_streak", 0)),
        "max_loss_streak": int(journal.get("max_loss_streak", 0)),
        "current_win_streak": int(journal.get("current_win_streak", 0)),
        "max_win_streak": int(journal.get("max_win_streak", 0)),
    }


def _resolve_net_pnl(report: Dict[str, Any]) -> Tuple[Decimal, str, str]:
    mode = _normalize_primary_source(report.get("primary_source", "bills_auto"))
    journal_val = _to_decimal(report["journal"].get("realized_pnl"))
    if mode == "journal":
        return journal_val, "journal", "forced_journal"
    if mode == "exchange_first":
        exch = report.get("exchange_positions")
        if isinstance(exch, dict) and int(exch.get("rows", 0)) > 0:
            return _to_decimal(exch.get("realized_pnl_sum")), "exchange", "ok"
        return journal_val, "journal", "exchange_unavailable_fallback_journal"

    bills = report.get("bills")
    if not bills:
        return journal_val, "journal", "bills_not_enabled"

    quality = report.get("bills_quality")
    if not isinstance(quality, dict):
        quality = _build_bills_mapping_quality(report)
    if not bool(quality.get("ok", False)):
        return journal_val, "journal", str(quality.get("net_note") or "bills_mapping_quality_bad")
    return _to_decimal(bills.get("recommended_net")), "bills", "ok"


def _load_equity_snapshots(path: Path) -> List[Tuple[int, Decimal]]:
    out: List[Tuple[int, Decimal]] = []
    if not path.exists():
        return out
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                ts = _to_int(row.get("ts_ms"))
                eq = _to_decimal(row.get("equity"))
                if ts > 0 and eq > 0:
                    out.append((ts, eq))
    except Exception:
        return []
    out.sort(key=lambda x: x[0])
    return out


def _load_equity_snapshots_from_reports(dir_path: Path) -> List[Tuple[int, Decimal]]:
    out: List[Tuple[int, Decimal]] = []
    if not dir_path.exists() or not dir_path.is_dir():
        return out
    for p in sorted(dir_path.glob("*.json")):
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
            eq_obj = obj.get("equity") if isinstance(obj, dict) else None
            if not isinstance(eq_obj, dict):
                continue
            eq = _to_decimal(eq_obj.get("equity"))
            if eq <= 0:
                continue
            end_utc = str(obj.get("window_end_utc", "") or "").strip()
            if not end_utc.endswith("UTC"):
                continue
            ts_text = end_utc.replace(" UTC", "")
            dt_utc = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
            ts_ms = int(dt_utc.timestamp() * 1000)
            if ts_ms > 0:
                out.append((ts_ms, eq))
        except Exception:
            continue
    out.sort(key=lambda x: x[0])
    return out


def _append_equity_snapshot(path: Path, *, ts_ms: int, equity: Decimal) -> None:
    if ts_ms <= 0 or equity <= 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        fieldnames = ["ts_ms", "equity"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({"ts_ms": int(ts_ms), "equity": str(equity)})


def _summarize_equity_delta(
    *,
    start_ms: int,
    end_ms: int,
    current_equity: Decimal,
    snapshot_path: Path,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "available": False,
        "snapshot_path": str(snapshot_path),
    }
    if current_equity <= 0:
        out["reason"] = "current_equity_unavailable"
        return out

    snap_map: Dict[int, Decimal] = {}
    for ts, eq in _load_equity_snapshots(snapshot_path):
        snap_map[int(ts)] = eq
    for ts, eq in _load_equity_snapshots_from_reports(snapshot_path.parent):
        snap_map[int(ts)] = eq
    snaps = sorted(snap_map.items(), key=lambda x: x[0])
    start_snap: Optional[Tuple[int, Decimal]] = None

    le = [x for x in snaps if x[0] <= start_ms]
    if le:
        start_snap = le[-1]
        out["start_source"] = "snapshot_le_start"
    else:
        ge = [x for x in snaps if x[0] >= start_ms]
        if ge:
            start_snap = ge[0]
            out["start_source"] = "snapshot_ge_start"

    if start_snap is None:
        out["reason"] = "no_start_snapshot"
        _append_equity_snapshot(snapshot_path, ts_ms=end_ms, equity=current_equity)
        return out

    start_ts, start_eq = start_snap
    window_ms = max(1, int(end_ms - start_ms))
    max_gap_ms = max(6 * 60 * 60 * 1000, int(window_ms * 0.6))
    if abs(int(start_ts) - int(start_ms)) > max_gap_ms:
        out["reason"] = "start_snapshot_too_far"
        out["start_ts_ms"] = int(start_ts)
        out["start_equity"] = start_eq
        _append_equity_snapshot(snapshot_path, ts_ms=end_ms, equity=current_equity)
        return out

    delta = current_equity - start_eq
    delta_pct = (delta / start_eq * Decimal("100")) if start_eq > 0 else Decimal("0")
    out.update(
        {
            "available": True,
            "start_ts_ms": int(start_ts),
            "start_equity": start_eq,
            "end_ts_ms": int(end_ms),
            "end_equity": current_equity,
            "delta_usdt": delta,
            "delta_pct": delta_pct,
        }
    )
    _append_equity_snapshot(snapshot_path, ts_ms=end_ms, equity=current_equity)
    return out


def _build_md_report(report: Dict[str, Any]) -> str:
    journal = report["journal"]
    runtime = report["runtime"]
    outcomes = report["outcomes"]
    outcomes_source = str(report.get("outcomes_source", "journal") or "journal")
    streaks = _resolve_streak_summary(report)
    winners: List[TradeSummary] = report["winners"]
    losers: List[TradeSummary] = report["losers"]
    open_count = int(journal["event_counter"].get("OPEN", 0))
    net_pnl, net_src, net_note = _resolve_net_pnl(report)
    bills_quality = report.get("bills_quality") or _build_bills_mapping_quality(report)

    lines: List[str] = []
    lines.append(f"# Daily Recap | {report['date_local']}")
    lines.append("")
    lines.append(f"- 窗口模式: {report.get('window_mode', 'day')}")
    lines.append(f"- 窗口(UTC): {report['window_start_utc']} -> {report['window_end_utc']}")
    lines.append(f"- 标的: {','.join(report['inst_ids']) if report['inst_ids'] else '-'}")
    lines.append(f"- 开仓事件数(OPEN): {open_count}")
    if outcomes_source == "exchange":
        lines.append(
            f"- 平仓交易数(交易所口径): {outcomes['total']} | 胜/负/平: {outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}"
        )
    else:
        lines.append(
            f"- 平仓交易数(按trade_id): {outcomes['total']} | 胜/负/平: {outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}"
        )
    lines.append(
        f"- 已实现PnL(journal close): {_fmt_decimal(journal['realized_pnl'])} USDT | close_rows={journal['close_row_count']}"
    )
    lines.append(f"- 净收益口径: {_fmt_decimal(net_pnl)} USDT ({net_src})")
    if net_note and net_note != "ok":
        lines.append(f"- 净收益口径说明: {net_note}")
    if bills_quality.get("enabled"):
        status = str(bills_quality.get("status", "ok")).upper()
        lines.append(
            f"- 对账质量: {status} | mapped={int(bills_quality.get('mapped_rows', 0))}/"
            f"{int(bills_quality.get('selected_rows', 0))} ({float(bills_quality.get('mapped_ratio', 0.0)):.2%}) "
            f"| unmapped={float(bills_quality.get('unmapped_ratio', 0.0)):.2%} "
            f"| ambiguous={int(bills_quality.get('ambiguous_rows', 0))}"
        )
        lines.append(
            f"- 对账阈值: max_unmapped={float(bills_quality.get('max_unmapped_ratio', 0.0)):.2%} "
            f"| alert_unmapped={float(bills_quality.get('alert_unmapped_ratio', 0.0)):.2%} "
            f"| alert_min_selected={int(bills_quality.get('alert_min_selected_rows', 0))}"
        )
        if bool(bills_quality.get("hard_alert", False)):
            lines.append(f"- 对账告警: {bills_quality.get('net_note', 'bills_mapping_quality_alert')}")
    if str(streaks.get("source")) == "exchange":
        lines.append(
            f"- 当前连亏(交易所口径)={int(streaks.get('current_loss_streak', 0))} | 当前连赢(交易所口径)=N/A | 当前连续stop-like(台账)={journal['current_stop_like_streak']}"
        )
        lines.append(
            f"- 当日最大连亏(交易所口径): {int(streaks.get('max_loss_streak', 0))} | 当日最大连赢(台账): {journal['max_win_streak']}"
        )
    else:
        lines.append(
            f"- 当前连亏={journal['current_loss_streak']} | 当前连赢={journal['current_win_streak']} | 当前连续stop-like={journal['current_stop_like_streak']}"
        )
        lines.append(
            f"- 当日最大连亏(窗口内): {journal['max_loss_streak']} | 当日最大连赢(窗口内): {journal['max_win_streak']}"
        )
    batch_stats = journal.get("batch_stats") or {}
    side_conc = journal.get("side_concurrency") or {}
    lines.append(
        f"- 批次连亏/连赢(当前): {int(batch_stats.get('current_loss_streak', 0))}/{int(batch_stats.get('current_win_streak', 0))}"
    )
    lines.append(
        f"- 批次连亏/连赢(最大): {int(batch_stats.get('max_loss_streak', 0))}/{int(batch_stats.get('max_win_streak', 0))}"
    )
    lines.append(
        f"- 同向并发峰值(long/short/same/total): {int(side_conc.get('max_long', 0))}/{int(side_conc.get('max_short', 0))}/{int(side_conc.get('max_same_side', 0))}/{int(side_conc.get('max_total', 0))}"
    )
    exec_stats = _entry_exec_stats(runtime)
    if int(exec_stats["total"]) > 0:
        lines.append(
            f"- 入场执行(legs): total={int(exec_stats['total'])} actions={int(exec_stats['actions'])} "
            f"market={int(exec_stats['market'])} limit_fill={int(exec_stats['limit_fill'])} "
            f"fallback_market={int(exec_stats['fallback_market'])} "
            f"limit_fill_rate={float(exec_stats['limit_fill_ratio']):.1%} "
            f"fallback_rate={float(exec_stats['fallback_ratio']):.1%}"
        )
    else:
        lines.append("- 入场执行(legs): N/A (窗口内无开仓执行日志)")
    equity = report.get("equity")
    if equity:
        eq_val = equity.get("equity")
        eq_text = _fmt_decimal(_to_decimal(eq_val)) if eq_val is not None else "N/A"
        lines.append(
            f"- 账户权益: {eq_text} USDT | 基准本金(compound_base_equity): {_fmt_decimal(_to_decimal(equity.get('base_equity')))} USDT"
        )
    equity_delta = report.get("equity_delta")
    if equity_delta:
        if bool(equity_delta.get("available", False)):
            lines.append(
                f"- 窗口权益变化: {_fmt_decimal(_to_decimal(equity_delta.get('delta_usdt')))} USDT "
                f"({_fmt_decimal(_to_decimal(equity_delta.get('delta_pct')))}%)"
            )
        else:
            lines.append(f"- 窗口权益变化: N/A ({equity_delta.get('reason', 'unavailable')})")
    lines.append("")

    lines.append("## 批次与并发风险")
    lines.append(
        f"- 批次统计(signal_ts+side): total={int(batch_stats.get('batch_count', 0))} "
        f"closed={int(batch_stats.get('closed_batch_count', 0))} unresolved={int(batch_stats.get('unresolved_batch_count', 0))}"
    )
    lines.append(
        f"- 批次胜负平: {int(batch_stats.get('win_batch_count', 0))}/"
        f"{int(batch_stats.get('loss_batch_count', 0))}/"
        f"{int(batch_stats.get('breakeven_batch_count', 0))} "
        f"| batch_pnl={_fmt_decimal(_to_decimal(batch_stats.get('batch_realized_pnl')))} USDT"
    )
    lines.append(
        f"- 最大同向并发(long/short/same): {int(side_conc.get('max_long', 0))}/"
        f"{int(side_conc.get('max_short', 0))}/{int(side_conc.get('max_same_side', 0))}"
    )
    lines.append(
        f"- 窗口起始活跃仓位(long/short/total): {int(side_conc.get('window_start_active_long', 0))}/"
        f"{int(side_conc.get('window_start_active_short', 0))}/{int(side_conc.get('window_start_active_total', 0))}"
    )
    lines.append(
        f"- short并发峰值时间: {_ts_ms_to_utc_text(int(side_conc.get('max_short_ts_ms', 0)))} | insts="
        f"{','.join(side_conc.get('max_short_insts', []) or []) or '-'}"
    )
    lines.append(
        f"- long并发峰值时间: {_ts_ms_to_utc_text(int(side_conc.get('max_long_ts_ms', 0)))} | insts="
        f"{','.join(side_conc.get('max_long_insts', []) or []) or '-'}"
    )
    lw = batch_stats.get("largest_win_batch")
    ll = batch_stats.get("largest_loss_batch")
    if lw:
        lines.append(
            f"- 最佳批次: {str(lw.get('signal_ts_utc') or '-')} {str(lw.get('side') or '-')} "
            f"pnl={_fmt_decimal(_to_decimal(lw.get('pnl')))} open={int(lw.get('open_count', 0))} "
            f"insts={','.join(lw.get('inst_ids') or []) or '-'}"
        )
    if ll:
        lines.append(
            f"- 最差批次: {str(ll.get('signal_ts_utc') or '-')} {str(ll.get('side') or '-')} "
            f"pnl={_fmt_decimal(_to_decimal(ll.get('pnl')))} open={int(ll.get('open_count', 0))} "
            f"insts={','.join(ll.get('inst_ids') or []) or '-'}"
        )
    lines.append("")

    lines.append("## 平仓原因分布")
    if journal["reason_counter"]:
        for reason, cnt in journal["reason_counter"].most_common():
            lines.append(f"- {reason}: {cnt}")
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Top Winners")
    if winners:
        for t in winners:
            lines.append(
                f"- {t.trade_id} | {t.inst_id} {t.side} | pnl={_fmt_decimal(t.pnl)} | closes={t.close_count}"
            )
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Top Losers")
    if losers:
        for t in losers:
            lines.append(
                f"- {t.trade_id} | {t.inst_id} {t.side} | pnl={_fmt_decimal(t.pnl)} | closes={t.close_count}"
            )
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Runtime Health")
    lines.append(
        f"- lines={runtime['lines']} warn={runtime['warn']} error={runtime['error']} heartbeat={runtime['heartbeat']}"
    )
    lines.append(
        f"- loop_error={runtime['instrument_loop_error']} sl_sync_failed={runtime['sl_sync_failed']} risk_guard_block={runtime['risk_guard_block']} no_open_alert={runtime['no_open_alert']}"
    )
    hb = runtime["heartbeat_totals"]
    lines.append(
        f"- hb_totals processed={hb['processed']} no_new={hb['no_new']} stale={hb['stale']} safety_skip={hb['safety_skip']} no_data={hb['no_data']} error={hb['error']}"
    )
    exec_stats = _entry_exec_stats(runtime)
    if int(exec_stats["total"]) > 0:
        lines.append(
            f"- entry_exec legs={int(exec_stats['total'])} actions={int(exec_stats['actions'])} "
            f"market={int(exec_stats['market'])} limit={int(exec_stats['limit_fill'])} "
            f"fallback_market={int(exec_stats['fallback_market'])} other={int(exec_stats['other'])}"
        )
        lines.append(
            f"- entry_exec ratio: market={int(exec_stats['market'])/max(1, int(exec_stats['total'])):.1%} "
            f"limit={int(exec_stats['limit_fill'])/max(1, int(exec_stats['total'])):.1%} "
            f"fallback_market={int(exec_stats['fallback_market'])/max(1, int(exec_stats['total'])):.1%} "
            f"| limit_fill_rate={float(exec_stats['limit_fill_ratio']):.1%} "
            f"fb_rate={float(exec_stats['fallback_ratio']):.1%}"
        )
    if runtime.get("last_heartbeat"):
        lines.append(f"- last_heartbeat: {runtime['last_heartbeat']}")
    lines.append("")

    bills = report.get("bills")
    if bills:
        lines.append("## Bills Reconcile (optional)")
        lines.append(
            f"- trade_net(pnl+fee)={_fmt_decimal(bills['trade_net'])} | funding_net={_fmt_decimal(bills['funding_bal'])} | recommended_net={_fmt_decimal(bills['recommended_net'])}"
        )
        lines.append(
            f"- selected_trade_rows={bills['selected_trade_rows']} funding_rows={bills['selected_funding_rows']} mapped_rows={bills['mapped_rows']} ambiguous={bills['ambiguous_rows']} unmapped={bills['unmapped_rows']} journal_only_trade_ids={bills['journal_only_trade_ids']}"
        )
        lines.append("")

    exch = report.get("exchange_positions")
    if exch:
        lines.append("## Exchange Closed Positions (optional)")
        lines.append(
            f"- rows={exch['rows']} | 胜/负/平={exch['win']}/{exch['loss']}/{exch['breakeven']} | realized_pnl_sum={_fmt_decimal(exch['realized_pnl_sum'])} USDT"
        )
        lines.append(
            f"- 当前连亏(交易所口径)={exch['current_loss_streak']} | 当日最大连亏(交易所口径)={exch['max_loss_streak']}"
        )
        if exch.get("top_losses"):
            for row in exch["top_losses"]:
                lines.append(
                    f"- top_loss {row['inst_id']} {row['side']} pnl={_fmt_decimal(_to_decimal(row['realized_pnl']))} open={_fmt_decimal(_to_decimal(row['open_avg_px']))} close={_fmt_decimal(_to_decimal(row['close_avg_px']))} pos={_fmt_decimal(_to_decimal(row['close_total_pos']))}"
                )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_rollup_line(report: Dict[str, Any]) -> str:
    journal = report["journal"]
    runtime = report["runtime"]
    outcomes = report["outcomes"]
    outcomes_source = str(report.get("outcomes_source", "journal") or "journal")
    streaks = _resolve_streak_summary(report)
    open_count = int(journal["event_counter"].get("OPEN", 0))
    batch_stats = journal.get("batch_stats") or {}
    side_conc = journal.get("side_concurrency") or {}
    net_pnl, _, _ = _resolve_net_pnl(report)
    bills_quality = report.get("bills_quality") or _build_bills_mapping_quality(report)
    parts = [
        report["date_local"],
        f"mode={report.get('window_mode', 'day')}",
        f"pnl={_fmt_decimal(net_pnl)}",
        f"opens={open_count}",
        f"trades={outcomes['total']}",
        f"w/l/b={outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}",
        f"loss_streak={int(streaks.get('current_loss_streak', 0))}",
        f"batch_loss_streak={int(batch_stats.get('current_loss_streak', 0))}",
        f"side_peak={int(side_conc.get('max_same_side', 0))}",
        f"stop_streak={journal['current_stop_like_streak']}",
        f"warn={runtime['warn']}",
        f"err={runtime['error']}",
    ]
    if outcomes_source != "journal":
        parts.append(f"outcomes={outcomes_source}")
    exec_stats = _entry_exec_stats(runtime)
    parts.append(f"entry_legs={int(exec_stats['total'])}")
    if int(exec_stats["limit_attempts"]) > 0:
        parts.append(f"entry_fb={float(exec_stats['fallback_ratio']):.1%}")
        parts.append(f"entry_limfill={float(exec_stats['limit_fill_ratio']):.1%}")
    exch = report.get("exchange_positions")
    if exch:
        parts.append(f"ex_loss_streak={int(exch.get('current_loss_streak', 0))}")
    if bills_quality.get("enabled"):
        parts.append(f"bills_q={str(bills_quality.get('status', 'ok'))}")
        parts.append(f"bills_unmapped={float(bills_quality.get('unmapped_ratio', 0.0)):.2%}")
    return " | ".join(parts)


def _build_telegram_summary(report: Dict[str, Any]) -> str:
    journal = report["journal"]
    batch_stats = journal.get("batch_stats") or {}
    side_conc = journal.get("side_concurrency") or {}
    outcomes = report["outcomes"]
    outcomes_source = str(report.get("outcomes_source", "journal") or "journal")
    streaks = _resolve_streak_summary(report)
    open_count = int(journal["event_counter"].get("OPEN", 0))
    net_pnl, net_src, net_note = _resolve_net_pnl(report)
    bills_quality = report.get("bills_quality") or _build_bills_mapping_quality(report)
    equity = report.get("equity") or {}
    exch = report.get("exchange_positions") or {}
    equity_delta = report.get("equity_delta") or {}

    eq_val = equity.get("equity")
    eq_text = f"{_fmt_decimal(_to_decimal(eq_val))} USDT" if eq_val is not None else "N/A"
    base_text = f"{_fmt_decimal(_to_decimal(equity.get('base_equity', 0)))} USDT"
    journal_loss = int(journal.get("current_loss_streak", 0))
    journal_win = int(journal.get("current_win_streak", 0))
    exch_loss = int(exch.get("current_loss_streak", 0)) if exch else None

    lines = [
        "【Daily Recap 24h】",
        f"窗口(UTC): {report['window_start_utc']} -> {report['window_end_utc']}",
        f"开仓次数: {open_count}",
        f"平仓交易({outcomes_source}): {outcomes['total']} | 胜/负/平: {outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}",
        f"净收益({net_src}): {_fmt_decimal(net_pnl)} USDT",
        f"当前权益: {eq_text}",
        f"基准本金: {base_text}",
    ]
    if bool(equity_delta.get("available", False)):
        lines.append(
            f"窗口权益变化: {_fmt_decimal(_to_decimal(equity_delta.get('delta_usdt')))} USDT "
            f"({_fmt_decimal(_to_decimal(equity_delta.get('delta_pct')))}%)"
        )
    elif equity_delta:
        lines.append(f"窗口权益变化: N/A ({equity_delta.get('reason', 'unavailable')})")
    if str(streaks.get("source")) == "exchange":
        lines.append(f"当前连亏(主口径=交易所): {int(streaks.get('current_loss_streak', 0))}")
    elif exch_loss is not None:
        lines.append(f"当前连亏(交易所): {exch_loss}")
    lines.append(f"当前连亏/连赢(台账): {journal_loss}/{journal_win}")
    lines.append(
        f"批次连亏/连赢(台账): {int(batch_stats.get('current_loss_streak', 0))}/{int(batch_stats.get('current_win_streak', 0))}"
    )
    lines.append(
        f"同向并发峰值(long/short/same): {int(side_conc.get('max_long', 0))}/{int(side_conc.get('max_short', 0))}/{int(side_conc.get('max_same_side', 0))}"
    )
    runtime = report.get("runtime") or {}
    exec_stats = _entry_exec_stats(runtime)
    if int(exec_stats["total"]) > 0:
        lines.append(
            f"入场执行(runtime): legs={int(exec_stats['total'])} market={int(exec_stats['market'])} "
            f"limit_fill={int(exec_stats['limit_fill'])} "
            f"fallback_market={int(exec_stats['fallback_market'])} "
            f"limit_fill_rate={float(exec_stats['limit_fill_ratio']):.1%} "
            f"fb_rate={float(exec_stats['fallback_ratio']):.1%}"
        )
    if bills_quality.get("enabled"):
        lines.append(
            f"对账质量: {str(bills_quality.get('status', 'ok')).upper()} | "
            f"mapped={int(bills_quality.get('mapped_rows', 0))}/{int(bills_quality.get('selected_rows', 0))} "
            f"({float(bills_quality.get('mapped_ratio', 0.0)):.2%}) | "
            f"unmapped={float(bills_quality.get('unmapped_ratio', 0.0)):.2%}"
        )
        if bool(bills_quality.get("hard_alert", False)):
            lines.append(f"对账告警: {bills_quality.get('net_note', 'bills_mapping_quality_alert')}")
    if net_note and net_note not in {"ok", "bills_not_enabled"}:
        lines.append(f"净收益说明: {net_note}")
    return "\n".join(lines)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Counter):
        return dict(value)
    if isinstance(value, TradeSummary):
        return {
            "trade_id": value.trade_id,
            "inst_id": value.inst_id,
            "side": value.side,
            "pnl": str(value.pnl),
            "close_count": value.close_count,
            "first_ts_ms": value.first_ts_ms,
            "last_ts_ms": value.last_ts_ms,
            "reasons": dict(value.reasons),
        }
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate daily recap from trade_journal + runtime log.")
    parser.add_argument("--env", default="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env")
    parser.add_argument("--date", default="", help="Local date in YYYY-MM-DD. Default: today in --tz-offset.")
    parser.add_argument("--tz-offset", default="+08:00", help="Timezone offset for date/runtime.log parsing.")
    parser.add_argument("--rolling-hours", type=float, default=0.0, help="Use rolling window (hours), e.g. 24.")
    parser.add_argument("--journal-path", default="", help="Override trade_journal.csv path.")
    parser.add_argument("--runtime-log", default="", help="Override runtime.log path.")
    parser.add_argument("--inst-ids", default="", help="Optional comma-separated scope.")
    parser.add_argument(
        "--primary-source",
        default="bills_auto",
        choices=["bills_auto", "journal", "exchange_first"],
        help="Primary source for recap headline metrics.",
    )
    parser.add_argument("--top-n", type=int, default=5)
    parser.add_argument("--with-bills", action="store_true", help="Fetch bills and add net reconcile.")
    parser.add_argument(
        "--with-exchange-history",
        action="store_true",
        help="Fetch exchange positions-history and add exchange closed-trade stats.",
    )
    parser.add_argument("--with-equity", action="store_true", help="Fetch current account equity and include in recap.")
    parser.add_argument("--trade-clord-prefix", default="AT")
    parser.add_argument("--bills-max-pages", type=int, default=120)
    parser.add_argument(
        "--bills-unmapped-max-ratio",
        type=float,
        default=_BILLS_UNMAPPED_MAX_RATIO_DEFAULT,
        help="Bills mapping guard threshold; above this ratio fallback to journal net.",
    )
    parser.add_argument(
        "--bills-alert-unmapped-ratio",
        type=float,
        default=_BILLS_UNMAPPED_ALERT_RATIO_DEFAULT,
        help="Hard alert threshold for unmapped ratio in recap summary.",
    )
    parser.add_argument(
        "--bills-alert-min-selected",
        type=int,
        default=_BILLS_ALERT_MIN_SELECTED_DEFAULT,
        help="Require at least this many selected trade rows before hard alert is raised.",
    )
    parser.add_argument("--equity-snapshot-path", default="", help="CSV path for equity snapshots used in window delta.")
    parser.add_argument("--out-md", default="", help="Write markdown report.")
    parser.add_argument("--out-json", default="", help="Write json report.")
    parser.add_argument("--append-summary", default="", help="Append one-line summary to file.")
    parser.add_argument("--print", action="store_true", help="Print markdown report.")
    parser.add_argument("--telegram", action="store_true", help="Send short summary to Telegram.")
    args = parser.parse_args()

    env_path = Path(args.env).expanduser().resolve()
    load_dotenv(str(env_path))
    cfg = read_config(None)

    tz = _parse_tz_offset(args.tz_offset)
    if float(args.rolling_hours or 0.0) > 0:
        start_ms, end_ms, date_local, tz_name = _resolve_rolling_window(args.rolling_hours, tz)
        window_mode = f"rolling_{float(args.rolling_hours):g}h"
    else:
        start_ms, end_ms, date_local, tz_name = _resolve_day_window(args.date, tz)
        window_mode = "day"
    inst_ids = [x.strip().upper() for x in args.inst_ids.split(",") if x.strip()] if args.inst_ids else list(cfg.inst_ids)

    journal_path = Path(args.journal_path or cfg.trade_journal_path).expanduser()
    runtime_path = Path(args.runtime_log or (Path(env_path).parent / "runtime.log")).expanduser()

    journal = summarize_trade_journal(journal_path, start_ms=start_ms, end_ms=end_ms)
    runtime = summarize_runtime_log(runtime_path, start_ms=start_ms, end_ms=end_ms, tz=tz)
    journal_outcomes = _trade_outcome_counts(journal["trade_summaries"])
    winners, losers = _top_trades(journal["trade_summaries"], n=max(1, int(args.top_n)))

    report: Dict[str, Any] = {
        "date_local": date_local,
        "tz": tz_name,
        "window_mode": window_mode,
        "window_start_utc": datetime.fromtimestamp(start_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "window_end_utc": datetime.fromtimestamp(end_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "inst_ids": inst_ids,
        "primary_source": _normalize_primary_source(args.primary_source),
        "journal": journal,
        "runtime": runtime,
        "journal_outcomes": journal_outcomes,
        "outcomes": journal_outcomes,
        "outcomes_source": "journal",
        "winners": winners,
        "losers": losers,
    }

    if args.with_bills:
        report["bills"] = _summarize_bills(
            cfg,
            start_ms=start_ms,
            end_ms=end_ms,
            inst_ids=inst_ids,
            trade_clord_prefix=args.trade_clord_prefix,
            bills_max_pages=args.bills_max_pages,
        )
    if args.with_exchange_history:
        report["exchange_positions"] = _summarize_exchange_positions_history(
            cfg,
            start_ms=start_ms,
            end_ms=end_ms,
            inst_ids=inst_ids,
        )
    report["outcomes"], report["outcomes_source"] = _resolve_outcomes(report)
    if args.with_equity:
        report["equity"] = _summarize_equity(cfg)
        eq_val = _to_decimal((report.get("equity") or {}).get("equity"))
        if args.equity_snapshot_path:
            snapshot_path = Path(args.equity_snapshot_path).expanduser()
        elif args.append_summary:
            snapshot_path = Path(args.append_summary).expanduser().parent / "equity_snapshots.csv"
        else:
            snapshot_path = Path(env_path).parent / "logs" / "daily_recap" / "equity_snapshots.csv"
        report["equity_delta"] = _summarize_equity_delta(
            start_ms=start_ms,
            end_ms=end_ms,
            current_equity=eq_val,
            snapshot_path=snapshot_path,
        )
    report["bills_quality"] = _build_bills_mapping_quality(
        report,
        max_unmapped_ratio=max(0.0, min(1.0, float(args.bills_unmapped_max_ratio))),
        alert_unmapped_ratio=max(0.0, min(1.0, float(args.bills_alert_unmapped_ratio))),
        alert_min_selected_rows=max(0, int(args.bills_alert_min_selected)),
    )

    md_text = _build_md_report(report)
    rollup_line = _build_rollup_line(report)

    if args.out_md:
        p = Path(args.out_md).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(md_text, encoding="utf-8")
    if args.out_json:
        p = Path(args.out_json).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")
    if args.append_summary:
        p = Path(args.append_summary).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(rollup_line + "\n")
    if args.print or (not args.out_md and not args.out_json):
        print(md_text, end="")
        print(rollup_line)

    if args.telegram:
        tg_text = truncate_text(_build_telegram_summary(report), 3600)
        send_telegram(cfg, tg_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
