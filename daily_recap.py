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
    }
    if not path.exists():
        return out

    per_trade: Dict[str, TradeSummary] = {}
    close_events: List[Tuple[int, Decimal, str, str]] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = _to_int(row.get("event_ts_ms"))
            if ts <= 0 or ts < start_ms or ts >= end_ms:
                continue
            out["row_count"] += 1
            event_type = str(row.get("event_type", "")).strip().upper()
            out["event_counter"][event_type] += 1
            if event_type not in _CLOSE_TYPES:
                continue

            out["close_row_count"] += 1
            reason = str(row.get("reason", "") or "unknown").strip() or "unknown"
            pnl = _to_decimal(row.get("pnl_usdt"))
            inst_id = str(row.get("inst_id", "")).strip().upper()
            side = str(row.get("side", "")).strip().lower()
            trade_id = str(row.get("trade_id", "")).strip() or str(row.get("entry_ord_id", "")).strip()

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


def _resolve_net_pnl(report: Dict[str, Any]) -> Tuple[Decimal, str]:
    bills = report.get("bills")
    if bills:
        return _to_decimal(bills.get("recommended_net")), "bills"
    return _to_decimal(report["journal"].get("realized_pnl")), "journal"


def _build_md_report(report: Dict[str, Any]) -> str:
    journal = report["journal"]
    runtime = report["runtime"]
    outcomes = report["outcomes"]
    winners: List[TradeSummary] = report["winners"]
    losers: List[TradeSummary] = report["losers"]
    open_count = int(journal["event_counter"].get("OPEN", 0))
    net_pnl, net_src = _resolve_net_pnl(report)

    lines: List[str] = []
    lines.append(f"# Daily Recap | {report['date_local']}")
    lines.append("")
    lines.append(f"- 窗口模式: {report.get('window_mode', 'day')}")
    lines.append(f"- 窗口(UTC): {report['window_start_utc']} -> {report['window_end_utc']}")
    lines.append(f"- 标的: {','.join(report['inst_ids']) if report['inst_ids'] else '-'}")
    lines.append(f"- 开仓事件数(OPEN): {open_count}")
    lines.append(
        f"- 平仓交易数(按trade_id): {outcomes['total']} | 胜/负/平: {outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}"
    )
    lines.append(
        f"- 已实现PnL(journal close): {_fmt_decimal(journal['realized_pnl'])} USDT | close_rows={journal['close_row_count']}"
    )
    lines.append(f"- 净收益口径: {_fmt_decimal(net_pnl)} USDT ({net_src})")
    lines.append(
        f"- 当前连亏={journal['current_loss_streak']} | 当前连赢={journal['current_win_streak']} | 当前连续stop-like={journal['current_stop_like_streak']}"
    )
    lines.append(f"- 当日最大连亏(窗口内): {journal['max_loss_streak']} | 当日最大连赢(窗口内): {journal['max_win_streak']}")
    equity = report.get("equity")
    if equity:
        eq_val = equity.get("equity")
        eq_text = _fmt_decimal(_to_decimal(eq_val)) if eq_val is not None else "N/A"
        lines.append(
            f"- 账户权益: {eq_text} USDT | 基准本金(compound_base_equity): {_fmt_decimal(_to_decimal(equity.get('base_equity')))} USDT"
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
    open_count = int(journal["event_counter"].get("OPEN", 0))
    net_pnl, _ = _resolve_net_pnl(report)
    parts = [
        report["date_local"],
        f"mode={report.get('window_mode', 'day')}",
        f"pnl={_fmt_decimal(net_pnl)}",
        f"opens={open_count}",
        f"trades={outcomes['total']}",
        f"w/l/b={outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}",
        f"loss_streak={journal['current_loss_streak']}",
        f"stop_streak={journal['current_stop_like_streak']}",
        f"warn={runtime['warn']}",
        f"err={runtime['error']}",
    ]
    exch = report.get("exchange_positions")
    if exch:
        parts.append(f"ex_loss_streak={int(exch.get('current_loss_streak', 0))}")
    return " | ".join(parts)


def _build_telegram_summary(report: Dict[str, Any]) -> str:
    journal = report["journal"]
    outcomes = report["outcomes"]
    open_count = int(journal["event_counter"].get("OPEN", 0))
    net_pnl, net_src = _resolve_net_pnl(report)
    equity = report.get("equity") or {}
    exch = report.get("exchange_positions") or {}

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
        f"平仓交易: {outcomes['total']} | 胜/负/平: {outcomes['win']}/{outcomes['loss']}/{outcomes['breakeven']}",
        f"净收益({net_src}): {_fmt_decimal(net_pnl)} USDT",
        f"当前权益: {eq_text}",
        f"基准本金: {base_text}",
    ]
    if exch_loss is not None:
        lines.append(f"当前连亏(交易所): {exch_loss}")
    lines.append(f"当前连亏/连赢(台账): {journal_loss}/{journal_win}")
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
    outcomes = _trade_outcome_counts(journal["trade_summaries"])
    winners, losers = _top_trades(journal["trade_summaries"], n=max(1, int(args.top_n)))

    report: Dict[str, Any] = {
        "date_local": date_local,
        "tz": tz_name,
        "window_mode": window_mode,
        "window_start_utc": datetime.fromtimestamp(start_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "window_end_utc": datetime.fromtimestamp(end_ms / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "inst_ids": inst_ids,
        "journal": journal,
        "runtime": runtime,
        "outcomes": outcomes,
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
    if args.with_equity:
        report["equity"] = _summarize_equity(cfg)

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
