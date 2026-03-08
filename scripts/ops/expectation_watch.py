#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from okx_trader.alerts import send_telegram  # noqa: E402
from okx_trader.common import load_dotenv, truncate_text  # noqa: E402
from okx_trader.config import read_config  # noqa: E402


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _fmt_decimal(value: Any, places: str = "0.00") -> str:
    try:
        return str(_to_decimal(value).quantize(Decimal(places)))
    except Exception:
        return str(value)


def _load_report(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"report json not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {
            "last_status": "unknown",
            "last_alert_ts": 0,
            "last_alert_fingerprint": "",
            "last_recover_ts": 0,
            "last_eval_ts": 0,
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {
        "last_status": "unknown",
        "last_alert_ts": 0,
        "last_alert_fingerprint": "",
        "last_recover_ts": 0,
        "last_eval_ts": 0,
    }


def _save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_metrics(report: Dict[str, Any]) -> Dict[str, Any]:
    journal = report.get("journal") or {}
    batch = journal.get("batch_stats") or {}
    exch = report.get("exchange_positions") or {}
    runtime = report.get("runtime") or {}
    equity_delta = report.get("equity_delta") or {}
    equity = report.get("equity") or {}

    return {
        "window_start_utc": str(report.get("window_start_utc") or ""),
        "window_end_utc": str(report.get("window_end_utc") or ""),
        "journal_loss_streak": _to_int(journal.get("current_loss_streak"), 0),
        "batch_loss_streak": _to_int(batch.get("current_loss_streak"), 0),
        "exchange_rows": _to_int(exch.get("rows"), 0),
        "exchange_loss_streak": _to_int(exch.get("current_loss_streak"), 0),
        "exchange_realized_pnl_usdt": _to_float(exch.get("realized_pnl_sum"), 0.0),
        "runtime_error": _to_int(runtime.get("error"), 0),
        "runtime_loop_error": _to_int(runtime.get("instrument_loop_error"), 0),
        "equity_delta_available": bool(equity_delta.get("available", False)),
        "equity_delta_pct": _to_float(equity_delta.get("delta_pct"), 0.0),
        "equity_delta_usdt": _to_float(equity_delta.get("delta_usdt"), 0.0),
        "current_equity_available": equity.get("equity") is not None,
        "current_equity_usdt": _to_float(equity.get("equity"), 0.0),
        "equity_snapshot_path": str(equity_delta.get("snapshot_path") or ""),
        "bills_hard_alert": bool((report.get("bills_quality") or {}).get("hard_alert", False)),
    }


def _max_equity_from_snapshot(path_text: str) -> Decimal:
    path = Path(str(path_text or "").strip()).expanduser()
    if not path.exists():
        return Decimal("0")
    best = Decimal("0")
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                eq = _to_decimal(row.get("equity"))
                if eq > best:
                    best = eq
    except Exception:
        return Decimal("0")
    return best


def _update_peak_and_drawdown(
    state: Dict[str, Any],
    metrics: Dict[str, Any],
    *,
    seed_peak_equity: float,
) -> Tuple[float, float]:
    peak = _to_decimal(state.get("peak_equity_usdt"))
    if peak <= 0:
        snap_peak = _max_equity_from_snapshot(str(metrics.get("equity_snapshot_path") or ""))
        if snap_peak > peak:
            peak = snap_peak
    seed = _to_decimal(seed_peak_equity)
    if seed > peak:
        peak = seed

    current_eq = _to_decimal(metrics.get("current_equity_usdt"))
    if bool(metrics.get("current_equity_available")) and current_eq > peak:
        peak = current_eq

    dd_pct = Decimal("0")
    if bool(metrics.get("current_equity_available")) and peak > 0:
        dd_pct = (peak - current_eq) / peak * Decimal("100")
        if dd_pct < 0:
            dd_pct = Decimal("0")

    state["peak_equity_usdt"] = str(peak)
    state["peak_equity_ts"] = int(time.time())
    return float(peak), float(dd_pct)


def _evaluate_deviation(
    report: Dict[str, Any],
    metrics: Dict[str, Any],
    *,
    max_drawdown_pct: float,
    drawdown_only: bool,
    max_exch_loss_streak: int,
    max_journal_loss_streak: int,
    max_batch_loss_streak: int,
    min_equity_delta_pct: float,
    min_exchange_pnl_usdt: float,
    min_exchange_rows: int,
    max_runtime_error: int,
    max_runtime_loop_error: int,
    enable_bills_hard_alert: bool,
) -> List[str]:
    triggers: List[str] = []

    if bool(metrics.get("current_equity_available")) and float(max_drawdown_pct) > 0:
        if float(metrics.get("live_drawdown_pct", 0.0)) >= float(max_drawdown_pct):
            triggers.append(
                f"live_drawdown_pct={float(metrics.get('live_drawdown_pct', 0.0)):.4f}%"
                f" >= {float(max_drawdown_pct):.4f}%"
            )

    if drawdown_only:
        return triggers

    exch_rows = int(metrics["exchange_rows"])
    if exch_rows >= int(min_exchange_rows):
        if int(metrics["exchange_loss_streak"]) >= int(max_exch_loss_streak):
            triggers.append(
                f"exchange_loss_streak={int(metrics['exchange_loss_streak'])}"
                f" >= {int(max_exch_loss_streak)}"
            )
        if float(metrics["exchange_realized_pnl_usdt"]) <= float(min_exchange_pnl_usdt):
            triggers.append(
                f"exchange_realized_pnl_usdt={metrics['exchange_realized_pnl_usdt']:.4f}"
                f" <= {float(min_exchange_pnl_usdt):.4f}"
            )

    if int(metrics["journal_loss_streak"]) >= int(max_journal_loss_streak):
        triggers.append(
            f"journal_loss_streak={int(metrics['journal_loss_streak'])}"
            f" >= {int(max_journal_loss_streak)}"
        )
    if int(metrics["batch_loss_streak"]) >= int(max_batch_loss_streak):
        triggers.append(
            f"batch_loss_streak={int(metrics['batch_loss_streak'])}"
            f" >= {int(max_batch_loss_streak)}"
        )
    if bool(metrics["equity_delta_available"]) and float(metrics["equity_delta_pct"]) <= float(min_equity_delta_pct):
        triggers.append(
            f"equity_delta_pct={metrics['equity_delta_pct']:.4f}% <= {float(min_equity_delta_pct):.4f}%"
        )
    if int(metrics["runtime_error"]) > int(max_runtime_error):
        triggers.append(f"runtime_error={int(metrics['runtime_error'])} > {int(max_runtime_error)}")
    if int(metrics["runtime_loop_error"]) > int(max_runtime_loop_error):
        triggers.append(
            f"runtime_loop_error={int(metrics['runtime_loop_error'])} > {int(max_runtime_loop_error)}"
        )
    if enable_bills_hard_alert and bool(metrics["bills_hard_alert"]):
        triggers.append("bills_hard_alert=true")
    return triggers


def _fingerprint(triggers: List[str]) -> str:
    raw = "\n".join(sorted(triggers))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _build_alert_text(report: Dict[str, Any], metrics: Dict[str, Any], triggers: List[str]) -> str:
    outcomes = report.get("outcomes") or {}
    outcomes_source = str(report.get("outcomes_source") or "journal")
    lines = [
        "【Expectation Deviation Alert】",
        f"窗口(UTC): {metrics['window_start_utc']} -> {metrics['window_end_utc']}",
        f"平仓交易({outcomes_source}): {int(outcomes.get('total', 0))} | 胜/负/平: "
        f"{int(outcomes.get('win', 0))}/{int(outcomes.get('loss', 0))}/{int(outcomes.get('breakeven', 0))}",
        f"当前连亏(交易所/台账/批次): {int(metrics['exchange_loss_streak'])}/"
        f"{int(metrics['journal_loss_streak'])}/{int(metrics['batch_loss_streak'])}",
        f"交易所窗口PnL: {_fmt_decimal(metrics['exchange_realized_pnl_usdt'], '0.0000')} USDT "
        f"| rows={int(metrics['exchange_rows'])}",
        f"峰值权益: {_fmt_decimal(metrics.get('peak_equity_usdt', 0.0), '0.0000')} USDT "
        f"| 当前回撤: {_fmt_decimal(metrics.get('live_drawdown_pct', 0.0), '0.0000')}%",
        f"窗口权益变化: {_fmt_decimal(metrics['equity_delta_usdt'], '0.0000')} USDT "
        f"({_fmt_decimal(metrics['equity_delta_pct'], '0.0000')}%)",
        f"Runtime error/loop_error: {int(metrics['runtime_error'])}/{int(metrics['runtime_loop_error'])}",
        "触发项:",
    ]
    for item in triggers:
        lines.append(f"- {item}")
    return "\n".join(lines)


def _build_recover_text(metrics: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "【Expectation Recover】",
            f"窗口(UTC): {metrics['window_start_utc']} -> {metrics['window_end_utc']}",
            "当前巡检已回到阈值内。",
            f"连亏(交易所/台账/批次): {int(metrics['exchange_loss_streak'])}/"
            f"{int(metrics['journal_loss_streak'])}/{int(metrics['batch_loss_streak'])}",
            f"峰值权益: {_fmt_decimal(metrics.get('peak_equity_usdt', 0.0), '0.0000')} USDT "
            f"| 当前回撤: {_fmt_decimal(metrics.get('live_drawdown_pct', 0.0), '0.0000')}%",
            f"窗口权益变化: {_fmt_decimal(metrics['equity_delta_usdt'], '0.0000')} USDT "
            f"({_fmt_decimal(metrics['equity_delta_pct'], '0.0000')}%)",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Expectation deviation detector for rolling recap json.")
    parser.add_argument("--report-json", required=True, help="Input recap report json (from daily_recap.py).")
    parser.add_argument("--state-file", required=True, help="State file for dedupe/cooldown.")
    parser.add_argument("--env", default="", help="Env file path (required when --telegram).")
    parser.add_argument("--cooldown-min", type=float, default=30.0)
    parser.add_argument("--max-drawdown-pct", type=float, default=23.0)
    parser.add_argument("--drawdown-only", action="store_true", help="Only evaluate drawdown trigger.")
    parser.add_argument("--seed-peak-equity", type=float, default=0.0, help="Optional peak equity seed (USDT).")
    parser.add_argument("--max-exch-loss-streak", type=int, default=10)
    parser.add_argument("--max-journal-loss-streak", type=int, default=8)
    parser.add_argument("--max-batch-loss-streak", type=int, default=5)
    parser.add_argument("--min-equity-delta-pct", type=float, default=-5.0)
    parser.add_argument("--min-exchange-pnl-usdt", type=float, default=-120.0)
    parser.add_argument("--min-exchange-rows", type=int, default=8)
    parser.add_argument("--max-runtime-error", type=int, default=2)
    parser.add_argument("--max-runtime-loop-error", type=int, default=2)
    parser.add_argument("--enable-bills-hard-alert", action="store_true")
    parser.add_argument("--telegram", action="store_true", help="Send telegram on deviation.")
    parser.add_argument("--telegram-recover", action="store_true", help="Send telegram when status recovers.")
    parser.add_argument("--print", action="store_true", help="Print status line.")
    parser.add_argument("--exit-on-alert", action="store_true", help="Return code 2 when deviation detected.")
    args = parser.parse_args()

    report_path = Path(args.report_json).expanduser()
    state_path = Path(args.state_file).expanduser()
    report = _load_report(report_path)
    state = _load_state(state_path)
    metrics = _build_metrics(report)
    peak_eq, live_dd_pct = _update_peak_and_drawdown(
        state=state,
        metrics=metrics,
        seed_peak_equity=float(args.seed_peak_equity),
    )
    metrics["peak_equity_usdt"] = peak_eq
    metrics["live_drawdown_pct"] = live_dd_pct

    triggers = _evaluate_deviation(
        report,
        metrics,
        max_drawdown_pct=float(args.max_drawdown_pct),
        drawdown_only=bool(args.drawdown_only),
        max_exch_loss_streak=int(args.max_exch_loss_streak),
        max_journal_loss_streak=int(args.max_journal_loss_streak),
        max_batch_loss_streak=int(args.max_batch_loss_streak),
        min_equity_delta_pct=float(args.min_equity_delta_pct),
        min_exchange_pnl_usdt=float(args.min_exchange_pnl_usdt),
        min_exchange_rows=max(0, int(args.min_exchange_rows)),
        max_runtime_error=max(0, int(args.max_runtime_error)),
        max_runtime_loop_error=max(0, int(args.max_runtime_loop_error)),
        enable_bills_hard_alert=bool(args.enable_bills_hard_alert),
    )
    status = "alert" if triggers else "ok"

    now = int(time.time())
    cooldown_sec = max(60, int(max(1.0, float(args.cooldown_min)) * 60))
    last_status = str(state.get("last_status") or "unknown")
    last_alert_ts = _to_int(state.get("last_alert_ts"), 0)
    last_recover_ts = _to_int(state.get("last_recover_ts"), 0)
    last_fp = str(state.get("last_alert_fingerprint") or "")
    fp = _fingerprint(triggers) if triggers else ""

    send_alert = False
    send_recover = False
    if status == "alert":
        if last_status != "alert":
            send_alert = True
        elif fp != last_fp:
            send_alert = True
        elif now - last_alert_ts >= cooldown_sec:
            send_alert = True
    elif bool(args.telegram_recover) and last_status == "alert":
        if now - last_recover_ts >= cooldown_sec:
            send_recover = True

    if args.telegram and send_alert:
        env_path = Path(args.env).expanduser() if args.env else (ROOT_DIR / "okx_auto_trader.env")
        load_dotenv(str(env_path))
        cfg = read_config(None)
        send_telegram(cfg, truncate_text(_build_alert_text(report, metrics, triggers), 3600))
    if args.telegram and send_recover:
        env_path = Path(args.env).expanduser() if args.env else (ROOT_DIR / "okx_auto_trader.env")
        load_dotenv(str(env_path))
        cfg = read_config(None)
        send_telegram(cfg, truncate_text(_build_recover_text(metrics), 3600))

    state["last_status"] = status
    state["last_eval_ts"] = now
    if send_alert:
        state["last_alert_ts"] = now
        state["last_alert_fingerprint"] = fp
    if send_recover:
        state["last_recover_ts"] = now
    _save_state(state_path, state)

    if args.print:
        parts: List[str] = [
            f"status={status}",
            f"live_dd={float(metrics.get('live_drawdown_pct', 0.0)):.4f}%",
            f"peak_eq={float(metrics.get('peak_equity_usdt', 0.0)):.4f}",
            f"ex_loss={int(metrics['exchange_loss_streak'])}",
            f"jr_loss={int(metrics['journal_loss_streak'])}",
            f"batch_loss={int(metrics['batch_loss_streak'])}",
            f"ex_pnl={metrics['exchange_realized_pnl_usdt']:.4f}",
            f"eq_delta_pct={metrics['equity_delta_pct']:.4f}",
            f"runtime_err={int(metrics['runtime_error'])}",
            f"runtime_loop_err={int(metrics['runtime_loop_error'])}",
            f"trigger_count={len(triggers)}",
        ]
        if send_alert:
            parts.append("tg_sent=alert")
        elif send_recover:
            parts.append("tg_sent=recover")
        else:
            parts.append("tg_sent=none")
        print(" | ".join(parts))
        if triggers:
            for item in triggers:
                print(f"trigger: {item}")

    if status == "alert" and args.exit_on_alert:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
