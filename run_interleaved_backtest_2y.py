#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import hashlib
import math
import os
import time
import traceback
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from okx_trader.alerts import send_telegram
from okx_trader.backtest import _build_backtest_precalc, _build_backtest_signal_fast
from okx_trader.backtest_report import (
    finalize_level_perf,
    format_backtest_result_line,
    new_level_perf,
    normalize_backtest_result,
    update_level_perf,
)
from okx_trader.common import bar_to_seconds, load_dotenv
from okx_trader.config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    read_config,
    resolve_exec_max_level,
)
from okx_trader.decision_core import resolve_entry_decision
from okx_trader.okx_client import OKXClient
from okx_trader.profile_vote import merge_entry_votes
from okx_trader.risk_guard import (
    is_open_limit_reached,
    min_open_gap_remaining_minutes,
    normalize_loss_base_mode,
    prune_loss_deque_window,
    prune_ts_deque_window,
    resolve_loss_base,
)


def ts_fmt(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v < 0:
        return 0.0
    if v > 1:
        return 1.0
    return v


def normalize_entry_exec_mode(mode: str) -> str:
    m = str(mode or "").strip().lower()
    if m not in {"market", "limit", "auto"}:
        return "market"
    return m


def normalize_entry_limit_fallback_mode(mode: str) -> str:
    m = str(mode or "").strip().lower()
    if m not in {"market", "skip"}:
        return "market"
    return m


def resolve_entry_exec_mode(mode: str, level: int, auto_market_level_min: int) -> str:
    norm = normalize_entry_exec_mode(mode)
    if norm != "auto":
        return norm
    threshold = max(1, min(3, int(auto_market_level_min)))
    return "market" if int(level) >= threshold else "limit"


def stable_u01(key: str) -> float:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    n = int.from_bytes(h, byteorder="big", signed=False)
    return n / float(2**64)


def apply_execution_penalty_r(
    *,
    r_raw: float,
    outcome: str,
    entry: float,
    stop: float,
    fee_rate: float,
    slippage_bps: float,
    stop_extra_r: float,
    tp_haircut_r: float,
) -> Tuple[float, float]:
    entry_px = max(1e-12, float(entry))
    stop_pct = abs(float(entry) - float(stop)) / entry_px
    stop_pct = max(stop_pct, 1e-6)
    fee = max(0.0, float(fee_rate))
    slip_one_way = max(0.0, float(slippage_bps)) / 10000.0
    friction_r = (fee + 2.0 * slip_one_way) / stop_pct

    r_adj = float(r_raw) - friction_r
    if outcome == "STOP":
        r_adj -= max(0.0, float(stop_extra_r))
    elif outcome in {"TP1", "TP2"}:
        r_adj -= max(0.0, float(tp_haircut_r))
    return r_adj, friction_r


def dump_trades_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    out = str(path or "").strip()
    if not out:
        return
    parent = os.path.dirname(out)
    if parent:
        os.makedirs(parent, exist_ok=True)
    fields = [
        "entry_ts",
        "exit_ts",
        "inst",
        "side",
        "level",
        "entry_px",
        "stop_px",
        "tp1_px",
        "tp2_px",
        "risk_px",
        "outcome",
        "entry_exec_mode",
        "r",
        "r_raw",
        "friction_r",
        "pnl",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in rows:
            w.writerow(
                {
                    "entry_ts": int(t.get("entry_ts", 0) or 0),
                    "exit_ts": int(t.get("exit_ts", 0) or 0),
                    "inst": str(t.get("inst", "")),
                    "side": str(t.get("side", "")),
                    "level": int(t.get("level", 0) or 0),
                    "entry_px": float(t.get("entry_px", 0.0) or 0.0),
                    "stop_px": float(t.get("stop_px", 0.0) or 0.0),
                    "tp1_px": float(t.get("tp1_px", 0.0) or 0.0),
                    "tp2_px": float(t.get("tp2_px", 0.0) or 0.0),
                    "risk_px": float(t.get("risk_px", 0.0) or 0.0),
                    "outcome": str(t.get("outcome", "")),
                    "entry_exec_mode": str(t.get("entry_exec_mode", "market") or "market"),
                    "r": float(t.get("r", 0.0) or 0.0),
                    "r_raw": float(t.get("r_raw", 0.0) or 0.0),
                    "friction_r": float(t.get("friction_r", 0.0) or 0.0),
                    "pnl": float(t.get("pnl", 0.0) or 0.0),
                }
            )


def _new_sim_position(
    *,
    decision: Any,
    entry_ts: int,
    entry_i: int,
    risk_amt: float,
) -> Dict[str, Any]:
    entry = float(decision.entry)
    stop = float(decision.stop)
    risk = float(decision.risk)
    if risk <= 0:
        risk = abs(entry - stop)
    risk = max(risk, 1e-8)
    return {
        "side": str(decision.side),
        "level": int(decision.level),
        "entry_ts": int(entry_ts),
        "entry_i": int(entry_i),
        "entry": float(entry),
        "stop": float(stop),
        "risk": float(risk),
        "tp1": float(decision.tp1),
        "tp2": float(decision.tp2),
        "risk_amt": float(risk_amt),
        "qty_rem": 1.0,
        "realized_r": 0.0,
        "tp1_done": False,
        "be_armed": False,
        "hard_stop": float(stop),
        "peak_price": float(entry),
        "trough_price": float(entry),
    }


def _close_remaining_r(side: str, entry: float, risk: float, close_px: float) -> float:
    side_u = str(side).strip().upper()
    if side_u == "LONG":
        return (float(close_px) - float(entry)) / float(risk)
    return (float(entry) - float(close_px)) / float(risk)


def _position_potential_loss_usdt(pos: Dict[str, Any]) -> float:
    risk_amt = max(0.0, float(pos.get("risk_amt", 0.0) or 0.0))
    if risk_amt <= 0:
        return 0.0
    qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
    if qty_rem <= 0:
        return 0.0
    side = str(pos.get("side", "") or "").strip().upper()
    if side not in {"LONG", "SHORT"}:
        return 0.0
    entry = float(pos.get("entry", 0.0) or 0.0)
    risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
    hard_stop = float(pos.get("hard_stop", pos.get("stop", entry)) or entry)
    r_at_stop = float(qty_rem) * float(_close_remaining_r(side, entry, risk, hard_stop))
    if r_at_stop >= 0:
        return 0.0
    return abs(float(r_at_stop) * float(risk_amt))


def _sum_open_positions_potential_loss(open_positions: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for _, pos in open_positions.items():
        if not isinstance(pos, dict):
            continue
        total += max(0.0, _position_potential_loss_usdt(pos))
    return total


def _simulate_live_position_step(
    *,
    pos: Dict[str, Any],
    sig: Dict[str, Any],
    params: Any,
    decision: Optional[Any],
    allow_reverse: bool,
    managed_exit: bool,
    use_signal_exit: bool = True,
) -> Dict[str, Any]:
    side_u = str(pos.get("side", "")).strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    close = float(sig.get("close", 0.0) or 0.0)
    entry = float(pos.get("entry", close) or close)
    risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
    tp1 = float(pos.get("tp1", entry))
    tp2 = float(pos.get("tp2", entry))
    qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
    realized_r = float(pos.get("realized_r", 0.0) or 0.0)
    tp1_done = bool(pos.get("tp1_done", False))
    be_armed = bool(pos.get("be_armed", False))
    hard_stop = float(pos.get("hard_stop", pos.get("stop", entry)) or entry)

    be_total_offset = max(0.0, float(params.be_offset_pct) + float(params.be_fee_buffer_pct))
    be_trigger_r = max(0.0, float(params.be_trigger_r_mult))

    if side_u == "LONG":
        if not managed_exit:
            stop_hit = close <= float(pos.get("stop", entry) or entry)
            tp_hit = close >= tp2
            long_exit = bool(sig.get("long_exit", False)) if use_signal_exit else False
            if stop_hit or tp_hit or long_exit:
                realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                outcome = "STOP" if stop_hit else ("TP2" if tp_hit else "EXIT")
                reverse_decision = None
                reverse_now = stop_hit or long_exit
                if reverse_now and allow_reverse and (decision is not None) and str(getattr(decision, "side", "")).upper() == "SHORT":
                    reverse_decision = decision
                return {
                    "closed": True,
                    "outcome": str(outcome),
                    "r_raw": float(realized_r),
                    "is_stop": bool(stop_hit),
                    "reverse_decision": reverse_decision,
                }
            return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

        peak = max(float(pos.get("peak_price", entry) or entry), close)
        pos["peak_price"] = peak

        if (not be_armed) and close >= entry + risk * be_trigger_r:
            be_armed = True
            pos["be_armed"] = True

        if (not tp1_done) and float(params.tp1_close_pct) > 0:
            tp1_price = entry + risk * float(params.tp1_r_mult)
            if close >= tp1_price:
                pct = min(max(float(params.tp1_close_pct), 0.0), 1.0)
                close_qty = qty_rem * pct
                if close_qty >= qty_rem * 0.999:
                    realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                    pos["qty_rem"] = 0.0
                    pos["realized_r"] = realized_r
                    return {
                        "closed": True,
                        "outcome": "TP1",
                        "r_raw": float(realized_r),
                        "is_stop": False,
                        "reverse_decision": None,
                    }
                if close_qty > 0:
                    realized_r += close_qty * _close_remaining_r("LONG", entry, risk, close)
                    qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    be_stop = entry * (1.0 + be_total_offset)
                    hard_stop = max(hard_stop, be_stop)
                    pos["qty_rem"] = qty_rem
                    pos["realized_r"] = realized_r
                    pos["tp1_done"] = True
                    pos["be_armed"] = True
                    pos["hard_stop"] = hard_stop
                    return {
                        "closed": False,
                        "outcome": "NONE",
                        "r_raw": 0.0,
                        "is_stop": False,
                        "reverse_decision": None,
                    }

        if bool(params.tp2_close_rest) and tp1_done and qty_rem > 0:
            tp2_price = entry + risk * float(params.tp2_r_mult)
            if close >= tp2_price:
                realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                return {
                    "closed": True,
                    "outcome": "TP2",
                    "r_raw": float(realized_r),
                    "is_stop": False,
                    "reverse_decision": None,
                }

        dynamic_stop = max(hard_stop, float(sig.get("long_stop", hard_stop) or hard_stop))
        if be_armed:
            dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
        if (not bool(params.trail_after_tp1)) or tp1_done:
            atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
            trail_stop = peak - atr_v * float(params.trail_atr_mult)
            dynamic_stop = max(dynamic_stop, trail_stop)

        stop_hit = close <= dynamic_stop
        long_exit = bool(sig.get("long_exit", False)) if use_signal_exit else False

        pos["qty_rem"] = qty_rem
        pos["realized_r"] = realized_r
        pos["tp1_done"] = tp1_done
        pos["be_armed"] = be_armed
        pos["hard_stop"] = dynamic_stop

        if long_exit or stop_hit:
            realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            if tp1_done:
                outcome = "TP1"
            elif stop_hit:
                outcome = "STOP"
            else:
                outcome = "EXIT"
            reverse_decision = None
            if allow_reverse and (decision is not None) and str(getattr(decision, "side", "")).upper() == "SHORT":
                reverse_decision = decision
            return {
                "closed": True,
                "outcome": str(outcome),
                "r_raw": float(realized_r),
                "is_stop": bool(stop_hit),
                "reverse_decision": reverse_decision,
            }
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    if not managed_exit:
        stop_hit = close >= float(pos.get("stop", entry) or entry)
        tp_hit = close <= tp2
        short_exit = bool(sig.get("short_exit", False)) if use_signal_exit else False
        if stop_hit or tp_hit or short_exit:
            realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            outcome = "STOP" if stop_hit else ("TP2" if tp_hit else "EXIT")
            reverse_decision = None
            reverse_now = stop_hit or short_exit
            if reverse_now and allow_reverse and (decision is not None) and str(getattr(decision, "side", "")).upper() == "LONG":
                reverse_decision = decision
            return {
                "closed": True,
                "outcome": str(outcome),
                "r_raw": float(realized_r),
                "is_stop": bool(stop_hit),
                "reverse_decision": reverse_decision,
            }
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    trough = min(float(pos.get("trough_price", entry) or entry), close)
    pos["trough_price"] = trough

    if (not be_armed) and close <= entry - risk * be_trigger_r:
        be_armed = True
        pos["be_armed"] = True

    if (not tp1_done) and float(params.tp1_close_pct) > 0:
        tp1_price = entry - risk * float(params.tp1_r_mult)
        if close <= tp1_price:
            pct = min(max(float(params.tp1_close_pct), 0.0), 1.0)
            close_qty = qty_rem * pct
            if close_qty >= qty_rem * 0.999:
                realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                return {
                    "closed": True,
                    "outcome": "TP1",
                    "r_raw": float(realized_r),
                    "is_stop": False,
                    "reverse_decision": None,
                }
            if close_qty > 0:
                realized_r += close_qty * _close_remaining_r("SHORT", entry, risk, close)
                qty_rem = max(0.0, qty_rem - close_qty)
                tp1_done = True
                be_armed = True
                be_stop = entry * (1.0 - be_total_offset)
                hard_stop = min(hard_stop, be_stop)
                pos["qty_rem"] = qty_rem
                pos["realized_r"] = realized_r
                pos["tp1_done"] = True
                pos["be_armed"] = True
                pos["hard_stop"] = hard_stop
                return {
                    "closed": False,
                    "outcome": "NONE",
                    "r_raw": 0.0,
                    "is_stop": False,
                    "reverse_decision": None,
                }

    if bool(params.tp2_close_rest) and tp1_done and qty_rem > 0:
        tp2_price = entry - risk * float(params.tp2_r_mult)
        if close <= tp2_price:
            realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            return {
                "closed": True,
                "outcome": "TP2",
                "r_raw": float(realized_r),
                "is_stop": False,
                "reverse_decision": None,
            }

    dynamic_stop = min(hard_stop, float(sig.get("short_stop", hard_stop) or hard_stop))
    if be_armed:
        dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
    if (not bool(params.trail_after_tp1)) or tp1_done:
        atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
        trail_stop = trough + atr_v * float(params.trail_atr_mult)
        dynamic_stop = min(dynamic_stop, trail_stop)

    stop_hit = close >= dynamic_stop
    short_exit = bool(sig.get("short_exit", False)) if use_signal_exit else False

    pos["qty_rem"] = qty_rem
    pos["realized_r"] = realized_r
    pos["tp1_done"] = tp1_done
    pos["be_armed"] = be_armed
    pos["hard_stop"] = dynamic_stop

    if short_exit or stop_hit:
        realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
        pos["qty_rem"] = 0.0
        pos["realized_r"] = realized_r
        if tp1_done:
            outcome = "TP1"
        elif stop_hit:
            outcome = "STOP"
        else:
            outcome = "EXIT"
        reverse_decision = None
        if allow_reverse and (decision is not None) and str(getattr(decision, "side", "")).upper() == "LONG":
            reverse_decision = decision
        return {
            "closed": True,
            "outcome": str(outcome),
            "r_raw": float(realized_r),
            "is_stop": bool(stop_hit),
            "reverse_decision": reverse_decision,
        }

    return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}


def main() -> int:
    parser = argparse.ArgumentParser(description="2Y interleaved portfolio backtest")
    parser.add_argument("--env", default="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env")
    parser.add_argument("--bars", type=int, default=70080, help="15m bars to evaluate (~70080 for 2 years)")
    parser.add_argument("--risk-frac", type=float, default=0.005, help="Risk fraction per trade for compounding")
    parser.add_argument("--title", default="2Y 真实顺序 TP1_ONLY（全币种）")
    parser.add_argument(
        "--pessimistic",
        action="store_true",
        help="Enable pessimistic execution model (fees/slippage/miss/TP haircut/stop worsening)",
    )
    parser.add_argument("--fee-rate", type=float, default=0.0, help="Round-trip fee rate, e.g. 0.0012 = 0.12%%")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="One-way slippage in bps")
    parser.add_argument("--stop-extra-r", type=float, default=0.0, help="Extra R loss for STOP outcome")
    parser.add_argument("--tp-haircut-r", type=float, default=0.0, help="R haircut for TP1/TP2 outcome")
    parser.add_argument("--miss-prob", type=float, default=0.0, help="Signal miss probability (0~1)")
    parser.add_argument(
        "--entry-exec-mode",
        default="",
        help="Entry execution mode: market|limit|auto (default: read from env STRAT_ENTRY_EXEC_MODE)",
    )
    parser.add_argument(
        "--entry-auto-market-level-min",
        type=int,
        default=None,
        help="When entry-exec-mode=auto, levels >= this use market (default: env STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN)",
    )
    parser.add_argument(
        "--entry-limit-fallback-mode",
        default="",
        help="When entry-exec-mode resolves to limit: market|skip (default: env STRAT_ENTRY_LIMIT_FALLBACK_MODE)",
    )
    parser.add_argument(
        "--entry-limit-slippage-bps",
        type=float,
        default=None,
        help="One-way slippage for limit-filled entries in bps (default: market_slippage*0.25)",
    )
    parser.add_argument(
        "--entry-limit-fee-rate",
        type=float,
        default=None,
        help="Round-trip fee rate for limit-filled entries (default: same as --fee-rate)",
    )
    parser.add_argument(
        "--managed-exit",
        action="store_true",
        help="Use managed exit path: TP1 partial + remaining TP2 + BE/fee-buffer stop",
    )
    parser.add_argument(
        "--dump-trades-csv",
        default="",
        help="Optional output CSV path for accepted trade list (for Monte Carlo etc.)",
    )
    parser.add_argument(
        "--ignore-signal-exit",
        action="store_true",
        help="Disable signal-based early close (long_exit/short_exit) in backtest management path",
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    cfg = read_config(None)
    client = OKXClient(cfg)
    run_start = time.monotonic()

    inst_ids = list(cfg.inst_ids)
    if not inst_ids:
        print("No inst ids configured.")
        return 1
    profile_ids_by_inst: Dict[str, List[str]] = {inst: get_strategy_profile_ids(cfg, inst) for inst in inst_ids}
    profile_by_inst: Dict[str, str] = {
        inst: (ids[0] if ids else get_strategy_profile_id(cfg, inst))
        for inst, ids in profile_ids_by_inst.items()
    }
    params_by_inst = {
        inst: cfg.strategy_profiles.get(profile_by_inst[inst], get_strategy_params(cfg, inst))
        for inst in inst_ids
    }

    bars = max(1200, int(args.bars))
    risk_frac = max(0.0, float(args.risk_frac))
    if risk_frac <= 0:
        risk_frac = 0.005

    fee_rate = max(0.0, float(args.fee_rate))
    slippage_bps = max(0.0, float(args.slippage_bps))
    stop_extra_r = max(0.0, float(args.stop_extra_r))
    tp_haircut_r = max(0.0, float(args.tp_haircut_r))
    miss_prob = clamp01(float(args.miss_prob))
    if bool(args.pessimistic):
        if fee_rate <= 0:
            fee_rate = 0.0012
        if slippage_bps <= 0:
            slippage_bps = 3.0
        if stop_extra_r <= 0:
            stop_extra_r = 0.10
        if tp_haircut_r <= 0:
            tp_haircut_r = 0.05
        if miss_prob <= 0:
            miss_prob = 0.08
    entry_exec_mode = normalize_entry_exec_mode(
        str(args.entry_exec_mode).strip().lower() if str(args.entry_exec_mode or "").strip() else str(getattr(cfg.params, "entry_exec_mode", "market"))
    )
    if args.entry_auto_market_level_min is None:
        entry_auto_market_level_min = int(getattr(cfg.params, "entry_auto_market_level_min", 3) or 3)
    else:
        entry_auto_market_level_min = int(args.entry_auto_market_level_min)
    entry_auto_market_level_min = max(1, min(3, entry_auto_market_level_min))

    entry_limit_fallback_mode = normalize_entry_limit_fallback_mode(
        str(args.entry_limit_fallback_mode).strip().lower()
        if str(args.entry_limit_fallback_mode or "").strip()
        else str(getattr(cfg.params, "entry_limit_fallback_mode", "market"))
    )
    if args.entry_limit_slippage_bps is None:
        entry_limit_slippage_bps = max(0.0, float(slippage_bps) * 0.25)
    else:
        entry_limit_slippage_bps = max(0.0, float(args.entry_limit_slippage_bps))
    if args.entry_limit_fee_rate is None:
        entry_limit_fee_rate = max(0.0, float(fee_rate))
    else:
        entry_limit_fee_rate = max(0.0, float(args.entry_limit_fee_rate))

    horizon_bars = 0
    min_level = 1
    base_exec_max_level = int(cfg.params.exec_max_level)
    if base_exec_max_level < 1:
        base_exec_max_level = 1
    if base_exec_max_level > 3:
        base_exec_max_level = 3
    max_level = base_exec_max_level
    all_params: List[Any] = []
    for inst in inst_ids:
        ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_level = max(max(1, min(3, int(p.exec_max_level))) for p in all_params)
    l3_inst_set = set()
    for p in all_params:
        l3_inst_set.update(str(x).strip().upper() for x in p.exec_l3_inst_ids if str(x).strip())
    exact_level = 0
    require_tp_sl = True
    tp1_only = not bool(args.managed_exit)
    managed_exit = bool(args.managed_exit)
    use_signal_exit = bool(cfg.params.signal_exit_enabled) and (not bool(args.ignore_signal_exit))

    per_inst_cap = int(cfg.params.max_open_entries)
    global_cap = int(cfg.params.max_open_entries_global)
    window_hours = int(cfg.params.open_window_hours)
    min_gap_minutes = int(cfg.params.min_open_interval_minutes)
    window_ms = max(1, window_hours) * 3600 * 1000
    stop_cooldown_minutes = int(max(0, cfg.params.stop_reentry_cooldown_minutes))
    stop_freeze_count = int(max(0, cfg.params.stop_streak_freeze_count))
    stop_freeze_hours = int(max(0, cfg.params.stop_streak_freeze_hours))
    stop_l2_only = bool(cfg.params.stop_streak_l2_only)

    loss_limit_pct = float(cfg.params.daily_loss_limit_pct)
    loss_base_fixed = float(cfg.params.daily_loss_base_usdt)
    loss_base_mode = normalize_loss_base_mode(str(cfg.params.daily_loss_base_mode))
    loss_window_ms = 24 * 3600 * 1000

    start_equity = loss_base_fixed if loss_base_fixed > 0 else 1000.0

    print("=== 2Y Interleaved Portfolio Backtest ===", flush=True)
    print(f"insts={','.join(inst_ids)}", flush=True)
    print(
        f"bars={bars} horizon=to_end min/max/exact={min_level}/{max_level}/{exact_level} "
        f"require_tp_sl={require_tp_sl} tp1_only={tp1_only} managed_exit={managed_exit}",
        flush=True,
    )
    print(
        f"level_control: base_exec_max={base_exec_max_level} "
        f"l3_whitelist={','.join(sorted(l3_inst_set)) if l3_inst_set else '-'}",
        flush=True,
    )
    non_default_profiles = []
    for inst in inst_ids:
        ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        if len(ids) > 1:
            non_default_profiles.append(f"{inst}:{'+'.join(ids)}")
        elif profile_by_inst.get(inst, "DEFAULT") != "DEFAULT":
            non_default_profiles.append(f"{inst}:{profile_by_inst.get(inst, 'DEFAULT')}")
    if non_default_profiles:
        print(f"profiles={','.join(non_default_profiles)}", flush=True)
    if any(len(profile_ids_by_inst.get(inst, [])) > 1 for inst in inst_ids):
        print(
            f"profile_vote: mode={cfg.strategy_profile_vote_mode} min_agree={cfg.strategy_profile_vote_min_agree}",
            flush=True,
        )
    print(
        f"constraints: min_gap={min_gap_minutes}m inst_cap={per_inst_cap}/24h global_cap={global_cap}/24h "
        f"loss_guard={loss_limit_pct*100:.2f}%({loss_base_mode},projected)",
        flush=True,
    )
    print(
        f"stop_guard: cooldown={stop_cooldown_minutes}m freeze={stop_freeze_count}/{stop_freeze_hours}h "
        f"l2_only={stop_l2_only}",
        flush=True,
    )
    print(
        f"execution_model: pessimistic={bool(args.pessimistic)} fee_rate={fee_rate:.5f} "
        f"slippage_bps={slippage_bps:.2f} stop_extra_r={stop_extra_r:.3f} "
        f"tp_haircut_r={tp_haircut_r:.3f} miss_prob={miss_prob:.3f}",
        flush=True,
    )
    print(
        f"entry_exec_model: mode={entry_exec_mode} auto_market_level_min={entry_auto_market_level_min} "
        f"limit_fallback={entry_limit_fallback_mode} "
        f"limit_fee_rate={entry_limit_fee_rate:.5f} limit_slippage_bps={entry_limit_slippage_bps:.2f}",
        flush=True,
    )
    print(f"signal_exit_enabled={use_signal_exit}", flush=True)
    print(
        f"compound: risk_per_trade={risk_frac*100:.2f}% start_equity={start_equity:.2f}",
        flush=True,
    )

    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)
    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
    need_ltf = bars + 300
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        max_loc_lookback = cfg.params.loc_lookback
        max_htf_ema_slow = cfg.params.htf_ema_slow_len
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

    data: Dict[str, Dict[str, Any]] = {}
    all_ts = set()
    for idx, inst in enumerate(inst_ids, 1):
        inst_params = params_by_inst.get(inst, cfg.params)
        inst_profile_ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        if profile_by_inst.get(inst, "DEFAULT") not in inst_profile_ids:
            inst_profile_ids = [profile_by_inst.get(inst, "DEFAULT")] + [x for x in inst_profile_ids if x != profile_by_inst.get(inst, "DEFAULT")]
        inst_exec_max_level = resolve_exec_max_level(inst_params, inst)
        print(f"[{idx}/{len(inst_ids)}] fetch {inst} ...", flush=True)
        try:
            htf = client.get_candles_history(inst, cfg.htf_bar, need_htf)
            loc = client.get_candles_history(inst, cfg.loc_bar, need_loc)
            ltf = client.get_candles_history(inst, cfg.ltf_bar, need_ltf)
        except Exception as e:
            print(f"[{inst}] fetch failed: {e}", flush=True)
            continue
        print(f"[{inst}] candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}", flush=True)
        if len(htf) < 50 or len(loc) < 120 or len(ltf) < 300:
            print(f"[{inst}] skip short data", flush=True)
            continue

        pre_by_profile: Dict[str, Dict[str, Any]] = {}
        try:
            pre_by_profile[profile_by_inst.get(inst, "DEFAULT")] = _build_backtest_precalc(htf, loc, ltf, inst_params)
            for pid in inst_profile_ids:
                if pid == profile_by_inst.get(inst, "DEFAULT"):
                    continue
                pp = cfg.strategy_profiles.get(pid, cfg.params)
                pre_by_profile[pid] = _build_backtest_precalc(htf, loc, ltf, pp)
        except Exception as e:
            print(f"[{inst}] precalc failed: {e}", flush=True)
            continue
        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]
        start_idx = max(0, len(ltf) - bars)
        ts_to_i = {int(ltf_ts[i]): i for i in range(start_idx, len(ltf) - 1)}
        for i in range(start_idx, len(ltf) - 1):
            all_ts.add(int(ltf_ts[i]))

        data[inst] = {
            "htf": htf,
            "loc": loc,
            "ltf": ltf,
            "pre_by_profile": pre_by_profile,
            "htf_ts": htf_ts,
            "loc_ts": loc_ts,
            "ltf_ts": ltf_ts,
            "start_idx": start_idx,
            "ts_to_i": ts_to_i,
            "params": inst_params,
            "profile_id": profile_by_inst.get(inst, "DEFAULT"),
            "profile_ids": inst_profile_ids,
            "exec_max_level": inst_exec_max_level,
            "vote_enabled": bool(len(inst_profile_ids) > 1),
        }

    inst_ids = [x for x in inst_ids if x in data]
    if not inst_ids:
        print("No instrument has enough history for backtest.")
        return 1

    timeline = sorted(all_ts)
    print(f"timeline_points={len(timeline)}", flush=True)

    open_positions: Dict[str, Dict[str, Any]] = {}
    open_positions_total = 0

    equity = float(start_equity)
    peak_equity = equity
    peak_ts = timeline[0]
    max_dd = 0.0
    max_dd_peak_ts = peak_ts
    max_dd_trough_ts = peak_ts
    max_concurrent = 0

    global_open_ts: Deque[int] = deque()
    inst_open_ts: Dict[str, Deque[int]] = {inst: deque() for inst in inst_ids}
    inst_last_open_ts: Dict[str, int | None] = {inst: None for inst in inst_ids}

    # (close_ts, loss_usdt)
    loss_events: Deque[Tuple[int, float]] = deque()

    accepted: List[Dict[str, Any]] = []
    by_level = defaultdict(int)
    by_side = defaultdict(int)
    level_perf = new_level_perf()
    tp1_count = 0
    tp2_count = 0
    stop_count = 0
    none_count = 0
    skip_miss = 0
    skip_limit_unfilled = 0
    total_raw_r = 0.0
    total_friction_r = 0.0
    by_entry_exec_mode = defaultdict(int)
    by_inst_stats: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "n": 0.0,
            "win": 0.0,
            "r_sum": 0.0,
            "r_raw_sum": 0.0,
            "friction_r_sum": 0.0,
            "tp1": 0.0,
            "tp2": 0.0,
            "stop": 0.0,
            "none": 0.0,
        }
    )
    skip_gap = 0
    skip_inst_cap = 0
    skip_global_cap = 0
    skip_unresolved = 0
    skip_loss_guard = 0
    skip_stop_guard = 0
    stop_guard: Dict[str, Dict[str, Dict[str, int]]] = {
        inst: {"long": {}, "short": {}} for inst in inst_ids
    }

    def _sg_bucket(inst: str, side: str) -> Dict[str, int]:
        side_k = str(side).strip().lower()
        if side_k not in {"long", "short"}:
            side_k = "long"
        one = stop_guard.setdefault(inst, {"long": {}, "short": {}})
        b = one.get(side_k)
        if not isinstance(b, dict):
            b = {}
            one[side_k] = b
        return b

    def _sg_record(inst: str, side: str, is_stop: bool, event_ts: int) -> None:
        b = _sg_bucket(inst, side)
        prev = int(b.get("streak", 0) or 0)
        if is_stop:
            streak = prev + 1
            b["streak"] = streak
            b["last_stop_ts_ms"] = int(event_ts)
            if stop_freeze_count > 0 and stop_freeze_hours > 0 and streak >= stop_freeze_count:
                b["freeze_until_ts_ms"] = int(event_ts) + stop_freeze_hours * 3600 * 1000
            return
        if prev > 0:
            b["streak"] = 0

    def _sg_allow(inst: str, side: str, level: int, now_ts: int) -> bool:
        nonlocal skip_stop_guard
        b = _sg_bucket(inst, side)
        last_stop_ts = int(b.get("last_stop_ts_ms", 0) or 0)
        if stop_cooldown_minutes > 0 and last_stop_ts > 0:
            if int(now_ts) - last_stop_ts < stop_cooldown_minutes * 60 * 1000:
                skip_stop_guard += 1
                return False
        freeze_until_ts = int(b.get("freeze_until_ts_ms", 0) or 0)
        if freeze_until_ts > int(now_ts):
            if stop_l2_only:
                if int(level) > 2:
                    skip_stop_guard += 1
                    return False
            else:
                skip_stop_guard += 1
                return False
        return True

    def _record_close(
        *,
        inst: str,
        pos: Dict[str, Any],
        exit_ts: int,
        outcome: str,
        r_raw: float,
        is_stop: bool,
    ) -> None:
        nonlocal equity, peak_equity, peak_ts, max_dd, max_dd_peak_ts, max_dd_trough_ts
        nonlocal tp1_count, tp2_count, stop_count, none_count, total_raw_r, total_friction_r, skip_unresolved

        outcome_k = str(outcome or "NONE").upper()
        if tp1_only and outcome_k == "TP2":
            outcome_k = "TP1"
        if require_tp_sl and outcome_k == "NONE":
            skip_unresolved += 1
            return

        entry = float(pos.get("entry", 0.0) or 0.0)
        stop = float(pos.get("stop", 0.0) or 0.0)
        level = int(pos.get("level", 0) or 0)
        side = str(pos.get("side", "") or "").upper()
        risk_amt = float(pos.get("risk_amt", 0.0) or 0.0)
        entry_ts = int(pos.get("entry_ts", exit_ts) or exit_ts)
        entry_exec_mode_used = str(pos.get("entry_exec_mode", "market") or "market")
        fee_rate_used = max(0.0, float(pos.get("fee_rate", fee_rate) or fee_rate))
        slippage_bps_used = max(0.0, float(pos.get("slippage_bps", slippage_bps) or slippage_bps))

        r_adj, friction_r = apply_execution_penalty_r(
            r_raw=float(r_raw),
            outcome=outcome_k,
            entry=entry,
            stop=stop,
            fee_rate=fee_rate_used,
            slippage_bps=slippage_bps_used,
            stop_extra_r=stop_extra_r,
            tp_haircut_r=tp_haircut_r,
        )
        pnl = float(risk_amt) * float(r_adj)
        equity += pnl
        if pnl < 0:
            loss_events.append((int(exit_ts), abs(float(pnl))))

        accepted.append(
            {
                "inst": inst,
                "entry_ts": int(entry_ts),
                "exit_ts": int(exit_ts),
                "r": float(r_adj),
                "r_raw": float(r_raw),
                "friction_r": float(friction_r),
                "pnl": float(pnl),
                "outcome": str(outcome_k),
                "level": int(level),
                "side": str(side),
                "entry_px": float(pos.get("entry", 0.0) or 0.0),
                "stop_px": float(pos.get("stop", 0.0) or 0.0),
                "tp1_px": float(pos.get("tp1", 0.0) or 0.0),
                "tp2_px": float(pos.get("tp2", 0.0) or 0.0),
                "risk_px": float(pos.get("risk", 0.0) or 0.0),
                "entry_exec_mode": entry_exec_mode_used,
            }
        )

        st = by_inst_stats[inst]
        st["n"] += 1.0
        st["r_sum"] += float(r_adj)
        st["r_raw_sum"] += float(r_raw)
        st["friction_r_sum"] += float(friction_r)
        total_raw_r += float(r_raw)
        total_friction_r += float(friction_r)
        by_side[str(side)] += 1
        by_entry_exec_mode[entry_exec_mode_used] += 1
        update_level_perf(level_perf, int(level), str(outcome_k), float(r_adj))

        if outcome_k in {"TP1", "TP2"}:
            st["win"] += 1.0
        if outcome_k == "TP2":
            st["tp2"] += 1.0
            st["tp1"] += 1.0
            tp2_count += 1
            tp1_count += 1
        elif outcome_k == "TP1":
            st["tp1"] += 1.0
            tp1_count += 1
        elif outcome_k == "STOP":
            st["stop"] += 1.0
            stop_count += 1
        else:
            st["none"] += 1.0
            none_count += 1

        _sg_record(inst, side, bool(is_stop), int(exit_ts))

        if equity > peak_equity:
            peak_equity = equity
            peak_ts = int(exit_ts)
        dd = 0.0 if peak_equity <= 0 else (peak_equity - equity) / peak_equity
        if dd > max_dd:
            max_dd = dd
            max_dd_peak_ts = peak_ts
            max_dd_trough_ts = int(exit_ts)

    def _can_open(inst: str, side: str, level: int, ts: int, candidate_loss_usdt: float = 0.0) -> bool:
        nonlocal skip_gap, skip_inst_cap, skip_global_cap, skip_loss_guard
        prune_ts_deque_window(global_open_ts, ts, window_ms)
        q_inst = inst_open_ts[inst]
        prune_ts_deque_window(q_inst, ts, window_ms)

        last_ts = inst_last_open_ts.get(inst)
        remain_min = min_open_gap_remaining_minutes(ts, last_ts, min_gap_minutes)
        if remain_min > 0:
            skip_gap += 1
            return False
        if is_open_limit_reached(len(q_inst), per_inst_cap):
            skip_inst_cap += 1
            return False
        if is_open_limit_reached(len(global_open_ts), global_cap):
            skip_global_cap += 1
            return False
        if not _sg_allow(inst, side, level, int(ts)):
            return False

        if loss_limit_pct > 0:
            rolling_loss = sum(x[1] for x in loss_events)
            open_risk = _sum_open_positions_potential_loss(open_positions)
            projected_loss = float(rolling_loss) + float(open_risk) + max(0.0, float(candidate_loss_usdt))
            base_val = resolve_loss_base(loss_base_mode, float(equity), float(loss_base_fixed))
            limit_val = float(base_val) * float(loss_limit_pct)
            if projected_loss > limit_val + 1e-12:
                skip_loss_guard += 1
                return False
        return True

    def _try_open(inst: str, row: Dict[str, Any], ts: int, i: int, decision: Optional[Any], *, is_reverse: bool) -> bool:
        nonlocal open_positions_total, max_concurrent, skip_miss, skip_limit_unfilled
        if decision is None:
            return False
        side = str(decision.side)
        level = int(decision.level)
        risk_amt = max(0.0, float(equity) * risk_frac)
        if risk_amt <= 0:
            return False
        if not _can_open(inst, side, level, ts, candidate_loss_usdt=float(risk_amt)):
            return False
        if miss_prob > 0:
            miss_key = f"{inst}|{int(ts)}|{side}|{int(level)}|{'rev' if is_reverse else 'std'}"
            if stable_u01(miss_key) < miss_prob:
                skip_miss += 1
                return False

        intended_exec_mode = resolve_entry_exec_mode(entry_exec_mode, int(level), entry_auto_market_level_min)
        effective_exec_mode = intended_exec_mode
        fee_rate_used = fee_rate
        slippage_bps_used = slippage_bps

        if intended_exec_mode == "limit":
            no_fill = False
            if miss_prob > 0:
                no_fill_key = f"{inst}|{int(ts)}|{side}|{int(level)}|limit_nofill|{'rev' if is_reverse else 'std'}"
                no_fill = stable_u01(no_fill_key) < miss_prob
            if no_fill:
                if entry_limit_fallback_mode == "skip":
                    skip_limit_unfilled += 1
                    return False
                effective_exec_mode = "limit_fallback_market"
                fee_rate_used = fee_rate
                slippage_bps_used = slippage_bps
            else:
                effective_exec_mode = "limit"
                fee_rate_used = entry_limit_fee_rate
                slippage_bps_used = entry_limit_slippage_bps

        pos = _new_sim_position(
            decision=decision,
            entry_ts=int(ts),
            entry_i=int(i),
            risk_amt=float(risk_amt),
        )
        pos["entry_exec_mode"] = str(effective_exec_mode)
        pos["entry_exec_mode_intended"] = str(intended_exec_mode)
        pos["fee_rate"] = float(fee_rate_used)
        pos["slippage_bps"] = float(slippage_bps_used)
        open_positions[inst] = pos
        open_positions_total += 1

        q_inst = inst_open_ts[inst]
        q_inst.append(ts)
        global_open_ts.append(ts)
        inst_last_open_ts[inst] = ts
        by_level[int(level)] += 1
        if open_positions_total > max_concurrent:
            max_concurrent = open_positions_total
        return True

    for step, ts in enumerate(timeline, 1):
        prune_loss_deque_window(loss_events, ts, loss_window_ms)

        for inst in inst_ids:
            row = data[inst]
            i = row["ts_to_i"].get(ts)
            if i is None:
                continue

            hi_idx = bisect.bisect_right(row["htf_ts"], int(row["ltf_ts"][i])) - 1
            li_idx = bisect.bisect_right(row["loc_ts"], int(row["ltf_ts"][i])) - 1
            if hi_idx < 0 or li_idx < 0:
                continue

            inst_params = row["params"]
            sig = _build_backtest_signal_fast(row["pre_by_profile"][row["profile_id"]], inst_params, hi_idx + 1, li_idx + 1, i)
            if sig is None:
                continue
            inst_exec_max_level = int(row["exec_max_level"])
            if bool(row.get("vote_enabled")):
                signals_by_profile: Dict[str, Dict[str, Any]] = {row["profile_id"]: sig}
                decisions_by_profile: Dict[str, Any] = {}
                decisions_by_profile[row["profile_id"]] = resolve_entry_decision(
                    sig,
                    max_level=inst_exec_max_level,
                    min_level=min_level,
                    exact_level=exact_level,
                    tp1_r=inst_params.tp1_r_mult,
                    tp2_r=inst_params.tp2_r_mult,
                    tp1_only=tp1_only,
                )
                for pid in row.get("profile_ids", []):
                    if pid == row["profile_id"]:
                        continue
                    pre_other = row["pre_by_profile"].get(pid)
                    if pre_other is None:
                        continue
                    p = cfg.strategy_profiles.get(pid, cfg.params)
                    sig_other = _build_backtest_signal_fast(pre_other, p, hi_idx + 1, li_idx + 1, i)
                    if sig_other is None:
                        continue
                    signals_by_profile[pid] = sig_other
                    decisions_by_profile[pid] = resolve_entry_decision(
                        sig_other,
                        max_level=resolve_exec_max_level(p, inst),
                        min_level=min_level,
                        exact_level=exact_level,
                        tp1_r=p.tp1_r_mult,
                        tp2_r=p.tp2_r_mult,
                        tp1_only=tp1_only,
                    )
                sig, _vote_meta = merge_entry_votes(
                    base_signal=sig,
                    profile_ids=[pid for pid in row.get("profile_ids", []) if pid in signals_by_profile],
                    signals_by_profile=signals_by_profile,
                    decisions_by_profile=decisions_by_profile,
                    mode=cfg.strategy_profile_vote_mode,
                    min_agree=cfg.strategy_profile_vote_min_agree,
                    enforce_max_level=inst_exec_max_level,
                    profile_score_map=cfg.strategy_profile_vote_score_map,
                    level_weight=cfg.strategy_profile_vote_level_weight,
                )

            decision = resolve_entry_decision(
                sig,
                max_level=inst_exec_max_level,
                min_level=min_level,
                exact_level=exact_level,
                tp1_r=inst_params.tp1_r_mult,
                tp2_r=inst_params.tp2_r_mult,
                tp1_only=tp1_only,
            )
            existing = open_positions.get(inst)
            if existing is not None:
                sim = _simulate_live_position_step(
                    pos=existing,
                    sig=sig,
                    params=inst_params,
                    decision=decision,
                    allow_reverse=bool(cfg.params.allow_reverse),
                    managed_exit=bool(managed_exit),
                    use_signal_exit=bool(use_signal_exit),
                )
                if bool(sim.get("closed", False)):
                    open_positions.pop(inst, None)
                    open_positions_total = max(0, open_positions_total - 1)
                    _record_close(
                        inst=inst,
                        pos=existing,
                        exit_ts=int(ts),
                        outcome=str(sim.get("outcome", "NONE")),
                        r_raw=float(sim.get("r_raw", 0.0) or 0.0),
                        is_stop=bool(sim.get("is_stop", False)),
                    )
                    rev = sim.get("reverse_decision")
                    if rev is not None:
                        _try_open(inst, row, int(ts), int(i), rev, is_reverse=True)
                continue

            _try_open(inst, row, int(ts), int(i), decision, is_reverse=False)

        if step % 10000 == 0:
            print(
                f"progress {step}/{len(timeline)} | open={open_positions_total} equity={equity:.2f} accepted={len(accepted)}",
                flush=True,
            )

    for inst in list(inst_ids):
        pos = open_positions.get(inst)
        if pos is None:
            continue
        row = data[inst]
        last_i = len(row["ltf"]) - 1
        if last_i < 0:
            continue
        close_px = float(row["ltf"][last_i].close)
        entry = float(pos.get("entry", close_px) or close_px)
        risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
        qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
        r_raw = float(pos.get("realized_r", 0.0) or 0.0)
        if qty_rem > 0:
            r_raw += qty_rem * _close_remaining_r(str(pos.get("side", "")), entry, risk, close_px)
        outcome = "TP1" if bool(pos.get("tp1_done", False)) else "NONE"
        open_positions.pop(inst, None)
        open_positions_total = max(0, open_positions_total - 1)
        _record_close(
            inst=inst,
            pos=pos,
            exit_ts=int(row["ltf_ts"][last_i]),
            outcome=str(outcome),
            r_raw=float(r_raw),
            is_stop=False,
        )

    if not accepted:
        text = f"【{args.title}】无有效成交信号。"
        print(text, flush=True)
        sent = send_telegram(cfg, text)
        print(f"telegram_sent={sent}", flush=True)
        return 0

    accepted.sort(key=lambda x: (x["exit_ts"], x["entry_ts"]))
    dump_trades_csv(args.dump_trades_csv, accepted)
    signals = len(accepted)
    wins = sum(1 for t in accepted if t["outcome"] in {"TP1", "TP2"})
    stops = sum(1 for t in accepted if t["outcome"] == "STOP")
    win_rate = wins / signals * 100.0
    avg_r = sum(t["r"] for t in accepted) / signals
    avg_raw_r = sum(t.get("r_raw", t["r"]) for t in accepted) / signals
    avg_friction_r = sum(t.get("friction_r", 0.0) for t in accepted) / signals
    ret_pct = (equity / start_equity - 1.0) * 100.0

    cur_ls = 0
    worst_ls = 0
    cur_ws = 0
    best_ws = 0
    for t in accepted:
        if t["pnl"] < 0:
            cur_ls += 1
            if cur_ls > worst_ls:
                worst_ls = cur_ls
            cur_ws = 0
        else:
            cur_ls = 0
            if t["outcome"] in {"TP1", "TP2"}:
                cur_ws += 1
                if cur_ws > best_ws:
                    best_ws = cur_ws
            else:
                cur_ws = 0

    start_ts = min(t["entry_ts"] for t in accepted)
    end_ts = max(t["exit_ts"] for t in accepted)
    period_days = (end_ts - start_ts) / 1000 / 86400
    elapsed_s = float(time.monotonic() - run_start)
    level_perf_final = finalize_level_perf(level_perf)

    per_inst_rows: List[Dict[str, Any]] = []
    for inst in inst_ids:
        st = by_inst_stats.get(inst)
        if not st or st["n"] <= 0:
            continue
        n = int(st["n"])
        per_inst_rows.append(
            {
                "inst_id": inst,
                "status": "ok",
                "error": "",
                "signals": n,
                "tp1": int(st.get("tp1", 0.0)),
                "tp2": int(st.get("tp2", 0.0)),
                "stop": int(st.get("stop", 0.0)),
                "none": int(st.get("none", 0.0)),
                "avg_r": (float(st["r_sum"]) / float(st["n"])) if float(st["n"]) > 0 else 0.0,
            }
        )

    std_result = normalize_backtest_result(
        {
            "max_level": max_level,
            "min_level": min_level,
            "exact_level": exact_level,
            "bars": bars,
            "horizon_bars": horizon_bars,
            "inst_ids": list(inst_ids),
            "signals": signals,
            "tp1": tp1_count,
            "tp2": tp2_count,
            "stop": stop_count,
            "none": none_count,
            "skip_gap": skip_gap,
            "skip_daycap": (skip_inst_cap + skip_global_cap),
            "skip_unresolved": skip_unresolved,
            "avg_r": avg_r,
            "by_level": dict(by_level),
            "by_side": {"LONG": int(by_side.get("LONG", 0)), "SHORT": int(by_side.get("SHORT", 0))},
            "level_perf": level_perf_final,
            "elapsed_s": elapsed_s,
            "per_inst": per_inst_rows,
        }
    )

    summary: List[str] = []
    summary.append(f"【{args.title}】")
    summary.append(f"完成时间：{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary.append(f"区间：{ts_fmt(start_ts)} -> {ts_fmt(end_ts)}（{period_days:.1f}天）")
    summary.append(f"标的：{','.join(inst_ids)}")
    summary.append(
        f"层级控制：base_exec_max={max_level} "
        f"l3_whitelist={','.join(sorted(l3_inst_set)) if l3_inst_set else '-'}"
    )
    summary.append(
        f"约束：min_gap={min_gap_minutes}m inst_cap={per_inst_cap}/24h global_cap={global_cap}/24h "
        f"loss_guard={loss_limit_pct*100:.2f}%({loss_base_mode},projected) "
        f"tp1_only={tp1_only} managed_exit={managed_exit}"
    )
    summary.append(
        f"复利：risk={risk_frac*100:.2f}%/单 start={start_equity:.2f} final={equity:.2f} "
        f"return={ret_pct:.2f}% maxDD={max_dd*100:.2f}%"
    )
    summary.append(
        f"结果：signals={signals} wins={wins} stops={stops} win_rate={win_rate:.2f}% "
        f"avgR={avg_r:.3f} max_concurrent={max_concurrent} worst_ls={worst_ls} best_ws={best_ws}"
    )
    summary.append(
        f"R分解：raw_avgR={avg_raw_r:.3f} friction_avgR={avg_friction_r:.3f} adj_avgR={avg_r:.3f}"
    )
    summary.append(
        f"入场执行：mode={entry_exec_mode} auto_market_level_min={entry_auto_market_level_min} "
        f"limit_fallback={entry_limit_fallback_mode} "
        f"limit_fee_rate={entry_limit_fee_rate:.5f} limit_slippage_bps={entry_limit_slippage_bps:.2f}"
    )
    summary.append(f"标准汇总：{format_backtest_result_line(std_result)}")
    if str(args.dump_trades_csv or "").strip():
        summary.append(f"trades_csv={args.dump_trades_csv}")
    summary.append(
        f"levels：L1={by_level.get(1,0)} L2={by_level.get(2,0)} L3={by_level.get(3,0)} "
        f"skip_gap={skip_gap} skip_inst_cap={skip_inst_cap} skip_global_cap={skip_global_cap} "
        f"skip_loss_guard={skip_loss_guard} skip_stop_guard={skip_stop_guard} "
        f"skip_miss={skip_miss} skip_unresolved={skip_unresolved}"
    )
    summary.append(
        f"entry_modes：market={by_entry_exec_mode.get('market',0)} "
        f"limit={by_entry_exec_mode.get('limit',0)} "
        f"limit_fallback_market={by_entry_exec_mode.get('limit_fallback_market',0)} "
        f"skip_limit_unfilled={skip_limit_unfilled}"
    )
    for inst in inst_ids:
        st = by_inst_stats.get(inst)
        if not st or st["n"] <= 0:
            continue
        n = int(st["n"])
        win = int(st["win"])
        avg_inst_r = st["r_sum"] / st["n"]
        summary.append(f"- {inst}: {n}单 胜率={win/n*100:.1f}% avgR={avg_inst_r:.3f}")

    text = "\n".join(summary)
    print("\n=== RESULT ===", flush=True)
    print(text, flush=True)

    sent = send_telegram(cfg, text)
    print(f"telegram_sent={sent}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("Unhandled exception in interleaved backtest runner:", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
