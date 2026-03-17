from __future__ import annotations

import bisect
import datetime as dt
import math
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

from .backtest_report import (
    finalize_level_perf,
    format_backtest_inst_line,
    format_backtest_result_line,
    level_perf_brief,
    new_level_perf,
    rate_str,
    update_level_perf,
)
from .common import bar_to_seconds, format_duration, log, make_progress_bar, truncate_text
from .config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    resolve_exec_max_level,
)
from .decision_core import resolve_entry_decision
from .indicators import atr, bollinger, ema, macd, rolling_high, rolling_low, rsi
from .models import Candle, Config, StrategyParams
from .okx_client import OKXClient
from .profile_vote import merge_entry_votes
from .signals import build_signals
from .strategy_contract import VariantSignalInputs
from .strategy_variant import resolve_variant_signal_state_from_inputs


def _close_remaining_r(side: str, entry: float, risk: float, close_px: float) -> float:
    side_u = str(side).strip().upper()
    if side_u == "LONG":
        return (float(close_px) - float(entry)) / float(risk)
    return (float(entry) - float(close_px)) / float(risk)


def _signal_high_low(sig: Dict[str, Any], close_px: float) -> Tuple[float, float]:
    hi = float(sig.get("high", close_px) or close_px)
    lo = float(sig.get("low", close_px) or close_px)
    hi = max(hi, close_px)
    lo = min(lo, close_px)
    if hi < lo:
        hi, lo = lo, hi
    return hi, lo


def _simulate_live_managed_step(
    *,
    side: str,
    entry: float,
    risk: float,
    tp1: float,
    tp2: float,
    pos: Dict[str, Any],
    sig: Dict[str, Any],
    tp1_close_pct: float,
    tp2_close_rest: bool,
    be_trigger_r_mult: float,
    be_offset_pct: float,
    be_fee_buffer_pct: float,
    auto_tighten_stop: bool,
    trail_after_tp1: bool,
    trail_atr_mult: float,
    signal_exit_enabled: bool,
    split_tp_enabled: bool,
) -> Dict[str, Any]:
    side_u = str(side).strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

    close = float(sig.get("close", entry) or entry)
    high, low = _signal_high_low(sig, close)
    qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
    realized_r = float(pos.get("realized_r", 0.0) or 0.0)
    tp1_done = bool(pos.get("tp1_done", False))
    be_armed = bool(pos.get("be_armed", False))
    hard_stop = float(pos.get("hard_stop", pos.get("stop", entry)) or entry)
    be_total_offset = max(0.0, float(be_offset_pct) + float(be_fee_buffer_pct))
    be_trigger_r = max(0.0, float(be_trigger_r_mult))
    tp1_pct = min(max(float(tp1_close_pct), 0.0), 1.0)
    use_tp2 = bool(tp2_close_rest and tp1_pct < 0.999)

    def _close_now(*, outcome: str, close_px: float, is_stop: bool) -> Dict[str, Any]:
        nonlocal realized_r
        realized_r += qty_rem * _close_remaining_r(side_u, entry, risk, close_px)
        pos["qty_rem"] = 0.0
        pos["realized_r"] = realized_r
        return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": bool(is_stop)}

    def _finalize_open_state(*, next_qty: float, next_realized_r: float, next_tp1_done: bool, next_be_armed: bool, next_hard_stop: float) -> None:
        pos["qty_rem"] = max(0.0, float(next_qty))
        pos["realized_r"] = float(next_realized_r)
        pos["tp1_done"] = bool(next_tp1_done)
        pos["be_armed"] = bool(next_be_armed)
        pos["hard_stop"] = float(next_hard_stop)

    if side_u == "LONG":
        if split_tp_enabled:
            active_stop = float(hard_stop)
            if not tp1_done:
                stop_hit = low <= active_stop
                tp1_hit = high >= tp1 and tp1_pct > 0.0
                tp2_hit = use_tp2 and high >= tp2
                if stop_hit and (tp1_hit or tp2_hit):
                    return _close_now(outcome="STOP", close_px=active_stop, is_stop=True)
                if stop_hit:
                    return _close_now(outcome="STOP", close_px=active_stop, is_stop=True)
                if tp1_hit:
                    close_qty = qty_rem * tp1_pct
                    if close_qty > 0:
                        realized_r += close_qty * _close_remaining_r("LONG", entry, risk, tp1)
                        qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    if qty_rem <= 1e-9:
                        pos["qty_rem"] = 0.0
                        pos["realized_r"] = realized_r
                        pos["tp1_done"] = True
                        pos["be_armed"] = True
                        return {"closed": True, "outcome": "TP1", "r_raw": float(realized_r), "is_stop": False}
                    if tp2_hit:
                        realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, tp2)
                        qty_rem = 0.0
                        pos["qty_rem"] = 0.0
                        pos["realized_r"] = realized_r
                        pos["tp1_done"] = True
                        pos["be_armed"] = True
                        return {"closed": True, "outcome": "TP2", "r_raw": float(realized_r), "is_stop": False}

            if tp1_done and qty_rem > 0:
                stop_hit = low <= active_stop
                tp2_hit = use_tp2 and high >= tp2
                if stop_hit and tp2_hit:
                    return _close_now(outcome="TP1", close_px=active_stop, is_stop=True)
                if stop_hit:
                    return _close_now(outcome="TP1", close_px=active_stop, is_stop=True)
                if tp2_hit:
                    return _close_now(outcome="TP2", close_px=tp2, is_stop=False)

            peak = max(float(pos.get("peak_price", entry) or entry), close)
            pos["peak_price"] = peak
            if bool(auto_tighten_stop) and (not be_armed) and close >= entry + risk * be_trigger_r:
                be_armed = True

            next_stop = float(active_stop)
            if bool(auto_tighten_stop):
                next_stop = max(next_stop, float(sig.get("long_stop", active_stop) or active_stop))
                if be_armed:
                    next_stop = max(next_stop, entry * (1.0 + be_total_offset))
                if (not bool(trail_after_tp1)) or tp1_done:
                    atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
                    trail_stop = peak - atr_v * float(trail_atr_mult)
                    next_stop = max(next_stop, trail_stop)
            elif be_armed:
                next_stop = max(next_stop, entry * (1.0 + be_total_offset))

            _finalize_open_state(
                next_qty=qty_rem,
                next_realized_r=realized_r,
                next_tp1_done=tp1_done,
                next_be_armed=be_armed,
                next_hard_stop=next_stop,
            )

            long_exit = bool(sig.get("long_exit", False)) if signal_exit_enabled else False
            stop_hit = close <= next_stop
            if long_exit or stop_hit:
                realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                outcome = "TP1" if tp1_done else ("STOP" if stop_hit else "EXIT")
                return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": bool(stop_hit)}
            return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

        peak = max(float(pos.get("peak_price", entry) or entry), close)
        pos["peak_price"] = peak

        if bool(auto_tighten_stop) and (not be_armed) and close >= entry + risk * be_trigger_r:
            be_armed = True
            pos["be_armed"] = True

        if (not tp1_done) and tp1_pct > 0:
            if close >= tp1:
                close_qty = qty_rem * tp1_pct
                if close_qty >= qty_rem * 0.999:
                    realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                    pos["qty_rem"] = 0.0
                    pos["realized_r"] = realized_r
                    return {"closed": True, "outcome": "TP1", "r_raw": float(realized_r), "is_stop": False}
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
                    return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

        if use_tp2 and tp1_done and qty_rem > 0:
            if close >= tp2:
                realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                return {"closed": True, "outcome": "TP2", "r_raw": float(realized_r), "is_stop": False}

        dynamic_stop = float(hard_stop)
        if bool(auto_tighten_stop):
            dynamic_stop = max(dynamic_stop, float(sig.get("long_stop", hard_stop) or hard_stop))
            if be_armed:
                dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
            if (not bool(trail_after_tp1)) or tp1_done:
                atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
                trail_stop = peak - atr_v * float(trail_atr_mult)
                dynamic_stop = max(dynamic_stop, trail_stop)
        elif be_armed:
            dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))

        stop_hit = close <= dynamic_stop
        long_exit = bool(sig.get("long_exit", False)) if signal_exit_enabled else False
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
            return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": bool(stop_hit)}
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

    if split_tp_enabled:
        active_stop = float(hard_stop)
        if not tp1_done:
            stop_hit = high >= active_stop
            tp1_hit = low <= tp1 and tp1_pct > 0.0
            tp2_hit = use_tp2 and low <= tp2
            if stop_hit and (tp1_hit or tp2_hit):
                return _close_now(outcome="STOP", close_px=active_stop, is_stop=True)
            if stop_hit:
                return _close_now(outcome="STOP", close_px=active_stop, is_stop=True)
            if tp1_hit:
                close_qty = qty_rem * tp1_pct
                if close_qty > 0:
                    realized_r += close_qty * _close_remaining_r("SHORT", entry, risk, tp1)
                    qty_rem = max(0.0, qty_rem - close_qty)
                tp1_done = True
                be_armed = True
                if qty_rem <= 1e-9:
                    pos["qty_rem"] = 0.0
                    pos["realized_r"] = realized_r
                    pos["tp1_done"] = True
                    pos["be_armed"] = True
                    return {"closed": True, "outcome": "TP1", "r_raw": float(realized_r), "is_stop": False}
                if tp2_hit:
                    realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, tp2)
                    qty_rem = 0.0
                    pos["qty_rem"] = 0.0
                    pos["realized_r"] = realized_r
                    pos["tp1_done"] = True
                    pos["be_armed"] = True
                    return {"closed": True, "outcome": "TP2", "r_raw": float(realized_r), "is_stop": False}

        if tp1_done and qty_rem > 0:
            stop_hit = high >= active_stop
            tp2_hit = use_tp2 and low <= tp2
            if stop_hit and tp2_hit:
                return _close_now(outcome="TP1", close_px=active_stop, is_stop=True)
            if stop_hit:
                return _close_now(outcome="TP1", close_px=active_stop, is_stop=True)
            if tp2_hit:
                return _close_now(outcome="TP2", close_px=tp2, is_stop=False)

        trough = min(float(pos.get("trough_price", entry) or entry), close)
        pos["trough_price"] = trough
        if bool(auto_tighten_stop) and (not be_armed) and close <= entry - risk * be_trigger_r:
            be_armed = True

        next_stop = float(active_stop)
        if bool(auto_tighten_stop):
            next_stop = min(next_stop, float(sig.get("short_stop", active_stop) or active_stop))
            if be_armed:
                next_stop = min(next_stop, entry * (1.0 - be_total_offset))
            if (not bool(trail_after_tp1)) or tp1_done:
                atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
                trail_stop = trough + atr_v * float(trail_atr_mult)
                next_stop = min(next_stop, trail_stop)
        elif be_armed:
            next_stop = min(next_stop, entry * (1.0 - be_total_offset))

        _finalize_open_state(
            next_qty=qty_rem,
            next_realized_r=realized_r,
            next_tp1_done=tp1_done,
            next_be_armed=be_armed,
            next_hard_stop=next_stop,
        )

        short_exit = bool(sig.get("short_exit", False)) if signal_exit_enabled else False
        stop_hit = close >= next_stop
        if short_exit or stop_hit:
            realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            outcome = "TP1" if tp1_done else ("STOP" if stop_hit else "EXIT")
            return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": bool(stop_hit)}
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

    trough = min(float(pos.get("trough_price", entry) or entry), close)
    pos["trough_price"] = trough

    if (not be_armed) and close <= entry - risk * be_trigger_r:
        be_armed = True
        pos["be_armed"] = True

    if (not tp1_done) and tp1_pct > 0:
        if close <= tp1:
            close_qty = qty_rem * tp1_pct
            if close_qty >= qty_rem * 0.999:
                realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                return {"closed": True, "outcome": "TP1", "r_raw": float(realized_r), "is_stop": False}
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
                return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

    if use_tp2 and tp1_done and qty_rem > 0:
        if close <= tp2:
            realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            return {"closed": True, "outcome": "TP2", "r_raw": float(realized_r), "is_stop": False}

    dynamic_stop = float(hard_stop)
    if bool(auto_tighten_stop):
        dynamic_stop = min(dynamic_stop, float(sig.get("short_stop", hard_stop) or hard_stop))
        if be_armed:
            dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
        if (not bool(trail_after_tp1)) or tp1_done:
            atr_v = max(0.0, float(sig.get("atr", 0.0) or 0.0))
            trail_stop = trough + atr_v * float(trail_atr_mult)
            dynamic_stop = min(dynamic_stop, trail_stop)
    elif be_armed:
        dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))

    stop_hit = close >= dynamic_stop
    short_exit = bool(sig.get("short_exit", False)) if signal_exit_enabled else False
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
        return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": bool(stop_hit)}

    return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}


def eval_signal_outcome(
    side: str,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    ltf_candles: List[Candle],
    start_idx: int,
    horizon_bars: int,
    managed_exit: bool = False,
    tp1_close_pct: float = 0.5,
    tp2_close_rest: bool = False,
    be_trigger_r_mult: float = 1.0,
    be_offset_pct: float = 0.0,
    be_fee_buffer_pct: float = 0.0,
    signal_lookup: Optional[Callable[[int], Optional[Dict[str, Any]]]] = None,
    trail_after_tp1: bool = True,
    auto_tighten_stop: bool = True,
    trail_atr_mult: float = 0.0,
    signal_exit_enabled: bool = False,
    split_tp_enabled: bool = False,
) -> Tuple[str, float, int, int]:
    risk = abs(entry - stop)
    if risk <= 0:
        risk = max(abs(entry) * 0.0005, 1e-8)

    side_u = side.upper()
    if int(horizon_bars) <= 0:
        end_idx = len(ltf_candles) - 1
    else:
        end_idx = min(len(ltf_candles) - 1, start_idx + max(1, int(horizon_bars)))
    if not managed_exit:
        outcome = "NONE"
        exit_price = ltf_candles[end_idx].close
        exit_idx = end_idx

        for i in range(start_idx + 1, end_idx + 1):
            c = ltf_candles[i]
            hi = c.high
            lo = c.low
            if side_u == "LONG":
                stop_hit = lo <= stop
                tp2_hit = hi >= tp2
                tp1_hit = hi >= tp1
                if stop_hit and (tp1_hit or tp2_hit):
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if stop_hit:
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if tp2_hit:
                    outcome = "TP2"
                    exit_price = tp2
                    exit_idx = i
                    break
                if tp1_hit:
                    outcome = "TP1"
                    exit_price = tp1
                    exit_idx = i
                    break
            else:
                stop_hit = hi >= stop
                tp2_hit = lo <= tp2
                tp1_hit = lo <= tp1
                if stop_hit and (tp1_hit or tp2_hit):
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if stop_hit:
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if tp2_hit:
                    outcome = "TP2"
                    exit_price = tp2
                    exit_idx = i
                    break
                if tp1_hit:
                    outcome = "TP1"
                    exit_price = tp1
                    exit_idx = i
                    break

        if side_u == "LONG":
            r_value = (exit_price - entry) / risk
        else:
            r_value = (entry - exit_price) / risk
        held = max(0, exit_idx - start_idx)
        return outcome, r_value, held, exit_idx

    if signal_lookup is not None:
        pos: Dict[str, Any] = {
            "side": side_u,
            "entry": float(entry),
            "stop": float(stop),
            "risk": float(risk),
            "tp1": float(tp1),
            "tp2": float(tp2),
            "qty_rem": 1.0,
            "realized_r": 0.0,
            "tp1_done": False,
            "be_armed": False,
            "hard_stop": float(stop),
            "peak_price": float(entry),
            "trough_price": float(entry),
        }
        outcome = "NONE"
        exit_idx = end_idx
        for i in range(start_idx + 1, end_idx + 1):
            c = ltf_candles[i]
            sig_i = signal_lookup(i)
            sig: Dict[str, Any] = {
                "close": float(c.close),
                "high": float(c.high),
                "low": float(c.low),
                "atr": 0.0,
                "long_stop": float(pos.get("hard_stop", stop) or stop),
                "short_stop": float(pos.get("hard_stop", stop) or stop),
                "long_exit": False,
                "short_exit": False,
            }
            if isinstance(sig_i, dict):
                sig.update(sig_i)
                try:
                    sig["close"] = float(sig.get("close", c.close) or c.close)
                except Exception:
                    sig["close"] = float(c.close)
                try:
                    sig["high"] = float(sig.get("high", c.high) or c.high)
                except Exception:
                    sig["high"] = float(c.high)
                try:
                    sig["low"] = float(sig.get("low", c.low) or c.low)
                except Exception:
                    sig["low"] = float(c.low)
                try:
                    sig["atr"] = max(0.0, float(sig.get("atr", 0.0) or 0.0))
                except Exception:
                    sig["atr"] = 0.0
                try:
                    sig["long_stop"] = float(sig.get("long_stop", pos.get("hard_stop", stop)) or pos.get("hard_stop", stop))
                except Exception:
                    sig["long_stop"] = float(pos.get("hard_stop", stop) or stop)
                try:
                    sig["short_stop"] = float(sig.get("short_stop", pos.get("hard_stop", stop)) or pos.get("hard_stop", stop))
                except Exception:
                    sig["short_stop"] = float(pos.get("hard_stop", stop) or stop)
                sig["long_exit"] = bool(sig.get("long_exit", False)) if signal_exit_enabled else False
                sig["short_exit"] = bool(sig.get("short_exit", False)) if signal_exit_enabled else False

            res = _simulate_live_managed_step(
                side=side_u,
                entry=float(entry),
                risk=float(risk),
                tp1=float(tp1),
                tp2=float(tp2),
                pos=pos,
                sig=sig,
                tp1_close_pct=tp1_close_pct,
                tp2_close_rest=tp2_close_rest,
                be_trigger_r_mult=be_trigger_r_mult,
                be_offset_pct=be_offset_pct,
                be_fee_buffer_pct=be_fee_buffer_pct,
                trail_after_tp1=trail_after_tp1,
                auto_tighten_stop=auto_tighten_stop,
                trail_atr_mult=trail_atr_mult,
                signal_exit_enabled=signal_exit_enabled,
                split_tp_enabled=split_tp_enabled,
            )
            if bool(res.get("closed", False)):
                outcome = str(res.get("outcome", "NONE"))
                exit_idx = i
                held = max(0, exit_idx - start_idx)
                return outcome, float(res.get("r_raw", 0.0) or 0.0), held, exit_idx

        qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
        realized_r = float(pos.get("realized_r", 0.0) or 0.0)
        if qty_rem > 1e-9:
            last_close = float(ltf_candles[end_idx].close)
            realized_r += qty_rem * _close_remaining_r(side_u, float(entry), float(risk), last_close)
            if bool(pos.get("tp1_done", False)) and outcome == "NONE":
                outcome = "TP1"
        held = max(0, exit_idx - start_idx)
        return outcome, realized_r, held, exit_idx

    # Fallback managed-exit path without signal lookup keeps the original OHLC-based approximation.
    qty_rem = 1.0
    realized_r = 0.0
    outcome = "NONE"
    exit_idx = end_idx
    tp1_done = False
    be_armed = False
    be_trigger = max(0.0, float(be_trigger_r_mult))
    be_total_offset = max(0.0, float(be_offset_pct) + float(be_fee_buffer_pct))
    tp1_pct = min(1.0, max(0.0, float(tp1_close_pct)))
    use_tp2 = bool(tp2_close_rest and tp1_pct < 0.999)
    dynamic_stop = float(stop)
    peak = float(entry)
    trough = float(entry)

    for i in range(start_idx + 1, end_idx + 1):
        c = ltf_candles[i]
        hi = float(c.high)
        lo = float(c.low)
        close_px = float(c.close)
        sig_i = signal_lookup(i) if signal_lookup is not None else None
        sig_close = close_px
        sig_atr = 0.0
        sig_long_stop = float(dynamic_stop)
        sig_short_stop = float(dynamic_stop)
        sig_long_exit = False
        sig_short_exit = False
        if isinstance(sig_i, dict):
            try:
                sig_close = float(sig_i.get("close", close_px) or close_px)
            except Exception:
                sig_close = close_px
            try:
                sig_atr = max(0.0, float(sig_i.get("atr", 0.0) or 0.0))
            except Exception:
                sig_atr = 0.0
            try:
                sig_long_stop = float(sig_i.get("long_stop", dynamic_stop) or dynamic_stop)
            except Exception:
                sig_long_stop = float(dynamic_stop)
            try:
                sig_short_stop = float(sig_i.get("short_stop", dynamic_stop) or dynamic_stop)
            except Exception:
                sig_short_stop = float(dynamic_stop)
            sig_long_exit = bool(sig_i.get("long_exit", False)) if signal_exit_enabled else False
            sig_short_exit = bool(sig_i.get("short_exit", False)) if signal_exit_enabled else False

        if side_u == "LONG":
            peak = max(float(peak), float(sig_close))
            if bool(auto_tighten_stop) and (not be_armed) and hi >= entry + risk * be_trigger:
                be_armed = True
            if be_armed:
                dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))

            if not tp1_done:
                if lo <= dynamic_stop:
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "STOP"
                    exit_idx = i
                    break
                if hi >= tp1:
                    close_qty = qty_rem * tp1_pct
                    if close_qty > 0:
                        realized_r += close_qty * ((tp1 - entry) / risk)
                        qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                    if qty_rem <= 1e-9:
                        outcome = "TP1"
                        exit_idx = i
                        break
                    if use_tp2 and hi >= tp2:
                        realized_r += qty_rem * ((tp2 - entry) / risk)
                        qty_rem = 0.0
                        outcome = "TP2"
                        exit_idx = i
                        break
                    continue
                else:
                    if bool(auto_tighten_stop):
                        dynamic_stop = max(dynamic_stop, sig_long_stop)
                        if be_armed:
                            dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                        if (not bool(trail_after_tp1)) or tp1_done:
                            trail_stop = peak - sig_atr * float(trail_atr_mult)
                            dynamic_stop = max(dynamic_stop, trail_stop)
                    elif be_armed:
                        dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                    stop_hit_close = sig_close <= dynamic_stop
                    if sig_long_exit or stop_hit_close:
                        realized_r += qty_rem * ((sig_close - entry) / risk)
                        qty_rem = 0.0
                        outcome = "STOP" if stop_hit_close else "EXIT"
                        exit_idx = i
                        break
                    continue

            if tp1_done and qty_rem > 0:
                stop_hit = lo <= dynamic_stop
                tp2_hit = use_tp2 and hi >= tp2
                if stop_hit and tp2_hit:
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if stop_hit:
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if tp2_hit:
                    realized_r += qty_rem * ((tp2 - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP2"
                    exit_idx = i
                    break
                if bool(auto_tighten_stop):
                    dynamic_stop = max(dynamic_stop, sig_long_stop)
                    if be_armed:
                        dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                    if (not bool(trail_after_tp1)) or tp1_done:
                        trail_stop = peak - sig_atr * float(trail_atr_mult)
                        dynamic_stop = max(dynamic_stop, trail_stop)
                elif be_armed:
                    dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                stop_hit_close = sig_close <= dynamic_stop
                if sig_long_exit or stop_hit_close:
                    realized_r += qty_rem * ((sig_close - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1" if tp1_done else ("STOP" if stop_hit_close else "EXIT")
                    exit_idx = i
                    break
        else:
            trough = min(float(trough), float(sig_close))
            if bool(auto_tighten_stop) and (not be_armed) and lo <= entry - risk * be_trigger:
                be_armed = True
            if be_armed:
                dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))

            if not tp1_done:
                if hi >= dynamic_stop:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "STOP"
                    exit_idx = i
                    break
                if lo <= tp1:
                    close_qty = qty_rem * tp1_pct
                    if close_qty > 0:
                        realized_r += close_qty * ((entry - tp1) / risk)
                        qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                    if qty_rem <= 1e-9:
                        outcome = "TP1"
                        exit_idx = i
                        break
                    if use_tp2 and lo <= tp2:
                        realized_r += qty_rem * ((entry - tp2) / risk)
                        qty_rem = 0.0
                        outcome = "TP2"
                        exit_idx = i
                        break
                    continue
                else:
                    if bool(auto_tighten_stop):
                        dynamic_stop = min(dynamic_stop, sig_short_stop)
                        if be_armed:
                            dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                        if (not bool(trail_after_tp1)) or tp1_done:
                            trail_stop = trough + sig_atr * float(trail_atr_mult)
                            dynamic_stop = min(dynamic_stop, trail_stop)
                    elif be_armed:
                        dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                    stop_hit_close = sig_close >= dynamic_stop
                    if sig_short_exit or stop_hit_close:
                        realized_r += qty_rem * ((entry - sig_close) / risk)
                        qty_rem = 0.0
                        outcome = "STOP" if stop_hit_close else "EXIT"
                        exit_idx = i
                        break
                    continue

            if tp1_done and qty_rem > 0:
                stop_hit = hi >= dynamic_stop
                tp2_hit = use_tp2 and lo <= tp2
                if stop_hit and tp2_hit:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if stop_hit:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if tp2_hit:
                    realized_r += qty_rem * ((entry - tp2) / risk)
                    qty_rem = 0.0
                    outcome = "TP2"
                    exit_idx = i
                    break
                if bool(auto_tighten_stop):
                    dynamic_stop = min(dynamic_stop, sig_short_stop)
                    if be_armed:
                        dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                    if (not bool(trail_after_tp1)) or tp1_done:
                        trail_stop = trough + sig_atr * float(trail_atr_mult)
                        dynamic_stop = min(dynamic_stop, trail_stop)
                elif be_armed:
                    dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                stop_hit_close = sig_close >= dynamic_stop
                if sig_short_exit or stop_hit_close:
                    realized_r += qty_rem * ((entry - sig_close) / risk)
                    qty_rem = 0.0
                    outcome = "TP1" if tp1_done else ("STOP" if stop_hit_close else "EXIT")
                    exit_idx = i
                    break

    if qty_rem > 1e-9:
        last_close = float(ltf_candles[end_idx].close)
        if side_u == "LONG":
            realized_r += qty_rem * ((last_close - entry) / risk)
        else:
            realized_r += qty_rem * ((entry - last_close) / risk)
        if tp1_done and outcome == "NONE":
            outcome = "TP1"

    held = max(0, exit_idx - start_idx)
    return outcome, realized_r, held, exit_idx


def _rolling_max_inclusive(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 0:
        return out
    dq: deque = deque()
    for i, v in enumerate(values):
        start = i - window + 1
        while dq and dq[0] < start:
            dq.popleft()
        while dq and values[dq[-1]] <= v:
            dq.pop()
        dq.append(i)
        out[i] = values[dq[0]]
    return out


def _rolling_min_inclusive(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 0:
        return out
    dq: deque = deque()
    for i, v in enumerate(values):
        start = i - window + 1
        while dq and dq[0] < start:
            dq.popleft()
        while dq and values[dq[-1]] >= v:
            dq.pop()
        dq.append(i)
        out[i] = values[dq[0]]
    return out


def _rolling_recent_valid_avg(values: List[Optional[float]], window: int) -> List[float]:
    out: List[float] = [0.0] * len(values)
    if window <= 0:
        return out
    q: deque = deque()
    running = 0.0
    for i, v in enumerate(values):
        if v is not None and not math.isnan(v):
            q.append(float(v))
            running += float(v)
            if len(q) > window:
                running -= float(q.popleft())
        out[i] = (running / len(q)) if q else 0.0
    return out


def _rolling_avg_exclusive(values: List[float], window: int) -> List[float]:
    out: List[float] = [0.0] * len(values)
    if window <= 0:
        return out
    q: deque = deque()
    running = 0.0
    for i, v in enumerate(values):
        out[i] = (running / len(q)) if q else 0.0
        vv = float(v)
        q.append(vv)
        running += vv
        if len(q) > window:
            running -= float(q.popleft())
    return out


def _build_daily_hlc_refs(
    ts_ms: List[int],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]], List[float], List[float]]:
    day_ms = 86_400_000
    prev_day_high: List[Optional[float]] = [None] * len(ts_ms)
    prev_day_low: List[Optional[float]] = [None] * len(ts_ms)
    prev_day_close: List[Optional[float]] = [None] * len(ts_ms)
    day_high_so_far: List[float] = [0.0] * len(ts_ms)
    day_low_so_far: List[float] = [0.0] * len(ts_ms)

    daily_high: Dict[int, float] = {}
    daily_low: Dict[int, float] = {}
    daily_close: Dict[int, float] = {}

    for i, t in enumerate(ts_ms):
        day = int(t // day_ms)
        h = float(highs[i])
        l = float(lows[i])
        c = float(closes[i])
        prev_h = daily_high.get(day)
        prev_l = daily_low.get(day)
        daily_high[day] = h if prev_h is None else max(prev_h, h)
        daily_low[day] = l if prev_l is None else min(prev_l, l)
        daily_close[day] = c

    running_day = -1
    run_hi = 0.0
    run_lo = 0.0
    for i, t in enumerate(ts_ms):
        day = int(t // day_ms)
        h = float(highs[i])
        l = float(lows[i])
        if day != running_day:
            running_day = day
            run_hi = h
            run_lo = l
        else:
            run_hi = max(run_hi, h)
            run_lo = min(run_lo, l)
        day_high_so_far[i] = run_hi
        day_low_so_far[i] = run_lo

        prev_day = day - 1
        prev_day_high[i] = daily_high.get(prev_day)
        prev_day_low[i] = daily_low.get(prev_day)
        prev_day_close[i] = daily_close.get(prev_day)

    return prev_day_high, prev_day_low, prev_day_close, day_high_so_far, day_low_so_far


def _build_backtest_precalc(
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
) -> Dict[str, Any]:
    htf_closes = [c.close for c in htf_candles]
    htf_ema_fast = ema(htf_closes, p.htf_ema_fast_len)
    htf_ema_slow = ema(htf_closes, p.htf_ema_slow_len)
    htf_rsi_line = rsi(htf_closes, p.htf_rsi_len)

    loc_highs = [c.high for c in loc_candles]
    loc_lows = [c.low for c in loc_candles]
    loc_recent_bars = max(2, p.loc_recent_bars)

    loc_lookback_high = _rolling_max_inclusive(loc_highs, max(1, p.loc_lookback))
    loc_lookback_low = _rolling_min_inclusive(loc_lows, max(1, p.loc_lookback))
    loc_recent_high = _rolling_max_inclusive(loc_highs, loc_recent_bars)
    loc_recent_low = _rolling_min_inclusive(loc_lows, loc_recent_bars)
    loc_sr_ref_high = _rolling_max_inclusive(loc_highs, max(1, p.loc_sr_lookback))
    loc_sr_ref_low = _rolling_min_inclusive(loc_lows, max(1, p.loc_sr_lookback))

    closes = [c.close for c in ltf_candles]
    opens = [c.open for c in ltf_candles]
    highs = [c.high for c in ltf_candles]
    lows = [c.low for c in ltf_candles]
    volumes = [max(0.0, float(c.volume)) for c in ltf_candles]

    ema_line = ema(closes, p.ltf_ema_len)
    rsi_line = rsi(closes, p.rsi_len)
    _, _, macd_hist = macd(closes, p.macd_fast, p.macd_slow, p.macd_signal)
    atr_line = atr(highs, lows, closes, p.atr_len)
    bb_mid, bb_up, bb_low = bollinger(closes, p.bb_len, p.bb_mult)
    hh = rolling_high(highs, p.break_len)
    ll = rolling_low(lows, p.break_len)
    exit_low = rolling_low(lows, p.exit_len)
    exit_high = rolling_high(highs, p.exit_len)
    pullback_low = _rolling_min_inclusive(lows, max(1, p.pullback_lookback))
    pullback_high = _rolling_max_inclusive(highs, max(1, p.pullback_lookback))
    ltf_ts_ms = [int(c.ts_ms) for c in ltf_candles]
    prev_day_high, prev_day_low, prev_day_close, day_high_so_far, day_low_so_far = _build_daily_hlc_refs(
        ltf_ts_ms,
        highs,
        lows,
        closes,
    )

    bb_width: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        up = bb_up[i]
        lo = bb_low[i]
        mid = bb_mid[i]
        if up is None or lo is None or mid is None or mid == 0:
            continue
        bb_width[i] = (up - lo) / mid
    bb_width_avg = _rolling_recent_valid_avg(bb_width, 100)
    volume_avg = _rolling_avg_exclusive(volumes, 20)

    min_htf = max(p.htf_ema_slow_len + 2, p.htf_rsi_len + 2)
    min_loc = max(p.loc_lookback + 2, p.loc_recent_bars + 2, p.loc_sr_lookback + p.loc_recent_bars + 2)
    min_ltf = max(
        p.break_len + 2,
        p.exit_len + 2,
        p.ltf_ema_len + 2,
        p.bb_len + 2,
        p.rsi_len + 2,
        p.macd_slow + p.macd_signal + 5,
        p.pullback_lookback + 2,
        p.atr_len + 2,
    )

    return {
        "min_htf": min_htf,
        "min_loc": min_loc,
        "min_ltf": min_ltf,
        "loc_recent_bars": loc_recent_bars,
        "htf_closes": htf_closes,
        "htf_ema_fast": htf_ema_fast,
        "htf_ema_slow": htf_ema_slow,
        "htf_rsi": htf_rsi_line,
        "loc_lookback_high": loc_lookback_high,
        "loc_lookback_low": loc_lookback_low,
        "loc_recent_high": loc_recent_high,
        "loc_recent_low": loc_recent_low,
        "loc_sr_ref_high": loc_sr_ref_high,
        "loc_sr_ref_low": loc_sr_ref_low,
        "closes": closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "volume_avg": volume_avg,
        "ema_line": ema_line,
        "rsi_line": rsi_line,
        "macd_hist": macd_hist,
        "atr_line": atr_line,
        "bb_mid": bb_mid,
        "bb_up": bb_up,
        "bb_low": bb_low,
        "bb_width": bb_width,
        "bb_width_avg": bb_width_avg,
        "hh": hh,
        "ll": ll,
        "exit_low": exit_low,
        "exit_high": exit_high,
        "pullback_low": pullback_low,
        "pullback_high": pullback_high,
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
        "prev_day_close": prev_day_close,
        "day_high_so_far": day_high_so_far,
        "day_low_so_far": day_low_so_far,
    }


def _build_backtest_signal_fast(
    pre: Dict[str, Any],
    p: StrategyParams,
    hi: int,
    li: int,
    i: int,
) -> Optional[Dict[str, Any]]:
    if hi < int(pre["min_htf"]) or li < int(pre["min_loc"]) or (i + 1) < int(pre["min_ltf"]):
        return None

    hidx = hi - 1
    lcid = li - 1

    htf_closes = pre["htf_closes"]
    htf_ema_fast = pre["htf_ema_fast"]
    htf_ema_slow = pre["htf_ema_slow"]
    htf_rsi = pre["htf_rsi"]

    h_close = htf_closes[hidx]
    h_ema_fast = htf_ema_fast[hidx]
    h_ema_slow = htf_ema_slow[hidx]
    h_rsi = htf_rsi[hidx]
    if h_ema_fast is None or h_ema_slow is None or h_rsi is None:
        return None

    bias = "neutral"
    if h_close > h_ema_fast > h_ema_slow and h_rsi >= p.htf_rsi_long_min:
        bias = "long"
    elif h_close < h_ema_fast < h_ema_slow and h_rsi <= p.htf_rsi_short_max:
        bias = "short"

    loc_high = pre["loc_lookback_high"][lcid]
    loc_low = pre["loc_lookback_low"][lcid]
    loc_recent_low = pre["loc_recent_low"][lcid]
    loc_recent_high = pre["loc_recent_high"][lcid]
    if loc_high is None or loc_low is None or loc_recent_low is None or loc_recent_high is None:
        return None

    loc_range = max(float(loc_high) - float(loc_low), 1e-9)
    fib_low = min(p.location_fib_low, p.location_fib_high)
    fib_high = max(p.location_fib_low, p.location_fib_high)
    long_fib_zone_hi = float(loc_high) - loc_range * fib_low
    long_fib_zone_lo = float(loc_high) - loc_range * fib_high
    short_fib_zone_lo = float(loc_low) + loc_range * fib_low
    short_fib_zone_hi = float(loc_low) + loc_range * fib_high

    fib_touch_long = long_fib_zone_lo <= float(loc_recent_low) <= long_fib_zone_hi
    fib_touch_short = short_fib_zone_lo <= float(loc_recent_high) <= short_fib_zone_hi

    retest_long = False
    retest_short = False
    sr_end = li - int(pre["loc_recent_bars"])
    if sr_end > 1:
        sr_idx = sr_end - 1
        sr_ref_high = pre["loc_sr_ref_high"][sr_idx]
        sr_ref_low = pre["loc_sr_ref_low"][sr_idx]
        if sr_ref_high is not None and float(sr_ref_high) > 0:
            retest_long = abs(float(loc_recent_low) - float(sr_ref_high)) / float(sr_ref_high) <= p.location_retest_tol
        if sr_ref_low is not None and float(sr_ref_low) > 0:
            retest_short = abs(float(loc_recent_high) - float(sr_ref_low)) / float(sr_ref_low) <= p.location_retest_tol

    long_location_ok = fib_touch_long or retest_long
    short_location_ok = fib_touch_short or retest_short

    close = pre["closes"][i]
    em = pre["ema_line"][i]
    r = pre["rsi_line"][i]
    mh = pre["macd_hist"][i]
    a = pre["atr_line"][i]
    upper = pre["bb_up"][i]
    lower = pre["bb_low"][i]
    mid = pre["bb_mid"][i]
    hhv = pre["hh"][i]
    llv = pre["ll"][i]
    exl = pre["exit_low"][i]
    exh = pre["exit_high"][i]
    pb_low = pre["pullback_low"][i]
    pb_high = pre["pullback_high"][i]
    width = pre["bb_width"][i]
    if None in {em, r, mh, a, upper, lower, mid, hhv, llv, exl, exh, pb_low, pb_high, width}:
        return None

    width_avg = float(pre["bb_width_avg"][i])
    vol_ok = width_avg > 0 and float(width) > width_avg * p.bb_width_k

    pullback_long = float(pb_low) <= float(em) * (1.0 + p.pullback_tolerance)
    pullback_short = float(pb_high) >= float(em) * (1.0 - p.pullback_tolerance)
    not_chasing_long = close <= float(em) * (1.0 + p.max_chase_from_ema)
    not_chasing_short = close >= float(em) * (1.0 - p.max_chase_from_ema)

    prev_hhv = pre["hh"][i - 1] if i > 0 else None
    prev_llv = pre["ll"][i - 1] if i > 0 else None
    variant_inputs = VariantSignalInputs(
        p=p,
        bias=bias,
        close=float(close),
        ema_value=float(em),
        rsi_value=float(r),
        macd_hist_value=float(mh),
        atr_value=float(a),
        hhv=float(hhv),
        llv=float(llv),
        exl=float(exl),
        exh=float(exh),
        pb_low=float(pb_low),
        pb_high=float(pb_high),
        h_close=float(h_close),
        h_ema_fast=float(h_ema_fast),
        h_ema_slow=float(h_ema_slow),
        width=float(width),
        width_avg=float(width_avg),
        long_location_ok=bool(long_location_ok),
        short_location_ok=bool(short_location_ok),
        pullback_long=bool(pullback_long),
        pullback_short=bool(pullback_short),
        not_chasing_long=bool(not_chasing_long),
        not_chasing_short=bool(not_chasing_short),
        prev_hhv=float(prev_hhv) if prev_hhv is not None else None,
        prev_llv=float(prev_llv) if prev_llv is not None else None,
        current_high=float(pre["highs"][i]) if i >= 0 else None,
        current_low=float(pre["lows"][i]) if i >= 0 else None,
        prev_high=float(pre["highs"][i - 1]) if i >= 1 else None,
        prev_low=float(pre["lows"][i - 1]) if i >= 1 else None,
        prev2_high=float(pre["highs"][i - 2]) if i >= 2 else None,
        prev2_low=float(pre["lows"][i - 2]) if i >= 2 else None,
        prev3_high=float(pre["highs"][i - 3]) if i >= 3 else None,
        prev3_low=float(pre["lows"][i - 3]) if i >= 3 else None,
        current_open=float(pre["opens"][i]) if i >= 0 else None,
        prev_open=float(pre["opens"][i - 1]) if i >= 1 else None,
        prev_close=float(pre["closes"][i - 1]) if i >= 1 else None,
        upper_band=float(pre["bb_up"][i]) if pre["bb_up"][i] is not None else None,
        lower_band=float(pre["bb_low"][i]) if pre["bb_low"][i] is not None else None,
        mid_band=float(pre["bb_mid"][i]) if pre["bb_mid"][i] is not None else None,
        prev_macd_hist=float(pre["macd_hist"][i - 1]) if i >= 1 and pre["macd_hist"][i - 1] is not None else None,
        volume=float(pre["volumes"][i]) if i >= 0 else None,
        volume_avg=float(pre["volume_avg"][i]) if i >= 0 else None,
        prev_day_high=float(pre["prev_day_high"][i]) if pre["prev_day_high"][i] is not None else None,
        prev_day_low=float(pre["prev_day_low"][i]) if pre["prev_day_low"][i] is not None else None,
        prev_day_close=float(pre["prev_day_close"][i]) if pre["prev_day_close"][i] is not None else None,
        day_high_so_far=float(pre["day_high_so_far"][i]) if pre["day_high_so_far"][i] is not None else None,
        day_low_so_far=float(pre["day_low_so_far"][i]) if pre["day_low_so_far"][i] is not None else None,
    )
    variant_state = resolve_variant_signal_state_from_inputs(variant_inputs)
    long_level = int(variant_state["long_level"])
    short_level = int(variant_state["short_level"])
    long_stop = float(variant_state["long_stop"])
    short_stop = float(variant_state["short_stop"])

    long_exit = close < em or close < exl or mh < 0 or bias == "short"
    short_exit = close > em or close > exh or mh > 0 or bias == "long"

    return {
        "close": float(close),
        "high": float(pre["highs"][i]),
        "low": float(pre["lows"][i]),
        "ema": float(em),
        "atr": float(a),
        "macd_hist": float(mh),
        "bias": str(bias),
        "long_level": int(long_level),
        "short_level": int(short_level),
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
        "long_exit": bool(long_exit),
        "short_exit": bool(short_exit),
    }


def _new_level_perf() -> Dict[int, Dict[str, float]]:
    return new_level_perf()


def _update_level_perf(level_perf: Dict[int, Dict[str, float]], level: int, outcome: str, r_value: float) -> None:
    update_level_perf(level_perf, level, outcome, r_value)


def _finalize_level_perf(level_perf: Dict[int, Dict[str, float]]) -> Dict[int, Dict[str, float]]:
    return finalize_level_perf(level_perf)


def _level_perf_brief(level_perf_final: Dict[int, Dict[str, float]]) -> str:
    return level_perf_brief(level_perf_final)


def run_backtest(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    max_level: int,
    min_level: int = 1,
    exact_level: int = 0,
    bt_min_open_interval_minutes: int = 0,
    bt_max_opens_per_day: int = 0,
    bt_require_tp_sl: bool = False,
    bt_tp1_only: bool = False,
    bt_managed_exit: bool = False,
    history_cache: Optional[Dict[str, Tuple[List[Candle], List[Candle], List[Candle]]]] = None,
) -> Dict[str, Any]:
    bars = max(300, int(bars))
    horizon_bars = max(0, int(horizon_bars))
    max_level = max(1, min(3, int(max_level)))
    min_level = max(1, min(3, int(min_level)))
    if min_level > max_level:
        min_level = max_level
    exact_level = int(exact_level or 0)
    if exact_level < 0 or exact_level > 3:
        exact_level = 0
    bt_min_open_interval_minutes = max(0, int(bt_min_open_interval_minutes))
    bt_max_opens_per_day = max(0, int(bt_max_opens_per_day))
    bt_require_tp_sl = bool(bt_require_tp_sl)
    bt_tp1_only = bool(bt_tp1_only)
    bt_managed_exit = bool(bt_managed_exit)
    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)

    profile_ids_by_inst: Dict[str, List[str]] = {inst_id: get_strategy_profile_ids(cfg, inst_id) for inst_id in inst_ids}
    profile_by_inst: Dict[str, str] = {
        inst_id: (ids[0] if ids else get_strategy_profile_id(cfg, inst_id))
        for inst_id, ids in profile_ids_by_inst.items()
    }
    params_by_inst: Dict[str, StrategyParams] = {
        inst_id: cfg.strategy_profiles.get(profile_by_inst[inst_id], get_strategy_params(cfg, inst_id))
        for inst_id in inst_ids
    }

    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
    need_ltf = bars + 300
    all_params: List[StrategyParams] = []
    for inst_id in inst_ids:
        ids = profile_ids_by_inst.get(inst_id) or [profile_by_inst.get(inst_id, "DEFAULT")]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        max_loc_lookback = cfg.params.loc_lookback
        max_htf_ema_slow = cfg.params.htf_ema_slow_len
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

    horizon_desc = "to_end" if horizon_bars <= 0 else str(horizon_bars)
    log(
        f"Backtest start | insts={','.join(inst_ids)} htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} "
        f"bars={bars} horizon={horizon_desc} max_level={max_level} min_level={min_level} exact_level={exact_level} "
        f"min_gap={bt_min_open_interval_minutes}m day_cap={bt_max_opens_per_day} "
        f"require_tp_sl={bt_require_tp_sl} tp1_only={bt_tp1_only} managed_exit={bt_managed_exit}"
    )
    bt_start = time.monotonic()
    inst_total = max(1, len(inst_ids))

    total_signals = 0
    total_r = 0.0
    total_tp1 = 0
    total_tp2 = 0
    total_stop = 0
    total_none = 0
    total_skip_gap = 0
    total_skip_daycap = 0
    total_skip_unresolved = 0
    total_by_level = {1: 0, 2: 0, 3: 0}
    total_by_side = {"LONG": 0, "SHORT": 0}
    total_level_perf = _new_level_perf()
    per_inst: List[Dict[str, Any]] = []

    for inst_idx, inst_id in enumerate(inst_ids, 1):
        inst_start = time.monotonic()
        inst_params = params_by_inst.get(inst_id, cfg.params)
        profile_id = profile_by_inst.get(inst_id, "DEFAULT")
        inst_profile_ids = profile_ids_by_inst.get(inst_id) or [profile_id]
        if profile_id not in inst_profile_ids:
            inst_profile_ids = [profile_id] + [x for x in inst_profile_ids if x != profile_id]
        vote_enabled = len(inst_profile_ids) > 1
        profile_disp = profile_id if not vote_enabled else f"{profile_id}+VOTE({'+'.join(inst_profile_ids)})"
        cached = history_cache.get(inst_id) if history_cache is not None else None
        if cached is not None:
            htf, loc, ltf = cached
            log(
                f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | "
                f"profile={profile_disp} using cached candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}"
            )
        else:
            log(f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | profile={profile_disp} fetching history candles...")
            try:
                htf = client.get_candles_history(inst_id, cfg.htf_bar, need_htf)
                loc = client.get_candles_history(inst_id, cfg.loc_bar, need_loc)
                ltf = client.get_candles_history(inst_id, cfg.ltf_bar, need_ltf)
            except Exception as e:
                msg = str(e)
                log(f"[{inst_id}] Backtest data error: {msg}")
                per_inst.append(
                    {
                        "inst_id": inst_id,
                        "status": "error",
                        "error": msg,
                        "signals": 0,
                        "tp1": 0,
                        "tp2": 0,
                        "stop": 0,
                        "none": 0,
                        "avg_r": 0.0,
                        "by_level": {1: 0, 2: 0, 3: 0},
                        "by_side": {"LONG": 0, "SHORT": 0},
                        "level_perf": _finalize_level_perf(_new_level_perf()),
                        "elapsed_s": float(time.monotonic() - inst_start),
                    }
                )
                continue
            if history_cache is not None:
                history_cache[inst_id] = (htf, loc, ltf)

        if len(htf) < 50 or len(loc) < 120 or len(ltf) < 300:
            msg = f"data too short htf={len(htf)} loc={len(loc)} ltf={len(ltf)}"
            log(f"[{inst_id}] Backtest {msg}")
            per_inst.append(
                {
                    "inst_id": inst_id,
                    "status": "error",
                    "error": msg,
                    "signals": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "stop": 0,
                    "none": 0,
                    "avg_r": 0.0,
                    "by_level": {1: 0, 2: 0, 3: 0},
                    "by_side": {"LONG": 0, "SHORT": 0},
                    "level_perf": _finalize_level_perf(_new_level_perf()),
                    "elapsed_s": float(time.monotonic() - inst_start),
                }
            )
            continue
        if cached is None:
            log(f"[{inst_id}] history ready | htf={len(htf)} loc={len(loc)} ltf={len(ltf)}")

        pre_by_profile: Dict[str, Dict[str, Any]] = {}
        try:
            pre_by_profile[profile_id] = _build_backtest_precalc(htf, loc, ltf, inst_params)
            for pid in inst_profile_ids:
                if pid == profile_id:
                    continue
                p = cfg.strategy_profiles.get(pid, cfg.params)
                pre_by_profile[pid] = _build_backtest_precalc(htf, loc, ltf, p)
        except Exception as e:
            msg = f"precalc failed: {e}"
            log(f"[{inst_id}] Backtest {msg}")
            per_inst.append(
                {
                    "inst_id": inst_id,
                    "status": "error",
                    "error": msg,
                    "signals": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "stop": 0,
                    "none": 0,
                    "avg_r": 0.0,
                    "by_level": {1: 0, 2: 0, 3: 0},
                    "by_side": {"LONG": 0, "SHORT": 0},
                    "level_perf": _finalize_level_perf(_new_level_perf()),
                    "elapsed_s": float(time.monotonic() - inst_start),
                }
            )
            continue

        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]

        start_idx = max(0, len(ltf) - bars)
        bt_live_split_tp = bool(
            bt_managed_exit
            and bool(getattr(cfg, "attach_tpsl_on_entry", False))
            and bool(getattr(inst_params, "enable_close", False))
            and bool(getattr(inst_params, "split_tp_on_entry", False))
            and 0.0 < float(getattr(inst_params, "tp1_close_pct", 0.0) or 0.0) < 1.0
        )
        signal_decision_cache: Dict[int, Tuple[Optional[Dict[str, Any]], Optional[Any]]] = {}

        def _signal_and_decision_at(ltf_i: int) -> Tuple[Optional[Dict[str, Any]], Optional[Any]]:
            cached_pair = signal_decision_cache.get(ltf_i)
            if cached_pair is not None:
                return cached_pair

            ts_i = ltf_ts[ltf_i]
            hi = bisect.bisect_right(htf_ts, ts_i)
            li = bisect.bisect_right(loc_ts, ts_i)
            if hi <= 0 or li <= 0:
                result = (None, None)
                signal_decision_cache[ltf_i] = result
                return result

            sig_local = _build_backtest_signal_fast(pre_by_profile[profile_id], inst_params, hi, li, ltf_i)
            if sig_local is None:
                result = (None, None)
                signal_decision_cache[ltf_i] = result
                return result

            if vote_enabled:
                signals_by_profile: Dict[str, Dict[str, Any]] = {profile_id: sig_local}
                decisions_by_profile: Dict[str, Optional[Any]] = {}
                primary_exec_max = min(max_level, resolve_exec_max_level(inst_params, inst_id))
                decisions_by_profile[profile_id] = resolve_entry_decision(
                    sig_local,
                    max_level=primary_exec_max,
                    min_level=min_level,
                    exact_level=exact_level,
                    tp1_r=inst_params.tp1_r_mult,
                    tp2_r=inst_params.tp2_r_mult,
                    tp1_only=bt_tp1_only,
                )
                for pid in inst_profile_ids:
                    if pid == profile_id:
                        continue
                    p = cfg.strategy_profiles.get(pid, cfg.params)
                    pre_other = pre_by_profile.get(pid)
                    if pre_other is None:
                        continue
                    other_sig = _build_backtest_signal_fast(pre_other, p, hi, li, ltf_i)
                    if other_sig is None:
                        continue
                    signals_by_profile[pid] = other_sig
                    other_exec_max = min(max_level, resolve_exec_max_level(p, inst_id))
                    decisions_by_profile[pid] = resolve_entry_decision(
                        other_sig,
                        max_level=other_exec_max,
                        min_level=min_level,
                        exact_level=exact_level,
                        tp1_r=p.tp1_r_mult,
                        tp2_r=p.tp2_r_mult,
                        tp1_only=bt_tp1_only,
                    )
                sig_local, _vote_meta = merge_entry_votes(
                    base_signal=sig_local,
                    profile_ids=[pid for pid in inst_profile_ids if pid in signals_by_profile],
                    signals_by_profile=signals_by_profile,
                    decisions_by_profile=decisions_by_profile,
                    mode=cfg.strategy_profile_vote_mode,
                    min_agree=cfg.strategy_profile_vote_min_agree,
                    enforce_max_level=primary_exec_max,
                    profile_score_map=cfg.strategy_profile_vote_score_map,
                    level_weight=cfg.strategy_profile_vote_level_weight,
                )

            decision_local = resolve_entry_decision(
                sig_local,
                max_level=min(max_level, resolve_exec_max_level(inst_params, inst_id)),
                min_level=min_level,
                exact_level=exact_level,
                tp1_r=inst_params.tp1_r_mult,
                tp2_r=inst_params.tp2_r_mult,
                tp1_only=bt_tp1_only,
            )
            result = (sig_local, decision_local)
            signal_decision_cache[ltf_i] = result
            return result

        def _signal_lookup(ltf_i: int) -> Optional[Dict[str, Any]]:
            if ltf_i < 0 or ltf_i >= len(ltf):
                return None
            return _signal_and_decision_at(ltf_i)[0]
        sig_n = 0
        sum_r = 0.0
        tp1_n = 0
        tp2_n = 0
        stop_n = 0
        none_n = 0
        skip_gap_n = 0
        skip_daycap_n = 0
        skip_unresolved_n = 0
        by_level = {1: 0, 2: 0, 3: 0}
        by_side = {"LONG": 0, "SHORT": 0}
        level_perf = _new_level_perf()
        next_open_i = start_idx
        last_open_ts_ms: Optional[int] = None
        opens_per_day: Dict[str, int] = {}
        total_steps = max(1, (len(ltf) - 1) - start_idx)
        next_progress = 10

        for step_idx, i in enumerate(range(start_idx, len(ltf) - 1), 1):
            ts = ltf_ts[i]
            if i >= next_open_i:
                sig, decision = _signal_and_decision_at(i)
                if sig is not None and decision is not None:
                    side = decision.side
                    level = int(decision.level)
                    stop = float(decision.stop)
                    entry = float(decision.entry)
                    risk = float(decision.risk)
                    tp1 = float(decision.tp1)
                    tp2 = float(decision.tp2)
                    if risk > 0:
                        if bt_min_open_interval_minutes > 0 and last_open_ts_ms is not None:
                            if ts - last_open_ts_ms < bt_min_open_interval_minutes * 60 * 1000:
                                skip_gap_n += 1
                                goto_progress = True
                            else:
                                goto_progress = False
                        else:
                            goto_progress = False
                        if not goto_progress:
                            day_key = dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                            day_used = int(opens_per_day.get(day_key, 0))
                            if bt_max_opens_per_day > 0 and day_used >= bt_max_opens_per_day:
                                skip_daycap_n += 1
                            else:
                                outcome, r_value, _, exit_idx = eval_signal_outcome(
                                    side=side,
                                    entry=entry,
                                    stop=float(stop),
                                    tp1=tp1,
                                    tp2=tp2,
                                    ltf_candles=ltf,
                                    start_idx=i,
                                    horizon_bars=horizon_bars,
                                    managed_exit=bt_managed_exit,
                                    tp1_close_pct=inst_params.tp1_close_pct,
                                    tp2_close_rest=inst_params.tp2_close_rest,
                                    be_trigger_r_mult=inst_params.be_trigger_r_mult,
                                    be_offset_pct=inst_params.be_offset_pct,
                                    be_fee_buffer_pct=inst_params.be_fee_buffer_pct,
                                    signal_lookup=_signal_lookup if bt_managed_exit else None,
                                    trail_after_tp1=inst_params.trail_after_tp1,
                                    auto_tighten_stop=inst_params.auto_tighten_stop,
                                    trail_atr_mult=inst_params.trail_atr_mult,
                                    signal_exit_enabled=inst_params.signal_exit_enabled,
                                    split_tp_enabled=bt_live_split_tp,
                                )
                                if bt_tp1_only and outcome == "TP2":
                                    outcome = "TP1"
                                if bt_require_tp_sl and outcome not in {"TP1", "TP2", "STOP"}:
                                    skip_unresolved_n += 1
                                else:
                                    sig_n += 1
                                    sum_r += r_value
                                    by_level[level] = by_level.get(level, 0) + 1
                                    by_side[side] = by_side.get(side, 0) + 1
                                    _update_level_perf(level_perf, level, outcome, r_value)
                                    _update_level_perf(total_level_perf, level, outcome, r_value)
                                    if outcome == "TP2":
                                        tp2_n += 1
                                        tp1_n += 1
                                    elif outcome == "TP1":
                                        tp1_n += 1
                                    elif outcome == "STOP":
                                        stop_n += 1
                                    else:
                                        none_n += 1
                                    opens_per_day[day_key] = day_used + 1
                                    last_open_ts_ms = ts
                                    next_open_i = max(next_open_i, int(exit_idx) + 1)

            pct = int((step_idx * 100) / total_steps)
            if pct >= next_progress or step_idx == total_steps:
                elapsed = time.monotonic() - inst_start
                speed = step_idx / elapsed if elapsed > 0 else 0.0
                remain_steps = max(0, total_steps - step_idx)
                eta = (remain_steps / speed) if speed > 0 else 0.0
                bar = make_progress_bar(step_idx, total_steps, width=24)
                log(
                    f"[{inst_id}] progress {bar} {pct:3d}% ({step_idx}/{total_steps}) "
                    f"elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
                )
                while pct >= next_progress:
                    next_progress += 10

        avg_r = (sum_r / sig_n) if sig_n > 0 else 0.0
        tp1_rate = (tp1_n / sig_n * 100.0) if sig_n > 0 else 0.0
        tp2_rate = (tp2_n / sig_n * 100.0) if sig_n > 0 else 0.0
        stop_rate = (stop_n / sig_n * 100.0) if sig_n > 0 else 0.0
        level_perf_final = _finalize_level_perf(level_perf)

        log(
            f"[{inst_id}] backtest | signals={sig_n} L1/L2/L3={by_level.get(1,0)}/{by_level.get(2,0)}/{by_level.get(3,0)} "
            f"long/short={by_side.get('LONG',0)}/{by_side.get('SHORT',0)} "
            f"tp1={tp1_n}({tp1_rate:.1f}%) tp2={tp2_n}({tp2_rate:.1f}%) stop={stop_n}({stop_rate:.1f}%) "
            f"none={none_n} avgR={avg_r:.3f} level_avgR={_level_perf_brief(level_perf_final)} "
            f"skip_gap={skip_gap_n} skip_daycap={skip_daycap_n} skip_unresolved={skip_unresolved_n} "
            f"elapsed={format_duration(time.monotonic() - inst_start)}"
        )
        per_inst.append(
            {
                "inst_id": inst_id,
                "status": "ok",
                "error": "",
                "signals": sig_n,
                "tp1": tp1_n,
                "tp2": tp2_n,
                "stop": stop_n,
                "none": none_n,
                "avg_r": avg_r,
                "by_level": dict(by_level),
                "by_side": dict(by_side),
                "level_perf": level_perf_final,
                "skip_gap": int(skip_gap_n),
                "skip_daycap": int(skip_daycap_n),
                "skip_unresolved": int(skip_unresolved_n),
                "elapsed_s": float(time.monotonic() - inst_start),
            }
        )

        total_signals += sig_n
        total_r += sum_r
        total_tp1 += tp1_n
        total_tp2 += tp2_n
        total_stop += stop_n
        total_none += none_n
        total_skip_gap += int(skip_gap_n)
        total_skip_daycap += int(skip_daycap_n)
        total_skip_unresolved += int(skip_unresolved_n)
        total_by_level[1] += by_level.get(1, 0)
        total_by_level[2] += by_level.get(2, 0)
        total_by_level[3] += by_level.get(3, 0)
        total_by_side["LONG"] += by_side.get("LONG", 0)
        total_by_side["SHORT"] += by_side.get("SHORT", 0)

    elapsed_total = float(time.monotonic() - bt_start)
    total_level_perf_final = _finalize_level_perf(total_level_perf)
    result: Dict[str, Any] = {
        "max_level": max_level,
        "min_level": min_level,
        "exact_level": exact_level,
        "bars": bars,
        "horizon_bars": horizon_bars,
        "inst_ids": list(inst_ids),
        "signals": total_signals,
        "tp1": total_tp1,
        "tp2": total_tp2,
        "stop": total_stop,
        "none": total_none,
        "skip_gap": total_skip_gap,
        "skip_daycap": total_skip_daycap,
        "skip_unresolved": total_skip_unresolved,
        "avg_r": (total_r / total_signals) if total_signals > 0 else 0.0,
        "by_level": dict(total_by_level),
        "by_side": dict(total_by_side),
        "level_perf": total_level_perf_final,
        "bt_min_open_interval_minutes": bt_min_open_interval_minutes,
        "bt_max_opens_per_day": bt_max_opens_per_day,
        "bt_require_tp_sl": bt_require_tp_sl,
        "bt_tp1_only": bt_tp1_only,
        "bt_managed_exit": bt_managed_exit,
        "elapsed_s": elapsed_total,
        "per_inst": per_inst,
    }

    if total_signals <= 0:
        log(f"Backtest done | no signals found in selected range. elapsed={format_duration(elapsed_total)}")
        return result

    total_avg_r = total_r / total_signals
    total_tp1_rate = total_tp1 / total_signals * 100.0
    total_tp2_rate = total_tp2 / total_signals * 100.0
    total_stop_rate = total_stop / total_signals * 100.0
    log(
        f"Backtest total | signals={total_signals} L1/L2/L3={total_by_level[1]}/{total_by_level[2]}/{total_by_level[3]} "
        f"long/short={total_by_side['LONG']}/{total_by_side['SHORT']} "
        f"tp1={total_tp1}({total_tp1_rate:.1f}%) "
        f"tp2={total_tp2}({total_tp2_rate:.1f}%) stop={total_stop}({total_stop_rate:.1f}%) "
        f"none={total_none} avgR={total_avg_r:.3f} "
        f"skip_gap={total_skip_gap} skip_daycap={total_skip_daycap} skip_unresolved={total_skip_unresolved} "
        f"level_avgR={_level_perf_brief(total_level_perf_final)} elapsed={format_duration(elapsed_total)}"
    )
    return result


def run_backtest_compare(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    levels: List[int],
    min_level: int = 1,
    exact_level: int = 0,
    bt_min_open_interval_minutes: int = 0,
    bt_max_opens_per_day: int = 0,
    bt_require_tp_sl: bool = False,
    bt_tp1_only: bool = False,
    bt_managed_exit: bool = False,
) -> List[Dict[str, Any]]:
    picked = [lv for lv in levels if 1 <= int(lv) <= 3]
    if not picked:
        return []

    cache: Dict[str, Tuple[List[Candle], List[Candle], List[Candle]]] = {}
    results: List[Dict[str, Any]] = []
    total = len(picked)
    for idx, level in enumerate(picked, 1):
        log(f"Backtest compare | level={level} ({idx}/{total})")
        one = run_backtest(
            client=client,
            cfg=cfg,
            inst_ids=inst_ids,
            bars=bars,
            horizon_bars=horizon_bars,
            max_level=level,
            min_level=min_level,
            exact_level=exact_level,
            bt_min_open_interval_minutes=bt_min_open_interval_minutes,
            bt_max_opens_per_day=bt_max_opens_per_day,
            bt_require_tp_sl=bt_require_tp_sl,
            bt_tp1_only=bt_tp1_only,
            bt_managed_exit=bt_managed_exit,
            history_cache=cache,
        )
        results.append(one)
    return results


def _rate_str(numerator: int, denominator: int) -> str:
    return rate_str(numerator, denominator)


def _fmt_backtest_result_line(res: Dict[str, Any]) -> str:
    return format_backtest_result_line(res)


def build_backtest_telegram_summary(
    cfg: Config,
    results: List[Dict[str, Any]],
    title: str = "",
) -> str:
    lines: List[str] = []
    title_txt = title.strip()
    if title_txt:
        lines.append(f"【{title_txt}】")
    lines.append(f"回测完成：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"周期：HTF={cfg.htf_bar} LOC={cfg.loc_bar} LTF={cfg.ltf_bar}")

    if results:
        first = results[0]
        bars = int(first.get("bars", 0))
        horizon = int(first.get("horizon_bars", 0))
        min_level = int(first.get("min_level", 1))
        exact_level = int(first.get("exact_level", 0))
        bt_gap = int(first.get("bt_min_open_interval_minutes", 0))
        bt_day_cap = int(first.get("bt_max_opens_per_day", 0))
        bt_require_tp_sl = bool(first.get("bt_require_tp_sl", False))
        bt_tp1_only = bool(first.get("bt_tp1_only", False))
        bt_managed_exit = bool(first.get("bt_managed_exit", False))
        inst_ids = first.get("inst_ids", [])
        inst_txt = ",".join(inst_ids) if isinstance(inst_ids, list) and inst_ids else "-"
        lines.append(f"样本：bars={bars} horizon={horizon} insts={inst_txt}")
        lines.append(
            f"执行约束：min_gap={bt_gap}m day_cap={bt_day_cap} require_tp_sl={bt_require_tp_sl} "
            f"tp1_only={bt_tp1_only} managed_exit={bt_managed_exit}"
        )
        if exact_level in {1, 2, 3}:
            lines.append(f"筛选：exact_level={exact_level}")
        else:
            lines.append(f"筛选：min_level={min_level}（各行max/range见下）")

        for res in results:
            lines.append(_fmt_backtest_result_line(res))
            per_inst = res.get("per_inst", [])
            if not isinstance(per_inst, list):
                continue
            for row in per_inst:
                line = format_backtest_inst_line(row if isinstance(row, dict) else {})
                if line:
                    lines.append(line)
    else:
        lines.append("无可用回测结果。")

    return truncate_text("\n".join(lines), limit=3800)
