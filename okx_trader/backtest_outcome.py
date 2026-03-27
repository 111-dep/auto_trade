from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from .models import Candle


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


def resolve_backtest_split_tp_enabled(
    *,
    attach_tpsl_on_entry: bool,
    enable_close: bool,
    split_tp_on_entry: bool,
    tp1_close_pct: float,
    force_managed_tp_fallback: bool = False,
) -> bool:
    if bool(force_managed_tp_fallback):
        return False
    return bool(
        bool(attach_tpsl_on_entry)
        and bool(enable_close)
        and bool(split_tp_on_entry)
        and 0.0 < float(tp1_close_pct or 0.0) < 1.0
    )


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

    def _finalize_open_state(
        *,
        next_qty: float,
        next_realized_r: float,
        next_tp1_done: bool,
        next_be_armed: bool,
        next_hard_stop: float,
    ) -> None:
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
            if long_exit:
                realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
                pos["qty_rem"] = 0.0
                pos["realized_r"] = realized_r
                outcome = "TP1" if tp1_done else "EXIT"
                return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": False}
            return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

        active_stop = float(hard_stop)
        if low <= active_stop:
            return _close_now(outcome="TP1" if tp1_done else "STOP", close_px=active_stop, is_stop=True)

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

        long_exit = bool(sig.get("long_exit", False)) if signal_exit_enabled else False
        pos["qty_rem"] = qty_rem
        pos["realized_r"] = realized_r
        pos["tp1_done"] = tp1_done
        pos["be_armed"] = be_armed
        pos["hard_stop"] = dynamic_stop

        if long_exit:
            realized_r += qty_rem * _close_remaining_r("LONG", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            outcome = "TP1" if tp1_done else "EXIT"
            return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": False}
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
        if short_exit:
            realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
            pos["qty_rem"] = 0.0
            pos["realized_r"] = realized_r
            outcome = "TP1" if tp1_done else "EXIT"
            return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": False}
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False}

    active_stop = float(hard_stop)
    if high >= active_stop:
        return _close_now(outcome="TP1" if tp1_done else "STOP", close_px=active_stop, is_stop=True)

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

    short_exit = bool(sig.get("short_exit", False)) if signal_exit_enabled else False
    pos["qty_rem"] = qty_rem
    pos["realized_r"] = realized_r
    pos["tp1_done"] = tp1_done
    pos["be_armed"] = be_armed
    pos["hard_stop"] = dynamic_stop

    if short_exit:
        realized_r += qty_rem * _close_remaining_r("SHORT", entry, risk, close)
        pos["qty_rem"] = 0.0
        pos["realized_r"] = realized_r
        outcome = "TP1" if tp1_done else "EXIT"
        return {"closed": True, "outcome": str(outcome), "r_raw": float(realized_r), "is_stop": False}
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
    include_start_bar: bool = False,
    max_hold_bars: int = 0,
) -> Tuple[str, float, int, int]:
    risk = abs(entry - stop)
    if risk <= 0:
        risk = max(abs(entry) * 0.0005, 1e-8)

    side_u = side.upper()
    if int(horizon_bars) <= 0:
        end_idx = len(ltf_candles) - 1
    else:
        end_idx = min(len(ltf_candles) - 1, start_idx + max(1, int(horizon_bars)))
    loop_start = max(0, int(start_idx) if include_start_bar else int(start_idx) + 1)
    if not managed_exit:
        outcome = "NONE"
        exit_price = ltf_candles[end_idx].close
        exit_idx = end_idx

        for i in range(loop_start, end_idx + 1):
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
                if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                    outcome = "TIME"
                    exit_price = c.close
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
                if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                    outcome = "TIME"
                    exit_price = c.close
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
        for i in range(loop_start, end_idx + 1):
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
            if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
                realized_r = float(pos.get("realized_r", 0.0) or 0.0)
                realized_r += qty_rem * _close_remaining_r(side_u, float(entry), float(risk), float(sig["close"]))
                exit_idx = i
                held = max(0, exit_idx - start_idx)
                return "TIME", realized_r, held, exit_idx

        qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
        realized_r = float(pos.get("realized_r", 0.0) or 0.0)
        if qty_rem > 1e-9:
            last_close = float(ltf_candles[end_idx].close)
            realized_r += qty_rem * _close_remaining_r(side_u, float(entry), float(risk), last_close)
            if bool(pos.get("tp1_done", False)) and outcome == "NONE":
                outcome = "TP1"
        held = max(0, exit_idx - start_idx)
        return outcome, realized_r, held, exit_idx

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

    for i in range(loop_start, end_idx + 1):
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
                    if sig_long_exit:
                        realized_r += qty_rem * ((sig_close - entry) / risk)
                        qty_rem = 0.0
                        outcome = "EXIT"
                        exit_idx = i
                        break
                    if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                        realized_r += qty_rem * ((sig_close - entry) / risk)
                        qty_rem = 0.0
                        outcome = "TIME"
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
                if sig_long_exit:
                    realized_r += qty_rem * ((sig_close - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1" if tp1_done else "EXIT"
                    exit_idx = i
                    break
                if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                    realized_r += qty_rem * ((sig_close - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TIME"
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
                    if sig_short_exit:
                        realized_r += qty_rem * ((entry - sig_close) / risk)
                        qty_rem = 0.0
                        outcome = "EXIT"
                        exit_idx = i
                        break
                    if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                        realized_r += qty_rem * ((entry - sig_close) / risk)
                        qty_rem = 0.0
                        outcome = "TIME"
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
                if sig_short_exit:
                    realized_r += qty_rem * ((entry - sig_close) / risk)
                    qty_rem = 0.0
                    outcome = "TP1" if tp1_done else "EXIT"
                    exit_idx = i
                    break
                if max_hold_bars > 0 and (i - start_idx) >= int(max_hold_bars):
                    realized_r += qty_rem * ((entry - sig_close) / risk)
                    qty_rem = 0.0
                    outcome = "TIME"
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
