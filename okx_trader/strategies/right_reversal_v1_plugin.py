from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _round_step(price: float) -> float:
    p = abs(float(price))
    if p >= 50000:
        return 500.0
    if p >= 10000:
        return 200.0
    if p >= 5000:
        return 100.0
    if p >= 1000:
        return 50.0
    if p >= 200:
        return 10.0
    if p >= 50:
        return 2.0
    if p >= 10:
        return 0.5
    if p >= 1:
        return 0.05
    if p >= 0.1:
        return 0.005
    if p >= 0.01:
        return 0.0005
    return 0.0001


def _near_round(price: float, atr_value: float) -> bool:
    step = _round_step(price)
    if step <= 0:
        return False
    px = float(price)
    nearest = round(px / step) * step
    tol = max(step * 0.08, abs(px) * 0.0006, abs(float(atr_value)) * 0.20)
    return abs(px - nearest) <= tol


def _resolve_right_reversal_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    em = _f(inputs.ema_value, close_f)
    a = max(1e-12, _f(inputs.atr_value, 0.0))
    r = _f(inputs.rsi_value, 50.0)
    mh = _f(inputs.macd_hist_value, 0.0)
    prev_mh = _f(inputs.prev_macd_hist, mh)

    h_close = _f(inputs.h_close, close_f)
    h_ema_fast = _f(inputs.h_ema_fast, h_close)
    h_ema_slow = _f(inputs.h_ema_slow, h_close)
    trend_sep = abs(h_ema_fast - h_ema_slow) / max(abs(h_close), 1e-9)

    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_o = _f(inputs.prev_open, curr_o)
    prev_c = _f(inputs.prev_close, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)
    prev2_hi = _f(inputs.prev2_high, prev_hi)
    prev2_lo = _f(inputs.prev2_low, prev_lo)

    exl = _f(inputs.exl, curr_lo)
    exh = _f(inputs.exh, curr_hi)
    pb_low = _f(inputs.pb_low, curr_lo)
    pb_high = _f(inputs.pb_high, curr_hi)

    rng = max(1e-9, curr_hi - curr_lo)
    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)

    # Right-side reversal patterns: need rejection + reclaim, not blind bottom picking.
    hammer = (
        lower_wick >= max(body * 1.8, a * 0.22)
        and upper_wick <= max(body * 0.9, a * 0.25)
        and close_f >= curr_o
        and close_f >= curr_lo + rng * 0.58
    )
    bullish_engulf = (
        close_f > curr_o
        and prev_c < prev_o
        and curr_o <= prev_c
        and close_f >= prev_o
        and close_f >= prev_hi * 0.998
    )
    w_bottom = (
        prev2_lo > 0
        and abs(curr_lo - prev2_lo) / max(prev2_lo, 1e-9) <= 0.006
        and close_f >= prev_hi * 1.001
        and close_f >= curr_o
    )

    drop_vs_ema = (em - curr_lo) / max(abs(em), 1e-9)
    drop_atr = (em - curr_lo) / max(a, 1e-9)
    sharp_drop = drop_vs_ema >= 0.0055 or drop_atr >= 1.5

    macd_turn_up = mh > prev_mh and (prev_mh <= 0.0 or mh >= 0.0)
    reclaim_ok = close_f >= curr_lo + rng * 0.60 and close_f >= em * 0.995

    vol = max(0.0, _f(inputs.volume, 0.0))
    vol_avg = max(0.0, _f(inputs.volume_avg, 0.0))
    vol_ok = vol_avg <= 0.0 or vol >= vol_avg * 1.05

    width = _f(inputs.width, 0.0)
    width_avg = _f(inputs.width_avg, 0.0)
    width_ok = width_avg > 0.0 and width >= width_avg * 0.65

    fib_ok = bool(inputs.long_location_ok)
    ema_support = curr_lo <= em * 1.0015 and close_f >= em * 0.998
    round_ok = _near_round(close_f, a) or _near_round(curr_lo, a)
    rsi_os_strict = r <= 37.0
    rsi_os_soft = r <= 44.0

    confluence = int(fib_ok) + int(ema_support) + int(round_ok) + int(bool(inputs.pullback_long)) + int(rsi_os_soft)
    pattern_hit = hammer or bullish_engulf or w_bottom

    # Keep this as a strict supplement for rebound longs.
    trend_guard = h_close >= h_ema_slow * 0.992 and trend_sep <= 0.04
    bias_guard = str(inputs.bias) == "long" or (
        str(inputs.bias) == "neutral" and h_close >= h_ema_fast >= h_ema_slow
    )
    allow_long = bool(inputs.not_chasing_long) and trend_guard and bias_guard

    long_entry_l1 = (
        allow_long
        and sharp_drop
        and pattern_hit
        and macd_turn_up
        and reclaim_ok
        and vol_ok
        and width_ok
        and rsi_os_strict
        and confluence >= 4
    )
    long_entry_l2 = False
    long_entry_l3 = False

    short_entry_l1 = False
    short_entry_l2 = False
    short_entry_l3 = False

    long_level = 1 if long_entry_l1 else (2 if long_entry_l2 else 0)
    short_level = 0

    stop_floor = min(curr_lo, prev_lo, pb_low, exl)
    long_stop = min(stop_floor - a * 0.10, close_f - max(a * 1.8, close_f * 0.004))
    short_stop = max(curr_hi, prev_hi, pb_high, exh) + max(a * 0.6, close_f * 0.002)

    min_stop_gap = max(a * 0.20, close_f * 0.0004)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "right_reversal_v1",
        "trend_sep": float(trend_sep),
        "vol_ok": bool(vol_ok and width_ok),
        "fresh_break_long": False,
        "fresh_break_short": False,
        "right_rev_hammer": bool(hammer),
        "right_rev_bullish_engulf": bool(bullish_engulf),
        "right_rev_w_bottom": bool(w_bottom),
        "right_rev_confluence": int(confluence),
        "right_rev_sharp_drop": bool(sharp_drop),
        "long_entry": bool(long_entry_l1),
        "short_entry": bool(short_entry_l1),
        "long_entry_l2": bool(long_entry_l2),
        "short_entry_l2": bool(short_entry_l2),
        "long_entry_l3": bool(long_entry_l3),
        "short_entry_l3": bool(short_entry_l3),
        "long_level": int(long_level),
        "short_level": int(short_level),
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver  # reserved for kwargs-based plugins
    register_variant_input_resolver("right_reversal_v1", _resolve_right_reversal_v1)
