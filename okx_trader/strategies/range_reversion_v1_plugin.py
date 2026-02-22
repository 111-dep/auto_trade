from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_range_reversion_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    a = max(1e-12, _f(inputs.atr_value, 0.0))
    h_close = _f(inputs.h_close, close_f)
    h_ema_fast = _f(inputs.h_ema_fast, h_close)
    h_ema_slow = _f(inputs.h_ema_slow, h_close)
    r = _f(inputs.rsi_value, 50.0)
    mh = _f(inputs.macd_hist_value, 0.0)
    prev_mh = _f(inputs.prev_macd_hist, mh)

    up = _f(inputs.upper_band, _f(inputs.hhv, close_f + a))
    lo = _f(inputs.lower_band, _f(inputs.llv, close_f - a))
    if up <= lo:
        up = max(up, close_f + a)
        lo = min(lo, close_f - a)
    mid = _f(inputs.mid_band, (up + lo) * 0.5)

    curr_o = _f(inputs.current_open, close_f)
    prev_close = _f(inputs.prev_close, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    exl = _f(inputs.exl, lo)
    exh = _f(inputs.exh, up)
    pb_low = _f(inputs.pb_low, lo)
    pb_high = _f(inputs.pb_high, up)

    range_span = max(1e-9, up - lo)
    touch_tol = max(close_f * 0.0008, a * 0.10)

    # Flat/weak-trend periods are preferred for range mean reversion.
    trend_sep = abs(h_ema_fast - h_ema_slow) / max(abs(h_close), 1e-9)
    trend_up = h_close >= h_ema_fast >= h_ema_slow
    trend_down = h_close <= h_ema_fast <= h_ema_slow
    trend_flat = trend_sep <= 0.012
    width_ok = bool(inputs.width_avg > 0 and inputs.width <= inputs.width_avg * 1.08)
    range_ready = bool(range_span >= max(close_f * 0.0060, a * 2.4))

    touch_lower = close_f <= (lo + touch_tol) or curr_lo <= lo
    touch_upper = close_f >= (up - touch_tol) or curr_hi >= up
    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)
    bull_reject = (
        touch_lower
        and close_f > curr_o
        and close_f >= prev_close
        and lower_wick >= max(body * 1.2, a * 0.10)
        and close_f >= lo + range_span * 0.18
    )
    bear_reject = (
        touch_upper
        and close_f < curr_o
        and close_f <= prev_close
        and upper_wick >= max(body * 1.2, a * 0.10)
        and close_f <= up - range_span * 0.18
    )
    macd_turn_up = prev_mh <= 0.0 and mh > prev_mh and mh <= 0.10
    macd_turn_down = prev_mh >= 0.0 and mh < prev_mh and mh >= -0.10

    allow_long = (
        bool(inputs.long_location_ok)
        and bool(inputs.pullback_long)
        and bool(inputs.not_chasing_long)
        and trend_flat
        and (not trend_down)
    )
    allow_short = (
        bool(inputs.short_location_ok)
        and bool(inputs.pullback_short)
        and bool(inputs.not_chasing_short)
        and trend_flat
        and (not trend_up)
    )

    # Keep this variant as a low-frequency, high-quality supplement:
    # only produce strict L1 entries.
    long_entry_l1 = allow_long and range_ready and width_ok and bull_reject and r <= 32.0 and macd_turn_up
    short_entry_l1 = allow_short and range_ready and width_ok and bear_reject and r >= 68.0 and macd_turn_down

    long_entry_l2 = False
    short_entry_l2 = False
    long_entry_l3 = False
    short_entry_l3 = False

    long_level = 0
    if long_entry_l1:
        long_level = 1
    elif long_entry_l2:
        long_level = 2
    elif long_entry_l3:
        long_level = 3

    short_level = 0
    if short_entry_l1:
        short_level = 1
    elif short_entry_l2:
        short_level = 2
    elif short_entry_l3:
        short_level = 3

    long_stop = min(curr_lo, exl, pb_low, lo - a * 0.45, close_f - max(a * 1.8, range_span * 0.24))
    short_stop = max(curr_hi, exh, pb_high, up + a * 0.45, close_f + max(a * 1.8, range_span * 0.24))

    min_stop_gap = max(a * 0.20, close_f * 0.0004)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "range_reversion_v1",
        "trend_sep": float(trend_sep),
        "vol_ok": bool(width_ok),
        "fresh_break_long": False,
        "fresh_break_short": False,
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
    register_variant_input_resolver("range_reversion_v1", _resolve_range_reversion_v1)
