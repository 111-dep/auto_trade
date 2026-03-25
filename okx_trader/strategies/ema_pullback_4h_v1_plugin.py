from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_ema_pullback_4h_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_c = _f(inputs.prev_close, close_f)

    daily_ema = _f(inputs.h_ema_fast, close_f)
    prev_daily_ema = _f(inputs.prev_h_ema_fast, daily_ema)
    daily_filter_long = close_f > daily_ema and daily_ema > prev_daily_ema

    fast = _f(inputs.ema_value, close_f)
    slow = _f(inputs.ema_slow_value, fast)
    prev_fast = _f(inputs.prev_ema_value, fast)
    prev_slow = _f(inputs.prev_ema_slow_value, slow)
    pb_low = _f(inputs.pb_low, curr_lo)
    pb_high = _f(inputs.pb_high, curr_hi)
    recent_rsi_min = _f(inputs.recent_rsi_min, _f(inputs.rsi_value, 50.0))
    atr_value = max(1e-12, _f(inputs.atr_value, close_f * 0.001))

    zone_lo = min(fast, slow)
    zone_hi = max(fast, slow)
    trend_sep = abs(fast - slow) / max(abs(close_f), 1e-9)

    trend_up = fast > slow and fast > prev_fast and slow > prev_slow and trend_sep >= 0.0010

    zone_touch_recent = pb_low <= zone_hi and pb_high >= zone_lo
    pullback_ready_long = zone_touch_recent and recent_rsi_min < 50.0

    bullish_reclaim = close_f > curr_o and close_f > fast
    prev_below_fast = prev_c <= prev_fast
    trigger_long = bullish_reclaim and (prev_below_fast or curr_lo <= zone_hi)

    long_entry_l1 = daily_filter_long and trend_up and pullback_ready_long and trigger_long

    long_entry_l2 = False
    short_entry_l2 = False
    long_entry_l3 = False
    short_entry_l3 = False

    long_level = 1 if long_entry_l1 else 0
    short_entry_l1 = False
    short_level = 0

    long_stop = min(pb_low, curr_lo) - atr_value
    short_stop = max(pb_high, curr_hi) + atr_value

    min_stop_gap = max(atr_value * 0.25, close_f * 0.0010)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "ema_pullback_4h_v1",
        "trend_sep": float(trend_sep),
        "vol_ok": True,
        "fresh_break_long": bool(trigger_long),
        "fresh_break_short": False,
        "ema_pb_daily_filter_long": bool(daily_filter_long),
        "ema_pb_daily_ema": float(daily_ema),
        "ema_pb_zone_lo": float(zone_lo),
        "ema_pb_zone_hi": float(zone_hi),
        "ema_pb_zone_touch_recent": bool(zone_touch_recent),
        "ema_pb_pullback_ready_long": bool(pullback_ready_long),
        "ema_pb_recent_rsi_min": float(recent_rsi_min),
        "ema_pb_trigger_long": bool(trigger_long),
        "long_entry": bool(long_entry_l1),
        "short_entry": False,
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
    _ = register_variant_resolver
    register_variant_input_resolver("ema_pullback_4h_v1", _resolve_ema_pullback_4h_v1)
