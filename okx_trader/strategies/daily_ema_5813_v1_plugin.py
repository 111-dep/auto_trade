from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_daily_ema_5813_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    ema5 = _f(inputs.h_ema_fast, close_f)
    ema8 = _f(inputs.h_ema_slow, close_f)
    ema13 = _f(inputs.ema_value, close_f)

    prev_ema5 = _f(inputs.prev_h_ema_fast, ema5)
    prev_ema8 = _f(inputs.prev_h_ema_slow, ema8)
    prev_ema13 = _f(inputs.prev_ema_value, ema13)

    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    atr_v = max(1e-12, _f(inputs.atr_value, close_f * 0.02))

    cross_up_5_8 = prev_ema5 <= prev_ema8 and ema5 > ema8
    cross_dn_5_8 = prev_ema5 >= prev_ema8 and ema5 < ema8
    cross_up_8_13 = prev_ema8 <= prev_ema13 and ema8 > ema13
    cross_dn_8_13 = prev_ema8 >= prev_ema13 and ema8 < ema13

    bullish_stack = ema5 > ema8 > ema13
    bearish_stack = ema5 < ema8 < ema13
    close_above_all = close_f > max(ema5, ema8, ema13)
    close_below_all = close_f < min(ema5, ema8, ema13)

    long_entry = cross_up_5_8 and bullish_stack and close_above_all
    short_entry = cross_dn_5_8 and bearish_stack and close_below_all

    long_exit = cross_dn_5_8 or cross_dn_8_13 or close_f < ema13
    short_exit = cross_up_5_8 or cross_up_8_13 or close_f > ema13

    long_stop = min(curr_lo, ema13 - 2.0 * atr_v)
    short_stop = max(curr_hi, ema13 + 2.0 * atr_v)

    min_stop_gap = max(atr_v * 0.25, close_f * 0.005)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "daily_ema_5813_v1",
        "vol_ok": True,
        "fresh_break_long": bool(cross_up_5_8),
        "fresh_break_short": bool(cross_dn_5_8),
        "long_entry": bool(long_entry),
        "short_entry": bool(short_entry),
        "long_entry_l2": False,
        "short_entry_l2": False,
        "long_entry_l3": False,
        "short_entry_l3": False,
        "long_level": 1 if long_entry else 0,
        "short_level": 1 if short_entry else 0,
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
        "long_exit": bool(long_exit),
        "short_exit": bool(short_exit),
        "ema_5": float(ema5),
        "ema_8": float(ema8),
        "ema_13": float(ema13),
        "stack_bull": bool(bullish_stack),
        "stack_bear": bool(bearish_stack),
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver
    register_variant_input_resolver("daily_ema_5813_v1", _resolve_daily_ema_5813_v1)
