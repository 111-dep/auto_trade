from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_turtle_donchian_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    atr_v = max(1e-12, _f(inputs.atr_value, max(abs(close_f) * 0.02, 1e-6)))

    prev_break_high = _f(inputs.prev_hhv, _f(inputs.hhv, curr_hi))
    prev_break_low = _f(inputs.prev_llv, _f(inputs.llv, curr_lo))
    prev_exit_low = _f(inputs.prev_exl, _f(inputs.exl, curr_lo))
    prev_exit_high = _f(inputs.prev_exh, _f(inputs.exh, curr_hi))

    stop_dist = 2.0 * atr_v
    long_stop = close_f - stop_dist
    short_stop = close_f + stop_dist
    min_stop_gap = max(atr_v * 0.25, abs(close_f) * 0.0025)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    long_entry = close_f > prev_break_high
    short_entry = close_f < prev_break_low
    long_exit = close_f < prev_exit_low
    short_exit = close_f > prev_exit_high

    return {
        "variant": "turtle_donchian_v1",
        "vol_ok": True,
        "fresh_break_long": bool(long_entry),
        "fresh_break_short": bool(short_entry),
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
        "entry_break_high": float(prev_break_high),
        "entry_break_low": float(prev_break_low),
        "exit_break_low": float(prev_exit_low),
        "exit_break_high": float(prev_exit_high),
        "atr_stop_dist": float(stop_dist),
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver
    register_variant_input_resolver("turtle_donchian_v1", _resolve_turtle_donchian_v1)
