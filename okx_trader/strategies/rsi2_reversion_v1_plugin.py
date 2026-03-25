from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_rsi2_reversion_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)
    prev_close = _f(inputs.prev_close, close_f)

    atr_v = max(1e-12, _f(inputs.atr_value, close_f * 0.002))
    rsi_v = _f(inputs.rsi_value, 50.0)
    recent_rsi_min = _f(inputs.recent_rsi_min, rsi_v)
    recent_rsi_max = _f(inputs.recent_rsi_max, rsi_v)
    ema_fast = _f(inputs.ema_value, close_f)

    upper = _f(inputs.upper_band, close_f + atr_v * 2.0)
    lower = _f(inputs.lower_band, close_f - atr_v * 2.0)
    if upper <= lower:
        upper = close_f + atr_v * 2.0
        lower = close_f - atr_v * 2.0
    mid = _f(inputs.mid_band, (upper + lower) * 0.5)
    band_span = max(1e-9, upper - lower)

    h_close = _f(inputs.h_close, close_f)
    h_fast = _f(inputs.h_ema_fast, h_close)
    h_slow = _f(inputs.h_ema_slow, h_close)
    prev_h_fast = _f(inputs.prev_h_ema_fast, h_fast)
    prev_h_slow = _f(inputs.prev_h_ema_slow, h_slow)

    uptrend = (
        h_close > h_slow
        and h_fast >= h_slow
        and h_fast >= prev_h_fast * 0.998
        and h_slow >= prev_h_slow * 0.998
    )
    downtrend = (
        h_close < h_slow
        and h_fast <= h_slow
        and h_fast <= prev_h_fast * 1.002
        and h_slow <= prev_h_slow * 1.002
    )

    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)
    bullish_reclaim = close_f > curr_o and lower_wick >= max(body * 0.8, atr_v * 0.10)
    bearish_reclaim = close_f < curr_o and upper_wick >= max(body * 0.8, atr_v * 0.10)

    stretch_long = (
        close_f <= ema_fast * 0.996
        or curr_lo <= lower + band_span * 0.18
        or curr_lo <= prev_lo
    )
    stretch_short = (
        close_f >= ema_fast * 1.004
        or curr_hi >= upper - band_span * 0.18
        or curr_hi >= prev_hi
    )

    long_entry = (
        uptrend
        and bool(inputs.not_chasing_long)
        and stretch_long
        and (
            rsi_v <= 5.0
            or recent_rsi_min <= 4.0
            or ((rsi_v <= 10.0 or recent_rsi_min <= 8.0) and (close_f <= prev_close or bullish_reclaim))
        )
    )
    short_entry = (
        downtrend
        and bool(inputs.not_chasing_short)
        and stretch_short
        and (
            rsi_v >= 95.0
            or recent_rsi_max >= 96.0
            or ((rsi_v >= 90.0 or recent_rsi_max >= 92.0) and (close_f >= prev_close or bearish_reclaim))
        )
    )

    long_exit = close_f >= ema_fast or close_f >= mid or rsi_v >= 55.0
    short_exit = close_f <= ema_fast or close_f <= mid or rsi_v <= 45.0

    stop_pad = max(atr_v * 1.1, close_f * 0.0030, band_span * 0.12)
    long_stop = min(curr_lo, prev_lo, _f(inputs.pb_low, curr_lo), lower) - stop_pad
    short_stop = max(curr_hi, prev_hi, _f(inputs.pb_high, curr_hi), upper) + stop_pad

    min_stop_gap = max(atr_v * 0.30, close_f * 0.0010)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "rsi2_reversion_v1",
        "vol_ok": True,
        "fresh_break_long": False,
        "fresh_break_short": False,
        "rsi2_uptrend": bool(uptrend),
        "rsi2_downtrend": bool(downtrend),
        "rsi2_stretch_long": bool(stretch_long),
        "rsi2_stretch_short": bool(stretch_short),
        "rsi2_recent_rsi_min": float(recent_rsi_min),
        "rsi2_recent_rsi_max": float(recent_rsi_max),
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
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver
    register_variant_input_resolver("rsi2_reversion_v1", _resolve_rsi2_reversion_v1)
