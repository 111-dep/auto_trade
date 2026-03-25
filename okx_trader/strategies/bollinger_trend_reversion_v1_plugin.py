from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_bollinger_trend_reversion_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)
    prev_close = _f(inputs.prev_close, close_f)

    atr_v = max(1e-12, _f(inputs.atr_value, close_f * 0.002))
    loc_atr = max(1e-12, _f(inputs.loc_atr_value, atr_v))
    rsi_v = _f(inputs.rsi_value, 50.0)
    loc_rsi = _f(inputs.loc_rsi_value, rsi_v)

    h_close = _f(inputs.h_close, close_f)
    h_fast = _f(inputs.h_ema_fast, h_close)
    h_slow = _f(inputs.h_ema_slow, h_close)
    prev_h_fast = _f(inputs.prev_h_ema_fast, h_fast)
    prev_h_slow = _f(inputs.prev_h_ema_slow, h_slow)
    trend_sep = abs(h_fast - h_slow) / max(abs(h_close), 1e-9)

    upper = _f(inputs.upper_band, close_f + atr_v * 2.0)
    lower = _f(inputs.lower_band, close_f - atr_v * 2.0)
    if upper <= lower:
        upper = close_f + atr_v * 2.0
        lower = close_f - atr_v * 2.0
    mid = _f(inputs.mid_band, (upper + lower) * 0.5)
    band_span = max(1e-9, upper - lower)

    width = _f(inputs.width, 0.0)
    width_avg = max(1e-12, _f(inputs.width_avg, width if width > 0 else 1e-6))

    uptrend = (
        h_close > h_slow
        and h_fast >= h_slow
        and h_fast >= prev_h_fast * 0.998
        and h_slow >= prev_h_slow * 0.998
        and trend_sep >= 0.0010
    )
    downtrend = (
        h_close < h_slow
        and h_fast <= h_slow
        and h_fast <= prev_h_fast * 1.002
        and h_slow <= prev_h_slow * 1.002
        and trend_sep >= 0.0010
    )

    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)
    touch_tol = max(close_f * 0.0010, atr_v * 0.20, band_span * 0.08)

    lower_touch = curr_lo <= lower + touch_tol or close_f <= lower + touch_tol
    upper_touch = curr_hi >= upper - touch_tol or close_f >= upper - touch_tol
    width_ok = width <= width_avg * 1.80

    bullish_reclaim = (
        close_f >= lower
        and close_f > curr_o
        and close_f >= prev_close
        and lower_wick >= max(body * 1.2, atr_v * 0.12)
    )
    bearish_reclaim = (
        close_f <= upper
        and close_f < curr_o
        and close_f <= prev_close
        and upper_wick >= max(body * 1.2, atr_v * 0.12)
    )

    long_entry = (
        uptrend
        and width_ok
        and lower_touch
        and bool(inputs.not_chasing_long)
        and (rsi_v <= 38.0 or loc_rsi <= 42.0)
        and (bullish_reclaim or close_f >= lower + band_span * 0.06)
    )
    short_entry = (
        downtrend
        and width_ok
        and upper_touch
        and bool(inputs.not_chasing_short)
        and (rsi_v >= 62.0 or loc_rsi >= 58.0)
        and (bearish_reclaim or close_f <= upper - band_span * 0.06)
    )

    long_exit = close_f >= mid or rsi_v >= 55.0 or curr_hi >= upper - band_span * 0.05
    short_exit = close_f <= mid or rsi_v <= 45.0 or curr_lo <= lower + band_span * 0.05

    stop_pad = max(atr_v * 0.90, loc_atr * 0.60, band_span * 0.10)
    long_stop = min(curr_lo, prev_lo, _f(inputs.pb_low, curr_lo), lower) - stop_pad
    short_stop = max(curr_hi, prev_hi, _f(inputs.pb_high, curr_hi), upper) + stop_pad

    min_stop_gap = max(atr_v * 0.30, close_f * 0.0010)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "bollinger_trend_reversion_v1",
        "vol_ok": bool(width_ok),
        "fresh_break_long": False,
        "fresh_break_short": False,
        "bbtr_uptrend": bool(uptrend),
        "bbtr_downtrend": bool(downtrend),
        "bbtr_lower_touch": bool(lower_touch),
        "bbtr_upper_touch": bool(upper_touch),
        "bbtr_bullish_reclaim": bool(bullish_reclaim),
        "bbtr_bearish_reclaim": bool(bearish_reclaim),
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
    register_variant_input_resolver("bollinger_trend_reversion_v1", _resolve_bollinger_trend_reversion_v1)
