from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_range_reversion_v2(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)
    prev_close = _f(inputs.prev_close, close_f)

    atr_v = max(1e-12, _f(inputs.atr_value, close_f * 0.0025))
    loc_atr = max(1e-12, _f(inputs.loc_atr_value, atr_v * 2.0))
    rsi_v = _f(inputs.rsi_value, 50.0)
    prev_mh = _f(inputs.prev_macd_hist, _f(inputs.macd_hist_value, 0.0))
    macd_hist = _f(inputs.macd_hist_value, prev_mh)

    h_close = _f(inputs.h_close, close_f)
    h_fast = _f(inputs.h_ema_fast, h_close)
    h_slow = _f(inputs.h_ema_slow, h_close)
    loc_close = _f(inputs.loc_close, close_f)
    loc_fast = _f(inputs.loc_ema_fast, loc_close)
    loc_slow = _f(inputs.loc_ema_slow, loc_close)
    prev_loc_fast = _f(inputs.prev_loc_ema_fast, loc_fast)
    prev_loc_slow = _f(inputs.prev_loc_ema_slow, loc_slow)
    loc_rsi = _f(inputs.loc_rsi_value, 50.0)

    upper = _f(inputs.upper_band, max(close_f + atr_v * 2.0, close_f))
    lower = _f(inputs.lower_band, min(close_f - atr_v * 2.0, close_f))
    if upper <= lower:
        upper = close_f + atr_v * 2.0
        lower = close_f - atr_v * 2.0
    mid = _f(inputs.mid_band, (upper + lower) * 0.5)

    band_span = max(1e-9, upper - lower)
    width = _f(inputs.width, 0.0)
    width_avg = max(1e-12, _f(inputs.width_avg, width if width > 0 else 1e-6))

    hour_open = _f(inputs.hour_open, curr_o)
    hour_close = _f(inputs.hour_close, close_f)
    hour_high = _f(inputs.hour_high, curr_hi)
    hour_low = _f(inputs.hour_low, curr_lo)
    hour_prev_close = _f(inputs.hour_prev_close, hour_close)
    hour_rsi = _f(inputs.hour_rsi_value, rsi_v)

    htf_sep = abs(h_fast - h_slow) / max(abs(h_close), 1e-9)
    loc_sep = abs(loc_fast - loc_slow) / max(abs(loc_close), 1e-9)

    hard_downtrend = h_close < h_fast < h_slow and htf_sep >= 0.018
    hard_uptrend = h_close > h_fast > h_slow and htf_sep >= 0.018
    regime_flat = htf_sep <= 0.025 and loc_sep <= 0.010
    width_ok = width <= width_avg * 1.40

    long_regime_ok = regime_flat and width_ok and (not hard_downtrend)
    short_regime_ok = regime_flat and width_ok and (not hard_uptrend)

    body = abs(close_f - curr_o)
    true_range = max(1e-12, curr_hi - curr_lo)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)

    band_touch_tol = max(close_f * 0.0010, atr_v * 0.20, band_span * 0.06)
    lower_touch = curr_lo <= lower + band_touch_tol or close_f <= lower + band_touch_tol
    upper_touch = curr_hi >= upper - band_touch_tol or close_f >= upper - band_touch_tol

    hour_reject_long = (
        hour_low <= lower + band_touch_tol
        and hour_close >= lower + band_span * 0.10
        and hour_close >= hour_open
        and hour_close >= hour_prev_close
    )
    hour_reject_short = (
        hour_high >= upper - band_touch_tol
        and hour_close <= upper - band_span * 0.10
        and hour_close <= hour_open
        and hour_close <= hour_prev_close
    )

    bullish_reclaim = (
        close_f > curr_o
        and close_f >= lower + band_span * 0.14
        and lower_wick >= max(body * 1.1, atr_v * 0.15)
        and close_f >= prev_close
    )
    bearish_reclaim = (
        close_f < curr_o
        and close_f <= upper - band_span * 0.14
        and upper_wick >= max(body * 1.1, atr_v * 0.15)
        and close_f <= prev_close
    )

    macd_turn_up = macd_hist >= prev_mh and macd_hist <= 0.08
    macd_turn_down = macd_hist <= prev_mh and macd_hist >= -0.08

    long_entry = (
        long_regime_ok
        and lower_touch
        and hour_reject_long
        and bullish_reclaim
        and (rsi_v <= 41.0 or hour_rsi <= 44.0 or loc_rsi <= 47.0)
        and macd_turn_up
        and bool(inputs.not_chasing_long)
    )
    short_entry = (
        short_regime_ok
        and upper_touch
        and hour_reject_short
        and bearish_reclaim
        and (rsi_v >= 59.0 or hour_rsi >= 56.0 or loc_rsi >= 53.0)
        and macd_turn_down
        and bool(inputs.not_chasing_short)
    )

    near_mid = band_span * 0.08
    near_far_side = band_span * 0.05
    long_exit = (
        close_f >= mid - near_mid
        or rsi_v >= 54.0
        or curr_hi >= upper - near_far_side
        or (hard_downtrend and close_f < mid)
    )
    short_exit = (
        close_f <= mid + near_mid
        or rsi_v <= 46.0
        or curr_lo <= lower + near_far_side
        or (hard_uptrend and close_f > mid)
    )

    stop_pad = max(atr_v * 0.65, loc_atr * 0.35, band_span * 0.08)
    long_stop = min(curr_lo, prev_lo, hour_low, _f(inputs.pb_low, curr_lo), lower) - stop_pad
    short_stop = max(curr_hi, prev_hi, hour_high, _f(inputs.pb_high, curr_hi), upper) + stop_pad

    min_stop_gap = max(atr_v * 0.25, close_f * 0.0010)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "range_reversion_v2",
        "vol_ok": bool(width_ok),
        "fresh_break_long": False,
        "fresh_break_short": False,
        "rr2_regime_flat": bool(regime_flat),
        "rr2_width_ok": bool(width_ok),
        "rr2_lower_touch": bool(lower_touch),
        "rr2_upper_touch": bool(upper_touch),
        "rr2_hour_reject_long": bool(hour_reject_long),
        "rr2_hour_reject_short": bool(hour_reject_short),
        "rr2_bullish_reclaim": bool(bullish_reclaim),
        "rr2_bearish_reclaim": bool(bearish_reclaim),
        "rr2_mid": float(mid),
        "rr2_upper": float(upper),
        "rr2_lower": float(lower),
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
    register_variant_input_resolver("range_reversion_v2", _resolve_range_reversion_v2)
