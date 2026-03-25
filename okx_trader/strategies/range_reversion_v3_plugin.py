from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_range_reversion_v3(inputs: VariantSignalInputs) -> Dict[str, Any]:
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
    loc_rsi = _f(inputs.loc_rsi_value, rsi_v)
    hour_rsi = _f(inputs.hour_rsi_value, rsi_v)
    recent_rsi_min = _f(inputs.recent_rsi_min, min(rsi_v, hour_rsi))
    recent_rsi_max = _f(inputs.recent_rsi_max, max(rsi_v, hour_rsi))
    macd_hist = _f(inputs.macd_hist_value, 0.0)
    prev_mh = _f(inputs.prev_macd_hist, macd_hist)

    h_close = _f(inputs.h_close, close_f)
    h_fast = _f(inputs.h_ema_fast, h_close)
    h_slow = _f(inputs.h_ema_slow, h_close)
    prev_h_fast = _f(inputs.prev_h_ema_fast, h_fast)
    prev_h_slow = _f(inputs.prev_h_ema_slow, h_slow)

    loc_close = _f(inputs.loc_close, close_f)
    loc_fast = _f(inputs.loc_ema_fast, loc_close)
    loc_slow = _f(inputs.loc_ema_slow, loc_close)
    prev_loc_fast = _f(inputs.prev_loc_ema_fast, loc_fast)
    prev_loc_slow = _f(inputs.prev_loc_ema_slow, loc_slow)
    loc_hi = _f(inputs.loc_current_high, curr_hi)
    loc_lo = _f(inputs.loc_current_low, curr_lo)

    upper = _f(inputs.upper_band, close_f + atr_v * 2.0)
    lower = _f(inputs.lower_band, close_f - atr_v * 2.0)
    if upper <= lower:
        upper = close_f + atr_v * 2.0
        lower = close_f - atr_v * 2.0
    mid = _f(inputs.mid_band, (upper + lower) * 0.5)
    band_span = max(1e-9, upper - lower)

    width = _f(inputs.width, 0.0)
    width_avg = max(1e-12, _f(inputs.width_avg, width if width > 0 else 1e-6))

    hour_open = _f(inputs.hour_open, curr_o)
    hour_high = _f(inputs.hour_high, curr_hi)
    hour_low = _f(inputs.hour_low, curr_lo)
    hour_close = _f(inputs.hour_close, close_f)
    hour_prev_close = _f(inputs.hour_prev_close, hour_close)

    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)

    htf_sep = abs(h_fast - h_slow) / max(abs(h_close), 1e-9)
    loc_sep = abs(loc_fast - loc_slow) / max(abs(loc_close), 1e-9)

    hard_downtrend = h_close < h_fast < h_slow and htf_sep >= 0.010
    hard_uptrend = h_close > h_fast > h_slow and htf_sep >= 0.010

    soft_up = (
        h_close >= h_slow * 0.995
        and (h_fast >= h_slow * 0.998 or h_fast >= prev_h_fast or h_slow >= prev_h_slow)
    )
    soft_down = (
        h_close <= h_slow * 1.005
        and (h_fast <= h_slow * 1.002 or h_fast <= prev_h_fast or h_slow <= prev_h_slow)
    )
    loc_soft_up = (
        loc_close >= loc_slow * 0.997
        or (loc_fast >= prev_loc_fast and loc_fast >= loc_slow * 0.995)
    )
    loc_soft_down = (
        loc_close <= loc_slow * 1.003
        or (loc_fast <= prev_loc_fast and loc_fast <= loc_slow * 1.005)
    )
    width_ok = width <= width_avg * 1.45

    long_regime_ok = width_ok and (not hard_downtrend) and soft_up and loc_soft_up
    short_regime_ok = False

    band_touch_tol = max(close_f * 0.0010, atr_v * 0.18, band_span * 0.05)
    lower_touch = curr_lo <= lower + band_touch_tol or close_f <= lower + band_touch_tol
    upper_touch = curr_hi >= upper - band_touch_tol or close_f >= upper - band_touch_tol
    deep_long = curr_lo <= lower or close_f <= lower + band_span * 0.10
    deep_short = curr_hi >= upper or close_f >= upper - band_span * 0.10
    extreme_long = curr_lo <= lower - band_span * 0.04
    extreme_short = curr_hi >= upper + band_span * 0.04

    hour_reject_long = (
        hour_low <= lower + band_touch_tol
        and hour_close >= hour_open
        and hour_close >= hour_prev_close
        and hour_close >= lower + band_span * 0.05
    )
    hour_reject_short = (
        hour_high >= upper - band_touch_tol
        and hour_close <= hour_open
        and hour_close <= hour_prev_close
        and hour_close <= upper - band_span * 0.05
    )

    bull_reclaim = (
        close_f > curr_o
        and close_f >= prev_close
        and close_f <= mid + band_span * 0.10
        and lower_wick >= max(body * 1.0, atr_v * 0.12)
    )
    bear_reclaim = (
        close_f < curr_o
        and close_f <= prev_close
        and close_f >= mid - band_span * 0.10
        and upper_wick >= max(body * 1.0, atr_v * 0.12)
    )

    macd_turn_up = macd_hist >= prev_mh and (prev_mh <= 0.0 or macd_hist <= 0.12)
    macd_turn_down = macd_hist <= prev_mh and (prev_mh >= 0.0 or macd_hist >= -0.12)

    oversold_l1 = rsi_v <= 40.0 or hour_rsi <= 40.0 or loc_rsi <= 45.0 or recent_rsi_min <= 38.0
    oversold_l2 = rsi_v <= 36.0 or hour_rsi <= 37.0 or recent_rsi_min <= 34.0
    oversold_l3 = rsi_v <= 35.0 or hour_rsi <= 36.0 or recent_rsi_min <= 33.0
    overbought_l1 = rsi_v >= 56.0 or hour_rsi >= 57.0 or loc_rsi >= 53.0 or recent_rsi_max >= 58.0
    overbought_l2 = rsi_v >= 61.0 or hour_rsi >= 61.0 or recent_rsi_max >= 64.0
    overbought_l3 = rsi_v >= 65.0 or hour_rsi >= 64.0 or recent_rsi_max >= 67.0

    long_base = (
        long_regime_ok
        and lower_touch
        and hour_reject_long
        and bull_reclaim
        and macd_turn_up
        and bool(inputs.not_chasing_long)
    )
    short_base = (
        short_regime_ok
        and upper_touch
        and hour_reject_short
        and bear_reclaim
        and macd_turn_down
        and bool(inputs.not_chasing_short)
    )

    long_entry_l1 = long_base and deep_long and oversold_l1
    long_entry_l2 = long_base and deep_long and oversold_l2
    long_entry_l3 = long_base and deep_long and extreme_long and oversold_l3

    short_entry_l1 = False
    short_entry_l2 = False
    short_entry_l3 = False

    long_level = 3 if long_entry_l3 else (2 if long_entry_l2 else (1 if long_entry_l1 else 0))
    short_level = 3 if short_entry_l3 else (2 if short_entry_l2 else (1 if short_entry_l1 else 0))

    long_entry = long_level > 0
    short_entry = short_level > 0

    long_exit = (
        (close_f >= mid and rsi_v >= 48.0)
        or (close_f >= loc_fast and rsi_v >= 52.0)
        or (curr_hi >= upper - band_span * 0.04)
        or (hard_downtrend and close_f < loc_slow)
    )
    short_exit = (
        (close_f <= mid and rsi_v <= 52.0)
        or (close_f <= loc_fast and rsi_v <= 48.0)
        or (curr_lo <= lower + band_span * 0.04)
        or (hard_uptrend and close_f > loc_slow)
    )

    stop_pad = max(atr_v * 0.55, loc_atr * 0.45, band_span * 0.06)
    long_stop = min(curr_lo, prev_lo, hour_low, loc_lo, _f(inputs.pb_low, curr_lo), lower) - stop_pad
    short_stop = max(curr_hi, prev_hi, hour_high, loc_hi, _f(inputs.pb_high, curr_hi), upper) + stop_pad

    min_stop_gap = max(atr_v * 0.25, close_f * 0.0010)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": "range_reversion_v3",
        "vol_ok": bool(width_ok),
        "fresh_break_long": False,
        "fresh_break_short": False,
        "rr3_long_regime_ok": bool(long_regime_ok),
        "rr3_short_regime_ok": bool(short_regime_ok),
        "rr3_lower_touch": bool(lower_touch),
        "rr3_upper_touch": bool(upper_touch),
        "rr3_hour_reject_long": bool(hour_reject_long),
        "rr3_hour_reject_short": bool(hour_reject_short),
        "rr3_bull_reclaim": bool(bull_reclaim),
        "rr3_bear_reclaim": bool(bear_reclaim),
        "rr3_mid": float(mid),
        "rr3_upper": float(upper),
        "rr3_lower": float(lower),
        "long_entry": bool(long_entry),
        "short_entry": bool(short_entry),
        "long_entry_l2": bool(long_entry_l2),
        "short_entry_l2": bool(short_entry_l2),
        "long_entry_l3": bool(long_entry_l3),
        "short_entry_l3": bool(short_entry_l3),
        "long_level": int(long_level),
        "short_level": int(short_level),
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
    register_variant_input_resolver("range_reversion_v3", _resolve_range_reversion_v3)
