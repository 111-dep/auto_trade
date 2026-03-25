from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_daily_sr_zones_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_o = _f(inputs.prev_open, curr_o)
    prev_c = _f(inputs.prev_close, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)

    ema21 = _f(inputs.ema_value, close_f)
    atr_v = max(1e-12, _f(inputs.atr_value, close_f * 0.02))
    rsi_v = _f(inputs.rsi_value, 50.0)
    volume = max(0.0, _f(inputs.volume, 0.0))
    volume_avg = max(0.0, _f(inputs.volume_avg, 0.0))

    support = _f(inputs.prev_llv, _f(inputs.llv, curr_lo))
    resistance = _f(inputs.prev_hhv, _f(inputs.hhv, curr_hi))
    exit_low = _f(inputs.exl, curr_lo)
    exit_high = _f(inputs.exh, curr_hi)

    h_close = _f(inputs.h_close, close_f)
    htf_fast = _f(inputs.h_ema_fast, h_close)
    htf_slow = _f(inputs.h_ema_slow, h_close)
    prev_htf_fast = _f(inputs.prev_h_ema_fast, htf_fast)
    prev_htf_slow = _f(inputs.prev_h_ema_slow, htf_slow)

    weekly_up = h_close > htf_fast > htf_slow and htf_fast > prev_htf_fast and htf_slow >= prev_htf_slow
    weekly_down = h_close < htf_fast < htf_slow and htf_fast < prev_htf_fast and htf_slow <= prev_htf_slow

    zone_w = max(close_f * 0.006, atr_v * 0.8)
    breakout_pad = zone_w * 0.20

    body = abs(close_f - curr_o)
    upper_wick = max(0.0, curr_hi - max(close_f, curr_o))
    lower_wick = max(0.0, min(close_f, curr_o) - curr_lo)
    true_range = max(1e-12, curr_hi - curr_lo)

    bull_pin = (
        close_f > curr_o
        and lower_wick >= max(body * 1.5, atr_v * 0.3)
        and lower_wick >= upper_wick * 1.5
        and body / true_range <= 0.55
    )
    bear_pin = (
        close_f < curr_o
        and upper_wick >= max(body * 1.5, atr_v * 0.3)
        and upper_wick >= lower_wick * 1.5
        and body / true_range <= 0.55
    )
    bull_engulf = (
        close_f > curr_o
        and prev_c < prev_o
        and close_f >= prev_o
        and curr_o <= prev_c
    )
    bear_engulf = (
        close_f < curr_o
        and prev_c > prev_o
        and close_f <= prev_o
        and curr_o >= prev_c
    )
    vol_spike = volume_avg > 0 and volume >= volume_avg * 1.5

    touch_support = curr_lo <= support + zone_w or prev_lo <= support + zone_w
    touch_resistance = curr_hi >= resistance - zone_w or prev_hi >= resistance - zone_w

    bounce_long = close_f >= support - zone_w * 0.10 and close_f > curr_o and close_f > ema21 * 0.995
    bounce_short = close_f <= resistance + zone_w * 0.10 and close_f < curr_o and close_f < ema21 * 1.005

    long_reversal = touch_support and bounce_long and (bull_pin or bull_engulf) and (rsi_v <= 45.0 or vol_spike) and (not weekly_down)
    short_reversal = touch_resistance and bounce_short and (bear_pin or bear_engulf) and (rsi_v >= 55.0 or vol_spike) and (not weekly_up)

    strong_breakout = body >= atr_v * 0.8 and body / true_range >= 0.55
    breakout_long = (
        close_f >= resistance + breakout_pad
        and close_f > curr_o
        and strong_breakout
        and vol_spike
        and weekly_up
    )
    breakout_short = (
        close_f <= support - breakout_pad
        and close_f < curr_o
        and strong_breakout
        and vol_spike
        and weekly_down
    )

    long_entry = long_reversal or breakout_long
    short_entry = short_reversal or breakout_short

    long_mode = "reversal" if long_reversal else ("breakout" if breakout_long else "")
    short_mode = "reversal" if short_reversal else ("breakout" if breakout_short else "")

    long_stop_base = support - max(atr_v * 1.5, zone_w)
    short_stop_base = resistance + max(atr_v * 1.5, zone_w)
    if breakout_long:
        long_stop_base = resistance - max(atr_v * 1.2, zone_w * 0.8)
    if breakout_short:
        short_stop_base = support + max(atr_v * 1.2, zone_w * 0.8)

    long_trail = max(long_stop_base, ema21 - atr_v, exit_low - 0.5 * atr_v)
    short_trail = min(short_stop_base, ema21 + atr_v, exit_high + 0.5 * atr_v)

    min_stop_gap = max(atr_v * 0.25, close_f * 0.0015)
    if long_trail >= close_f - min_stop_gap:
        long_trail = close_f - min_stop_gap
    if short_trail <= close_f + min_stop_gap:
        short_trail = close_f + min_stop_gap

    bear_reject_from_res = touch_resistance and bounce_short and (bear_pin or bear_engulf)
    bull_reject_from_sup = touch_support and bounce_long and (bull_pin or bull_engulf)

    long_exit = bear_reject_from_res or breakout_short or (close_f < ema21 and close_f < support - breakout_pad)
    short_exit = bull_reject_from_sup or breakout_long or (close_f > ema21 and close_f > resistance + breakout_pad)

    return {
        "variant": "daily_sr_zones_v1",
        "vol_ok": True,
        "fresh_break_long": bool(breakout_long or long_reversal),
        "fresh_break_short": bool(breakout_short or short_reversal),
        "sr_support": float(support),
        "sr_resistance": float(resistance),
        "sr_zone_width": float(zone_w),
        "sr_weekly_up": bool(weekly_up),
        "sr_weekly_down": bool(weekly_down),
        "sr_touch_support": bool(touch_support),
        "sr_touch_resistance": bool(touch_resistance),
        "sr_long_mode": long_mode,
        "sr_short_mode": short_mode,
        "long_entry": bool(long_entry),
        "short_entry": bool(short_entry),
        "long_entry_l2": False,
        "short_entry_l2": False,
        "long_entry_l3": False,
        "short_entry_l3": False,
        "long_level": 1 if long_entry else 0,
        "short_level": 1 if short_entry else 0,
        "long_stop": float(long_trail),
        "short_stop": float(short_trail),
        "long_exit": bool(long_exit),
        "short_exit": bool(short_exit),
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver
    register_variant_input_resolver("daily_sr_zones_v1", _resolve_daily_sr_zones_v1)
