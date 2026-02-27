from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_xau_sibi_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    em = _f(inputs.ema_value, close_f)
    a = max(1e-12, _f(inputs.atr_value, 0.0))
    r = _f(inputs.rsi_value, 50.0)
    mh = _f(inputs.macd_hist_value, 0.0)
    width = _f(inputs.width, 0.0)
    width_avg = _f(inputs.width_avg, 0.0)

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
    prev2_hi = _f(inputs.prev2_high, curr_hi)
    prev2_lo = _f(inputs.prev2_low, curr_lo)
    prev3_hi = _f(inputs.prev3_high, prev_hi)
    prev3_lo = _f(inputs.prev3_low, prev_lo)

    exh = _f(inputs.exh, curr_hi)
    pb_high = _f(inputs.pb_high, curr_hi)
    prev_hhv = _f(inputs.prev_hhv, curr_hi)

    # SIBI/FVG approximation:
    # Previous bar-set (t-3, t-2, t-1) forms bearish inefficiency when:
    # c1.low(t-3) > c3.high(t-1), zone = [c3.high, c1.low].
    # Then current bar t revisits zone and rejects lower.
    gap_min = max(a * 0.04, close_f * 0.00025)
    strict_zone_lo = prev_hi
    strict_zone_hi = prev3_lo
    strict_zone_valid = strict_zone_hi > (strict_zone_lo + gap_min)
    strict_zone_size = max(0.0, strict_zone_hi - strict_zone_lo)
    strict_min_zone = max(a * 0.12, close_f * 0.0007)
    strict_zone_ok = strict_zone_valid and strict_zone_size >= strict_min_zone

    # Soft imbalance: use large displacement candle (t-2) upper body/range as
    # a "low-efficiency" retest zone when strict FVG is too sparse on XAU.
    disp_span = max(0.0, prev2_hi - prev2_lo)
    soft_zone_lo = prev2_lo + disp_span * 0.55
    soft_zone_hi = prev2_hi
    soft_zone_ok = disp_span >= max(a * 0.55, close_f * 0.0010)

    if strict_zone_ok:
        zone_lo = strict_zone_lo
        zone_hi = strict_zone_hi
    else:
        zone_lo = soft_zone_lo
        zone_hi = soft_zone_hi

    zone_size = max(0.0, zone_hi - zone_lo)
    zone_ok = zone_size > max(a * 0.10, close_f * 0.0005) and (strict_zone_ok or soft_zone_ok)
    zone_mid = zone_lo + zone_size * 0.5

    # One-touch approximation: previous bar was still below/within lower edge,
    # current bar pokes into zone and fails to hold above mid.
    tol = max(close_f * 0.00035, a * 0.08)
    touch_zone = zone_ok and (curr_hi >= (zone_lo - tol)) and (curr_lo <= (zone_hi + tol))
    prev_below = prev_c <= (zone_lo + tol)
    reject_down = touch_zone and (close_f <= (zone_mid + zone_size * 0.10)) and (close_f <= curr_o)
    first_touch_like = prev_below and reject_down

    impulse_prev_down = (prev_c < prev_o) and ((prev_o - prev_c) >= a * 0.25)
    break_down = close_f < min(prev_hhv, em)

    trend_down = h_close < h_ema_fast < h_ema_slow and trend_sep >= 0.00015
    bias_down = str(inputs.bias) in {"short", "neutral"}
    width_ok = width_avg > 0.0 and width >= width_avg * 0.50
    vol = max(0.0, _f(inputs.volume, 0.0))
    vol_avg = max(0.0, _f(inputs.volume_avg, 0.0))
    vol_ok = vol_avg <= 0.0 or vol >= vol_avg * 0.70

    structure_down = (prev_hi <= prev3_hi * 1.0020) or break_down

    short_entry_l1 = (
        trend_down
        and bias_down
        and zone_ok
        and first_touch_like
        and impulse_prev_down
        and structure_down
        and break_down
        and width_ok
        and vol_ok
        and bool(inputs.not_chasing_short)
        and r <= 58.0
        and mh <= 0.10
    )

    short_entry_l2 = (
        trend_down
        and zone_ok
        and touch_zone
        and reject_down
        and structure_down
        and bool(inputs.not_chasing_short)
        and (r <= 62.0)
        and (mh <= 0.20)
        and (bool(inputs.pullback_short) or bool(inputs.short_location_ok) or close_f <= em)
    )

    short_entry_l3 = False
    long_entry_l1 = False
    long_entry_l2 = False
    long_entry_l3 = False

    long_level = 0
    short_level = 1 if short_entry_l1 else (2 if short_entry_l2 else 0)

    stop_buf = max(a * 0.30, close_f * 0.0012)
    short_stop = max(exh, pb_high, curr_hi, zone_hi) + stop_buf
    long_stop = min(curr_lo, em) - max(a * 0.80, close_f * 0.002)

    min_stop_gap = max(a * 0.25, close_f * 0.0006)
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap

    return {
        "variant": "xau_sibi_v1",
        "trend_sep": float(trend_sep),
        "vol_ok": bool(vol_ok and width_ok),
        "fresh_break_long": False,
        "fresh_break_short": bool(break_down),
        "smc_sweep_long": False,
        "smc_sweep_short": bool(touch_zone),
        "smc_bullish_fvg": False,
        "smc_bearish_fvg": bool(zone_ok),
        "xau_sibi_zone_ok": bool(zone_ok),
        "xau_sibi_zone_mode": "strict" if strict_zone_ok else ("soft" if soft_zone_ok else "none"),
        "xau_sibi_touch": bool(touch_zone),
        "xau_sibi_reject": bool(reject_down),
        "xau_sibi_first_touch_like": bool(first_touch_like),
        "xau_sibi_zone_lo": float(zone_lo),
        "xau_sibi_zone_hi": float(zone_hi),
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
    register_variant_input_resolver("xau_sibi_v1", _resolve_xau_sibi_v1)
