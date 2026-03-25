from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_mtf_ema_trend_v3(inputs: VariantSignalInputs) -> Dict[str, Any]:
    p = inputs.p
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)
    prev_close = _f(inputs.prev_close, close_f)

    h_close = _f(inputs.h_close, close_f)
    htf_fast = _f(inputs.h_ema_fast, h_close)
    htf_slow = _f(inputs.h_ema_slow, h_close)
    prev_htf_fast = _f(inputs.prev_h_ema_fast, htf_fast)
    prev_htf_slow = _f(inputs.prev_h_ema_slow, htf_slow)

    loc_close = _f(inputs.loc_close, close_f)
    loc_fast = _f(inputs.loc_ema_fast, loc_close)
    loc_slow = _f(inputs.loc_ema_slow, loc_close)
    prev_loc_fast = _f(inputs.prev_loc_ema_fast, loc_fast)
    prev_loc_slow = _f(inputs.prev_loc_ema_slow, loc_slow)
    loc_hi = _f(inputs.loc_current_high, curr_hi)
    loc_lo = _f(inputs.loc_current_low, curr_lo)
    loc_atr = max(1e-12, _f(inputs.loc_atr_value, _f(inputs.atr_value, close_f * 0.003)))
    loc_rsi = _f(inputs.loc_rsi_value, _f(inputs.rsi_value, 50.0))

    ltf_ema = _f(inputs.ema_value, close_f)
    ltf_rsi = _f(inputs.rsi_value, 50.0)
    macd_hist = _f(inputs.macd_hist_value, 0.0)
    prev_macd_hist = _f(inputs.prev_macd_hist, macd_hist)

    hour_open = _f(inputs.hour_open, curr_o)
    hour_close = _f(inputs.hour_close, close_f)
    hour_high = _f(inputs.hour_high, curr_hi)
    hour_low = _f(inputs.hour_low, curr_lo)
    hour_prev_close = _f(inputs.hour_prev_close, hour_close)
    hour_rsi = _f(inputs.hour_rsi_value, ltf_rsi)

    pullback_tol = max(0.0008, float(getattr(p, "pullback_tolerance", 0.0015)) * 1.25)
    chase_k = max(0.0018, float(getattr(p, "max_chase_from_ema", 0.0035)))

    htf_sep = abs(htf_fast - htf_slow) / max(abs(h_close), 1e-9)
    loc_sep = abs(loc_fast - loc_slow) / max(abs(loc_close), 1e-9)

    htf_up = (
        h_close > htf_fast > htf_slow
        and htf_fast > prev_htf_fast
        and htf_slow >= prev_htf_slow
        and htf_sep >= 0.0010
    )
    htf_down = (
        h_close < htf_fast < htf_slow
        and htf_fast < prev_htf_fast
        and htf_slow <= prev_htf_slow
        and htf_sep >= 0.0014
    )

    loc_trend_long = (
        loc_fast > loc_slow
        and loc_fast >= prev_loc_fast
        and loc_slow >= prev_loc_slow
        and loc_close >= loc_slow
        and loc_sep >= 0.0008
    )
    loc_trend_short = (
        loc_fast < loc_slow
        and loc_fast <= prev_loc_fast
        and loc_slow <= prev_loc_slow
        and loc_close <= loc_slow
        and loc_sep >= 0.0012
    )

    zone_lo = min(loc_fast, loc_slow)
    zone_hi = max(loc_fast, loc_slow)
    zone_mid = (zone_lo + zone_hi) * 0.5

    touched_long_zone = (
        curr_lo <= zone_hi * (1.0 + pullback_tol)
        or _f(inputs.pb_low, curr_lo) <= zone_hi * (1.0 + pullback_tol)
        or prev_lo <= zone_hi * (1.0 + pullback_tol)
    )
    deep_touched_long = (
        curr_lo <= zone_lo * (1.0 + 2.0 * pullback_tol)
        or _f(inputs.pb_low, curr_lo) <= zone_lo * (1.0 + 2.0 * pullback_tol)
    )
    hour_zone_touch_long = hour_low <= zone_hi * (1.0 + 2.0 * pullback_tol) and hour_low >= zone_lo * (1.0 - 4.0 * pullback_tol)
    hour_reclaim_long = hour_close >= zone_mid and hour_close >= hour_open and hour_close >= hour_prev_close

    touched_short_zone = (
        curr_hi >= zone_lo * (1.0 - pullback_tol)
        or _f(inputs.pb_high, curr_hi) >= zone_lo * (1.0 - pullback_tol)
        or prev_hi >= zone_lo * (1.0 - pullback_tol)
    )
    deep_touched_short = (
        curr_hi >= zone_hi * (1.0 - 2.0 * pullback_tol)
        or _f(inputs.pb_high, curr_hi) >= zone_hi * (1.0 - 2.0 * pullback_tol)
    )
    hour_zone_touch_short = hour_high >= zone_lo * (1.0 - 2.0 * pullback_tol) and hour_high <= zone_hi * (1.0 + 4.0 * pullback_tol)
    hour_reclaim_short = hour_close <= zone_mid and hour_close <= hour_open and hour_close <= hour_prev_close

    ltf_reclaim_long = (
        close_f > curr_o
        and close_f > ltf_ema
        and (prev_close <= ltf_ema * (1.0 + pullback_tol) or curr_lo <= ltf_ema * (1.0 + pullback_tol) or hour_zone_touch_long)
    )
    ltf_reclaim_short = (
        close_f < curr_o
        and close_f < ltf_ema
        and (prev_close >= ltf_ema * (1.0 - pullback_tol) or curr_hi >= ltf_ema * (1.0 - pullback_tol) or hour_zone_touch_short)
    )

    not_extended_long = close_f <= loc_fast * (1.0 + chase_k)
    not_extended_short = close_f >= loc_fast * (1.0 - chase_k * 0.85)

    momentum_long_score = (
        int(close_f > prev_close)
        + int(ltf_rsi >= 50.0)
        + int(loc_rsi >= 50.0)
        + int(macd_hist >= prev_macd_hist)
        + int(close_f > prev_hi)
    )
    momentum_short_score = (
        int(close_f < prev_close)
        + int(ltf_rsi <= 48.0)
        + int(loc_rsi <= 48.0)
        + int(macd_hist <= prev_macd_hist)
        + int(close_f < prev_lo)
    )

    breakout_long = htf_up and loc_trend_long and close_f > prev_hi and close_f > ltf_ema and not_extended_long
    breakout_short = htf_down and loc_trend_short and close_f < prev_lo and close_f < ltf_ema and not_extended_short

    pullback_long_ok = bool(inputs.pullback_long) or bool(inputs.long_location_ok) or touched_long_zone
    pullback_short_ok = bool(inputs.pullback_short) or bool(inputs.short_location_ok) or touched_short_zone

    long_entry_l1 = (
        htf_up
        and loc_trend_long
        and pullback_long_ok
        and touched_long_zone
        and hour_reclaim_long
        and ltf_reclaim_long
        and not_extended_long
        and momentum_long_score >= 3
        and bool(inputs.not_chasing_long)
    )
    long_entry_l2 = (
        htf_up
        and loc_trend_long
        and deep_touched_long
        and hour_reclaim_long
        and ltf_reclaim_long
        and not_extended_long
        and momentum_long_score >= 2
        and (ltf_rsi <= 54.0 or hour_rsi <= 49.0)
        and bool(inputs.not_chasing_long)
    )
    long_entry_l3 = (
        breakout_long
        and momentum_long_score >= 4
        and loc_sep >= 0.0012
        and htf_sep >= 0.0012
        and bool(inputs.not_chasing_long)
    )

    short_entry_l1 = (
        htf_down
        and loc_trend_short
        and pullback_short_ok
        and touched_short_zone
        and hour_reclaim_short
        and ltf_reclaim_short
        and not_extended_short
        and momentum_short_score >= 4
        and hour_rsi >= 54.0
        and bool(inputs.not_chasing_short)
    )
    short_entry_l2 = (
        htf_down
        and loc_trend_short
        and deep_touched_short
        and hour_reclaim_short
        and ltf_reclaim_short
        and not_extended_short
        and momentum_short_score >= 3
        and hour_rsi >= 58.0
        and bool(inputs.not_chasing_short)
    )
    short_entry_l3 = (
        breakout_short
        and momentum_short_score >= 4
        and loc_sep >= 0.0015
        and htf_sep >= 0.0018
        and bool(inputs.not_chasing_short)
    )

    long_level = 3 if long_entry_l3 else (2 if long_entry_l2 else (1 if long_entry_l1 else 0))
    short_level = 3 if short_entry_l3 else (2 if short_entry_l2 else (1 if short_entry_l1 else 0))

    structure_long = min(zone_lo, loc_lo, curr_lo, prev_lo, hour_low, _f(inputs.pb_low, curr_lo))
    structure_short = max(zone_hi, loc_hi, curr_hi, prev_hi, hour_high, _f(inputs.pb_high, curr_hi))
    initial_long_stop = structure_long - 1.1 * loc_atr
    initial_short_stop = structure_short + 1.15 * loc_atr

    trail_long = min(htf_fast, loc_slow) - 0.8 * loc_atr
    trail_short = max(htf_fast, loc_slow) + 0.8 * loc_atr
    long_stop = max(initial_long_stop, trail_long) if loc_trend_long else initial_long_stop
    short_stop = min(initial_short_stop, trail_short) if loc_trend_short else initial_short_stop

    min_stop_gap = max(loc_atr * 0.25, close_f * 0.0012)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    cross_dn = prev_loc_fast >= prev_loc_slow and loc_fast < loc_slow
    cross_up = prev_loc_fast <= prev_loc_slow and loc_fast > loc_slow
    long_exit = cross_dn or (loc_close < zone_lo and ltf_rsi < 46.0) or (htf_down and close_f < loc_fast)
    short_exit = cross_up or (loc_close > zone_hi and ltf_rsi > 54.0) or (htf_up and close_f > loc_fast)

    return {
        "variant": "mtf_ema_trend_v3",
        "vol_ok": True,
        "fresh_break_long": bool(breakout_long),
        "fresh_break_short": bool(breakout_short),
        "mtf3_htf_up": bool(htf_up),
        "mtf3_htf_down": bool(htf_down),
        "mtf3_loc_trend_long": bool(loc_trend_long),
        "mtf3_loc_trend_short": bool(loc_trend_short),
        "mtf3_touched_long_zone": bool(touched_long_zone),
        "mtf3_touched_short_zone": bool(touched_short_zone),
        "mtf3_deep_long": bool(deep_touched_long),
        "mtf3_deep_short": bool(deep_touched_short),
        "mtf3_hour_reclaim_long": bool(hour_reclaim_long),
        "mtf3_hour_reclaim_short": bool(hour_reclaim_short),
        "mtf3_momentum_long_score": int(momentum_long_score),
        "mtf3_momentum_short_score": int(momentum_short_score),
        "long_entry": bool(long_level > 0),
        "short_entry": bool(short_level > 0),
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
    register_variant_input_resolver("mtf_ema_trend_v3", _resolve_mtf_ema_trend_v3)
