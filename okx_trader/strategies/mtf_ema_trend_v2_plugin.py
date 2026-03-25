from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_mtf_ema_trend_v2(inputs: VariantSignalInputs) -> Dict[str, Any]:
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
    ltf_rsi = _f(inputs.rsi_value, 50.0)
    macd_hist = _f(inputs.macd_hist_value, 0.0)
    prev_macd_hist = _f(inputs.prev_macd_hist, macd_hist)

    pullback_tol = max(0.0010, float(getattr(p, "pullback_tolerance", 0.0015)) * 1.5)
    chase_k = max(0.0020, float(getattr(p, "max_chase_from_ema", 0.0035)) * 0.9)

    htf_up = (
        inputs.bias == "long"
        and h_close > htf_fast > htf_slow
        and htf_fast > prev_htf_fast
        and htf_slow >= prev_htf_slow
    )
    htf_down = (
        inputs.bias == "short"
        and h_close < htf_fast < htf_slow
        and htf_fast < prev_htf_fast
        and htf_slow <= prev_htf_slow
    )

    loc_trend_long = (
        loc_fast > loc_slow
        and loc_fast >= prev_loc_fast
        and loc_slow >= prev_loc_slow
        and loc_close >= loc_slow
    )
    loc_trend_short = (
        loc_fast < loc_slow
        and loc_fast <= prev_loc_fast
        and loc_slow <= prev_loc_slow
        and loc_close <= loc_slow
    )

    pb_low = _f(inputs.pb_low, curr_lo)
    pb_high = _f(inputs.pb_high, curr_hi)
    touched_long_zone = (
        curr_lo <= loc_fast * (1.0 + pullback_tol)
        or pb_low <= loc_fast * (1.0 + pullback_tol)
        or prev_lo <= loc_fast * (1.0 + pullback_tol)
    )
    touched_short_zone = (
        curr_hi >= loc_fast * (1.0 - pullback_tol)
        or pb_high >= loc_fast * (1.0 - pullback_tol)
        or prev_hi >= loc_fast * (1.0 - pullback_tol)
    )

    reclaim_long = (
        close_f > curr_o
        and close_f > loc_fast
        and (prev_close <= loc_fast * (1.0 + pullback_tol) or curr_lo <= loc_fast * (1.0 + pullback_tol))
    )
    reclaim_short = (
        close_f < curr_o
        and close_f < loc_fast
        and (prev_close >= loc_fast * (1.0 - pullback_tol) or curr_hi >= loc_fast * (1.0 - pullback_tol))
    )

    not_extended_long = close_f <= loc_fast * (1.0 + chase_k)
    not_extended_short = close_f >= loc_fast * (1.0 - chase_k)

    momentum_long_score = int(close_f > prev_close) + int(ltf_rsi >= 50.0) + int(loc_rsi >= 50.0) + int(
        macd_hist >= 0.0 or prev_macd_hist <= macd_hist
    )
    momentum_short_score = int(close_f < prev_close) + int(ltf_rsi <= 50.0) + int(loc_rsi <= 50.0) + int(
        macd_hist <= 0.0 or prev_macd_hist >= macd_hist
    )

    long_entry = htf_up and loc_trend_long and touched_long_zone and reclaim_long and not_extended_long and momentum_long_score >= 2
    short_entry = (
        htf_down
        and loc_trend_short
        and touched_short_zone
        and reclaim_short
        and not_extended_short
        and momentum_short_score >= 2
    )

    structure_long = min(loc_lo, curr_lo, prev_lo, pb_low)
    structure_short = max(loc_hi, curr_hi, prev_hi, pb_high)
    initial_long_stop = structure_long - 2.0 * loc_atr
    initial_short_stop = structure_short + 2.0 * loc_atr

    trail_long = min(htf_fast, loc_slow) - loc_atr
    trail_short = max(htf_fast, loc_slow) + loc_atr
    long_stop = max(initial_long_stop, trail_long) if loc_trend_long else initial_long_stop
    short_stop = min(initial_short_stop, trail_short) if loc_trend_short else initial_short_stop

    min_stop_gap = max(loc_atr * 0.25, close_f * 0.0015)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    cross_dn = prev_loc_fast >= prev_loc_slow and loc_fast < loc_slow
    cross_up = prev_loc_fast <= prev_loc_slow and loc_fast > loc_slow
    long_exit = cross_dn or loc_close < loc_slow or (inputs.bias != "long" and loc_close < loc_fast)
    short_exit = cross_up or loc_close > loc_slow or (inputs.bias != "short" and loc_close > loc_fast)

    return {
        "variant": "mtf_ema_trend_v2",
        "vol_ok": True,
        "fresh_break_long": bool(reclaim_long),
        "fresh_break_short": bool(reclaim_short),
        "mtf_htf_up": bool(htf_up),
        "mtf_htf_down": bool(htf_down),
        "mtf_loc_trend_long": bool(loc_trend_long),
        "mtf_loc_trend_short": bool(loc_trend_short),
        "mtf_touched_long_zone": bool(touched_long_zone),
        "mtf_touched_short_zone": bool(touched_short_zone),
        "mtf_reclaim_long": bool(reclaim_long),
        "mtf_reclaim_short": bool(reclaim_short),
        "mtf_momentum_long_score": int(momentum_long_score),
        "mtf_momentum_short_score": int(momentum_short_score),
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
    register_variant_input_resolver("mtf_ema_trend_v2", _resolve_mtf_ema_trend_v2)
