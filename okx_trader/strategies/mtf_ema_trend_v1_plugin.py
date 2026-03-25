from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_mtf_ema_trend_v1(inputs: VariantSignalInputs) -> Dict[str, Any]:
    p = inputs.p
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_lo = _f(inputs.prev_low, curr_lo)

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

    cross_up = prev_loc_fast <= prev_loc_slow and loc_fast > loc_slow
    cross_dn = prev_loc_fast >= prev_loc_slow and loc_fast < loc_slow
    loc_bull = loc_close > loc_fast and loc_close > loc_slow
    loc_bear = loc_close < loc_fast and loc_close < loc_slow
    ltf_bull = close_f >= curr_o
    ltf_bear = close_f <= curr_o

    chase_k = max(0.0015, float(getattr(p, "max_chase_from_ema", 0.0035)))
    not_extended_long = close_f <= loc_fast * (1.0 + chase_k)
    not_extended_short = close_f >= loc_fast * (1.0 - chase_k)

    long_entry = htf_up and cross_up and loc_bull and ltf_bull and not_extended_long
    short_entry = htf_down and cross_dn and loc_bear and ltf_bear and not_extended_short

    structure_long = min(loc_lo, curr_lo, prev_lo, _f(inputs.pb_low, curr_lo))
    structure_short = max(loc_hi, curr_hi, prev_hi, _f(inputs.pb_high, curr_hi))
    initial_long_stop = structure_long - 2.0 * loc_atr
    initial_short_stop = structure_short + 2.0 * loc_atr

    trail_long = min(htf_fast, loc_slow) - loc_atr
    trail_short = max(htf_fast, loc_slow) + loc_atr
    long_stop = max(initial_long_stop, trail_long) if htf_up else initial_long_stop
    short_stop = min(initial_short_stop, trail_short) if htf_down else initial_short_stop

    min_stop_gap = max(loc_atr * 0.25, close_f * 0.0015)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    long_exit = cross_dn or loc_close < loc_slow or (inputs.bias != "long" and loc_close < loc_fast)
    short_exit = cross_up or loc_close > loc_slow or (inputs.bias != "short" and loc_close > loc_fast)

    return {
        "variant": "mtf_ema_trend_v1",
        "vol_ok": True,
        "fresh_break_long": bool(cross_up),
        "fresh_break_short": bool(cross_dn),
        "mtf_htf_up": bool(htf_up),
        "mtf_htf_down": bool(htf_down),
        "mtf_loc_cross_up": bool(cross_up),
        "mtf_loc_cross_dn": bool(cross_dn),
        "mtf_loc_bull": bool(loc_bull),
        "mtf_loc_bear": bool(loc_bear),
        "mtf_not_extended_long": bool(not_extended_long),
        "mtf_not_extended_short": bool(not_extended_short),
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
    register_variant_input_resolver("mtf_ema_trend_v1", _resolve_mtf_ema_trend_v1)
