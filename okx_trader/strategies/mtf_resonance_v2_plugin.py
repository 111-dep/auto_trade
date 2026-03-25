from __future__ import annotations

from typing import Any, Callable, Dict

from ..strategy_contract import VariantSignalInputs


def _f(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _resolve_mtf_resonance_v2(inputs: VariantSignalInputs) -> Dict[str, Any]:
    close_f = _f(inputs.close, 0.0)
    curr_o = _f(inputs.current_open, close_f)
    curr_hi = _f(inputs.current_high, close_f)
    curr_lo = _f(inputs.current_low, close_f)
    prev_hi = _f(inputs.prev_high, curr_hi)
    prev_c = _f(inputs.prev_close, close_f)

    daily_close = _f(inputs.h_close, close_f)
    daily_ema200 = _f(inputs.h_ema_fast, daily_close)
    prev_daily_ema200 = _f(inputs.prev_h_ema_fast, daily_ema200)
    daily_filter_long = daily_close > daily_ema200 and daily_ema200 > prev_daily_ema200

    loc_close = _f(inputs.loc_close, close_f)
    loc_ema20 = _f(inputs.loc_ema_fast, loc_close)
    loc_ema50 = _f(inputs.loc_ema_slow, loc_close)
    prev_loc_ema20 = _f(inputs.prev_loc_ema_fast, loc_ema20)
    prev_loc_ema50 = _f(inputs.prev_loc_ema_slow, loc_ema50)
    loc_rsi = _f(inputs.loc_rsi_value, 50.0)
    loc_atr = max(1e-12, _f(inputs.loc_atr_value, close_f * 0.003))
    loc_low = _f(inputs.loc_current_low, curr_lo)

    zone_lo = min(loc_ema20, loc_ema50)
    zone_hi = max(loc_ema20, loc_ema50)
    loc_trend_sep = abs(loc_ema20 - loc_ema50) / max(abs(loc_close), 1e-9)
    loc_trend_long = (
        loc_ema20 > loc_ema50
        and loc_ema20 > prev_loc_ema20
        and loc_ema50 > prev_loc_ema50
        and loc_close >= loc_ema20
        and loc_rsi >= 52.0
        and loc_trend_sep >= 0.0010
    )

    hour_open = _f(inputs.hour_open, close_f)
    hour_close = _f(inputs.hour_close, close_f)
    hour_low = _f(inputs.hour_low, curr_lo)
    hour_prev_close = _f(inputs.hour_prev_close, hour_close)
    hour_rsi = _f(inputs.hour_rsi_value, _f(inputs.rsi_value, 50.0))
    hour_zone_touch = hour_low <= zone_hi * 1.0025 and hour_low >= zone_lo * 0.9900
    hour_pullback_long = hour_zone_touch and hour_rsi <= 48.0 and hour_close >= zone_lo * 0.9925
    hour_reclaim = hour_close >= hour_open or hour_close >= hour_prev_close

    ltf_ema20 = _f(inputs.ema_value, close_f)
    ltf_macd = _f(inputs.macd_hist_value, 0.0)
    prev_ltf_macd = _f(inputs.prev_macd_hist, ltf_macd)
    bullish_candle = close_f > curr_o
    above_ltf_ema = close_f > ltf_ema20
    macd_turn_up = ltf_macd > 0.0 and prev_ltf_macd <= 0.0
    micro_break = close_f > prev_hi
    trigger_score = int(bullish_candle) + int(above_ltf_ema) + int(macd_turn_up or micro_break)
    trigger_long = bullish_candle and above_ltf_ema and trigger_score >= 3

    long_entry_l1 = daily_filter_long and loc_trend_long and hour_pullback_long and hour_reclaim and trigger_long
    short_entry_l1 = False
    long_entry_l2 = False
    short_entry_l2 = False
    long_entry_l3 = False
    short_entry_l3 = False

    swing_low = min(hour_low, loc_low, _f(inputs.pb_low, curr_lo), curr_lo)
    long_stop = swing_low - loc_atr
    min_stop_gap = max(loc_atr * 0.25, close_f * 0.0012)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    short_stop = close_f + min_stop_gap

    return {
        "variant": "mtf_resonance_v2",
        "trend_sep": float(loc_trend_sep),
        "vol_ok": True,
        "fresh_break_long": bool(trigger_long),
        "fresh_break_short": False,
        "mtf_daily_filter_long": bool(daily_filter_long),
        "mtf_loc_trend_long": bool(loc_trend_long),
        "mtf_hour_pullback_long": bool(hour_pullback_long),
        "mtf_hour_zone_touch": bool(hour_zone_touch),
        "mtf_hour_rsi": float(hour_rsi),
        "mtf_trigger_score": int(trigger_score),
        "long_entry": bool(long_entry_l1),
        "short_entry": False,
        "long_entry_l2": False,
        "short_entry_l2": False,
        "long_entry_l3": False,
        "short_entry_l3": False,
        "long_level": 1 if long_entry_l1 else 0,
        "short_level": 0,
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
    }


def register(
    *,
    register_variant_resolver: Callable[[str, Callable[..., Dict[str, Any]]], None],
    register_variant_input_resolver: Callable[[str, Callable[[VariantSignalInputs], Dict[str, Any]]], None],
) -> None:
    _ = register_variant_resolver
    register_variant_input_resolver("mtf_resonance_v2", _resolve_mtf_resonance_v2)
