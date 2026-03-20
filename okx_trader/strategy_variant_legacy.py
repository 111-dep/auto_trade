from __future__ import annotations

from typing import Any, Dict, Optional

from .models import StrategyParams
from .strategy_contract import VariantSignalInputs

_VARIANT_CLASSIC = "classic"
_VARIANT_BTCETH_V1 = "btceth_v1"
_VARIANT_BTCETH_SMC_V0 = "btceth_smc_v0"
_VARIANT_BTCETH_SMC_V1 = "btceth_smc_v1"
_VARIANT_BTCETH_SMC_A1 = "btceth_smc_a1"
_VARIANT_BTCETH_SMC_A2 = "btceth_smc_a2"
_VARIANT_BTCETH_BREAKOUT_V1 = "btceth_breakout_v1"
_VARIANT_BTCETH_COMBO_V1 = "btceth_combo_v1"
_VARIANT_BTCETH_CANDLE_MACD_V1 = "btceth_candle_macd_v1"
_VARIANT_ELDER_TSS_V1 = "elder_tss_v1"
_VARIANT_R_BREAKER_V1 = "r_breaker_v1"
_VARIANT_RANGE_REVERSION_V1 = "range_reversion_v1"
_VARIANT_RIGHT_REVERSAL_V1 = "right_reversal_v1"
_VARIANT_XAU_SIBI_V1 = "xau_sibi_v1"


def normalize_strategy_variant(raw: str) -> str:
    key = str(raw or "").strip().lower().replace("-", "_")
    if key in {"btceth", "btc_eth", "btceth_v1", "btc_eth_v1"}:
        return _VARIANT_BTCETH_V1
    if key in {
        "btceth_candle_macd",
        "btc_eth_candle_macd",
        "btceth_candle_macd_v1",
        "btc_eth_candle_macd_v1",
        "candle_macd",
        "cmacd",
    }:
        return _VARIANT_BTCETH_CANDLE_MACD_V1
    if key in {"btceth_combo", "btc_eth_combo", "btceth_combo_v1", "btc_eth_combo_v1", "combo"}:
        return _VARIANT_BTCETH_COMBO_V1
    if key in {"btceth_smc_v1", "btc_eth_smc_v1"}:
        return _VARIANT_BTCETH_SMC_V1
    if key in {"btceth_breakout", "btc_eth_breakout", "btceth_breakout_v1", "btc_eth_breakout_v1", "breakout"}:
        return _VARIANT_BTCETH_BREAKOUT_V1
    if key in {"elder_tss_v1", "elder_tss", "elder", "triple_screen", "tss"}:
        return _VARIANT_ELDER_TSS_V1
    if key in {"r_breaker_v1", "r_breaker", "rbreaker", "rb"}:
        return _VARIANT_R_BREAKER_V1
    if key in {"range_reversion_v1", "range_reversion", "range_revert", "rr"}:
        return _VARIANT_RANGE_REVERSION_V1
    if key in {"right_reversal_v1", "right_reversal", "rightside_reversal", "rrv1"}:
        return _VARIANT_RIGHT_REVERSAL_V1
    if key in {"xau_sibi_v1", "xau_sibi", "sibi", "fvg_pullback_v1", "xau_fvg"}:
        return _VARIANT_XAU_SIBI_V1
    if key in {"btceth_smc_a2", "btc_eth_smc_a2", "smc_a2"}:
        return _VARIANT_BTCETH_SMC_A2
    if key in {"btceth_smc_a1", "btc_eth_smc_a1", "smc_a1", "smc_plus", "smc_a_plus"}:
        return _VARIANT_BTCETH_SMC_A1
    if key in {"btceth_smc", "btc_eth_smc", "btceth_smc_v0", "btc_eth_smc_v0", "smc", "ict"}:
        return _VARIANT_BTCETH_SMC_V0
    return _VARIANT_CLASSIC


def resolve_variant_signal_state(
    *,
    p: StrategyParams,
    bias: str,
    close: float,
    ema_value: float,
    rsi_value: float,
    macd_hist_value: float,
    atr_value: float,
    hhv: float,
    llv: float,
    exl: float,
    exh: float,
    pb_low: float,
    pb_high: float,
    h_close: float,
    h_ema_fast: float,
    h_ema_slow: float,
    width: float,
    width_avg: float,
    long_location_ok: bool,
    short_location_ok: bool,
    pullback_long: bool,
    pullback_short: bool,
    not_chasing_long: bool,
    not_chasing_short: bool,
    prev_hhv: Optional[float] = None,
    prev_llv: Optional[float] = None,
    current_high: Optional[float] = None,
    current_low: Optional[float] = None,
    prev_high: Optional[float] = None,
    prev_low: Optional[float] = None,
    prev2_high: Optional[float] = None,
    prev2_low: Optional[float] = None,
    prev3_high: Optional[float] = None,
    prev3_low: Optional[float] = None,
    current_open: Optional[float] = None,
    prev_open: Optional[float] = None,
    prev_close: Optional[float] = None,
    upper_band: Optional[float] = None,
    lower_band: Optional[float] = None,
    mid_band: Optional[float] = None,
    prev_macd_hist: Optional[float] = None,
    volume: Optional[float] = None,
    volume_avg: Optional[float] = None,
    prev_day_high: Optional[float] = None,
    prev_day_low: Optional[float] = None,
    prev_day_close: Optional[float] = None,
    day_high_so_far: Optional[float] = None,
    day_low_so_far: Optional[float] = None,
) -> Dict[str, Any]:
    variant = normalize_strategy_variant(getattr(p, "strategy_variant", _VARIANT_CLASSIC))

    close_f = float(close)
    em = float(ema_value)
    r = float(rsi_value)
    mh = float(macd_hist_value)
    a = max(1e-12, float(atr_value))

    long_rsi_l1 = float(p.rsi_long_min)
    long_rsi_l2 = float(p.rsi_long_min - p.l2_rsi_relax)
    long_rsi_l3 = float(p.rsi_long_min - p.l3_rsi_relax)
    short_rsi_l1 = float(p.rsi_short_max)
    short_rsi_l2 = float(p.rsi_short_max + p.l2_rsi_relax)
    short_rsi_l3 = float(p.rsi_short_max + p.l3_rsi_relax)

    vol_ok = float(width_avg) > 0 and float(width) > float(width_avg) * float(p.bb_width_k)
    trend_sep = abs(float(h_ema_fast) - float(h_ema_slow)) / max(abs(float(h_close)), 1e-9)
    fresh_break_long = close_f > float(prev_hhv if prev_hhv is not None else hhv)
    fresh_break_short = close_f < float(prev_llv if prev_llv is not None else llv)
    sweep_long = False
    sweep_short = False
    bullish_fvg = False
    bearish_fvg = False
    squeeze = False
    vol_spike_2x = False
    bull_pattern = False
    bear_pattern = False
    macd_gc = False
    macd_dc = False
    touch_lower = False
    touch_upper = False
    breakout_long = False
    breakout_short = False
    retest_long = False
    retest_short = False
    ready = False
    reversal_long = False
    reversal_short = False

    if variant == _VARIANT_BTCETH_V1:
        min_trend_sep = 0.0015
        chase_factor = 0.8
        stop_atr_floor = 2.2

        trend_ok = trend_sep >= min_trend_sep
        btceth_vol_k = max(0.8, float(p.bb_width_k) * 0.8)
        vol_ok = float(width_avg) > 0 and float(width) > float(width_avg) * btceth_vol_k

        not_chasing_long_v = close_f <= em * (1.0 + float(p.max_chase_from_ema) * chase_factor)
        not_chasing_short_v = close_f >= em * (1.0 - float(p.max_chase_from_ema) * chase_factor)

        long_rsi_l1 = max(long_rsi_l1, 52.0)
        long_rsi_l2 = max(long_rsi_l2, 48.0)
        long_rsi_l3 = max(long_rsi_l3, 46.0)
        short_rsi_l1 = min(short_rsi_l1, 48.0)
        short_rsi_l2 = min(short_rsi_l2, 52.0)
        short_rsi_l3 = min(short_rsi_l3, 54.0)

        long_entry_l1 = (
            bias == "long"
            and trend_ok
            and long_location_ok
            and fresh_break_long
            and close_f > em
            and vol_ok
            and pullback_long
            and not_chasing_long_v
            and r > long_rsi_l1
            and mh > 0
        )
        short_entry_l1 = (
            bias == "short"
            and trend_ok
            and short_location_ok
            and fresh_break_short
            and close_f < em
            and vol_ok
            and pullback_short
            and not_chasing_short_v
            and r < short_rsi_l1
            and mh < 0
        )
        long_entry_l2 = (
            bias == "long"
            and trend_ok
            and long_location_ok
            and close_f > em
            and pullback_long
            and not_chasing_long_v
            and r > long_rsi_l2
            and mh >= 0
            and (fresh_break_long or vol_ok)
        )
        short_entry_l2 = (
            bias == "short"
            and trend_ok
            and short_location_ok
            and close_f < em
            and pullback_short
            and not_chasing_short_v
            and r < short_rsi_l2
            and mh <= 0
            and (fresh_break_short or vol_ok)
        )
        long_entry_l3 = (
            bias == "long"
            and trend_ok
            and long_location_ok
            and close_f > em
            and pullback_long
            and r > long_rsi_l3
            and mh >= 0
        )
        short_entry_l3 = (
            bias == "short"
            and trend_ok
            and short_location_ok
            and close_f < em
            and pullback_short
            and r < short_rsi_l3
            and mh <= 0
        )

        stop_atr_mult = max(float(p.atr_stop_mult), stop_atr_floor)
        long_stop = min(float(exl), float(pb_low), em - (a * stop_atr_mult), close_f - (a * stop_atr_mult))
        short_stop = max(float(exh), float(pb_high), em + (a * stop_atr_mult), close_f + (a * stop_atr_mult))
    elif variant == _VARIANT_BTCETH_SMC_V0:
        min_trend_sep = 0.0004
        trend_ok = trend_sep >= min_trend_sep
        fresh_break_long = close_f > float(prev_hhv if prev_hhv is not None else hhv)
        fresh_break_short = close_f < float(prev_llv if prev_llv is not None else llv)

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p_hi = float(prev_high) if prev_high is not None else hi
        p_lo = float(prev_low) if prev_low is not None else lo
        p2_hi = float(prev2_high) if prev2_high is not None else p_hi
        p2_lo = float(prev2_low) if prev2_low is not None else p_lo

        prev_liq_low = float(prev_llv if prev_llv is not None else llv)
        prev_liq_high = float(prev_hhv if prev_hhv is not None else hhv)
        bar_range = max(1e-9, hi - lo)
        reclaim_long = close_f >= (lo + bar_range * 0.55)
        reclaim_short = close_f <= (hi - bar_range * 0.55)
        sweep_long = ((lo < prev_liq_low) or (float(pb_low) < prev_liq_low)) and reclaim_long
        sweep_short = ((hi > prev_liq_high) or (float(pb_high) > prev_liq_high)) and reclaim_short
        bullish_fvg = lo > p2_hi
        bearish_fvg = hi < p2_lo
        displacement_long = (close_f > em) and (mh >= 0) and ((close_f - em) >= a * 0.10)
        displacement_short = (close_f < em) and (mh <= 0) and ((em - close_f) >= a * 0.10)
        vol_ok = float(width_avg) > 0 and float(width) > float(width_avg) * max(0.65, float(p.bb_width_k) * 0.70)

        long_entry_l1 = (
            bias == "long"
            and trend_ok
            and sweep_long
            and displacement_long
            and (bullish_fvg or fresh_break_long or long_location_ok)
            and not_chasing_long
            and r > max(long_rsi_l1, 50.0)
        )
        short_entry_l1 = (
            bias == "short"
            and trend_ok
            and sweep_short
            and displacement_short
            and (bearish_fvg or fresh_break_short or short_location_ok)
            and not_chasing_short
            and r < min(short_rsi_l1, 50.0)
        )
        long_entry_l2 = (
            bias == "long"
            and trend_ok
            and sweep_long
            and displacement_long
            and (bullish_fvg or vol_ok or fresh_break_long or long_location_ok)
            and not_chasing_long
            and r > max(long_rsi_l2, 45.0)
        )
        short_entry_l2 = (
            bias == "short"
            and trend_ok
            and sweep_short
            and displacement_short
            and (bearish_fvg or vol_ok or fresh_break_short or short_location_ok)
            and not_chasing_short
            and r < min(short_rsi_l2, 55.0)
        )
        # SMC/ICT v0: keep L3 disabled to avoid over-trading on noisy structure.
        long_entry_l3 = False
        short_entry_l3 = False

        stop_atr_mult = max(float(p.atr_stop_mult), 2.5)
        long_stop = min(float(exl), float(pb_low), float(prev_llv if prev_llv is not None else llv), close_f - (a * stop_atr_mult))
        short_stop = max(float(exh), float(pb_high), float(prev_hhv if prev_hhv is not None else hhv), close_f + (a * stop_atr_mult))
    elif variant == _VARIANT_BTCETH_SMC_V1:
        min_trend_sep = 0.00085
        trend_ok = trend_sep >= min_trend_sep
        fresh_break_long = close_f > float(prev_hhv if prev_hhv is not None else hhv)
        fresh_break_short = close_f < float(prev_llv if prev_llv is not None else llv)

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p_hi = float(prev_high) if prev_high is not None else hi
        p_lo = float(prev_low) if prev_low is not None else lo
        p2_hi = float(prev2_high) if prev2_high is not None else p_hi
        p2_lo = float(prev2_low) if prev2_low is not None else p_lo
        prev_liq_low = float(prev_llv if prev_llv is not None else llv)
        prev_liq_high = float(prev_hhv if prev_hhv is not None else hhv)
        bar_range = max(1e-9, hi - lo)

        reclaim_long = close_f >= (lo + bar_range * 0.50)
        reclaim_short = close_f <= (hi - bar_range * 0.50)
        sweep_long = (lo < prev_liq_low) and reclaim_long
        sweep_short = (hi > prev_liq_high) and reclaim_short

        bullish_fvg = lo > p2_hi
        bearish_fvg = hi < p2_lo
        bos_long = close_f > prev_liq_high
        bos_short = close_f < prev_liq_low

        retrace_long = (lo <= em * (1.0 + 0.0015)) and (close_f >= em)
        retrace_short = (hi >= em * (1.0 - 0.0015)) and (close_f <= em)
        displacement_long = (close_f > em) and (mh >= 0) and ((close_f - em) >= a * 0.08)
        displacement_short = (close_f < em) and (mh <= 0) and ((em - close_f) >= a * 0.08)

        vol_ok = float(width_avg) > 0 and float(width) > float(width_avg) * max(0.60, float(p.bb_width_k) * 0.65)

        reversal_long = sweep_long and displacement_long and (bullish_fvg or vol_ok)
        reversal_short = sweep_short and displacement_short and (bearish_fvg or vol_ok)
        continuation_long = bos_long and retrace_long and pullback_long and mh >= 0
        continuation_short = bos_short and retrace_short and pullback_short and mh <= 0

        long_entry_l1 = (
            bias == "long"
            and trend_ok
            and long_location_ok
            and not_chasing_long
            and r > max(long_rsi_l1, 51.0)
            and (reversal_long or continuation_long)
        )
        short_entry_l1 = (
            bias == "short"
            and trend_ok
            and short_location_ok
            and not_chasing_short
            and r < min(short_rsi_l1, 49.0)
            and (reversal_short or continuation_short)
        )
        long_entry_l2 = (
            bias == "long"
            and trend_ok
            and not_chasing_long
            and r > max(long_rsi_l2, 47.0)
            and (
                reversal_long
                or continuation_long
                or (displacement_long and (long_location_ok or fresh_break_long))
            )
        )
        short_entry_l2 = (
            bias == "short"
            and trend_ok
            and not_chasing_short
            and r < min(short_rsi_l2, 53.0)
            and (
                reversal_short
                or continuation_short
                or (displacement_short and (short_location_ok or fresh_break_short))
            )
        )
        long_entry_l3 = False
        short_entry_l3 = False

        stop_atr_mult = max(float(p.atr_stop_mult), 2.3)
        long_stop = min(float(exl), float(pb_low), prev_liq_low, em - (a * stop_atr_mult), close_f - (a * stop_atr_mult))
        short_stop = max(float(exh), float(pb_high), prev_liq_high, em + (a * stop_atr_mult), close_f + (a * stop_atr_mult))
    elif variant == _VARIANT_BTCETH_SMC_A1:
        # SMC A+: stricter entry quality for BTC/ETH, lower frequency.
        min_trend_sep = 0.0007
        trend_ok = trend_sep >= min_trend_sep
        fresh_break_long = close_f > float(prev_hhv if prev_hhv is not None else hhv)
        fresh_break_short = close_f < float(prev_llv if prev_llv is not None else llv)

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p_hi = float(prev_high) if prev_high is not None else hi
        p_lo = float(prev_low) if prev_low is not None else lo
        p2_hi = float(prev2_high) if prev2_high is not None else p_hi
        p2_lo = float(prev2_low) if prev2_low is not None else p_lo
        prev_liq_low = float(prev_llv if prev_llv is not None else llv)
        prev_liq_high = float(prev_hhv if prev_hhv is not None else hhv)
        bar_range = max(1e-9, hi - lo)

        reclaim_long = close_f >= (lo + bar_range * 0.55)
        reclaim_short = close_f <= (hi - bar_range * 0.55)
        sweep_long = (lo < prev_liq_low) and reclaim_long
        sweep_short = (hi > prev_liq_high) and reclaim_short

        bullish_fvg = lo > p2_hi
        bearish_fvg = hi < p2_lo
        bos_long = close_f > prev_liq_high
        bos_short = close_f < prev_liq_low
        retrace_long = (lo <= em * (1.0 + 0.0012)) and (close_f >= em)
        retrace_short = (hi >= em * (1.0 - 0.0012)) and (close_f <= em)
        displacement_long = (close_f > em) and (mh >= 0) and ((close_f - em) >= a * 0.10)
        displacement_short = (close_f < em) and (mh <= 0) and ((em - close_f) >= a * 0.10)

        vol = max(0.0, float(volume or 0.0))
        vol_avg_v = max(0.0, float(volume_avg or 0.0))
        vol_ok = vol_avg_v > 0 and vol >= vol_avg_v * 0.95
        width_ok = float(width_avg) > 0 and float(width) > float(width_avg) * 0.50

        reversal_long = sweep_long and displacement_long and (bullish_fvg or vol_ok)
        reversal_short = sweep_short and displacement_short and (bearish_fvg or vol_ok)
        continuation_long = bos_long and retrace_long and pullback_long and mh >= 0
        continuation_short = bos_short and retrace_short and pullback_short and mh <= 0

        # Disable L1 and L3; execute only A+ L2 setups.
        long_entry_l1 = False
        short_entry_l1 = False
        long_entry_l2 = (
            bias == "long"
            and trend_ok
            and not_chasing_long
            and close_f > em
            and r > 46.0
            and width_ok
            and vol_ok
            and (long_location_ok or sweep_long or bos_long)
            and (reversal_long or continuation_long)
        )
        short_entry_l2 = (
            bias == "short"
            and trend_ok
            and not_chasing_short
            and close_f < em
            and r < 54.0
            and width_ok
            and vol_ok
            and (short_location_ok or sweep_short or bos_short)
            and (reversal_short or continuation_short)
        )
        long_entry_l3 = False
        short_entry_l3 = False

        stop_atr_mult = max(float(p.atr_stop_mult), 2.6)
        stop_span = max(a * stop_atr_mult, close_f * 0.010)
        long_stop = min(float(exl), float(pb_low), prev_liq_low, em - (a * 2.4), close_f - stop_span)
        short_stop = max(float(exh), float(pb_high), prev_liq_high, em + (a * 2.4), close_f + stop_span)
    elif variant == _VARIANT_BTCETH_SMC_A2:
        # SMC A2: tradable version of A+, still quality-biased.
        min_trend_sep = 0.0008
        trend_ok = trend_sep >= min_trend_sep
        fresh_break_long = close_f > float(prev_hhv if prev_hhv is not None else hhv)
        fresh_break_short = close_f < float(prev_llv if prev_llv is not None else llv)

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p2_hi = float(prev2_high) if prev2_high is not None else hi
        p2_lo = float(prev2_low) if prev2_low is not None else lo
        prev_liq_low = float(prev_llv if prev_llv is not None else llv)
        prev_liq_high = float(prev_hhv if prev_hhv is not None else hhv)
        bar_range = max(1e-9, hi - lo)

        reclaim_long = close_f >= (lo + bar_range * 0.50)
        reclaim_short = close_f <= (hi - bar_range * 0.50)
        sweep_long = (lo < prev_liq_low) and reclaim_long
        sweep_short = (hi > prev_liq_high) and reclaim_short

        bullish_fvg = lo > p2_hi
        bearish_fvg = hi < p2_lo
        bos_long = close_f > prev_liq_high
        bos_short = close_f < prev_liq_low
        retrace_long = (lo <= em * (1.0 + 0.0015)) and (close_f >= em)
        retrace_short = (hi >= em * (1.0 - 0.0015)) and (close_f <= em)
        displacement_long = (close_f > em) and (mh >= 0) and ((close_f - em) >= a * 0.08)
        displacement_short = (close_f < em) and (mh <= 0) and ((em - close_f) >= a * 0.08)

        vol = max(0.0, float(volume or 0.0))
        vol_avg_v = max(0.0, float(volume_avg or 0.0))
        vol_ok = vol_avg_v > 0 and vol >= vol_avg_v * 0.95
        width_ok = float(width_avg) > 0 and float(width) > float(width_avg) * 0.50

        reversal_long = sweep_long and displacement_long and (bullish_fvg or vol_ok)
        reversal_short = sweep_short and displacement_short and (bearish_fvg or vol_ok)
        continuation_long = bos_long and retrace_long and pullback_long and mh >= 0
        continuation_short = bos_short and retrace_short and pullback_short and mh <= 0

        long_entry_l1 = (
            bias == "long"
            and trend_ok
            and not_chasing_long
            and close_f > em
            and r > 50.0
            and width_ok
            and vol_ok
            and (long_location_ok or sweep_long)
            and (reversal_long or continuation_long)
        )
        short_entry_l1 = (
            bias == "short"
            and trend_ok
            and not_chasing_short
            and close_f < em
            and r < 50.0
            and width_ok
            and vol_ok
            and (short_location_ok or sweep_short)
            and (reversal_short or continuation_short)
        )
        long_entry_l2 = (
            bias == "long"
            and trend_ok
            and not_chasing_long
            and close_f > em
            and r > 46.0
            and width_ok
            and vol_ok
            and (long_location_ok or sweep_long or fresh_break_long)
            and (
                reversal_long
                or continuation_long
                or (displacement_long and (long_location_ok or fresh_break_long))
            )
        )
        short_entry_l2 = (
            bias == "short"
            and trend_ok
            and not_chasing_short
            and close_f < em
            and r < 54.0
            and width_ok
            and vol_ok
            and (short_location_ok or sweep_short or fresh_break_short)
            and (
                reversal_short
                or continuation_short
                or (displacement_short and (short_location_ok or fresh_break_short))
            )
        )
        # L3: relaxed SMC filter for higher frequency, still keep trend/width guard.
        long_entry_l3 = (
            bias == "long"
            and trend_ok
            and close_f > em
            and width_ok
            and r > 44.0
            and (long_location_ok or sweep_long or fresh_break_long)
            and (
                continuation_long
                or (displacement_long and (long_location_ok or fresh_break_long))
                or (sweep_long and (bullish_fvg or vol_ok))
            )
        )
        short_entry_l3 = (
            bias == "short"
            and trend_ok
            and close_f < em
            and width_ok
            and r < 56.0
            and (short_location_ok or sweep_short or fresh_break_short)
            and (
                continuation_short
                or (displacement_short and (short_location_ok or fresh_break_short))
                or (sweep_short and (bearish_fvg or vol_ok))
            )
        )

        stop_atr_mult = max(float(p.atr_stop_mult), 2.4)
        stop_span = max(a * stop_atr_mult, close_f * 0.009)
        long_stop = min(float(exl), float(pb_low), prev_liq_low, em - (a * 2.2), close_f - stop_span)
        short_stop = max(float(exh), float(pb_high), prev_liq_high, em + (a * 2.2), close_f + stop_span)
    elif variant == _VARIANT_ELDER_TSS_V1:
        # Elder Triple Screen v1:
        # 1) HTF trend filter (bias + EMA structure)
        # 2) Pullback screen on trigger timeframe (RSI/pullback/location)
        # 3) Directional trigger (fresh break or momentum re-acceleration)
        trend_up = bias == "long" and float(h_close) > float(h_ema_fast) > float(h_ema_slow)
        trend_down = bias == "short" and float(h_close) < float(h_ema_fast) < float(h_ema_slow)
        trend_ok = trend_sep >= 0.0005

        prev_mh = float(prev_macd_hist) if prev_macd_hist is not None else mh
        mh_rising = mh >= prev_mh
        mh_falling = mh <= prev_mh
        width_ok = float(width_avg) > 0 and float(width) > float(width_avg) * 0.45

        # Screen-2 pullback candidates.
        pullback2_long = (
            (pullback_long or long_location_ok or close_f <= em * (1.0 + 0.0015))
            and r <= 49.0
        )
        pullback2_short = (
            (pullback_short or short_location_ok or close_f >= em * (1.0 - 0.0015))
            and r >= 51.0
        )

        # Screen-3 trigger: breakout continuation or momentum re-acceleration.
        trigger_long = (
            fresh_break_long
            or (close_f > em and mh_rising and r > 47.0 and not_chasing_long)
        )
        trigger_short = (
            fresh_break_short
            or (close_f < em and mh_falling and r < 53.0 and not_chasing_short)
        )

        long_entry_l1 = (
            trend_up
            and trend_ok
            and width_ok
            and pullback2_long
            and fresh_break_long
            and mh_rising
            and not_chasing_long
            and 43.0 <= r <= 56.0
        )
        short_entry_l1 = (
            trend_down
            and trend_ok
            and width_ok
            and pullback2_short
            and fresh_break_short
            and mh_falling
            and not_chasing_short
            and 44.0 <= r <= 57.0
        )

        long_entry_l2 = (
            trend_up
            and trend_ok
            and width_ok
            and pullback2_long
            and trigger_long
            and mh_rising
            and r >= 45.0
        )
        short_entry_l2 = (
            trend_down
            and trend_ok
            and width_ok
            and pullback2_short
            and trigger_short
            and mh_falling
            and r <= 55.0
        )

        long_entry_l3 = (
            trend_up
            and close_f > em
            and (pullback_long or long_location_ok)
            and mh_rising
            and r >= 44.0
        )
        short_entry_l3 = (
            trend_down
            and close_f < em
            and (pullback_short or short_location_ok)
            and mh_falling
            and r <= 56.0
        )

        stop_atr_mult = max(float(p.atr_stop_mult), 2.0)
        stop_span = max(a * stop_atr_mult, close_f * 0.006)
        long_stop = min(float(exl), float(pb_low), em - (a * 1.8), close_f - stop_span)
        short_stop = max(float(exh), float(pb_high), em + (a * 1.8), close_f + stop_span)
    elif variant == _VARIANT_R_BREAKER_V1:
        # R-Breaker (UTC day cut): previous day H/L/C -> 6 levels.
        # Entry mix:
        # - Breakout: close crosses Bbreak / Sbreak.
        # - Reversal: day sweep of setup level, then close re-enters via enter level.
        pdh = float(prev_day_high) if prev_day_high is not None else 0.0
        pdl = float(prev_day_low) if prev_day_low is not None else 0.0
        pdc = float(prev_day_close) if prev_day_close is not None else 0.0
        pd_range = max(0.0, pdh - pdl)
        ready = pdh > 0.0 and pdl > 0.0 and pd_range > max(1e-9, close_f * 0.0012)

        # Running intraday extremes for sweep detection.
        day_hi = float(day_high_so_far) if day_high_so_far is not None else close_f
        day_lo = float(day_low_so_far) if day_low_so_far is not None else close_f

        if ready:
            pivot = (pdh + pdl + pdc) / 3.0
            b_break = pdh + 2.0 * (pivot - pdl)
            s_break = pdl - 2.0 * (pdh - pivot)
            s_setup = pivot + (pdh - pdl)
            b_setup = pivot - (pdh - pdl)
            s_enter = 2.0 * pivot - pdl
            b_enter = 2.0 * pivot - pdh
        else:
            pivot = close_f
            b_break = close_f * 1.02
            s_break = close_f * 0.98
            s_setup = close_f * 1.01
            b_setup = close_f * 0.99
            s_enter = close_f * 1.005
            b_enter = close_f * 0.995

        trend_up = float(h_close) > float(h_ema_fast)
        trend_down = float(h_close) < float(h_ema_fast)
        width_ok = float(width_avg) > 0 and float(width) > float(width_avg) * 0.80
        vol_ok = float(width_avg) > 0 and float(width) > float(width_avg) * 0.70
        mh_up = mh >= 0
        mh_dn = mh <= 0

        breakout_long = ready and close_f >= b_break and close_f > em and mh_up
        breakout_short = ready and close_f <= s_break and close_f < em and mh_dn
        reversal_long = ready and day_lo <= b_setup and close_f >= b_enter and close_f > em and mh_up
        reversal_short = ready and day_hi >= s_setup and close_f <= s_enter and close_f < em and mh_dn

        long_bias_ok = bias in {"long", "neutral"}
        short_bias_ok = bias in {"short", "neutral"}

        long_entry_l1 = (
            long_bias_ok
            and trend_up
            and width_ok
            and not_chasing_long
            and r >= 50.0
            and breakout_long
        )
        short_entry_l1 = (
            short_bias_ok
            and trend_down
            and width_ok
            and not_chasing_short
            and r <= 50.0
            and breakout_short
        )
        long_entry_l2 = (
            long_bias_ok
            and vol_ok
            and not_chasing_long
            and r >= 45.0
            and (breakout_long or reversal_long)
        )
        short_entry_l2 = (
            short_bias_ok
            and vol_ok
            and not_chasing_short
            and r <= 55.0
            and (breakout_short or reversal_short)
        )
        long_entry_l3 = (
            long_bias_ok
            and close_f > em
            and r >= 42.0
            and (reversal_long or breakout_long)
        )
        short_entry_l3 = (
            short_bias_ok
            and close_f < em
            and r <= 58.0
            and (reversal_short or breakout_short)
        )

        stop_span = max(a * max(float(p.atr_stop_mult), 1.8), close_f * 0.006)
        long_stop = min(float(exl), float(pb_low), b_setup, close_f - stop_span)
        short_stop = max(float(exh), float(pb_high), s_setup, close_f + stop_span)
    elif variant == _VARIANT_BTCETH_BREAKOUT_V1:
        trend_up = float(h_close) > float(h_ema_fast) > float(h_ema_slow)
        trend_down = float(h_close) < float(h_ema_fast) < float(h_ema_slow)
        prev_liq_high = float(prev_hhv if prev_hhv is not None else hhv)
        prev_liq_low = float(prev_llv if prev_llv is not None else llv)

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        curr_o = float(current_open) if current_open is not None else close_f
        bar_range = max(1e-9, hi - lo)
        body = abs(close_f - curr_o)
        bull_body = close_f > curr_o and body >= bar_range * 0.35
        bear_body = close_f < curr_o and body >= bar_range * 0.35

        vol = max(0.0, float(volume or 0.0))
        vol_avg_v = max(0.0, float(volume_avg or 0.0))
        vol_spike = vol_avg_v > 0 and vol >= vol_avg_v * 1.30
        vol_ok = vol_avg_v > 0 and vol >= vol_avg_v * 1.00

        squeeze = float(width_avg) > 0 and float(width) <= float(width_avg) * 0.85
        expand_ok = float(width_avg) > 0 and float(width) >= float(width_avg) * 0.90
        breakout_buffer = 0.0008
        retest_tol = 0.0015

        breakout_long = close_f > prev_liq_high * (1.0 + breakout_buffer)
        breakout_short = close_f < prev_liq_low * (1.0 - breakout_buffer)
        retest_long = (lo <= prev_liq_high * (1.0 + retest_tol)) and (
            close_f >= prev_liq_high * (1.0 + breakout_buffer * 0.3)
        )
        retest_short = (hi >= prev_liq_low * (1.0 - retest_tol)) and (
            close_f <= prev_liq_low * (1.0 - breakout_buffer * 0.3)
        )

        long_entry_l1 = (
            trend_up
            and squeeze
            and breakout_long
            and vol_spike
            and bull_body
            and close_f > em
            and mh >= 0
            and r > 50.0
            and not_chasing_long
        )
        short_entry_l1 = (
            trend_down
            and squeeze
            and breakout_short
            and vol_spike
            and bear_body
            and close_f < em
            and mh <= 0
            and r < 50.0
            and not_chasing_short
        )
        long_entry_l2 = (
            trend_up
            and (breakout_long or retest_long or fresh_break_long)
            and (squeeze or expand_ok)
            and vol_ok
            and close_f > em
            and mh >= 0
            and r > 47.0
            and not_chasing_long
        )
        short_entry_l2 = (
            trend_down
            and (breakout_short or retest_short or fresh_break_short)
            and (squeeze or expand_ok)
            and vol_ok
            and close_f < em
            and mh <= 0
            and r < 53.0
            and not_chasing_short
        )
        long_entry_l3 = False
        short_entry_l3 = False

        stop_span = max(a * max(float(p.atr_stop_mult), 2.4), close_f * 0.010)
        long_stop = min(float(exl), float(pb_low), prev_liq_high * (1.0 - 0.003), close_f - stop_span)
        short_stop = max(float(exh), float(pb_high), prev_liq_low * (1.0 + 0.003), close_f + stop_span)
    elif variant == _VARIANT_BTCETH_CANDLE_MACD_V1:
        trend_up = float(h_close) > float(h_ema_fast) > float(h_ema_slow)
        trend_down = float(h_close) < float(h_ema_fast) < float(h_ema_slow)
        trend_sideways = trend_sep <= 0.0085

        up = float(upper_band) if upper_band is not None else close_f
        lo_band = float(lower_band) if lower_band is not None else close_f
        _mid = float(mid_band) if mid_band is not None else close_f

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p_hi = float(prev_high) if prev_high is not None else hi
        p_lo = float(prev_low) if prev_low is not None else lo

        curr_o = float(current_open) if current_open is not None else close_f
        prev_o = float(prev_open) if prev_open is not None else curr_o
        prev_c = float(prev_close) if prev_close is not None else close_f
        prev_mh = float(prev_macd_hist) if prev_macd_hist is not None else mh

        body = max(1e-9, abs(close_f - curr_o))
        lower_wick = max(0.0, min(close_f, curr_o) - lo)
        upper_wick = max(0.0, hi - max(close_f, curr_o))
        hammer = (lower_wick >= body * 1.8) and (upper_wick <= body * 0.9) and (close_f >= curr_o)
        shooting_star = (upper_wick >= body * 1.8) and (lower_wick <= body * 0.9) and (close_f <= curr_o)
        bullish_engulf = (prev_c < prev_o) and (close_f > curr_o) and (close_f >= prev_o) and (curr_o <= prev_c)
        bearish_engulf = (prev_c > prev_o) and (close_f < curr_o) and (close_f <= prev_o) and (curr_o >= prev_c)
        bull_pattern = hammer or bullish_engulf
        bear_pattern = shooting_star or bearish_engulf

        squeeze = (float(width_avg) > 0) and (float(width) <= float(width_avg) * 1.10)
        touch_lower = lo <= lo_band * 1.003
        touch_upper = hi >= up * 0.997
        macd_gc = prev_mh <= 0 and mh > 0
        macd_dc = prev_mh >= 0 and mh < 0
        macd_rising = mh > prev_mh
        macd_falling = mh < prev_mh

        vol = max(0.0, float(volume or 0.0))
        vol_avg_v = max(0.0, float(volume_avg or 0.0))
        vol_spike_15x = vol_avg_v > 0 and vol >= vol_avg_v * 1.5
        vol_spike_12x = vol_avg_v > 0 and vol >= vol_avg_v * 1.2
        vol_ok = vol_spike_12x

        long_entry_l1 = (
            trend_up
            and (trend_sideways or squeeze)
            and touch_lower
            and bull_pattern
            and macd_gc
            and r <= 40.0
            and vol_spike_15x
        )
        short_entry_l1 = (
            trend_down
            and (trend_sideways or squeeze)
            and touch_upper
            and bear_pattern
            and macd_dc
            and r >= 60.0
            and vol_spike_15x
        )

        long_entry_l2 = (
            trend_up
            and touch_lower
            and bull_pattern
            and (macd_gc or (macd_rising and mh <= 0))
            and r <= 48.0
            and vol_ok
        )
        short_entry_l2 = (
            trend_down
            and touch_upper
            and bear_pattern
            and (macd_dc or (macd_falling and mh >= 0))
            and r >= 52.0
            and vol_ok
        )
        long_entry_l3 = False
        short_entry_l3 = False

        pat_low = min(lo, p_lo)
        pat_high = max(hi, p_hi)
        cm_stop_span = max(a * 1.8, close_f * 0.010)
        long_stop = min(lo_band * 0.99, pat_low, float(pb_low), _mid - a * 1.4, close_f - cm_stop_span)
        short_stop = max(up * 1.01, pat_high, float(pb_high), _mid + a * 1.4, close_f + cm_stop_span)
    elif variant == _VARIANT_BTCETH_COMBO_V1:
        trend_up = float(h_close) > float(h_ema_fast) > float(h_ema_slow)
        trend_down = float(h_close) < float(h_ema_fast) < float(h_ema_slow)

        up = float(upper_band) if upper_band is not None else close_f
        lo_band = float(lower_band) if lower_band is not None else close_f
        _mid = float(mid_band) if mid_band is not None else close_f

        hi = float(current_high) if current_high is not None else close_f
        lo = float(current_low) if current_low is not None else close_f
        p_hi = float(prev_high) if prev_high is not None else hi
        p_lo = float(prev_low) if prev_low is not None else lo

        curr_o = float(current_open) if current_open is not None else close_f
        prev_o = float(prev_open) if prev_open is not None else curr_o
        prev_c = float(prev_close) if prev_close is not None else close_f
        prev_mh = float(prev_macd_hist) if prev_macd_hist is not None else mh

        body = max(1e-9, abs(close_f - curr_o))
        lower_wick = max(0.0, min(close_f, curr_o) - lo)
        upper_wick = max(0.0, hi - max(close_f, curr_o))
        hammer = (lower_wick >= body * 1.8) and (upper_wick <= body * 0.9) and (close_f >= curr_o)
        shooting_star = (upper_wick >= body * 1.8) and (lower_wick <= body * 0.9) and (close_f <= curr_o)
        bullish_engulf = (prev_c < prev_o) and (close_f > curr_o) and (close_f >= prev_o) and (curr_o <= prev_c)
        bearish_engulf = (prev_c > prev_o) and (close_f < curr_o) and (close_f <= prev_o) and (curr_o >= prev_c)
        bull_pattern = hammer or bullish_engulf
        bear_pattern = shooting_star or bearish_engulf

        squeeze = (float(width_avg) > 0) and (float(width) <= float(width_avg) * 1.00)
        touch_lower = lo <= lo_band * 1.002
        touch_upper = hi >= up * 0.998
        macd_gc = prev_mh <= 0 and mh > 0
        macd_dc = prev_mh >= 0 and mh < 0
        rsi_os = r < 35.0
        rsi_ob = r > 65.0

        vol = max(0.0, float(volume or 0.0))
        vol_avg_v = max(0.0, float(volume_avg or 0.0))
        vol_spike_2x = vol_avg_v > 0 and vol >= vol_avg_v * 2.0
        vol_spike_15x = vol_avg_v > 0 and vol >= vol_avg_v * 1.5
        vol_spike_12x = vol_avg_v > 0 and vol >= vol_avg_v * 1.2
        vol_spike_10x = vol_avg_v > 0 and vol >= vol_avg_v * 1.0
        vol_ok = vol_spike_12x

        # Combo profile in BTC/ETH: L1 tended to be noisy in recent tests.
        # Keep only L2-quality triggers for better execution robustness.
        long_entry_l1 = False
        short_entry_l1 = False

        long_entry_l2 = (
            trend_up
            and touch_lower
            and bull_pattern
            and (macd_gc or (r < 38.0))
            and vol_spike_12x
        )
        short_entry_l2 = (
            trend_down
            and touch_upper
            and bear_pattern
            and (macd_dc or (r > 62.0))
            and vol_spike_12x
        )
        long_entry_l3 = False
        short_entry_l3 = False

        pat_low = min(lo, p_lo)
        pat_high = max(hi, p_hi)
        combo_stop_span = max(a * 2.0, close_f * 0.012)
        long_stop = min(lo_band * 0.99, pat_low, float(pb_low), _mid - a * 1.8, close_f - combo_stop_span)
        short_stop = max(up * 1.01, pat_high, float(pb_high), _mid + a * 1.8, close_f + combo_stop_span)
    else:
        long_entry_l1 = (
            bias == "long"
            and long_location_ok
            and close_f > float(hhv)
            and close_f > em
            and vol_ok
            and pullback_long
            and not_chasing_long
            and r > long_rsi_l1
            and mh > 0
        )
        short_entry_l1 = (
            bias == "short"
            and short_location_ok
            and close_f < float(llv)
            and close_f < em
            and vol_ok
            and pullback_short
            and not_chasing_short
            and r < short_rsi_l1
            and mh < 0
        )
        long_entry_l2 = (
            bias == "long"
            and long_location_ok
            and close_f > em
            and pullback_long
            and not_chasing_long
            and r > long_rsi_l2
            and mh >= 0
            and (close_f > float(hhv) or vol_ok)
        )
        short_entry_l2 = (
            bias == "short"
            and short_location_ok
            and close_f < em
            and pullback_short
            and not_chasing_short
            and r < short_rsi_l2
            and mh <= 0
            and (close_f < float(llv) or vol_ok)
        )
        long_entry_l3 = (
            bias == "long"
            and long_location_ok
            and close_f > em
            and pullback_long
            and not_chasing_long
            and r > long_rsi_l3
        )
        short_entry_l3 = (
            bias == "short"
            and short_location_ok
            and close_f < em
            and pullback_short
            and not_chasing_short
            and r < short_rsi_l3
        )

        long_stop = min(float(exl), float(pb_low), em - (a * float(p.atr_stop_mult)))
        short_stop = max(float(exh), float(pb_high), em + (a * float(p.atr_stop_mult)))

    long_level = 0
    short_level = 0
    if long_entry_l1:
        long_level = 1
    elif long_entry_l2:
        long_level = 2
    elif long_entry_l3:
        long_level = 3

    if short_entry_l1:
        short_level = 1
    elif short_entry_l2:
        short_level = 2
    elif short_entry_l3:
        short_level = 3

    min_stop_gap = max(a * 0.25, close_f * 0.0004)
    if long_stop >= close_f - min_stop_gap:
        long_stop = close_f - min_stop_gap
    if short_stop <= close_f + min_stop_gap:
        short_stop = close_f + min_stop_gap

    return {
        "variant": variant,
        "trend_sep": float(trend_sep),
        "vol_ok": bool(vol_ok),
        "fresh_break_long": bool(fresh_break_long),
        "fresh_break_short": bool(fresh_break_short),
        "smc_sweep_long": bool(sweep_long),
        "smc_sweep_short": bool(sweep_short),
        "smc_bullish_fvg": bool(bullish_fvg),
        "smc_bearish_fvg": bool(bearish_fvg),
        "combo_squeeze": bool(squeeze),
        "combo_vol_spike": bool(vol_spike_2x),
        "combo_bull_pattern": bool(bull_pattern),
        "combo_bear_pattern": bool(bear_pattern),
        "combo_macd_gc": bool(macd_gc),
        "combo_macd_dc": bool(macd_dc),
        "combo_touch_lower": bool(touch_lower),
        "combo_touch_upper": bool(touch_upper),
        "breakout_squeeze": bool(squeeze),
        "breakout_long": bool(breakout_long),
        "breakout_short": bool(breakout_short),
        "breakout_retest_long": bool(retest_long),
        "breakout_retest_short": bool(retest_short),
        "rbreaker_ready": bool(ready),
        "rbreaker_breakout_long": bool(breakout_long),
        "rbreaker_breakout_short": bool(breakout_short),
        "rbreaker_reversal_long": bool(reversal_long),
        "rbreaker_reversal_short": bool(reversal_short),
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


def resolve_variant_signal_state_from_inputs(inputs: VariantSignalInputs) -> Dict[str, Any]:
    return resolve_variant_signal_state(
        p=inputs.p,
        bias=inputs.bias,
        close=inputs.close,
        ema_value=inputs.ema_value,
        rsi_value=inputs.rsi_value,
        macd_hist_value=inputs.macd_hist_value,
        atr_value=inputs.atr_value,
        hhv=inputs.hhv,
        llv=inputs.llv,
        exl=inputs.exl,
        exh=inputs.exh,
        pb_low=inputs.pb_low,
        pb_high=inputs.pb_high,
        h_close=inputs.h_close,
        h_ema_fast=inputs.h_ema_fast,
        h_ema_slow=inputs.h_ema_slow,
        width=inputs.width,
        width_avg=inputs.width_avg,
        long_location_ok=inputs.long_location_ok,
        short_location_ok=inputs.short_location_ok,
        pullback_long=inputs.pullback_long,
        pullback_short=inputs.pullback_short,
        not_chasing_long=inputs.not_chasing_long,
        not_chasing_short=inputs.not_chasing_short,
        prev_hhv=inputs.prev_hhv,
        prev_llv=inputs.prev_llv,
        current_high=inputs.current_high,
        current_low=inputs.current_low,
        prev_high=inputs.prev_high,
        prev_low=inputs.prev_low,
        prev2_high=inputs.prev2_high,
        prev2_low=inputs.prev2_low,
        prev3_high=inputs.prev3_high,
        prev3_low=inputs.prev3_low,
        current_open=inputs.current_open,
        prev_open=inputs.prev_open,
        prev_close=inputs.prev_close,
        upper_band=inputs.upper_band,
        lower_band=inputs.lower_band,
        mid_band=inputs.mid_band,
        prev_macd_hist=inputs.prev_macd_hist,
        volume=inputs.volume,
        volume_avg=inputs.volume_avg,
        prev_day_high=inputs.prev_day_high,
        prev_day_low=inputs.prev_day_low,
        prev_day_close=inputs.prev_day_close,
        day_high_so_far=inputs.day_high_so_far,
        day_low_so_far=inputs.day_low_so_far,
    )
