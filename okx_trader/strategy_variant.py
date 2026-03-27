from __future__ import annotations

from typing import Any, Dict, Optional

from .models import StrategyParams
from .pa_oral_baseline import PA_ORAL_BASELINE_V1, is_pa_oral_baseline_variant
from .strategy_contract import VariantSignalInputs


def normalize_strategy_variant(raw: str) -> str:
    key = str(raw or "").strip()
    if not key:
        return PA_ORAL_BASELINE_V1
    if is_pa_oral_baseline_variant(key):
        return PA_ORAL_BASELINE_V1
    raise ValueError(
        f"Unsupported strategy variant: {raw}. "
        f"Only {PA_ORAL_BASELINE_V1} is available in this project."
    )


def list_variant_resolvers() -> Dict[str, str]:
    return {
        PA_ORAL_BASELINE_V1: "dedicated_pa_oral_engine",
    }


def _dispatch_error(variant: str) -> RuntimeError:
    normalized = normalize_strategy_variant(variant)
    return RuntimeError(
        f"{normalized} uses the dedicated engine in okx_trader.pa_oral_baseline. "
        "Do not route this strategy through strategy_variant.py."
    )


def resolve_variant_signal_state_from_inputs(inputs: VariantSignalInputs) -> Dict[str, Any]:
    raise _dispatch_error(getattr(inputs.p, "strategy_variant", ""))


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
    prev_exl: Optional[float] = None,
    prev_exh: Optional[float] = None,
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
    prev2_open: Optional[float] = None,
    prev2_close: Optional[float] = None,
    prev3_open: Optional[float] = None,
    prev3_close: Optional[float] = None,
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
    prev_h_ema_fast: Optional[float] = None,
    prev_h_ema_slow: Optional[float] = None,
    recent_rsi_min: Optional[float] = None,
    recent_rsi_max: Optional[float] = None,
    prev_ema_value: Optional[float] = None,
    prev2_ema_value: Optional[float] = None,
    prev3_ema_value: Optional[float] = None,
    prev5_ema_value: Optional[float] = None,
    ema_slow_value: Optional[float] = None,
    prev_ema_slow_value: Optional[float] = None,
    loc_close: Optional[float] = None,
    loc_ema_fast: Optional[float] = None,
    loc_ema_slow: Optional[float] = None,
    prev_loc_ema_fast: Optional[float] = None,
    prev_loc_ema_slow: Optional[float] = None,
    loc_rsi_value: Optional[float] = None,
    loc_atr_value: Optional[float] = None,
    loc_current_high: Optional[float] = None,
    loc_current_low: Optional[float] = None,
    hour_open: Optional[float] = None,
    hour_high: Optional[float] = None,
    hour_low: Optional[float] = None,
    hour_close: Optional[float] = None,
    hour_prev_close: Optional[float] = None,
    hour_rsi_value: Optional[float] = None,
) -> Dict[str, Any]:
    _ = (
        bias,
        close,
        ema_value,
        rsi_value,
        macd_hist_value,
        atr_value,
        hhv,
        llv,
        exl,
        exh,
        pb_low,
        pb_high,
        h_close,
        h_ema_fast,
        h_ema_slow,
        width,
        width_avg,
        long_location_ok,
        short_location_ok,
        pullback_long,
        pullback_short,
        not_chasing_long,
        not_chasing_short,
        prev_hhv,
        prev_llv,
        prev_exl,
        prev_exh,
        current_high,
        current_low,
        prev_high,
        prev_low,
        prev2_high,
        prev2_low,
        prev3_high,
        prev3_low,
        current_open,
        prev_open,
        prev_close,
        prev2_open,
        prev2_close,
        prev3_open,
        prev3_close,
        upper_band,
        lower_band,
        mid_band,
        prev_macd_hist,
        volume,
        volume_avg,
        prev_day_high,
        prev_day_low,
        prev_day_close,
        day_high_so_far,
        day_low_so_far,
        prev_h_ema_fast,
        prev_h_ema_slow,
        recent_rsi_min,
        recent_rsi_max,
        prev_ema_value,
        prev2_ema_value,
        prev3_ema_value,
        prev5_ema_value,
        ema_slow_value,
        prev_ema_slow_value,
        loc_close,
        loc_ema_fast,
        loc_ema_slow,
        prev_loc_ema_fast,
        prev_loc_ema_slow,
        loc_rsi_value,
        loc_atr_value,
        loc_current_high,
        loc_current_low,
        hour_open,
        hour_high,
        hour_low,
        hour_close,
        hour_prev_close,
        hour_rsi_value,
    )
    raise _dispatch_error(getattr(p, "strategy_variant", ""))
