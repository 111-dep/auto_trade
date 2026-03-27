from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .models import StrategyParams


@dataclass(frozen=True, slots=True)
class VariantSignalInputs:
    p: StrategyParams
    bias: str
    close: float
    ema_value: float
    rsi_value: float
    macd_hist_value: float
    atr_value: float
    hhv: float
    llv: float
    exl: float
    exh: float
    pb_low: float
    pb_high: float
    h_close: float
    h_ema_fast: float
    h_ema_slow: float
    width: float
    width_avg: float
    long_location_ok: bool
    short_location_ok: bool
    pullback_long: bool
    pullback_short: bool
    not_chasing_long: bool
    not_chasing_short: bool
    prev_hhv: Optional[float] = None
    prev_llv: Optional[float] = None
    prev_exl: Optional[float] = None
    prev_exh: Optional[float] = None
    current_high: Optional[float] = None
    current_low: Optional[float] = None
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    prev2_high: Optional[float] = None
    prev2_low: Optional[float] = None
    prev3_high: Optional[float] = None
    prev3_low: Optional[float] = None
    current_open: Optional[float] = None
    prev_open: Optional[float] = None
    prev_close: Optional[float] = None
    prev2_open: Optional[float] = None
    prev2_close: Optional[float] = None
    prev3_open: Optional[float] = None
    prev3_close: Optional[float] = None
    upper_band: Optional[float] = None
    lower_band: Optional[float] = None
    mid_band: Optional[float] = None
    prev_macd_hist: Optional[float] = None
    volume: Optional[float] = None
    volume_avg: Optional[float] = None
    prev_day_high: Optional[float] = None
    prev_day_low: Optional[float] = None
    prev_day_close: Optional[float] = None
    day_high_so_far: Optional[float] = None
    day_low_so_far: Optional[float] = None
    prev_h_ema_fast: Optional[float] = None
    prev_h_ema_slow: Optional[float] = None
    recent_rsi_min: Optional[float] = None
    recent_rsi_max: Optional[float] = None
    prev_ema_value: Optional[float] = None
    prev2_ema_value: Optional[float] = None
    prev3_ema_value: Optional[float] = None
    prev5_ema_value: Optional[float] = None
    ema_slow_value: Optional[float] = None
    prev_ema_slow_value: Optional[float] = None
    loc_close: Optional[float] = None
    loc_ema_fast: Optional[float] = None
    loc_ema_slow: Optional[float] = None
    prev_loc_ema_fast: Optional[float] = None
    prev_loc_ema_slow: Optional[float] = None
    loc_rsi_value: Optional[float] = None
    loc_atr_value: Optional[float] = None
    loc_current_high: Optional[float] = None
    loc_current_low: Optional[float] = None
    hour_open: Optional[float] = None
    hour_high: Optional[float] = None
    hour_low: Optional[float] = None
    hour_close: Optional[float] = None
    hour_prev_close: Optional[float] = None
    hour_rsi_value: Optional[float] = None

    def to_kwargs(self) -> Dict[str, Any]:
        return {
            "p": self.p,
            "bias": self.bias,
            "close": self.close,
            "ema_value": self.ema_value,
            "rsi_value": self.rsi_value,
            "macd_hist_value": self.macd_hist_value,
            "atr_value": self.atr_value,
            "hhv": self.hhv,
            "llv": self.llv,
            "exl": self.exl,
            "exh": self.exh,
            "pb_low": self.pb_low,
            "pb_high": self.pb_high,
            "h_close": self.h_close,
            "h_ema_fast": self.h_ema_fast,
            "h_ema_slow": self.h_ema_slow,
            "width": self.width,
            "width_avg": self.width_avg,
            "long_location_ok": self.long_location_ok,
            "short_location_ok": self.short_location_ok,
            "pullback_long": self.pullback_long,
            "pullback_short": self.pullback_short,
            "not_chasing_long": self.not_chasing_long,
            "not_chasing_short": self.not_chasing_short,
            "prev_hhv": self.prev_hhv,
            "prev_llv": self.prev_llv,
            "prev_exl": self.prev_exl,
            "prev_exh": self.prev_exh,
            "current_high": self.current_high,
            "current_low": self.current_low,
            "prev_high": self.prev_high,
            "prev_low": self.prev_low,
            "prev2_high": self.prev2_high,
            "prev2_low": self.prev2_low,
            "prev3_high": self.prev3_high,
            "prev3_low": self.prev3_low,
            "current_open": self.current_open,
            "prev_open": self.prev_open,
            "prev_close": self.prev_close,
            "prev2_open": self.prev2_open,
            "prev2_close": self.prev2_close,
            "prev3_open": self.prev3_open,
            "prev3_close": self.prev3_close,
            "upper_band": self.upper_band,
            "lower_band": self.lower_band,
            "mid_band": self.mid_band,
            "prev_macd_hist": self.prev_macd_hist,
            "volume": self.volume,
            "volume_avg": self.volume_avg,
            "prev_day_high": self.prev_day_high,
            "prev_day_low": self.prev_day_low,
            "prev_day_close": self.prev_day_close,
            "day_high_so_far": self.day_high_so_far,
            "day_low_so_far": self.day_low_so_far,
            "prev_h_ema_fast": self.prev_h_ema_fast,
            "prev_h_ema_slow": self.prev_h_ema_slow,
            "recent_rsi_min": self.recent_rsi_min,
            "recent_rsi_max": self.recent_rsi_max,
            "prev_ema_value": self.prev_ema_value,
            "prev2_ema_value": self.prev2_ema_value,
            "prev3_ema_value": self.prev3_ema_value,
            "prev5_ema_value": self.prev5_ema_value,
            "ema_slow_value": self.ema_slow_value,
            "prev_ema_slow_value": self.prev_ema_slow_value,
            "loc_close": self.loc_close,
            "loc_ema_fast": self.loc_ema_fast,
            "loc_ema_slow": self.loc_ema_slow,
            "prev_loc_ema_fast": self.prev_loc_ema_fast,
            "prev_loc_ema_slow": self.prev_loc_ema_slow,
            "loc_rsi_value": self.loc_rsi_value,
            "loc_atr_value": self.loc_atr_value,
            "loc_current_high": self.loc_current_high,
            "loc_current_low": self.loc_current_low,
            "hour_open": self.hour_open,
            "hour_high": self.hour_high,
            "hour_low": self.hour_low,
            "hour_close": self.hour_close,
            "hour_prev_close": self.hour_prev_close,
            "hour_rsi_value": self.hour_rsi_value,
        }
