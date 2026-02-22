from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .models import StrategyParams


@dataclass(frozen=True)
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
    current_high: Optional[float] = None
    current_low: Optional[float] = None
    prev_high: Optional[float] = None
    prev_low: Optional[float] = None
    prev2_high: Optional[float] = None
    prev2_low: Optional[float] = None
    current_open: Optional[float] = None
    prev_open: Optional[float] = None
    prev_close: Optional[float] = None
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
            "current_high": self.current_high,
            "current_low": self.current_low,
            "prev_high": self.prev_high,
            "prev_low": self.prev_low,
            "prev2_high": self.prev2_high,
            "prev2_low": self.prev2_low,
            "current_open": self.current_open,
            "prev_open": self.prev_open,
            "prev_close": self.prev_close,
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
        }
