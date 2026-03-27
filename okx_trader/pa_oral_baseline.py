from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

from .indicators import atr
from .models import Candle


PA_ORAL_BASELINE_V1 = "pa_oral_baseline_v1"

_SIGNAL_CLOSE_LONG_MIN = 0.55
_SIGNAL_CLOSE_SHORT_MAX = 0.45
_SIGNAL_BAR_RANGE_MAX_ATR = 3.0
_SIGNAL_BODY_LONG_MIN = 0.35
_SIGNAL_BODY_SHORT_MIN = 0.45
_STOP_DISTANCE_MAX_ATR = 2.5
_FIRST_TARGET_MIN_R_SETUP_A = 0.85
_FIRST_TARGET_MIN_R_SETUP_B = 0.80
_RANGE_SWEEP_MIN_ATR = 0.10
_RANGE_SWEEP_MAX_ATR = 1.25
_ENTRY_SIGNAL_VALID_BARS = 4
_SETUP_B_LONG_ENABLED = True


@dataclass
class _PendingOrder:
    side: str
    setup_type: str
    signal_i: int
    activate_i: int
    expire_i: int
    planned_entry: float
    stop: float
    first_target: float
    second_target: float
    key_low: float
    key_high: float
    protected_swing: float
    planned_risk: float


@dataclass
class _ActiveTrade:
    side: str
    setup_type: str
    entry: float
    stop: float
    dynamic_stop: float
    first_target: float
    second_target: float
    tp1_done: bool = False


def is_pa_oral_baseline_variant(raw: str) -> bool:
    key = str(raw or "").strip().lower().replace("-", "_")
    return key in {
        PA_ORAL_BASELINE_V1,
        "pa_oral_baseline",
        "pa_oral",
        "oral_pa",
        "oral_price_action",
        "price_action_oral_v1",
    }


def _infer_step_ms(candles: List[Candle], fallback_ms: int) -> int:
    if len(candles) >= 2:
        step = int(candles[-1].ts_ms) - int(candles[-2].ts_ms)
        if step > 0:
            return step
    return int(fallback_ms)


def _swing_flags(highs: List[float], lows: List[float]) -> Tuple[List[bool], List[bool]]:
    n = len(highs)
    swing_high = [False] * n
    swing_low = [False] * n
    for i in range(2, max(2, n - 2)):
        hi = float(highs[i])
        lo = float(lows[i])
        if hi > float(highs[i - 1]) and hi > float(highs[i - 2]) and hi >= float(highs[i + 1]) and hi >= float(highs[i + 2]):
            swing_high[i] = True
        if lo < float(lows[i - 1]) and lo < float(lows[i - 2]) and lo <= float(lows[i + 1]) and lo <= float(lows[i + 2]):
            swing_low[i] = True
    return swing_high, swing_low


def _last_before(indices: List[int], bound: int) -> Optional[int]:
    for idx in reversed(indices):
        if int(idx) < int(bound):
            return int(idx)
    return None


def _bar_touches_zone(high_px: float, low_px: float, zone_low: float, zone_high: float) -> bool:
    if zone_low > zone_high:
        return False
    return float(low_px) <= float(zone_high) and float(high_px) >= float(zone_low)


def _bar_bias(bg_state: str) -> str:
    if bg_state == "bullish":
        return "long"
    if bg_state == "bearish":
        return "short"
    return "neutral"


def _build_htf_background_states(htf_candles: List[Candle]) -> List[str]:
    highs = [float(c.high) for c in htf_candles]
    lows = [float(c.low) for c in htf_candles]
    swing_high, swing_low = _swing_flags(highs, lows)
    confirmed_highs: List[int] = []
    confirmed_lows: List[int] = []
    states: List[str] = ["neutral"] * len(htf_candles)
    for i in range(len(htf_candles)):
        s = i - 2
        if s >= 2:
            if swing_high[s]:
                confirmed_highs.append(int(s))
            if swing_low[s]:
                confirmed_lows.append(int(s))
        bullish = (
            len(confirmed_highs) >= 2
            and len(confirmed_lows) >= 2
            and highs[confirmed_highs[-1]] > highs[confirmed_highs[-2]]
            and lows[confirmed_lows[-1]] > lows[confirmed_lows[-2]]
        )
        bearish = (
            len(confirmed_highs) >= 2
            and len(confirmed_lows) >= 2
            and highs[confirmed_highs[-1]] < highs[confirmed_highs[-2]]
            and lows[confirmed_lows[-1]] < lows[confirmed_lows[-2]]
        )
        if bullish:
            states[i] = "bullish"
        elif bearish:
            states[i] = "bearish"
    return states


def _build_ltf_to_htf_map(htf_candles: List[Candle], ltf_candles: List[Candle]) -> List[int]:
    if not ltf_candles:
        return []
    htf_step_ms = _infer_step_ms(htf_candles, 4 * 3_600_000)
    ltf_step_ms = _infer_step_ms(ltf_candles, 3_600_000)
    htf_close_ts = [int(c.ts_ms) + int(htf_step_ms) for c in htf_candles]
    ltf_close_ts = [int(c.ts_ms) + int(ltf_step_ms) for c in ltf_candles]
    out: List[int] = [-1] * len(ltf_candles)
    hidx = -1
    for i, close_ts in enumerate(ltf_close_ts):
        while (hidx + 1) < len(htf_close_ts) and int(htf_close_ts[hidx + 1]) <= int(close_ts):
            hidx += 1
        out[i] = int(hidx)
    return out


def _fallback_atr(close_px: float, highs: List[float], lows: List[float], i: int) -> float:
    start = max(0, int(i) - 19)
    rngs = [max(1e-9, float(highs[j]) - float(lows[j])) for j in range(start, int(i) + 1)]
    avg_rng = (sum(rngs) / len(rngs)) if rngs else max(abs(float(close_px)) * 0.0025, 1e-6)
    return max(avg_rng, abs(float(close_px)) * 0.0015, 1e-6)


def _range_box_end_exclusive(
    highs: List[float],
    lows: List[float],
    end_exclusive: int,
    *,
    lookback: int = 20,
) -> Optional[Tuple[float, float, float]]:
    end = int(end_exclusive)
    size = max(1, int(lookback))
    if end < size:
        return None
    hist_high = max(float(x) for x in highs[end - size : end])
    hist_low = min(float(x) for x in lows[end - size : end])
    if hist_high <= hist_low:
        return None
    return float(hist_high), float(hist_low), float(hist_high - hist_low)


def _range_box(highs: List[float], lows: List[float], i: int) -> Optional[Tuple[float, float, float]]:
    return _range_box_end_exclusive(highs, lows, i)


def _empty_payload(
    *,
    candle: Candle,
    atr_v: float,
    bias: str,
    market_state: str,
    long_stop: float,
    short_stop: float,
) -> Dict[str, Any]:
    return {
        "variant": PA_ORAL_BASELINE_V1,
        "strategy_variant": PA_ORAL_BASELINE_V1,
        "bias": str(bias),
        "market_state": str(market_state),
        "setup_type": "",
        "close": float(candle.close),
        "high": float(candle.high),
        "low": float(candle.low),
        "atr": float(atr_v),
        "vol_ok": True,
        "long_entry": False,
        "short_entry": False,
        "long_entry_l2": False,
        "short_entry_l2": False,
        "long_entry_l3": False,
        "short_entry_l3": False,
        "long_level": 0,
        "short_level": 0,
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
        "long_exit": False,
        "short_exit": False,
        "entry_include_start_bar": False,
        "tp1_close_pct_override": 0.5,
        "tp2_close_rest_override": False,
        "be_trigger_r_mult_override": 999.0,
        "auto_tighten_stop_override": True,
        "trail_after_tp1_override": True,
        "signal_exit_enabled_override": False,
    }


def _signal_body_min_fraction(*, side: str) -> float:
    if str(side).upper() == "SHORT":
        return float(_SIGNAL_BODY_SHORT_MIN)
    return float(_SIGNAL_BODY_LONG_MIN)


def _first_target_min_r(*, setup_type: str) -> float:
    if str(setup_type) == "setup_B":
        return float(_FIRST_TARGET_MIN_R_SETUP_B)
    return float(_FIRST_TARGET_MIN_R_SETUP_A)


def _stop_after_tp1(*, trade: _ActiveTrade) -> float:
    if str(trade.setup_type) == "setup_A":
        return float(trade.dynamic_stop)
        # pass
    if str(trade.side).upper() == "SHORT":
        return min(float(trade.dynamic_stop), float(trade.entry))
    return max(float(trade.dynamic_stop), float(trade.entry))
    # return float(trade.dynamic_stop)


def build_pa_oral_signal_table(
    *,
    htf_candles: List[Candle],
    ltf_candles: List[Candle],
) -> List[Dict[str, Any]]:
    if not ltf_candles:
        return []

    highs = [float(c.high) for c in ltf_candles]
    lows = [float(c.low) for c in ltf_candles]
    opens = [float(c.open) for c in ltf_candles]
    closes = [float(c.close) for c in ltf_candles]
    atr_line = atr(highs, lows, closes, 20)
    swing_high, swing_low = _swing_flags(highs, lows)
    htf_states = _build_htf_background_states(htf_candles)
    htf_map = _build_ltf_to_htf_map(htf_candles, ltf_candles)

    confirmed_highs: List[int] = []
    confirmed_lows: List[int] = []
    bos_events: Deque[int] = deque()
    last_broken_high_idx: Optional[int] = None
    last_broken_low_idx: Optional[int] = None

    active_trend_side: str = ""
    active_protect_idx: Optional[int] = None
    active_broken_idx: Optional[int] = None
    active_impulse_extreme_idx: Optional[int] = None
    active_impulse_extreme_price: Optional[float] = None

    pending: Optional[_PendingOrder] = None
    active_trade: Optional[_ActiveTrade] = None

    out: List[Dict[str, Any]] = []

    for i, candle in enumerate(ltf_candles):
        s = i - 2
        if s >= 2:
            if swing_high[s]:
                confirmed_highs.append(int(s))
            if swing_low[s]:
                confirmed_lows.append(int(s))

        atr_v = float(atr_line[i]) if i < len(atr_line) and atr_line[i] is not None else _fallback_atr(closes[i], highs, lows, i)
        atr_v = max(float(atr_v), 1e-6)

        while bos_events and int(bos_events[0]) < int(i) - 11:
            bos_events.popleft()

        if active_trend_side == "long" and active_protect_idx is not None and closes[i] < lows[active_protect_idx]:
            active_trend_side = ""
            active_protect_idx = None
            active_broken_idx = None
            active_impulse_extreme_idx = None
            active_impulse_extreme_price = None
        elif active_trend_side == "short" and active_protect_idx is not None and closes[i] > highs[active_protect_idx]:
            active_trend_side = ""
            active_protect_idx = None
            active_broken_idx = None
            active_impulse_extreme_idx = None
            active_impulse_extreme_price = None

        last_sh = confirmed_highs[-1] if confirmed_highs else None
        last_sl = confirmed_lows[-1] if confirmed_lows else None
        bos_buf = 0.10 * atr_v

        if last_sh is not None and last_sh != last_broken_high_idx and closes[i] > highs[last_sh] + bos_buf:
            protect_idx = _last_before(confirmed_lows, int(last_sh))
            if protect_idx is not None:
                active_trend_side = "long"
                active_protect_idx = int(protect_idx)
                active_broken_idx = int(last_sh)
                active_impulse_extreme_idx = int(i)
                active_impulse_extreme_price = float(highs[i])
                last_broken_high_idx = int(last_sh)
                bos_events.append(int(i))
        elif last_sl is not None and last_sl != last_broken_low_idx and closes[i] < lows[last_sl] - bos_buf:
            protect_idx = _last_before(confirmed_highs, int(last_sl))
            if protect_idx is not None:
                active_trend_side = "short"
                active_protect_idx = int(protect_idx)
                active_broken_idx = int(last_sl)
                active_impulse_extreme_idx = int(i)
                active_impulse_extreme_price = float(lows[i])
                last_broken_low_idx = int(last_sl)
                bos_events.append(int(i))

        if active_trend_side == "long" and active_impulse_extreme_price is not None and highs[i] >= float(active_impulse_extreme_price):
            active_impulse_extreme_price = float(highs[i])
            active_impulse_extreme_idx = int(i)
        elif active_trend_side == "short" and active_impulse_extreme_price is not None and lows[i] <= float(active_impulse_extreme_price):
            active_impulse_extreme_price = float(lows[i])
            active_impulse_extreme_idx = int(i)

        bg_idx = htf_map[i] if i < len(htf_map) else -1
        bg_state = htf_states[bg_idx] if 0 <= bg_idx < len(htf_states) else "neutral"
        bias = _bar_bias(bg_state)

        range_box = _range_box(highs, lows, i)
        market_state = "unclear"
        range_high = None
        range_low = None
        range_width = None
        range_mid = None
        if bg_state == "bullish" and active_trend_side == "long":
            market_state = "trend_long"
        elif bg_state == "bearish" and active_trend_side == "short":
            market_state = "trend_short"
        elif range_box is not None:
            range_high, range_low, range_width = range_box
            range_mid = (float(range_high) + float(range_low)) / 2.0
            if bg_state == "neutral" and not bos_events and (1.5 * atr_v) <= float(range_width) <= (5.0 * atr_v):
                market_state = "range"

        long_stop_ref = float(active_trade.dynamic_stop) if active_trade is not None and active_trade.side == "LONG" else float(lows[i] - 0.10 * atr_v)
        short_stop_ref = float(active_trade.dynamic_stop) if active_trade is not None and active_trade.side == "SHORT" else float(highs[i] + 0.10 * atr_v)
        payload = _empty_payload(
            candle=candle,
            atr_v=atr_v,
            bias=bias,
            market_state=market_state,
            long_stop=long_stop_ref,
            short_stop=short_stop_ref,
        )

        clear_trade_after_bar = False
        if active_trade is not None:
            payload["setup_type"] = str(active_trade.setup_type)
            if active_trade.side == "LONG":
                payload["long_stop"] = float(active_trade.dynamic_stop)
                if not bool(active_trade.tp1_done):
                    if lows[i] <= float(active_trade.dynamic_stop):
                        clear_trade_after_bar = True
                    elif highs[i] >= float(active_trade.first_target):
                        active_trade.tp1_done = True
                        active_trade.dynamic_stop = _stop_after_tp1(trade=active_trade)
                        payload["long_stop"] = float(active_trade.dynamic_stop)
                        if active_trade.setup_type == "setup_B" and highs[i] >= float(active_trade.second_target):
                            clear_trade_after_bar = True
                else:
                    payload["long_stop"] = float(active_trade.dynamic_stop)
                    if lows[i] <= float(active_trade.dynamic_stop):
                        clear_trade_after_bar = True
                    elif active_trade.setup_type == "setup_B" and highs[i] >= float(active_trade.second_target):
                        clear_trade_after_bar = True
                    elif active_trade.setup_type == "setup_A" and i >= 2 and closes[i] < min(lows[i - 1], lows[i - 2]):
                        payload["long_exit"] = True
                        payload["signal_exit_enabled_override"] = True
                        clear_trade_after_bar = True
            else:
                payload["short_stop"] = float(active_trade.dynamic_stop)
                if not bool(active_trade.tp1_done):
                    if highs[i] >= float(active_trade.dynamic_stop):
                        clear_trade_after_bar = True
                    elif lows[i] <= float(active_trade.first_target):
                        active_trade.tp1_done = True
                        active_trade.dynamic_stop = _stop_after_tp1(trade=active_trade)
                        payload["short_stop"] = float(active_trade.dynamic_stop)
                        if active_trade.setup_type == "setup_B" and lows[i] <= float(active_trade.second_target):
                            clear_trade_after_bar = True
                else:
                    payload["short_stop"] = float(active_trade.dynamic_stop)
                    if highs[i] >= float(active_trade.dynamic_stop):
                        clear_trade_after_bar = True
                    elif active_trade.setup_type == "setup_B" and lows[i] <= float(active_trade.second_target):
                        clear_trade_after_bar = True
                    elif active_trade.setup_type == "setup_A" and i >= 2 and closes[i] > max(highs[i - 1], highs[i - 2]):
                        payload["short_exit"] = True
                        payload["signal_exit_enabled_override"] = True
                        clear_trade_after_bar = True

        if clear_trade_after_bar:
            active_trade = None

        if active_trade is None and pending is not None:
            filled = False
            fill_px = 0.0
            if int(i) > int(pending.expire_i):
                pending = None
            elif int(i) >= int(pending.activate_i):
                if pending.side == "LONG":
                    if opens[i] > float(pending.planned_entry):
                        if (opens[i] - float(pending.planned_entry)) <= (0.25 * float(pending.planned_risk)):
                            fill_px = float(opens[i])
                            filled = True
                        else:
                            pending = None
                    elif highs[i] >= float(pending.planned_entry):
                        fill_px = float(pending.planned_entry)
                        filled = True
                else:
                    if opens[i] < float(pending.planned_entry):
                        if (float(pending.planned_entry) - opens[i]) <= (0.25 * float(pending.planned_risk)):
                            fill_px = float(opens[i])
                            filled = True
                        else:
                            pending = None
                    elif lows[i] <= float(pending.planned_entry):
                        fill_px = float(pending.planned_entry)
                        filled = True

            if filled:
                active_trade = _ActiveTrade(
                    side=str(pending.side),
                    setup_type=str(pending.setup_type),
                    entry=float(fill_px),
                    stop=float(pending.stop),
                    dynamic_stop=float(pending.stop),
                    first_target=float(pending.first_target),
                    second_target=float(pending.second_target),
                    tp1_done=False,
                )
                payload["setup_type"] = str(pending.setup_type)
                payload["key_level_low"] = float(pending.key_low)
                payload["key_level_high"] = float(pending.key_high)
                payload["protected_swing_price"] = float(pending.protected_swing)
                payload["entry_include_start_bar"] = True
                payload["entry_price_override"] = float(fill_px)
                payload["tp1_price_override"] = float(pending.first_target)
                payload["tp2_price_override"] = float(pending.second_target)
                payload["tp1_close_pct_override"] = 0.5
                payload["be_trigger_r_mult_override"] = 999.0
                payload["auto_tighten_stop_override"] = True
                payload["trail_after_tp1_override"] = True
                if pending.setup_type == "setup_A":
                    payload["signal_exit_enabled_override"] = True
                    payload["tp2_close_rest_override"] = False
                else:
                    payload["signal_exit_enabled_override"] = False
                    payload["tp2_close_rest_override"] = True
                if pending.side == "LONG":
                    payload["long_entry"] = True
                    payload["long_level"] = 1
                    payload["long_stop"] = float(pending.stop)
                else:
                    payload["short_entry"] = True
                    payload["short_level"] = 1
                    payload["short_stop"] = float(pending.stop)

                if active_trade.side == "LONG":
                    if lows[i] <= float(active_trade.dynamic_stop):
                        active_trade = None
                    elif highs[i] >= float(active_trade.first_target):
                        active_trade.tp1_done = True
                        active_trade.dynamic_stop = _stop_after_tp1(trade=active_trade)
                        payload["long_stop"] = float(active_trade.dynamic_stop)
                        if active_trade.setup_type == "setup_B" and highs[i] >= float(active_trade.second_target):
                            active_trade = None
                else:
                    if highs[i] >= float(active_trade.dynamic_stop):
                        active_trade = None
                    elif lows[i] <= float(active_trade.first_target):
                        active_trade.tp1_done = True
                        active_trade.dynamic_stop = _stop_after_tp1(trade=active_trade)
                        payload["short_stop"] = float(active_trade.dynamic_stop)
                        if active_trade.setup_type == "setup_B" and lows[i] <= float(active_trade.second_target):
                            active_trade = None
                pending = None

        if active_trade is None and pending is None:
            if market_state == "trend_long" and active_broken_idx is not None and active_protect_idx is not None and active_impulse_extreme_idx is not None and active_impulse_extreme_price is not None and i >= 1:
                broken_px = float(highs[active_broken_idx])
                protect_px = float(lows[active_protect_idx])
                bos_zone_low = broken_px - 0.15 * atr_v
                bos_zone_high = broken_px + 0.15 * atr_v
                impulse_high = float(active_impulse_extreme_price)
                impulse_range = max(1e-9, impulse_high - protect_px)
                value_zone_low = impulse_high - impulse_range * 0.66
                value_zone_high = impulse_high - impulse_range * 0.33
                key_low = float(value_zone_low)
                key_high = float(value_zone_high)
                if key_low <= key_high:
                    touched = _bar_touches_zone(highs[i], lows[i], key_low, key_high)
                    if not touched and i >= 1:
                        touched = _bar_touches_zone(highs[i - 1], lows[i - 1], key_low, key_high)
                    bar_range = max(1e-9, highs[i] - lows[i])
                    body = abs(closes[i] - opens[i])
                    pullback_low = min(float(x) for x in lows[active_impulse_extreme_idx : i + 1])
                    planned_entry = float(highs[i] + 0.05 * atr_v)
                    stop_px = float(min(lows[i], pullback_low) - 0.1 * atr_v)
                    stop_distance = max(1e-8, planned_entry - stop_px)
                    first_target = float(impulse_high)
                    reward = float(first_target - planned_entry)
                    if (
                        touched
                        and closes[i] > opens[i]
                        and closes[i] >= lows[i] + bar_range * _SIGNAL_CLOSE_LONG_MIN
                        and body >= bar_range * _signal_body_min_fraction(side="LONG")
                        and bar_range <= _SIGNAL_BAR_RANGE_MAX_ATR * atr_v
                        and stop_distance <= _STOP_DISTANCE_MAX_ATR * atr_v
                        and reward > 0
                        and (reward / stop_distance) >= _first_target_min_r(setup_type="setup_A")
                    ):
                        pending = _PendingOrder(
                            side="LONG",
                            setup_type="setup_A",
                            signal_i=int(i),
                            activate_i=int(i + 1),
                            expire_i=int(i + _ENTRY_SIGNAL_VALID_BARS),
                            planned_entry=float(planned_entry),
                            stop=float(stop_px),
                            first_target=float(first_target),
                            second_target=float(first_target),
                            key_low=float(key_low),
                            key_high=float(key_high),
                            protected_swing=float(protect_px),
                            planned_risk=float(stop_distance),
                        )
            elif market_state == "trend_short" and active_broken_idx is not None and active_protect_idx is not None and active_impulse_extreme_idx is not None and active_impulse_extreme_price is not None and i >= 1:
                broken_px = float(lows[active_broken_idx])
                protect_px = float(highs[active_protect_idx])
                bos_zone_low = broken_px - 0.15 * atr_v
                bos_zone_high = broken_px + 0.15 * atr_v
                impulse_low = float(active_impulse_extreme_price)
                impulse_range = max(1e-9, protect_px - impulse_low)
                value_zone_low = impulse_low + impulse_range * 0.33
                value_zone_high = impulse_low + impulse_range * 0.66
                key_low = float(value_zone_low)
                key_high = float(value_zone_high)
                if key_low <= key_high:
                    touched = _bar_touches_zone(highs[i], lows[i], key_low, key_high)
                    if not touched and i >= 1:
                        touched = _bar_touches_zone(highs[i - 1], lows[i - 1], key_low, key_high)
                    bar_range = max(1e-9, highs[i] - lows[i])
                    body = abs(closes[i] - opens[i])
                    pullback_high = max(float(x) for x in highs[active_impulse_extreme_idx : i + 1])
                    planned_entry = float(lows[i] - 0.05 * atr_v)
                    stop_px = float(max(highs[i], pullback_high) + 0.10 * atr_v)
                    stop_distance = max(1e-8, stop_px - planned_entry)
                    first_target = float(impulse_low)
                    reward = float(planned_entry - first_target)
                    if (
                        touched
                        and closes[i] < opens[i]
                        and closes[i] <= lows[i] + bar_range * _SIGNAL_CLOSE_SHORT_MAX
                        and body >= bar_range * _signal_body_min_fraction(side="SHORT")
                        and bar_range <= _SIGNAL_BAR_RANGE_MAX_ATR * atr_v
                        and stop_distance <= _STOP_DISTANCE_MAX_ATR * atr_v
                        and reward > 0
                        and (reward / stop_distance) >= _first_target_min_r(setup_type="setup_A")
                    ):
                        pending = _PendingOrder(
                            side="SHORT",
                            setup_type="setup_A",
                            signal_i=int(i),
                            activate_i=int(i + 1),
                            expire_i=int(i + _ENTRY_SIGNAL_VALID_BARS),
                            planned_entry=float(planned_entry),
                            stop=float(stop_px),
                            first_target=float(first_target),
                            second_target=float(first_target),
                            key_low=float(key_low),
                            key_high=float(key_high),
                            protected_swing=float(protect_px),
                            planned_risk=float(stop_distance),
                        )
            elif market_state == "range" and range_high is not None and range_low is not None and range_mid is not None and i >= 1:
                bar_range = max(1e-9, highs[i] - lows[i])
                body = abs(closes[i] - opens[i])

                current_range_high = float(range_high)
                current_range_low = float(range_low)
                current_range_mid = float(range_mid)
                prev_range_box = _range_box_end_exclusive(highs, lows, i - 1) if i >= 21 else None

                long_range_high = None
                long_range_low = None
                long_range_mid = None
                sweep_low = None
                if lows[i] < current_range_low:
                    long_range_high = current_range_high
                    long_range_low = current_range_low
                    long_range_mid = current_range_mid
                    sweep_low = float(lows[i])
                elif prev_range_box is not None and lows[i - 1] < float(prev_range_box[1]):
                    long_range_high = float(prev_range_box[0])
                    long_range_low = float(prev_range_box[1])
                    long_range_mid = (float(prev_range_box[0]) + float(prev_range_box[1])) / 2.0
                    sweep_low = float(lows[i - 1])
                if sweep_low is not None and long_range_low is not None and long_range_mid is not None and long_range_high is not None:
                    sweep_mag = float(long_range_low) - float(sweep_low)
                    planned_entry = float(highs[i] + 0.05 * atr_v)
                    stop_px = float(sweep_low - 0.10 * atr_v)
                    stop_distance = max(1e-8, planned_entry - stop_px)
                    reward = float(long_range_high - planned_entry)
                    if (
                        _SETUP_B_LONG_ENABLED
                        and
                        (_RANGE_SWEEP_MIN_ATR * atr_v) <= sweep_mag <= (_RANGE_SWEEP_MAX_ATR * atr_v)
                        and closes[i] > float(long_range_low)
                        and closes[i] > opens[i]
                        and closes[i] >= lows[i] + bar_range * _SIGNAL_CLOSE_LONG_MIN
                        and body >= bar_range * _signal_body_min_fraction(side="LONG")
                        and bar_range <= _SIGNAL_BAR_RANGE_MAX_ATR * atr_v
                        and stop_distance <= _STOP_DISTANCE_MAX_ATR * atr_v
                        and reward > 0
                        and (reward / stop_distance) >= _first_target_min_r(setup_type="setup_B")
                    ):
                        pending = _PendingOrder(
                            side="LONG",
                            setup_type="setup_B",
                            signal_i=int(i),
                            activate_i=int(i + 1),
                            expire_i=int(i + _ENTRY_SIGNAL_VALID_BARS),
                            planned_entry=float(planned_entry),
                            stop=float(stop_px),
                            first_target=float(long_range_high),
                            second_target=float(long_range_high),
                            key_low=float(long_range_low),
                            key_high=float(long_range_low),
                            protected_swing=float(sweep_low),
                            planned_risk=float(stop_distance),
                        )

                if pending is None:
                    short_range_high = None
                    short_range_low = None
                    short_range_mid = None
                    sweep_high = None
                    if highs[i] > current_range_high:
                        short_range_high = current_range_high
                        short_range_low = current_range_low
                        short_range_mid = current_range_mid
                        sweep_high = float(highs[i])
                    elif prev_range_box is not None and highs[i - 1] > float(prev_range_box[0]):
                        short_range_high = float(prev_range_box[0])
                        short_range_low = float(prev_range_box[1])
                        short_range_mid = (float(prev_range_box[0]) + float(prev_range_box[1])) / 2.0
                        sweep_high = float(highs[i - 1])
                    if sweep_high is not None and short_range_high is not None and short_range_mid is not None and short_range_low is not None:
                        sweep_mag = float(sweep_high) - float(short_range_high)
                        planned_entry = float(lows[i] - 0.05 * atr_v)
                        stop_px = float(sweep_high + 0.10 * atr_v)
                        stop_distance = max(1e-8, stop_px - planned_entry)
                        reward = float(planned_entry - short_range_low)
                        if (
                            (_RANGE_SWEEP_MIN_ATR * atr_v) <= sweep_mag <= (_RANGE_SWEEP_MAX_ATR * atr_v)
                            and closes[i] < float(short_range_high)
                            and closes[i] < opens[i]
                            and closes[i] <= lows[i] + bar_range * _SIGNAL_CLOSE_SHORT_MAX
                            and body >= bar_range * _signal_body_min_fraction(side="SHORT")
                            and bar_range <= _SIGNAL_BAR_RANGE_MAX_ATR * atr_v
                            and stop_distance <= _STOP_DISTANCE_MAX_ATR * atr_v
                            and reward > 0
                            and (reward / stop_distance) >= _first_target_min_r(setup_type="setup_B")
                        ):
                            pending = _PendingOrder(
                                side="SHORT",
                                setup_type="setup_B",
                                signal_i=int(i),
                                activate_i=int(i + 1),
                                expire_i=int(i + _ENTRY_SIGNAL_VALID_BARS),
                                planned_entry=float(planned_entry),
                                stop=float(stop_px),
                                first_target=float(short_range_low),
                                second_target=float(short_range_low),
                                key_low=float(short_range_high),
                                key_high=float(short_range_high),
                                protected_swing=float(sweep_high),
                                planned_risk=float(stop_distance),
                            )

        out.append(payload)

    return out


def build_pa_oral_signal_snapshot(
    *,
    htf_candles: List[Candle],
    ltf_candles: List[Candle],
) -> Dict[str, Any]:
    table = build_pa_oral_signal_table(htf_candles=htf_candles, ltf_candles=ltf_candles)
    if not table:
        raise RuntimeError("No LTF candles available for pa_oral_baseline_v1")
    return dict(table[-1])
