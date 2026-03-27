from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .indicators import atr, bollinger, ema, macd, rolling_high, rolling_low, rsi
from .models import Candle, StrategyParams
from .pa_oral_baseline import PA_ORAL_BASELINE_V1, build_pa_oral_signal_snapshot, is_pa_oral_baseline_variant
from .signal_contract import SignalSnapshot
from .strategy_contract import VariantSignalInputs
from .strategy_variant import resolve_variant_signal_state_from_inputs

_VARIANT_SIGNAL_EXTRA_KEYS = (
    "entry_on_next_open",
    "max_stop_pct",
    "tp1_r_override",
    "tp2_r_override",
    "tp1_only_override",
    "tp1_close_pct_override",
    "tp2_close_rest_override",
    "be_trigger_r_mult_override",
    "auto_tighten_stop_override",
    "trail_after_tp1_override",
    "signal_exit_enabled_override",
    "max_hold_bars",
)


def _prev_utc_day_hlc_and_today_range(
    candles: List[Candle],
    idx: int,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if idx <= 0 or idx >= len(candles):
        return None, None, None, None, None
    day_ms = 86_400_000
    curr_day = int(candles[idx].ts_ms // day_ms)
    prev_day = curr_day - 1

    today_hi: Optional[float] = None
    today_lo: Optional[float] = None
    prev_hi: Optional[float] = None
    prev_lo: Optional[float] = None
    prev_close: Optional[float] = None

    for j in range(idx, -1, -1):
        c = candles[j]
        d = int(c.ts_ms // day_ms)
        if d == curr_day:
            today_hi = c.high if today_hi is None else max(today_hi, c.high)
            today_lo = c.low if today_lo is None else min(today_lo, c.low)
            continue
        if d == prev_day:
            if prev_close is None:
                # First bar found while iterating backwards -> previous day's final close.
                prev_close = c.close
            prev_hi = c.high if prev_hi is None else max(prev_hi, c.high)
            prev_lo = c.low if prev_lo is None else min(prev_lo, c.low)
            continue
        if d < prev_day:
            break

    return prev_hi, prev_lo, prev_close, today_hi, today_lo


def _latest_completed_hour_context(
    candles: List[Candle],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if not candles:
        return None, None, None, None, None, None

    groups: Dict[int, List[Candle]] = {}
    ordered: List[int] = []
    for candle in candles:
        hour_key = int(candle.ts_ms // 3_600_000)
        if hour_key not in groups:
            groups[hour_key] = []
            ordered.append(hour_key)
        groups[hour_key].append(candle)

    hourly: List[Candle] = []
    for hour_key in ordered:
        parts = groups.get(hour_key) or []
        if len(parts) != 4:
            continue
        hourly.append(
            Candle(
                ts_ms=int(parts[0].ts_ms),
                open=float(parts[0].open),
                high=max(float(x.high) for x in parts),
                low=min(float(x.low) for x in parts),
                close=float(parts[-1].close),
                confirm=True,
                volume=sum(max(0.0, float(x.volume)) for x in parts),
            )
        )

    if not hourly:
        return None, None, None, None, None, None

    idx = len(hourly) - 1
    closes = [float(c.close) for c in hourly]
    hour_rsi = rsi(closes, 14)
    current = hourly[idx]
    prev_close = float(hourly[idx - 1].close) if idx >= 1 else None
    rsi_value = float(hour_rsi[idx]) if idx < len(hour_rsi) and hour_rsi[idx] is not None else None
    return (
        float(current.open),
        float(current.high),
        float(current.low),
        float(current.close),
        prev_close,
        rsi_value,
    )


def build_signals(
    htf_candles: List[Candle], loc_candles: List[Candle], ltf_candles: List[Candle], p: StrategyParams
) -> Dict[str, Any]:
    if is_pa_oral_baseline_variant(getattr(p, "strategy_variant", "")):
        oral = build_pa_oral_signal_snapshot(htf_candles=htf_candles, ltf_candles=ltf_candles)
        oral_payload = {
            "signal_ts_ms": int(ltf_candles[-1].ts_ms) if ltf_candles else 0,
            "signal_confirm": bool(ltf_candles[-1].confirm) if ltf_candles else True,
            "htf_ts_ms": int(htf_candles[-1].ts_ms) if htf_candles else 0,
            "loc_ts_ms": int(loc_candles[-1].ts_ms) if loc_candles else 0,
        }
        oral_payload.update(dict(oral))
        return SignalSnapshot.from_dict(oral_payload).to_dict()

    raise RuntimeError(
        f"Unsupported strategy variant {getattr(p, 'strategy_variant', '')}. "
        f"Only {PA_ORAL_BASELINE_V1} is available."
    )

    min_htf = max(p.htf_ema_slow_len + 2, p.htf_rsi_len + 2)
    min_loc = max(p.loc_lookback + 2, p.loc_recent_bars + 2, p.loc_sr_lookback + p.loc_recent_bars + 2)
    min_ltf = max(
        p.break_len + 2,
        p.exit_len + 2,
        p.ltf_ema_len + 2,
        p.bb_len + 2,
        p.rsi_len + 2,
        p.macd_slow + p.macd_signal + 5,
        p.pullback_lookback + 2,
        p.atr_len + 2,
    )
    if len(htf_candles) < min_htf:
        raise RuntimeError(f"Not enough HTF candles for strategy (need >= {min_htf})")
    if len(loc_candles) < min_loc:
        raise RuntimeError(f"Not enough LOC candles for strategy (need >= {min_loc})")
    if len(ltf_candles) < min_ltf:
        raise RuntimeError(f"Not enough LTF candles for strategy (need >= {min_ltf})")

    htf_closes = [c.close for c in htf_candles]
    htf_ema_fast = ema(htf_closes, p.htf_ema_fast_len)
    htf_ema_slow = ema(htf_closes, p.htf_ema_slow_len)
    htf_rsi_line = rsi(htf_closes, p.htf_rsi_len)
    hidx = len(htf_candles) - 1

    h_close = htf_closes[hidx]
    h_ema_fast = htf_ema_fast[hidx]
    h_ema_slow = htf_ema_slow[hidx]
    h_rsi = htf_rsi_line[hidx]
    if None in {h_ema_fast, h_ema_slow, h_rsi}:
        raise RuntimeError("HTF indicators are not ready yet")
    prev_h_ema_fast = htf_ema_fast[hidx - 1] if hidx >= 1 else None
    prev_h_ema_slow = htf_ema_slow[hidx - 1] if hidx >= 1 else None

    bias = "neutral"
    if h_close > h_ema_fast > h_ema_slow and h_rsi >= p.htf_rsi_long_min:
        bias = "long"
    elif h_close < h_ema_fast < h_ema_slow and h_rsi <= p.htf_rsi_short_max:
        bias = "short"

    loc_highs = [c.high for c in loc_candles]
    loc_lows = [c.low for c in loc_candles]
    loc_closes = [c.close for c in loc_candles]
    lcid = len(loc_candles) - 1
    loc_ema_fast_line = ema(loc_closes, 20)
    loc_ema_slow_line = ema(loc_closes, 50)
    loc_rsi_line = rsi(loc_closes, 14)
    loc_atr_line = atr(loc_highs, loc_lows, loc_closes, 14)

    loc_start = max(0, len(loc_candles) - p.loc_lookback)
    loc_high = max(loc_highs[loc_start:])
    loc_low = min(loc_lows[loc_start:])
    loc_range = max(loc_high - loc_low, 1e-9)
    fib_low = min(p.location_fib_low, p.location_fib_high)
    fib_high = max(p.location_fib_low, p.location_fib_high)
    long_fib_zone_hi = loc_high - loc_range * fib_low
    long_fib_zone_lo = loc_high - loc_range * fib_high
    short_fib_zone_lo = loc_low + loc_range * fib_low
    short_fib_zone_hi = loc_low + loc_range * fib_high

    recent_bars = max(2, p.loc_recent_bars)
    loc_recent_start = max(0, len(loc_candles) - recent_bars)
    loc_recent_low = min(loc_lows[loc_recent_start:])
    loc_recent_high = max(loc_highs[loc_recent_start:])

    fib_touch_long = long_fib_zone_lo <= loc_recent_low <= long_fib_zone_hi
    fib_touch_short = short_fib_zone_lo <= loc_recent_high <= short_fib_zone_hi

    sr_end = len(loc_candles) - recent_bars
    retest_long = False
    retest_short = False
    sr_ref_high = None
    sr_ref_low = None
    if sr_end > 1:
        sr_start = max(0, sr_end - p.loc_sr_lookback)
        sr_ref_high = max(loc_highs[sr_start:sr_end])
        sr_ref_low = min(loc_lows[sr_start:sr_end])
        if sr_ref_high and sr_ref_high > 0:
            retest_long = abs(loc_recent_low - sr_ref_high) / sr_ref_high <= p.location_retest_tol
        if sr_ref_low and sr_ref_low > 0:
            retest_short = abs(loc_recent_high - sr_ref_low) / sr_ref_low <= p.location_retest_tol

    long_location_ok = fib_touch_long or retest_long
    short_location_ok = fib_touch_short or retest_short
    loc_close = loc_closes[lcid]
    loc_ema_fast = loc_ema_fast_line[lcid]
    loc_ema_slow = loc_ema_slow_line[lcid]
    loc_rsi_value = loc_rsi_line[lcid]
    loc_atr_value = loc_atr_line[lcid]
    if None in {loc_ema_fast, loc_ema_slow, loc_rsi_value, loc_atr_value}:
        raise RuntimeError("LOC indicators are not ready yet")
    prev_loc_ema_fast = loc_ema_fast_line[lcid - 1] if lcid >= 1 else None
    prev_loc_ema_slow = loc_ema_slow_line[lcid - 1] if lcid >= 1 else None

    closes = [c.close for c in ltf_candles]
    opens = [c.open for c in ltf_candles]
    highs = [c.high for c in ltf_candles]
    lows = [c.low for c in ltf_candles]
    volumes = [max(0.0, float(c.volume)) for c in ltf_candles]

    vol_window = 20
    volume_avg: List[float] = [0.0] * len(volumes)
    vol_q: deque = deque()
    vol_sum = 0.0
    for i, v in enumerate(volumes):
        volume_avg[i] = (vol_sum / len(vol_q)) if vol_q else 0.0
        vv = max(0.0, float(v))
        vol_q.append(vv)
        vol_sum += vv
        if len(vol_q) > vol_window:
            vol_sum -= float(vol_q.popleft())

    ema_line = ema(closes, p.ltf_ema_len)
    ema_20_line = ema(closes, 20)
    ema_50_line = ema(closes, 50)
    rsi_line = rsi(closes, p.rsi_len)
    _, _, macd_hist = macd(closes, p.macd_fast, p.macd_slow, p.macd_signal)
    atr_line = atr(highs, lows, closes, p.atr_len)
    bb_mid, bb_up, bb_low = bollinger(closes, p.bb_len, p.bb_mult)
    hh = rolling_high(highs, p.break_len)
    ll = rolling_low(lows, p.break_len)
    exit_low = rolling_low(lows, p.exit_len)
    exit_high = rolling_high(highs, p.exit_len)

    idx = len(ltf_candles) - 1
    close = closes[idx]
    em = ema_line[idx]
    em20 = ema_20_line[idx]
    em50 = ema_50_line[idx]
    r = rsi_line[idx]
    mh = macd_hist[idx]
    a = atr_line[idx]
    upper = bb_up[idx]
    lower = bb_low[idx]
    hhv = hh[idx]
    llv = ll[idx]
    exl = exit_low[idx]
    exh = exit_high[idx]

    if None in {em, em20, em50, r, mh, a, upper, lower, hhv, llv, exl, exh, bb_mid[idx]}:
        raise RuntimeError("LTF indicators are not ready yet")

    width = (upper - lower) / bb_mid[idx] if bb_mid[idx] else 0.0
    widths: List[float] = []
    for i in range(len(ltf_candles)):
        if bb_up[i] is None or bb_low[i] is None or bb_mid[i] in (None, 0):
            continue
        widths.append((bb_up[i] - bb_low[i]) / bb_mid[i])
    width_avg = sum(widths[-100:]) / len(widths[-100:]) if widths else 0.0
    vol_ok = width_avg > 0 and width > width_avg * p.bb_width_k

    pb_start = max(0, idx - p.pullback_lookback + 1)
    recent_lows = lows[pb_start : idx + 1]
    recent_highs = highs[pb_start : idx + 1]
    recent_pullback_low = min(recent_lows) if recent_lows else close
    recent_pullback_high = max(recent_highs) if recent_highs else close
    recent_rsi_vals = [float(v) for v in rsi_line[pb_start : idx + 1] if v is not None]
    recent_rsi_min = min(recent_rsi_vals) if recent_rsi_vals else None
    recent_rsi_max = max(recent_rsi_vals) if recent_rsi_vals else None
    pullback_long = recent_pullback_low <= em * (1.0 + p.pullback_tolerance)
    pullback_short = recent_pullback_high >= em * (1.0 - p.pullback_tolerance)
    not_chasing_long = close <= em * (1.0 + p.max_chase_from_ema)
    not_chasing_short = close >= em * (1.0 - p.max_chase_from_ema)

    prev_hhv = hh[idx - 1] if idx > 0 else None
    prev_llv = ll[idx - 1] if idx > 0 else None
    prev_exl = exit_low[idx - 1] if idx > 0 else None
    prev_exh = exit_high[idx - 1] if idx > 0 else None
    prev_day_high, prev_day_low, prev_day_close, day_high_so_far, day_low_so_far = _prev_utc_day_hlc_and_today_range(
        ltf_candles, idx
    )
    hour_open, hour_high, hour_low, hour_close, hour_prev_close, hour_rsi_value = _latest_completed_hour_context(
        ltf_candles
    )
    variant_inputs = VariantSignalInputs(
        p=p,
        bias=bias,
        close=float(close),
        ema_value=float(em),
        rsi_value=float(r),
        macd_hist_value=float(mh),
        atr_value=float(a),
        hhv=float(hhv),
        llv=float(llv),
        exl=float(exl),
        exh=float(exh),
        pb_low=float(recent_pullback_low),
        pb_high=float(recent_pullback_high),
        h_close=float(h_close),
        h_ema_fast=float(h_ema_fast),
        h_ema_slow=float(h_ema_slow),
        width=float(width),
        width_avg=float(width_avg),
        long_location_ok=bool(long_location_ok),
        short_location_ok=bool(short_location_ok),
        pullback_long=bool(pullback_long),
        pullback_short=bool(pullback_short),
        not_chasing_long=bool(not_chasing_long),
        not_chasing_short=bool(not_chasing_short),
        prev_hhv=float(prev_hhv) if prev_hhv is not None else None,
        prev_llv=float(prev_llv) if prev_llv is not None else None,
        prev_exl=float(prev_exl) if prev_exl is not None else None,
        prev_exh=float(prev_exh) if prev_exh is not None else None,
        current_high=float(highs[idx]) if idx >= 0 else None,
        current_low=float(lows[idx]) if idx >= 0 else None,
        prev_high=float(highs[idx - 1]) if idx >= 1 else None,
        prev_low=float(lows[idx - 1]) if idx >= 1 else None,
        prev2_high=float(highs[idx - 2]) if idx >= 2 else None,
        prev2_low=float(lows[idx - 2]) if idx >= 2 else None,
        prev3_high=float(highs[idx - 3]) if idx >= 3 else None,
        prev3_low=float(lows[idx - 3]) if idx >= 3 else None,
        current_open=float(opens[idx]) if idx >= 0 else None,
        prev_open=float(opens[idx - 1]) if idx >= 1 else None,
        prev_close=float(closes[idx - 1]) if idx >= 1 else None,
        prev2_open=float(opens[idx - 2]) if idx >= 2 else None,
        prev2_close=float(closes[idx - 2]) if idx >= 2 else None,
        prev3_open=float(opens[idx - 3]) if idx >= 3 else None,
        prev3_close=float(closes[idx - 3]) if idx >= 3 else None,
        upper_band=float(upper) if upper is not None else None,
        lower_band=float(lower) if lower is not None else None,
        mid_band=float(bb_mid[idx]) if bb_mid[idx] is not None else None,
        prev_macd_hist=float(macd_hist[idx - 1]) if idx >= 1 and macd_hist[idx - 1] is not None else None,
        volume=float(volumes[idx]) if idx >= 0 else None,
        volume_avg=float(volume_avg[idx]) if idx >= 0 else None,
        prev_day_high=float(prev_day_high) if prev_day_high is not None else None,
        prev_day_low=float(prev_day_low) if prev_day_low is not None else None,
        prev_day_close=float(prev_day_close) if prev_day_close is not None else None,
        day_high_so_far=float(day_high_so_far) if day_high_so_far is not None else None,
        day_low_so_far=float(day_low_so_far) if day_low_so_far is not None else None,
        prev_h_ema_fast=float(prev_h_ema_fast) if prev_h_ema_fast is not None else None,
        prev_h_ema_slow=float(prev_h_ema_slow) if prev_h_ema_slow is not None else None,
        recent_rsi_min=float(recent_rsi_min) if recent_rsi_min is not None else None,
        recent_rsi_max=float(recent_rsi_max) if recent_rsi_max is not None else None,
        prev_ema_value=float(ema_20_line[idx - 1]) if idx >= 1 and ema_20_line[idx - 1] is not None else None,
        prev2_ema_value=float(ema_line[idx - 2]) if idx >= 2 and ema_line[idx - 2] is not None else None,
        prev3_ema_value=float(ema_line[idx - 3]) if idx >= 3 and ema_line[idx - 3] is not None else None,
        prev5_ema_value=float(ema_line[idx - 5]) if idx >= 5 and ema_line[idx - 5] is not None else None,
        ema_slow_value=float(em50) if em50 is not None else None,
        prev_ema_slow_value=float(ema_50_line[idx - 1]) if idx >= 1 and ema_50_line[idx - 1] is not None else None,
        loc_close=float(loc_close),
        loc_ema_fast=float(loc_ema_fast),
        loc_ema_slow=float(loc_ema_slow),
        prev_loc_ema_fast=float(prev_loc_ema_fast) if prev_loc_ema_fast is not None else None,
        prev_loc_ema_slow=float(prev_loc_ema_slow) if prev_loc_ema_slow is not None else None,
        loc_rsi_value=float(loc_rsi_value),
        loc_atr_value=float(loc_atr_value),
        loc_current_high=float(loc_candles[lcid].high) if lcid >= 0 else None,
        loc_current_low=float(loc_candles[lcid].low) if lcid >= 0 else None,
        hour_open=float(hour_open) if hour_open is not None else None,
        hour_high=float(hour_high) if hour_high is not None else None,
        hour_low=float(hour_low) if hour_low is not None else None,
        hour_close=float(hour_close) if hour_close is not None else None,
        hour_prev_close=float(hour_prev_close) if hour_prev_close is not None else None,
        hour_rsi_value=float(hour_rsi_value) if hour_rsi_value is not None else None,
    )
    variant_state = resolve_variant_signal_state_from_inputs(variant_inputs)

    long_entry = bool(variant_state["long_entry"])
    short_entry = bool(variant_state["short_entry"])
    long_entry_l2 = bool(variant_state["long_entry_l2"])
    short_entry_l2 = bool(variant_state["short_entry_l2"])
    long_entry_l3 = bool(variant_state["long_entry_l3"])
    short_entry_l3 = bool(variant_state["short_entry_l3"])
    long_level = int(variant_state["long_level"])
    short_level = int(variant_state["short_level"])

    long_exit_default = close < em or close < exl or mh < 0 or bias == "short"
    short_exit_default = close > em or close > exh or mh > 0 or bias == "long"
    long_exit = bool(variant_state["long_exit"]) if "long_exit" in variant_state else bool(long_exit_default)
    short_exit = bool(variant_state["short_exit"]) if "short_exit" in variant_state else bool(short_exit_default)

    long_stop = float(variant_state["long_stop"])
    short_stop = float(variant_state["short_stop"])

    payload = {
        "signal_ts_ms": ltf_candles[idx].ts_ms,
        "signal_confirm": bool(ltf_candles[idx].confirm),
        "htf_ts_ms": htf_candles[hidx].ts_ms,
        "loc_ts_ms": loc_candles[lcid].ts_ms,
        "bias": bias,
        "close": close,
        "ema": em,
        "rsi": r,
        "macd_hist": mh,
        "bb_width": width,
        "bb_width_avg": width_avg,
        "vol_ok": bool(variant_state.get("vol_ok", vol_ok)),
        "htf_close": h_close,
        "htf_ema_fast": h_ema_fast,
        "htf_ema_slow": h_ema_slow,
        "htf_rsi": h_rsi,
        "strategy_variant": str(variant_state.get("variant", PA_ORAL_BASELINE_V1)),
        "trend_sep": float(variant_state.get("trend_sep", 0.0)),
        "loc_high": loc_high,
        "loc_low": loc_low,
        "loc_recent_low": loc_recent_low,
        "loc_recent_high": loc_recent_high,
        "loc_sr_ref_high": sr_ref_high,
        "loc_sr_ref_low": sr_ref_low,
        "long_fib_zone_lo": long_fib_zone_lo,
        "long_fib_zone_hi": long_fib_zone_hi,
        "short_fib_zone_lo": short_fib_zone_lo,
        "short_fib_zone_hi": short_fib_zone_hi,
        "retest_long": bool(retest_long),
        "retest_short": bool(retest_short),
        "fib_touch_long": bool(fib_touch_long),
        "fib_touch_short": bool(fib_touch_short),
        "location_long_ok": bool(long_location_ok),
        "location_short_ok": bool(short_location_ok),
        "fresh_break_long": bool(variant_state.get("fresh_break_long", False)),
        "fresh_break_short": bool(variant_state.get("fresh_break_short", False)),
        "smc_sweep_long": bool(variant_state.get("smc_sweep_long", False)),
        "smc_sweep_short": bool(variant_state.get("smc_sweep_short", False)),
        "smc_bullish_fvg": bool(variant_state.get("smc_bullish_fvg", False)),
        "smc_bearish_fvg": bool(variant_state.get("smc_bearish_fvg", False)),
        "combo_squeeze": bool(variant_state.get("combo_squeeze", False)),
        "combo_vol_spike": bool(variant_state.get("combo_vol_spike", False)),
        "combo_bull_pattern": bool(variant_state.get("combo_bull_pattern", False)),
        "combo_bear_pattern": bool(variant_state.get("combo_bear_pattern", False)),
        "combo_macd_gc": bool(variant_state.get("combo_macd_gc", False)),
        "combo_macd_dc": bool(variant_state.get("combo_macd_dc", False)),
        "combo_touch_lower": bool(variant_state.get("combo_touch_lower", False)),
        "combo_touch_upper": bool(variant_state.get("combo_touch_upper", False)),
        "rbreaker_ready": bool(variant_state.get("rbreaker_ready", False)),
        "rbreaker_breakout_long": bool(variant_state.get("rbreaker_breakout_long", False)),
        "rbreaker_breakout_short": bool(variant_state.get("rbreaker_breakout_short", False)),
        "rbreaker_reversal_long": bool(variant_state.get("rbreaker_reversal_long", False)),
        "rbreaker_reversal_short": bool(variant_state.get("rbreaker_reversal_short", False)),
        "volume": float(volumes[idx]),
        "volume_avg": float(volume_avg[idx]),
        "atr": a,
        "recent_pullback_low": recent_pullback_low,
        "recent_pullback_high": recent_pullback_high,
        "long_stop": long_stop,
        "short_stop": short_stop,
        "long_entry": bool(long_entry),
        "short_entry": bool(short_entry),
        "long_entry_l2": bool(long_entry_l2),
        "short_entry_l2": bool(short_entry_l2),
        "long_entry_l3": bool(long_entry_l3),
        "short_entry_l3": bool(short_entry_l3),
        "long_level": int(long_level),
        "short_level": int(short_level),
        "long_exit": bool(long_exit),
        "short_exit": bool(short_exit),
    }
    for key in _VARIANT_SIGNAL_EXTRA_KEYS:
        if key in variant_state:
            payload[key] = variant_state[key]
    return SignalSnapshot.from_dict(payload).to_dict()


def build_signal_snapshot(
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
) -> SignalSnapshot:
    return SignalSnapshot.from_dict(build_signals(htf_candles, loc_candles, ltf_candles, p))

def compute_alert_targets(side: str, entry_price: float, stop_price: float, tp1_r: float, tp2_r: float) -> Tuple[float, float, float]:
    risk = abs(entry_price - stop_price)
    if risk <= 0:
        risk = max(abs(entry_price) * 0.0005, 1e-8)

    s = side.strip().upper()
    if s == "LONG":
        tp1 = entry_price + risk * tp1_r
        tp2 = entry_price + risk * tp2_r
    elif s == "SHORT":
        tp1 = entry_price - risk * tp1_r
        tp2 = entry_price - risk * tp2_r
    else:
        raise RuntimeError(f"Unsupported side for target calc: {side}")
    return risk, tp1, tp2

def select_signal_candidate(
    sig: Mapping[str, Any],
    max_level: int,
    min_level: int = 1,
    exact_level: int = 0,
) -> Optional[Tuple[str, int, float]]:
    max_level = max(1, min(3, int(max_level)))
    min_level = max(1, min(3, int(min_level)))
    if min_level > max_level:
        min_level = max_level
    exact_level = int(exact_level or 0)

    long_level = int(sig.get("long_level", 0) or 0)
    short_level = int(sig.get("short_level", 0) or 0)
    candidates: List[Tuple[str, int, float]] = []

    def _ok(level: int) -> bool:
        if level <= 0:
            return False
        if 1 <= exact_level <= 3:
            return level == exact_level
        return min_level <= level <= max_level

    if _ok(long_level):
        candidates.append(("LONG", long_level, float(sig["long_stop"])))
    if _ok(short_level):
        candidates.append(("SHORT", short_level, float(sig["short_stop"])))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1])
    return candidates[0]
