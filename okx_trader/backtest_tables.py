from __future__ import annotations

import math
from collections import deque
from typing import Any, Callable, Dict, List, Optional, Tuple

from .backtest_cache import (
    _backtest_live_table_cache_enabled,
    _backtest_live_table_cache_path,
    _build_backtest_live_table_cache_key,
    _load_backtest_live_table_cache,
    _save_backtest_live_table_cache,
)
from .common import bar_to_seconds
from .config import resolve_exec_max_level
from .decision_core import resolve_entry_decision
from .indicators import atr, bollinger, ema, macd, rolling_high, rolling_low, rsi
from .models import Candle, Config, StrategyParams
from .pa_oral_baseline import build_pa_oral_signal_table, is_pa_oral_baseline_variant
from .profile_vote import merge_entry_votes
from .signals import _VARIANT_SIGNAL_EXTRA_KEYS
from .strategy_contract import VariantSignalInputs
from .strategy_variant import resolve_variant_signal_state_from_inputs


BuildSignalsFn = Callable[[List[Candle], List[Candle], List[Candle], StrategyParams], Dict[str, Any]]


def _resolve_signal_entry_decision(
    sig_local: Dict[str, Any],
    *,
    ltf_candles: List[Candle],
    ltf_i: int,
    p: StrategyParams,
    exec_max_level: int,
    min_level: int,
    exact_level: int,
    tp1_only: bool,
) -> Optional[Any]:
    sig_for_decision = dict(sig_local)
    entry_idx = int(sig_local.get("entry_idx_override", ltf_i) or ltf_i)
    include_start_bar = bool(sig_local.get("entry_include_start_bar", False))
    if bool(sig_local.get("entry_on_next_open", False)):
        next_i = int(ltf_i) + 1
        if next_i >= len(ltf_candles):
            return None
        sig_for_decision["close"] = float(ltf_candles[next_i].open)
        entry_idx = next_i
        include_start_bar = True
    sig_for_decision["entry_idx"] = entry_idx
    sig_for_decision["entry_include_start_bar"] = include_start_bar
    sig_for_decision["max_hold_bars"] = int(sig_local.get("max_hold_bars", 0) or 0)
    return resolve_entry_decision(
        sig_for_decision,
        max_level=exec_max_level,
        min_level=min_level,
        exact_level=exact_level,
        tp1_r=p.tp1_r_mult,
        tp2_r=p.tp2_r_mult,
        tp1_only=tp1_only,
    )


def _rolling_max_inclusive(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 0:
        return out
    dq: deque = deque()
    for i, v in enumerate(values):
        start = i - window + 1
        while dq and dq[0] < start:
            dq.popleft()
        while dq and values[dq[-1]] <= v:
            dq.pop()
        dq.append(i)
        out[i] = values[dq[0]]
    return out


def _rolling_min_inclusive(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 0:
        return out
    dq: deque = deque()
    for i, v in enumerate(values):
        start = i - window + 1
        while dq and dq[0] < start:
            dq.popleft()
        while dq and values[dq[-1]] >= v:
            dq.pop()
        dq.append(i)
        out[i] = values[dq[0]]
    return out


def _rolling_recent_valid_avg(values: List[Optional[float]], window: int) -> List[float]:
    out: List[float] = [0.0] * len(values)
    if window <= 0:
        return out
    q: deque = deque()
    running = 0.0
    for i, v in enumerate(values):
        if v is not None and not math.isnan(v):
            q.append(float(v))
            running += float(v)
            if len(q) > window:
                running -= float(q.popleft())
        out[i] = (running / len(q)) if q else 0.0
    return out


def _rolling_avg_exclusive(values: List[float], window: int) -> List[float]:
    out: List[float] = [0.0] * len(values)
    if window <= 0:
        return out
    q: deque = deque()
    running = 0.0
    for i, v in enumerate(values):
        out[i] = (running / len(q)) if q else 0.0
        vv = float(v)
        q.append(vv)
        running += vv
        if len(q) > window:
            running -= float(q.popleft())
    return out


def _build_daily_hlc_refs(
    ts_ms: List[int],
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]], List[float], List[float]]:
    day_ms = 86_400_000
    prev_day_high: List[Optional[float]] = [None] * len(ts_ms)
    prev_day_low: List[Optional[float]] = [None] * len(ts_ms)
    prev_day_close: List[Optional[float]] = [None] * len(ts_ms)
    day_high_so_far: List[float] = [0.0] * len(ts_ms)
    day_low_so_far: List[float] = [0.0] * len(ts_ms)

    daily_high: Dict[int, float] = {}
    daily_low: Dict[int, float] = {}
    daily_close: Dict[int, float] = {}

    for i, t in enumerate(ts_ms):
        day = int(t // day_ms)
        h = float(highs[i])
        l = float(lows[i])
        c = float(closes[i])
        prev_h = daily_high.get(day)
        prev_l = daily_low.get(day)
        daily_high[day] = h if prev_h is None else max(prev_h, h)
        daily_low[day] = l if prev_l is None else min(prev_l, l)
        daily_close[day] = c

    running_day = -1
    run_hi = 0.0
    run_lo = 0.0
    for i, t in enumerate(ts_ms):
        day = int(t // day_ms)
        h = float(highs[i])
        l = float(lows[i])
        if day != running_day:
            running_day = day
            run_hi = h
            run_lo = l
        else:
            run_hi = max(run_hi, h)
            run_lo = min(run_lo, l)
        day_high_so_far[i] = run_hi
        day_low_so_far[i] = run_lo

        prev_day = day - 1
        prev_day_high[i] = daily_high.get(prev_day)
        prev_day_low[i] = daily_low.get(prev_day)
        prev_day_close[i] = daily_close.get(prev_day)

    return prev_day_high, prev_day_low, prev_day_close, day_high_so_far, day_low_so_far


def _build_completed_hourly_refs(
    ltf_candles: List[Candle],
) -> Dict[str, List[Optional[float]]]:
    n = len(ltf_candles)
    hour_open: List[Optional[float]] = [None] * n
    hour_high: List[Optional[float]] = [None] * n
    hour_low: List[Optional[float]] = [None] * n
    hour_close: List[Optional[float]] = [None] * n
    hour_prev_close: List[Optional[float]] = [None] * n
    hour_rsi_value: List[Optional[float]] = [None] * n

    latest_completed_idx: Optional[int] = None
    hourly: List[Candle] = []
    hour_idx_by_ltf: List[Optional[int]] = [None] * n
    groups: Dict[int, List[Candle]] = {}

    for idx, candle in enumerate(ltf_candles):
        hour_key = int(candle.ts_ms // 3_600_000)
        group = groups.setdefault(hour_key, [])
        group.append(candle)
        if len(group) == 4:
            latest_completed_idx = len(hourly)
            hourly.append(
                Candle(
                    ts_ms=int(group[0].ts_ms),
                    open=float(group[0].open),
                    high=max(float(x.high) for x in group),
                    low=min(float(x.low) for x in group),
                    close=float(group[-1].close),
                    confirm=True,
                    volume=sum(max(0.0, float(x.volume)) for x in group),
                )
            )
        hour_idx_by_ltf[idx] = latest_completed_idx

    if hourly:
        hour_rsi_line = rsi([float(c.close) for c in hourly], 14)
        for i in range(n):
            hour_idx = hour_idx_by_ltf[i]
            if hour_idx is None:
                continue
            current = hourly[hour_idx]
            hour_open[i] = float(current.open)
            hour_high[i] = float(current.high)
            hour_low[i] = float(current.low)
            hour_close[i] = float(current.close)
            if hour_idx >= 1:
                hour_prev_close[i] = float(hourly[hour_idx - 1].close)
            if hour_idx < len(hour_rsi_line) and hour_rsi_line[hour_idx] is not None:
                hour_rsi_value[i] = float(hour_rsi_line[hour_idx])

    return {
        "hour_open": hour_open,
        "hour_high": hour_high,
        "hour_low": hour_low,
        "hour_close": hour_close,
        "hour_prev_close": hour_prev_close,
        "hour_rsi_value": hour_rsi_value,
    }


def _build_backtest_precalc(
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
) -> Dict[str, Any]:
    if is_pa_oral_baseline_variant(getattr(p, "strategy_variant", "")):
        return {
            "min_htf": 1,
            "min_loc": 1,
            "min_ltf": 1,
            "pa_oral_signal_table": build_pa_oral_signal_table(htf_candles=htf_candles, ltf_candles=ltf_candles),
            "pa_oral_htf_ts_ms": [int(c.ts_ms) for c in htf_candles],
            "pa_oral_loc_ts_ms": [int(c.ts_ms) for c in loc_candles],
            "pa_oral_ltf_ts_ms": [int(c.ts_ms) for c in ltf_candles],
            "pa_oral_ltf_confirm": [bool(c.confirm) for c in ltf_candles],
        }

    htf_closes = [c.close for c in htf_candles]
    htf_ema_fast = ema(htf_closes, p.htf_ema_fast_len)
    htf_ema_slow = ema(htf_closes, p.htf_ema_slow_len)
    htf_rsi_line = rsi(htf_closes, p.htf_rsi_len)

    loc_highs = [c.high for c in loc_candles]
    loc_lows = [c.low for c in loc_candles]
    loc_closes = [c.close for c in loc_candles]
    loc_recent_bars = max(2, p.loc_recent_bars)
    loc_ema_fast = ema(loc_closes, 20)
    loc_ema_slow = ema(loc_closes, 50)
    loc_rsi_line = rsi(loc_closes, 14)
    loc_atr_line = atr(loc_highs, loc_lows, loc_closes, 14)

    loc_lookback_high = _rolling_max_inclusive(loc_highs, max(1, p.loc_lookback))
    loc_lookback_low = _rolling_min_inclusive(loc_lows, max(1, p.loc_lookback))
    loc_recent_high = _rolling_max_inclusive(loc_highs, loc_recent_bars)
    loc_recent_low = _rolling_min_inclusive(loc_lows, loc_recent_bars)
    loc_sr_ref_high = _rolling_max_inclusive(loc_highs, max(1, p.loc_sr_lookback))
    loc_sr_ref_low = _rolling_min_inclusive(loc_lows, max(1, p.loc_sr_lookback))

    closes = [c.close for c in ltf_candles]
    opens = [c.open for c in ltf_candles]
    highs = [c.high for c in ltf_candles]
    lows = [c.low for c in ltf_candles]
    volumes = [max(0.0, float(c.volume)) for c in ltf_candles]

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
    pullback_low = _rolling_min_inclusive(lows, max(1, p.pullback_lookback))
    pullback_high = _rolling_max_inclusive(highs, max(1, p.pullback_lookback))
    ltf_ts_ms = [int(c.ts_ms) for c in ltf_candles]
    prev_day_high, prev_day_low, prev_day_close, day_high_so_far, day_low_so_far = _build_daily_hlc_refs(
        ltf_ts_ms,
        highs,
        lows,
        closes,
    )
    hourly_refs = _build_completed_hourly_refs(ltf_candles)

    bb_width: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        up = bb_up[i]
        lo = bb_low[i]
        mid = bb_mid[i]
        if up is None or lo is None or mid is None or mid == 0:
            continue
        bb_width[i] = (up - lo) / mid
    bb_width_avg = _rolling_recent_valid_avg(bb_width, 100)
    volume_avg = _rolling_avg_exclusive(volumes, 20)

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

    return {
        "min_htf": min_htf,
        "min_loc": min_loc,
        "min_ltf": min_ltf,
        "loc_recent_bars": loc_recent_bars,
        "htf_closes": htf_closes,
        "htf_ema_fast": htf_ema_fast,
        "htf_ema_slow": htf_ema_slow,
        "htf_rsi": htf_rsi_line,
        "loc_lookback_high": loc_lookback_high,
        "loc_lookback_low": loc_lookback_low,
        "loc_recent_high": loc_recent_high,
        "loc_recent_low": loc_recent_low,
        "loc_sr_ref_high": loc_sr_ref_high,
        "loc_sr_ref_low": loc_sr_ref_low,
        "loc_highs": loc_highs,
        "loc_lows": loc_lows,
        "loc_closes": loc_closes,
        "loc_ema_fast": loc_ema_fast,
        "loc_ema_slow": loc_ema_slow,
        "loc_rsi_line": loc_rsi_line,
        "loc_atr_line": loc_atr_line,
        "closes": closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "volume_avg": volume_avg,
        "ema_line": ema_line,
        "ema_20_line": ema_20_line,
        "ema_50_line": ema_50_line,
        "rsi_line": rsi_line,
        "macd_hist": macd_hist,
        "atr_line": atr_line,
        "bb_mid": bb_mid,
        "bb_up": bb_up,
        "bb_low": bb_low,
        "bb_width": bb_width,
        "bb_width_avg": bb_width_avg,
        "hh": hh,
        "ll": ll,
        "exit_low": exit_low,
        "exit_high": exit_high,
        "pullback_low": pullback_low,
        "pullback_high": pullback_high,
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
        "prev_day_close": prev_day_close,
        "day_high_so_far": day_high_so_far,
        "day_low_so_far": day_low_so_far,
        "hour_open": hourly_refs["hour_open"],
        "hour_high": hourly_refs["hour_high"],
        "hour_low": hourly_refs["hour_low"],
        "hour_close": hourly_refs["hour_close"],
        "hour_prev_close": hourly_refs["hour_prev_close"],
        "hour_rsi_value": hourly_refs["hour_rsi_value"],
    }


def _build_backtest_signal_fast(
    pre: Dict[str, Any],
    p: StrategyParams,
    hi: int,
    li: int,
    i: int,
) -> Optional[Dict[str, Any]]:
    if "pa_oral_signal_table" in pre:
        table = pre.get("pa_oral_signal_table") or []
        if 0 <= int(i) < len(table):
            payload = dict(table[int(i)])
            ltf_ts_ms = pre.get("pa_oral_ltf_ts_ms") or []
            ltf_confirm = pre.get("pa_oral_ltf_confirm") or []
            htf_ts_ms = pre.get("pa_oral_htf_ts_ms") or []
            loc_ts_ms = pre.get("pa_oral_loc_ts_ms") or []
            payload["signal_ts_ms"] = int(ltf_ts_ms[int(i)]) if int(i) < len(ltf_ts_ms) else 0
            payload["signal_confirm"] = bool(ltf_confirm[int(i)]) if int(i) < len(ltf_confirm) else True
            payload["htf_ts_ms"] = int(htf_ts_ms[int(hi) - 1]) if int(hi) > 0 and (int(hi) - 1) < len(htf_ts_ms) else 0
            payload["loc_ts_ms"] = int(loc_ts_ms[int(li) - 1]) if int(li) > 0 and (int(li) - 1) < len(loc_ts_ms) else 0
            return payload
        return None

    if hi < int(pre["min_htf"]) or li < int(pre["min_loc"]) or (i + 1) < int(pre["min_ltf"]):
        return None

    hidx = hi - 1
    lcid = li - 1

    htf_closes = pre["htf_closes"]
    htf_ema_fast = pre["htf_ema_fast"]
    htf_ema_slow = pre["htf_ema_slow"]
    htf_rsi = pre["htf_rsi"]

    h_close = htf_closes[hidx]
    h_ema_fast = htf_ema_fast[hidx]
    h_ema_slow = htf_ema_slow[hidx]
    h_rsi = htf_rsi[hidx]
    if h_ema_fast is None or h_ema_slow is None or h_rsi is None:
        return None
    prev_h_ema_fast = htf_ema_fast[hidx - 1] if hidx >= 1 else None
    prev_h_ema_slow = htf_ema_slow[hidx - 1] if hidx >= 1 else None

    bias = "neutral"
    if h_close > h_ema_fast > h_ema_slow and h_rsi >= p.htf_rsi_long_min:
        bias = "long"
    elif h_close < h_ema_fast < h_ema_slow and h_rsi <= p.htf_rsi_short_max:
        bias = "short"

    loc_high = pre["loc_lookback_high"][lcid]
    loc_low = pre["loc_lookback_low"][lcid]
    loc_recent_low = pre["loc_recent_low"][lcid]
    loc_recent_high = pre["loc_recent_high"][lcid]
    if loc_high is None or loc_low is None or loc_recent_low is None or loc_recent_high is None:
        return None

    loc_range = max(float(loc_high) - float(loc_low), 1e-9)
    fib_low = min(p.location_fib_low, p.location_fib_high)
    fib_high = max(p.location_fib_low, p.location_fib_high)
    long_fib_zone_hi = float(loc_high) - loc_range * fib_low
    long_fib_zone_lo = float(loc_high) - loc_range * fib_high
    short_fib_zone_lo = float(loc_low) + loc_range * fib_low
    short_fib_zone_hi = float(loc_low) + loc_range * fib_high

    fib_touch_long = long_fib_zone_lo <= float(loc_recent_low) <= long_fib_zone_hi
    fib_touch_short = short_fib_zone_lo <= float(loc_recent_high) <= short_fib_zone_hi

    retest_long = False
    retest_short = False
    sr_end = li - int(pre["loc_recent_bars"])
    if sr_end > 1:
        sr_idx = sr_end - 1
        sr_ref_high = pre["loc_sr_ref_high"][sr_idx]
        sr_ref_low = pre["loc_sr_ref_low"][sr_idx]
        if sr_ref_high is not None and float(sr_ref_high) > 0:
            retest_long = abs(float(loc_recent_low) - float(sr_ref_high)) / float(sr_ref_high) <= p.location_retest_tol
        if sr_ref_low is not None and float(sr_ref_low) > 0:
            retest_short = abs(float(loc_recent_high) - float(sr_ref_low)) / float(sr_ref_low) <= p.location_retest_tol

    long_location_ok = fib_touch_long or retest_long
    short_location_ok = fib_touch_short or retest_short
    loc_close = pre["loc_closes"][lcid]
    loc_ema_fast = pre["loc_ema_fast"][lcid]
    loc_ema_slow = pre["loc_ema_slow"][lcid]
    loc_rsi_value = pre["loc_rsi_line"][lcid]
    loc_atr_value = pre["loc_atr_line"][lcid]
    if loc_ema_fast is None or loc_ema_slow is None or loc_rsi_value is None or loc_atr_value is None:
        return None
    prev_loc_ema_fast = pre["loc_ema_fast"][lcid - 1] if lcid >= 1 else None
    prev_loc_ema_slow = pre["loc_ema_slow"][lcid - 1] if lcid >= 1 else None

    close = pre["closes"][i]
    em = pre["ema_line"][i]
    em50 = pre["ema_50_line"][i]
    r = pre["rsi_line"][i]
    mh = pre["macd_hist"][i]
    a = pre["atr_line"][i]
    upper = pre["bb_up"][i]
    lower = pre["bb_low"][i]
    mid = pre["bb_mid"][i]
    hhv = pre["hh"][i]
    llv = pre["ll"][i]
    exl = pre["exit_low"][i]
    exh = pre["exit_high"][i]
    pb_low = pre["pullback_low"][i]
    pb_high = pre["pullback_high"][i]
    width = pre["bb_width"][i]
    if (
        em is None
        or em50 is None
        or r is None
        or mh is None
        or a is None
        or upper is None
        or lower is None
        or mid is None
        or hhv is None
        or llv is None
        or exl is None
        or exh is None
        or pb_low is None
        or pb_high is None
        or width is None
    ):
        return None

    width_avg = float(pre["bb_width_avg"][i])
    vol_ok = width_avg > 0 and float(width) > width_avg * p.bb_width_k

    pullback_long = float(pb_low) <= float(em) * (1.0 + p.pullback_tolerance)
    pullback_short = float(pb_high) >= float(em) * (1.0 - p.pullback_tolerance)
    not_chasing_long = close <= float(em) * (1.0 + p.max_chase_from_ema)
    not_chasing_short = close >= float(em) * (1.0 - p.max_chase_from_ema)
    recent_rsi_vals = [
        float(v)
        for v in pre["rsi_line"][max(0, i - max(1, int(p.pullback_lookback)) + 1) : i + 1]
        if v is not None
    ]
    recent_rsi_min = min(recent_rsi_vals) if recent_rsi_vals else None
    recent_rsi_max = max(recent_rsi_vals) if recent_rsi_vals else None

    prev_hhv = pre["hh"][i - 1] if i > 0 else None
    prev_llv = pre["ll"][i - 1] if i > 0 else None
    prev_exl = pre["exit_low"][i - 1] if i > 0 else None
    prev_exh = pre["exit_high"][i - 1] if i > 0 else None
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
        pb_low=float(pb_low),
        pb_high=float(pb_high),
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
        current_high=float(pre["highs"][i]) if i >= 0 else None,
        current_low=float(pre["lows"][i]) if i >= 0 else None,
        prev_high=float(pre["highs"][i - 1]) if i >= 1 else None,
        prev_low=float(pre["lows"][i - 1]) if i >= 1 else None,
        prev2_high=float(pre["highs"][i - 2]) if i >= 2 else None,
        prev2_low=float(pre["lows"][i - 2]) if i >= 2 else None,
        prev3_high=float(pre["highs"][i - 3]) if i >= 3 else None,
        prev3_low=float(pre["lows"][i - 3]) if i >= 3 else None,
        current_open=float(pre["opens"][i]) if i >= 0 else None,
        prev_open=float(pre["opens"][i - 1]) if i >= 1 else None,
        prev_close=float(pre["closes"][i - 1]) if i >= 1 else None,
        prev2_open=float(pre["opens"][i - 2]) if i >= 2 else None,
        prev2_close=float(pre["closes"][i - 2]) if i >= 2 else None,
        prev3_open=float(pre["opens"][i - 3]) if i >= 3 else None,
        prev3_close=float(pre["closes"][i - 3]) if i >= 3 else None,
        upper_band=float(pre["bb_up"][i]) if pre["bb_up"][i] is not None else None,
        lower_band=float(pre["bb_low"][i]) if pre["bb_low"][i] is not None else None,
        mid_band=float(pre["bb_mid"][i]) if pre["bb_mid"][i] is not None else None,
        prev_macd_hist=float(pre["macd_hist"][i - 1]) if i >= 1 and pre["macd_hist"][i - 1] is not None else None,
        volume=float(pre["volumes"][i]) if i >= 0 else None,
        volume_avg=float(pre["volume_avg"][i]) if i >= 0 else None,
        prev_day_high=float(pre["prev_day_high"][i]) if pre["prev_day_high"][i] is not None else None,
        prev_day_low=float(pre["prev_day_low"][i]) if pre["prev_day_low"][i] is not None else None,
        prev_day_close=float(pre["prev_day_close"][i]) if pre["prev_day_close"][i] is not None else None,
        day_high_so_far=float(pre["day_high_so_far"][i]) if pre["day_high_so_far"][i] is not None else None,
        day_low_so_far=float(pre["day_low_so_far"][i]) if pre["day_low_so_far"][i] is not None else None,
        prev_h_ema_fast=float(prev_h_ema_fast) if prev_h_ema_fast is not None else None,
        prev_h_ema_slow=float(prev_h_ema_slow) if prev_h_ema_slow is not None else None,
        recent_rsi_min=float(recent_rsi_min) if recent_rsi_min is not None else None,
        recent_rsi_max=float(recent_rsi_max) if recent_rsi_max is not None else None,
        prev_ema_value=float(pre["ema_20_line"][i - 1]) if i >= 1 and pre["ema_20_line"][i - 1] is not None else None,
        prev2_ema_value=float(pre["ema_line"][i - 2]) if i >= 2 and pre["ema_line"][i - 2] is not None else None,
        prev3_ema_value=float(pre["ema_line"][i - 3]) if i >= 3 and pre["ema_line"][i - 3] is not None else None,
        prev5_ema_value=float(pre["ema_line"][i - 5]) if i >= 5 and pre["ema_line"][i - 5] is not None else None,
        ema_slow_value=float(em50) if em50 is not None else None,
        prev_ema_slow_value=float(pre["ema_50_line"][i - 1]) if i >= 1 and pre["ema_50_line"][i - 1] is not None else None,
        loc_close=float(loc_close),
        loc_ema_fast=float(loc_ema_fast),
        loc_ema_slow=float(loc_ema_slow),
        prev_loc_ema_fast=float(prev_loc_ema_fast) if prev_loc_ema_fast is not None else None,
        prev_loc_ema_slow=float(prev_loc_ema_slow) if prev_loc_ema_slow is not None else None,
        loc_rsi_value=float(loc_rsi_value),
        loc_atr_value=float(loc_atr_value),
        loc_current_high=float(pre["loc_highs"][lcid]) if lcid >= 0 else None,
        loc_current_low=float(pre["loc_lows"][lcid]) if lcid >= 0 else None,
        hour_open=float(pre["hour_open"][i]) if pre["hour_open"][i] is not None else None,
        hour_high=float(pre["hour_high"][i]) if pre["hour_high"][i] is not None else None,
        hour_low=float(pre["hour_low"][i]) if pre["hour_low"][i] is not None else None,
        hour_close=float(pre["hour_close"][i]) if pre["hour_close"][i] is not None else None,
        hour_prev_close=float(pre["hour_prev_close"][i]) if pre["hour_prev_close"][i] is not None else None,
        hour_rsi_value=float(pre["hour_rsi_value"][i]) if pre["hour_rsi_value"][i] is not None else None,
    )
    variant_state = resolve_variant_signal_state_from_inputs(variant_inputs)
    long_level = int(variant_state["long_level"])
    short_level = int(variant_state["short_level"])
    long_stop = float(variant_state["long_stop"])
    short_stop = float(variant_state["short_stop"])

    long_exit_default = close < em or close < exl or mh < 0 or bias == "short"
    short_exit_default = close > em or close > exh or mh > 0 or bias == "long"
    long_exit = bool(variant_state["long_exit"]) if "long_exit" in variant_state else bool(long_exit_default)
    short_exit = bool(variant_state["short_exit"]) if "short_exit" in variant_state else bool(short_exit_default)

    payload = {
        "close": float(close),
        "high": float(pre["highs"][i]),
        "low": float(pre["lows"][i]),
        "ema": float(em),
        "atr": float(a),
        "macd_hist": float(mh),
        "bias": str(bias),
        "long_level": int(long_level),
        "short_level": int(short_level),
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
        "long_exit": bool(long_exit),
        "short_exit": bool(short_exit),
        "vol_ok": bool(vol_ok),
    }
    for key in _VARIANT_SIGNAL_EXTRA_KEYS:
        if key in variant_state:
            payload[key] = variant_state[key]
    return payload


def _build_backtest_signal_live_window(
    *,
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
    hi: int,
    li: int,
    i: int,
    candle_limit: int,
    build_signals_fn: BuildSignalsFn,
) -> Optional[Dict[str, Any]]:
    limit = max(1, int(candle_limit))
    if hi <= 0 or li <= 0 or i < 0:
        return None
    if i >= len(ltf_candles):
        return None

    h_start = max(0, hi - limit)
    l_start = max(0, li - limit)
    t_end = i + 1
    t_start = max(0, t_end - limit)

    htf_slice = htf_candles[h_start:hi]
    loc_slice = loc_candles[l_start:li]
    ltf_slice = ltf_candles[t_start:t_end]
    try:
        sig = build_signals_fn(htf_slice, loc_slice, ltf_slice, p)
    except Exception:
        return None
    if not isinstance(sig, dict):
        return None

    c = ltf_candles[i]
    sig["high"] = float(getattr(c, "high", sig.get("high", sig.get("close", 0.0))) or sig.get("close", 0.0))
    sig["low"] = float(getattr(c, "low", sig.get("low", sig.get("close", 0.0))) or sig.get("close", 0.0))
    return sig


def _build_backtest_alignment_counts(
    htf_ts: List[int],
    loc_ts: List[int],
    ltf_ts: List[int],
    *,
    htf_bar_ms: int = 0,
    loc_bar_ms: int = 0,
    ltf_bar_ms: int = 0,
    start_idx: int = 0,
) -> Tuple[List[int], List[int]]:
    htf_counts: List[int] = [0] * len(ltf_ts)
    loc_counts: List[int] = [0] * len(ltf_ts)
    hi = 0
    li = 0
    htf_n = len(htf_ts)
    loc_n = len(loc_ts)
    start = max(0, int(start_idx))
    htf_span = max(0, int(htf_bar_ms))
    loc_span = max(0, int(loc_bar_ms))
    ltf_span = max(0, int(ltf_bar_ms))
    for i, ts in enumerate(ltf_ts):
        ts_i = int(ts)
        signal_close_ts = ts_i + ltf_span if ltf_span > 0 else ts_i
        if htf_span > 0:
            while hi < htf_n and int(htf_ts[hi]) + htf_span <= signal_close_ts:
                hi += 1
        else:
            while hi < htf_n and int(htf_ts[hi]) <= ts_i:
                hi += 1
        if loc_span > 0:
            while li < loc_n and int(loc_ts[li]) + loc_span <= signal_close_ts:
                li += 1
        else:
            while li < loc_n and int(loc_ts[li]) <= ts_i:
                li += 1
        if i >= start:
            htf_counts[i] = hi
            loc_counts[i] = li
    return htf_counts, loc_counts


def _build_backtest_signal_decision_tables(
    *,
    cfg: Config,
    inst_id: str,
    profile_id: str,
    inst_profile_ids: List[str],
    params_by_profile: Dict[str, StrategyParams],
    pre_by_profile: Dict[str, Dict[str, Any]],
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    htf_ts: List[int],
    loc_ts: List[int],
    ltf_ts: List[int],
    max_level: int,
    min_level: int,
    exact_level: int,
    tp1_only: bool,
    start_idx: int = 0,
    live_signal_window_limit: int = 0,
    build_signals_fn: BuildSignalsFn = None,  # type: ignore[assignment]
) -> Dict[str, Any]:
    if build_signals_fn is None:
        raise ValueError("build_signals_fn is required for live-window backtest tables")

    ordered_profile_ids = list(inst_profile_ids)
    if profile_id not in ordered_profile_ids:
        ordered_profile_ids.insert(0, profile_id)
    elif ordered_profile_ids and ordered_profile_ids[0] != profile_id:
        ordered_profile_ids = [profile_id] + [pid for pid in ordered_profile_ids if pid != profile_id]

    profile_params: Dict[str, StrategyParams] = {}
    profile_exec_max: Dict[str, int] = {}
    for pid in ordered_profile_ids:
        p = params_by_profile.get(pid, cfg.strategy_profiles.get(pid, cfg.params))
        profile_params[pid] = p
        profile_exec_max[pid] = min(max_level, resolve_exec_max_level(p, inst_id))

    cache_path: Optional[str] = None
    cache_key: Optional[str] = None
    if int(live_signal_window_limit or 0) > 0 and _backtest_live_table_cache_enabled():
        cache_key = _build_backtest_live_table_cache_key(
            cfg=cfg,
            inst_id=inst_id,
            profile_id=profile_id,
            ordered_profile_ids=ordered_profile_ids,
            profile_params=profile_params,
            htf_candles=htf_candles,
            loc_candles=loc_candles,
            ltf_candles=ltf_candles,
            max_level=max_level,
            min_level=min_level,
            exact_level=exact_level,
            tp1_only=tp1_only,
            start_idx=start_idx,
            live_signal_window_limit=live_signal_window_limit,
        )
        cache_path = _backtest_live_table_cache_path(cfg, inst_id, cache_key)
        cached_bundle = _load_backtest_live_table_cache(cache_path, cache_key, inst_id=inst_id)
        if cached_bundle is not None:
            if not str(cached_bundle.get("table_cache_key", "") or ""):
                cached_bundle["table_cache_key"] = str(cache_key)
            return cached_bundle

    primary_params = profile_params[profile_id]
    primary_exec_max = profile_exec_max[profile_id]
    vote_enabled = len(ordered_profile_ids) > 1
    htf_counts, loc_counts = _build_backtest_alignment_counts(
        htf_ts,
        loc_ts,
        ltf_ts,
        htf_bar_ms=bar_to_seconds(cfg.htf_bar) * 1000,
        loc_bar_ms=bar_to_seconds(cfg.loc_bar) * 1000,
        ltf_bar_ms=bar_to_seconds(cfg.ltf_bar) * 1000,
        start_idx=start_idx,
    )
    signal_table: List[Optional[Dict[str, Any]]] = [None] * len(ltf_ts)
    decision_table: List[Optional[Any]] = [None] * len(ltf_ts)

    def _build_signal_for_profile(pid: str, hi_val: int, li_val: int, ltf_i: int) -> Optional[Dict[str, Any]]:
        p = profile_params[pid]
        if pid in pre_by_profile and "pa_oral_signal_table" in pre_by_profile[pid]:
            return _build_backtest_signal_fast(pre_by_profile[pid], p, hi_val, li_val, ltf_i)
        if int(live_signal_window_limit or 0) > 0:
            return _build_backtest_signal_live_window(
                htf_candles=htf_candles,
                loc_candles=loc_candles,
                ltf_candles=ltf_candles,
                p=p,
                hi=hi_val,
                li=li_val,
                i=ltf_i,
                candle_limit=int(live_signal_window_limit),
                build_signals_fn=build_signals_fn,
            )
        return _build_backtest_signal_fast(pre_by_profile[pid], p, hi_val, li_val, ltf_i)

    for i in range(max(0, int(start_idx)), len(ltf_ts)):
        hi = htf_counts[i]
        li = loc_counts[i]
        if hi <= 0 or li <= 0:
            continue

        sig_local = _build_signal_for_profile(profile_id, hi, li, i)
        if sig_local is None:
            continue

        if vote_enabled:
            signals_by_profile: Dict[str, Dict[str, Any]] = {profile_id: sig_local}
            decisions_by_profile: Dict[str, Optional[Any]] = {
                profile_id: _resolve_signal_entry_decision(
                    sig_local,
                    ltf_candles=ltf_candles,
                    ltf_i=i,
                    p=primary_params,
                    exec_max_level=primary_exec_max,
                    min_level=min_level,
                    exact_level=exact_level,
                    tp1_only=tp1_only,
                )
            }
            for pid in ordered_profile_ids:
                if pid == profile_id:
                    continue
                if int(live_signal_window_limit or 0) <= 0 and pid not in pre_by_profile:
                    continue
                other_params = profile_params[pid]
                other_sig = _build_signal_for_profile(pid, hi, li, i)
                if other_sig is None:
                    continue
                signals_by_profile[pid] = other_sig
                decisions_by_profile[pid] = _resolve_signal_entry_decision(
                    other_sig,
                    ltf_candles=ltf_candles,
                    ltf_i=i,
                    p=other_params,
                    exec_max_level=profile_exec_max[pid],
                    min_level=min_level,
                    exact_level=exact_level,
                    tp1_only=tp1_only,
                )
            sig_local, _vote_meta = merge_entry_votes(
                base_signal=sig_local,
                profile_ids=[pid for pid in ordered_profile_ids if pid in signals_by_profile],
                signals_by_profile=signals_by_profile,
                decisions_by_profile=decisions_by_profile,
                mode=cfg.strategy_profile_vote_mode,
                min_agree=cfg.strategy_profile_vote_min_agree,
                enforce_max_level=primary_exec_max,
                profile_score_map=cfg.strategy_profile_vote_score_map,
                level_weight=cfg.strategy_profile_vote_level_weight,
                fallback_profile_ids=cfg.strategy_profile_vote_fallback_profiles,
            )

        signal_table[i] = sig_local
        decision_table[i] = _resolve_signal_entry_decision(
            sig_local,
            ltf_candles=ltf_candles,
            ltf_i=i,
            p=primary_params,
            exec_max_level=primary_exec_max,
            min_level=min_level,
            exact_level=exact_level,
            tp1_only=tp1_only,
        )

    table_bundle = {
        "htf_counts": htf_counts,
        "loc_counts": loc_counts,
        "signal_table": signal_table,
        "decision_table": decision_table,
        "table_cache_key": str(cache_key or ""),
    }
    if cache_path and cache_key:
        _save_backtest_live_table_cache(cache_path, cache_key, inst_id=inst_id, table_bundle=table_bundle)
    return table_bundle
