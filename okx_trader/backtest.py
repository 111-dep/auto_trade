from __future__ import annotations

import bisect
import datetime as dt
import math
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from .backtest_report import (
    finalize_level_perf,
    format_backtest_inst_line,
    format_backtest_result_line,
    level_perf_brief,
    new_level_perf,
    rate_str,
    update_level_perf,
)
from .common import bar_to_seconds, format_duration, log, make_progress_bar, truncate_text
from .config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    resolve_exec_max_level,
)
from .decision_core import resolve_entry_decision
from .indicators import atr, bollinger, ema, macd, rolling_high, rolling_low, rsi
from .models import Candle, Config, StrategyParams
from .okx_client import OKXClient
from .profile_vote import merge_entry_votes
from .signals import build_signals
from .strategy_contract import VariantSignalInputs
from .strategy_variant import resolve_variant_signal_state_from_inputs


def eval_signal_outcome(
    side: str,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    ltf_candles: List[Candle],
    start_idx: int,
    horizon_bars: int,
    managed_exit: bool = False,
    tp1_close_pct: float = 0.5,
    tp2_close_rest: bool = False,
    be_trigger_r_mult: float = 1.0,
    be_offset_pct: float = 0.0,
    be_fee_buffer_pct: float = 0.0,
) -> Tuple[str, float, int, int]:
    risk = abs(entry - stop)
    if risk <= 0:
        risk = max(abs(entry) * 0.0005, 1e-8)

    side_u = side.upper()
    # horizon_bars <= 0 means "hold until TP/SL or end of available data".
    if int(horizon_bars) <= 0:
        end_idx = len(ltf_candles) - 1
    else:
        end_idx = min(len(ltf_candles) - 1, start_idx + max(1, int(horizon_bars)))
    if not managed_exit:
        outcome = "NONE"
        exit_price = ltf_candles[end_idx].close
        exit_idx = end_idx

        for i in range(start_idx + 1, end_idx + 1):
            c = ltf_candles[i]
            hi = c.high
            lo = c.low
            if side_u == "LONG":
                stop_hit = lo <= stop
                tp2_hit = hi >= tp2
                tp1_hit = hi >= tp1
                if stop_hit and (tp1_hit or tp2_hit):
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if stop_hit:
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if tp2_hit:
                    outcome = "TP2"
                    exit_price = tp2
                    exit_idx = i
                    break
                if tp1_hit:
                    outcome = "TP1"
                    exit_price = tp1
                    exit_idx = i
                    break
            else:
                stop_hit = hi >= stop
                tp2_hit = lo <= tp2
                tp1_hit = lo <= tp1
                if stop_hit and (tp1_hit or tp2_hit):
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if stop_hit:
                    outcome = "STOP"
                    exit_price = stop
                    exit_idx = i
                    break
                if tp2_hit:
                    outcome = "TP2"
                    exit_price = tp2
                    exit_idx = i
                    break
                if tp1_hit:
                    outcome = "TP1"
                    exit_price = tp1
                    exit_idx = i
                    break

        if side_u == "LONG":
            r_value = (exit_price - entry) / risk
        else:
            r_value = (entry - exit_price) / risk
        held = max(0, exit_idx - start_idx)
        return outcome, r_value, held, exit_idx

    # Managed-exit mode:
    # - TP1 partial close by tp1_close_pct
    # - remaining position closes at TP2 (if enabled)
    # - stop is moved to BE (+/- offset and fee buffer) once armed.
    qty_rem = 1.0
    realized_r = 0.0
    outcome = "NONE"
    exit_idx = end_idx
    tp1_done = False
    be_armed = False
    be_trigger = max(0.0, float(be_trigger_r_mult))
    be_total_offset = max(0.0, float(be_offset_pct) + float(be_fee_buffer_pct))
    tp1_pct = min(1.0, max(0.0, float(tp1_close_pct)))
    use_tp2 = bool(tp2_close_rest and tp1_pct < 0.999)
    dynamic_stop = float(stop)

    for i in range(start_idx + 1, end_idx + 1):
        c = ltf_candles[i]
        hi = float(c.high)
        lo = float(c.low)

        if side_u == "LONG":
            if (not be_armed) and hi >= entry + risk * be_trigger:
                be_armed = True
            if be_armed:
                dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))

            if not tp1_done:
                if lo <= dynamic_stop:
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "STOP"
                    exit_idx = i
                    break
                if hi >= tp1:
                    close_qty = qty_rem * tp1_pct
                    if close_qty > 0:
                        realized_r += close_qty * ((tp1 - entry) / risk)
                        qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
                    if qty_rem <= 1e-9:
                        outcome = "TP1"
                        exit_idx = i
                        break
                    if use_tp2 and hi >= tp2:
                        realized_r += qty_rem * ((tp2 - entry) / risk)
                        qty_rem = 0.0
                        outcome = "TP2"
                        exit_idx = i
                        break
                    continue

            if tp1_done and qty_rem > 0:
                stop_hit = lo <= dynamic_stop
                tp2_hit = use_tp2 and hi >= tp2
                if stop_hit and tp2_hit:
                    # In-bar ordering unknown; keep conservative stop-first assumption.
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if stop_hit:
                    realized_r += qty_rem * ((dynamic_stop - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if tp2_hit:
                    realized_r += qty_rem * ((tp2 - entry) / risk)
                    qty_rem = 0.0
                    outcome = "TP2"
                    exit_idx = i
                    break
        else:
            if (not be_armed) and lo <= entry - risk * be_trigger:
                be_armed = True
            if be_armed:
                dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))

            if not tp1_done:
                if hi >= dynamic_stop:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "STOP"
                    exit_idx = i
                    break
                if lo <= tp1:
                    close_qty = qty_rem * tp1_pct
                    if close_qty > 0:
                        realized_r += close_qty * ((entry - tp1) / risk)
                        qty_rem = max(0.0, qty_rem - close_qty)
                    tp1_done = True
                    be_armed = True
                    dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
                    if qty_rem <= 1e-9:
                        outcome = "TP1"
                        exit_idx = i
                        break
                    if use_tp2 and lo <= tp2:
                        realized_r += qty_rem * ((entry - tp2) / risk)
                        qty_rem = 0.0
                        outcome = "TP2"
                        exit_idx = i
                        break
                    continue

            if tp1_done and qty_rem > 0:
                stop_hit = hi >= dynamic_stop
                tp2_hit = use_tp2 and lo <= tp2
                if stop_hit and tp2_hit:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if stop_hit:
                    realized_r += qty_rem * ((entry - dynamic_stop) / risk)
                    qty_rem = 0.0
                    outcome = "TP1"
                    exit_idx = i
                    break
                if tp2_hit:
                    realized_r += qty_rem * ((entry - tp2) / risk)
                    qty_rem = 0.0
                    outcome = "TP2"
                    exit_idx = i
                    break

    if qty_rem > 1e-9:
        last_close = float(ltf_candles[end_idx].close)
        if side_u == "LONG":
            realized_r += qty_rem * ((last_close - entry) / risk)
        else:
            realized_r += qty_rem * ((entry - last_close) / risk)
        if tp1_done and outcome == "NONE":
            outcome = "TP1"

    held = max(0, exit_idx - start_idx)
    return outcome, realized_r, held, exit_idx


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


def _build_backtest_precalc(
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
) -> Dict[str, Any]:
    htf_closes = [c.close for c in htf_candles]
    htf_ema_fast = ema(htf_closes, p.htf_ema_fast_len)
    htf_ema_slow = ema(htf_closes, p.htf_ema_slow_len)
    htf_rsi_line = rsi(htf_closes, p.htf_rsi_len)

    loc_highs = [c.high for c in loc_candles]
    loc_lows = [c.low for c in loc_candles]
    loc_recent_bars = max(2, p.loc_recent_bars)

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
        "closes": closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "volume_avg": volume_avg,
        "ema_line": ema_line,
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
    }


def _build_backtest_signal_fast(
    pre: Dict[str, Any],
    p: StrategyParams,
    hi: int,
    li: int,
    i: int,
) -> Optional[Dict[str, Any]]:
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

    close = pre["closes"][i]
    em = pre["ema_line"][i]
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
    if None in {em, r, mh, a, upper, lower, mid, hhv, llv, exl, exh, pb_low, pb_high, width}:
        return None

    width_avg = float(pre["bb_width_avg"][i])
    vol_ok = width_avg > 0 and float(width) > width_avg * p.bb_width_k

    pullback_long = float(pb_low) <= float(em) * (1.0 + p.pullback_tolerance)
    pullback_short = float(pb_high) >= float(em) * (1.0 - p.pullback_tolerance)
    not_chasing_long = close <= float(em) * (1.0 + p.max_chase_from_ema)
    not_chasing_short = close >= float(em) * (1.0 - p.max_chase_from_ema)

    prev_hhv = pre["hh"][i - 1] if i > 0 else None
    prev_llv = pre["ll"][i - 1] if i > 0 else None
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
    )
    variant_state = resolve_variant_signal_state_from_inputs(variant_inputs)
    long_level = int(variant_state["long_level"])
    short_level = int(variant_state["short_level"])
    long_stop = float(variant_state["long_stop"])
    short_stop = float(variant_state["short_stop"])

    long_exit = close < em or close < exl or mh < 0 or bias == "short"
    short_exit = close > em or close > exh or mh > 0 or bias == "long"

    return {
        "close": float(close),
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
    }


def _new_level_perf() -> Dict[int, Dict[str, float]]:
    return new_level_perf()


def _update_level_perf(level_perf: Dict[int, Dict[str, float]], level: int, outcome: str, r_value: float) -> None:
    update_level_perf(level_perf, level, outcome, r_value)


def _finalize_level_perf(level_perf: Dict[int, Dict[str, float]]) -> Dict[int, Dict[str, float]]:
    return finalize_level_perf(level_perf)


def _level_perf_brief(level_perf_final: Dict[int, Dict[str, float]]) -> str:
    return level_perf_brief(level_perf_final)


def run_backtest(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    max_level: int,
    min_level: int = 1,
    exact_level: int = 0,
    bt_min_open_interval_minutes: int = 0,
    bt_max_opens_per_day: int = 0,
    bt_require_tp_sl: bool = False,
    bt_tp1_only: bool = False,
    bt_managed_exit: bool = False,
    history_cache: Optional[Dict[str, Tuple[List[Candle], List[Candle], List[Candle]]]] = None,
) -> Dict[str, Any]:
    bars = max(300, int(bars))
    horizon_bars = max(0, int(horizon_bars))
    max_level = max(1, min(3, int(max_level)))
    min_level = max(1, min(3, int(min_level)))
    if min_level > max_level:
        min_level = max_level
    exact_level = int(exact_level or 0)
    if exact_level < 0 or exact_level > 3:
        exact_level = 0
    bt_min_open_interval_minutes = max(0, int(bt_min_open_interval_minutes))
    bt_max_opens_per_day = max(0, int(bt_max_opens_per_day))
    bt_require_tp_sl = bool(bt_require_tp_sl)
    bt_tp1_only = bool(bt_tp1_only)
    bt_managed_exit = bool(bt_managed_exit)
    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)

    profile_ids_by_inst: Dict[str, List[str]] = {inst_id: get_strategy_profile_ids(cfg, inst_id) for inst_id in inst_ids}
    profile_by_inst: Dict[str, str] = {
        inst_id: (ids[0] if ids else get_strategy_profile_id(cfg, inst_id))
        for inst_id, ids in profile_ids_by_inst.items()
    }
    params_by_inst: Dict[str, StrategyParams] = {
        inst_id: cfg.strategy_profiles.get(profile_by_inst[inst_id], get_strategy_params(cfg, inst_id))
        for inst_id in inst_ids
    }

    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
    need_ltf = bars + 300
    all_params: List[StrategyParams] = []
    for inst_id in inst_ids:
        ids = profile_ids_by_inst.get(inst_id) or [profile_by_inst.get(inst_id, "DEFAULT")]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        max_loc_lookback = cfg.params.loc_lookback
        max_htf_ema_slow = cfg.params.htf_ema_slow_len
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

    horizon_desc = "to_end" if horizon_bars <= 0 else str(horizon_bars)
    log(
        f"Backtest start | insts={','.join(inst_ids)} htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} "
        f"bars={bars} horizon={horizon_desc} max_level={max_level} min_level={min_level} exact_level={exact_level} "
        f"min_gap={bt_min_open_interval_minutes}m day_cap={bt_max_opens_per_day} "
        f"require_tp_sl={bt_require_tp_sl} tp1_only={bt_tp1_only} managed_exit={bt_managed_exit}"
    )
    bt_start = time.monotonic()
    inst_total = max(1, len(inst_ids))

    total_signals = 0
    total_r = 0.0
    total_tp1 = 0
    total_tp2 = 0
    total_stop = 0
    total_none = 0
    total_skip_gap = 0
    total_skip_daycap = 0
    total_skip_unresolved = 0
    total_by_level = {1: 0, 2: 0, 3: 0}
    total_by_side = {"LONG": 0, "SHORT": 0}
    total_level_perf = _new_level_perf()
    per_inst: List[Dict[str, Any]] = []

    for inst_idx, inst_id in enumerate(inst_ids, 1):
        inst_start = time.monotonic()
        inst_params = params_by_inst.get(inst_id, cfg.params)
        profile_id = profile_by_inst.get(inst_id, "DEFAULT")
        inst_profile_ids = profile_ids_by_inst.get(inst_id) or [profile_id]
        if profile_id not in inst_profile_ids:
            inst_profile_ids = [profile_id] + [x for x in inst_profile_ids if x != profile_id]
        vote_enabled = len(inst_profile_ids) > 1
        profile_disp = profile_id if not vote_enabled else f"{profile_id}+VOTE({'+'.join(inst_profile_ids)})"
        cached = history_cache.get(inst_id) if history_cache is not None else None
        if cached is not None:
            htf, loc, ltf = cached
            log(
                f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | "
                f"profile={profile_disp} using cached candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}"
            )
        else:
            log(f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | profile={profile_disp} fetching history candles...")
            try:
                htf = client.get_candles_history(inst_id, cfg.htf_bar, need_htf)
                loc = client.get_candles_history(inst_id, cfg.loc_bar, need_loc)
                ltf = client.get_candles_history(inst_id, cfg.ltf_bar, need_ltf)
            except Exception as e:
                msg = str(e)
                log(f"[{inst_id}] Backtest data error: {msg}")
                per_inst.append(
                    {
                        "inst_id": inst_id,
                        "status": "error",
                        "error": msg,
                        "signals": 0,
                        "tp1": 0,
                        "tp2": 0,
                        "stop": 0,
                        "none": 0,
                        "avg_r": 0.0,
                        "by_level": {1: 0, 2: 0, 3: 0},
                        "by_side": {"LONG": 0, "SHORT": 0},
                        "level_perf": _finalize_level_perf(_new_level_perf()),
                        "elapsed_s": float(time.monotonic() - inst_start),
                    }
                )
                continue
            if history_cache is not None:
                history_cache[inst_id] = (htf, loc, ltf)

        if len(htf) < 50 or len(loc) < 120 or len(ltf) < 300:
            msg = f"data too short htf={len(htf)} loc={len(loc)} ltf={len(ltf)}"
            log(f"[{inst_id}] Backtest {msg}")
            per_inst.append(
                {
                    "inst_id": inst_id,
                    "status": "error",
                    "error": msg,
                    "signals": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "stop": 0,
                    "none": 0,
                    "avg_r": 0.0,
                    "by_level": {1: 0, 2: 0, 3: 0},
                    "by_side": {"LONG": 0, "SHORT": 0},
                    "level_perf": _finalize_level_perf(_new_level_perf()),
                    "elapsed_s": float(time.monotonic() - inst_start),
                }
            )
            continue
        if cached is None:
            log(f"[{inst_id}] history ready | htf={len(htf)} loc={len(loc)} ltf={len(ltf)}")

        pre_by_profile: Dict[str, Dict[str, Any]] = {}
        try:
            pre_by_profile[profile_id] = _build_backtest_precalc(htf, loc, ltf, inst_params)
            for pid in inst_profile_ids:
                if pid == profile_id:
                    continue
                p = cfg.strategy_profiles.get(pid, cfg.params)
                pre_by_profile[pid] = _build_backtest_precalc(htf, loc, ltf, p)
        except Exception as e:
            msg = f"precalc failed: {e}"
            log(f"[{inst_id}] Backtest {msg}")
            per_inst.append(
                {
                    "inst_id": inst_id,
                    "status": "error",
                    "error": msg,
                    "signals": 0,
                    "tp1": 0,
                    "tp2": 0,
                    "stop": 0,
                    "none": 0,
                    "avg_r": 0.0,
                    "by_level": {1: 0, 2: 0, 3: 0},
                    "by_side": {"LONG": 0, "SHORT": 0},
                    "level_perf": _finalize_level_perf(_new_level_perf()),
                    "elapsed_s": float(time.monotonic() - inst_start),
                }
            )
            continue

        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]

        start_idx = max(0, len(ltf) - bars)
        sig_n = 0
        sum_r = 0.0
        tp1_n = 0
        tp2_n = 0
        stop_n = 0
        none_n = 0
        skip_gap_n = 0
        skip_daycap_n = 0
        skip_unresolved_n = 0
        by_level = {1: 0, 2: 0, 3: 0}
        by_side = {"LONG": 0, "SHORT": 0}
        level_perf = _new_level_perf()
        next_open_i = start_idx
        last_open_ts_ms: Optional[int] = None
        opens_per_day: Dict[str, int] = {}
        total_steps = max(1, (len(ltf) - 1) - start_idx)
        next_progress = 10

        for step_idx, i in enumerate(range(start_idx, len(ltf) - 1), 1):
            ts = ltf_ts[i]
            if i >= next_open_i:
                hi = bisect.bisect_right(htf_ts, ts)
                li = bisect.bisect_right(loc_ts, ts)
                if hi > 0 and li > 0:
                    sig = _build_backtest_signal_fast(pre_by_profile[profile_id], inst_params, hi, li, i)
                    if sig is not None:
                        if vote_enabled:
                            signals_by_profile: Dict[str, Dict[str, Any]] = {profile_id: sig}
                            decisions_by_profile: Dict[str, Optional[Any]] = {}
                            primary_exec_max = min(max_level, resolve_exec_max_level(inst_params, inst_id))
                            decisions_by_profile[profile_id] = resolve_entry_decision(
                                sig,
                                max_level=primary_exec_max,
                                min_level=min_level,
                                exact_level=exact_level,
                                tp1_r=inst_params.tp1_r_mult,
                                tp2_r=inst_params.tp2_r_mult,
                                tp1_only=bt_tp1_only,
                            )
                            for pid in inst_profile_ids:
                                if pid == profile_id:
                                    continue
                                p = cfg.strategy_profiles.get(pid, cfg.params)
                                pre_other = pre_by_profile.get(pid)
                                if pre_other is None:
                                    continue
                                other_sig = _build_backtest_signal_fast(pre_other, p, hi, li, i)
                                if other_sig is None:
                                    continue
                                signals_by_profile[pid] = other_sig
                                other_exec_max = min(max_level, resolve_exec_max_level(p, inst_id))
                                decisions_by_profile[pid] = resolve_entry_decision(
                                    other_sig,
                                    max_level=other_exec_max,
                                    min_level=min_level,
                                    exact_level=exact_level,
                                    tp1_r=p.tp1_r_mult,
                                    tp2_r=p.tp2_r_mult,
                                    tp1_only=bt_tp1_only,
                                )
                            sig, _vote_meta = merge_entry_votes(
                                base_signal=sig,
                                profile_ids=[pid for pid in inst_profile_ids if pid in signals_by_profile],
                                signals_by_profile=signals_by_profile,
                                decisions_by_profile=decisions_by_profile,
                                mode=cfg.strategy_profile_vote_mode,
                                min_agree=cfg.strategy_profile_vote_min_agree,
                                enforce_max_level=primary_exec_max,
                                profile_score_map=cfg.strategy_profile_vote_score_map,
                                level_weight=cfg.strategy_profile_vote_level_weight,
                            )

                        decision = resolve_entry_decision(
                            sig,
                            max_level=min(max_level, resolve_exec_max_level(inst_params, inst_id)),
                            min_level=min_level,
                            exact_level=exact_level,
                            tp1_r=inst_params.tp1_r_mult,
                            tp2_r=inst_params.tp2_r_mult,
                            tp1_only=bt_tp1_only,
                        )
                        if decision is not None:
                            side = decision.side
                            level = int(decision.level)
                            stop = float(decision.stop)
                            entry = float(decision.entry)
                            risk = float(decision.risk)
                            tp1 = float(decision.tp1)
                            tp2 = float(decision.tp2)
                            if risk > 0:
                                if bt_min_open_interval_minutes > 0 and last_open_ts_ms is not None:
                                    if ts - last_open_ts_ms < bt_min_open_interval_minutes * 60 * 1000:
                                        skip_gap_n += 1
                                        goto_progress = True
                                    else:
                                        goto_progress = False
                                else:
                                    goto_progress = False
                                if not goto_progress:
                                    day_key = dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                                    day_used = int(opens_per_day.get(day_key, 0))
                                    if bt_max_opens_per_day > 0 and day_used >= bt_max_opens_per_day:
                                        skip_daycap_n += 1
                                    else:
                                        outcome, r_value, _, exit_idx = eval_signal_outcome(
                                            side=side,
                                            entry=entry,
                                            stop=float(stop),
                                            tp1=tp1,
                                            tp2=tp2,
                                            ltf_candles=ltf,
                                            start_idx=i,
                                            horizon_bars=horizon_bars,
                                            managed_exit=bt_managed_exit,
                                            tp1_close_pct=inst_params.tp1_close_pct,
                                            tp2_close_rest=inst_params.tp2_close_rest,
                                            be_trigger_r_mult=inst_params.be_trigger_r_mult,
                                            be_offset_pct=inst_params.be_offset_pct,
                                            be_fee_buffer_pct=inst_params.be_fee_buffer_pct,
                                        )
                                        if bt_tp1_only and outcome == "TP2":
                                            outcome = "TP1"
                                        if bt_require_tp_sl and outcome not in {"TP1", "TP2", "STOP"}:
                                            skip_unresolved_n += 1
                                        else:
                                            sig_n += 1
                                            sum_r += r_value
                                            by_level[level] = by_level.get(level, 0) + 1
                                            by_side[side] = by_side.get(side, 0) + 1
                                            _update_level_perf(level_perf, level, outcome, r_value)
                                            _update_level_perf(total_level_perf, level, outcome, r_value)
                                            if outcome == "TP2":
                                                tp2_n += 1
                                                tp1_n += 1
                                            elif outcome == "TP1":
                                                tp1_n += 1
                                            elif outcome == "STOP":
                                                stop_n += 1
                                            else:
                                                none_n += 1
                                            opens_per_day[day_key] = day_used + 1
                                            last_open_ts_ms = ts
                                            next_open_i = max(next_open_i, int(exit_idx) + 1)

            pct = int((step_idx * 100) / total_steps)
            if pct >= next_progress or step_idx == total_steps:
                elapsed = time.monotonic() - inst_start
                speed = step_idx / elapsed if elapsed > 0 else 0.0
                remain_steps = max(0, total_steps - step_idx)
                eta = (remain_steps / speed) if speed > 0 else 0.0
                bar = make_progress_bar(step_idx, total_steps, width=24)
                log(
                    f"[{inst_id}] progress {bar} {pct:3d}% ({step_idx}/{total_steps}) "
                    f"elapsed={format_duration(elapsed)} eta={format_duration(eta)}"
                )
                while pct >= next_progress:
                    next_progress += 10

        avg_r = (sum_r / sig_n) if sig_n > 0 else 0.0
        tp1_rate = (tp1_n / sig_n * 100.0) if sig_n > 0 else 0.0
        tp2_rate = (tp2_n / sig_n * 100.0) if sig_n > 0 else 0.0
        stop_rate = (stop_n / sig_n * 100.0) if sig_n > 0 else 0.0
        level_perf_final = _finalize_level_perf(level_perf)

        log(
            f"[{inst_id}] backtest | signals={sig_n} L1/L2/L3={by_level.get(1,0)}/{by_level.get(2,0)}/{by_level.get(3,0)} "
            f"long/short={by_side.get('LONG',0)}/{by_side.get('SHORT',0)} "
            f"tp1={tp1_n}({tp1_rate:.1f}%) tp2={tp2_n}({tp2_rate:.1f}%) stop={stop_n}({stop_rate:.1f}%) "
            f"none={none_n} avgR={avg_r:.3f} level_avgR={_level_perf_brief(level_perf_final)} "
            f"skip_gap={skip_gap_n} skip_daycap={skip_daycap_n} skip_unresolved={skip_unresolved_n} "
            f"elapsed={format_duration(time.monotonic() - inst_start)}"
        )
        per_inst.append(
            {
                "inst_id": inst_id,
                "status": "ok",
                "error": "",
                "signals": sig_n,
                "tp1": tp1_n,
                "tp2": tp2_n,
                "stop": stop_n,
                "none": none_n,
                "avg_r": avg_r,
                "by_level": dict(by_level),
                "by_side": dict(by_side),
                "level_perf": level_perf_final,
                "skip_gap": int(skip_gap_n),
                "skip_daycap": int(skip_daycap_n),
                "skip_unresolved": int(skip_unresolved_n),
                "elapsed_s": float(time.monotonic() - inst_start),
            }
        )

        total_signals += sig_n
        total_r += sum_r
        total_tp1 += tp1_n
        total_tp2 += tp2_n
        total_stop += stop_n
        total_none += none_n
        total_skip_gap += int(skip_gap_n)
        total_skip_daycap += int(skip_daycap_n)
        total_skip_unresolved += int(skip_unresolved_n)
        total_by_level[1] += by_level.get(1, 0)
        total_by_level[2] += by_level.get(2, 0)
        total_by_level[3] += by_level.get(3, 0)
        total_by_side["LONG"] += by_side.get("LONG", 0)
        total_by_side["SHORT"] += by_side.get("SHORT", 0)

    elapsed_total = float(time.monotonic() - bt_start)
    total_level_perf_final = _finalize_level_perf(total_level_perf)
    result: Dict[str, Any] = {
        "max_level": max_level,
        "min_level": min_level,
        "exact_level": exact_level,
        "bars": bars,
        "horizon_bars": horizon_bars,
        "inst_ids": list(inst_ids),
        "signals": total_signals,
        "tp1": total_tp1,
        "tp2": total_tp2,
        "stop": total_stop,
        "none": total_none,
        "skip_gap": total_skip_gap,
        "skip_daycap": total_skip_daycap,
        "skip_unresolved": total_skip_unresolved,
        "avg_r": (total_r / total_signals) if total_signals > 0 else 0.0,
        "by_level": dict(total_by_level),
        "by_side": dict(total_by_side),
        "level_perf": total_level_perf_final,
        "bt_min_open_interval_minutes": bt_min_open_interval_minutes,
        "bt_max_opens_per_day": bt_max_opens_per_day,
        "bt_require_tp_sl": bt_require_tp_sl,
        "bt_tp1_only": bt_tp1_only,
        "bt_managed_exit": bt_managed_exit,
        "elapsed_s": elapsed_total,
        "per_inst": per_inst,
    }

    if total_signals <= 0:
        log(f"Backtest done | no signals found in selected range. elapsed={format_duration(elapsed_total)}")
        return result

    total_avg_r = total_r / total_signals
    total_tp1_rate = total_tp1 / total_signals * 100.0
    total_tp2_rate = total_tp2 / total_signals * 100.0
    total_stop_rate = total_stop / total_signals * 100.0
    log(
        f"Backtest total | signals={total_signals} L1/L2/L3={total_by_level[1]}/{total_by_level[2]}/{total_by_level[3]} "
        f"long/short={total_by_side['LONG']}/{total_by_side['SHORT']} "
        f"tp1={total_tp1}({total_tp1_rate:.1f}%) "
        f"tp2={total_tp2}({total_tp2_rate:.1f}%) stop={total_stop}({total_stop_rate:.1f}%) "
        f"none={total_none} avgR={total_avg_r:.3f} "
        f"skip_gap={total_skip_gap} skip_daycap={total_skip_daycap} skip_unresolved={total_skip_unresolved} "
        f"level_avgR={_level_perf_brief(total_level_perf_final)} elapsed={format_duration(elapsed_total)}"
    )
    return result


def run_backtest_compare(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    levels: List[int],
    min_level: int = 1,
    exact_level: int = 0,
    bt_min_open_interval_minutes: int = 0,
    bt_max_opens_per_day: int = 0,
    bt_require_tp_sl: bool = False,
    bt_tp1_only: bool = False,
    bt_managed_exit: bool = False,
) -> List[Dict[str, Any]]:
    picked = [lv for lv in levels if 1 <= int(lv) <= 3]
    if not picked:
        return []

    cache: Dict[str, Tuple[List[Candle], List[Candle], List[Candle]]] = {}
    results: List[Dict[str, Any]] = []
    total = len(picked)
    for idx, level in enumerate(picked, 1):
        log(f"Backtest compare | level={level} ({idx}/{total})")
        one = run_backtest(
            client=client,
            cfg=cfg,
            inst_ids=inst_ids,
            bars=bars,
            horizon_bars=horizon_bars,
            max_level=level,
            min_level=min_level,
            exact_level=exact_level,
            bt_min_open_interval_minutes=bt_min_open_interval_minutes,
            bt_max_opens_per_day=bt_max_opens_per_day,
            bt_require_tp_sl=bt_require_tp_sl,
            bt_tp1_only=bt_tp1_only,
            bt_managed_exit=bt_managed_exit,
            history_cache=cache,
        )
        results.append(one)
    return results


def _rate_str(numerator: int, denominator: int) -> str:
    return rate_str(numerator, denominator)


def _fmt_backtest_result_line(res: Dict[str, Any]) -> str:
    return format_backtest_result_line(res)


def build_backtest_telegram_summary(
    cfg: Config,
    results: List[Dict[str, Any]],
    title: str = "",
) -> str:
    lines: List[str] = []
    title_txt = title.strip()
    if title_txt:
        lines.append(f"【{title_txt}】")
    lines.append(f"回测完成：{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"周期：HTF={cfg.htf_bar} LOC={cfg.loc_bar} LTF={cfg.ltf_bar}")

    if results:
        first = results[0]
        bars = int(first.get("bars", 0))
        horizon = int(first.get("horizon_bars", 0))
        min_level = int(first.get("min_level", 1))
        exact_level = int(first.get("exact_level", 0))
        bt_gap = int(first.get("bt_min_open_interval_minutes", 0))
        bt_day_cap = int(first.get("bt_max_opens_per_day", 0))
        bt_require_tp_sl = bool(first.get("bt_require_tp_sl", False))
        bt_tp1_only = bool(first.get("bt_tp1_only", False))
        bt_managed_exit = bool(first.get("bt_managed_exit", False))
        inst_ids = first.get("inst_ids", [])
        inst_txt = ",".join(inst_ids) if isinstance(inst_ids, list) and inst_ids else "-"
        lines.append(f"样本：bars={bars} horizon={horizon} insts={inst_txt}")
        lines.append(
            f"执行约束：min_gap={bt_gap}m day_cap={bt_day_cap} require_tp_sl={bt_require_tp_sl} "
            f"tp1_only={bt_tp1_only} managed_exit={bt_managed_exit}"
        )
        if exact_level in {1, 2, 3}:
            lines.append(f"筛选：exact_level={exact_level}")
        else:
            lines.append(f"筛选：min_level={min_level}（各行max/range见下）")

        for res in results:
            lines.append(_fmt_backtest_result_line(res))
            per_inst = res.get("per_inst", [])
            if not isinstance(per_inst, list):
                continue
            for row in per_inst:
                line = format_backtest_inst_line(row if isinstance(row, dict) else {})
                if line:
                    lines.append(line)
    else:
        lines.append("无可用回测结果。")

    return truncate_text("\n".join(lines), limit=3800)
