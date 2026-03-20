from __future__ import annotations

import math
from typing import List, Optional, Tuple


def ema(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    alpha = 2.0 / (period + 1.0)
    prev = seed
    for i in range(period, len(values)):
        prev = (values[i] - prev) * alpha + prev
        out[i] = prev
    return out


def rsi(values: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or len(values) <= period:
        return out

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        delta = values[i] - values[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        out[period] = 100.0
    else:
        rs = avg_gain / avg_loss
        out[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, len(values)):
        delta = values[i] - values[i - 1]
        gain = max(delta, 0.0)
        loss = max(-delta, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def macd(
    values: List[float], fast: int, slow: int, signal: int
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    fast_ema = ema(values, fast)
    slow_ema = ema(values, slow)
    line: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if fast_ema[i] is None or slow_ema[i] is None:
            continue
        line[i] = fast_ema[i] - slow_ema[i]

    line_values = [v if v is not None else 0.0 for v in line]
    signal_line = ema(line_values, signal)
    hist: List[Optional[float]] = [None] * len(values)
    for i in range(len(values)):
        if line[i] is None or signal_line[i] is None:
            continue
        hist[i] = line[i] - signal_line[i]
    return line, signal_line, hist


def rolling_high(values: List[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0:
        return out
    for i in range(length, len(values)):
        out[i] = max(values[i - length : i])
    return out


def rolling_low(values: List[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0:
        return out
    for i in range(length, len(values)):
        out[i] = min(values[i - length : i])
    return out


def bollinger(
    values: List[float], length: int, mult: float
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    mid: List[Optional[float]] = [None] * len(values)
    up: List[Optional[float]] = [None] * len(values)
    low: List[Optional[float]] = [None] * len(values)
    if length <= 1:
        return mid, up, low

    running_sum = 0.0
    running_sum_sq = 0.0
    for i, v in enumerate(values):
        vv = float(v)
        running_sum += vv
        running_sum_sq += vv * vv
        if i >= length:
            old = float(values[i - length])
            running_sum -= old
            running_sum_sq -= old * old
        if i < length - 1:
            continue

        mean = running_sum / length
        variance = (running_sum_sq / length) - (mean * mean)
        if variance < 0.0 and variance > -1e-12:
            variance = 0.0
        sd = math.sqrt(max(0.0, variance))
        mid[i] = mean
        up[i] = mean + mult * sd
        low[i] = mean - mult * sd
    return mid, up, low


def atr(highs: List[float], lows: List[float], closes: List[float], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(closes)
    if period <= 0 or len(closes) <= period:
        return out

    trs: List[float] = [0.0] * len(closes)
    for i in range(len(closes)):
        if i == 0:
            trs[i] = highs[i] - lows[i]
        else:
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i - 1])
            tr3 = abs(lows[i] - closes[i - 1])
            trs[i] = max(tr1, tr2, tr3)

    seed = sum(trs[1 : period + 1]) / period
    out[period] = seed
    prev = seed
    for i in range(period + 1, len(closes)):
        prev = ((prev * (period - 1)) + trs[i]) / period
        out[i] = prev
    return out
