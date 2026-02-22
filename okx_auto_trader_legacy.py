#!/usr/bin/env python3
"""
OKX auto trader (WSL-friendly, no third-party deps).

What it does:
1. Pulls closed candles from OKX.
2. Computes EMA + Bollinger + RSI + MACD.
3. Uses adaptive breakout/exit rules (not fixed absolute prices).
4. Places market orders on OKX when signals trigger.

Safety defaults:
- OKX_DRY_RUN=1 (print actions, do not place real orders)
- OKX_PAPER=1 (OKX simulated trading header)
"""

from __future__ import annotations

import argparse
import base64
import bisect
import datetime as dt
import hashlib
import hmac
import json
import math
import os
import smtplib
import statistics
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple


def log(msg: str) -> None:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def format_duration(seconds: float) -> str:
    sec = max(0.0, float(seconds))
    if sec < 60:
        return f"{sec:.1f}s"
    if sec < 3600:
        return f"{sec / 60.0:.1f}m"
    return f"{sec / 3600.0:.2f}h"


def make_progress_bar(done: int, total: int, width: int = 20) -> str:
    t = max(1, int(total))
    d = max(0, min(int(done), t))
    fill = int(round((d / t) * width))
    fill = max(0, min(fill, width))
    return "[" + ("#" * fill) + ("." * (width - fill)) + "]"


def truncate_text(text: str, limit: int = 3800) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n...（消息过长，已截断）"
    keep = max(0, limit - len(suffix))
    return text[:keep] + suffix


def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_inst_ids(value: str) -> List[str]:
    if value is None:
        return []
    out: List[str] = []
    for part in value.split(","):
        item = part.strip().upper()
        if item:
            out.append(item)
    return out


def parse_csv(value: str) -> List[str]:
    if value is None:
        return []
    out: List[str] = []
    for part in value.split(","):
        item = part.strip()
        if item:
            out.append(item)
    return out


def parse_backtest_levels(value: str) -> List[int]:
    out: List[int] = []
    seen: set = set()
    for raw in parse_csv(value):
        try:
            lv = int(raw)
        except Exception:
            continue
        if lv < 1 or lv > 3:
            continue
        if lv in seen:
            continue
        seen.add(lv)
        out.append(lv)
    return out


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value


def bar_to_seconds(bar: str) -> int:
    s = bar.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    raise ValueError(f"Unsupported bar format: {bar}")


def round_size(sz: float) -> str:
    if sz <= 0:
        raise ValueError("Order size must be > 0")
    txt = f"{sz:.8f}".rstrip("0").rstrip(".")
    return txt if txt else "0"


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def parse_inst_parts(inst_id: str) -> Tuple[str, str, str]:
    parts = inst_id.split("-")
    if len(parts) < 3:
        return "", "", ""
    return parts[0], parts[1], parts[2]


def infer_inst_type(inst_id: str) -> str:
    upper = inst_id.upper()
    if upper.endswith("-SWAP"):
        return "SWAP"
    if upper.endswith("-FUTURES"):
        return "FUTURES"
    if upper.endswith("-OPTION"):
        return "OPTION"
    return "MARGIN"


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
    for i in range(length - 1, len(values)):
        window = values[i - length + 1 : i + 1]
        mean = sum(window) / length
        sd = statistics.pstdev(window)
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


@dataclass
class Candle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    confirm: bool


@dataclass
class StrategyParams:
    htf_ema_fast_len: int
    htf_ema_slow_len: int
    htf_rsi_len: int
    htf_rsi_long_min: float
    htf_rsi_short_max: float
    loc_lookback: int
    loc_recent_bars: int
    loc_sr_lookback: int
    location_fib_low: float
    location_fib_high: float
    location_retest_tol: float

    break_len: int
    exit_len: int
    ltf_ema_len: int
    bb_len: int
    bb_mult: float
    bb_width_k: float
    rsi_len: int
    rsi_long_min: float
    rsi_short_max: float
    l2_rsi_relax: float
    l3_rsi_relax: float
    macd_fast: int
    macd_slow: int
    macd_signal: int
    pullback_lookback: int
    pullback_tolerance: float
    max_chase_from_ema: float
    atr_len: int
    atr_stop_mult: float
    min_risk_atr_mult: float
    min_risk_pct: float
    tp1_r_mult: float
    tp2_r_mult: float
    tp1_close_pct: float
    be_trigger_r_mult: float
    be_offset_pct: float
    trail_atr_mult: float
    trail_after_tp1: bool
    max_open_entries: int
    open_window_hours: int
    allow_reverse: bool
    manage_only_script_positions: bool


@dataclass
class Config:
    base_url: str
    api_key: str
    secret_key: str
    passphrase: str
    paper: bool
    dry_run: bool
    inst_ids: List[str]
    htf_bar: str
    loc_bar: str
    ltf_bar: str
    poll_seconds: int
    candle_limit: int
    td_mode: str
    pos_mode: str
    order_size: float
    sizing_mode: str
    margin_usdt: float
    leverage: float
    state_file: str
    user_agent: str
    alert_only: bool
    alert_email_enabled: bool
    alert_smtp_host: str
    alert_smtp_port: int
    alert_smtp_user: str
    alert_smtp_pass: str
    alert_smtp_from: str
    alert_smtp_to: List[str]
    alert_smtp_use_ssl: bool
    alert_smtp_starttls: bool
    alert_tg_enabled: bool
    alert_tg_bot_token: str
    alert_tg_chat_id: str
    alert_tg_api_base: str
    alert_tg_parse_mode: str
    alert_max_level: int
    alert_intrabar_enabled: bool
    alert_stats_keep_days: int
    alert_local_sound: bool
    alert_local_file: bool
    alert_local_file_path: str
    params: StrategyParams


class OKXClient:
    def __init__(self, config: Config):
        self.cfg = config
        self._instrument_cache: Dict[str, Dict[str, Any]] = {}
        self._leverage_ready: Dict[str, bool] = {}
        self._force_pos_side: Dict[str, bool] = {}

    @staticmethod
    def _is_pos_side_error(exc: Exception) -> bool:
        txt = str(exc)
        return ("51000" in txt) and ("posSide" in txt or "posside" in txt.lower())

    @staticmethod
    def _infer_pos_side(side: str, reduce_only: bool) -> str:
        s = side.strip().lower()
        if s == "buy":
            return "short" if reduce_only else "long"
        if s == "sell":
            return "long" if reduce_only else "short"
        raise RuntimeError(f"Unsupported side for posSide inference: {side}")

    def mark_force_pos_side(self, inst_id: str) -> None:
        self._force_pos_side[inst_id] = True

    def use_pos_side(self, inst_id: str) -> bool:
        return self.cfg.pos_mode == "long_short" or self._force_pos_side.get(inst_id, False)

    def _timestamp(self) -> str:
        return dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    def _sign(self, prehash: str) -> str:
        digest = hmac.new(
            self.cfg.secret_key.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        private: bool = False,
    ) -> Dict[str, Any]:
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)
        request_path = path + query
        payload = json.dumps(body, separators=(",", ":")) if body else ""
        url = self.cfg.base_url + request_path

        headers = {"Content-Type": "application/json"}
        headers["User-Agent"] = self.cfg.user_agent
        headers["Accept"] = "application/json, text/plain, */*"
        if self.cfg.paper:
            headers["x-simulated-trading"] = "1"

        if private:
            if not self.cfg.api_key or not self.cfg.secret_key or not self.cfg.passphrase:
                raise RuntimeError("Missing OKX credentials in env")
            ts = self._timestamp()
            prehash = f"{ts}{method.upper()}{request_path}{payload}"
            headers.update(
                {
                    "OK-ACCESS-KEY": self.cfg.api_key,
                    "OK-ACCESS-SIGN": self._sign(prehash),
                    "OK-ACCESS-TIMESTAMP": ts,
                    "OK-ACCESS-PASSPHRASE": self.cfg.passphrase,
                }
            )

        req = urllib.request.Request(
            url=url,
            data=payload.encode("utf-8") if payload else None,
            headers=headers,
            method=method.upper(),
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            snippet = body[:240].replace("\n", " ").strip()
            raise RuntimeError(
                f"HTTP request failed: {method} {request_path} | HTTP {e.code} {e.reason}"
                + (f" | body={snippet}" if snippet else "")
            ) from e
        except Exception as e:
            raise RuntimeError(f"HTTP request failed: {method} {request_path} | {e}") from e

        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON response: {raw[:500]}") from e

        if data.get("code") != "0":
            raise RuntimeError(
                f"OKX API error: code={data.get('code')} msg={data.get('msg')} data={data.get('data')}"
            )
        return data

    def get_candles(self, inst_id: str, bar: str, limit: int, include_unconfirmed: bool = False) -> List[Candle]:
        data = self._request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(limit)},
            private=False,
        )
        out: List[Candle] = []
        for row in data.get("data", []):
            # [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
            try:
                out.append(
                    Candle(
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        confirm=(str(row[8]) == "1"),
                    )
                )
            except Exception:
                continue
        out.sort(key=lambda c: c.ts_ms)
        if include_unconfirmed:
            return out
        return [c for c in out if c.confirm]

    def get_candles_history(self, inst_id: str, bar: str, total_limit: int) -> List[Candle]:
        need = max(1, int(total_limit))
        page_limit = 300
        seen: Dict[int, Candle] = {}
        cursor_after: Optional[int] = None
        stall = 0

        while len(seen) < need:
            params: Dict[str, str] = {"instId": inst_id, "bar": bar, "limit": str(page_limit)}
            if cursor_after is not None:
                params["after"] = str(cursor_after)
            data = self._request("GET", "/api/v5/market/history-candles", params=params, private=False)
            rows = data.get("data", [])
            if not rows:
                break

            added = 0
            oldest_ts: Optional[int] = None
            for row in rows:
                try:
                    c = Candle(
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        confirm=(str(row[8]) == "1"),
                    )
                except Exception:
                    continue
                if not c.confirm:
                    continue
                if c.ts_ms not in seen:
                    seen[c.ts_ms] = c
                    added += 1
                if oldest_ts is None or c.ts_ms < oldest_ts:
                    oldest_ts = c.ts_ms

            if oldest_ts is None:
                break
            cursor_after = oldest_ts - 1

            if added == 0:
                stall += 1
                if stall >= 2:
                    break
            else:
                stall = 0

            if len(rows) < page_limit:
                break

        out = sorted(seen.values(), key=lambda x: x.ts_ms)
        if len(out) > need:
            out = out[-need:]
        return out

    def get_positions(self, inst_id: str) -> List[Dict[str, Any]]:
        data = self._request(
            "GET",
            "/api/v5/account/positions",
            params={"instId": inst_id},
            private=True,
        )
        return data.get("data", [])

    def get_instrument(self, inst_id: str) -> Dict[str, Any]:
        if inst_id in self._instrument_cache:
            return self._instrument_cache[inst_id]

        inst_type = infer_inst_type(inst_id)
        data = self._request(
            "GET",
            "/api/v5/public/instruments",
            params={"instType": inst_type, "instId": inst_id},
            private=False,
        )
        rows = data.get("data", [])
        if not rows:
            raise RuntimeError(f"Instrument not found: {inst_id}")
        info = rows[0]
        self._instrument_cache[inst_id] = info
        return info

    def ensure_leverage(self, inst_id: str, pos_side: Optional[str] = None) -> None:
        if self.cfg.leverage <= 0:
            return
        if self.cfg.td_mode != "isolated":
            return

        cache_key = f"{inst_id}:{self.cfg.td_mode}:{self.cfg.leverage}:{pos_side or 'net'}"
        if self._leverage_ready.get(cache_key):
            return

        body: Dict[str, Any] = {
            "instId": inst_id,
            "mgnMode": self.cfg.td_mode,
            "lever": str(self.cfg.leverage),
        }
        if pos_side:
            body["posSide"] = pos_side

        if self.cfg.dry_run:
            log(f"[DRY-RUN] set_leverage payload={json.dumps(body, ensure_ascii=False)}")
            self._leverage_ready[cache_key] = True
            return

        self._request("POST", "/api/v5/account/set-leverage", body=body, private=True)
        self._leverage_ready[cache_key] = True
        log(
            "Leverage set: instId={} mgnMode={} lever={}{}".format(
                inst_id,
                self.cfg.td_mode,
                self.cfg.leverage,
                f" posSide={pos_side}" if pos_side else "",
            )
        )

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side: Optional[str] = None,
        reduce_only: bool = False,
    ) -> Dict[str, Any]:
        effective_pos_side = pos_side
        if not effective_pos_side and self.use_pos_side(inst_id):
            effective_pos_side = self._infer_pos_side(side, reduce_only)

        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": self.cfg.td_mode,
            "side": side,
            "ordType": "market",
            "sz": round_size(sz),
        }
        if effective_pos_side:
            body["posSide"] = effective_pos_side
        if reduce_only:
            body["reduceOnly"] = "true"

        if self.cfg.dry_run:
            log(f"[DRY-RUN] place_order payload={json.dumps(body, ensure_ascii=False)}")
            return {"data": [{"ordId": "DRY_RUN"}]}

        try:
            data = self._request("POST", "/api/v5/trade/order", body=body, private=True)
            return data
        except Exception as e:
            if effective_pos_side or not self._is_pos_side_error(e):
                raise

            inferred_pos_side = self._infer_pos_side(side, reduce_only)
            self.mark_force_pos_side(inst_id)
            log(
                f"[{inst_id}] Detected posSide requirement, retry with posSide={inferred_pos_side}. "
                "Set OKX_POS_MODE=long_short to match account mode."
            )

            if self.cfg.td_mode == "isolated":
                try:
                    self.ensure_leverage(inst_id, inferred_pos_side)
                except Exception as lev_err:
                    log(f"[{inst_id}] Leverage retry warning: {lev_err}")

            retry_body = dict(body)
            retry_body["posSide"] = inferred_pos_side
            data = self._request("POST", "/api/v5/trade/order", body=retry_body, private=True)
            return data


@dataclass
class PositionState:
    side: str  # flat | long | short | mixed
    size: float


def parse_position(rows: List[Dict[str, Any]], pos_mode: str) -> PositionState:
    if not rows:
        return PositionState("flat", 0.0)

    if pos_mode == "net":
        net = 0.0
        for r in rows:
            try:
                net += float(r.get("pos", "0") or "0")
            except Exception:
                continue
        if net > 0:
            return PositionState("long", abs(net))
        if net < 0:
            return PositionState("short", abs(net))
        return PositionState("flat", 0.0)

    long_sz = 0.0
    short_sz = 0.0
    for r in rows:
        pos_side = str(r.get("posSide", "")).lower()
        try:
            pos = abs(float(r.get("pos", "0") or "0"))
        except Exception:
            pos = 0.0
        if pos_side == "long":
            long_sz += pos
        elif pos_side == "short":
            short_sz += pos

    if long_sz > 0 and short_sz > 0:
        return PositionState("mixed", max(long_sz, short_sz))
    if long_sz > 0:
        return PositionState("long", long_sz)
    if short_sz > 0:
        return PositionState("short", short_sz)
    return PositionState("flat", 0.0)


def calc_order_size(client: OKXClient, cfg: Config, inst_id: str, entry_price: float) -> float:
    if cfg.sizing_mode == "fixed":
        return cfg.order_size

    if cfg.sizing_mode != "margin":
        raise RuntimeError(f"Unsupported OKX_SIZING_MODE: {cfg.sizing_mode}")

    if cfg.margin_usdt <= 0:
        raise RuntimeError("OKX_MARGIN_USDT must be > 0 when OKX_SIZING_MODE=margin")
    if cfg.leverage <= 0:
        raise RuntimeError("OKX_LEVERAGE must be > 0 when OKX_SIZING_MODE=margin")
    if entry_price <= 0:
        raise RuntimeError("Entry price must be > 0 to calculate margin-based size")

    info = client.get_instrument(inst_id)
    try:
        ct_val = float(info.get("ctVal", "0") or "0")
    except Exception:
        ct_val = 0.0
    try:
        lot_sz = float(info.get("lotSz", "1") or "1")
    except Exception:
        lot_sz = 1.0
    try:
        min_sz = float(info.get("minSz", "0") or "0")
    except Exception:
        min_sz = 0.0
    if min_sz <= 0:
        min_sz = lot_sz

    if ct_val <= 0:
        raise RuntimeError(f"Invalid ctVal for {inst_id}: {info.get('ctVal')}")

    _, quote, _ = parse_inst_parts(inst_id)
    quote = quote.upper()
    ct_val_ccy = str(info.get("ctValCcy", "")).upper()

    # For linear contracts (ctVal in base ccy), contract notional in quote = ctVal * price.
    # For quote-valued ctVal, contract notional in quote = ctVal.
    if quote and ct_val_ccy == quote:
        contract_notional_quote = ct_val
    else:
        contract_notional_quote = ct_val * entry_price

    if contract_notional_quote <= 0:
        raise RuntimeError("Computed contract notional <= 0")

    target_notional = cfg.margin_usdt * cfg.leverage
    raw_sz = target_notional / contract_notional_quote
    sized = floor_to_step(raw_sz, lot_sz)
    if sized < min_sz:
        sized = min_sz

    log(
        "Sizing: mode=margin margin={} lever={} target_notional={} contract_notional={} raw_sz={} final_sz={}".format(
            cfg.margin_usdt,
            cfg.leverage,
            round(target_notional, 6),
            round(contract_notional_quote, 6),
            round(raw_sz, 6),
            round_size(sized),
        )
    )
    return sized


def build_signals(
    htf_candles: List[Candle], loc_candles: List[Candle], ltf_candles: List[Candle], p: StrategyParams
) -> Dict[str, Any]:
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

    bias = "neutral"
    if h_close > h_ema_fast > h_ema_slow and h_rsi >= p.htf_rsi_long_min:
        bias = "long"
    elif h_close < h_ema_fast < h_ema_slow and h_rsi <= p.htf_rsi_short_max:
        bias = "short"

    loc_highs = [c.high for c in loc_candles]
    loc_lows = [c.low for c in loc_candles]
    lcid = len(loc_candles) - 1

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

    closes = [c.close for c in ltf_candles]
    highs = [c.high for c in ltf_candles]
    lows = [c.low for c in ltf_candles]

    ema_line = ema(closes, p.ltf_ema_len)
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
    r = rsi_line[idx]
    mh = macd_hist[idx]
    a = atr_line[idx]
    upper = bb_up[idx]
    lower = bb_low[idx]
    hhv = hh[idx]
    llv = ll[idx]
    exl = exit_low[idx]
    exh = exit_high[idx]

    if None in {em, r, mh, a, upper, lower, hhv, llv, exl, exh, bb_mid[idx]}:
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
    pullback_long = recent_pullback_low <= em * (1.0 + p.pullback_tolerance)
    pullback_short = recent_pullback_high >= em * (1.0 - p.pullback_tolerance)
    not_chasing_long = close <= em * (1.0 + p.max_chase_from_ema)
    not_chasing_short = close >= em * (1.0 - p.max_chase_from_ema)

    long_rsi_l1 = p.rsi_long_min
    long_rsi_l2 = p.rsi_long_min - p.l2_rsi_relax
    long_rsi_l3 = p.rsi_long_min - p.l3_rsi_relax
    short_rsi_l1 = p.rsi_short_max
    short_rsi_l2 = p.rsi_short_max + p.l2_rsi_relax
    short_rsi_l3 = p.rsi_short_max + p.l3_rsi_relax

    long_entry_l1 = (
        bias == "long"
        and long_location_ok
        and close > hhv
        and close > em
        and vol_ok
        and pullback_long
        and not_chasing_long
        and r > long_rsi_l1
        and mh > 0
    )
    short_entry_l1 = (
        bias == "short"
        and short_location_ok
        and close < llv
        and close < em
        and vol_ok
        and pullback_short
        and not_chasing_short
        and r < short_rsi_l1
        and mh < 0
    )

    long_entry_l2 = (
        bias == "long"
        and long_location_ok
        and close > em
        and pullback_long
        and not_chasing_long
        and r > long_rsi_l2
        and mh >= 0
        and (close > hhv or vol_ok)
    )
    short_entry_l2 = (
        bias == "short"
        and short_location_ok
        and close < em
        and pullback_short
        and not_chasing_short
        and r < short_rsi_l2
        and mh <= 0
        and (close < llv or vol_ok)
    )

    long_entry_l3 = (
        bias == "long"
        and long_location_ok
        and close > em
        and pullback_long
        and not_chasing_long
        and r > long_rsi_l3
    )
    short_entry_l3 = (
        bias == "short"
        and short_location_ok
        and close < em
        and pullback_short
        and not_chasing_short
        and r < short_rsi_l3
    )

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

    # Keep strict L1 flags for real-trading branch compatibility.
    long_entry = long_entry_l1
    short_entry = short_entry_l1

    long_exit = close < em or close < exl or mh < 0 or bias == "short"
    short_exit = close > em or close > exh or mh > 0 or bias == "long"

    long_stop = min(exl, recent_pullback_low, em - (a * p.atr_stop_mult))
    short_stop = max(exh, recent_pullback_high, em + (a * p.atr_stop_mult))
    min_stop_gap = max(a * 0.25, close * 0.0004)
    if long_stop >= close - min_stop_gap:
        long_stop = close - min_stop_gap
    if short_stop <= close + min_stop_gap:
        short_stop = close + min_stop_gap

    return {
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
        "htf_close": h_close,
        "htf_ema_fast": h_ema_fast,
        "htf_ema_slow": h_ema_slow,
        "htf_rsi": h_rsi,
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


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def day_key_from_ts_ms(ts_ms: int) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d")


def _prune_key_map_by_day(key_map: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    if not isinstance(key_map, dict):
        return {}
    keep_days = max(1, int(keep_days))
    oldest = (dt.datetime.utcnow().date() - dt.timedelta(days=keep_days - 1)).strftime("%Y-%m-%d")
    out: Dict[str, Any] = {}
    for k, v in key_map.items():
        if not isinstance(k, str):
            continue
        day = k.split(":", 1)[0]
        if len(day) != 10:
            continue
        if day >= oldest:
            out[k] = v
    return out


def _prune_daily_stats(daily: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    if not isinstance(daily, dict):
        return {}
    keep_days = max(1, int(keep_days))
    oldest = (dt.datetime.utcnow().date() - dt.timedelta(days=keep_days - 1)).strftime("%Y-%m-%d")
    out: Dict[str, Any] = {}
    for day, bucket in daily.items():
        if not isinstance(day, str) or len(day) != 10:
            continue
        if day >= oldest and isinstance(bucket, dict):
            out[day] = bucket
    return out


def _get_daily_bucket(inst_state: Dict[str, Any], day: str) -> Dict[str, Any]:
    daily = inst_state.get("daily_stats")
    if not isinstance(daily, dict):
        daily = {}
        inst_state["daily_stats"] = daily

    bucket = daily.get(day)
    if not isinstance(bucket, dict):
        bucket = {}
        daily[day] = bucket

    defaults = {
        "opp_total": 0,
        "opp_l1": 0,
        "opp_l2": 0,
        "opp_l3": 0,
        "opp_long": 0,
        "opp_short": 0,
        "opp_live": 0,
        "opp_confirm": 0,
        "alert_total": 0,
        "alert_l1": 0,
        "alert_l2": 0,
        "alert_l3": 0,
        "alert_long": 0,
        "alert_short": 0,
        "alert_live": 0,
        "alert_confirm": 0,
    }
    for k, v in defaults.items():
        try:
            bucket[k] = int(bucket.get(k, v))
        except Exception:
            bucket[k] = v
    return bucket


def _record_opportunity(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
    signal_confirm: bool,
    side: str,
    level: int,
) -> None:
    if level <= 0:
        return
    side_u = side.strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return

    day = day_key_from_ts_ms(signal_ts_ms)
    stage = "C" if signal_confirm else "L"
    key = f"{day}:{int(signal_ts_ms)}:{side_u}:{stage}"

    seen = inst_state.get("opp_seen_levels")
    if not isinstance(seen, dict):
        seen = {}
        inst_state["opp_seen_levels"] = seen
    seen = _prune_key_map_by_day(seen, cfg.alert_stats_keep_days)
    inst_state["opp_seen_levels"] = seen

    daily = inst_state.get("daily_stats")
    inst_state["daily_stats"] = _prune_daily_stats(daily, cfg.alert_stats_keep_days)
    bucket = _get_daily_bucket(inst_state, day)

    prev_raw = seen.get(key)
    prev_level: Optional[int] = None
    try:
        prev_level = int(prev_raw) if prev_raw is not None else None
    except Exception:
        prev_level = None

    if prev_level is None:
        bucket["opp_total"] += 1
        bucket[f"opp_l{level}"] += 1
        if side_u == "LONG":
            bucket["opp_long"] += 1
        else:
            bucket["opp_short"] += 1
        if signal_confirm:
            bucket["opp_confirm"] += 1
        else:
            bucket["opp_live"] += 1
        seen[key] = level
        return

    # If the same side/stage signal strengthens (e.g., L3 -> L2 -> L1), upgrade bucket.
    if level < prev_level:
        old_key = f"opp_l{prev_level}"
        new_key = f"opp_l{level}"
        bucket[old_key] = max(0, int(bucket.get(old_key, 0)) - 1)
        bucket[new_key] = int(bucket.get(new_key, 0)) + 1
        seen[key] = level


def _mark_alert_sent(cfg: Config, inst_state: Dict[str, Any], alert_key: str) -> bool:
    sent = inst_state.get("sent_alert_keys")
    if not isinstance(sent, dict):
        sent = {}
        inst_state["sent_alert_keys"] = sent
    sent = _prune_key_map_by_day(sent, cfg.alert_stats_keep_days)
    inst_state["sent_alert_keys"] = sent
    if alert_key in sent:
        return False
    sent[alert_key] = 1
    return True


def _record_alert(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
    signal_confirm: bool,
    side: str,
    level: int,
) -> None:
    side_u = side.strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return
    if level <= 0:
        return

    day = day_key_from_ts_ms(signal_ts_ms)
    daily = inst_state.get("daily_stats")
    inst_state["daily_stats"] = _prune_daily_stats(daily, cfg.alert_stats_keep_days)
    bucket = _get_daily_bucket(inst_state, day)

    bucket["alert_total"] += 1
    bucket[f"alert_l{level}"] += 1
    if side_u == "LONG":
        bucket["alert_long"] += 1
    else:
        bucket["alert_short"] += 1
    if signal_confirm:
        bucket["alert_confirm"] += 1
    else:
        bucket["alert_live"] += 1


def format_ts_ms(ts_ms: int) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts_ms)


def send_email(cfg: Config, subject: str, body: str) -> bool:
    if not cfg.alert_email_enabled:
        log("[Alert] Email disabled by ALERT_EMAIL_ENABLED=0")
        return False
    if not cfg.alert_smtp_host:
        log("[Alert] Email skipped: ALERT_SMTP_HOST is empty")
        return False
    if not cfg.alert_smtp_to:
        log("[Alert] Email skipped: ALERT_EMAIL_TO is empty")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.alert_smtp_from or cfg.alert_smtp_user or "okx-bot@localhost"
    msg["To"] = ", ".join(cfg.alert_smtp_to)
    msg.set_content(body)

    try:
        timeout = 20
        if cfg.alert_smtp_use_ssl:
            with smtplib.SMTP_SSL(cfg.alert_smtp_host, cfg.alert_smtp_port, timeout=timeout) as smtp:
                if cfg.alert_smtp_user:
                    smtp.login(cfg.alert_smtp_user, cfg.alert_smtp_pass)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(cfg.alert_smtp_host, cfg.alert_smtp_port, timeout=timeout) as smtp:
                smtp.ehlo()
                if cfg.alert_smtp_starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if cfg.alert_smtp_user:
                    smtp.login(cfg.alert_smtp_user, cfg.alert_smtp_pass)
                smtp.send_message(msg)
        return True
    except Exception as e:
        log(f"[Alert] Email send failed: {e}")
        return False


def send_telegram(cfg: Config, text: str) -> bool:
    if not cfg.alert_tg_enabled:
        log("[Alert] Telegram disabled by ALERT_TG_ENABLED=0")
        return False
    if not cfg.alert_tg_bot_token:
        log("[Alert] Telegram skipped: ALERT_TG_BOT_TOKEN is empty")
        return False
    if not cfg.alert_tg_chat_id:
        log("[Alert] Telegram skipped: ALERT_TG_CHAT_ID is empty")
        return False

    base = (cfg.alert_tg_api_base or "https://api.telegram.org").rstrip("/")
    url = f"{base}/bot{cfg.alert_tg_bot_token}/sendMessage"
    form: Dict[str, str] = {
        "chat_id": cfg.alert_tg_chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if cfg.alert_tg_parse_mode:
        form["parse_mode"] = cfg.alert_tg_parse_mode

    payload = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        if not bool(data.get("ok")):
            log(f"[Alert] Telegram send failed: {raw[:240]}")
            return False
        return True
    except Exception as e:
        log(f"[Alert] Telegram send failed: {e}")
        return False


def emit_local_alert(cfg: Config, subject: str, body: str) -> bool:
    # Always print a clear alert line in terminal.
    log(f"[ALERT] {subject}")

    if cfg.alert_local_sound:
        try:
            print("\a", end="", flush=True)
        except Exception:
            pass

    if not cfg.alert_local_file:
        return False

    path = cfg.alert_local_file_path.strip()
    if not path:
        return False

    try:
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write("=" * 72 + "\n")
            f.write(subject + "\n")
            f.write(body.rstrip() + "\n")
        return True
    except Exception as e:
        log(f"[Alert] Local file write failed: {e}")
        return False


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


def handle_entry_alert(cfg: Config, inst_id: str, sig: Dict[str, Any], state: Dict[str, Any]) -> None:
    side: Optional[str] = None
    side_cn: Optional[str] = None
    stage_cn = "收线确认" if bool(sig.get("signal_confirm", True)) else "盘中预警"
    level = 0
    level_tag = ""
    stop: Optional[float] = None
    long_level = int(sig.get("long_level", 0) or 0)
    short_level = int(sig.get("short_level", 0) or 0)

    candidates: List[Tuple[str, str, float, int]] = []
    if long_level > 0 and long_level <= cfg.alert_max_level:
        candidates.append(("LONG", "做多", float(sig["long_stop"]), long_level))
    if short_level > 0 and short_level <= cfg.alert_max_level:
        candidates.append(("SHORT", "做空", float(sig["short_stop"]), short_level))

    if not candidates:
        log(
            f"[{inst_id}] Alert: NONE (no entry signal within max_level={cfg.alert_max_level}) "
            f"long_level={long_level} short_level={short_level}"
        )
        return

    # Pick stronger signal first: lower level number means stricter/higher confidence.
    candidates.sort(key=lambda x: x[3])
    side, side_cn, stop_val_raw, level = candidates[0]
    stop = float(stop_val_raw)
    level_map = {1: "1级-严格", 2: "2级-中等", 3: "3级-宽松"}
    level_tag = level_map.get(level, f"{level}级")

    stage_key = "C" if bool(sig.get("signal_confirm", True)) else "L"
    day = day_key_from_ts_ms(int(sig["signal_ts_ms"]))
    alert_key = f"{day}:{int(sig['signal_ts_ms'])}:{side}:{stage_key}:L{level}"
    if not _mark_alert_sent(cfg, state, alert_key):
        log(f"[{inst_id}] Alert: duplicate key={alert_key}, skip")
        return

    subject = f"[交易信号][{stage_cn}][{level_tag}] {inst_id} {side_cn} {sig['close']:.2f}（{cfg.ltf_bar}）"
    entry = float(sig["close"])
    stop_val = float(stop)
    risk_val, tp1_val, tp2_val = compute_alert_targets(
        side,
        entry_price=entry,
        stop_price=stop_val,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=cfg.params.tp2_r_mult,
    )
    body = (
        f"信号时间：{format_ts_ms(int(sig['signal_ts_ms']))}\n"
        f"交易对：{inst_id}\n"
        f"方向：{side_cn}\n"
        f"信号类型：{stage_cn}\n"
        f"信号等级：{level_tag}（当前最大推送等级={cfg.alert_max_level}）\n"
        f"周期：{cfg.ltf_bar}\n"
        f"入场参考价（收盘）：{entry:.2f}\n"
        f"建议止损：{stop_val:.2f}\n"
        f"风险（1R）：{risk_val:.2f}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{tp1_val:.2f}\n"
        f"止盈二（{cfg.params.tp2_r_mult}R）：{tp2_val:.2f}\n"
        f"大周期方向：{sig['bias']}\n"
        f"HTF收盘/EMA50/EMA200：{float(sig['htf_close']):.2f} / {float(sig['htf_ema_fast']):.2f} / {float(sig['htf_ema_slow']):.2f}\n"
        f"LTF EMA/RSI/MACD柱：{float(sig['ema']):.2f} / {float(sig['rsi']):.2f} / {float(sig['macd_hist']):.4f}\n"
        f"位置过滤：fibL={sig['fib_touch_long']} fibS={sig['fib_touch_short']} rtL={sig['retest_long']} rtS={sig['retest_short']}\n"
        f"触发标记：LE={sig['long_entry']} SE={sig['short_entry']}\n"
    )
    tg_text = (
        f"{subject}\n"
        f"信号时间：{format_ts_ms(int(sig['signal_ts_ms']))}\n"
        f"方向：{side_cn}\n"
        f"信号类型：{stage_cn} | 等级：{level_tag}\n"
        f"入场：{entry:.2f} | 止损：{stop_val:.2f}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{tp1_val:.2f} | 止盈二（{cfg.params.tp2_r_mult}R）：{tp2_val:.2f}\n"
        f"大周期方向：{sig['bias']} | L1E={sig['long_entry']} S1E={sig['short_entry']} "
        f"L2E={sig.get('long_entry_l2', False)} S2E={sig.get('short_entry_l2', False)} "
        f"L3E={sig.get('long_entry_l3', False)} S3E={sig.get('short_entry_l3', False)}"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = send_telegram(cfg, tg_text)
    file_written = emit_local_alert(cfg, subject, body)
    state["last_alert_key"] = alert_key
    _record_alert(
        cfg=cfg,
        inst_state=state,
        signal_ts_ms=int(sig["signal_ts_ms"]),
        signal_confirm=bool(sig.get("signal_confirm", True)),
        side=side,
        level=int(level),
    )
    log(
        f"[{inst_id}] Alert: {side} level={level} | email_sent={email_sent} telegram_sent={telegram_sent} "
        f"file_written={file_written}"
    )


def run_test_alert(cfg: Config, test_inst: Optional[str] = None) -> int:
    inst_id = (test_inst or "").strip().upper() or (cfg.inst_ids[0] if cfg.inst_ids else "TEST-USDT-SWAP")
    now_ms = int(time.time() * 1000)
    subject = f"[交易信号][测试][2级-中等] {inst_id} 做多 99999.99（{cfg.ltf_bar}）"
    entry = 99999.99
    stop_val = 99888.88
    risk_val, tp1_val, tp2_val = compute_alert_targets(
        "LONG",
        entry_price=entry,
        stop_price=stop_val,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=cfg.params.tp2_r_mult,
    )
    body = (
        f"信号时间：{format_ts_ms(now_ms)}\n"
        f"交易对：{inst_id}\n"
        f"方向：做多（测试）\n"
        f"信号等级：2级-中等\n"
        f"周期：{cfg.ltf_bar}\n"
        f"入场参考价（收盘）：{entry:.2f}\n"
        f"建议止损：{stop_val:.2f}\n"
        f"风险（1R）：{risk_val:.2f}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{tp1_val:.2f}\n"
        f"止盈二（{cfg.params.tp2_r_mult}R）：{tp2_val:.2f}\n"
        f"大周期方向：test\n"
        f"触发标记：LE=True SE=False\n"
        f"说明：这是手动测试提醒。\n"
    )
    tg_text = (
        f"{subject}\n"
        f"信号时间：{format_ts_ms(now_ms)}\n"
        "方向：做多（测试） | 等级：2级-中等\n"
        f"入场：{entry:.2f} | 止损：{stop_val:.2f}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{tp1_val:.2f} | 止盈二（{cfg.params.tp2_r_mult}R）：{tp2_val:.2f}\n"
        "说明：这是手动测试提醒。"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = send_telegram(cfg, tg_text)
    file_written = emit_local_alert(cfg, subject, body)
    log(
        f"[{inst_id}] Test alert done | email_sent={email_sent} telegram_sent={telegram_sent} "
        f"file_written={file_written}"
    )

    if cfg.alert_email_enabled or cfg.alert_tg_enabled:
        return 0 if (email_sent or telegram_sent or file_written) else 1
    return 0 if file_written or cfg.alert_local_sound else 1


def execute_decision(
    client: OKXClient, cfg: Config, inst_id: str, sig: Dict[str, Any], pos: PositionState, state: Dict[str, Any]
) -> None:
    log(
        "[{}] signal bias={} close={:.2f} ema={:.2f} rsi={:.1f} macd_hist={:.4f} "
        "width={:.5f}/{:.5f} htf_close={:.2f} htf_ema={:.2f}/{:.2f} htf_rsi={:.1f} "
        "locL={} locS={} fibL={} fibS={} rtL={} rtS={} L1E={} S1E={} L2E={} S2E={} L3E={} S3E={} "
        "Llv={} Slv={} LX={} SX={} pos={}({})".format(
            inst_id,
            sig["bias"],
            sig["close"],
            sig["ema"],
            sig["rsi"],
            sig["macd_hist"],
            sig["bb_width"],
            sig["bb_width_avg"],
            sig["htf_close"],
            sig["htf_ema_fast"],
            sig["htf_ema_slow"],
            sig["htf_rsi"],
            sig["location_long_ok"],
            sig["location_short_ok"],
            sig["fib_touch_long"],
            sig["fib_touch_short"],
            sig["retest_long"],
            sig["retest_short"],
            sig["long_entry"],
            sig["short_entry"],
            sig.get("long_entry_l2", False),
            sig.get("short_entry_l2", False),
            sig.get("long_entry_l3", False),
            sig.get("short_entry_l3", False),
            sig.get("long_level", 0),
            sig.get("short_level", 0),
            sig["long_exit"],
            sig["short_exit"],
            pos.side,
            round_size(max(pos.size, 0.0)) if pos.size > 0 else "0",
        )
    )

    if pos.side == "mixed":
        log("Detected mixed long+short positions. Script will not trade in this state.")
        return

    if cfg.alert_only:
        handle_entry_alert(cfg, inst_id, sig, state)
        return

    trade_state = state.get("trade") if isinstance(state.get("trade"), dict) else None

    def clear_trade_state() -> None:
        state.pop("trade", None)

    def is_script_trade_state(trade: Any, expected_side: Optional[str] = None) -> bool:
        if not isinstance(trade, dict):
            return False
        side = str(trade.get("side", "")).strip().lower()
        if side not in {"long", "short"}:
            return False
        if expected_side and side != expected_side:
            return False

        managed_by = str(trade.get("managed_by", "")).strip().lower()
        if managed_by == "script":
            return True

        # Backward compatibility:
        # historical script states had no "managed_by" and manual bootstrap had bootstrapped=True.
        if (not managed_by) and (not bool(trade.get("bootstrapped", False))):
            trade["managed_by"] = "script"
            return True
        return False

    def prune_open_history() -> List[int]:
        raw = state.get("open_entry_ts_ms")
        if not isinstance(raw, list):
            raw = []
        try:
            window_ms = int(max(1, cfg.params.open_window_hours) * 3600 * 1000)
        except Exception:
            window_ms = 24 * 3600 * 1000
        now_ts = int(sig["signal_ts_ms"])
        kept: List[int] = []
        for item in raw:
            try:
                ts = int(item)
            except Exception:
                continue
            if now_ts - ts <= window_ms:
                kept.append(ts)
        state["open_entry_ts_ms"] = kept
        return kept

    def can_open_entry() -> bool:
        limit = int(cfg.params.max_open_entries)
        if limit <= 0:
            return True
        recent = prune_open_history()
        if len(recent) >= limit:
            log(
                "RiskGuard: open limit reached ({}/{} in {}h), skip entry.".format(
                    len(recent), limit, cfg.params.open_window_hours
                )
            )
            return False
        return True

    def mark_open_entry() -> None:
        recent = prune_open_history()
        recent.append(int(sig["signal_ts_ms"]))
        state["open_entry_ts_ms"] = recent
        if cfg.params.max_open_entries > 0:
            log(
                "RiskGuard: open usage {}/{} in {}h.".format(
                    len(recent), cfg.params.max_open_entries, cfg.params.open_window_hours
                )
            )

    def min_risk(price: float, atr_value: float) -> float:
        return max(atr_value * cfg.params.min_risk_atr_mult, price * cfg.params.min_risk_pct)

    def init_trade_state(side: str, entry_price: float, suggested_stop: float) -> None:
        atr_value = float(sig["atr"])
        min_gap = min_risk(entry_price, atr_value)

        if side == "long":
            stop = min(float(suggested_stop), entry_price - min_gap)
            risk = max(entry_price - stop, min_gap)
            state["trade"] = {
                "side": "long",
                "entry_price": entry_price,
                "hard_stop": stop,
                "risk": risk,
                "tp1_done": False,
                "be_armed": False,
                "peak_price": entry_price,
                "trough_price": entry_price,
                "created_ts_ms": int(sig["signal_ts_ms"]),
                "inst_id": inst_id,
                "managed_by": "script",
            }
            return

        stop = max(float(suggested_stop), entry_price + min_gap)
        risk = max(stop - entry_price, min_gap)
        state["trade"] = {
            "side": "short",
            "entry_price": entry_price,
            "hard_stop": stop,
            "risk": risk,
            "tp1_done": False,
            "be_armed": False,
            "peak_price": entry_price,
            "trough_price": entry_price,
            "created_ts_ms": int(sig["signal_ts_ms"]),
            "inst_id": inst_id,
            "managed_by": "script",
        }

    def ensure_trade_state_for_position() -> None:
        current = state.get("trade")
        if pos.side not in {"long", "short"}:
            return
        if isinstance(current, dict) and current.get("side") == pos.side:
            return

        if pos.side == "long":
            init_trade_state("long", float(sig["close"]), float(sig["long_stop"]))
        else:
            init_trade_state("short", float(sig["close"]), float(sig["short_stop"]))
        state["trade"]["bootstrapped"] = True
        log(f"Management: bootstrapped {pos.side} trade state from current position.")

    def prepare_new_entry(entry_side: str) -> float:
        lev_pos_side: Optional[str] = None
        if client.use_pos_side(inst_id):
            lev_pos_side = "long" if entry_side == "long" else "short"
        client.ensure_leverage(inst_id, lev_pos_side)
        return calc_order_size(client, cfg, inst_id, float(sig["close"]))

    def open_long() -> bool:
        if not can_open_entry():
            return False
        size = prepare_new_entry("long")
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "buy", size, pos_side=None, reduce_only=False)
        else:
            client.place_order(inst_id, "buy", size, pos_side="long", reduce_only=False)
        mark_open_entry()
        log(f"[{inst_id}] Action: OPEN LONG | size={round_size(size)}")
        return True

    def open_short() -> bool:
        if not can_open_entry():
            return False
        size = prepare_new_entry("short")
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "sell", size, pos_side=None, reduce_only=False)
        else:
            client.place_order(inst_id, "sell", size, pos_side="short", reduce_only=False)
        mark_open_entry()
        log(f"[{inst_id}] Action: OPEN SHORT | size={round_size(size)}")
        return True

    def close_long(size: float) -> None:
        if size <= 0:
            return
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "sell", size, pos_side=None, reduce_only=True)
        else:
            client.place_order(inst_id, "sell", size, pos_side="long", reduce_only=True)
        log(f"[{inst_id}] Action: CLOSE LONG")

    def close_short(size: float) -> None:
        if size <= 0:
            return
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "buy", size, pos_side=None, reduce_only=True)
        else:
            client.place_order(inst_id, "buy", size, pos_side="short", reduce_only=True)
        log(f"[{inst_id}] Action: CLOSE SHORT")

    if pos.side == "flat":
        clear_trade_state()
        if sig["long_entry"]:
            if open_long():
                init_trade_state("long", float(sig["close"]), float(sig["long_stop"]))
        elif sig["short_entry"]:
            if open_short():
                init_trade_state("short", float(sig["close"]), float(sig["short_stop"]))
        else:
            log("Action: NONE (flat, no entry)")
        return

    if cfg.params.manage_only_script_positions:
        current_trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if not is_script_trade_state(current_trade, pos.side):
            if is_script_trade_state(current_trade) and str(current_trade.get("side", "")).strip().lower() != pos.side:
                clear_trade_state()
                log(f"[{inst_id}] Tracked script trade side mismatch with account position. Cleared local trade state.")
            log(f"[{inst_id}] Detected untracked {pos.side} position. Skip management by policy.")
            return
    else:
        ensure_trade_state_for_position()

    trade_state = state.get("trade") if isinstance(state.get("trade"), dict) else None
    if not trade_state:
        log("Action: HOLD (no trade state available)")
        return

    if pos.side == "long":
        entry = float(trade_state.get("entry_price", sig["close"]))
        risk = float(trade_state.get("risk", max(float(sig["atr"]), entry * 0.001)))
        peak = max(float(trade_state.get("peak_price", entry)), float(sig["close"]))
        trade_state["peak_price"] = peak

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) >= entry + risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (long).")

        if (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry + risk * cfg.params.tp1_r_mult
            if float(sig["close"]) >= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    close_long(pos.size)
                    clear_trade_state()
                    return
                if close_size > 0:
                    close_long(close_size)
                    trade_state["tp1_done"] = True
                    trade_state["be_armed"] = True
                    log(
                        "Management: TP1 hit (long). partial_close={:.4f}, tp1={:.2f}".format(
                            close_size, tp1_price
                        )
                    )
                    return

        dynamic_stop = float(trade_state.get("hard_stop", sig["long_stop"]))
        dynamic_stop = max(dynamic_stop, float(sig["long_stop"]))
        if trade_state.get("be_armed", False):
            dynamic_stop = max(dynamic_stop, entry * (1.0 + cfg.params.be_offset_pct))
        if (not cfg.params.trail_after_tp1) or trade_state.get("tp1_done", False):
            trail_stop = peak - float(sig["atr"]) * cfg.params.trail_atr_mult
            dynamic_stop = max(dynamic_stop, trail_stop)
        trade_state["hard_stop"] = dynamic_stop

        if sig["long_exit"] or float(sig["close"]) <= dynamic_stop:
            close_long(pos.size)
            clear_trade_state()
            if cfg.params.allow_reverse and sig["short_entry"]:
                if open_short():
                    init_trade_state("short", float(sig["close"]), float(sig["short_stop"]))
        else:
            log(
                "Action: HOLD LONG | stop={:.2f} entry={:.2f} risk={:.2f} tp1_done={} be={}".format(
                    dynamic_stop,
                    entry,
                    risk,
                    trade_state.get("tp1_done", False),
                    trade_state.get("be_armed", False),
                )
            )
        return

    if pos.side == "short":
        entry = float(trade_state.get("entry_price", sig["close"]))
        risk = float(trade_state.get("risk", max(float(sig["atr"]), entry * 0.001)))
        trough = min(float(trade_state.get("trough_price", entry)), float(sig["close"]))
        trade_state["trough_price"] = trough

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) <= entry - risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (short).")

        if (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry - risk * cfg.params.tp1_r_mult
            if float(sig["close"]) <= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    close_short(pos.size)
                    clear_trade_state()
                    return
                if close_size > 0:
                    close_short(close_size)
                    trade_state["tp1_done"] = True
                    trade_state["be_armed"] = True
                    log(
                        "Management: TP1 hit (short). partial_close={:.4f}, tp1={:.2f}".format(
                            close_size, tp1_price
                        )
                    )
                    return

        dynamic_stop = float(trade_state.get("hard_stop", sig["short_stop"]))
        dynamic_stop = min(dynamic_stop, float(sig["short_stop"]))
        if trade_state.get("be_armed", False):
            dynamic_stop = min(dynamic_stop, entry * (1.0 - cfg.params.be_offset_pct))
        if (not cfg.params.trail_after_tp1) or trade_state.get("tp1_done", False):
            trail_stop = trough + float(sig["atr"]) * cfg.params.trail_atr_mult
            dynamic_stop = min(dynamic_stop, trail_stop)
        trade_state["hard_stop"] = dynamic_stop

        if sig["short_exit"] or float(sig["close"]) >= dynamic_stop:
            close_short(pos.size)
            clear_trade_state()
            if cfg.params.allow_reverse and sig["long_entry"]:
                if open_long():
                    init_trade_state("long", float(sig["close"]), float(sig["long_stop"]))
        else:
            log(
                "Action: HOLD SHORT | stop={:.2f} entry={:.2f} risk={:.2f} tp1_done={} be={}".format(
                    dynamic_stop,
                    entry,
                    risk,
                    trade_state.get("tp1_done", False),
                    trade_state.get("be_armed", False),
                )
            )


def read_config(state_file_override: Optional[str]) -> Config:
    params = StrategyParams(
        htf_ema_fast_len=int(os.getenv("STRAT_HTF_EMA_FAST_LEN", "50")),
        htf_ema_slow_len=int(os.getenv("STRAT_HTF_EMA_SLOW_LEN", "200")),
        htf_rsi_len=int(os.getenv("STRAT_HTF_RSI_LEN", "14")),
        htf_rsi_long_min=float(os.getenv("STRAT_HTF_RSI_LONG_MIN", "52")),
        htf_rsi_short_max=float(os.getenv("STRAT_HTF_RSI_SHORT_MAX", "48")),
        loc_lookback=int(os.getenv("STRAT_LOC_LOOKBACK", os.getenv("STRAT_HTF_LOCATION_LOOKBACK", "120"))),
        loc_recent_bars=int(os.getenv("STRAT_LOC_RECENT_BARS", "8")),
        loc_sr_lookback=int(os.getenv("STRAT_LOC_SR_LOOKBACK", "40")),
        location_fib_low=float(os.getenv("STRAT_LOCATION_FIB_LOW", "0.382")),
        location_fib_high=float(os.getenv("STRAT_LOCATION_FIB_HIGH", "0.618")),
        location_retest_tol=float(os.getenv("STRAT_LOCATION_RETEST_TOL", "0.003")),
        break_len=int(os.getenv("STRAT_LTF_BREAK_LEN", os.getenv("STRAT_BREAK_LEN", "20"))),
        exit_len=int(os.getenv("STRAT_LTF_EXIT_LEN", os.getenv("STRAT_EXIT_LEN", "10"))),
        ltf_ema_len=int(os.getenv("STRAT_LTF_EMA_LEN", os.getenv("STRAT_EMA_LEN", "20"))),
        bb_len=int(os.getenv("STRAT_LTF_BB_LEN", os.getenv("STRAT_BB_LEN", "20"))),
        bb_mult=float(os.getenv("STRAT_LTF_BB_MULT", os.getenv("STRAT_BB_MULT", "2.0"))),
        bb_width_k=float(os.getenv("STRAT_LTF_BB_WIDTH_K", os.getenv("STRAT_BB_WIDTH_K", "1.0"))),
        rsi_len=int(os.getenv("STRAT_LTF_RSI_LEN", os.getenv("STRAT_RSI_LEN", "14"))),
        rsi_long_min=float(os.getenv("STRAT_LTF_RSI_LONG_MIN", os.getenv("STRAT_RSI_LONG_MIN", "50"))),
        rsi_short_max=float(os.getenv("STRAT_LTF_RSI_SHORT_MAX", os.getenv("STRAT_RSI_SHORT_MAX", "50"))),
        l2_rsi_relax=float(os.getenv("STRAT_ALERT_L2_RSI_RELAX", "2")),
        l3_rsi_relax=float(os.getenv("STRAT_ALERT_L3_RSI_RELAX", "5")),
        macd_fast=int(os.getenv("STRAT_LTF_MACD_FAST", os.getenv("STRAT_MACD_FAST", "12"))),
        macd_slow=int(os.getenv("STRAT_LTF_MACD_SLOW", os.getenv("STRAT_MACD_SLOW", "26"))),
        macd_signal=int(os.getenv("STRAT_LTF_MACD_SIGNAL", os.getenv("STRAT_MACD_SIGNAL", "9"))),
        pullback_lookback=int(os.getenv("STRAT_LTF_PULLBACK_LOOKBACK", "8")),
        pullback_tolerance=float(os.getenv("STRAT_LTF_PULLBACK_TOL", "0.0015")),
        max_chase_from_ema=float(os.getenv("STRAT_LTF_MAX_CHASE_EMA", "0.0035")),
        atr_len=int(os.getenv("STRAT_LTF_ATR_LEN", "14")),
        atr_stop_mult=float(os.getenv("STRAT_LTF_ATR_STOP_MULT", "1.2")),
        min_risk_atr_mult=float(os.getenv("STRAT_LTF_MIN_RISK_ATR_MULT", "0.8")),
        min_risk_pct=float(os.getenv("STRAT_LTF_MIN_RISK_PCT", "0.001")),
        tp1_r_mult=float(os.getenv("STRAT_MGMT_TP1_R", "1.5")),
        tp2_r_mult=float(os.getenv("STRAT_MGMT_TP2_R", "2.5")),
        tp1_close_pct=float(os.getenv("STRAT_MGMT_TP1_CLOSE_PCT", "0.5")),
        be_trigger_r_mult=float(os.getenv("STRAT_MGMT_BE_TRIGGER_R", "1.0")),
        be_offset_pct=float(os.getenv("STRAT_MGMT_BE_OFFSET_PCT", "0.0005")),
        trail_atr_mult=float(os.getenv("STRAT_MGMT_TRAIL_ATR_MULT", "1.8")),
        trail_after_tp1=parse_bool(os.getenv("STRAT_MGMT_TRAIL_AFTER_TP1", "1"), True),
        max_open_entries=int(os.getenv("STRAT_MAX_OPEN_ENTRIES", "0")),
        open_window_hours=int(os.getenv("STRAT_OPEN_WINDOW_HOURS", "24")),
        allow_reverse=parse_bool(os.getenv("STRAT_ALLOW_REVERSE", "1"), True),
        manage_only_script_positions=parse_bool(os.getenv("STRAT_MANAGE_ONLY_SCRIPT_POSITIONS", "1"), True),
    )

    inst_ids = parse_inst_ids(os.getenv("OKX_INST_IDS", ""))
    if not inst_ids:
        inst_ids = [os.getenv("OKX_INST_ID", "XAU-USDT-SWAP").strip().upper()]

    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_state = os.path.join(base_dir, ".okx_auto_trader_state.json")
    default_alert_file = os.path.join(base_dir, "alerts.log")
    cfg = Config(
        base_url=os.getenv("OKX_BASE_URL", "https://www.okx.com").rstrip("/"),
        api_key=os.getenv("OKX_API_KEY", ""),
        secret_key=os.getenv("OKX_SECRET_KEY", ""),
        passphrase=os.getenv("OKX_PASSPHRASE", ""),
        paper=parse_bool(os.getenv("OKX_PAPER", "1"), True),
        dry_run=parse_bool(os.getenv("OKX_DRY_RUN", "1"), True),
        inst_ids=inst_ids,
        htf_bar=os.getenv("OKX_HTF_BAR", "4H"),
        loc_bar=os.getenv("OKX_LOC_BAR", "1H"),
        ltf_bar=os.getenv("OKX_LTF_BAR", os.getenv("OKX_BAR", "15m")),
        poll_seconds=max(3, int(os.getenv("OKX_POLL_SECONDS", "10"))),
        candle_limit=max(120, int(os.getenv("OKX_CANDLE_LIMIT", "300"))),
        td_mode=os.getenv("OKX_TD_MODE", "cross"),
        pos_mode=os.getenv("OKX_POS_MODE", "net").lower(),
        order_size=float(os.getenv("OKX_ORDER_SIZE", "1")),
        sizing_mode=os.getenv("OKX_SIZING_MODE", "fixed").lower(),
        margin_usdt=float(os.getenv("OKX_MARGIN_USDT", "25")),
        leverage=float(os.getenv("OKX_LEVERAGE", "5")),
        state_file=state_file_override or os.getenv("OKX_STATE_FILE", default_state),
        user_agent=os.getenv(
            "OKX_USER_AGENT",
            (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        ),
        alert_only=parse_bool(os.getenv("ALERT_ONLY_MODE", "1"), True),
        alert_email_enabled=parse_bool(os.getenv("ALERT_EMAIL_ENABLED", "0"), False),
        alert_smtp_host=os.getenv("ALERT_SMTP_HOST", "").strip(),
        alert_smtp_port=int(os.getenv("ALERT_SMTP_PORT", "465")),
        alert_smtp_user=os.getenv("ALERT_SMTP_USER", "").strip(),
        alert_smtp_pass=os.getenv("ALERT_SMTP_PASS", ""),
        alert_smtp_from=os.getenv("ALERT_EMAIL_FROM", "").strip(),
        alert_smtp_to=parse_csv(os.getenv("ALERT_EMAIL_TO", "")),
        alert_smtp_use_ssl=parse_bool(os.getenv("ALERT_SMTP_USE_SSL", "1"), True),
        alert_smtp_starttls=parse_bool(os.getenv("ALERT_SMTP_STARTTLS", "0"), False),
        alert_tg_enabled=parse_bool(os.getenv("ALERT_TG_ENABLED", "0"), False),
        alert_tg_bot_token=os.getenv("ALERT_TG_BOT_TOKEN", "").strip(),
        alert_tg_chat_id=os.getenv("ALERT_TG_CHAT_ID", "").strip(),
        alert_tg_api_base=os.getenv("ALERT_TG_API_BASE", "https://api.telegram.org").strip(),
        alert_tg_parse_mode=os.getenv("ALERT_TG_PARSE_MODE", "").strip(),
        alert_max_level=int(os.getenv("ALERT_MAX_LEVEL", "1")),
        alert_intrabar_enabled=parse_bool(os.getenv("ALERT_INTRABAR_ENABLED", "1"), True),
        alert_stats_keep_days=max(1, int(os.getenv("ALERT_STATS_KEEP_DAYS", "14"))),
        alert_local_sound=parse_bool(os.getenv("ALERT_LOCAL_SOUND", "1"), True),
        alert_local_file=parse_bool(os.getenv("ALERT_LOCAL_FILE", "1"), True),
        alert_local_file_path=os.getenv("ALERT_LOCAL_FILE_PATH", default_alert_file).strip(),
        params=params,
    )
    if cfg.pos_mode not in {"net", "long_short"}:
        raise ValueError("OKX_POS_MODE must be net or long_short")
    if cfg.td_mode not in {"cross", "isolated"}:
        raise ValueError("OKX_TD_MODE must be cross or isolated")
    if cfg.sizing_mode not in {"fixed", "margin"}:
        raise ValueError("OKX_SIZING_MODE must be fixed or margin")
    if cfg.alert_max_level < 1:
        cfg.alert_max_level = 1
    if cfg.alert_max_level > 3:
        cfg.alert_max_level = 3
    if cfg.alert_stats_keep_days < 1:
        cfg.alert_stats_keep_days = 1
    return cfg


def _migrate_legacy_state(state: Dict[str, Any], fallback_inst_id: str) -> None:
    if "inst_state" in state and isinstance(state.get("inst_state"), dict):
        return

    legacy_keys = {"last_processed_ts_ms", "trade", "open_entry_ts_ms"}
    if not any(k in state for k in legacy_keys):
        return

    inst_state: Dict[str, Any] = {}
    for k in legacy_keys:
        if k in state:
            inst_state[k] = state.pop(k)
    state["inst_state"] = {fallback_inst_id: inst_state}


def _get_inst_state(state: Dict[str, Any], inst_id: str) -> Dict[str, Any]:
    bucket = state.get("inst_state")
    if not isinstance(bucket, dict):
        bucket = {}
        state["inst_state"] = bucket

    inst_state = bucket.get(inst_id)
    if not isinstance(inst_state, dict):
        inst_state = {}
        bucket[inst_id] = inst_state
    return inst_state


def run_once_for_inst(client: OKXClient, cfg: Config, inst_id: str, inst_state: Dict[str, Any]) -> bool:
    intrabar_mode = cfg.alert_only and cfg.alert_intrabar_enabled
    htf_candles = client.get_candles(inst_id, cfg.htf_bar, cfg.candle_limit)
    loc_candles = client.get_candles(inst_id, cfg.loc_bar, cfg.candle_limit)
    ltf_candles = client.get_candles(inst_id, cfg.ltf_bar, cfg.candle_limit, include_unconfirmed=intrabar_mode)
    if not htf_candles:
        log(f"[{inst_id}] No HTF candle data returned from OKX.")
        return False
    if not loc_candles:
        log(f"[{inst_id}] No LOC candle data returned from OKX.")
        return False
    if not ltf_candles:
        log(f"[{inst_id}] No LTF candle data returned from OKX.")
        return False

    sig = build_signals(htf_candles, loc_candles, ltf_candles, cfg.params)
    last_ts_raw = inst_state.get("last_processed_ts_ms")
    try:
        last_ts = int(last_ts_raw) if last_ts_raw is not None else None
    except (TypeError, ValueError):
        last_ts = None

    signal_ts = int(sig["signal_ts_ms"])
    signal_confirm = bool(sig.get("signal_confirm", True))
    if last_ts is not None and last_ts == signal_ts:
        if (not intrabar_mode) or signal_confirm:
            log(f"[{inst_id}] No new closed candle yet.")
            return False

    now_ms = int(time.time() * 1000)
    bar_s = bar_to_seconds(cfg.ltf_bar)
    if now_ms - signal_ts > bar_s * 1000 * 2:
        log(f"[{inst_id}] Latest closed candle is stale. Skip trading this round.")
        inst_state["last_processed_ts_ms"] = signal_ts
        return False

    _record_opportunity(
        cfg=cfg,
        inst_state=inst_state,
        signal_ts_ms=signal_ts,
        signal_confirm=signal_confirm,
        side="LONG",
        level=int(sig.get("long_level", 0) or 0),
    )
    _record_opportunity(
        cfg=cfg,
        inst_state=inst_state,
        signal_ts_ms=signal_ts,
        signal_confirm=signal_confirm,
        side="SHORT",
        level=int(sig.get("short_level", 0) or 0),
    )

    if cfg.alert_only:
        pos = PositionState("flat", 0.0)
    elif cfg.dry_run and (not cfg.api_key or not cfg.secret_key or not cfg.passphrase):
        log(f"[{inst_id}] Dry-run without API credentials: assume flat position for signal simulation.")
        pos = PositionState("flat", 0.0)
    else:
        rows = client.get_positions(inst_id)
        if cfg.pos_mode == "net":
            has_dual_side = False
            for row in rows:
                pos_side_raw = str(row.get("posSide", "")).strip().lower()
                if pos_side_raw in {"long", "short"}:
                    has_dual_side = True
                    break
            if has_dual_side:
                client.mark_force_pos_side(inst_id)
                if not inst_state.get("warned_pos_mode_mismatch"):
                    log(
                        f"[{inst_id}] Detected dual-side positions while OKX_POS_MODE=net. "
                        "Auto-posSide fallback enabled. Recommend setting OKX_POS_MODE=long_short."
                    )
                    inst_state["warned_pos_mode_mismatch"] = True
        pos = parse_position(rows, cfg.pos_mode)

    execute_decision(client, cfg, inst_id, sig, pos, inst_state)
    inst_state["last_processed_ts_ms"] = signal_ts
    return True


def run_once(client: OKXClient, cfg: Config, state: Dict[str, Any]) -> bool:
    fallback_inst = cfg.inst_ids[0]
    _migrate_legacy_state(state, fallback_inst)

    any_processed = False
    for inst_id in cfg.inst_ids:
        inst_state = _get_inst_state(state, inst_id)
        try:
            processed = run_once_for_inst(client, cfg, inst_id, inst_state)
            any_processed = any_processed or processed
        except Exception as e:
            log(f"[{inst_id}] Instrument loop error: {e}")

    save_state(cfg.state_file, state)
    return any_processed


def print_stats(cfg: Config, state: Dict[str, Any], days: int) -> int:
    if not cfg.inst_ids:
        log("Stats: no instrument configured.")
        return 1

    days = max(1, int(days))
    fallback_inst = cfg.inst_ids[0]
    _migrate_legacy_state(state, fallback_inst)

    log(f"Stats | recent_days={days} keep_days={cfg.alert_stats_keep_days} insts={','.join(cfg.inst_ids)}")
    for inst_id in cfg.inst_ids:
        inst_state = _get_inst_state(state, inst_id)
        daily = inst_state.get("daily_stats")
        if not isinstance(daily, dict) or not daily:
            log(f"[{inst_id}] no stats yet.")
            continue

        day_keys = [k for k in daily.keys() if isinstance(k, str) and len(k) == 10]
        day_keys.sort(reverse=True)
        picked = day_keys[:days]
        if not picked:
            log(f"[{inst_id}] no valid daily buckets.")
            continue

        for day in picked:
            b = daily.get(day, {})
            if not isinstance(b, dict):
                continue
            opp_total = int(b.get("opp_total", 0))
            opp_l1 = int(b.get("opp_l1", 0))
            opp_l2 = int(b.get("opp_l2", 0))
            opp_l3 = int(b.get("opp_l3", 0))
            opp_live = int(b.get("opp_live", 0))
            opp_confirm = int(b.get("opp_confirm", 0))
            opp_long = int(b.get("opp_long", 0))
            opp_short = int(b.get("opp_short", 0))

            alert_total = int(b.get("alert_total", 0))
            alert_l1 = int(b.get("alert_l1", 0))
            alert_l2 = int(b.get("alert_l2", 0))
            alert_l3 = int(b.get("alert_l3", 0))
            alert_live = int(b.get("alert_live", 0))
            alert_confirm = int(b.get("alert_confirm", 0))
            alert_long = int(b.get("alert_long", 0))
            alert_short = int(b.get("alert_short", 0))

            log(
                f"[{inst_id}] {day} | opp={opp_total} (L1/L2/L3={opp_l1}/{opp_l2}/{opp_l3}, "
                f"long/short={opp_long}/{opp_short}, live/confirm={opp_live}/{opp_confirm}) | "
                f"alert={alert_total} (L1/L2/L3={alert_l1}/{alert_l2}/{alert_l3}, "
                f"long/short={alert_long}/{alert_short}, live/confirm={alert_live}/{alert_confirm})"
            )
    return 0


def select_signal_candidate(sig: Dict[str, Any], max_level: int) -> Optional[Tuple[str, int, float]]:
    max_level = max(1, min(3, int(max_level)))
    long_level = int(sig.get("long_level", 0) or 0)
    short_level = int(sig.get("short_level", 0) or 0)
    candidates: List[Tuple[str, int, float]] = []
    if long_level > 0 and long_level <= max_level:
        candidates.append(("LONG", long_level, float(sig["long_stop"])))
    if short_level > 0 and short_level <= max_level:
        candidates.append(("SHORT", short_level, float(sig["short_stop"])))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1])
    return candidates[0]


def eval_signal_outcome(
    side: str,
    entry: float,
    stop: float,
    tp1: float,
    tp2: float,
    ltf_candles: List[Candle],
    start_idx: int,
    horizon_bars: int,
) -> Tuple[str, float, int]:
    risk = abs(entry - stop)
    if risk <= 0:
        risk = max(abs(entry) * 0.0005, 1e-8)

    side_u = side.upper()
    end_idx = min(len(ltf_candles) - 1, start_idx + max(1, int(horizon_bars)))
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
    return outcome, r_value, held


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
    highs = [c.high for c in ltf_candles]
    lows = [c.low for c in ltf_candles]

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

    bb_width: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        up = bb_up[i]
        lo = bb_low[i]
        mid = bb_mid[i]
        if up is None or lo is None or mid is None or mid == 0:
            continue
        bb_width[i] = (up - lo) / mid
    bb_width_avg = _rolling_recent_valid_avg(bb_width, 100)

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
        "highs": highs,
        "lows": lows,
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

    long_rsi_l1 = p.rsi_long_min
    long_rsi_l2 = p.rsi_long_min - p.l2_rsi_relax
    long_rsi_l3 = p.rsi_long_min - p.l3_rsi_relax
    short_rsi_l1 = p.rsi_short_max
    short_rsi_l2 = p.rsi_short_max + p.l2_rsi_relax
    short_rsi_l3 = p.rsi_short_max + p.l3_rsi_relax

    long_entry_l1 = (
        bias == "long"
        and long_location_ok
        and close > float(hhv)
        and close > float(em)
        and vol_ok
        and pullback_long
        and not_chasing_long
        and float(r) > long_rsi_l1
        and float(mh) > 0
    )
    short_entry_l1 = (
        bias == "short"
        and short_location_ok
        and close < float(llv)
        and close < float(em)
        and vol_ok
        and pullback_short
        and not_chasing_short
        and float(r) < short_rsi_l1
        and float(mh) < 0
    )

    long_entry_l2 = (
        bias == "long"
        and long_location_ok
        and close > float(em)
        and pullback_long
        and not_chasing_long
        and float(r) > long_rsi_l2
        and float(mh) >= 0
        and (close > float(hhv) or vol_ok)
    )
    short_entry_l2 = (
        bias == "short"
        and short_location_ok
        and close < float(em)
        and pullback_short
        and not_chasing_short
        and float(r) < short_rsi_l2
        and float(mh) <= 0
        and (close < float(llv) or vol_ok)
    )

    long_entry_l3 = (
        bias == "long"
        and long_location_ok
        and close > float(em)
        and pullback_long
        and not_chasing_long
        and float(r) > long_rsi_l3
    )
    short_entry_l3 = (
        bias == "short"
        and short_location_ok
        and close < float(em)
        and pullback_short
        and not_chasing_short
        and float(r) < short_rsi_l3
    )

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

    long_stop = min(float(exl), float(pb_low), float(em) - (float(a) * p.atr_stop_mult))
    short_stop = max(float(exh), float(pb_high), float(em) + (float(a) * p.atr_stop_mult))
    min_stop_gap = max(float(a) * 0.25, close * 0.0004)
    if long_stop >= close - min_stop_gap:
        long_stop = close - min_stop_gap
    if short_stop <= close + min_stop_gap:
        short_stop = close + min_stop_gap

    return {
        "close": close,
        "long_level": int(long_level),
        "short_level": int(short_level),
        "long_stop": float(long_stop),
        "short_stop": float(short_stop),
    }


def run_backtest(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    max_level: int,
    history_cache: Optional[Dict[str, Tuple[List[Candle], List[Candle], List[Candle]]]] = None,
) -> Dict[str, Any]:
    bars = max(300, int(bars))
    horizon_bars = max(1, int(horizon_bars))
    max_level = max(1, min(3, int(max_level)))
    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)

    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
    need_ltf = bars + 300
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + cfg.params.loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + cfg.params.htf_ema_slow_len + 120

    log(
        f"Backtest start | insts={','.join(inst_ids)} htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} "
        f"bars={bars} horizon={horizon_bars} max_level={max_level}"
    )
    bt_start = time.monotonic()
    inst_total = max(1, len(inst_ids))

    total_signals = 0
    total_r = 0.0
    total_tp1 = 0
    total_tp2 = 0
    total_stop = 0
    total_none = 0
    total_by_level = {1: 0, 2: 0, 3: 0}
    total_by_side = {"LONG": 0, "SHORT": 0}
    per_inst: List[Dict[str, Any]] = []

    for inst_idx, inst_id in enumerate(inst_ids, 1):
        inst_start = time.monotonic()
        cached = history_cache.get(inst_id) if history_cache is not None else None
        if cached is not None:
            htf, loc, ltf = cached
            log(
                f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | "
                f"using cached candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}"
            )
        else:
            log(f"[{inst_id}] backtest begin ({inst_idx}/{inst_total}) | fetching history candles...")
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
                    "elapsed_s": float(time.monotonic() - inst_start),
                }
            )
            continue
        if cached is None:
            log(f"[{inst_id}] history ready | htf={len(htf)} loc={len(loc)} ltf={len(ltf)}")

        try:
            pre = _build_backtest_precalc(htf, loc, ltf, cfg.params)
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
        by_level = {1: 0, 2: 0, 3: 0}
        by_side = {"LONG": 0, "SHORT": 0}
        total_steps = max(1, (len(ltf) - 1) - start_idx)
        next_progress = 10

        for step_idx, i in enumerate(range(start_idx, len(ltf) - 1), 1):
            ts = ltf_ts[i]
            hi = bisect.bisect_right(htf_ts, ts)
            li = bisect.bisect_right(loc_ts, ts)
            if hi > 0 and li > 0:
                sig = _build_backtest_signal_fast(pre, cfg.params, hi, li, i)
                if sig is not None:
                    pick = select_signal_candidate(sig, max_level)
                    if pick:
                        side, level, stop = pick
                        entry = float(sig["close"])
                        risk, tp1, tp2 = compute_alert_targets(
                            side=side,
                            entry_price=entry,
                            stop_price=float(stop),
                            tp1_r=cfg.params.tp1_r_mult,
                            tp2_r=cfg.params.tp2_r_mult,
                        )
                        if risk > 0:
                            outcome, r_value, _ = eval_signal_outcome(
                                side=side,
                                entry=entry,
                                stop=float(stop),
                                tp1=tp1,
                                tp2=tp2,
                                ltf_candles=ltf,
                                start_idx=i,
                                horizon_bars=horizon_bars,
                            )

                            sig_n += 1
                            sum_r += r_value
                            by_level[level] = by_level.get(level, 0) + 1
                            by_side[side] = by_side.get(side, 0) + 1
                            if outcome == "TP2":
                                tp2_n += 1
                                tp1_n += 1
                            elif outcome == "TP1":
                                tp1_n += 1
                            elif outcome == "STOP":
                                stop_n += 1
                            else:
                                none_n += 1

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

        log(
            f"[{inst_id}] backtest | signals={sig_n} L1/L2/L3={by_level.get(1,0)}/{by_level.get(2,0)}/{by_level.get(3,0)} "
            f"long/short={by_side.get('LONG',0)}/{by_side.get('SHORT',0)} "
            f"tp1={tp1_n}({tp1_rate:.1f}%) tp2={tp2_n}({tp2_rate:.1f}%) stop={stop_n}({stop_rate:.1f}%) "
            f"none={none_n} avgR={avg_r:.3f} elapsed={format_duration(time.monotonic() - inst_start)}"
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
                "elapsed_s": float(time.monotonic() - inst_start),
            }
        )

        total_signals += sig_n
        total_r += sum_r
        total_tp1 += tp1_n
        total_tp2 += tp2_n
        total_stop += stop_n
        total_none += none_n
        total_by_level[1] += by_level.get(1, 0)
        total_by_level[2] += by_level.get(2, 0)
        total_by_level[3] += by_level.get(3, 0)
        total_by_side["LONG"] += by_side.get("LONG", 0)
        total_by_side["SHORT"] += by_side.get("SHORT", 0)

    elapsed_total = float(time.monotonic() - bt_start)
    result: Dict[str, Any] = {
        "max_level": max_level,
        "bars": bars,
        "horizon_bars": horizon_bars,
        "inst_ids": list(inst_ids),
        "signals": total_signals,
        "tp1": total_tp1,
        "tp2": total_tp2,
        "stop": total_stop,
        "none": total_none,
        "avg_r": (total_r / total_signals) if total_signals > 0 else 0.0,
        "by_level": dict(total_by_level),
        "by_side": dict(total_by_side),
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
        f"none={total_none} avgR={total_avg_r:.3f} elapsed={format_duration(elapsed_total)}"
    )
    return result


def run_backtest_compare(
    client: OKXClient,
    cfg: Config,
    inst_ids: List[str],
    bars: int,
    horizon_bars: int,
    levels: List[int],
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
            history_cache=cache,
        )
        results.append(one)
    return results


def _rate_str(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0.0%"
    return f"{(numerator / denominator * 100.0):.1f}%"


def _fmt_backtest_result_line(res: Dict[str, Any]) -> str:
    signals = int(res.get("signals", 0))
    tp1 = int(res.get("tp1", 0))
    tp2 = int(res.get("tp2", 0))
    stop = int(res.get("stop", 0))
    none = int(res.get("none", 0))
    avg_r = float(res.get("avg_r", 0.0))
    by_level = res.get("by_level", {}) if isinstance(res.get("by_level"), dict) else {}
    by_side = res.get("by_side", {}) if isinstance(res.get("by_side"), dict) else {}
    elapsed = float(res.get("elapsed_s", 0.0))
    max_level = int(res.get("max_level", 0))
    return (
        f"L{max_level} | signals={signals} L1/L2/L3={int(by_level.get(1,0))}/{int(by_level.get(2,0))}/{int(by_level.get(3,0))} "
        f"long/short={int(by_side.get('LONG',0))}/{int(by_side.get('SHORT',0))} "
        f"tp1={tp1}({_rate_str(tp1, signals)}) tp2={tp2}({_rate_str(tp2, signals)}) "
        f"stop={stop}({_rate_str(stop, signals)}) none={none} avgR={avg_r:.3f} "
        f"elapsed={format_duration(elapsed)}"
    )


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
        inst_ids = first.get("inst_ids", [])
        inst_txt = ",".join(inst_ids) if isinstance(inst_ids, list) and inst_ids else "-"
        lines.append(f"样本：bars={bars} horizon={horizon} insts={inst_txt}")

        for res in results:
            lines.append(_fmt_backtest_result_line(res))
            per_inst = res.get("per_inst", [])
            if not isinstance(per_inst, list):
                continue
            for row in per_inst:
                if not isinstance(row, dict):
                    continue
                inst_id = str(row.get("inst_id", ""))
                status = str(row.get("status", "ok"))
                if status != "ok":
                    err = str(row.get("error", "unknown"))[:120]
                    lines.append(f"- {inst_id} 数据异常: {err}")
                    continue
                sig_n = int(row.get("signals", 0))
                avg_r = float(row.get("avg_r", 0.0))
                stop_n = int(row.get("stop", 0))
                tp2_n = int(row.get("tp2", 0))
                lines.append(
                    f"- {inst_id}: signals={sig_n} tp2={tp2_n}({_rate_str(tp2_n, sig_n)}) "
                    f"stop={stop_n}({_rate_str(stop_n, sig_n)}) avgR={avg_r:.3f}"
                )
    else:
        lines.append("无可用回测结果。")

    return truncate_text("\n".join(lines), limit=3800)


def main() -> int:
    parser = argparse.ArgumentParser(description="OKX adaptive auto trader")
    parser.add_argument("--env", default=os.path.join(os.path.dirname(__file__), "okx_auto_trader.env"))
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
    parser.add_argument("--test-alert", action="store_true", help="Send one test alert and exit")
    parser.add_argument("--test-inst", default="", help="Instrument name used by --test-alert")
    parser.add_argument("--stats", action="store_true", help="Print daily alert/opportunity stats and exit")
    parser.add_argument("--stats-days", default="3", help="How many recent days to print for --stats")
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest and exit")
    parser.add_argument("--bt-bars", default="1200", help="LTF bars to evaluate in backtest")
    parser.add_argument("--bt-horizon-bars", default="24", help="Forward bars for signal outcome evaluation")
    parser.add_argument(
        "--bt-max-level",
        default="0",
        help="Max alert level to evaluate in backtest (0 uses ALERT_MAX_LEVEL, 1~3 override)",
    )
    parser.add_argument(
        "--bt-compare-levels",
        default="",
        help="Comma-separated levels to compare sequentially (e.g. 1,2); overrides --bt-max-level",
    )
    parser.add_argument(
        "--bt-inst-ids",
        default="",
        help="Comma-separated instruments for backtest (empty uses OKX_INST_IDS/OKX_INST_ID)",
    )
    parser.add_argument(
        "--bt-send-telegram",
        action="store_true",
        help="Send backtest summary to Telegram after run",
    )
    parser.add_argument(
        "--bt-title",
        default="",
        help="Optional title for backtest summary message",
    )
    parser.add_argument("--state-file", default=None, help="Override state file path")
    args = parser.parse_args()

    load_dotenv(args.env)
    cfg = read_config(args.state_file)
    if args.test_alert:
        return run_test_alert(cfg, args.test_inst)

    if args.stats:
        state = load_state(cfg.state_file)
        try:
            days = int(args.stats_days)
        except Exception:
            days = 3
        return print_stats(cfg, state, days)

    if args.backtest:
        client = OKXClient(cfg)
        try:
            bars = int(args.bt_bars)
        except Exception:
            bars = 1200
        try:
            horizon = int(args.bt_horizon_bars)
        except Exception:
            horizon = 24
        try:
            max_level_cli = int(args.bt_max_level)
        except Exception:
            max_level_cli = 0
        inst_ids = parse_inst_ids(args.bt_inst_ids) or cfg.inst_ids
        compare_levels = parse_backtest_levels(args.bt_compare_levels)

        results: List[Dict[str, Any]] = []
        if compare_levels:
            results = run_backtest_compare(client, cfg, inst_ids, bars, horizon, compare_levels)
        else:
            max_level = cfg.alert_max_level if max_level_cli <= 0 else max_level_cli
            one = run_backtest(client, cfg, inst_ids, bars, horizon, max_level)
            results = [one]

        if args.bt_send_telegram:
            text = build_backtest_telegram_summary(cfg, results, args.bt_title)
            sent = send_telegram(cfg, text)
            log(f"Backtest summary telegram_sent={sent}")
            if not sent:
                return 1
        return 0

    client = OKXClient(cfg)
    state = load_state(cfg.state_file)

    insts_display = ",".join(cfg.inst_ids)
    log(
        f"Start | insts={insts_display} htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} dry_run={cfg.dry_run} "
        f"paper={cfg.paper} pos_mode={cfg.pos_mode} td_mode={cfg.td_mode} "
        f"sizing_mode={cfg.sizing_mode} order_size={round_size(cfg.order_size)} "
        f"margin={cfg.margin_usdt} leverage={cfg.leverage} "
        f"open_limit={cfg.params.max_open_entries}/{cfg.params.open_window_hours}h "
        f"alert_only={cfg.alert_only} email_enabled={cfg.alert_email_enabled} "
        f"tg_enabled={cfg.alert_tg_enabled} max_level={cfg.alert_max_level} "
        f"intrabar={cfg.alert_intrabar_enabled} stats_keep_days={cfg.alert_stats_keep_days} "
        f"local_sound={cfg.alert_local_sound} local_file={cfg.alert_local_file}"
    )

    try:
        if args.once:
            try:
                run_once(client, cfg, state)
                return 0
            except Exception as e:
                log(f"Run error: {e}")
                return 1

        while True:
            try:
                run_once(client, cfg, state)
            except Exception as e:
                log(f"Loop error: {e}")
            time.sleep(cfg.poll_seconds)
    except KeyboardInterrupt:
        log("Stopped by user.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
