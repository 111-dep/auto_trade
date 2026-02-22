#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import lzma
import os
import struct
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Sequence, Tuple

from okx_trader.models import Candle


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build XAU proxy cache from Dukascopy XAUUSD minute candles")
    p.add_argument("--inst-id", default="XAU-USDT-SWAP", help="Target OKX inst id cache key")
    p.add_argument("--symbol", default="XAUUSD", help="Dukascopy symbol")
    p.add_argument("--start", default="2024-02-20", help="Start date YYYY-MM-DD (UTC)")
    p.add_argument("--end", default=dt.datetime.utcnow().strftime("%Y-%m-%d"), help="End date YYYY-MM-DD (UTC)")
    p.add_argument(
        "--cache-dir",
        default="/home/dandan/Workspace/test/okx_trade_suite/okx_trader/.cache/history_proxy_gold",
        help="Output cache dir",
    )
    p.add_argument("--scale", type=float, default=1000.0, help="Price integer divisor for Dukascopy records")
    p.add_argument("--workers", type=int, default=8, help="Parallel download workers")
    p.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=2, help="Retries per day on transient failure")
    return p.parse_args()


def day_range(start_day: dt.date, end_day: dt.date) -> Iterable[dt.date]:
    cur = start_day
    one = dt.timedelta(days=1)
    while cur <= end_day:
        yield cur
        cur += one


def dukascopy_day_url(symbol: str, day: dt.date) -> str:
    # Dukascopy month folder is zero-based, day folder is one-based.
    y = day.year
    m0 = day.month - 1
    dd = day.day
    return f"https://datafeed.dukascopy.com/datafeed/{symbol}/{y}/{m0:02d}/{dd:02d}/BID_candles_min_1.bi5"


def fetch_one_day(
    symbol: str,
    day: dt.date,
    *,
    timeout: float,
    retries: int,
    scale: float,
) -> Tuple[dt.date, List[Candle], str]:
    url = dukascopy_day_url(symbol, day)
    last_err = ""
    for attempt in range(max(1, int(retries) + 1)):
        try:
            raw = urllib.request.urlopen(url, timeout=float(timeout)).read()
            blob = lzma.decompress(raw)
            if len(blob) % 24 != 0:
                return day, [], f"bad_blob_len={len(blob)}"
            day_ts_ms = int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp() * 1000)
            out: List[Candle] = []
            for i in range(0, len(blob), 24):
                # Dukascopy candle offset here is in seconds-from-day-start.
                sec_off, o, c, l, h, v = struct.unpack(">IIIII f", blob[i : i + 24])
                ts_ms = int(day_ts_ms + int(sec_off) * 1000)
                out.append(
                    Candle(
                        ts_ms=ts_ms,
                        open=float(o) / float(scale),
                        high=float(h) / float(scale),
                        low=float(l) / float(scale),
                        close=float(c) / float(scale),
                        confirm=True,
                        volume=float(v),
                    )
                )
            out.sort(key=lambda x: x.ts_ms)
            return day, out, ""
        except urllib.error.HTTPError as e:
            if int(getattr(e, "code", 0) or 0) == 404:
                return day, [], "404"
            last_err = f"http_{getattr(e, 'code', 'na')}"
        except Exception as e:
            last_err = str(e)
        if attempt < int(retries):
            time.sleep(0.35 * (2**attempt))
    return day, [], last_err or "fetch_failed"


def resample(candles: Sequence[Candle], tf_seconds: int) -> List[Candle]:
    if not candles:
        return []
    out: List[Candle] = []
    cur_bucket = None
    o = h = l = c = v = 0.0
    for k in sorted(candles, key=lambda x: x.ts_ms):
        sec = int(k.ts_ms // 1000)
        b = (sec // int(tf_seconds)) * int(tf_seconds)
        if cur_bucket is None:
            cur_bucket = b
            o = float(k.open)
            h = float(k.high)
            l = float(k.low)
            c = float(k.close)
            v = float(k.volume)
            continue
        if b != cur_bucket:
            out.append(
                Candle(
                    ts_ms=int(cur_bucket * 1000),
                    open=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    confirm=True,
                    volume=float(v),
                )
            )
            cur_bucket = b
            o = float(k.open)
            h = float(k.high)
            l = float(k.low)
            c = float(k.close)
            v = float(k.volume)
        else:
            if float(k.high) > h:
                h = float(k.high)
            if float(k.low) < l:
                l = float(k.low)
            c = float(k.close)
            v += float(k.volume)
    if cur_bucket is not None:
        out.append(
            Candle(
                ts_ms=int(cur_bucket * 1000),
                open=float(o),
                high=float(h),
                low=float(l),
                close=float(c),
                confirm=True,
                volume=float(v),
            )
        )
    return out


def save_cache(path: str, inst_id: str, bar: str, candles: Sequence[Candle], source: Dict[str, str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    payload = {
        "inst_id": str(inst_id).upper(),
        "bar": str(bar),
        "saved_ts": int(time.time()),
        "source": source,
        "candles": [[c.ts_ms, c.open, c.high, c.low, c.close, c.volume, True] for c in candles],
    }
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def main() -> int:
    args = parse_args()
    start_day = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    end_day = dt.datetime.strptime(args.end, "%Y-%m-%d").date()
    if end_day < start_day:
        print("end must be >= start")
        return 2

    days = list(day_range(start_day, end_day))
    got_days = 0
    miss_404 = 0
    miss_err = 0
    all_m1: List[Candle] = []

    print(
        f"[XAU proxy] symbol={args.symbol} inst={args.inst_id} range={start_day}..{end_day} days={len(days)} "
        f"workers={args.workers}",
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as ex:
        futs = [
            ex.submit(
                fetch_one_day,
                args.symbol,
                d,
                timeout=float(args.timeout),
                retries=int(args.retries),
                scale=float(args.scale),
            )
            for d in days
        ]
        for i, fut in enumerate(as_completed(futs), 1):
            day, rows, err = fut.result()
            if rows:
                got_days += 1
                all_m1.extend(rows)
            else:
                if err == "404":
                    miss_404 += 1
                else:
                    miss_err += 1
                    print(f"[WARN] {day} {err}", flush=True)
            if i % 100 == 0 or i == len(days):
                print(
                    f"progress {i}/{len(days)} got_days={got_days} miss404={miss_404} missErr={miss_err} m1={len(all_m1)}",
                    flush=True,
                )

    if not all_m1:
        print("no minute candles downloaded")
        return 1

    all_m1.sort(key=lambda x: x.ts_ms)
    bars_15m = resample(all_m1, 15 * 60)
    bars_1h = resample(all_m1, 60 * 60)
    bars_4h = resample(all_m1, 4 * 60 * 60)

    inst_key = str(args.inst_id).upper()
    out_dir = str(args.cache_dir)
    source = {
        "provider": "dukascopy",
        "symbol": str(args.symbol),
        "start": str(start_day),
        "end": str(end_day),
    }
    save_cache(os.path.join(out_dir, f"{inst_key}__15m.json"), inst_key, "15m", bars_15m, source)
    save_cache(os.path.join(out_dir, f"{inst_key}__1H.json"), inst_key, "1H", bars_1h, source)
    save_cache(os.path.join(out_dir, f"{inst_key}__4H.json"), inst_key, "4H", bars_4h, source)

    print(
        f"done cache_dir={out_dir} m1={len(all_m1)} 15m={len(bars_15m)} 1H={len(bars_1h)} 4H={len(bars_4h)} "
        f"got_days={got_days} miss404={miss_404} missErr={miss_err}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
