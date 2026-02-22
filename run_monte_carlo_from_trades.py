#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
from typing import Any, Dict, List, Sequence


def percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if q <= 0:
        return float(sorted_vals[0])
    if q >= 1:
        return float(sorted_vals[-1])
    n = len(sorted_vals)
    pos = (n - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    w = pos - lo
    return float(sorted_vals[lo]) * (1.0 - w) + float(sorted_vals[hi]) * w


def summarize(vals: Sequence[float]) -> Dict[str, float]:
    if not vals:
        return {"p05": 0.0, "p50": 0.0, "p95": 0.0}
    s = sorted(float(x) for x in vals)
    return {
        "p05": percentile(s, 0.05),
        "p50": percentile(s, 0.50),
        "p95": percentile(s, 0.95),
    }


def simulate_path(
    *,
    rs: Sequence[float],
    rng: random.Random,
    n: int,
    start_equity: float,
    risk_frac: float,
    block_size: int,
) -> Dict[str, float]:
    if n <= 0 or not rs:
        return {"final": start_equity, "ret_pct": 0.0, "max_dd": 0.0, "worst_ls": 0.0, "best_ws": 0.0}

    picks: List[float] = []
    if block_size <= 1:
        picks = [float(rs[rng.randrange(len(rs))]) for _ in range(n)]
    else:
        m = len(rs)
        bs = max(1, int(block_size))
        while len(picks) < n:
            j = rng.randrange(m)
            end = min(m, j + bs)
            picks.extend(float(x) for x in rs[j:end])
        if len(picks) > n:
            picks = picks[:n]

    equity = float(start_equity)
    peak = equity
    max_dd = 0.0
    cur_ls = 0
    worst_ls = 0
    cur_ws = 0
    best_ws = 0
    rf = float(risk_frac)

    for r in picks:
        step_mult = 1.0 + rf * float(r)
        if step_mult < 0.0:
            step_mult = 0.0
        equity *= step_mult

        if equity > peak:
            peak = equity
        if peak > 0:
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd

        if r < 0:
            cur_ls += 1
            if cur_ls > worst_ls:
                worst_ls = cur_ls
            cur_ws = 0
        elif r > 0:
            cur_ws += 1
            if cur_ws > best_ws:
                best_ws = cur_ws
            cur_ls = 0
        else:
            cur_ls = 0
            cur_ws = 0

    ret_pct = (equity / float(start_equity) - 1.0) * 100.0 if start_equity > 0 else 0.0
    return {
        "final": float(equity),
        "ret_pct": float(ret_pct),
        "max_dd": float(max_dd),
        "worst_ls": float(worst_ls),
        "best_ws": float(best_ws),
    }


def run_historical_order(rs: Sequence[float], start_equity: float, risk_frac: float) -> Dict[str, float]:
    equity = float(start_equity)
    peak = equity
    max_dd = 0.0
    cur_ls = 0
    worst_ls = 0
    cur_ws = 0
    best_ws = 0
    rf = float(risk_frac)
    for r in rs:
        equity *= max(0.0, 1.0 + rf * float(r))
        if equity > peak:
            peak = equity
        if peak > 0:
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        if r < 0:
            cur_ls += 1
            worst_ls = max(worst_ls, cur_ls)
            cur_ws = 0
        elif r > 0:
            cur_ws += 1
            best_ws = max(best_ws, cur_ws)
            cur_ls = 0
        else:
            cur_ls = 0
            cur_ws = 0
    ret_pct = (equity / float(start_equity) - 1.0) * 100.0 if start_equity > 0 else 0.0
    return {
        "final": equity,
        "ret_pct": ret_pct,
        "max_dd": max_dd,
        "worst_ls": float(worst_ls),
        "best_ws": float(best_ws),
    }


def run_realized_pnl_order(pnls: Sequence[float], start_equity: float) -> Dict[str, float]:
    equity = float(start_equity)
    peak = equity
    max_dd = 0.0
    cur_ls = 0
    worst_ls = 0
    cur_ws = 0
    best_ws = 0
    for pnl in pnls:
        equity += float(pnl)
        if equity > peak:
            peak = equity
        if peak > 0:
            dd = (peak - equity) / peak
            if dd > max_dd:
                max_dd = dd
        if pnl < 0:
            cur_ls += 1
            worst_ls = max(worst_ls, cur_ls)
            cur_ws = 0
        elif pnl > 0:
            cur_ws += 1
            best_ws = max(best_ws, cur_ws)
            cur_ls = 0
        else:
            cur_ls = 0
            cur_ws = 0
    ret_pct = (equity / float(start_equity) - 1.0) * 100.0 if start_equity > 0 else 0.0
    return {
        "final": equity,
        "ret_pct": ret_pct,
        "max_dd": max_dd,
        "worst_ls": float(worst_ls),
        "best_ws": float(best_ws),
    }


def load_trades(path: str, min_level: int, max_level: int) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                lv = int(float(row.get("level", "0") or 0))
            except Exception:
                lv = 0
            if lv < min_level or lv > max_level:
                continue
            try:
                r = float(row.get("r", "0") or 0.0)
                r_raw = float(row.get("r_raw", "0") or 0.0)
                pnl = float(row.get("pnl", "0") or 0.0)
            except Exception:
                continue
            out.append({"r": r, "r_raw": r_raw, "pnl": pnl})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Monte Carlo simulation from backtest trade CSV")
    parser.add_argument("--trades-csv", required=True, help="Input trades CSV from run_interleaved_backtest_2y.py")
    parser.add_argument("--field", default="r", choices=["r", "r_raw"], help="R field to simulate")
    parser.add_argument("--paths", type=int, default=5000, help="Number of MC paths")
    parser.add_argument("--n-trades", type=int, default=0, help="Override number of trades per path (0=use sample size)")
    parser.add_argument("--risk-frac", type=float, default=0.005, help="Risk fraction per trade")
    parser.add_argument("--start-equity", type=float, default=1000.0, help="Starting equity")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--block-size", type=int, default=1, help="Bootstrap block size (1=IID resample)")
    parser.add_argument("--min-level", type=int, default=1, help="Min signal level to include")
    parser.add_argument("--max-level", type=int, default=3, help="Max signal level to include")
    parser.add_argument("--dd-threshold", type=float, default=0.30, help="Drawdown threshold for probability summary")
    args = parser.parse_args()

    trades = load_trades(args.trades_csv, int(args.min_level), int(args.max_level))
    rs = [float(t.get(args.field, 0.0)) for t in trades]
    pnls = [float(t.get("pnl", 0.0)) for t in trades]
    if not rs:
        print("No trades found after filters.")
        return 2

    n = int(args.n_trades) if int(args.n_trades) > 0 else len(rs)
    paths = max(1, int(args.paths))
    rng = random.Random(int(args.seed))

    finals: List[float] = []
    returns: List[float] = []
    dds: List[float] = []
    worst_lss: List[float] = []
    best_wss: List[float] = []

    for _ in range(paths):
        one = simulate_path(
            rs=rs,
            rng=rng,
            n=n,
            start_equity=float(args.start_equity),
            risk_frac=float(args.risk_frac),
            block_size=int(args.block_size),
        )
        finals.append(float(one["final"]))
        returns.append(float(one["ret_pct"]))
        dds.append(float(one["max_dd"]))
        worst_lss.append(float(one["worst_ls"]))
        best_wss.append(float(one["best_ws"]))

    hist_r = run_historical_order(rs, float(args.start_equity), float(args.risk_frac))
    hist_pnl = run_realized_pnl_order(pnls, float(args.start_equity))
    s_final = summarize(finals)
    s_ret = summarize(returns)
    s_dd = summarize(dds)
    s_ls = summarize(worst_lss)
    s_ws = summarize(best_wss)

    dd_th = max(0.0, min(1.0, float(args.dd_threshold)))
    prob_dd_over = sum(1 for x in dds if x >= dd_th) / float(paths) * 100.0
    prob_neg = sum(1 for x in returns if x < 0.0) / float(paths) * 100.0

    print("=== Monte Carlo (from trade R) ===")
    print(
        f"sample_trades={len(rs)} n_trades_per_path={n} paths={paths} "
        f"risk={float(args.risk_frac)*100:.2f}% field={args.field} block_size={int(args.block_size)}"
    )
    print(
        f"historical_csv_pnl: final={hist_pnl['final']:.2f} return={hist_pnl['ret_pct']:.2f}% "
        f"maxDD={hist_pnl['max_dd']*100:.2f}% worst_ls={int(hist_pnl['worst_ls'])} best_ws={int(hist_pnl['best_ws'])}"
    )
    print(
        f"historical_seq_r  : final={hist_r['final']:.2f} return={hist_r['ret_pct']:.2f}% "
        f"maxDD={hist_r['max_dd']*100:.2f}% worst_ls={int(hist_r['worst_ls'])} best_ws={int(hist_r['best_ws'])}"
    )
    print(
        f"MC final equity   : p05={s_final['p05']:.2f} p50={s_final['p50']:.2f} p95={s_final['p95']:.2f}"
    )
    print(f"MC return (%)     : p05={s_ret['p05']:.2f}% p50={s_ret['p50']:.2f}% p95={s_ret['p95']:.2f}%")
    print(f"MC maxDD (%)      : p05={s_dd['p05']*100:.2f}% p50={s_dd['p50']*100:.2f}% p95={s_dd['p95']*100:.2f}%")
    print(
        f"MC worst_ls       : p05={s_ls['p05']:.0f} p50={s_ls['p50']:.0f} p95={s_ls['p95']:.0f} | "
        f"MC best_ws: p05={s_ws['p05']:.0f} p50={s_ws['p50']:.0f} p95={s_ws['p95']:.0f}"
    )
    print(f"MC P(return<0)={prob_neg:.2f}% | MC P(maxDD>={dd_th*100:.1f}%)={prob_dd_over:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
