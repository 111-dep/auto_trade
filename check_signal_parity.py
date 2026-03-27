#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from typing import Any, Dict, List

from okx_trader.client_factory import create_client
from okx_trader.common import bar_to_seconds, load_dotenv, parse_inst_ids
from okx_trader.config import get_strategy_params, get_strategy_profile_id, get_strategy_profile_ids, read_config
from okx_trader.signal_parity import build_signal_parity_report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check runtime-like signal parity against backtest signal tables.")
    parser.add_argument(
        "--env",
        default="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env",
        help="Path to env file.",
    )
    parser.add_argument(
        "--inst-ids",
        default="",
        help="Comma-separated instruments. Empty means using env instruments.",
    )
    parser.add_argument(
        "--bars",
        type=int,
        default=500,
        help="How many latest LTF bars to compare.",
    )
    parser.add_argument(
        "--compare-fast",
        action="store_true",
        help="Also compare runtime-like signals with the fast backtest path.",
    )
    parser.add_argument(
        "--show-mismatches",
        type=int,
        default=20,
        help="How many mismatch rows to print per instrument.",
    )
    parser.add_argument(
        "--json-out",
        default="",
        help="Optional output path for full JSON report.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable history cache before fetching candles.",
    )
    return parser.parse_args()


def _calc_need_sizes(cfg: Any, inst_ids: List[str], bars: int) -> tuple[int, int, int]:
    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)
    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))

    all_params = []
    for inst in inst_ids:
        ids = get_strategy_profile_ids(cfg, inst) or [get_strategy_profile_id(cfg, inst)]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        one = cfg.params
        max_loc_lookback = one.loc_lookback
        max_htf_ema_slow = one.htf_ema_slow_len

    need_ltf = max(cfg.candle_limit + 50, int(bars) + cfg.candle_limit + 20)
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120
    return need_htf, need_loc, need_ltf


def _print_report(report: Dict[str, Any], show_mismatches: int) -> None:
    inst_id = str(report.get("inst_id", ""))
    live_mismatch = int(report.get("runtime_live_mismatch_count", 0))
    fast_mismatch = int(report.get("runtime_fast_mismatch_count", 0))
    live_mismatch_fields = int(report.get("runtime_live_mismatch_fields", 0))
    fast_mismatch_fields = int(report.get("runtime_fast_mismatch_fields", 0))
    bars_compared = int(report.get("bars_compared", 0))
    candle_limit = int(report.get("candle_limit", 0))
    compare_fast = bool(report.get("compare_fast", False))
    status = "PASS" if live_mismatch == 0 else "FAIL"
    extra = (
        f" runtime_vs_fast_bars={fast_mismatch}/{bars_compared} runtime_vs_fast_fields={fast_mismatch_fields}"
        if compare_fast
        else ""
    )
    print(
        f"[{inst_id}] {status} bars={bars_compared} candle_limit={candle_limit} "
        f"runtime_vs_live_bars={live_mismatch}/{bars_compared} runtime_vs_live_fields={live_mismatch_fields}{extra}",
        flush=True,
    )
    if show_mismatches <= 0:
        return
    for row in report.get("mismatches", [])[:show_mismatches]:
        print(
            f"  - {row['signal_ts_utc']} idx={row['ltf_index']} "
            f"runtime_vs_live={row['runtime_vs_live']} runtime_vs_fast={row['runtime_vs_fast']}",
            flush=True,
        )


if __name__ == "__main__":
    args = _parse_args()
    load_dotenv(args.env)
    if args.no_cache:
        os.environ["OKX_HISTORY_CACHE_ENABLED"] = "0"

    cfg = read_config(None)
    client = create_client(cfg)
    inst_ids = parse_inst_ids(args.inst_ids) or list(cfg.inst_ids)
    if not inst_ids:
        raise SystemExit("No instruments configured.")

    need_htf, need_loc, need_ltf = _calc_need_sizes(cfg, inst_ids, args.bars)
    print(
        f"signal_parity start insts={','.join(inst_ids)} bars={args.bars} "
        f"candle_limit={cfg.candle_limit} compare_fast={bool(args.compare_fast)} "
        f"need_candles htf={need_htf} loc={need_loc} ltf={need_ltf}",
        flush=True,
    )

    reports: List[Dict[str, Any]] = []
    for inst in inst_ids:
        params = get_strategy_params(cfg, inst)
        profile_ids = get_strategy_profile_ids(cfg, inst) or [get_strategy_profile_id(cfg, inst)]
        print(
            f"[{inst}] fetch htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} "
            f"profile={profile_ids[0]} variant={params.strategy_variant}",
            flush=True,
        )
        htf = client.get_candles_history(inst, cfg.htf_bar, need_htf)
        loc = client.get_candles_history(inst, cfg.loc_bar, need_loc)
        ltf = client.get_candles_history(inst, cfg.ltf_bar, need_ltf)
        report = build_signal_parity_report(
            cfg=cfg,
            inst_id=inst,
            htf_candles=htf,
            loc_candles=loc,
            ltf_candles=ltf,
            bars=args.bars,
            compare_fast=bool(args.compare_fast),
        )
        reports.append(report)
        _print_report(report, args.show_mismatches)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump({"reports": reports}, f, ensure_ascii=False, indent=2)
        print(f"json_saved={args.json_out}", flush=True)
