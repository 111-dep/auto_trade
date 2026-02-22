#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import datetime as dt
import math
import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from okx_trader.backtest import _build_backtest_precalc, _build_backtest_signal_fast
from okx_trader.common import bar_to_seconds, load_dotenv
from okx_trader.config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    read_config,
    resolve_exec_max_level,
)
from okx_trader.decision_core import resolve_entry_decision
from okx_trader.okx_client import OKXClient
from okx_trader.profile_vote import merge_entry_votes


def _fmt_ts(ts_ms: int) -> str:
    return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Strict recent-window signal check (common synchronized end across instruments)."
    )
    parser.add_argument(
        "--env",
        default="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env",
        help="Path to env file.",
    )
    parser.add_argument("--hours", type=float, default=20.0, help="Window size in hours (default: 20).")
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable history cache and force live fetch from OKX.",
    )
    parser.add_argument(
        "--show-events",
        type=int,
        default=30,
        help="Max event lines to print (0 to disable).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.hours <= 0:
        print("--hours must be > 0")
        return 2

    load_dotenv(args.env)
    if bool(args.no_cache):
        os.environ["OKX_HISTORY_CACHE_ENABLED"] = "0"

    cfg = read_config(None)
    client = OKXClient(cfg)

    inst_ids = list(cfg.inst_ids)
    if not inst_ids:
        print("No inst ids configured.")
        return 1

    profile_ids_by_inst: Dict[str, List[str]] = {inst: get_strategy_profile_ids(cfg, inst) for inst in inst_ids}
    profile_by_inst: Dict[str, str] = {
        inst: (ids[0] if ids else get_strategy_profile_id(cfg, inst))
        for inst, ids in profile_ids_by_inst.items()
    }
    params_by_inst = {
        inst: cfg.strategy_profiles.get(profile_by_inst[inst], get_strategy_params(cfg, inst))
        for inst in inst_ids
    }

    all_params: List[Any] = []
    for inst in inst_ids:
        ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        max_loc_lookback = cfg.params.loc_lookback
        max_htf_ema_slow = cfg.params.htf_ema_slow_len

    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)
    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))

    # Keep extra warmup bars so indicator state is stable.
    window_ltf_bars = int(math.ceil(float(args.hours) * 3600.0 / float(ltf_s))) + 2
    need_ltf = max(450, window_ltf_bars + 360)
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

    print("=== Strict Recent Signal Check ===")
    print(f"insts={','.join(inst_ids)}")
    print(
        f"window={args.hours:.2f}h ltf={cfg.ltf_bar} loc={cfg.loc_bar} htf={cfg.htf_bar} "
        f"no_cache={bool(args.no_cache)}"
    )
    print(f"need candles: htf={need_htf} loc={need_loc} ltf={need_ltf}")

    data: Dict[str, Dict[str, Any]] = {}
    for idx, inst in enumerate(inst_ids, 1):
        inst_params = params_by_inst.get(inst, cfg.params)
        inst_profile_ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        if profile_by_inst.get(inst, "DEFAULT") not in inst_profile_ids:
            inst_profile_ids = [profile_by_inst.get(inst, "DEFAULT")] + [
                x for x in inst_profile_ids if x != profile_by_inst.get(inst, "DEFAULT")
            ]
        inst_exec_max_level = resolve_exec_max_level(inst_params, inst)
        print(f"[{idx}/{len(inst_ids)}] fetch {inst} ...")
        try:
            htf = client.get_candles_history(inst, cfg.htf_bar, need_htf)
            loc = client.get_candles_history(inst, cfg.loc_bar, need_loc)
            ltf = client.get_candles_history(inst, cfg.ltf_bar, need_ltf)
        except Exception as e:
            print(f"[{inst}] fetch failed: {e}")
            continue

        if len(htf) < 50 or len(loc) < 120 or len(ltf) < 300:
            print(f"[{inst}] skip short data htf={len(htf)} loc={len(loc)} ltf={len(ltf)}")
            continue
        print(f"[{inst}] candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}")

        pre_by_profile: Dict[str, Dict[str, Any]] = {}
        try:
            pre_by_profile[profile_by_inst.get(inst, "DEFAULT")] = _build_backtest_precalc(htf, loc, ltf, inst_params)
            for pid in inst_profile_ids:
                if pid == profile_by_inst.get(inst, "DEFAULT"):
                    continue
                pp = cfg.strategy_profiles.get(pid, cfg.params)
                pre_by_profile[pid] = _build_backtest_precalc(htf, loc, ltf, pp)
        except Exception as e:
            print(f"[{inst}] precalc failed: {e}")
            continue

        data[inst] = {
            "params": inst_params,
            "profile_id": profile_by_inst.get(inst, "DEFAULT"),
            "profile_ids": inst_profile_ids,
            "vote_enabled": bool(len(inst_profile_ids) > 1),
            "exec_max_level": int(inst_exec_max_level),
            "pre_by_profile": pre_by_profile,
            "htf_ts": [c.ts_ms for c in htf],
            "loc_ts": [c.ts_ms for c in loc],
            "ltf_ts": [c.ts_ms for c in ltf],
        }

    if not data:
        print("No instrument has enough history for check.")
        return 1

    common_end = min(int(v["ltf_ts"][-1]) for v in data.values())
    start_ts = int(common_end - float(args.hours) * 3600.0 * 1000.0)
    now_ms = int(time.time() * 1000)
    lag_m = max(0.0, (now_ms - common_end) / 60000.0)

    print(f"common_window: {_fmt_ts(start_ts)} -> {_fmt_ts(common_end)}")
    print(f"common_end_lag={lag_m:.1f}m from now")
    print("note: eval=bars with indicator evaluation, raw=bars with L1/L2/L3 signal before vote")
    for inst in inst_ids:
        one = data.get(inst)
        if one is None:
            continue
        last_ts = int(one["ltf_ts"][-1])
        print(f"last[{inst}]={_fmt_ts(last_ts)}")

    per_inst = defaultdict(
        lambda: {
            "bars": 0,
            "evaluated": 0,
            "raw_signal": 0,
            "raw_long": 0,
            "raw_short": 0,
            "decision": 0,
            "long": 0,
            "short": 0,
            "l1": 0,
            "l2": 0,
            "l3": 0,
            "vote_enabled": 0,
            "vote_none": 0,
        }
    )
    total = {
        "bars": 0,
        "evaluated": 0,
        "raw_signal": 0,
        "decision": 0,
        "long": 0,
        "short": 0,
        "l1": 0,
        "l2": 0,
        "l3": 0,
        "vote_none": 0,
    }
    events: List[str] = []

    for inst in inst_ids:
        row = data.get(inst)
        if row is None:
            continue
        inst_params = row["params"]
        inst_profile = row["profile_id"]
        inst_exec_max = int(row["exec_max_level"])
        htf_ts = row["htf_ts"]
        loc_ts = row["loc_ts"]
        ltf_ts = row["ltf_ts"]
        pstat = per_inst[inst]

        for i, ts in enumerate(ltf_ts):
            ts_i = int(ts)
            if ts_i < start_ts or ts_i > common_end:
                continue
            pstat["bars"] += 1
            total["bars"] += 1

            hi = bisect.bisect_right(htf_ts, ts_i)
            li = bisect.bisect_right(loc_ts, ts_i)
            if hi <= 0 or li <= 0:
                continue

            sig = _build_backtest_signal_fast(row["pre_by_profile"][inst_profile], inst_params, hi, li, i)
            if sig is None:
                continue
            pstat["evaluated"] += 1
            total["evaluated"] += 1
            raw_long_lv = int(sig.get("long_level", 0) or 0)
            raw_short_lv = int(sig.get("short_level", 0) or 0)
            if raw_long_lv > 0:
                pstat["raw_long"] += 1
            if raw_short_lv > 0:
                pstat["raw_short"] += 1
            if raw_long_lv > 0 or raw_short_lv > 0:
                pstat["raw_signal"] += 1
                total["raw_signal"] += 1

            if bool(row["vote_enabled"]):
                pstat["vote_enabled"] += 1
                signals_by_profile: Dict[str, Dict[str, Any]] = {inst_profile: sig}
                decisions_by_profile: Dict[str, Any] = {}
                decisions_by_profile[inst_profile] = resolve_entry_decision(
                    sig,
                    max_level=inst_exec_max,
                    min_level=1,
                    exact_level=0,
                    tp1_r=inst_params.tp1_r_mult,
                    tp2_r=inst_params.tp2_r_mult,
                    tp1_only=False,
                )
                for pid in row.get("profile_ids", []):
                    if pid == inst_profile:
                        continue
                    pre_other = row["pre_by_profile"].get(pid)
                    if pre_other is None:
                        continue
                    p = cfg.strategy_profiles.get(pid, cfg.params)
                    sig_other = _build_backtest_signal_fast(pre_other, p, hi, li, i)
                    if sig_other is None:
                        continue
                    signals_by_profile[pid] = sig_other
                    decisions_by_profile[pid] = resolve_entry_decision(
                        sig_other,
                        max_level=resolve_exec_max_level(p, inst),
                        min_level=1,
                        exact_level=0,
                        tp1_r=p.tp1_r_mult,
                        tp2_r=p.tp2_r_mult,
                        tp1_only=False,
                    )
                sig, vote_meta = merge_entry_votes(
                    base_signal=sig,
                    profile_ids=[pid for pid in row.get("profile_ids", []) if pid in signals_by_profile],
                    signals_by_profile=signals_by_profile,
                    decisions_by_profile=decisions_by_profile,
                    mode=cfg.strategy_profile_vote_mode,
                    min_agree=cfg.strategy_profile_vote_min_agree,
                    enforce_max_level=inst_exec_max,
                    profile_score_map=cfg.strategy_profile_vote_score_map,
                    level_weight=cfg.strategy_profile_vote_level_weight,
                )
                if str(vote_meta.get("winner_side", "NONE")).upper() == "NONE":
                    pstat["vote_none"] += 1
                    total["vote_none"] += 1

            decision = resolve_entry_decision(
                sig,
                max_level=inst_exec_max,
                min_level=1,
                exact_level=0,
                tp1_r=inst_params.tp1_r_mult,
                tp2_r=inst_params.tp2_r_mult,
                tp1_only=False,
            )
            if decision is None:
                continue

            side = str(decision.side).upper()
            level = int(decision.level)
            pstat["decision"] += 1
            total["decision"] += 1
            if side == "LONG":
                pstat["long"] += 1
                total["long"] += 1
            elif side == "SHORT":
                pstat["short"] += 1
                total["short"] += 1
            if level == 1:
                pstat["l1"] += 1
                total["l1"] += 1
            elif level == 2:
                pstat["l2"] += 1
                total["l2"] += 1
            elif level == 3:
                pstat["l3"] += 1
                total["l3"] += 1

            if args.show_events > 0 and len(events) < int(args.show_events):
                events.append(
                    f"{_fmt_ts(ts_i)} {inst} side={side} L{level} "
                    f"vote={sig.get('vote_winner', 'N/A')} profile={sig.get('vote_winner_profile', '-')}"
                )

    print("--- per inst ---")
    for inst in inst_ids:
        st = per_inst.get(inst)
        if st is None:
            continue
        print(
            f"{inst}: bars={st['bars']} eval={st['evaluated']} raw={st['raw_signal']} "
            f"(rawL={st['raw_long']} rawS={st['raw_short']}) "
            f"decision={st['decision']} long/short={st['long']}/{st['short']} "
            f"L1/L2/L3={st['l1']}/{st['l2']}/{st['l3']} "
            f"voteNone={st['vote_none']}"
        )

    print("--- total ---")
    print(
        f"bars={total['bars']} eval={total['evaluated']} raw={total['raw_signal']} decision={total['decision']} "
        f"long/short={total['long']}/{total['short']} "
        f"L1/L2/L3={total['l1']}/{total['l2']}/{total['l3']} "
        f"voteNone={total['vote_none']}"
    )

    if args.show_events > 0:
        print("--- sample events ---")
        if not events:
            print("(none)")
        else:
            for line in events:
                print(line)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
