from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, List, Optional

from .backtest_tables import (
    _build_backtest_alignment_counts,
    _build_backtest_precalc,
    _build_backtest_signal_decision_tables,
    _build_backtest_signal_live_window,
)
from .common import bar_to_seconds
from .config import get_strategy_params, get_strategy_profile_id, get_strategy_profile_ids, resolve_exec_max_level
from .models import Candle, Config, StrategyParams
from .profile_vote import merge_entry_votes
from .signals import build_signals


def _fmt_ts(ts_ms: int) -> str:
    return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


def _values_equal(left: Any, right: Any, *, tol: float = 1e-9) -> bool:
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return bool(left) is bool(right)
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        lf = float(left)
        rf = float(right)
        if math.isnan(lf) and math.isnan(rf):
            return True
        return math.isclose(lf, rf, rel_tol=tol, abs_tol=tol)
    return left == right


def _diff_signal_fields(
    runtime_signal: Optional[Dict[str, Any]],
    backtest_signal: Optional[Dict[str, Any]],
    *,
    label: str,
) -> List[str]:
    if runtime_signal is None and backtest_signal is None:
        return []
    if runtime_signal is None:
        return [f"{label}:missing_in_runtime"]
    if backtest_signal is None:
        return [f"{label}:missing_in_backtest"]

    diffs: List[str] = []
    keys = set(runtime_signal.keys()) | set(backtest_signal.keys())
    for key in sorted(keys):
        if not _values_equal(runtime_signal.get(key), backtest_signal.get(key)):
            diffs.append(f"{label}:{key}")
    return diffs


def _ordered_profile_ids(cfg: Config, inst_id: str) -> List[str]:
    profile_ids = get_strategy_profile_ids(cfg, inst_id)
    if not profile_ids:
        profile_ids = [get_strategy_profile_id(cfg, inst_id)]
    primary = profile_ids[0]
    ordered = [primary]
    for pid in profile_ids[1:]:
        if pid not in ordered:
            ordered.append(pid)
    return ordered


def _profile_params(cfg: Config, inst_id: str, ordered_profile_ids: List[str]) -> Dict[str, StrategyParams]:
    primary = ordered_profile_ids[0]
    default_params = get_strategy_params(cfg, inst_id)
    out: Dict[str, StrategyParams] = {primary: cfg.strategy_profiles.get(primary, default_params)}
    for pid in ordered_profile_ids[1:]:
        out[pid] = cfg.strategy_profiles.get(pid, cfg.params)
    return out


def _build_runtime_like_signal(
    *,
    cfg: Config,
    inst_id: str,
    ordered_profile_ids: List[str],
    params_by_profile: Dict[str, StrategyParams],
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    hi: int,
    li: int,
    i: int,
) -> Optional[Dict[str, Any]]:
    if hi <= 0 or li <= 0:
        return None

    primary = ordered_profile_ids[0]
    primary_params = params_by_profile[primary]
    primary_exec_max = resolve_exec_max_level(primary_params, inst_id)
    vote_enabled = len(ordered_profile_ids) > 1

    sig = _build_backtest_signal_live_window(
        htf_candles=htf_candles,
        loc_candles=loc_candles,
        ltf_candles=ltf_candles,
        p=primary_params,
        hi=hi,
        li=li,
        i=i,
        candle_limit=cfg.candle_limit,
        build_signals_fn=build_signals,
    )
    if sig is None:
        return None

    if not vote_enabled:
        return sig

    from .backtest_tables import _resolve_signal_entry_decision

    signals_by_profile: Dict[str, Dict[str, Any]] = {primary: sig}
    decisions_by_profile: Dict[str, Optional[Any]] = {
        primary: _resolve_signal_entry_decision(
            sig,
            ltf_candles=ltf_candles,
            ltf_i=i,
            p=primary_params,
            exec_max_level=primary_exec_max,
            min_level=1,
            exact_level=0,
            tp1_only=False,
        )
    }
    for pid in ordered_profile_ids[1:]:
        p = params_by_profile[pid]
        one_sig = _build_backtest_signal_live_window(
            htf_candles=htf_candles,
            loc_candles=loc_candles,
            ltf_candles=ltf_candles,
            p=p,
            hi=hi,
            li=li,
            i=i,
            candle_limit=cfg.candle_limit,
            build_signals_fn=build_signals,
        )
        if one_sig is None:
            continue
        signals_by_profile[pid] = one_sig
        decisions_by_profile[pid] = _resolve_signal_entry_decision(
            one_sig,
            ltf_candles=ltf_candles,
            ltf_i=i,
            p=p,
            exec_max_level=resolve_exec_max_level(p, inst_id),
            min_level=1,
            exact_level=0,
            tp1_only=False,
        )

    sig, _vote_meta = merge_entry_votes(
        base_signal=sig,
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
    return sig


def build_signal_parity_report(
    *,
    cfg: Config,
    inst_id: str,
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    bars: int,
    compare_fast: bool = False,
    max_mismatches: int = 200,
) -> Dict[str, Any]:
    ordered_profile_ids = _ordered_profile_ids(cfg, inst_id)
    primary = ordered_profile_ids[0]
    params_by_profile = _profile_params(cfg, inst_id, ordered_profile_ids)

    htf_ts = [int(c.ts_ms) for c in htf_candles]
    loc_ts = [int(c.ts_ms) for c in loc_candles]
    ltf_ts = [int(c.ts_ms) for c in ltf_candles]
    start_idx = max(0, len(ltf_candles) - max(1, int(bars)))

    live_table = _build_backtest_signal_decision_tables(
        cfg=cfg,
        inst_id=inst_id,
        profile_id=primary,
        inst_profile_ids=ordered_profile_ids,
        params_by_profile=params_by_profile,
        pre_by_profile={},
        htf_candles=htf_candles,
        loc_candles=loc_candles,
        ltf_candles=ltf_candles,
        htf_ts=htf_ts,
        loc_ts=loc_ts,
        ltf_ts=ltf_ts,
        max_level=3,
        min_level=1,
        exact_level=0,
        tp1_only=False,
        start_idx=start_idx,
        live_signal_window_limit=cfg.candle_limit,
        build_signals_fn=build_signals,
    )
    fast_table: Optional[Dict[str, Any]] = None
    if compare_fast:
        pre_by_profile = {
            pid: _build_backtest_precalc(htf_candles, loc_candles, ltf_candles, params_by_profile[pid])
            for pid in ordered_profile_ids
        }
        fast_table = _build_backtest_signal_decision_tables(
            cfg=cfg,
            inst_id=inst_id,
            profile_id=primary,
            inst_profile_ids=ordered_profile_ids,
            params_by_profile=params_by_profile,
            pre_by_profile=pre_by_profile,
            htf_candles=htf_candles,
            loc_candles=loc_candles,
            ltf_candles=ltf_candles,
            htf_ts=htf_ts,
            loc_ts=loc_ts,
            ltf_ts=ltf_ts,
            max_level=3,
            min_level=1,
            exact_level=0,
            tp1_only=False,
            start_idx=start_idx,
            live_signal_window_limit=0,
            build_signals_fn=build_signals,
        )

    htf_counts, loc_counts = _build_backtest_alignment_counts(
        htf_ts,
        loc_ts,
        ltf_ts,
        htf_bar_ms=bar_to_seconds(cfg.htf_bar) * 1000,
        loc_bar_ms=bar_to_seconds(cfg.loc_bar) * 1000,
        ltf_bar_ms=bar_to_seconds(cfg.ltf_bar) * 1000,
        start_idx=start_idx,
    )

    mismatches: List[Dict[str, Any]] = []
    runtime_live_mismatch_count = 0
    runtime_fast_mismatch_count = 0
    runtime_live_mismatch_fields = 0
    runtime_fast_mismatch_fields = 0
    compared_bars = 0

    for i in range(start_idx, len(ltf_candles)):
        runtime_signal = _build_runtime_like_signal(
            cfg=cfg,
            inst_id=inst_id,
            ordered_profile_ids=ordered_profile_ids,
            params_by_profile=params_by_profile,
            htf_candles=htf_candles,
            loc_candles=loc_candles,
            ltf_candles=ltf_candles,
            hi=htf_counts[i],
            li=loc_counts[i],
            i=i,
        )
        live_signal = live_table["signal_table"][i]
        fast_signal = fast_table["signal_table"][i] if fast_table is not None else None

        live_diffs = _diff_signal_fields(runtime_signal, live_signal, label="runtime_vs_live")
        fast_diffs = _diff_signal_fields(runtime_signal, fast_signal, label="runtime_vs_fast") if fast_table else []

        compared_bars += 1
        if live_diffs:
            runtime_live_mismatch_count += 1
            runtime_live_mismatch_fields += len(live_diffs)
        if fast_diffs:
            runtime_fast_mismatch_count += 1
            runtime_fast_mismatch_fields += len(fast_diffs)

        if not live_diffs and not fast_diffs:
            continue
        if len(mismatches) >= max_mismatches:
            continue

        mismatches.append(
            {
                "inst_id": inst_id,
                "ltf_index": int(i),
                "signal_ts_ms": int(ltf_ts[i]),
                "signal_ts_utc": _fmt_ts(int(ltf_ts[i])),
                "runtime_vs_live": live_diffs,
                "runtime_vs_fast": fast_diffs,
                "runtime_signal": runtime_signal,
                "live_signal": live_signal,
                "fast_signal": fast_signal,
            }
        )

    return {
        "inst_id": inst_id,
        "profile_ids": ordered_profile_ids,
        "bars_requested": int(bars),
        "bars_compared": int(compared_bars),
        "candle_limit": int(cfg.candle_limit),
        "runtime_live_mismatch_count": int(runtime_live_mismatch_count),
        "runtime_fast_mismatch_count": int(runtime_fast_mismatch_count),
        "runtime_live_mismatch_fields": int(runtime_live_mismatch_fields),
        "runtime_fast_mismatch_fields": int(runtime_fast_mismatch_fields),
        "compare_fast": bool(compare_fast),
        "mismatches": mismatches,
        "start_ts_ms": int(ltf_ts[start_idx]) if start_idx < len(ltf_ts) else 0,
        "end_ts_ms": int(ltf_ts[-1]) if ltf_ts else 0,
    }
