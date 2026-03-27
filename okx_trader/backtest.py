from __future__ import annotations

import datetime as dt
import math
import time
from typing import Any, Dict, List, Optional, Tuple

from .backtest_cache import (
    _backtest_live_table_cache_enabled,
    _backtest_live_table_cache_path,
    _build_backtest_live_table_cache_key,
    _load_backtest_live_table_cache,
    _save_backtest_live_table_cache,
)
from .backtest_outcome import (
    _close_remaining_r,
    _signal_high_low,
    _simulate_live_managed_step,
    eval_signal_outcome,
    resolve_backtest_split_tp_enabled,
)
from .backtest_report import (
    finalize_level_perf,
    format_backtest_inst_line,
    format_backtest_result_line,
    level_perf_brief,
    new_level_perf,
    rate_str,
    update_level_perf,
)
from .backtest_tables import (
    _build_backtest_alignment_counts as _build_backtest_alignment_counts_impl,
    _build_backtest_precalc as _build_backtest_precalc_impl,
    _build_backtest_signal_decision_tables as _build_backtest_signal_decision_tables_impl,
    _build_backtest_signal_fast as _build_backtest_signal_fast_impl,
    _build_backtest_signal_live_window as _build_backtest_signal_live_window_impl,
)
from .common import bar_to_seconds, format_duration, log, make_progress_bar, truncate_text
from .config import get_strategy_params, get_strategy_profile_id, get_strategy_profile_ids
from .models import Candle, Config, StrategyParams
from .okx_client import OKXClient
from .pa_oral_baseline import is_pa_oral_baseline_variant
from .signals import build_signals


def _build_backtest_precalc(
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    p: StrategyParams,
) -> Dict[str, Any]:
    return _build_backtest_precalc_impl(htf_candles, loc_candles, ltf_candles, p)


def _build_backtest_signal_fast(
    pre: Dict[str, Any],
    p: StrategyParams,
    hi: int,
    li: int,
    i: int,
) -> Optional[Dict[str, Any]]:
    return _build_backtest_signal_fast_impl(pre, p, hi, li, i)


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
) -> Optional[Dict[str, Any]]:
    return _build_backtest_signal_live_window_impl(
        htf_candles=htf_candles,
        loc_candles=loc_candles,
        ltf_candles=ltf_candles,
        p=p,
        hi=hi,
        li=li,
        i=i,
        candle_limit=candle_limit,
        build_signals_fn=build_signals,
    )


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
    return _build_backtest_alignment_counts_impl(
        htf_ts,
        loc_ts,
        ltf_ts,
        htf_bar_ms=htf_bar_ms,
        loc_bar_ms=loc_bar_ms,
        ltf_bar_ms=ltf_bar_ms,
        start_idx=start_idx,
    )


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
) -> Dict[str, Any]:
    # Keep this wrapper in backtest.py so tests can patch okx_trader.backtest.build_signals
    # and still observe the live-window path through the public backtest facade.
    return _build_backtest_signal_decision_tables_impl(
        cfg=cfg,
        inst_id=inst_id,
        profile_id=profile_id,
        inst_profile_ids=inst_profile_ids,
        params_by_profile=params_by_profile,
        pre_by_profile=pre_by_profile,
        htf_candles=htf_candles,
        loc_candles=loc_candles,
        ltf_candles=ltf_candles,
        htf_ts=htf_ts,
        loc_ts=loc_ts,
        ltf_ts=ltf_ts,
        max_level=max_level,
        min_level=min_level,
        exact_level=exact_level,
        tp1_only=tp1_only,
        start_idx=start_idx,
        live_signal_window_limit=live_signal_window_limit,
        build_signals_fn=build_signals,
    )


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
    bt_force_managed_tp_fallback: bool = False,
    bt_live_window_signals: bool = False,
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
    bt_force_managed_tp_fallback = bool(bt_force_managed_tp_fallback)
    bt_live_window_signals = bool(bt_live_window_signals)
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
        f"require_tp_sl={bt_require_tp_sl} tp1_only={bt_tp1_only} managed_exit={bt_managed_exit} "
        f"live_window_signals={bt_live_window_signals}"
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
        needs_oral_precalc = any(
            is_pa_oral_baseline_variant(getattr(cfg.strategy_profiles.get(pid, inst_params), "strategy_variant", ""))
            for pid in inst_profile_ids
        )
        if (not bt_live_window_signals) or needs_oral_precalc:
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

        start_idx = max(0, len(ltf) - bars)
        bt_live_split_tp = bool(
            bt_managed_exit
            and resolve_backtest_split_tp_enabled(
                attach_tpsl_on_entry=bool(getattr(cfg, "attach_tpsl_on_entry", False)),
                enable_close=bool(getattr(inst_params, "enable_close", False)),
                split_tp_on_entry=bool(getattr(inst_params, "split_tp_on_entry", False)),
                tp1_close_pct=float(getattr(inst_params, "tp1_close_pct", 0.0) or 0.0),
                force_managed_tp_fallback=bt_force_managed_tp_fallback,
            )
        )
        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]
        params_by_profile: Dict[str, StrategyParams] = {profile_id: inst_params}
        for pid in inst_profile_ids:
            if pid == profile_id:
                continue
            params_by_profile[pid] = cfg.strategy_profiles.get(pid, cfg.params)
        try:
            table_bundle = _build_backtest_signal_decision_tables(
                cfg=cfg,
                inst_id=inst_id,
                profile_id=profile_id,
                inst_profile_ids=inst_profile_ids,
                params_by_profile=params_by_profile,
                pre_by_profile=pre_by_profile,
                htf_candles=htf,
                loc_candles=loc,
                ltf_candles=ltf,
                htf_ts=htf_ts,
                loc_ts=loc_ts,
                ltf_ts=ltf_ts,
                max_level=max_level,
                min_level=min_level,
                exact_level=exact_level,
                tp1_only=bt_tp1_only,
                start_idx=start_idx,
                live_signal_window_limit=cfg.candle_limit if bt_live_window_signals else 0,
            )
        except Exception as e:
            msg = f"table build failed: {e}"
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
        signal_table = table_bundle["signal_table"]
        decision_table = table_bundle["decision_table"]

        def _signal_lookup(ltf_i: int) -> Optional[Dict[str, Any]]:
            if ltf_i < 0 or ltf_i >= len(ltf):
                return None
            return signal_table[ltf_i]

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
                sig = signal_table[i]
                decision = decision_table[i]
                if sig is not None and decision is not None:
                    side = decision.side
                    level = int(decision.level)
                    stop = float(decision.stop)
                    entry = float(decision.entry)
                    risk = float(decision.risk)
                    tp1 = float(decision.tp1)
                    tp2 = float(decision.tp2)
                    entry_idx = int(getattr(decision, "entry_idx", i) or i)
                    include_start_bar = bool(getattr(decision, "include_start_bar", False))
                    max_hold_bars = int(getattr(decision, "max_hold_bars", 0) or 0)
                    entry_ts = ltf_ts[entry_idx] if 0 <= entry_idx < len(ltf_ts) else ts
                    tp1_close_pct_eff = float(sig.get("tp1_close_pct_override", inst_params.tp1_close_pct) or inst_params.tp1_close_pct)
                    tp2_close_rest_eff = bool(sig.get("tp2_close_rest_override", inst_params.tp2_close_rest))
                    be_trigger_r_mult_eff = float(
                        sig.get("be_trigger_r_mult_override", inst_params.be_trigger_r_mult) or inst_params.be_trigger_r_mult
                    )
                    trail_after_tp1_eff = bool(sig.get("trail_after_tp1_override", inst_params.trail_after_tp1))
                    auto_tighten_stop_eff = bool(sig.get("auto_tighten_stop_override", inst_params.auto_tighten_stop))
                    signal_exit_enabled_eff = bool(
                        sig.get("signal_exit_enabled_override", inst_params.signal_exit_enabled)
                    )
                    if risk > 0:
                        if bt_min_open_interval_minutes > 0 and last_open_ts_ms is not None:
                            if entry_ts - last_open_ts_ms < bt_min_open_interval_minutes * 60 * 1000:
                                skip_gap_n += 1
                                goto_progress = True
                            else:
                                goto_progress = False
                        else:
                            goto_progress = False
                        if not goto_progress:
                            day_key = dt.datetime.utcfromtimestamp(entry_ts / 1000).strftime("%Y-%m-%d")
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
                                    start_idx=entry_idx,
                                    horizon_bars=horizon_bars,
                                    managed_exit=bt_managed_exit,
                                    tp1_close_pct=tp1_close_pct_eff,
                                    tp2_close_rest=tp2_close_rest_eff,
                                    be_trigger_r_mult=be_trigger_r_mult_eff,
                                    be_offset_pct=inst_params.be_offset_pct,
                                    be_fee_buffer_pct=inst_params.be_fee_buffer_pct,
                                    signal_lookup=_signal_lookup if bt_managed_exit else None,
                                    trail_after_tp1=trail_after_tp1_eff,
                                    auto_tighten_stop=auto_tighten_stop_eff,
                                    trail_atr_mult=inst_params.trail_atr_mult,
                                    signal_exit_enabled=signal_exit_enabled_eff,
                                    split_tp_enabled=bt_live_split_tp,
                                    include_start_bar=include_start_bar,
                                    max_hold_bars=max_hold_bars,
                                )
                                if bt_tp1_only and outcome == "TP2":
                                    outcome = "TP1"
                                if bt_require_tp_sl and outcome not in {"TP1", "TP2", "STOP", "TIME"}:
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
                                    last_open_ts_ms = entry_ts
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
        "bt_live_window_signals": bt_live_window_signals,
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
    bt_live_window_signals: bool = False,
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
            bt_live_window_signals=bt_live_window_signals,
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
