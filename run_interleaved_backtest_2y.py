#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import os
import pickle
import time
import traceback
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from okx_trader.alerts import send_telegram
from okx_trader.backtest import (
    _build_backtest_precalc,
    _build_backtest_signal_decision_tables,
    _simulate_live_managed_step,
    resolve_backtest_split_tp_enabled,
)
from okx_trader.backtest_report import (
    finalize_level_perf,
    format_backtest_result_line,
    new_level_perf,
    normalize_backtest_result,
    update_level_perf,
)
from okx_trader.client_factory import create_client
from okx_trader.common import apply_backtest_env_overrides, bar_to_seconds, load_dotenv, parse_bool
from okx_trader.config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    read_config,
    resolve_exec_max_level,
)
from okx_trader.decision_core import resolve_entry_decision
from okx_trader.entry_exec_policy import (
    normalize_entry_exec_mode,
    normalize_entry_limit_fallback_mode,
    resolve_entry_exec_mode,
    resolve_entry_limit_fallback_mode_for_params,
)
from okx_trader.risk_guard import (
    is_open_limit_reached,
    min_open_gap_remaining_minutes,
    normalize_loss_base_mode,
    prune_loss_deque_window,
    prune_ts_deque_window,
    resolve_loss_base,
)
from okx_trader.tp2_reentry_guard import arm_tp2_reentry_bucket, get_tp2_reentry_gate


def ts_fmt(ms: int) -> str:
    return dt.datetime.utcfromtimestamp(int(ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")


def clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0
    if v < 0:
        return 0.0
    if v > 1:
        return 1.0
    return v



def stable_u01(key: str) -> float:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    n = int.from_bytes(h, byteorder="big", signed=False)
    return n / float(2**64)


_INTERLEAVED_TRACE_CACHE_VERSION = "interleaved_trace_v1"


def _interleaved_trace_cache_enabled() -> bool:
    raw = str(os.getenv("OKX_BACKTEST_INTERLEAVED_TRACE_CACHE_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _interleaved_trace_cache_dir(cfg: Any) -> str:
    override = str(os.getenv("OKX_BACKTEST_INTERLEAVED_TRACE_CACHE_DIR", "") or "").strip()
    if override:
        return override
    hist_dir = str(getattr(cfg, "history_cache_dir", "") or "").strip()
    if hist_dir:
        return os.path.join(os.path.dirname(os.path.abspath(hist_dir.rstrip(os.sep))), "backtest_interleaved_traces")
    return os.path.join(os.getcwd(), ".cache", "backtest_interleaved_traces")


def _interleaved_trace_cache_key(
    *,
    table_cache_key: str,
    params: Any,
    managed_exit: bool,
    use_signal_exit: bool,
    exit_model: str,
    split_tp_enabled: bool,
) -> str:
    payload = {
        "version": _INTERLEAVED_TRACE_CACHE_VERSION,
        "table_cache_key": str(table_cache_key or ""),
        "managed_exit": bool(managed_exit),
        "use_signal_exit": bool(use_signal_exit),
        "exit_model": str(exit_model or "standard"),
        "split_tp_enabled": bool(split_tp_enabled),
        "params": {
            "tp1_close_pct": float(getattr(params, "tp1_close_pct", 0.0) or 0.0),
            "tp2_close_rest": bool(getattr(params, "tp2_close_rest", False)),
            "be_trigger_r_mult": float(getattr(params, "be_trigger_r_mult", 0.0) or 0.0),
            "be_offset_pct": float(getattr(params, "be_offset_pct", 0.0) or 0.0),
            "be_fee_buffer_pct": float(getattr(params, "be_fee_buffer_pct", 0.0) or 0.0),
            "auto_tighten_stop": bool(getattr(params, "auto_tighten_stop", False)),
            "trail_after_tp1": bool(getattr(params, "trail_after_tp1", False)),
            "trail_atr_mult": float(getattr(params, "trail_atr_mult", 0.0) or 0.0),
        },
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _interleaved_trace_cache_path(cfg: Any, inst_id: str, trace_cache_key: str) -> str:
    root = _interleaved_trace_cache_dir(cfg)
    safe_inst = str(inst_id).replace("/", "_").replace("-", "_")
    return os.path.join(root, f"{safe_inst}__{trace_cache_key}.pkl")


def _load_interleaved_trace_cache(path: str, trace_cache_key: str, *, inst_id: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "rb") as fh:
            payload = pickle.load(fh)
    except Exception as e:
        print(f"[{inst_id}] trace cache load failed: {e}", flush=True)
        return {}
    if not isinstance(payload, dict) or payload.get("trace_cache_key") != trace_cache_key:
        return {}
    data = payload.get("trajectories")
    if not isinstance(data, dict):
        return {}
    print(f"[{inst_id}] trace cache hit | items={len(data)} key={trace_cache_key[:12]} path={path}", flush=True)
    return data


def _save_interleaved_trace_cache(path: str, trace_cache_key: str, *, inst_id: str, trajectories: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "wb") as fh:
            pickle.dump({
                "trace_cache_key": trace_cache_key,
                "trajectories": trajectories,
            }, fh, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp_path, path)
        print(f"[{inst_id}] trace cache save | items={len(trajectories)} key={trace_cache_key[:12]} path={path}", flush=True)
    except Exception as e:
        print(f"[{inst_id}] trace cache save failed: {e}", flush=True)


def _trajectory_candidate_key(*, entry_i: int, decision: Any, exit_model: str, split_tp_enabled: bool) -> str:
    raw = (
        f"{int(entry_i)}|{str(getattr(decision, 'side', '')).upper()}|{int(getattr(decision, 'level', 0) or 0)}|"
        f"{repr(float(getattr(decision, 'entry', 0.0) or 0.0))}|{repr(float(getattr(decision, 'stop', 0.0) or 0.0))}|"
        f"{repr(float(getattr(decision, 'tp1', 0.0) or 0.0))}|{repr(float(getattr(decision, 'tp2', 0.0) or 0.0))}|"
        f"{str(exit_model or 'standard')}|{1 if bool(split_tp_enabled) else 0}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _tracked_position_state(pos: Dict[str, Any]) -> Tuple[float, float, bool, bool, float]:
    return (
        float(pos.get("qty_rem", 0.0) or 0.0),
        float(pos.get("realized_r", 0.0) or 0.0),
        bool(pos.get("tp1_done", False)),
        bool(pos.get("be_armed", False)),
        float(pos.get("hard_stop", pos.get("stop", 0.0)) or 0.0),
    )


def _build_interleaved_trade_trajectory(
    *,
    ltf_ts: List[int],
    signal_table: List[Optional[Dict[str, Any]]],
    decision_table: List[Optional[Any]],
    params: Any,
    entry_i: int,
    decision: Any,
    managed_exit: bool,
    use_signal_exit: bool,
    exit_model: str,
    split_tp_enabled: bool,
    swap_stop_r_mult: float = 0.0,
) -> List[Dict[str, Any]]:
    pos = _new_sim_position(
        decision=decision,
        entry_ts=int(ltf_ts[entry_i]),
        entry_i=int(entry_i),
        risk_amt=1.0,
        exit_model=exit_model,
        swap_stop_r_mult=float(swap_stop_r_mult),
    )
    pos["exchange_split_tp_enabled"] = bool(split_tp_enabled)
    out: List[Dict[str, Any]] = []
    for i in range(int(entry_i) + 1, max(int(entry_i) + 1, len(signal_table) - 1)):
        sig = signal_table[i]
        if sig is None:
            continue
        before = _tracked_position_state(pos)
        res = _simulate_live_position_step(
            pos=pos,
            sig=sig,
            params=params,
            decision=decision_table[i],
            allow_reverse=False,
            managed_exit=managed_exit,
            use_signal_exit=use_signal_exit,
        )
        after = _tracked_position_state(pos)
        if bool(res.get("closed", False)) or after != before:
            out.append(
                {
                    "i": int(i),
                    "qty_rem": float(pos.get("qty_rem", 0.0) or 0.0),
                    "realized_r": float(pos.get("realized_r", 0.0) or 0.0),
                    "tp1_done": bool(pos.get("tp1_done", False)),
                    "be_armed": bool(pos.get("be_armed", False)),
                    "hard_stop": float(pos.get("hard_stop", pos.get("stop", 0.0)) or 0.0),
                    "closed": bool(res.get("closed", False)),
                    "outcome": str(res.get("outcome", "NONE") or "NONE"),
                    "r_raw": float(res.get("r_raw", 0.0) or 0.0),
                    "is_stop": bool(res.get("is_stop", False)),
                }
            )
        if bool(res.get("closed", False)):
            break
    return out


def _apply_cached_trade_trajectory_step(*, pos: Dict[str, Any], current_i: int, decision: Optional[Any], allow_reverse: bool) -> Dict[str, Any]:
    events = pos.get("_trajectory_events")
    if not isinstance(events, list):
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}
    ptr = int(pos.get("_trajectory_ptr", 0) or 0)
    if ptr >= len(events):
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}
    ev = events[ptr]
    if int(ev.get("i", -1) or -1) != int(current_i):
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    pos["qty_rem"] = float(ev.get("qty_rem", pos.get("qty_rem", 0.0)) or 0.0)
    pos["realized_r"] = float(ev.get("realized_r", pos.get("realized_r", 0.0)) or 0.0)
    pos["tp1_done"] = bool(ev.get("tp1_done", pos.get("tp1_done", False)))
    pos["be_armed"] = bool(ev.get("be_armed", pos.get("be_armed", False)))
    pos["hard_stop"] = float(ev.get("hard_stop", pos.get("hard_stop", pos.get("stop", 0.0))) or 0.0)
    pos["_trajectory_ptr"] = ptr + 1

    closed = bool(ev.get("closed", False))
    reverse_decision = None
    if closed and allow_reverse and (decision is not None):
        side_u = str(pos.get("side", "")).strip().upper()
        want_side = "SHORT" if side_u == "LONG" else "LONG"
        if str(getattr(decision, "side", "")).upper() == want_side:
            reverse_decision = decision
    return {
        "closed": closed,
        "outcome": str(ev.get("outcome", "NONE") or "NONE"),
        "r_raw": float(ev.get("r_raw", 0.0) or 0.0),
        "is_stop": bool(ev.get("is_stop", False)),
        "reverse_decision": reverse_decision,
    }


def _select_post_close_open_decision(
    *,
    sim: Dict[str, Any],
    decision: Optional[Any],
    reopen_on_close_same_bar: bool,
) -> Tuple[Optional[Any], bool]:
    reverse_decision = sim.get("reverse_decision")
    if reverse_decision is not None:
        return reverse_decision, True
    if reopen_on_close_same_bar and decision is not None:
        return decision, False
    return None, False


def _get_or_build_interleaved_trade_trajectory(
    *,
    row: Dict[str, Any],
    entry_i: int,
    decision: Any,
    params: Any,
    managed_exit: bool,
    use_signal_exit: bool,
    exit_model: str,
    split_tp_enabled: bool,
    swap_stop_r_mult: float = 0.0,
) -> Optional[List[Dict[str, Any]]]:
    trace_store = row.get("trace_cache_store")
    if not isinstance(trace_store, dict):
        return None
    candidate_key = _trajectory_candidate_key(
        entry_i=entry_i,
        decision=decision,
        exit_model=exit_model,
        split_tp_enabled=split_tp_enabled,
    )
    cached = trace_store.get(candidate_key)
    if isinstance(cached, list):
        return cached
    trajectory = _build_interleaved_trade_trajectory(
        ltf_ts=row["ltf_ts"],
        signal_table=row["signal_table"],
        decision_table=row["decision_table"],
        params=params,
        entry_i=entry_i,
        decision=decision,
        managed_exit=managed_exit,
        use_signal_exit=use_signal_exit,
        exit_model=exit_model,
        split_tp_enabled=split_tp_enabled,
        swap_stop_r_mult=swap_stop_r_mult,
    )
    trace_store[candidate_key] = trajectory
    row["trace_cache_dirty"] = True
    return trajectory


def apply_execution_penalty_r(
    *,
    r_raw: float,
    outcome: str,
    entry: float,
    stop: float,
    fee_rate: float,
    slippage_bps: float,
    stop_extra_r: float,
    tp_haircut_r: float,
) -> Tuple[float, float]:
    entry_px = max(1e-12, float(entry))
    stop_pct = abs(float(entry) - float(stop)) / entry_px
    stop_pct = max(stop_pct, 1e-6)
    fee = max(0.0, float(fee_rate))
    slip_one_way = max(0.0, float(slippage_bps)) / 10000.0
    friction_r = (fee + 2.0 * slip_one_way) / stop_pct

    r_adj = float(r_raw) - friction_r
    if outcome == "STOP":
        r_adj -= max(0.0, float(stop_extra_r))
    elif outcome in {"TP1", "TP2"}:
        r_adj -= max(0.0, float(tp_haircut_r))
    return r_adj, friction_r


def dump_trades_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    out = str(path or "").strip()
    if not out:
        return
    parent = os.path.dirname(out)
    if parent:
        os.makedirs(parent, exist_ok=True)
    fields = [
        "entry_ts",
        "exit_ts",
        "inst",
        "profile_id",
        "position_key",
        "side",
        "level",
        "entry_px",
        "stop_px",
        "tp1_px",
        "tp2_px",
        "risk_px",
        "outcome",
        "entry_exec_mode",
        "r",
        "r_raw",
        "friction_r",
        "pnl",
    ]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for t in rows:
            w.writerow(
                {
                    "entry_ts": int(t.get("entry_ts", 0) or 0),
                    "exit_ts": int(t.get("exit_ts", 0) or 0),
                    "inst": str(t.get("inst", "")),
                    "profile_id": str(t.get("profile_id", "")),
                    "position_key": str(t.get("position_key", "")),
                    "side": str(t.get("side", "")),
                    "level": int(t.get("level", 0) or 0),
                    "entry_px": float(t.get("entry_px", 0.0) or 0.0),
                    "stop_px": float(t.get("stop_px", 0.0) or 0.0),
                    "tp1_px": float(t.get("tp1_px", 0.0) or 0.0),
                    "tp2_px": float(t.get("tp2_px", 0.0) or 0.0),
                    "risk_px": float(t.get("risk_px", 0.0) or 0.0),
                    "outcome": str(t.get("outcome", "")),
                    "entry_exec_mode": str(t.get("entry_exec_mode", "market") or "market"),
                    "r": float(t.get("r", 0.0) or 0.0),
                    "r_raw": float(t.get("r_raw", 0.0) or 0.0),
                    "friction_r": float(t.get("friction_r", 0.0) or 0.0),
                    "pnl": float(t.get("pnl", 0.0) or 0.0),
                }
            )


def _reverse_swap_exit_prices(*, side: str, entry: float, risk: float, stop_r_mult: float) -> tuple[float, float]:
    side_u = str(side or "").strip().upper()
    stop_r = max(1.0, float(stop_r_mult or 1.0))
    if side_u == "LONG":
        return float(entry) + float(risk), float(entry) - float(risk) * stop_r
    if side_u == "SHORT":
        return float(entry) - float(risk), float(entry) + float(risk) * stop_r
    raise RuntimeError(f"Unsupported side for swapped reverse exit prices: {side}")


def _new_sim_position(
    *,
    decision: Any,
    entry_ts: int,
    entry_i: int,
    risk_amt: float,
    exit_model: str = "standard",
    swap_stop_r_mult: float = 0.0,
    profile_id: str = "",
    position_key: str = "",
) -> Dict[str, Any]:
    entry = float(decision.entry)
    stop = float(decision.stop)
    risk = float(decision.risk)
    if risk <= 0:
        risk = abs(entry - stop)
    risk = max(risk, 1e-8)
    pos = {
        "side": str(decision.side),
        "level": int(decision.level),
        "entry_ts": int(entry_ts),
        "entry_i": int(entry_i),
        "entry": float(entry),
        "stop": float(stop),
        "risk": float(risk),
        "tp1": float(decision.tp1),
        "tp2": float(decision.tp2),
        "risk_amt": float(risk_amt),
        "qty_rem": 1.0,
        "realized_r": 0.0,
        "tp1_done": False,
        "be_armed": False,
        "hard_stop": float(stop),
        "peak_price": float(entry),
        "trough_price": float(entry),
        "exit_model": str(exit_model or "standard"),
        "friction_stop": float(stop),
        "profile_id": str(profile_id or ""),
        "position_key": str(position_key or ""),
    }
    if pos["exit_model"] == "swapped_reverse":
        take_px, hard_stop_px = _reverse_swap_exit_prices(
            side=str(decision.side),
            entry=float(entry),
            risk=float(risk),
            stop_r_mult=float(swap_stop_r_mult),
        )
        pos["tp1"] = float(take_px)
        pos["tp2"] = float(take_px)
        pos["stop"] = float(hard_stop_px)
        pos["hard_stop"] = float(hard_stop_px)
        pos["swap_take_px"] = float(take_px)
        pos["swap_stop_px"] = float(hard_stop_px)
        pos["swap_take_r"] = 1.0
        pos["swap_stop_r"] = max(1.0, float(swap_stop_r_mult or 1.0))
    return pos


def _runtime_slot_key(inst: str, profile_id: str) -> str:
    return f"{str(inst or '').strip().upper()}::{str(profile_id or '').strip().upper()}"


def _build_interleaved_runtime_slots(
    *,
    inst_ids: List[str],
    profile_ids_by_inst: Dict[str, List[str]],
    profile_by_inst: Dict[str, str],
    independent_profile_positions: bool,
) -> List[Dict[str, Any]]:
    slots: List[Dict[str, Any]] = []
    for inst in inst_ids:
        primary_profile_id = str(profile_by_inst.get(inst, "DEFAULT") or "DEFAULT").strip().upper()
        inst_profile_ids = [str(x or "").strip().upper() for x in (profile_ids_by_inst.get(inst) or [primary_profile_id]) if str(x or "").strip()]
        if primary_profile_id not in inst_profile_ids:
            inst_profile_ids = [primary_profile_id] + [x for x in inst_profile_ids if x != primary_profile_id]
        if not inst_profile_ids:
            inst_profile_ids = [primary_profile_id]
        if independent_profile_positions:
            for pid in inst_profile_ids:
                slot_key = _runtime_slot_key(inst, pid)
                slots.append(
                    {
                        "inst": inst,
                        "profile_id": pid,
                        "profile_ids": [pid],
                        "position_key": slot_key,
                        "state_key": slot_key,
                        "display_name": f"{inst}:{pid}",
                    }
                )
            continue
        slots.append(
            {
                "inst": inst,
                "profile_id": primary_profile_id,
                "profile_ids": inst_profile_ids,
                "position_key": str(inst),
                "state_key": str(inst),
                "display_name": str(inst),
            }
        )
    return slots


def _close_remaining_r(side: str, entry: float, risk: float, close_px: float) -> float:
    side_u = str(side).strip().upper()
    if side_u == "LONG":
        return (float(close_px) - float(entry)) / float(risk)
    return (float(entry) - float(close_px)) / float(risk)


def _signal_high_low(sig: Dict[str, Any], close_px: float) -> tuple[float, float]:
    hi = float(sig.get("high", close_px) or close_px)
    lo = float(sig.get("low", close_px) or close_px)
    hi = max(hi, close_px)
    lo = min(lo, close_px)
    if hi < lo:
        hi, lo = lo, hi
    return hi, lo


def _flip_side(side: str) -> str:
    side_u = str(side or "").strip().upper()
    if side_u == "LONG":
        return "SHORT"
    if side_u == "SHORT":
        return "LONG"
    raise RuntimeError(f"Unsupported side for inversion: {side}")


def _mirror_stop(close_px: float, reference_stop: float, *, side: str) -> float:
    entry = float(close_px)
    dist = abs(float(reference_stop) - entry)
    if dist <= 1e-12:
        dist = max(abs(entry) * 0.0005, 1e-8)
    side_u = str(side or "").strip().upper()
    if side_u == "LONG":
        return entry - dist
    if side_u == "SHORT":
        return entry + dist
    raise RuntimeError(f"Unsupported side for mirrored stop: {side}")


def _invert_signal_symmetric(sig: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(sig or {})
    close_px = float(out.get("close", 0.0) or 0.0)
    orig_long_stop = float(out.get("long_stop", close_px) or close_px)
    orig_short_stop = float(out.get("short_stop", close_px) or close_px)

    swap_pairs = (
        ("long_entry", "short_entry"),
        ("long_entry_l2", "short_entry_l2"),
        ("long_entry_l3", "short_entry_l3"),
        ("long_level", "short_level"),
        ("long_exit", "short_exit"),
    )
    for left_key, right_key in swap_pairs:
        left_val = out.get(left_key)
        right_val = out.get(right_key)
        out[left_key] = right_val
        out[right_key] = left_val

    out["long_stop"] = _mirror_stop(close_px, orig_short_stop, side="LONG")
    out["short_stop"] = _mirror_stop(close_px, orig_long_stop, side="SHORT")

    bias = str(out.get("bias", "") or "").strip().lower()
    if bias == "long":
        out["bias"] = "short"
    elif bias == "short":
        out["bias"] = "long"

    vote_winner = str(out.get("vote_winner", "") or "").strip().upper()
    if vote_winner in {"LONG", "SHORT"}:
        out["vote_winner"] = _flip_side(vote_winner)

    return out



def _position_potential_loss_usdt(pos: Dict[str, Any]) -> float:
    risk_amt = max(0.0, float(pos.get("risk_amt", 0.0) or 0.0))
    if risk_amt <= 0:
        return 0.0
    qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
    if qty_rem <= 0:
        return 0.0
    side = str(pos.get("side", "") or "").strip().upper()
    if side not in {"LONG", "SHORT"}:
        return 0.0
    entry = float(pos.get("entry", 0.0) or 0.0)
    risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
    hard_stop = float(pos.get("hard_stop", pos.get("stop", entry)) or entry)
    r_at_stop = float(qty_rem) * float(_close_remaining_r(side, entry, risk, hard_stop))
    if r_at_stop >= 0:
        return 0.0
    return abs(float(r_at_stop) * float(risk_amt))


def _sum_open_positions_potential_loss(open_positions: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for _, pos in open_positions.items():
        if not isinstance(pos, dict):
            continue
        total += max(0.0, _position_potential_loss_usdt(pos))
    return total


def _count_open_l3_side_positions(open_positions: Dict[str, Dict[str, Any]], side: str) -> int:
    side_u = str(side or '').strip().upper()
    if side_u not in {'LONG', 'SHORT'}:
        return 0
    total = 0
    for pos in open_positions.values():
        if not isinstance(pos, dict):
            continue
        if int(pos.get('level', 0) or 0) != 3:
            continue
        if str(pos.get('side', '') or '').strip().upper() != side_u:
            continue
        if max(0.0, float(pos.get('qty_rem', 0.0) or 0.0)) <= 0:
            continue
        total += 1
    return total


def _simulate_live_position_step(
    *,
    pos: Dict[str, Any],
    sig: Dict[str, Any],
    params: Any,
    decision: Optional[Any],
    allow_reverse: bool,
    managed_exit: bool,
    use_signal_exit: bool = True,
) -> Dict[str, Any]:
    side_u = str(pos.get("side", "")).strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    close = float(sig.get("close", 0.0) or 0.0)
    entry = float(pos.get("entry", close) or close)
    risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
    tp1 = float(pos.get("tp1", entry))
    tp2 = float(pos.get("tp2", entry))
    hard_stop = float(pos.get("hard_stop", pos.get("stop", entry)) or entry)
    high, low = _signal_high_low(sig, close)

    def _with_reverse(base: Dict[str, Any]) -> Dict[str, Any]:
        reverse_decision = None
        if bool(base.get("closed", False)) and allow_reverse and (decision is not None):
            want_side = "SHORT" if side_u == "LONG" else "LONG"
            if str(getattr(decision, "side", "")).upper() == want_side:
                reverse_decision = decision
        out = dict(base)
        out["reverse_decision"] = reverse_decision
        return out

    exit_model = str(pos.get("exit_model", "standard") or "standard")
    if exit_model == "swapped_reverse":
        swap_take_px = float(pos.get("swap_take_px", tp1) or tp1)
        swap_stop_px = float(pos.get("swap_stop_px", hard_stop) or hard_stop)
        pos["hard_stop"] = float(swap_stop_px)
        if side_u == "LONG":
            stop_hit = low <= swap_stop_px
            tp_hit = high >= swap_take_px
            long_exit = bool(sig.get("long_exit", False)) if use_signal_exit else False
            if stop_hit and tp_hit:
                return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, swap_stop_px), "is_stop": True})
            if stop_hit:
                return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, swap_stop_px), "is_stop": True})
            if tp_hit:
                return _with_reverse({"closed": True, "outcome": "TP1", "r_raw": _close_remaining_r(side_u, entry, risk, swap_take_px), "is_stop": False})
            if long_exit:
                return _with_reverse({"closed": True, "outcome": "EXIT", "r_raw": _close_remaining_r(side_u, entry, risk, close), "is_stop": False})
            return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

        stop_hit = high >= swap_stop_px
        tp_hit = low <= swap_take_px
        short_exit = bool(sig.get("short_exit", False)) if use_signal_exit else False
        if stop_hit and tp_hit:
            return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, swap_stop_px), "is_stop": True})
        if stop_hit:
            return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, swap_stop_px), "is_stop": True})
        if tp_hit:
            return _with_reverse({"closed": True, "outcome": "TP1", "r_raw": _close_remaining_r(side_u, entry, risk, swap_take_px), "is_stop": False})
        if short_exit:
            return _with_reverse({"closed": True, "outcome": "EXIT", "r_raw": _close_remaining_r(side_u, entry, risk, close), "is_stop": False})
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    if managed_exit:
        res = _simulate_live_managed_step(
            side=side_u,
            entry=entry,
            risk=risk,
            tp1=tp1,
            tp2=tp2,
            pos=pos,
            sig=sig,
            tp1_close_pct=float(params.tp1_close_pct),
            tp2_close_rest=bool(params.tp2_close_rest),
            be_trigger_r_mult=float(params.be_trigger_r_mult),
            be_offset_pct=float(params.be_offset_pct),
            be_fee_buffer_pct=float(params.be_fee_buffer_pct),
            auto_tighten_stop=bool(getattr(params, "auto_tighten_stop", True)),
            trail_after_tp1=bool(getattr(params, "trail_after_tp1", True)),
            trail_atr_mult=float(getattr(params, "trail_atr_mult", 0.0)),
            signal_exit_enabled=bool(use_signal_exit),
            split_tp_enabled=bool(pos.get("exchange_split_tp_enabled", False)),
        )
        return _with_reverse(res)

    if side_u == "LONG":
        stop_hit = low <= hard_stop
        tp2_hit = high >= tp2
        tp1_hit = high >= tp1
        long_exit = bool(sig.get("long_exit", False)) if use_signal_exit else False
        if stop_hit and (tp1_hit or tp2_hit):
            return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, hard_stop), "is_stop": True})
        if stop_hit:
            return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, hard_stop), "is_stop": True})
        if tp2_hit:
            return _with_reverse({"closed": True, "outcome": "TP2", "r_raw": _close_remaining_r(side_u, entry, risk, tp2), "is_stop": False})
        if tp1_hit:
            return _with_reverse({"closed": True, "outcome": "TP1", "r_raw": _close_remaining_r(side_u, entry, risk, tp1), "is_stop": False})
        if long_exit:
            return _with_reverse({"closed": True, "outcome": "EXIT", "r_raw": _close_remaining_r(side_u, entry, risk, close), "is_stop": False})
        return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}

    stop_hit = high >= hard_stop
    tp2_hit = low <= tp2
    tp1_hit = low <= tp1
    short_exit = bool(sig.get("short_exit", False)) if use_signal_exit else False
    if stop_hit and (tp1_hit or tp2_hit):
        return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, hard_stop), "is_stop": True})
    if stop_hit:
        return _with_reverse({"closed": True, "outcome": "STOP", "r_raw": _close_remaining_r(side_u, entry, risk, hard_stop), "is_stop": True})
    if tp2_hit:
        return _with_reverse({"closed": True, "outcome": "TP2", "r_raw": _close_remaining_r(side_u, entry, risk, tp2), "is_stop": False})
    if tp1_hit:
        return _with_reverse({"closed": True, "outcome": "TP1", "r_raw": _close_remaining_r(side_u, entry, risk, tp1), "is_stop": False})
    if short_exit:
        return _with_reverse({"closed": True, "outcome": "EXIT", "r_raw": _close_remaining_r(side_u, entry, risk, close), "is_stop": False})
    return {"closed": False, "outcome": "NONE", "r_raw": 0.0, "is_stop": False, "reverse_decision": None}


def main() -> int:
    parser = argparse.ArgumentParser(description="2Y interleaved portfolio backtest")
    parser.add_argument("--env", default="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env")
    parser.add_argument("--bars", type=int, default=70080, help="15m bars to evaluate (~70080 for 2 years)")
    parser.add_argument("--risk-frac", type=float, default=0.005, help="Risk fraction per trade for compounding")
    parser.add_argument("--title", default="2Y 真实顺序 TP1_ONLY（全币种）")
    parser.add_argument("--min-level", type=int, default=None, help="Min signal level to include (1~3)")
    parser.add_argument("--max-level", type=int, default=None, help="Max signal level to include (1~3)")
    parser.add_argument("--exact-level", type=int, default=0, help="Exact signal level to include (1~3)")
    parser.add_argument(
        "--pessimistic",
        action="store_true",
        help="Enable pessimistic execution model (fees/slippage/miss/TP haircut/stop worsening)",
    )
    parser.add_argument("--fee-rate", type=float, default=0.0, help="Round-trip fee rate, e.g. 0.0012 = 0.12%%")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="One-way slippage in bps")
    parser.add_argument("--stop-extra-r", type=float, default=0.0, help="Extra R loss for STOP outcome")
    parser.add_argument("--tp-haircut-r", type=float, default=0.0, help="R haircut for TP1/TP2 outcome")
    parser.add_argument("--miss-prob", type=float, default=0.0, help="Signal miss probability (0~1)")
    parser.add_argument(
        "--entry-exec-mode",
        default="",
        help="Entry execution mode: market|limit|auto (default: read from env STRAT_ENTRY_EXEC_MODE)",
    )
    parser.add_argument(
        "--entry-auto-market-level-min",
        type=int,
        default=None,
        help="When entry-exec-mode=auto, levels >= this use market (default: env STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN)",
    )
    parser.add_argument(
        "--entry-auto-market-level-max",
        type=int,
        default=None,
        help=(
            "When entry-exec-mode=auto and this is set to 1~3, levels <= this use market "
            "(default: env STRAT_ENTRY_AUTO_MARKET_LEVEL_MAX, 0=disabled)"
        ),
    )
    parser.add_argument(
        "--entry-limit-fallback-mode",
        default="",
        help="When entry-exec-mode resolves to limit: market|skip (default: env STRAT_ENTRY_LIMIT_FALLBACK_MODE)",
    )
    parser.add_argument(
        "--entry-limit-slippage-bps",
        type=float,
        default=None,
        help="One-way slippage for limit-filled entries in bps (default: market_slippage*0.25)",
    )
    parser.add_argument(
        "--entry-limit-fee-rate",
        type=float,
        default=None,
        help="Round-trip fee rate for limit-filled entries (default: same as --fee-rate)",
    )
    parser.add_argument(
        "--managed-exit",
        action="store_true",
        help="Use managed exit path: TP1 partial + remaining TP2 + BE/fee-buffer stop",
    )
    parser.add_argument(
        "--force-managed-tp-fallback",
        action="store_true",
        help="Force backtest to simulate managed TP fallback instead of native split TP on entry",
    )
    parser.add_argument(
        "--live-window-signals",
        action="store_true",
        help="Build signals with the same rolling candle window as live runtime (slower but parity-safe)",
    )
    parser.add_argument(
        "--dump-trades-csv",
        default="",
        help="Optional output CSV path for accepted trade list (for Monte Carlo etc.)",
    )
    parser.add_argument(
        "--ignore-signal-exit",
        action="store_true",
        help="Disable signal-based early close (long_exit/short_exit) in backtest management path",
    )
    parser.add_argument(
        "--l3-side-cap",
        type=int,
        default=0,
        help="Max concurrent open L3 positions per side across instruments (<=0 disables)",
    )
    parser.add_argument(
        "--invert-signals",
        action="store_true",
        help="Invert long/short signals and mirror risk symmetrically around entry price",
    )
    parser.add_argument(
        "--invert-signals-swap-exits",
        action="store_true",
        help="Invert signals and swap exit structure to 1R TP / original-TP1-distance stop",
    )
    parser.add_argument(
        "--independent-profile-positions",
        action="store_true",
        help=(
            "When one instrument is mapped to multiple strategy profiles, run them as independent "
            "sub-systems with separate positions keyed by inst+profile instead of vote-merging"
        ),
    )
    args = parser.parse_args()

    load_dotenv(args.env)
    override_notes = apply_backtest_env_overrides()
    if override_notes:
        print(f"backtest_env_overrides: {'; '.join(override_notes)}", flush=True)
    cfg = read_config(None)
    client = create_client(cfg)
    run_start = time.monotonic()

    inst_ids = list(cfg.inst_ids)
    if not inst_ids:
        print("No inst ids configured.")
        return 1
    profile_ids_by_inst: Dict[str, List[str]] = {inst: get_strategy_profile_ids(cfg, inst) for inst in inst_ids}
    profile_by_inst: Dict[str, str] = {
        inst: (ids[0] if ids else get_strategy_profile_id(cfg, inst))
        for inst, ids in profile_ids_by_inst.items()
    }
    independent_profile_positions = bool(args.independent_profile_positions)
    runtime_slots = _build_interleaved_runtime_slots(
        inst_ids=inst_ids,
        profile_ids_by_inst=profile_ids_by_inst,
        profile_by_inst=profile_by_inst,
        independent_profile_positions=independent_profile_positions,
    )
    slots_by_inst: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for slot in runtime_slots:
        slots_by_inst[str(slot.get("inst", ""))].append(slot)
    params_by_inst = {
        inst: cfg.strategy_profiles.get(profile_by_inst[inst], get_strategy_params(cfg, inst))
        for inst in inst_ids
    }

    bars = max(1200, int(args.bars))
    risk_frac = max(0.0, float(args.risk_frac))
    if risk_frac <= 0:
        risk_frac = 0.005

    fee_rate = max(0.0, float(args.fee_rate))
    slippage_bps = max(0.0, float(args.slippage_bps))
    stop_extra_r = max(0.0, float(args.stop_extra_r))
    tp_haircut_r = max(0.0, float(args.tp_haircut_r))
    miss_prob = clamp01(float(args.miss_prob))
    if bool(args.pessimistic):
        if fee_rate <= 0:
            fee_rate = 0.0012
        if slippage_bps <= 0:
            slippage_bps = 3.0
        if stop_extra_r <= 0:
            stop_extra_r = 0.10
        if tp_haircut_r <= 0:
            tp_haircut_r = 0.05
        if miss_prob <= 0:
            miss_prob = 0.08
    entry_exec_mode = normalize_entry_exec_mode(
        str(args.entry_exec_mode).strip().lower() if str(args.entry_exec_mode or "").strip() else str(getattr(cfg.params, "entry_exec_mode", "market"))
    )
    if args.entry_auto_market_level_min is None:
        entry_auto_market_level_min = int(getattr(cfg.params, "entry_auto_market_level_min", 3) or 3)
    else:
        entry_auto_market_level_min = int(args.entry_auto_market_level_min)
    entry_auto_market_level_min = max(1, min(3, entry_auto_market_level_min))
    if args.entry_auto_market_level_max is None:
        entry_auto_market_level_max = int(getattr(cfg.params, "entry_auto_market_level_max", 0) or 0)
    else:
        entry_auto_market_level_max = int(args.entry_auto_market_level_max)
    entry_auto_market_level_max = max(0, min(3, entry_auto_market_level_max))

    entry_limit_fallback_mode = normalize_entry_limit_fallback_mode(
        str(args.entry_limit_fallback_mode).strip().lower()
        if str(args.entry_limit_fallback_mode or "").strip()
        else str(getattr(cfg.params, "entry_limit_fallback_mode", "market"))
    )
    if args.entry_limit_slippage_bps is None:
        entry_limit_slippage_bps = max(0.0, float(slippage_bps) * 0.25)
    else:
        entry_limit_slippage_bps = max(0.0, float(args.entry_limit_slippage_bps))
    if args.entry_limit_fee_rate is None:
        entry_limit_fee_rate = max(0.0, float(fee_rate))
    else:
        entry_limit_fee_rate = max(0.0, float(args.entry_limit_fee_rate))

    horizon_bars = 0
    min_level = 1
    base_exec_max_level = int(cfg.params.exec_max_level)
    if base_exec_max_level < 1:
        base_exec_max_level = 1
    if base_exec_max_level > 3:
        base_exec_max_level = 3
    max_level = base_exec_max_level
    all_params: List[Any] = []
    for inst in inst_ids:
        ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        for pid in ids:
            all_params.append(cfg.strategy_profiles.get(pid, cfg.params))
    if all_params:
        max_level = max(max(1, min(3, int(p.exec_max_level))) for p in all_params)
    l3_inst_set = set()
    for p in all_params:
        l3_inst_set.update(str(x).strip().upper() for x in p.exec_l3_inst_ids if str(x).strip())
    if args.min_level is not None:
        min_level = max(1, min(3, int(args.min_level)))
    if args.max_level is not None:
        max_level = max(1, min(3, int(args.max_level)))
    exact_level = int(args.exact_level or 0)
    if exact_level not in {0, 1, 2, 3}:
        exact_level = 0
    if exact_level in {1, 2, 3}:
        min_level = exact_level
        max_level = exact_level
    if min_level > max_level:
        min_level = max_level
    require_tp_sl = True
    tp1_only = not bool(args.managed_exit)
    managed_exit = bool(args.managed_exit)
    force_managed_tp_fallback = bool(args.force_managed_tp_fallback)
    use_signal_exit = bool(cfg.params.signal_exit_enabled) and (not bool(args.ignore_signal_exit))
    invert_signal_mode = bool(args.invert_signals) or bool(args.invert_signals_swap_exits)
    inversion_mode_label = "swap_exits" if bool(args.invert_signals_swap_exits) else ("symmetric" if bool(args.invert_signals) else "off")
    default_exit_model = "swapped_reverse" if bool(args.invert_signals_swap_exits) else "standard"
    reopen_on_close_same_bar = parse_bool(os.getenv("OKX_BACKTEST_REOPEN_ON_CLOSE_SAME_BAR", "1"), True)
    default_exchange_split_tp_enabled = bool(
        managed_exit
        and resolve_backtest_split_tp_enabled(
            attach_tpsl_on_entry=bool(getattr(cfg, "attach_tpsl_on_entry", False)),
            enable_close=bool(getattr(cfg.params, "enable_close", False)),
            split_tp_on_entry=bool(getattr(cfg.params, "split_tp_on_entry", False)),
            tp1_close_pct=float(getattr(cfg.params, "tp1_close_pct", 0.0) or 0.0),
            force_managed_tp_fallback=force_managed_tp_fallback,
        )
    )
    trace_cache_enabled = bool(args.live_window_signals) and _interleaved_trace_cache_enabled()

    per_inst_cap = int(cfg.params.max_open_entries)
    global_cap = int(cfg.params.max_open_entries_global)
    window_hours = int(cfg.params.open_window_hours)
    min_gap_minutes = int(cfg.params.min_open_interval_minutes)
    window_ms = max(1, window_hours) * 3600 * 1000
    stop_cooldown_minutes = int(max(0, cfg.params.stop_reentry_cooldown_minutes))
    tp2_cooldown_hours = float(max(0.0, getattr(cfg.params, "tp2_reentry_cooldown_hours", 0.0) or 0.0))
    tp2_partial_until_hours = float(
        max(0.0, getattr(cfg.params, "tp2_reentry_partial_until_hours", 0.0) or 0.0)
    )
    tp2_partial_max_level = int(max(0, getattr(cfg.params, "tp2_reentry_partial_max_level", 0) or 0))
    stop_freeze_count = int(max(0, cfg.params.stop_streak_freeze_count))
    stop_freeze_hours = int(max(0, cfg.params.stop_streak_freeze_hours))
    stop_l2_only = bool(cfg.params.stop_streak_l2_only)

    loss_limit_pct = float(cfg.params.daily_loss_limit_pct)
    loss_base_fixed = float(cfg.params.daily_loss_base_usdt)
    loss_base_mode = normalize_loss_base_mode(str(cfg.params.daily_loss_base_mode))
    loss_window_ms = 24 * 3600 * 1000
    l3_side_cap = max(0, int(args.l3_side_cap))

    start_equity = loss_base_fixed if loss_base_fixed > 0 else 1000.0

    print("=== 2Y Interleaved Portfolio Backtest ===", flush=True)
    print(f"insts={','.join(inst_ids)}", flush=True)
    print(
        f"bars={bars} horizon=to_end min/max/exact={min_level}/{max_level}/{exact_level} "
        f"require_tp_sl={require_tp_sl} tp1_only={tp1_only} managed_exit={managed_exit} "
        f"live_window_signals={bool(args.live_window_signals)}",
        flush=True,
    )
    print(f"signal_inversion={inversion_mode_label}", flush=True)
    print(
        f"level_control: base_exec_max={base_exec_max_level} "
        f"l3_whitelist={','.join(sorted(l3_inst_set)) if l3_inst_set else '-'}",
        flush=True,
    )
    non_default_profiles = []
    for inst in inst_ids:
        ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        if len(ids) > 1:
            non_default_profiles.append(f"{inst}:{'+'.join(ids)}")
        elif profile_by_inst.get(inst, "DEFAULT") != "DEFAULT":
            non_default_profiles.append(f"{inst}:{profile_by_inst.get(inst, 'DEFAULT')}")
    if non_default_profiles:
        print(f"profiles={','.join(non_default_profiles)}", flush=True)
    if independent_profile_positions:
        print("profile_mode=independent_positions(slot=inst+profile)", flush=True)
    elif any(len(profile_ids_by_inst.get(inst, [])) > 1 for inst in inst_ids):
        print(
            f"profile_vote: mode={cfg.strategy_profile_vote_mode} min_agree={cfg.strategy_profile_vote_min_agree}",
            flush=True,
        )
    print(
        f"constraints: min_gap={min_gap_minutes}m inst_cap={per_inst_cap}/24h global_cap={global_cap}/24h "
        f"loss_guard={loss_limit_pct*100:.2f}%({loss_base_mode},projected) "
        f"l3_side_cap={(l3_side_cap if l3_side_cap > 0 else '-')}",
        flush=True,
    )
    print(
        f"stop_guard: cooldown={stop_cooldown_minutes}m tp2_cooldown={tp2_cooldown_hours:g}h "
        f"tp2_partial_until={tp2_partial_until_hours:g}h "
        f"tp2_partial_max_level={tp2_partial_max_level} "
        f"freeze={stop_freeze_count}/{stop_freeze_hours}h "
        f"l2_only={stop_l2_only}",
        flush=True,
    )
    print(
        f"execution_model: pessimistic={bool(args.pessimistic)} fee_rate={fee_rate:.5f} "
        f"slippage_bps={slippage_bps:.2f} stop_extra_r={stop_extra_r:.3f} "
        f"tp_haircut_r={tp_haircut_r:.3f} miss_prob={miss_prob:.3f}",
        flush=True,
    )
    print(
        f"entry_exec_model: mode={entry_exec_mode} auto_market_level_min={entry_auto_market_level_min} "
        f"auto_market_level_max={entry_auto_market_level_max} "
        f"limit_fallback={entry_limit_fallback_mode} "
        f"limit_fee_rate={entry_limit_fee_rate:.5f} limit_slippage_bps={entry_limit_slippage_bps:.2f}",
        flush=True,
    )
    print(f"signal_exit_enabled={use_signal_exit}", flush=True)
    print(f"split_tp_parity=managed_fallback_only:{force_managed_tp_fallback}", flush=True)
    print(f"diag: reopen_on_close_same_bar={reopen_on_close_same_bar}", flush=True)
    print(
        f"compound: risk_per_trade={risk_frac*100:.2f}% start_equity={start_equity:.2f}",
        flush=True,
    )

    ltf_s = bar_to_seconds(cfg.ltf_bar)
    loc_s = bar_to_seconds(cfg.loc_bar)
    htf_s = bar_to_seconds(cfg.htf_bar)
    ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
    ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
    need_ltf = bars + 300
    if all_params:
        max_loc_lookback = max(p.loc_lookback for p in all_params)
        max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
    else:
        max_loc_lookback = cfg.params.loc_lookback
        max_htf_ema_slow = cfg.params.htf_ema_slow_len
    need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
    need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

    data: Dict[str, Dict[str, Any]] = {}
    row_keys_in_order: List[str] = []
    active_inst_ids: List[str] = []
    all_ts = set()
    for idx, inst in enumerate(inst_ids, 1):
        inst_params = params_by_inst.get(inst, cfg.params)
        inst_profile_ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
        if profile_by_inst.get(inst, "DEFAULT") not in inst_profile_ids:
            inst_profile_ids = [profile_by_inst.get(inst, "DEFAULT")] + [x for x in inst_profile_ids if x != profile_by_inst.get(inst, "DEFAULT")]
        print(f"[{idx}/{len(inst_ids)}] fetch {inst} ...", flush=True)
        try:
            htf = client.get_candles_history(inst, cfg.htf_bar, need_htf)
            loc = client.get_candles_history(inst, cfg.loc_bar, need_loc)
            ltf = client.get_candles_history(inst, cfg.ltf_bar, need_ltf)
        except Exception as e:
            print(f"[{inst}] fetch failed: {e}", flush=True)
            continue
        print(f"[{inst}] candles htf={len(htf)} loc={len(loc)} ltf={len(ltf)}", flush=True)
        if len(htf) < 50 or len(loc) < 120 or len(ltf) < 300:
            print(f"[{inst}] skip short data", flush=True)
            continue

        pre_by_profile: Dict[str, Dict[str, Any]] = {}
        if not bool(args.live_window_signals):
            try:
                pre_by_profile[profile_by_inst.get(inst, "DEFAULT")] = _build_backtest_precalc(htf, loc, ltf, inst_params)
                for pid in inst_profile_ids:
                    if pid == profile_by_inst.get(inst, "DEFAULT"):
                        continue
                    pp = cfg.strategy_profiles.get(pid, cfg.params)
                    pre_by_profile[pid] = _build_backtest_precalc(htf, loc, ltf, pp)
            except Exception as e:
                print(f"[{inst}] precalc failed: {e}", flush=True)
                continue
        htf_ts = [c.ts_ms for c in htf]
        loc_ts = [c.ts_ms for c in loc]
        ltf_ts = [c.ts_ms for c in ltf]
        start_idx = max(0, len(ltf) - bars)
        ts_to_i = {int(ltf_ts[i]): i for i in range(start_idx, len(ltf) - 1)}
        for i in range(start_idx, len(ltf) - 1):
            all_ts.add(int(ltf_ts[i]))
        params_by_profile: Dict[str, Any] = {profile_by_inst.get(inst, "DEFAULT"): inst_params}
        for pid in inst_profile_ids:
            if pid == profile_by_inst.get(inst, "DEFAULT"):
                continue
            params_by_profile[pid] = cfg.strategy_profiles.get(pid, cfg.params)
        inst_rows_added = 0
        for slot in slots_by_inst.get(inst, []):
            slot_profile_id = str(slot.get("profile_id", profile_by_inst.get(inst, "DEFAULT")) or profile_by_inst.get(inst, "DEFAULT"))
            slot_profile_ids = list(slot.get("profile_ids", [slot_profile_id]) or [slot_profile_id])
            slot_params = params_by_profile.get(slot_profile_id, cfg.strategy_profiles.get(slot_profile_id, cfg.params))
            slot_exec_max_level = resolve_exec_max_level(slot_params, inst)
            slot_split_tp_enabled = bool(
                managed_exit
                and resolve_backtest_split_tp_enabled(
                    attach_tpsl_on_entry=bool(getattr(cfg, "attach_tpsl_on_entry", False)),
                    enable_close=bool(getattr(slot_params, "enable_close", False)),
                    split_tp_on_entry=bool(getattr(slot_params, "split_tp_on_entry", False)),
                    tp1_close_pct=float(getattr(slot_params, "tp1_close_pct", 0.0) or 0.0),
                    force_managed_tp_fallback=force_managed_tp_fallback,
                )
            )
            try:
                table_bundle = _build_backtest_signal_decision_tables(
                    cfg=cfg,
                    inst_id=inst,
                    profile_id=slot_profile_id,
                    inst_profile_ids=slot_profile_ids,
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
                    tp1_only=tp1_only,
                    start_idx=start_idx,
                    live_signal_window_limit=cfg.candle_limit if bool(args.live_window_signals) else 0,
                )
            except Exception as e:
                print(f"[{inst}:{slot_profile_id}] table build failed: {e}", flush=True)
                continue

            signal_table = table_bundle["signal_table"]
            decision_table = table_bundle["decision_table"]
            table_cache_key = str(table_bundle.get("table_cache_key", "") or "")
            trace_cache_key = ""
            trace_cache_path = ""
            trace_cache_store: Dict[str, Any] = {}
            if trace_cache_enabled and table_cache_key:
                trace_cache_key = _interleaved_trace_cache_key(
                    table_cache_key=table_cache_key,
                    params=slot_params,
                    managed_exit=managed_exit,
                    use_signal_exit=use_signal_exit,
                    exit_model=default_exit_model,
                    split_tp_enabled=slot_split_tp_enabled,
                )
                trace_cache_path = _interleaved_trace_cache_path(cfg, inst, trace_cache_key)
                trace_cache_store = _load_interleaved_trace_cache(trace_cache_path, trace_cache_key, inst_id=inst)
            if invert_signal_mode:
                inverted_signal_table: List[Optional[Dict[str, Any]]] = [None] * len(signal_table)
                inverted_decision_table: List[Optional[Any]] = [None] * len(decision_table)
                for i in range(start_idx, len(signal_table)):
                    sig = signal_table[i]
                    if sig is None:
                        continue
                    inv_sig = _invert_signal_symmetric(sig)
                    inverted_signal_table[i] = inv_sig
                    inverted_decision_table[i] = resolve_entry_decision(
                        inv_sig,
                        max_level=slot_exec_max_level,
                        min_level=min_level,
                        exact_level=exact_level,
                        tp1_r=slot_params.tp1_r_mult,
                        tp2_r=slot_params.tp2_r_mult,
                        tp1_only=tp1_only,
                    )
                signal_table = inverted_signal_table
                decision_table = inverted_decision_table

            row_key = str(slot.get("position_key", inst) or inst)
            data[row_key] = {
                "inst": inst,
                "profile_id": slot_profile_id,
                "profile_ids": slot_profile_ids,
                "position_key": row_key,
                "state_key": str(slot.get("state_key", row_key) or row_key),
                "display_name": str(slot.get("display_name", row_key) or row_key),
                "ltf": ltf,
                "ltf_ts": ltf_ts,
                "ts_to_i": ts_to_i,
                "params": slot_params,
                "signal_table": signal_table,
                "decision_table": decision_table,
                "table_cache_key": table_cache_key,
                "trace_cache_key": trace_cache_key,
                "trace_cache_path": trace_cache_path,
                "trace_cache_store": trace_cache_store,
                "trace_cache_dirty": False,
                "exchange_split_tp_enabled": slot_split_tp_enabled,
            }
            row_keys_in_order.append(row_key)
            inst_rows_added += 1
        if inst_rows_added > 0:
            active_inst_ids.append(inst)

    inst_ids = list(active_inst_ids)
    if not inst_ids:
        print("No instrument has enough history for backtest.")
        return 1

    timeline = sorted(all_ts)
    print(f"timeline_points={len(timeline)}", flush=True)

    def _flush_trace_caches() -> None:
        if not trace_cache_enabled:
            return
        for row_key in row_keys_in_order:
            row = data.get(row_key)
            if not isinstance(row, dict):
                continue
            if not bool(row.get("trace_cache_dirty", False)):
                continue
            trace_cache_key = str(row.get("trace_cache_key", "") or "")
            trace_cache_path = str(row.get("trace_cache_path", "") or "")
            trace_cache_store = row.get("trace_cache_store")
            if not trace_cache_key or not trace_cache_path or not isinstance(trace_cache_store, dict):
                continue
            _save_interleaved_trace_cache(
                trace_cache_path,
                trace_cache_key,
                inst_id=str(row.get("inst", row_key) or row_key),
                trajectories=trace_cache_store,
            )
            row["trace_cache_dirty"] = False

    open_positions: Dict[str, Dict[str, Any]] = {}
    open_positions_total = 0

    equity = float(start_equity)
    peak_equity = equity
    peak_ts = timeline[0]
    max_dd = 0.0
    max_dd_peak_ts = peak_ts
    max_dd_trough_ts = peak_ts
    max_concurrent = 0

    global_open_ts: Deque[int] = deque()
    state_keys = [str(data[row_key].get("state_key", row_key) or row_key) for row_key in row_keys_in_order]
    inst_open_ts: Dict[str, Deque[int]] = {state_key: deque() for state_key in state_keys}
    inst_last_open_ts: Dict[str, int | None] = {state_key: None for state_key in state_keys}

    # (close_ts, loss_usdt)
    loss_events: Deque[Tuple[int, float]] = deque()

    accepted: List[Dict[str, Any]] = []
    by_level = defaultdict(int)
    by_side = defaultdict(int)
    level_perf = new_level_perf()
    tp1_count = 0
    tp2_count = 0
    stop_count = 0
    none_count = 0
    skip_miss = 0
    skip_limit_unfilled = 0
    total_raw_r = 0.0
    total_friction_r = 0.0
    by_entry_exec_mode = defaultdict(int)
    by_inst_stats: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "n": 0.0,
            "win": 0.0,
            "r_sum": 0.0,
            "r_raw_sum": 0.0,
            "friction_r_sum": 0.0,
            "tp1": 0.0,
            "tp2": 0.0,
            "stop": 0.0,
            "none": 0.0,
        }
    )
    by_profile_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(
        lambda: {
            "n": 0.0,
            "win": 0.0,
            "r_sum": 0.0,
            "r_raw_sum": 0.0,
            "friction_r_sum": 0.0,
            "tp1": 0.0,
            "tp2": 0.0,
            "stop": 0.0,
            "none": 0.0,
        }
    )
    skip_gap = 0
    skip_inst_cap = 0
    skip_global_cap = 0
    skip_unresolved = 0
    skip_loss_guard = 0
    skip_stop_guard = 0
    skip_tp2_cooldown = 0
    skip_tp2_partial = 0
    skip_l3_side_cap = 0
    stop_guard: Dict[str, Dict[str, Dict[str, int]]] = {
        state_key: {"long": {}, "short": {}} for state_key in state_keys
    }

    def _sg_bucket(state_key: str, side: str) -> Dict[str, Any]:
        side_k = str(side).strip().lower()
        if side_k not in {"long", "short"}:
            side_k = "long"
        one = stop_guard.setdefault(state_key, {"long": {}, "short": {}})
        b = one.get(side_k)
        if not isinstance(b, dict):
            b = {}
            one[side_k] = b
        return b

    def _sg_record(state_key: str, side: str, outcome: str, event_ts: int) -> None:
        b = _sg_bucket(state_key, side)
        prev = int(b.get("streak", 0) or 0)
        outcome_k = str(outcome or "").strip().upper()
        is_stop = outcome_k == "STOP"
        if is_stop:
            streak = prev + 1
            b["streak"] = streak
            b["last_stop_ts_ms"] = int(event_ts)
            if stop_freeze_count > 0 and stop_freeze_hours > 0 and streak >= stop_freeze_count:
                b["freeze_until_ts_ms"] = int(event_ts) + stop_freeze_hours * 3600 * 1000
            return
        if prev > 0:
            b["streak"] = 0
        if outcome_k == "TP2" and (tp2_cooldown_hours > 0 or tp2_partial_until_hours > 0):
            arm_tp2_reentry_bucket(
                b,
                event_ts_ms=int(event_ts),
                block_hours=tp2_cooldown_hours,
                partial_until_hours=tp2_partial_until_hours,
            )

    def _sg_allow(state_key: str, side: str, level: int, now_ts: int) -> bool:
        nonlocal skip_stop_guard, skip_tp2_cooldown, skip_tp2_partial
        b = _sg_bucket(state_key, side)
        last_stop_ts = int(b.get("last_stop_ts_ms", 0) or 0)
        if stop_cooldown_minutes > 0 and last_stop_ts > 0:
            if int(now_ts) - last_stop_ts < stop_cooldown_minutes * 60 * 1000:
                skip_stop_guard += 1
                return False
        tp2_gate = get_tp2_reentry_gate(
            b,
            now_ts_ms=int(now_ts),
            planned_level=int(level),
            block_hours=tp2_cooldown_hours,
            partial_until_hours=tp2_partial_until_hours,
            partial_max_level=tp2_partial_max_level,
        )
        tp2_status = str(tp2_gate.get("status") or "")
        if tp2_status == "block":
            skip_tp2_cooldown += 1
            return False
        if tp2_status == "partial_level_cap":
            skip_tp2_partial += 1
            return False
        freeze_until_ts = int(b.get("freeze_until_ts_ms", 0) or 0)
        if freeze_until_ts > int(now_ts):
            if stop_l2_only:
                if int(level) > 2:
                    skip_stop_guard += 1
                    return False
            else:
                skip_stop_guard += 1
                return False
        return True

    def _record_close(
        *,
        inst: str,
        pos: Dict[str, Any],
        exit_ts: int,
        outcome: str,
        r_raw: float,
        is_stop: bool,
    ) -> None:
        nonlocal equity, peak_equity, peak_ts, max_dd, max_dd_peak_ts, max_dd_trough_ts
        nonlocal tp1_count, tp2_count, stop_count, none_count, total_raw_r, total_friction_r, skip_unresolved

        outcome_k = str(outcome or "NONE").upper()
        if tp1_only and outcome_k == "TP2":
            outcome_k = "TP1"
        if require_tp_sl and outcome_k == "NONE":
            skip_unresolved += 1
            return

        entry = float(pos.get("entry", 0.0) or 0.0)
        stop = float(pos.get("stop", 0.0) or 0.0)
        friction_stop = float(pos.get("friction_stop", stop) or stop)
        level = int(pos.get("level", 0) or 0)
        side = str(pos.get("side", "") or "").upper()
        risk_amt = float(pos.get("risk_amt", 0.0) or 0.0)
        entry_ts = int(pos.get("entry_ts", exit_ts) or exit_ts)
        entry_exec_mode_used = str(pos.get("entry_exec_mode", "market") or "market")
        fee_rate_used = max(0.0, float(pos.get("fee_rate", fee_rate) or fee_rate))
        slippage_bps_used = max(0.0, float(pos.get("slippage_bps", slippage_bps) or slippage_bps))
        profile_id = str(pos.get("profile_id", "") or "")
        position_key = str(pos.get("position_key", "") or "")
        state_key = str(pos.get("state_key", position_key or inst) or inst)

        r_adj, friction_r = apply_execution_penalty_r(
            r_raw=float(r_raw),
            outcome=outcome_k,
            entry=entry,
            stop=friction_stop,
            fee_rate=fee_rate_used,
            slippage_bps=slippage_bps_used,
            stop_extra_r=stop_extra_r,
            tp_haircut_r=tp_haircut_r,
        )
        pnl = float(risk_amt) * float(r_adj)
        equity += pnl
        if pnl < 0:
            loss_events.append((int(exit_ts), abs(float(pnl))))

        accepted.append(
            {
                "inst": inst,
                "profile_id": profile_id,
                "position_key": position_key,
                "entry_ts": int(entry_ts),
                "exit_ts": int(exit_ts),
                "r": float(r_adj),
                "r_raw": float(r_raw),
                "friction_r": float(friction_r),
                "pnl": float(pnl),
                "outcome": str(outcome_k),
                "level": int(level),
                "side": str(side),
                "entry_px": float(pos.get("entry", 0.0) or 0.0),
                "stop_px": float(pos.get("stop", 0.0) or 0.0),
                "tp1_px": float(pos.get("tp1", 0.0) or 0.0),
                "tp2_px": float(pos.get("tp2", 0.0) or 0.0),
                "risk_px": float(pos.get("risk", 0.0) or 0.0),
                "entry_exec_mode": entry_exec_mode_used,
            }
        )

        st = by_inst_stats[inst]
        st["n"] += 1.0
        st["r_sum"] += float(r_adj)
        st["r_raw_sum"] += float(r_raw)
        st["friction_r_sum"] += float(friction_r)
        total_raw_r += float(r_raw)
        total_friction_r += float(friction_r)
        pst = by_profile_stats[(inst, profile_id)]
        pst["n"] += 1.0
        pst["r_sum"] += float(r_adj)
        pst["r_raw_sum"] += float(r_raw)
        pst["friction_r_sum"] += float(friction_r)
        by_side[str(side)] += 1
        by_entry_exec_mode[entry_exec_mode_used] += 1
        update_level_perf(level_perf, int(level), str(outcome_k), float(r_adj))

        if outcome_k in {"TP1", "TP2"}:
            st["win"] += 1.0
            pst["win"] += 1.0
        if outcome_k == "TP2":
            st["tp2"] += 1.0
            st["tp1"] += 1.0
            pst["tp2"] += 1.0
            pst["tp1"] += 1.0
            tp2_count += 1
            tp1_count += 1
        elif outcome_k == "TP1":
            st["tp1"] += 1.0
            pst["tp1"] += 1.0
            tp1_count += 1
        elif outcome_k == "STOP":
            st["stop"] += 1.0
            pst["stop"] += 1.0
            stop_count += 1
        else:
            st["none"] += 1.0
            pst["none"] += 1.0
            none_count += 1

        _sg_record(state_key, side, outcome_k, int(exit_ts))

        if equity > peak_equity:
            peak_equity = equity
            peak_ts = int(exit_ts)
        dd = 0.0 if peak_equity <= 0 else (peak_equity - equity) / peak_equity
        if dd > max_dd:
            max_dd = dd
            max_dd_peak_ts = peak_ts
            max_dd_trough_ts = int(exit_ts)

    def _can_open(state_key: str, side: str, level: int, ts: int, candidate_loss_usdt: float = 0.0) -> bool:
        nonlocal skip_gap, skip_inst_cap, skip_global_cap, skip_loss_guard, skip_l3_side_cap
        prune_ts_deque_window(global_open_ts, ts, window_ms)
        q_inst = inst_open_ts[state_key]
        prune_ts_deque_window(q_inst, ts, window_ms)

        last_ts = inst_last_open_ts.get(state_key)
        remain_min = min_open_gap_remaining_minutes(ts, last_ts, min_gap_minutes)
        if remain_min > 0:
            skip_gap += 1
            return False
        if is_open_limit_reached(len(q_inst), per_inst_cap):
            skip_inst_cap += 1
            return False
        if is_open_limit_reached(len(global_open_ts), global_cap):
            skip_global_cap += 1
            return False
        if l3_side_cap > 0 and int(level) == 3:
            if _count_open_l3_side_positions(open_positions, side) >= l3_side_cap:
                skip_l3_side_cap += 1
                return False
        if not _sg_allow(state_key, side, level, int(ts)):
            return False

        if loss_limit_pct > 0:
            rolling_loss = sum(x[1] for x in loss_events)
            open_risk = _sum_open_positions_potential_loss(open_positions)
            projected_loss = float(rolling_loss) + float(open_risk) + max(0.0, float(candidate_loss_usdt))
            base_val = resolve_loss_base(loss_base_mode, float(equity), float(loss_base_fixed))
            limit_val = float(base_val) * float(loss_limit_pct)
            if projected_loss > limit_val + 1e-12:
                skip_loss_guard += 1
                return False
        return True

    def _try_open(inst: str, row: Dict[str, Any], ts: int, i: int, decision: Optional[Any], *, is_reverse: bool) -> bool:
        nonlocal open_positions_total, max_concurrent, skip_miss, skip_limit_unfilled
        if decision is None:
            return False
        side = str(decision.side)
        level = int(decision.level)
        position_key = str(row.get("position_key", inst) or inst)
        state_key = str(row.get("state_key", position_key) or position_key)
        profile_id = str(row.get("profile_id", "") or "")
        risk_amt = max(0.0, float(equity) * risk_frac)
        if risk_amt <= 0:
            return False

        exit_model = default_exit_model
        swap_stop_r_mult = 0.0
        candidate_loss_usdt = float(risk_amt)
        if exit_model == "swapped_reverse":
            base_risk = max(1e-8, float(decision.risk))
            swap_stop_r_mult = abs(float(decision.tp1) - float(decision.entry)) / base_risk
            swap_stop_r_mult = max(1.0, float(swap_stop_r_mult))
            candidate_loss_usdt = float(risk_amt) * float(swap_stop_r_mult)

        if not _can_open(state_key, side, level, ts, candidate_loss_usdt=float(candidate_loss_usdt)):
            return False
        if miss_prob > 0:
            miss_key = f"{position_key}|{int(ts)}|{side}|{int(level)}|{'rev' if is_reverse else 'std'}"
            if stable_u01(miss_key) < miss_prob:
                skip_miss += 1
                return False

        intended_exec_mode = resolve_entry_exec_mode(
            entry_exec_mode,
            int(level),
            entry_auto_market_level_min,
            entry_auto_market_level_max,
        )
        effective_exec_mode = intended_exec_mode
        fee_rate_used = fee_rate
        slippage_bps_used = slippage_bps

        if intended_exec_mode == "limit":
            no_fill = False
            if miss_prob > 0:
                no_fill_key = f"{position_key}|{int(ts)}|{side}|{int(level)}|limit_nofill|{'rev' if is_reverse else 'std'}"
                no_fill = stable_u01(no_fill_key) < miss_prob
            if no_fill:
                resolved_limit_fallback_mode = resolve_entry_limit_fallback_mode_for_params(cfg.params, int(level))
                if resolved_limit_fallback_mode == "skip":
                    skip_limit_unfilled += 1
                    return False
                effective_exec_mode = "limit_fallback_market"
                fee_rate_used = fee_rate
                slippage_bps_used = slippage_bps
            else:
                effective_exec_mode = "limit"
                fee_rate_used = entry_limit_fee_rate
                slippage_bps_used = entry_limit_slippage_bps

        pos = _new_sim_position(
            decision=decision,
            entry_ts=int(ts),
            entry_i=int(i),
            risk_amt=float(risk_amt),
            exit_model=exit_model,
            swap_stop_r_mult=float(swap_stop_r_mult),
            profile_id=profile_id,
            position_key=position_key,
        )
        pos["state_key"] = state_key
        pos["entry_exec_mode"] = str(effective_exec_mode)
        pos["entry_exec_mode_intended"] = str(intended_exec_mode)
        pos["fee_rate"] = float(fee_rate_used)
        pos["slippage_bps"] = float(slippage_bps_used)
        pos["exchange_split_tp_enabled"] = bool(row.get("exchange_split_tp_enabled", False))
        trajectory = _get_or_build_interleaved_trade_trajectory(
            row=row,
            entry_i=int(i),
            decision=decision,
            params=row["params"],
            managed_exit=managed_exit,
            use_signal_exit=use_signal_exit,
            exit_model=exit_model,
            split_tp_enabled=bool(pos["exchange_split_tp_enabled"]),
            swap_stop_r_mult=float(swap_stop_r_mult),
        )
        if isinstance(trajectory, list):
            pos["_trajectory_events"] = trajectory
            pos["_trajectory_ptr"] = 0
        open_positions[position_key] = pos
        open_positions_total += 1

        q_inst = inst_open_ts[state_key]
        q_inst.append(ts)
        global_open_ts.append(ts)
        inst_last_open_ts[state_key] = ts
        by_level[int(level)] += 1
        if open_positions_total > max_concurrent:
            max_concurrent = open_positions_total
        return True

    for step, ts in enumerate(timeline, 1):
        prune_loss_deque_window(loss_events, ts, loss_window_ms)

        for row_key in row_keys_in_order:
            row = data[row_key]
            inst = str(row.get("inst", row_key) or row_key)
            position_key = str(row.get("position_key", row_key) or row_key)
            i = row["ts_to_i"].get(ts)
            if i is None:
                continue

            inst_params = row["params"]
            sig = row["signal_table"][i]
            if sig is None:
                continue
            decision = row["decision_table"][i]
            existing = open_positions.get(position_key)
            if existing is not None:
                if isinstance(existing.get("_trajectory_events"), list):
                    sim = _apply_cached_trade_trajectory_step(
                        pos=existing,
                        current_i=int(i),
                        decision=decision,
                        allow_reverse=bool(cfg.params.allow_reverse),
                    )
                else:
                    sim = _simulate_live_position_step(
                        pos=existing,
                        sig=sig,
                        params=inst_params,
                        decision=decision,
                        allow_reverse=bool(cfg.params.allow_reverse),
                        managed_exit=bool(managed_exit),
                        use_signal_exit=bool(use_signal_exit),
                    )
                if bool(sim.get("closed", False)):
                    open_positions.pop(position_key, None)
                    open_positions_total = max(0, open_positions_total - 1)
                    _record_close(
                        inst=inst,
                        pos=existing,
                        exit_ts=int(ts),
                        outcome=str(sim.get("outcome", "NONE")),
                        r_raw=float(sim.get("r_raw", 0.0) or 0.0),
                        is_stop=bool(sim.get("is_stop", False)),
                    )
                    reopen_decision, reopen_is_reverse = _select_post_close_open_decision(
                        sim=sim,
                        decision=decision,
                        reopen_on_close_same_bar=reopen_on_close_same_bar,
                    )
                    if reopen_decision is not None:
                        _try_open(
                            inst,
                            row,
                            int(ts),
                            int(i),
                            reopen_decision,
                            is_reverse=bool(reopen_is_reverse),
                        )
                continue

            _try_open(inst, row, int(ts), int(i), decision, is_reverse=False)

        if step % 10000 == 0:
            print(
                f"progress {step}/{len(timeline)} | open={open_positions_total} equity={equity:.2f} accepted={len(accepted)}",
                flush=True,
            )

    for row_key in list(row_keys_in_order):
        row = data[row_key]
        inst = str(row.get("inst", row_key) or row_key)
        position_key = str(row.get("position_key", row_key) or row_key)
        pos = open_positions.get(position_key)
        if pos is None:
            continue
        last_i = len(row["ltf"]) - 1
        if last_i < 0:
            continue
        close_px = float(row["ltf"][last_i].close)
        entry = float(pos.get("entry", close_px) or close_px)
        risk = max(1e-8, float(pos.get("risk", 0.0) or 0.0))
        qty_rem = max(0.0, float(pos.get("qty_rem", 0.0) or 0.0))
        r_raw = float(pos.get("realized_r", 0.0) or 0.0)
        if qty_rem > 0:
            r_raw += qty_rem * _close_remaining_r(str(pos.get("side", "")), entry, risk, close_px)
        outcome = "TP1" if bool(pos.get("tp1_done", False)) else "NONE"
        open_positions.pop(position_key, None)
        open_positions_total = max(0, open_positions_total - 1)
        _record_close(
            inst=inst,
            pos=pos,
            exit_ts=int(row["ltf_ts"][last_i]),
            outcome=str(outcome),
            r_raw=float(r_raw),
            is_stop=False,
        )

    if not accepted:
        _flush_trace_caches()
        text = f"【{args.title}】无有效成交信号。"
        print(text, flush=True)
        sent = send_telegram(cfg, text)
        print(f"telegram_sent={sent}", flush=True)
        return 0

    accepted.sort(key=lambda x: (x["exit_ts"], x["entry_ts"], x.get("profile_id", ""), x.get("position_key", "")))
    dump_trades_csv(args.dump_trades_csv, accepted)
    signals = len(accepted)
    wins = sum(1 for t in accepted if t["outcome"] in {"TP1", "TP2"})
    stops = sum(1 for t in accepted if t["outcome"] == "STOP")
    win_rate = wins / signals * 100.0
    avg_r = sum(t["r"] for t in accepted) / signals
    avg_raw_r = sum(t.get("r_raw", t["r"]) for t in accepted) / signals
    avg_friction_r = sum(t.get("friction_r", 0.0) for t in accepted) / signals
    ret_pct = (equity / start_equity - 1.0) * 100.0

    cur_ls = 0
    worst_ls = 0
    cur_ws = 0
    best_ws = 0
    for t in accepted:
        if t["pnl"] < 0:
            cur_ls += 1
            if cur_ls > worst_ls:
                worst_ls = cur_ls
            cur_ws = 0
        else:
            cur_ls = 0
            if t["outcome"] in {"TP1", "TP2"}:
                cur_ws += 1
                if cur_ws > best_ws:
                    best_ws = cur_ws
            else:
                cur_ws = 0

    start_ts = min(t["entry_ts"] for t in accepted)
    end_ts = max(t["exit_ts"] for t in accepted)
    period_days = (end_ts - start_ts) / 1000 / 86400
    elapsed_s = float(time.monotonic() - run_start)
    level_perf_final = finalize_level_perf(level_perf)

    per_inst_rows: List[Dict[str, Any]] = []
    for inst in inst_ids:
        st = by_inst_stats.get(inst)
        if not st or st["n"] <= 0:
            continue
        n = int(st["n"])
        per_inst_rows.append(
            {
                "inst_id": inst,
                "status": "ok",
                "error": "",
                "signals": n,
                "tp1": int(st.get("tp1", 0.0)),
                "tp2": int(st.get("tp2", 0.0)),
                "stop": int(st.get("stop", 0.0)),
                "none": int(st.get("none", 0.0)),
                "avg_r": (float(st["r_sum"]) / float(st["n"])) if float(st["n"]) > 0 else 0.0,
            }
        )

    std_result = normalize_backtest_result(
        {
            "max_level": max_level,
            "min_level": min_level,
            "exact_level": exact_level,
            "bars": bars,
            "horizon_bars": horizon_bars,
            "inst_ids": list(inst_ids),
            "signals": signals,
            "tp1": tp1_count,
            "tp2": tp2_count,
            "stop": stop_count,
            "none": none_count,
            "skip_gap": skip_gap,
            "skip_daycap": (skip_inst_cap + skip_global_cap),
            "skip_unresolved": skip_unresolved,
            "avg_r": avg_r,
            "by_level": dict(by_level),
            "by_side": {"LONG": int(by_side.get("LONG", 0)), "SHORT": int(by_side.get("SHORT", 0))},
            "level_perf": level_perf_final,
            "elapsed_s": elapsed_s,
            "per_inst": per_inst_rows,
        }
    )

    summary: List[str] = []
    summary.append(f"【{args.title}】")
    summary.append(f"完成时间：{dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary.append(f"区间：{ts_fmt(start_ts)} -> {ts_fmt(end_ts)}（{period_days:.1f}天）")
    summary.append(f"标的：{','.join(inst_ids)}")
    if independent_profile_positions:
        summary.append("仓位模式：independent_profiles（slot=inst+profile，共享权益池，允许同向叠加/多空对冲）")
    summary.append(
        f"层级控制：base_exec_max={max_level} "
        f"l3_whitelist={','.join(sorted(l3_inst_set)) if l3_inst_set else '-'}"
    )
    summary.append(
        f"约束：min_gap={min_gap_minutes}m inst_cap={per_inst_cap}/24h global_cap={global_cap}/24h "
        f"loss_guard={loss_limit_pct*100:.2f}%({loss_base_mode},projected) "
        f"l3_side_cap={(l3_side_cap if l3_side_cap > 0 else '-')} "
        f"tp2_cd={tp2_cooldown_hours:g}h "
        f"tp2_partial_until={tp2_partial_until_hours:g}h "
        f"tp2_partial_max_level={tp2_partial_max_level} "
        f"tp1_only={tp1_only} managed_exit={managed_exit} inversion={inversion_mode_label}"
    )
    summary.append(
        f"复利：risk={risk_frac*100:.2f}%/单 start={start_equity:.2f} final={equity:.2f} "
        f"return={ret_pct:.2f}% maxDD={max_dd*100:.2f}%"
    )
    summary.append(
        f"结果：signals={signals} wins={wins} stops={stops} win_rate={win_rate:.2f}% "
        f"avgR={avg_r:.3f} max_concurrent={max_concurrent} worst_ls={worst_ls} best_ws={best_ws}"
    )
    summary.append(
        f"R分解：raw_avgR={avg_raw_r:.3f} friction_avgR={avg_friction_r:.3f} adj_avgR={avg_r:.3f}"
    )
    summary.append(
        f"入场执行：mode={entry_exec_mode} auto_market_level_min={entry_auto_market_level_min} "
        f"auto_market_level_max={entry_auto_market_level_max} "
        f"limit_fallback={entry_limit_fallback_mode} "
        f"limit_fee_rate={entry_limit_fee_rate:.5f} limit_slippage_bps={entry_limit_slippage_bps:.2f}"
    )
    summary.append(f"标准汇总：{format_backtest_result_line(std_result)}")
    if str(args.dump_trades_csv or "").strip():
        summary.append(f"trades_csv={args.dump_trades_csv}")
    summary.append(
        f"levels：L1={by_level.get(1,0)} L2={by_level.get(2,0)} L3={by_level.get(3,0)} "
        f"skip_gap={skip_gap} skip_inst_cap={skip_inst_cap} skip_global_cap={skip_global_cap} "
        f"skip_loss_guard={skip_loss_guard} skip_stop_guard={skip_stop_guard} "
        f"skip_tp2_cooldown={skip_tp2_cooldown} skip_tp2_partial={skip_tp2_partial} "
        f"skip_l3_side_cap={skip_l3_side_cap} "
        f"skip_miss={skip_miss} skip_unresolved={skip_unresolved}"
    )
    summary.append(
        f"entry_modes：market={by_entry_exec_mode.get('market',0)} "
        f"limit={by_entry_exec_mode.get('limit',0)} "
        f"limit_fallback_market={by_entry_exec_mode.get('limit_fallback_market',0)} "
        f"skip_limit_unfilled={skip_limit_unfilled}"
    )
    for inst in inst_ids:
        st = by_inst_stats.get(inst)
        if not st or st["n"] <= 0:
            continue
        n = int(st["n"])
        win = int(st["win"])
        avg_inst_r = st["r_sum"] / st["n"]
        summary.append(f"- {inst}: {n}单 胜率={win/n*100:.1f}% avgR={avg_inst_r:.3f}")
    if independent_profile_positions:
        for (inst, profile_id), st in sorted(by_profile_stats.items()):
            if not st or st["n"] <= 0:
                continue
            n = int(st["n"])
            win = int(st["win"])
            avg_profile_r = st["r_sum"] / st["n"]
            summary.append(f"- {inst}@{profile_id}: {n}单 胜率={win/n*100:.1f}% avgR={avg_profile_r:.3f}")

    text = "\n".join(summary)
    print("\n=== RESULT ===", flush=True)
    print(text, flush=True)

    _flush_trace_caches()
    sent = send_telegram(cfg, text)
    print(f"telegram_sent={sent}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        print("Unhandled exception in interleaved backtest runner:", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
