from __future__ import annotations

import hashlib
import json
import os
import pickle
from dataclasses import asdict
from typing import Any, Dict, List, Optional

from .common import log
from .models import Candle, Config, StrategyParams


_LIVE_WINDOW_TABLE_CACHE_VERSION = "live_window_tables_v3"
_LIVE_WINDOW_TABLE_CACHE_BASE_CODE_FILES = (
    "backtest.py",
    "backtest_cache.py",
    "backtest_outcome.py",
    "backtest_tables.py",
    "decision_core.py",
    "indicators.py",
    "pa_oral_baseline.py",
    "profile_vote.py",
    "signals.py",
    "strategy_contract.py",
    "strategy_variant.py",
)

_SIGNAL_DECISION_PARAM_FIELDS = (
    "strategy_variant",
    "htf_ema_fast_len",
    "htf_ema_slow_len",
    "htf_rsi_len",
    "htf_rsi_long_min",
    "htf_rsi_short_max",
    "loc_lookback",
    "loc_recent_bars",
    "loc_sr_lookback",
    "location_fib_low",
    "location_fib_high",
    "location_retest_tol",
    "break_len",
    "exit_len",
    "ltf_ema_len",
    "bb_len",
    "bb_mult",
    "bb_width_k",
    "rsi_len",
    "rsi_long_min",
    "rsi_short_max",
    "l2_rsi_relax",
    "l3_rsi_relax",
    "macd_fast",
    "macd_slow",
    "macd_signal",
    "pullback_lookback",
    "pullback_tolerance",
    "max_chase_from_ema",
    "atr_len",
    "atr_stop_mult",
    "min_risk_atr_mult",
    "min_risk_pct",
    "tp1_r_mult",
    "tp2_r_mult",
)


def _backtest_live_table_cache_enabled() -> bool:
    raw = str(os.getenv("OKX_BACKTEST_LIVE_TABLE_CACHE_ENABLED", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _backtest_live_table_cache_dir(cfg: Config) -> str:
    override = str(os.getenv("OKX_BACKTEST_LIVE_TABLE_CACHE_DIR", "") or "").strip()
    if override:
        return override
    hist_dir = str(getattr(cfg, "history_cache_dir", "") or "").strip()
    if hist_dir:
        return os.path.join(os.path.dirname(os.path.abspath(hist_dir.rstrip(os.sep))), "backtest_live_tables")
    return os.path.join(os.getcwd(), ".cache", "backtest_live_tables")


def _backtest_live_table_code_signature() -> str:
    pkg_dir = os.path.dirname(os.path.abspath(__file__))
    code_files: List[str] = list(_LIVE_WINDOW_TABLE_CACHE_BASE_CODE_FILES)
    parts: List[str] = []
    for rel in code_files:
        full = os.path.join(pkg_dir, rel)
        try:
            st = os.stat(full)
            parts.append(f"{rel}:{st.st_mtime_ns}:{st.st_size}")
        except OSError:
            parts.append(f"{rel}:missing")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _hash_candles_for_live_table_cache(candles: List[Candle]) -> str:
    h = hashlib.sha256()
    h.update(str(len(candles)).encode("utf-8"))
    for c in candles:
        h.update(
            (
                f"{int(c.ts_ms)}|{repr(float(c.open))}|{repr(float(c.high))}|{repr(float(c.low))}|"
                f"{repr(float(c.close))}|{1 if bool(c.confirm) else 0}|{repr(float(getattr(c, 'volume', 0.0) or 0.0))};"
            ).encode("utf-8")
        )
    return h.hexdigest()


def _build_backtest_live_table_cache_key(
    *,
    cfg: Config,
    inst_id: str,
    profile_id: str,
    ordered_profile_ids: List[str],
    profile_params: Dict[str, StrategyParams],
    htf_candles: List[Candle],
    loc_candles: List[Candle],
    ltf_candles: List[Candle],
    max_level: int,
    min_level: int,
    exact_level: int,
    tp1_only: bool,
    start_idx: int,
    live_signal_window_limit: int,
) -> str:
    def _signal_decision_param_payload(params: StrategyParams) -> Dict[str, Any]:
        raw = asdict(params)
        return {key: raw.get(key) for key in _SIGNAL_DECISION_PARAM_FIELDS}

    payload = {
        "version": _LIVE_WINDOW_TABLE_CACHE_VERSION,
        "code_sig": _backtest_live_table_code_signature(),
        "exchange_provider": str(getattr(cfg, "exchange_provider", "") or ""),
        "inst_id": str(inst_id),
        "profile_id": str(profile_id),
        "ordered_profile_ids": list(ordered_profile_ids),
        "profile_params": {
            pid: _signal_decision_param_payload(profile_params[pid]) for pid in ordered_profile_ids
        },
        "profile_vote_mode": str(getattr(cfg, "strategy_profile_vote_mode", "") or ""),
        "profile_vote_min_agree": int(getattr(cfg, "strategy_profile_vote_min_agree", 0) or 0),
        "profile_vote_score_map": dict(getattr(cfg, "strategy_profile_vote_score_map", {}) or {}),
        "profile_vote_level_weight": float(getattr(cfg, "strategy_profile_vote_level_weight", 0.0) or 0.0),
        "profile_vote_fallback_profiles": list(getattr(cfg, "strategy_profile_vote_fallback_profiles", []) or []),
        "bars": {
            "htf": str(getattr(cfg, "htf_bar", "") or ""),
            "loc": str(getattr(cfg, "loc_bar", "") or ""),
            "ltf": str(getattr(cfg, "ltf_bar", "") or ""),
        },
        "max_level": int(max_level),
        "min_level": int(min_level),
        "exact_level": int(exact_level),
        "tp1_only": bool(tp1_only),
        "start_idx": int(start_idx),
        "live_signal_window_limit": int(live_signal_window_limit),
        "htf_sig": _hash_candles_for_live_table_cache(htf_candles),
        "loc_sig": _hash_candles_for_live_table_cache(loc_candles),
        "ltf_sig": _hash_candles_for_live_table_cache(ltf_candles),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _backtest_live_table_cache_path(cfg: Config, inst_id: str, cache_key: str) -> str:
    root = _backtest_live_table_cache_dir(cfg)
    safe_inst = str(inst_id).replace("/", "_").replace("-", "_")
    return os.path.join(root, f"{safe_inst}__{cache_key}.pkl")


def _load_backtest_live_table_cache(cache_path: str, cache_key: str, *, inst_id: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(cache_path):
        return None
    try:
        with open(cache_path, "rb") as fh:
            payload = pickle.load(fh)
    except Exception as e:
        log(f"[{inst_id}] live-window table cache load failed: {e}", level="WARN")
        return None
    if not isinstance(payload, dict) or payload.get("cache_key") != cache_key:
        return None
    table_bundle = payload.get("table_bundle")
    if not isinstance(table_bundle, dict):
        return None
    signal_table = table_bundle.get("signal_table")
    decision_table = table_bundle.get("decision_table")
    if not isinstance(signal_table, list) or not isinstance(decision_table, list):
        return None
    log(
        f"[{inst_id}] live-window table cache hit | rows={len(signal_table)} key={cache_key[:12]} path={cache_path}"
    )
    return table_bundle


def _save_backtest_live_table_cache(cache_path: str, cache_key: str, *, inst_id: str, table_bundle: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        tmp_path = f"{cache_path}.tmp"
        payload = {
            "cache_key": cache_key,
            "table_bundle": table_bundle,
        }
        with open(tmp_path, "wb") as fh:
            pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp_path, cache_path)
        log(
            f"[{inst_id}] live-window table cache save | rows={len(table_bundle.get('signal_table', []))} key={cache_key[:12]} path={cache_path}"
        )
    except Exception as e:
        log(f"[{inst_id}] live-window table cache save failed: {e}", level="WARN")
