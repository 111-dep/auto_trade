from __future__ import annotations

import datetime as dt
import os
from dataclasses import replace
from typing import Dict, Optional

from .common import parse_bool, parse_csv, parse_inst_ids
from .models import Config, StrategyParams
from .strategy_variant import normalize_strategy_variant


_STRATEGY_PROFILE_SUFFIX_ALIAS: Dict[str, str] = {
    "VARIANT": "strategy_variant",
    "LTF_BREAK_LEN": "break_len",
    "BREAK_LEN": "break_len",
    "LTF_EXIT_LEN": "exit_len",
    "EXIT_LEN": "exit_len",
    "LTF_BB_LEN": "bb_len",
    "LTF_BB_MULT": "bb_mult",
    "LTF_BB_WIDTH_K": "bb_width_k",
    "LTF_RSI_LEN": "rsi_len",
    "LTF_RSI_LONG_MIN": "rsi_long_min",
    "LTF_RSI_SHORT_MAX": "rsi_short_max",
    "ALERT_L2_RSI_RELAX": "l2_rsi_relax",
    "ALERT_L3_RSI_RELAX": "l3_rsi_relax",
    "LTF_MACD_FAST": "macd_fast",
    "LTF_MACD_SLOW": "macd_slow",
    "LTF_MACD_SIGNAL": "macd_signal",
    "LTF_PULLBACK_LOOKBACK": "pullback_lookback",
    "LTF_PULLBACK_TOL": "pullback_tolerance",
    "LTF_MAX_CHASE_EMA": "max_chase_from_ema",
    "LTF_ATR_LEN": "atr_len",
    "LTF_ATR_STOP_MULT": "atr_stop_mult",
    "LTF_MIN_RISK_ATR_MULT": "min_risk_atr_mult",
    "LTF_MIN_RISK_PCT": "min_risk_pct",
    "MGMT_TP1_R": "tp1_r_mult",
    "MGMT_TP2_R": "tp2_r_mult",
    "MGMT_TP1_CLOSE_PCT": "tp1_close_pct",
    "MGMT_TP2_CLOSE_REST": "tp2_close_rest",
    "MGMT_BE_TRIGGER_R": "be_trigger_r_mult",
    "MGMT_BE_OFFSET_PCT": "be_offset_pct",
    "MGMT_BE_FEE_BUFFER_PCT": "be_fee_buffer_pct",
    "MGMT_TRAIL_ATR_MULT": "trail_atr_mult",
    "MGMT_TRAIL_AFTER_TP1": "trail_after_tp1",
    "HTF_LOCATION_LOOKBACK": "loc_lookback",
}


def _parse_float_env(
    raw_value: str,
    *,
    env_key: str,
    allow_percent: bool = False,
    require_percent: bool = False,
) -> float:
    text = str(raw_value or "").strip()
    if require_percent and (not text.endswith("%")):
        raise ValueError(f"{env_key} must use percent format, e.g. 0.5% or 30%")
    if allow_percent and text.endswith("%"):
        num_txt = text[:-1].strip()
        if not num_txt:
            raise ValueError(f"{env_key} invalid percent value: {raw_value}")
        try:
            return float(num_txt) / 100.0
        except Exception as exc:
            raise ValueError(f"{env_key} invalid percent value: {raw_value}") from exc
    try:
        return float(text)
    except Exception as exc:
        raise ValueError(f"{env_key} invalid float value: {raw_value}") from exc


def _normalize_strategy_params(params: StrategyParams) -> None:
    params.strategy_variant = normalize_strategy_variant(getattr(params, "strategy_variant", "classic"))
    if params.min_open_interval_minutes < 0:
        params.min_open_interval_minutes = 0
    if params.leverage < 0:
        params.leverage = 0.0
    if params.risk_frac < 0:
        params.risk_frac = 0.0
    if params.risk_frac > 1:
        params.risk_frac = 1.0
    if params.risk_max_margin_frac < 0:
        params.risk_max_margin_frac = 0.0
    # Accept either ratio (0.3) or percent (30).
    if params.risk_max_margin_frac >= 1:
        params.risk_max_margin_frac = params.risk_max_margin_frac / 100.0
    if params.risk_max_margin_frac > 1:
        params.risk_max_margin_frac = 1.0
    if params.be_fee_buffer_pct < 0:
        params.be_fee_buffer_pct = 0.0
    if params.daily_loss_limit_pct < 0:
        params.daily_loss_limit_pct = 0.0
    # Accept either 0.03 (ratio) or 3 (percent).
    if params.daily_loss_limit_pct >= 1:
        params.daily_loss_limit_pct = params.daily_loss_limit_pct / 100.0
    if params.daily_loss_limit_pct > 1:
        params.daily_loss_limit_pct = 1.0
    if params.daily_loss_base_usdt < 0:
        params.daily_loss_base_usdt = 0.0
    if params.daily_loss_base_mode not in {"fixed", "current", "min"}:
        params.daily_loss_base_mode = "current"
    if params.stop_reentry_cooldown_minutes < 0:
        params.stop_reentry_cooldown_minutes = 0
    if params.stop_streak_freeze_count < 0:
        params.stop_streak_freeze_count = 0
    if params.stop_streak_freeze_hours < 0:
        params.stop_streak_freeze_hours = 0
    if params.max_open_entries_global < 0:
        params.max_open_entries_global = 0
    if params.exec_max_level < 1:
        params.exec_max_level = 1
    if params.exec_max_level > 3:
        params.exec_max_level = 3
    params.exec_l3_inst_ids = parse_inst_ids(",".join(params.exec_l3_inst_ids))
    params.entry_exec_mode = str(getattr(params, "entry_exec_mode", "market") or "market").strip().lower()
    if params.entry_exec_mode not in {"market", "limit", "auto"}:
        params.entry_exec_mode = "market"
    if params.entry_auto_market_level_min < 1:
        params.entry_auto_market_level_min = 1
    if params.entry_auto_market_level_min > 3:
        params.entry_auto_market_level_min = 3
    if params.entry_limit_offset_bps < 0:
        params.entry_limit_offset_bps = 0.0
    if params.entry_limit_ttl_sec < 0:
        params.entry_limit_ttl_sec = 0
    if params.entry_limit_poll_ms < 100:
        params.entry_limit_poll_ms = 100
    if params.entry_limit_reprice_max < 0:
        params.entry_limit_reprice_max = 0
    params.entry_limit_fallback_mode = str(getattr(params, "entry_limit_fallback_mode", "market") or "market").strip().lower()
    if params.entry_limit_fallback_mode not in {"market", "skip"}:
        params.entry_limit_fallback_mode = "market"


def _build_base_strategy_params() -> StrategyParams:
    return StrategyParams(
        strategy_variant=os.getenv("STRAT_VARIANT", "classic").strip(),
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
        leverage=float(os.getenv("STRAT_LEVERAGE", "0")),
        risk_frac=_parse_float_env(
            os.getenv("STRAT_RISK_FRAC", "0%"),
            env_key="STRAT_RISK_FRAC",
            allow_percent=True,
            require_percent=True,
        ),
        risk_max_margin_frac=_parse_float_env(
            os.getenv("STRAT_RISK_MAX_MARGIN_FRAC", "30%"),
            env_key="STRAT_RISK_MAX_MARGIN_FRAC",
            allow_percent=True,
            require_percent=True,
        ),
        tp1_r_mult=float(os.getenv("STRAT_MGMT_TP1_R", "1.5")),
        tp2_r_mult=float(os.getenv("STRAT_MGMT_TP2_R", "2.5")),
        tp1_close_pct=float(os.getenv("STRAT_MGMT_TP1_CLOSE_PCT", "0.5")),
        tp2_close_rest=parse_bool(os.getenv("STRAT_MGMT_TP2_CLOSE_REST", "1"), True),
        be_trigger_r_mult=float(os.getenv("STRAT_MGMT_BE_TRIGGER_R", "1.0")),
        be_offset_pct=float(os.getenv("STRAT_MGMT_BE_OFFSET_PCT", "0.0005")),
        be_fee_buffer_pct=float(os.getenv("STRAT_MGMT_BE_FEE_BUFFER_PCT", "0.0008")),
        trail_atr_mult=float(os.getenv("STRAT_MGMT_TRAIL_ATR_MULT", "1.8")),
        trail_after_tp1=parse_bool(os.getenv("STRAT_MGMT_TRAIL_AFTER_TP1", "1"), True),
        max_open_entries=int(os.getenv("STRAT_MAX_OPEN_ENTRIES", "0")),
        max_open_entries_global=int(os.getenv("STRAT_MAX_OPEN_ENTRIES_GLOBAL", "0")),
        open_window_hours=int(os.getenv("STRAT_OPEN_WINDOW_HOURS", "24")),
        min_open_interval_minutes=int(os.getenv("STRAT_MIN_OPEN_INTERVAL_MINUTES", "0")),
        daily_loss_limit_pct=_parse_float_env(
            os.getenv("STRAT_DAILY_LOSS_LIMIT_PCT", "0"),
            env_key="STRAT_DAILY_LOSS_LIMIT_PCT",
            allow_percent=True,
        ),
        daily_loss_base_usdt=float(
            os.getenv(
                "STRAT_DAILY_LOSS_BASE_USDT",
                os.getenv("OKX_COMPOUND_BASE_EQUITY", os.getenv("OKX_MARGIN_USDT", "1000")),
            )
        ),
        daily_loss_base_mode=os.getenv("STRAT_DAILY_LOSS_BASE_MODE", "current").strip().lower(),
        stop_reentry_cooldown_minutes=int(os.getenv("STRAT_STOP_REENTRY_COOLDOWN_MINUTES", "60")),
        stop_streak_freeze_count=int(os.getenv("STRAT_STOP_STREAK_FREEZE_COUNT", "2")),
        stop_streak_freeze_hours=int(os.getenv("STRAT_STOP_STREAK_FREEZE_HOURS", "4")),
        stop_streak_l2_only=parse_bool(os.getenv("STRAT_STOP_STREAK_L2_ONLY", "1"), True),
        exec_max_level=int(os.getenv("STRAT_EXEC_MAX_LEVEL", "1")),
        exec_l3_inst_ids=parse_inst_ids(os.getenv("STRAT_EXEC_L3_INST_IDS", "")),
        enable_close=parse_bool(os.getenv("STRAT_ENABLE_CLOSE", "1"), True),
        signal_exit_enabled=parse_bool(os.getenv("STRAT_SIGNAL_EXIT_ENABLED", "0"), False),
        split_tp_on_entry=parse_bool(os.getenv("STRAT_SPLIT_TP_ON_ENTRY", "0"), False),
        allow_reverse=parse_bool(os.getenv("STRAT_ALLOW_REVERSE", "1"), True),
        entry_exec_mode=os.getenv("STRAT_ENTRY_EXEC_MODE", "market").strip().lower(),
        entry_auto_market_level_min=int(os.getenv("STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN", "3")),
        entry_limit_offset_bps=float(os.getenv("STRAT_ENTRY_LIMIT_OFFSET_BPS", "1.0")),
        entry_limit_ttl_sec=int(os.getenv("STRAT_ENTRY_LIMIT_TTL_SEC", "10")),
        entry_limit_poll_ms=int(os.getenv("STRAT_ENTRY_LIMIT_POLL_MS", "500")),
        entry_limit_reprice_max=int(os.getenv("STRAT_ENTRY_LIMIT_REPRICE_MAX", "0")),
        entry_limit_fallback_mode=os.getenv("STRAT_ENTRY_LIMIT_FALLBACK_MODE", "market").strip().lower(),
        manage_only_script_positions=parse_bool(os.getenv("STRAT_MANAGE_ONLY_SCRIPT_POSITIONS", "1"), True),
        skip_on_foreign_mgnmode_pos=parse_bool(os.getenv("STRAT_SKIP_ON_FOREIGN_MGNMODE_POS", "1"), True),
    )


def _normalize_profile_id(raw: str) -> str:
    profile = str(raw or "").strip().upper().replace("-", "_")
    if not profile:
        return "DEFAULT"
    for ch in profile:
        if not (ch.isalnum() or ch == "_"):
            raise ValueError(f"Invalid strategy profile id: {raw}")
    return profile


def _parse_strategy_profile_map(raw: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not raw:
        return mapping
    for item in raw.split(","):
        pair = item.strip()
        if not pair:
            continue
        if ":" not in pair:
            raise ValueError(f"Invalid STRAT_PROFILE_MAP item: {pair} (expected INST:PROFILE)")
        inst_id, profile_id = pair.split(":", 1)
        inst = str(inst_id).strip().upper()
        if not inst:
            continue
        mapping[inst] = _normalize_profile_id(profile_id)
    return mapping


def _parse_strategy_profile_inst_groups(raw: str) -> Dict[str, str]:
    """Parse grouped profile mapping: PROFILE:INST1,INST2;PROFILE2:INST3,..."""
    mapping: Dict[str, str] = {}
    if not raw:
        return mapping
    for item in raw.split(";"):
        pair = item.strip()
        if not pair:
            continue
        if ":" not in pair:
            raise ValueError(
                f"Invalid STRAT_PROFILE_INST_GROUPS item: {pair} "
                "(expected PROFILE:INST1,INST2)"
            )
        profile_id, inst_list = pair.split(":", 1)
        pid = _normalize_profile_id(profile_id)
        inst_ids = parse_inst_ids(inst_list)
        if not inst_ids:
            raise ValueError(
                f"Invalid STRAT_PROFILE_INST_GROUPS item: {pair} "
                "(no valid inst id)"
            )
        for inst in inst_ids:
            mapping[inst] = pid
    return mapping


def _parse_profile_id_list(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    norm = text.replace("|", "+").replace("/", "+")
    out: list[str] = []
    seen: set[str] = set()
    for item in norm.split("+"):
        if not str(item).strip():
            continue
        profile_id = _normalize_profile_id(item)
        if profile_id in seen:
            continue
        seen.add(profile_id)
        out.append(profile_id)
    return out


def _parse_strategy_profile_vote_map(raw: str) -> Dict[str, list[str]]:
    mapping: Dict[str, list[str]] = {}
    if not raw:
        return mapping
    for item in raw.split(","):
        pair = item.strip()
        if not pair:
            continue
        if ":" not in pair:
            raise ValueError(f"Invalid STRAT_PROFILE_VOTE_MAP item: {pair} (expected INST:PROFILE1+PROFILE2)")
        inst_id, profile_list = pair.split(":", 1)
        inst = str(inst_id).strip().upper()
        if not inst:
            continue
        profiles = _parse_profile_id_list(profile_list)
        if not profiles:
            raise ValueError(f"Invalid STRAT_PROFILE_VOTE_MAP item: {pair} (no valid profile id)")
        mapping[inst] = profiles
    return mapping


def _parse_strategy_profile_vote_inst_groups(raw: str) -> Dict[str, list[str]]:
    """Parse grouped vote mapping: PROFILE1+PROFILE2:INST1,INST2;PROFILE3+PROFILE4:INST3"""
    mapping: Dict[str, list[str]] = {}
    if not raw:
        return mapping
    for item in raw.split(";"):
        pair = item.strip()
        if not pair:
            continue
        if ":" not in pair:
            raise ValueError(
                f"Invalid STRAT_PROFILE_VOTE_INST_GROUPS item: {pair} "
                "(expected PROFILE1+PROFILE2:INST1,INST2)"
            )
        profile_list, inst_list = pair.split(":", 1)
        profiles = _parse_profile_id_list(profile_list)
        if not profiles:
            raise ValueError(
                f"Invalid STRAT_PROFILE_VOTE_INST_GROUPS item: {pair} "
                "(no valid profile id)"
            )
        inst_ids = parse_inst_ids(inst_list)
        if not inst_ids:
            raise ValueError(
                f"Invalid STRAT_PROFILE_VOTE_INST_GROUPS item: {pair} "
                "(no valid inst id)"
            )
        for inst in inst_ids:
            mapping[inst] = profiles
    return mapping


def _parse_strategy_profile_vote_score_map(raw: str) -> Dict[str, float]:
    mapping: Dict[str, float] = {}
    if not raw:
        return mapping
    for item in raw.split(","):
        pair = item.strip()
        if not pair:
            continue
        if "=" not in pair:
            raise ValueError(
                f"Invalid STRAT_PROFILE_VOTE_SCORE_MAP item: {pair} "
                "(expected PROFILE=score)"
            )
        profile_id, score_text = pair.split("=", 1)
        pid = _normalize_profile_id(profile_id)
        try:
            score = float(str(score_text).strip())
        except Exception as exc:
            raise ValueError(
                f"Invalid STRAT_PROFILE_VOTE_SCORE_MAP score for {profile_id}: {score_text}"
            ) from exc
        mapping[pid] = score
    return mapping


def _parse_strategy_profile_vote_fallback_profiles(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not raw:
        return out
    for item in str(raw).split(","):
        item = item.strip()
        if not item:
            continue
        for pid in _parse_profile_id_list(item):
            if pid in seen:
                continue
            seen.add(pid)
            out.append(pid)
    return out


def _parse_profile_override_value(
    env_key: str,
    raw_value: str,
    current_value: object,
    field_name: str = "",
) -> object:
    try:
        if isinstance(current_value, bool):
            return parse_bool(raw_value, current_value)
        if isinstance(current_value, int) and not isinstance(current_value, bool):
            return int(raw_value)
        if isinstance(current_value, float):
            allow_percent = field_name in {"risk_frac", "risk_max_margin_frac", "daily_loss_limit_pct"}
            require_percent = field_name in {"risk_frac", "risk_max_margin_frac"}
            return _parse_float_env(
                raw_value,
                env_key=env_key,
                allow_percent=allow_percent,
                require_percent=require_percent,
            )
        if isinstance(current_value, list):
            return parse_inst_ids(raw_value)
        return str(raw_value).strip()
    except Exception as exc:
        raise ValueError(f"{env_key} invalid value: {raw_value}") from exc


def _resolve_profile_field(suffix: str) -> Optional[str]:
    if not suffix:
        return None
    key = suffix.strip().upper()
    mapped = _STRATEGY_PROFILE_SUFFIX_ALIAS.get(key)
    if mapped is not None:
        return mapped
    candidate = key.lower()
    if candidate in StrategyParams.__dataclass_fields__:
        return candidate
    return None


def _build_profile_params(base_params: StrategyParams, profile_id: str) -> StrategyParams:
    prefix = f"STRAT_PROFILE_{profile_id}_"
    overrides: Dict[str, object] = {}
    unknown_keys = []
    for env_key, raw_value in os.environ.items():
        if not env_key.startswith(prefix):
            continue
        suffix = env_key[len(prefix) :].strip().upper()
        field_name = _resolve_profile_field(suffix)
        if field_name is None:
            unknown_keys.append(env_key)
            continue
        current_value = getattr(base_params, field_name)
        overrides[field_name] = _parse_profile_override_value(
            env_key,
            raw_value,
            current_value,
            field_name=field_name,
        )
    if unknown_keys:
        raise ValueError("Unknown strategy profile env keys: " + ", ".join(sorted(unknown_keys)))

    if not overrides:
        prof = replace(base_params)
    else:
        prof = replace(base_params, **overrides)
    _normalize_strategy_params(prof)
    return prof


def get_strategy_profile_id(cfg: Config, inst_id: str) -> str:
    key = str(inst_id or "").strip().upper()
    profile_id = cfg.strategy_profile_map.get(key, "DEFAULT")
    profile_id = _normalize_profile_id(profile_id)
    if profile_id not in cfg.strategy_profiles:
        return "DEFAULT"
    return profile_id


def get_strategy_profile_ids(cfg: Config, inst_id: str) -> list[str]:
    key = str(inst_id or "").strip().upper()
    from_vote = cfg.strategy_profile_vote_map.get(key)
    if isinstance(from_vote, list) and from_vote:
        out: list[str] = []
        for profile_id in from_vote:
            norm_id = _normalize_profile_id(profile_id)
            if norm_id in cfg.strategy_profiles and norm_id not in out:
                out.append(norm_id)
        if out:
            return out
    return [get_strategy_profile_id(cfg, key)]


def get_strategy_params(cfg: Config, inst_id: str) -> StrategyParams:
    profile_id = get_strategy_profile_id(cfg, inst_id)
    return cfg.strategy_profiles.get(profile_id, cfg.params)


def resolve_exec_max_level(params: StrategyParams, inst_id: str) -> int:
    inst = str(inst_id or "").strip().upper()
    inst_l3_set = set(str(x).strip().upper() for x in params.exec_l3_inst_ids if str(x).strip())
    out = int(params.exec_max_level)
    if inst in inst_l3_set:
        out = 3
    if out < 1:
        out = 1
    if out > 3:
        out = 3
    return out


def read_config(state_file_override: Optional[str]) -> Config:
    params = _build_base_strategy_params()
    _normalize_strategy_params(params)

    inst_ids = parse_inst_ids(os.getenv("OKX_INST_IDS", ""))
    if not inst_ids:
        inst_ids = [os.getenv("OKX_INST_ID", "XAU-USDT-SWAP").strip().upper()]

    strategy_profile_map_grouped = _parse_strategy_profile_inst_groups(os.getenv("STRAT_PROFILE_INST_GROUPS", ""))
    strategy_profile_map_explicit = _parse_strategy_profile_map(os.getenv("STRAT_PROFILE_MAP", ""))
    strategy_profile_map = dict(strategy_profile_map_grouped)
    strategy_profile_map.update(strategy_profile_map_explicit)

    strategy_profile_vote_map_grouped = _parse_strategy_profile_vote_inst_groups(
        os.getenv("STRAT_PROFILE_VOTE_INST_GROUPS", "")
    )
    strategy_profile_vote_map_explicit = _parse_strategy_profile_vote_map(os.getenv("STRAT_PROFILE_VOTE_MAP", ""))
    strategy_profile_vote_map = dict(strategy_profile_vote_map_grouped)
    strategy_profile_vote_map.update(strategy_profile_vote_map_explicit)
    strategy_profile_vote_mode = os.getenv("STRAT_PROFILE_VOTE_MODE", "majority").strip().lower()
    strategy_profile_vote_min_agree = int(os.getenv("STRAT_PROFILE_VOTE_MIN_AGREE", "1"))
    strategy_profile_vote_score_map = _parse_strategy_profile_vote_score_map(
        os.getenv("STRAT_PROFILE_VOTE_SCORE_MAP", "")
    )
    strategy_profile_vote_level_weight = float(os.getenv("STRAT_PROFILE_VOTE_LEVEL_WEIGHT", "0.0"))
    strategy_profile_vote_fallback_profiles = _parse_strategy_profile_vote_fallback_profiles(
        os.getenv("STRAT_PROFILE_VOTE_FALLBACK_PROFILES", "")
    )
    strategy_profiles: Dict[str, StrategyParams] = {"DEFAULT": params}
    profile_ids_needed = set(strategy_profile_map.values())
    for id_list in strategy_profile_vote_map.values():
        profile_ids_needed.update(id_list)
    profile_ids_needed.update(strategy_profile_vote_score_map.keys())
    profile_ids_needed.update(strategy_profile_vote_fallback_profiles)
    for profile_id in sorted(profile_ids_needed):
        if profile_id == "DEFAULT":
            continue
        strategy_profiles[profile_id] = _build_profile_params(params, profile_id)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_state = os.path.join(base_dir, ".okx_auto_trader_state.json")
    default_alert_file = os.path.join(base_dir, "alerts.log")
    default_trade_journal_file = os.path.join(base_dir, "trade_journal.csv")
    default_history_cache_dir = os.path.join(base_dir, ".cache", "history")
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
        ws_tp1_be_enabled=parse_bool(os.getenv("OKX_WS_TP1_BE_ENABLED", "1"), True),
        ws_private_url=os.getenv(
            "OKX_WS_PRIVATE_URL",
            "wss://wspap.okx.com:8443/ws/v5/private" if parse_bool(os.getenv("OKX_PAPER", "1"), True) else "wss://ws.okx.com:8443/ws/v5/private",
        ).strip(),
        ws_reconnect_seconds=max(1, int(os.getenv("OKX_WS_RECONNECT_SECONDS", "3"))),
        candle_limit=max(120, int(os.getenv("OKX_CANDLE_LIMIT", "300"))),
        history_cache_enabled=parse_bool(os.getenv("OKX_HISTORY_CACHE_ENABLED", "1"), True),
        history_cache_dir=os.getenv("OKX_HISTORY_CACHE_DIR", default_history_cache_dir).strip(),
        history_cache_ttl_seconds=max(0, int(os.getenv("OKX_HISTORY_CACHE_TTL_SECONDS", "21600"))),
        td_mode=os.getenv("OKX_TD_MODE", "cross"),
        pos_mode=os.getenv("OKX_POS_MODE", "net").lower(),
        order_size=float(os.getenv("OKX_ORDER_SIZE", "1")),
        sizing_mode=os.getenv("OKX_SIZING_MODE", "fixed").lower(),
        margin_usdt=float(os.getenv("OKX_MARGIN_USDT", "25")),
        leverage=float(os.getenv("OKX_LEVERAGE", "5")),
        attach_tpsl_on_entry=parse_bool(os.getenv("OKX_ATTACH_TPSL_ON_ENTRY", "0"), False),
        attach_tpsl_tp_r=float(os.getenv("OKX_ATTACH_TPSL_TP_R", "2.5")),
        attach_tpsl_trigger_px_type=os.getenv("OKX_ATTACH_TPSL_TRIGGER_PX_TYPE", "last").strip().lower(),
        compound_enabled=parse_bool(os.getenv("OKX_COMPOUND_ENABLE", "0"), False),
        compound_mode=os.getenv("OKX_COMPOUND_MODE", "step").strip().lower(),
        compound_base_equity=float(os.getenv("OKX_COMPOUND_BASE_EQUITY", "1000")),
        compound_base_margin=float(os.getenv("OKX_COMPOUND_BASE_MARGIN", "0")),
        compound_step_equity=float(os.getenv("OKX_COMPOUND_STEP_EQUITY", "250")),
        compound_step_margin=float(os.getenv("OKX_COMPOUND_STEP_MARGIN", "2")),
        compound_ratio_power=float(os.getenv("OKX_COMPOUND_RATIO_POWER", "1.0")),
        compound_min_margin=float(os.getenv("OKX_COMPOUND_MIN_MARGIN", "5")),
        compound_max_margin=float(os.getenv("OKX_COMPOUND_MAX_MARGIN", "500")),
        compound_dd_guard_pct=float(os.getenv("OKX_COMPOUND_DD_GUARD_PCT", "0.15")),
        compound_dd_factor=float(os.getenv("OKX_COMPOUND_DD_FACTOR", "0.5")),
        compound_balance_ccy=os.getenv("OKX_COMPOUND_BALANCE_CCY", "USDT").strip().upper(),
        compound_cache_seconds=max(0, int(os.getenv("OKX_COMPOUND_CACHE_SECONDS", "30"))),
        state_file=state_file_override or os.getenv("OKX_STATE_FILE", default_state),
        user_agent=os.getenv(
            "OKX_USER_AGENT",
            (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        log_heartbeat_seconds=max(30, int(os.getenv("LOG_HEARTBEAT_SECONDS", "300"))),
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
        alert_tg_trade_exec_enabled=parse_bool(os.getenv("ALERT_TG_TRADE_EXEC_ENABLED", "1"), True),
        alert_tg_bot_token=os.getenv("ALERT_TG_BOT_TOKEN", "").strip(),
        alert_tg_chat_id=os.getenv("ALERT_TG_CHAT_ID", "").strip(),
        alert_tg_api_base=os.getenv("ALERT_TG_API_BASE", "https://api.telegram.org").strip(),
        alert_tg_parse_mode=os.getenv("ALERT_TG_PARSE_MODE", "").strip(),
        alert_max_level=int(os.getenv("ALERT_MAX_LEVEL", "1")),
        alert_intrabar_enabled=parse_bool(os.getenv("ALERT_INTRABAR_ENABLED", "1"), True),
        alert_stats_keep_days=max(1, int(os.getenv("ALERT_STATS_KEEP_DAYS", "14"))),
        alert_no_open_hours=max(0.0, float(os.getenv("ALERT_NO_OPEN_HOURS", "24"))),
        alert_no_open_cooldown_hours=max(0.0, float(os.getenv("ALERT_NO_OPEN_COOLDOWN_HOURS", "24"))),
        alert_local_sound=parse_bool(os.getenv("ALERT_LOCAL_SOUND", "1"), True),
        alert_local_file=parse_bool(os.getenv("ALERT_LOCAL_FILE", "1"), True),
        alert_local_file_path=os.getenv("ALERT_LOCAL_FILE_PATH", default_alert_file).strip(),
        trade_journal_enabled=parse_bool(os.getenv("TRADE_JOURNAL_ENABLED", "1"), True),
        trade_journal_path=os.getenv("TRADE_JOURNAL_PATH", default_trade_journal_file).strip(),
        trade_order_link_enabled=parse_bool(os.getenv("TRADE_ORDER_LINK_ENABLED", "1"), True),
        trade_order_link_path=os.getenv("TRADE_ORDER_LINK_PATH", "").strip(),
        params=params,
        strategy_profile_map=strategy_profile_map,
        strategy_profile_vote_map=strategy_profile_vote_map,
        strategy_profile_vote_mode=strategy_profile_vote_mode,
        strategy_profile_vote_min_agree=strategy_profile_vote_min_agree,
        strategy_profile_vote_score_map=strategy_profile_vote_score_map,
        strategy_profile_vote_level_weight=strategy_profile_vote_level_weight,
        strategy_profile_vote_fallback_profiles=strategy_profile_vote_fallback_profiles,
        strategy_profiles=strategy_profiles,
    )
    if cfg.pos_mode not in {"net", "long_short"}:
        raise ValueError("OKX_POS_MODE must be net or long_short")
    if cfg.td_mode not in {"cross", "isolated"}:
        raise ValueError("OKX_TD_MODE must be cross or isolated")
    if cfg.sizing_mode not in {"fixed", "margin"}:
        raise ValueError("OKX_SIZING_MODE must be fixed or margin")
    if not cfg.history_cache_dir:
        cfg.history_cache_dir = default_history_cache_dir
    if not cfg.trade_journal_path:
        cfg.trade_journal_path = default_trade_journal_file
    if not cfg.trade_order_link_path:
        base, ext = os.path.splitext(cfg.trade_journal_path)
        if ext.lower() == ".csv":
            cfg.trade_order_link_path = f"{base}_order_links.csv"
        else:
            cfg.trade_order_link_path = f"{cfg.trade_journal_path}.order_links.csv"
    if cfg.log_level not in {"DEBUG", "INFO", "WARN", "ERROR"}:
        cfg.log_level = "INFO"
    if not cfg.ws_private_url:
        cfg.ws_private_url = "wss://ws.okx.com:8443/ws/v5/private"
    if cfg.ws_reconnect_seconds < 1:
        cfg.ws_reconnect_seconds = 1
    if cfg.log_heartbeat_seconds < 30:
        cfg.log_heartbeat_seconds = 30
    if cfg.attach_tpsl_tp_r <= 0:
        cfg.attach_tpsl_tp_r = 2.5
    if cfg.attach_tpsl_trigger_px_type not in {"last", "index", "mark"}:
        cfg.attach_tpsl_trigger_px_type = "last"
    if cfg.compound_mode not in {"step", "ratio"}:
        cfg.compound_mode = "step"
    if cfg.compound_base_equity <= 0:
        cfg.compound_base_equity = 1000.0
    if cfg.compound_base_margin <= 0:
        cfg.compound_base_margin = cfg.margin_usdt
    if cfg.compound_step_equity <= 0:
        cfg.compound_step_equity = 250.0
    if cfg.compound_step_margin <= 0:
        cfg.compound_step_margin = max(cfg.compound_base_margin * 0.1, 1.0)
    if cfg.compound_ratio_power <= 0:
        cfg.compound_ratio_power = 1.0
    if cfg.compound_min_margin <= 0:
        cfg.compound_min_margin = min(cfg.compound_base_margin, cfg.margin_usdt)
    if cfg.compound_max_margin < cfg.compound_min_margin:
        cfg.compound_max_margin = cfg.compound_min_margin
    if cfg.compound_dd_guard_pct < 0:
        cfg.compound_dd_guard_pct = 0.0
    if cfg.compound_dd_factor <= 0:
        cfg.compound_dd_factor = 1.0
    if cfg.compound_dd_factor > 1:
        cfg.compound_dd_factor = 1.0
    if cfg.alert_max_level < 1:
        cfg.alert_max_level = 1
    if cfg.alert_max_level > 3:
        cfg.alert_max_level = 3
    if cfg.alert_stats_keep_days < 1:
        cfg.alert_stats_keep_days = 1
    if cfg.alert_no_open_hours < 0:
        cfg.alert_no_open_hours = 0.0
    if cfg.alert_no_open_cooldown_hours < 0:
        cfg.alert_no_open_cooldown_hours = 0.0
    if cfg.alert_no_open_hours > 0 and cfg.alert_no_open_cooldown_hours <= 0:
        cfg.alert_no_open_cooldown_hours = cfg.alert_no_open_hours
    if cfg.strategy_profile_vote_mode not in {"any", "majority", "unanimous"}:
        cfg.strategy_profile_vote_mode = "majority"
    if cfg.strategy_profile_vote_min_agree < 1:
        cfg.strategy_profile_vote_min_agree = 1
    if cfg.strategy_profile_vote_level_weight < 0:
        cfg.strategy_profile_vote_level_weight = 0.0
    normalized_fallback_profiles: list[str] = []
    seen_fallback: set[str] = set()
    for raw_id in cfg.strategy_profile_vote_fallback_profiles:
        pid = _normalize_profile_id(raw_id)
        if pid not in cfg.strategy_profiles:
            continue
        if pid in seen_fallback:
            continue
        seen_fallback.add(pid)
        normalized_fallback_profiles.append(pid)
    cfg.strategy_profile_vote_fallback_profiles = normalized_fallback_profiles
    normalized_vote_map: Dict[str, list[str]] = {}
    for inst, ids in cfg.strategy_profile_vote_map.items():
        picked: list[str] = []
        for raw_id in ids:
            pid = _normalize_profile_id(raw_id)
            if pid not in cfg.strategy_profiles:
                continue
            if pid in picked:
                continue
            picked.append(pid)
        if picked:
            normalized_vote_map[str(inst).strip().upper()] = picked
    cfg.strategy_profile_vote_map = normalized_vote_map
    normalized_score_map: Dict[str, float] = {}
    for profile_id, score in cfg.strategy_profile_vote_score_map.items():
        pid = _normalize_profile_id(profile_id)
        if pid not in cfg.strategy_profiles:
            continue
        normalized_score_map[pid] = float(score)
    cfg.strategy_profile_vote_score_map = normalized_score_map
    _normalize_strategy_params(cfg.params)
    for profile_id, profile_params in cfg.strategy_profiles.items():
        _normalize_strategy_params(profile_params)
        cfg.strategy_profiles[profile_id] = profile_params
    return cfg
