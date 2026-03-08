from __future__ import annotations

from typing import Any


_VALID_ENTRY_EXEC_MODES = {"market", "limit", "auto"}
_VALID_LIMIT_FALLBACK_MODES = {"market", "skip"}


def normalize_entry_exec_mode(mode: str, *, allow_empty: bool = False) -> str:
    m = str(mode or "").strip().lower()
    if allow_empty and not m:
        return ""
    if m not in _VALID_ENTRY_EXEC_MODES:
        return "" if allow_empty else "market"
    return m


def normalize_entry_limit_fallback_mode(mode: str, *, allow_empty: bool = False) -> str:
    m = str(mode or "").strip().lower()
    if allow_empty and not m:
        return ""
    if m not in _VALID_LIMIT_FALLBACK_MODES:
        return "" if allow_empty else "market"
    return m


def resolve_entry_exec_mode(
    mode: str,
    level: int,
    auto_market_level_min: int,
    auto_market_level_max: int,
    *,
    level_mode_l1: str = "",
    level_mode_l2: str = "",
    level_mode_l3: str = "",
) -> str:
    override_raw = {1: level_mode_l1, 2: level_mode_l2, 3: level_mode_l3}.get(max(1, min(3, int(level or 0))), "")
    override = normalize_entry_exec_mode(override_raw, allow_empty=True)
    if override:
        return override

    norm = normalize_entry_exec_mode(mode)
    if norm != "auto":
        return norm
    max_level = max(0, min(3, int(auto_market_level_max)))
    if max_level > 0:
        return "market" if int(level) <= max_level else "limit"
    threshold = max(1, min(3, int(auto_market_level_min)))
    return "market" if int(level) >= threshold else "limit"


def resolve_entry_exec_mode_for_params(params: Any, level: int) -> str:
    return resolve_entry_exec_mode(
        str(getattr(params, "entry_exec_mode", "market") or "market"),
        int(level),
        int(getattr(params, "entry_auto_market_level_min", 3) or 3),
        int(getattr(params, "entry_auto_market_level_max", 0) or 0),
        level_mode_l1=str(getattr(params, "entry_exec_mode_l1", "") or ""),
        level_mode_l2=str(getattr(params, "entry_exec_mode_l2", "") or ""),
        level_mode_l3=str(getattr(params, "entry_exec_mode_l3", "") or ""),
    )


def resolve_entry_limit_ttl_sec(default_ttl_sec: int, level: int, *, ttl_sec_l1: int = -1, ttl_sec_l2: int = -1, ttl_sec_l3: int = -1) -> int:
    override = {1: ttl_sec_l1, 2: ttl_sec_l2, 3: ttl_sec_l3}.get(max(1, min(3, int(level or 0))), -1)
    try:
        override_i = int(override)
    except Exception:
        override_i = -1
    if override_i >= 0:
        return override_i
    try:
        base = int(default_ttl_sec)
    except Exception:
        base = 0
    return max(0, base)


def resolve_entry_limit_ttl_sec_for_params(params: Any, level: int) -> int:
    return resolve_entry_limit_ttl_sec(
        int(getattr(params, "entry_limit_ttl_sec", 10)),
        int(level),
        ttl_sec_l1=int(getattr(params, "entry_limit_ttl_sec_l1", -1)),
        ttl_sec_l2=int(getattr(params, "entry_limit_ttl_sec_l2", -1)),
        ttl_sec_l3=int(getattr(params, "entry_limit_ttl_sec_l3", -1)),
    )


def resolve_entry_limit_fallback_mode(
    default_mode: str,
    level: int,
    *,
    fallback_mode_l1: str = "",
    fallback_mode_l2: str = "",
    fallback_mode_l3: str = "",
) -> str:
    override_raw = {1: fallback_mode_l1, 2: fallback_mode_l2, 3: fallback_mode_l3}.get(max(1, min(3, int(level or 0))), "")
    override = normalize_entry_limit_fallback_mode(override_raw, allow_empty=True)
    if override:
        return override
    return normalize_entry_limit_fallback_mode(default_mode)


def resolve_entry_limit_fallback_mode_for_params(params: Any, level: int) -> str:
    return resolve_entry_limit_fallback_mode(
        str(getattr(params, "entry_limit_fallback_mode", "market") or "market"),
        int(level),
        fallback_mode_l1=str(getattr(params, "entry_limit_fallback_mode_l1", "") or ""),
        fallback_mode_l2=str(getattr(params, "entry_limit_fallback_mode_l2", "") or ""),
        fallback_mode_l3=str(getattr(params, "entry_limit_fallback_mode_l3", "") or ""),
    )
