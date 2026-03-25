from __future__ import annotations

from typing import Any, Dict, Mapping, MutableMapping


def normalize_tp2_reentry_windows(
    block_hours: float,
    partial_until_hours: float,
    partial_max_level: int,
) -> tuple[float, float, int]:
    block = max(0.0, float(block_hours or 0.0))
    partial_until = max(0.0, float(partial_until_hours or 0.0))
    if partial_until < block:
        partial_until = block
    max_level = int(partial_max_level or 0)
    if max_level < 0:
        max_level = 0
    if max_level > 3:
        max_level = 3
    return block, partial_until, max_level


def _hours_to_ms(hours: float) -> int:
    return int(max(0.0, float(hours or 0.0)) * 3600 * 1000)


def arm_tp2_reentry_bucket(
    bucket: MutableMapping[str, Any],
    *,
    event_ts_ms: int,
    block_hours: float,
    partial_until_hours: float,
) -> Dict[str, int]:
    block, partial_until, _ = normalize_tp2_reentry_windows(block_hours, partial_until_hours, 0)
    out: Dict[str, int] = {}

    if block > 0:
        block_until = int(event_ts_ms) + _hours_to_ms(block)
        prev_block_until = int(bucket.get("tp2_cooldown_until_ts_ms", 0) or 0)
        if block_until > prev_block_until:
            bucket["tp2_cooldown_until_ts_ms"] = block_until
        out["tp2_cooldown_until_ts_ms"] = int(bucket.get("tp2_cooldown_until_ts_ms", 0) or 0)

    if partial_until > block:
        partial_until_ts = int(event_ts_ms) + _hours_to_ms(partial_until)
        prev_partial_until_ts = int(bucket.get("tp2_partial_until_ts_ms", 0) or 0)
        if partial_until_ts > prev_partial_until_ts:
            bucket["tp2_partial_until_ts_ms"] = partial_until_ts
        out["tp2_partial_until_ts_ms"] = int(bucket.get("tp2_partial_until_ts_ms", 0) or 0)

    return out


def get_tp2_reentry_gate(
    bucket: Mapping[str, Any],
    *,
    now_ts_ms: int,
    planned_level: int,
    block_hours: float,
    partial_until_hours: float,
    partial_max_level: int,
) -> Dict[str, int | str]:
    block, partial_until, max_level = normalize_tp2_reentry_windows(
        block_hours,
        partial_until_hours,
        partial_max_level,
    )
    now_ts = int(now_ts_ms)
    level = int(planned_level or 0)

    block_until_ts = int(bucket.get("tp2_cooldown_until_ts_ms", 0) or 0)
    if block > 0 and block_until_ts > now_ts:
        remain_ms = max(0, block_until_ts - now_ts)
        remain_min = max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))
        return {
            "status": "block",
            "remaining_ms": remain_ms,
            "remaining_minutes": remain_min,
            "required_level": 0,
        }

    partial_until_ts = int(bucket.get("tp2_partial_until_ts_ms", 0) or 0)
    if partial_until > block and 0 < max_level < 3 and partial_until_ts > now_ts and level > max_level:
        remain_ms = max(0, partial_until_ts - now_ts)
        remain_min = max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))
        return {
            "status": "partial_level_cap",
            "remaining_ms": remain_ms,
            "remaining_minutes": remain_min,
            "required_level": max_level,
        }

    return {
        "status": "allow",
        "remaining_ms": 0,
        "remaining_minutes": 0,
        "required_level": 0,
    }
