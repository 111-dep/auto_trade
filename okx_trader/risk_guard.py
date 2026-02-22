from __future__ import annotations

from typing import Any, Deque, Dict, Iterable, List, MutableMapping, Optional, Tuple


def normalize_loss_base_mode(mode: str) -> str:
    m = str(mode or "current").strip().lower()
    if m not in {"fixed", "current", "min"}:
        return "current"
    return m


def resolve_loss_base(mode: str, equity: Optional[float], fixed_base: float) -> float:
    m = normalize_loss_base_mode(mode)
    eq = float(equity or 0.0)
    fixed = float(fixed_base or 0.0)

    if m == "fixed":
        return fixed
    if m == "min":
        if eq > 0:
            if fixed > 0:
                return min(fixed, eq)
            return eq
        return fixed
    # current
    if eq > 0:
        return eq
    return fixed


def open_window_ms(window_hours: int) -> int:
    try:
        hours = int(window_hours)
    except Exception:
        hours = 24
    return int(max(1, hours) * 3600 * 1000)


def prune_state_ts_list(
    state: MutableMapping[str, Any],
    key: str,
    *,
    now_ts_ms: int,
    window_ms: int,
    allow_future_ms: Optional[int] = None,
) -> List[int]:
    raw = state.get(key)
    if not isinstance(raw, list):
        raw = []

    kept: List[int] = []
    future_limit = None if allow_future_ms is None else (int(now_ts_ms) + int(max(0, allow_future_ms)))
    for item in raw:
        try:
            ts = int(item)
        except Exception:
            continue
        if future_limit is not None and ts > future_limit:
            continue
        if int(now_ts_ms) - ts > int(window_ms):
            continue
        kept.append(ts)

    state[key] = kept
    return kept


def prune_state_loss_events(
    state: MutableMapping[str, Any],
    key: str,
    *,
    now_ts_ms: int,
    window_ms: int,
    allow_future_ms: int = 0,
) -> List[Dict[str, Any]]:
    raw = state.get(key)
    if not isinstance(raw, list):
        raw = []

    kept: List[Dict[str, Any]] = []
    future_limit = int(now_ts_ms) + int(max(0, allow_future_ms))
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            ts_ms = int(item.get("ts_ms"))
            loss_usdt = float(item.get("loss_usdt"))
        except Exception:
            continue
        if ts_ms > future_limit:
            continue
        if int(now_ts_ms) - ts_ms > int(window_ms):
            continue
        if loss_usdt <= 0:
            continue
        kept.append(
            {
                "ts_ms": ts_ms,
                "loss_usdt": float(loss_usdt),
                "inst_id": str(item.get("inst_id", "")),
                "reason": str(item.get("reason", "")),
            }
        )

    state[key] = kept
    return kept


def rolling_loss_sum(loss_events: Iterable[Dict[str, Any]]) -> float:
    total = 0.0
    for ev in loss_events:
        try:
            total += max(0.0, float(ev.get("loss_usdt", 0.0)))
        except Exception:
            continue
    return total


def is_daily_loss_halted(loss_sum: float, base_usdt: float, limit_ratio: float, eps: float = 1e-12) -> bool:
    if float(limit_ratio) <= 0:
        return False
    if float(base_usdt) <= 0:
        return False
    return float(loss_sum) + float(eps) >= float(base_usdt) * float(limit_ratio)


def min_open_gap_remaining_minutes(now_ts_ms: int, last_open_ts_ms: Optional[int], min_gap_minutes: int) -> int:
    try:
        min_gap = int(min_gap_minutes)
    except Exception:
        min_gap = 0
    if min_gap <= 0 or last_open_ts_ms is None:
        return 0
    gap_ms = int(now_ts_ms) - int(last_open_ts_ms)
    min_gap_ms = min_gap * 60 * 1000
    if gap_ms >= min_gap_ms:
        return 0
    remain_ms = max(0, min_gap_ms - gap_ms)
    return max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))


def is_open_limit_reached(current_count: int, limit: int) -> bool:
    try:
        l = int(limit)
    except Exception:
        l = 0
    if l <= 0:
        return False
    return int(current_count) >= l


def prune_ts_deque_window(entries: Deque[int], now_ts_ms: int, window_ms: int) -> None:
    while entries and int(now_ts_ms) - int(entries[0]) > int(window_ms):
        entries.popleft()


def prune_loss_deque_window(entries: Deque[Tuple[int, float]], now_ts_ms: int, window_ms: int) -> None:
    while entries and int(now_ts_ms) - int(entries[0][0]) > int(window_ms):
        entries.popleft()
