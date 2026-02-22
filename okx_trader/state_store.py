from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, Optional

from .models import Config


def load_state(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(path: str, state: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def day_key_from_ts_ms(ts_ms: int) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d")
    except Exception:
        return dt.datetime.utcnow().strftime("%Y-%m-%d")


def _prune_key_map_by_day(key_map: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    if not isinstance(key_map, dict):
        return {}
    keep_days = max(1, int(keep_days))
    oldest = (dt.datetime.utcnow().date() - dt.timedelta(days=keep_days - 1)).strftime("%Y-%m-%d")
    out: Dict[str, Any] = {}
    for k, v in key_map.items():
        if not isinstance(k, str):
            continue
        day = k.split(":", 1)[0]
        if len(day) != 10:
            continue
        if day >= oldest:
            out[k] = v
    return out


def _prune_daily_stats(daily: Dict[str, Any], keep_days: int) -> Dict[str, Any]:
    if not isinstance(daily, dict):
        return {}
    keep_days = max(1, int(keep_days))
    oldest = (dt.datetime.utcnow().date() - dt.timedelta(days=keep_days - 1)).strftime("%Y-%m-%d")
    out: Dict[str, Any] = {}
    for day, bucket in daily.items():
        if not isinstance(day, str) or len(day) != 10:
            continue
        if day >= oldest and isinstance(bucket, dict):
            out[day] = bucket
    return out


def _get_daily_bucket(inst_state: Dict[str, Any], day: str) -> Dict[str, Any]:
    daily = inst_state.get("daily_stats")
    if not isinstance(daily, dict):
        daily = {}
        inst_state["daily_stats"] = daily

    bucket = daily.get(day)
    if not isinstance(bucket, dict):
        bucket = {}
        daily[day] = bucket

    defaults = {
        "opp_total": 0,
        "opp_l1": 0,
        "opp_l2": 0,
        "opp_l3": 0,
        "opp_long": 0,
        "opp_short": 0,
        "opp_live": 0,
        "opp_confirm": 0,
        "alert_total": 0,
        "alert_l1": 0,
        "alert_l2": 0,
        "alert_l3": 0,
        "alert_long": 0,
        "alert_short": 0,
        "alert_live": 0,
        "alert_confirm": 0,
    }
    for k, v in defaults.items():
        try:
            bucket[k] = int(bucket.get(k, v))
        except Exception:
            bucket[k] = v
    return bucket


def _record_opportunity(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
    signal_confirm: bool,
    side: str,
    level: int,
) -> None:
    if level <= 0:
        return
    side_u = side.strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return

    day = day_key_from_ts_ms(signal_ts_ms)
    stage = "C" if signal_confirm else "L"
    key = f"{day}:{int(signal_ts_ms)}:{side_u}:{stage}"

    seen = inst_state.get("opp_seen_levels")
    if not isinstance(seen, dict):
        seen = {}
        inst_state["opp_seen_levels"] = seen
    seen = _prune_key_map_by_day(seen, cfg.alert_stats_keep_days)
    inst_state["opp_seen_levels"] = seen

    daily = inst_state.get("daily_stats")
    inst_state["daily_stats"] = _prune_daily_stats(daily, cfg.alert_stats_keep_days)
    bucket = _get_daily_bucket(inst_state, day)

    prev_raw = seen.get(key)
    prev_level: Optional[int] = None
    try:
        prev_level = int(prev_raw) if prev_raw is not None else None
    except Exception:
        prev_level = None

    if prev_level is None:
        bucket["opp_total"] += 1
        bucket[f"opp_l{level}"] += 1
        if side_u == "LONG":
            bucket["opp_long"] += 1
        else:
            bucket["opp_short"] += 1
        if signal_confirm:
            bucket["opp_confirm"] += 1
        else:
            bucket["opp_live"] += 1
        seen[key] = level
        return

    # If the same side/stage signal strengthens (e.g., L3 -> L2 -> L1), upgrade bucket.
    if level < prev_level:
        old_key = f"opp_l{prev_level}"
        new_key = f"opp_l{level}"
        bucket[old_key] = max(0, int(bucket.get(old_key, 0)) - 1)
        bucket[new_key] = int(bucket.get(new_key, 0)) + 1
        seen[key] = level


def _mark_alert_sent(cfg: Config, inst_state: Dict[str, Any], alert_key: str) -> bool:
    sent = inst_state.get("sent_alert_keys")
    if not isinstance(sent, dict):
        sent = {}
        inst_state["sent_alert_keys"] = sent
    sent = _prune_key_map_by_day(sent, cfg.alert_stats_keep_days)
    inst_state["sent_alert_keys"] = sent
    if alert_key in sent:
        return False
    sent[alert_key] = 1
    return True


def _record_alert(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
    signal_confirm: bool,
    side: str,
    level: int,
) -> None:
    side_u = side.strip().upper()
    if side_u not in {"LONG", "SHORT"}:
        return
    if level <= 0:
        return

    day = day_key_from_ts_ms(signal_ts_ms)
    daily = inst_state.get("daily_stats")
    inst_state["daily_stats"] = _prune_daily_stats(daily, cfg.alert_stats_keep_days)
    bucket = _get_daily_bucket(inst_state, day)

    bucket["alert_total"] += 1
    bucket[f"alert_l{level}"] += 1
    if side_u == "LONG":
        bucket["alert_long"] += 1
    else:
        bucket["alert_short"] += 1
    if signal_confirm:
        bucket["alert_confirm"] += 1
    else:
        bucket["alert_live"] += 1

def _migrate_legacy_state(state: Dict[str, Any], fallback_inst_id: str) -> None:
    if "inst_state" in state and isinstance(state.get("inst_state"), dict):
        return

    legacy_keys = {"last_processed_ts_ms", "trade", "open_entry_ts_ms"}
    if not any(k in state for k in legacy_keys):
        return

    inst_state: Dict[str, Any] = {}
    for k in legacy_keys:
        if k in state:
            inst_state[k] = state.pop(k)
    state["inst_state"] = {fallback_inst_id: inst_state}


def _get_inst_state(state: Dict[str, Any], inst_id: str) -> Dict[str, Any]:
    bucket = state.get("inst_state")
    if not isinstance(bucket, dict):
        bucket = {}
        state["inst_state"] = bucket

    inst_state = bucket.get(inst_id)
    if not isinstance(inst_state, dict):
        inst_state = {}
        bucket[inst_id] = inst_state
    return inst_state

