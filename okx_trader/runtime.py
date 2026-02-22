from __future__ import annotations

import time
from typing import Any, Dict

from .alerts import notify_no_open_timeout, send_telegram
from .common import log
from .models import Config
from .okx_client import OKXClient
from .runtime_run_once_for_inst import run_once_for_inst
from .state_store import _get_inst_state, _migrate_legacy_state, save_state

_HEARTBEAT_KEYS = ("processed", "no_new", "stale", "safety_skip", "no_data", "error")
_HEARTBEAT_STATE: Dict[str, Any] = {
    "last_emit_ts": 0.0,
    "loops": 0,
    "processed": 0,
    "no_new": 0,
    "stale": 0,
    "safety_skip": 0,
    "no_data": 0,
    "error": 0,
}
_RISK_GUARD_BLOCK_TOKEN = "Risk guard blocked order"
_RISK_GUARD_TG_COOLDOWN_SECONDS = 15 * 60
_NO_OPEN_MONITOR_START_TS_KEY = "monitor_start_ts_ms"
_NO_OPEN_LAST_ALERT_TS_KEY = "last_no_open_alert_ts_ms"
_NO_OPEN_LAST_ALERT_REF_TS_KEY = "last_no_open_alert_ref_open_ts_ms"


def _maybe_emit_heartbeat(cfg: Config, loop_counts: Dict[str, int]) -> None:
    hb = _HEARTBEAT_STATE
    hb["loops"] += 1
    for key in _HEARTBEAT_KEYS:
        hb[key] += int(loop_counts.get(key, 0))

    now = time.time()
    if hb["last_emit_ts"] <= 0:
        hb["last_emit_ts"] = now
        return
    if (now - hb["last_emit_ts"]) < float(cfg.log_heartbeat_seconds):
        return

    log(
        "Heartbeat | loops={} insts={} processed={} no_new={} stale={} safety_skip={} no_data={} error={}".format(
            hb["loops"],
            len(cfg.inst_ids),
            hb["processed"],
            hb["no_new"],
            hb["stale"],
            hb["safety_skip"],
            hb["no_data"],
            hb["error"],
        )
    )
    hb["last_emit_ts"] = now
    hb["loops"] = 0
    for key in _HEARTBEAT_KEYS:
        hb[key] = 0


def _maybe_notify_risk_guard_block(cfg: Config, inst_id: str, inst_state: Dict[str, Any], err_text: str) -> None:
    text = str(err_text or "").strip()
    if _RISK_GUARD_BLOCK_TOKEN not in text:
        return

    now_ts = int(time.time())
    guard = inst_state.get("risk_guard_tg_notify")
    if not isinstance(guard, dict):
        guard = {}
        inst_state["risk_guard_tg_notify"] = guard

    sig = f"{inst_id}|{text}"
    last_sig = str(guard.get("last_sig", "") or "")
    try:
        last_ts = int(guard.get("last_ts", 0) or 0)
    except Exception:
        last_ts = 0

    if last_sig == sig and (now_ts - last_ts) < _RISK_GUARD_TG_COOLDOWN_SECONDS:
        remain = _RISK_GUARD_TG_COOLDOWN_SECONDS - max(0, now_ts - last_ts)
        remain_min = max(1, int((remain + 59) / 60))
        log(
            f"[{inst_id}] RiskGuard TG notify throttled (same reason, wait {remain_min}m).",
            level="WARN",
        )
        return

    now_utc = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(now_ts))
    msg = (
        f"【交易风控拦截】\n"
        f"时间：{now_utc}\n"
        f"标的：{inst_id}\n"
        f"事件：风险仓位硬保护触发，已拒绝开仓\n"
        f"原因：{text[:800]}\n"
        f"参数：risk_frac={cfg.params.risk_frac} cap={cfg.params.risk_max_margin_frac} leverage={cfg.leverage}"
    )
    sent = send_telegram(cfg, msg)
    guard["last_sig"] = sig
    guard["last_ts"] = now_ts
    guard["last_sent_ok"] = bool(sent)
    if sent:
        log(f"[{inst_id}] RiskGuard block telegram_sent=True")
    else:
        log(f"[{inst_id}] RiskGuard block telegram_sent=False", level="WARN")


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _resolve_last_open_signal_ts_ms(state: Dict[str, Any], cfg: Config) -> int:
    last_ts = _safe_int(state.get("last_open_signal_ts_ms", 0))

    global_open = state.get("global_open_entry_ts_ms")
    if isinstance(global_open, list):
        for x in global_open:
            ts = _safe_int(x)
            if ts > last_ts:
                last_ts = ts

    inst_root = state.get("inst_state")
    if isinstance(inst_root, dict):
        for inst_id in cfg.inst_ids:
            inst_state = inst_root.get(inst_id)
            if not isinstance(inst_state, dict):
                continue
            inst_open = inst_state.get("open_entry_ts_ms")
            if not isinstance(inst_open, list):
                continue
            for x in inst_open:
                ts = _safe_int(x)
                if ts > last_ts:
                    last_ts = ts
    return last_ts


def _maybe_notify_no_open_timeout(cfg: Config, state: Dict[str, Any]) -> None:
    threshold_hours = float(getattr(cfg, "alert_no_open_hours", 0.0) or 0.0)
    if threshold_hours <= 0:
        return
    cooldown_hours = float(getattr(cfg, "alert_no_open_cooldown_hours", 0.0) or 0.0)
    if cooldown_hours <= 0:
        cooldown_hours = threshold_hours

    now_ts_ms = int(time.time() * 1000)
    if now_ts_ms <= 0:
        return

    monitor_start_ts_ms = _safe_int(state.get(_NO_OPEN_MONITOR_START_TS_KEY, 0))
    if monitor_start_ts_ms <= 0:
        monitor_start_ts_ms = now_ts_ms
        state[_NO_OPEN_MONITOR_START_TS_KEY] = monitor_start_ts_ms

    last_open_signal_ts_ms = _resolve_last_open_signal_ts_ms(state, cfg)
    if last_open_signal_ts_ms > 0:
        state["last_open_signal_ts_ms"] = int(last_open_signal_ts_ms)

    ref_ts_ms = last_open_signal_ts_ms if last_open_signal_ts_ms > 0 else monitor_start_ts_ms
    if ref_ts_ms <= 0 or now_ts_ms <= ref_ts_ms:
        return

    elapsed_ms = now_ts_ms - ref_ts_ms
    threshold_ms = int(max(1e-6, threshold_hours) * 3600 * 1000)
    if elapsed_ms < threshold_ms:
        return

    last_alert_ts_ms = _safe_int(state.get(_NO_OPEN_LAST_ALERT_TS_KEY, 0))
    cooldown_ms = int(max(1e-6, cooldown_hours) * 3600 * 1000)
    if last_alert_ts_ms > 0 and (now_ts_ms - last_alert_ts_ms) < cooldown_ms:
        return

    elapsed_hours = elapsed_ms / 3600_000.0
    last_open_inst_id = str(state.get("last_open_inst_id", "") or "").strip().upper()
    notify_no_open_timeout(
        cfg,
        now_ts_ms=now_ts_ms,
        threshold_hours=threshold_hours,
        elapsed_hours=elapsed_hours,
        last_open_ts_ms=int(last_open_signal_ts_ms),
        last_open_inst_id=last_open_inst_id,
    )
    state[_NO_OPEN_LAST_ALERT_TS_KEY] = now_ts_ms
    state[_NO_OPEN_LAST_ALERT_REF_TS_KEY] = int(last_open_signal_ts_ms)


def run_once(client: OKXClient, cfg: Config, state: Dict[str, Any]) -> bool:
    fallback_inst = cfg.inst_ids[0]
    _migrate_legacy_state(state, fallback_inst)

    any_processed = False
    loop_counts: Dict[str, int] = {
        "processed": 0,
        "no_new": 0,
        "stale": 0,
        "safety_skip": 0,
        "no_data": 0,
        "error": 0,
    }
    for inst_id in cfg.inst_ids:
        inst_state = _get_inst_state(state, inst_id)
        try:
            processed, status = run_once_for_inst(client, cfg, inst_id, inst_state, root_state=state)
            if status in loop_counts:
                loop_counts[status] += 1
            any_processed = any_processed or processed
        except Exception as e:
            loop_counts["error"] += 1
            err_text = str(e)
            log(f"[{inst_id}] Instrument loop error: {err_text}", level="ERROR")
            _maybe_notify_risk_guard_block(cfg, inst_id, inst_state, err_text)

    _maybe_emit_heartbeat(cfg, loop_counts)
    _maybe_notify_no_open_timeout(cfg, state)

    save_state(cfg.state_file, state)
    return any_processed


def print_stats(cfg: Config, state: Dict[str, Any], days: int) -> int:
    if not cfg.inst_ids:
        log("Stats: no instrument configured.")
        return 1

    days = max(1, int(days))
    fallback_inst = cfg.inst_ids[0]
    _migrate_legacy_state(state, fallback_inst)

    log(f"Stats | recent_days={days} keep_days={cfg.alert_stats_keep_days} insts={','.join(cfg.inst_ids)}")
    for inst_id in cfg.inst_ids:
        inst_state = _get_inst_state(state, inst_id)
        daily = inst_state.get("daily_stats")
        if not isinstance(daily, dict) or not daily:
            log(f"[{inst_id}] no stats yet.")
            continue

        day_keys = [k for k in daily.keys() if isinstance(k, str) and len(k) == 10]
        day_keys.sort(reverse=True)
        picked = day_keys[:days]
        if not picked:
            log(f"[{inst_id}] no valid daily buckets.")
            continue

        for day in picked:
            b = daily.get(day, {})
            if not isinstance(b, dict):
                continue
            opp_total = int(b.get("opp_total", 0))
            opp_l1 = int(b.get("opp_l1", 0))
            opp_l2 = int(b.get("opp_l2", 0))
            opp_l3 = int(b.get("opp_l3", 0))
            opp_live = int(b.get("opp_live", 0))
            opp_confirm = int(b.get("opp_confirm", 0))
            opp_long = int(b.get("opp_long", 0))
            opp_short = int(b.get("opp_short", 0))

            alert_total = int(b.get("alert_total", 0))
            alert_l1 = int(b.get("alert_l1", 0))
            alert_l2 = int(b.get("alert_l2", 0))
            alert_l3 = int(b.get("alert_l3", 0))
            alert_live = int(b.get("alert_live", 0))
            alert_confirm = int(b.get("alert_confirm", 0))
            alert_long = int(b.get("alert_long", 0))
            alert_short = int(b.get("alert_short", 0))

            log(
                f"[{inst_id}] {day} | opp={opp_total} (L1/L2/L3={opp_l1}/{opp_l2}/{opp_l3}, "
                f"long/short={opp_long}/{opp_short}, live/confirm={opp_live}/{opp_confirm}) | "
                f"alert={alert_total} (L1/L2/L3={alert_l1}/{alert_l2}/{alert_l3}, "
                f"long/short={alert_long}/{alert_short}, live/confirm={alert_live}/{alert_confirm})"
            )
    return 0
