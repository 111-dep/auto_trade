from __future__ import annotations

import time
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

from .alerts import handle_entry_alert, notify_trade_execution
from .common import bar_to_seconds, log, round_size
from .decision_core import resolve_entry_decision
from .entry_exec_policy import (
    resolve_entry_exec_mode_for_params,
    resolve_entry_limit_fallback_mode_for_params,
    resolve_entry_limit_ttl_sec_for_params,
)
from .managed_tp2 import (
    cancel_managed_tp1_order,
    cancel_managed_tp2_order,
    clear_managed_tp1_order_state,
    ensure_managed_tp1_limit_order,
    ensure_managed_tp2_limit_order,
    has_external_tp1_fill_mode,
)
from .models import Config, PositionState
from .okx_client import calc_order_size as okx_calc_order_size
from .risk_guard import (
    is_open_limit_reached,
    min_open_gap_remaining_minutes,
    normalize_loss_base_mode,
    open_window_ms,
    prune_state_loss_events,
    prune_state_ts_list,
    resolve_loss_base,
    rolling_loss_sum,
)
from .signals import compute_alert_targets
from .trade_journal import append_trade_journal, append_trade_order_link
from .runtime_order_id import build_runtime_order_cl_id

def execute_decision(
    client: Any,
    cfg: Config,
    inst_id: str,
    sig: Dict[str, Any],
    pos: PositionState,
    state: Dict[str, Any],
    root_state: Optional[Dict[str, Any]] = None,
    profile_id: str = "DEFAULT",
) -> None:
    l3_inst_set = set(x.strip().upper() for x in cfg.params.exec_l3_inst_ids if x)
    exec_max_level = int(cfg.params.exec_max_level)
    if inst_id in l3_inst_set:
        exec_max_level = 3
    if exec_max_level < 1:
        exec_max_level = 1
    if exec_max_level > 3:
        exec_max_level = 3

    entry_decision = resolve_entry_decision(
        sig,
        max_level=exec_max_level,
        min_level=1,
        exact_level=0,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=cfg.params.tp2_r_mult,
    )
    entry_side = entry_decision.side if entry_decision else ""
    entry_level = int(entry_decision.level) if entry_decision else 0
    entry_stop = float(entry_decision.stop) if entry_decision else 0.0
    exec_long_entry = entry_side == "LONG"
    exec_short_entry = entry_side == "SHORT"

    log(
        "[{}] signal bias={} close={:.2f} ema={:.2f} rsi={:.1f} macd_hist={:.4f} "
        "width={:.5f}/{:.5f} htf_close={:.2f} htf_ema={:.2f}/{:.2f} htf_rsi={:.1f} profile={} variant={} execMax={} execSide={} execLv={} "
        "locL={} locS={} fibL={} fibS={} rtL={} rtS={} smcSL={} smcSS={} smcBFVG={} smcSFVG={} "
        "L1E={} S1E={} L2E={} S2E={} L3E={} S3E={} "
        "Llv={} Slv={} LX={} SX={} pos={}({})".format(
            inst_id,
            sig["bias"],
            sig["close"],
            sig["ema"],
            sig["rsi"],
            sig["macd_hist"],
            sig["bb_width"],
            sig["bb_width_avg"],
            sig["htf_close"],
            sig["htf_ema_fast"],
            sig["htf_ema_slow"],
            sig["htf_rsi"],
            profile_id,
            sig.get("strategy_variant", "classic"),
            exec_max_level,
            entry_side or "NONE",
            entry_level,
            sig["location_long_ok"],
            sig["location_short_ok"],
            sig["fib_touch_long"],
            sig["fib_touch_short"],
            sig["retest_long"],
            sig["retest_short"],
            sig.get("smc_sweep_long", False),
            sig.get("smc_sweep_short", False),
            sig.get("smc_bullish_fvg", False),
            sig.get("smc_bearish_fvg", False),
            sig["long_entry"],
            sig["short_entry"],
            sig.get("long_entry_l2", False),
            sig.get("short_entry_l2", False),
            sig.get("long_entry_l3", False),
            sig.get("short_entry_l3", False),
            sig.get("long_level", 0),
            sig.get("short_level", 0),
            sig["long_exit"],
            sig["short_exit"],
            pos.side,
            round_size(max(pos.size, 0.0)) if pos.size > 0 else "0",
        )
    )

    if pos.side == "mixed":
        log("Detected mixed long+short positions. Script will not trade in this state.")
        return

    if cfg.alert_only:
        handle_entry_alert(cfg, inst_id, sig, state)
        return

    trade_state = state.get("trade") if isinstance(state.get("trade"), dict) else None

    def clear_trade_state(reason: str = "") -> None:
        trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if isinstance(trade, dict):
            cancel_managed_tp1_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=reason or "clear_trade_state",
                quiet=True,
            )
            cancel_managed_tp2_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=reason or "clear_trade_state",
                quiet=True,
            )
        state.pop("trade", None)

    def _side_key(side: str) -> str:
        s = str(side).strip().lower()
        if s in {"long", "short"}:
            return s
        return ""

    def _stop_guard_root() -> Dict[str, Any]:
        root = state.get("stop_guard")
        if not isinstance(root, dict):
            root = {}
            state["stop_guard"] = root
        return root

    def _stop_guard_bucket(side: str) -> Dict[str, Any]:
        key = _side_key(side)
        root = _stop_guard_root()
        bucket = root.get(key)
        if not isinstance(bucket, dict):
            bucket = {}
            root[key] = bucket
        return bucket

    def _record_stop_guard(side: str, is_stop_event: bool, reason: str) -> None:
        key = _side_key(side)
        if key not in {"long", "short"}:
            return
        now_ts = int(sig["signal_ts_ms"])
        bucket = _stop_guard_bucket(key)
        prev_streak = int(bucket.get("streak", 0) or 0)

        if is_stop_event:
            streak = prev_streak + 1
            bucket["streak"] = streak
            bucket["last_stop_ts_ms"] = now_ts

            freeze_count = int(max(0, cfg.params.stop_streak_freeze_count))
            freeze_hours = int(max(0, cfg.params.stop_streak_freeze_hours))
            if freeze_count > 0 and freeze_hours > 0 and streak >= freeze_count:
                freeze_until = now_ts + freeze_hours * 3600 * 1000
                prev_until = int(bucket.get("freeze_until_ts_ms", 0) or 0)
                if freeze_until > prev_until:
                    bucket["freeze_until_ts_ms"] = freeze_until
                log(
                    f"[{inst_id}] RiskGuard: {key} stop streak={streak}, freeze {freeze_hours}h "
                    f"(reason={reason}, l2_only={cfg.params.stop_streak_l2_only})"
                )
            else:
                log(f"[{inst_id}] RiskGuard: {key} stop streak={streak} (reason={reason})")
            return

        if prev_streak > 0:
            bucket["streak"] = 0
            log(f"[{inst_id}] RiskGuard: {key} stop streak reset (reason={reason})")

    def is_script_trade_state(trade: Any, expected_side: Optional[str] = None) -> bool:
        if not isinstance(trade, dict):
            return False
        side = str(trade.get("side", "")).strip().lower()
        if side not in {"long", "short"}:
            return False
        if expected_side and side != expected_side:
            return False

        managed_by = str(trade.get("managed_by", "")).strip().lower()
        if managed_by == "script":
            return True

        # Backward compatibility:
        # historical script states had no "managed_by" and manual bootstrap had bootstrapped=True.
        if (not managed_by) and (not bool(trade.get("bootstrapped", False))):
            trade["managed_by"] = "script"
            return True
        return False

    global_state = root_state if isinstance(root_state, dict) else state

    def prune_open_history(target_state: Dict[str, Any], key: str) -> List[int]:
        now_ts = int(sig["signal_ts_ms"])
        window_ms = open_window_ms(getattr(cfg.params, "open_window_hours", 24))
        return prune_state_ts_list(
            target_state,
            key,
            now_ts_ms=now_ts,
            window_ms=window_ms,
            allow_future_ms=None,
        )

    def prune_script_loss_events() -> List[Dict[str, Any]]:
        now_ts = int(sig["signal_ts_ms"])
        return prune_state_loss_events(
            global_state,
            "script_loss_events",
            now_ts_ms=now_ts,
            window_ms=24 * 3600 * 1000,
            allow_future_ms=5 * 60 * 1000,
        )

    def compute_trade_pnl_usdt_for_inst(
        target_inst_id: str,
        side: str,
        entry_px: float,
        exit_px: float,
        close_size: float,
    ) -> float:
        if close_size <= 0 or entry_px <= 0 or exit_px <= 0:
            return 0.0
        side_l = str(side).strip().lower()
        if side_l not in {"long", "short"}:
            return 0.0
        sign = 1.0 if side_l == "long" else -1.0

        try:
            info = client.get_instrument(str(target_inst_id).strip().upper())
            ct_val = float(info.get("ctVal", "0") or "0")
            ct_val_ccy = str(info.get("ctValCcy", "")).strip().upper()
        except Exception:
            ct_val = 0.0
            ct_val_ccy = ""

        if ct_val <= 0:
            return 0.0
        parts = str(target_inst_id).strip().upper().split("-")
        quote_ccy = parts[1].upper() if len(parts) >= 2 else ""
        if ct_val_ccy and quote_ccy and ct_val_ccy == quote_ccy:
            return sign * ((exit_px - entry_px) / entry_px) * close_size * ct_val
        return sign * (exit_px - entry_px) * close_size * ct_val

    def compute_trade_pnl_usdt(side: str, entry_px: float, exit_px: float, close_size: float) -> float:
        return compute_trade_pnl_usdt_for_inst(inst_id, side, entry_px, exit_px, close_size)

    def _build_trade_id(side: str, trade_ref: Optional[Dict[str, Any]] = None) -> str:
        ref = trade_ref if isinstance(trade_ref, dict) else {}
        trade_id = str(ref.get("journal_trade_id", "") or "").strip()
        if trade_id:
            return trade_id
        ord_id = str(ref.get("entry_ord_id", "") or "").strip()
        if ord_id:
            return f"{inst_id}:{ord_id}"
        side_k = str(side or "").strip().lower()
        now_ms = int(time.time() * 1000)
        return f"{inst_id}:{side_k}:{int(sig.get('signal_ts_ms', 0) or 0)}:{now_ms}"

    def _extract_order_ids(order_resp: Optional[Dict[str, Any]]) -> tuple[str, str]:
        if not isinstance(order_resp, dict):
            return "", ""
        try:
            rows = order_resp.get("data")
            if not isinstance(rows, list) or (not rows) or (not isinstance(rows[0], dict)):
                return "", ""
            row = rows[0]
            return str(row.get("ordId", "") or "").strip(), str(row.get("clOrdId", "") or "").strip()
        except Exception:
            return "", ""

    def journal_trade_event(
        *,
        event_type: str,
        side: str,
        size: float,
        reason: str = "",
        entry_px: Optional[float] = None,
        exit_px: Optional[float] = None,
        stop_px: Optional[float] = None,
        tp1_px: Optional[float] = None,
        tp2_px: Optional[float] = None,
        pnl_usdt: Optional[float] = None,
        entry_level_value: int = 0,
        trade_ref: Optional[Dict[str, Any]] = None,
        order_resp: Optional[Dict[str, Any]] = None,
        trade_id: str = "",
    ) -> None:
        if not cfg.trade_journal_enabled:
            return
        try:
            size_v = float(size)
        except Exception:
            size_v = 0.0
        if size_v <= 0:
            return

        ref = trade_ref if isinstance(trade_ref, dict) else {}
        entry_ord_id = str(ref.get("entry_ord_id", "") or "").strip()
        entry_cl_ord_id = str(ref.get("entry_cl_ord_id", "") or "").strip()
        ord_id_new, cl_id_new = _extract_order_ids(order_resp)
        event_ord_id = str(ord_id_new or "").strip()
        event_cl_ord_id = str(cl_id_new or "").strip()
        # Keep stable entry order ids; only backfill from event response when local entry ids are missing.
        if (not entry_ord_id) and event_ord_id:
            entry_ord_id = event_ord_id
        if (not entry_cl_ord_id) and event_cl_ord_id:
            entry_cl_ord_id = event_cl_ord_id

        trade_id_val = str(trade_id or "").strip() or _build_trade_id(side, ref if ref else {"entry_ord_id": entry_ord_id})

        signal_ts_ms = int(sig.get("signal_ts_ms", 0) or 0)
        event_ts_ms = int(time.time() * 1000)
        row = {
            "event_ts_ms": event_ts_ms,
            "signal_ts_ms": signal_ts_ms,
            "event_type": str(event_type or "").strip().upper(),
            "trade_id": trade_id_val,
            "inst_id": inst_id,
            "side": str(side or "").strip().lower(),
            "size": size_v,
            "entry_price": entry_px if entry_px is not None else "",
            "exit_price": exit_px if exit_px is not None else "",
            "stop_price": stop_px if stop_px is not None else "",
            "tp1_price": tp1_px if tp1_px is not None else "",
            "tp2_price": tp2_px if tp2_px is not None else "",
            "entry_level": int(entry_level_value or 0),
            "reason": str(reason or "").strip(),
            "pnl_usdt": pnl_usdt if pnl_usdt is not None else "",
            "entry_ord_id": entry_ord_id,
            "entry_cl_ord_id": entry_cl_ord_id,
            "profile_id": profile_id,
            "strategy_variant": str(sig.get("strategy_variant", "") or ""),
            "vote_enabled": 1 if bool(sig.get("vote_enabled", False)) else 0,
            "vote_mode": str(sig.get("vote_mode", "") or ""),
            "vote_winner": str(sig.get("vote_winner", "") or ""),
            "vote_winner_profile": str(sig.get("vote_winner_profile", "") or ""),
            "vote_winner_level": int(sig.get("vote_winner_level", 0) or 0),
        }
        append_trade_journal(cfg, row)
        append_trade_order_link(
            cfg,
            {
                "event_ts_ms": event_ts_ms,
                "signal_ts_ms": signal_ts_ms,
                "event_type": str(event_type or "").strip().upper(),
                "trade_id": trade_id_val,
                "inst_id": inst_id,
                "side": str(side or "").strip().lower(),
                "size": size_v,
                "reason": str(reason or "").strip(),
                "entry_ord_id": entry_ord_id,
                "entry_cl_ord_id": entry_cl_ord_id,
                "event_ord_id": event_ord_id,
                "event_cl_ord_id": event_cl_ord_id,
                "profile_id": profile_id,
                "strategy_variant": str(sig.get("strategy_variant", "") or ""),
            },
        )

    def record_script_realized_loss(
        side: str,
        entry_px: float,
        exit_px: float,
        close_size: float,
        reason: str,
    ) -> float:
        if close_size <= 0:
            return 0.0
        pnl_usdt = compute_trade_pnl_usdt(side=side, entry_px=entry_px, exit_px=exit_px, close_size=close_size)
        if pnl_usdt < 0:
            loss_usdt = abs(pnl_usdt)
            events = prune_script_loss_events()
            events.append(
                {
                    "ts_ms": int(sig["signal_ts_ms"]),
                    "inst_id": inst_id,
                    "loss_usdt": float(loss_usdt),
                    "reason": reason,
                }
            )
            global_state["script_loss_events"] = events
            log(
                "[{}] RiskGuard: script loss recorded loss={}usdt side={} close_size={} reason={}".format(
                    inst_id,
                    round(loss_usdt, 6),
                    side,
                    round_size(close_size),
                    reason,
                )
            )
        return float(pnl_usdt)

    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    def _safe_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except Exception:
            return int(default)

    def _trade_open_size(trade_ref: Dict[str, Any]) -> float:
        return max(0.0, _safe_float(trade_ref.get("open_size", 0.0), 0.0))

    def _trade_realized_size(trade_ref: Dict[str, Any]) -> float:
        if "realized_size" in trade_ref:
            return max(0.0, _safe_float(trade_ref.get("realized_size", 0.0), 0.0))
        # Backward compatibility for older states:
        # if TP1 is already done but no realized_size field, infer realized from open-remaining.
        if bool(trade_ref.get("tp1_done", False)):
            open_size = _trade_open_size(trade_ref)
            rem_size = max(0.0, _safe_float(trade_ref.get("remaining_size", open_size), open_size))
            if open_size > 0 and rem_size <= open_size:
                return max(0.0, open_size - rem_size)
        return 0.0

    def _trade_unclosed_size(trade_ref: Dict[str, Any], *, fallback_remaining: float = 0.0) -> float:
        open_size = _trade_open_size(trade_ref)
        realized_size = _trade_realized_size(trade_ref)
        if open_size > 0:
            out = max(0.0, open_size - min(open_size, realized_size))
            if out > 0:
                return out
        fb = max(0.0, float(fallback_remaining or 0.0))
        return fb

    def _mark_trade_realized(trade_ref: Dict[str, Any], *, size_delta: float, pnl_delta: float = 0.0) -> None:
        if not isinstance(trade_ref, dict):
            return
        add_size = max(0.0, float(size_delta or 0.0))
        if add_size <= 0:
            return
        open_size = _trade_open_size(trade_ref)
        realized_size_old = _trade_realized_size(trade_ref)
        realized_size_new = realized_size_old + add_size
        if open_size > 0:
            realized_size_new = min(open_size, realized_size_new)
            trade_ref["remaining_size"] = max(0.0, open_size - realized_size_new)
        trade_ref["realized_size"] = realized_size_new
        trade_ref["realized_pnl_usdt"] = _safe_float(trade_ref.get("realized_pnl_usdt", 0.0), 0.0) + float(
            pnl_delta or 0.0
        )

    def _infer_external_close_is_stop(side: str, exit_px: float, hard_stop: float) -> bool:
        s = str(side or "").strip().lower()
        stop = float(hard_stop or 0.0)
        px = float(exit_px or 0.0)
        if stop <= 0 or px <= 0:
            return False
        tol = max(1e-9, abs(stop) * 8e-4)
        if s == "long":
            return px <= (stop + tol)
        if s == "short":
            return px >= (stop - tol)
        return False

    def _mark_seen_closed_pos_id(pos_id: str) -> None:
        pid = str(pos_id or "").strip()
        if not pid:
            return
        key = "closed_pos_history_seen_ids"
        raw = state.get(key)
        seen: List[str] = [str(x).strip() for x in raw if str(x).strip()] if isinstance(raw, list) else []
        if pid in seen:
            state[key] = seen[-200:]
            return
        seen.append(pid)
        state[key] = seen[-200:]

    def _fetch_closed_position_history_row(side: str, created_ts_ms: int) -> Optional[Dict[str, Any]]:
        direction = str(side or "").strip().lower()
        if direction not in {"long", "short"}:
            return None
        try:
            payload = client._request(
                "GET",
                "/api/v5/account/positions-history",
                params={"instType": "SWAP", "instId": inst_id, "state": "filled", "limit": "20"},
                private=True,
            )
            rows = payload.get("data", []) or []
        except Exception as e:
            log(f"[{inst_id}] Positions-history lookup failed: {e}", level="WARN")
            return None

        seen_key = "closed_pos_history_seen_ids"
        seen_set = set(str(x).strip() for x in (state.get(seen_key) or []) if str(x).strip())
        created_cut = int(created_ts_ms or 0)
        best: Optional[Dict[str, Any]] = None
        best_ut = -1
        for row in rows:
            if str(row.get("instId", "")).strip().upper() != str(inst_id).strip().upper():
                continue
            if str(row.get("direction", "")).strip().lower() != direction:
                continue
            if str(row.get("mgnMode", "")).strip().lower() != str(cfg.td_mode).strip().lower():
                continue
            pos_id = str(row.get("posId", "") or "").strip()
            if pos_id and pos_id in seen_set:
                continue
            u_time = _safe_int(row.get("uTime"), 0)
            c_time = _safe_int(row.get("cTime"), 0)
            if created_cut > 0:
                # Position close/open should be later than the tracked trade creation.
                if u_time > 0 and u_time + 2 * 60 * 1000 < created_cut:
                    continue
                if c_time > 0 and c_time + 30 * 60 * 1000 < created_cut:
                    continue
            if u_time > best_ut:
                best_ut = u_time
                best = row
        if not isinstance(best, dict):
            return None
        return {
            "pos_id": str(best.get("posId", "") or "").strip(),
            "open_avg_px": _safe_float(best.get("openAvgPx"), 0.0),
            "close_avg_px": _safe_float(best.get("closeAvgPx"), 0.0),
            "open_max_pos": _safe_float(best.get("openMaxPos"), 0.0),
            "close_total_pos": _safe_float(best.get("closeTotalPos"), 0.0),
            "realized_pnl": _safe_float(best.get("realizedPnl"), 0.0),
            "u_time": _safe_int(best.get("uTime"), 0),
            "c_time": _safe_int(best.get("cTime"), 0),
        }

    def iter_script_trade_states() -> List[tuple[str, Dict[str, Any]]]:
        out: List[tuple[str, Dict[str, Any]]] = []
        inst_bucket = global_state.get("inst_state")
        if isinstance(inst_bucket, dict):
            for one_inst, one_state in inst_bucket.items():
                if not isinstance(one_state, dict):
                    continue
                one_trade = one_state.get("trade")
                if not is_script_trade_state(one_trade):
                    continue
                out.append((str(one_inst).strip().upper(), one_trade))
            if out:
                return out
        one_trade = state.get("trade")
        if is_script_trade_state(one_trade):
            out.append((inst_id, one_trade))
        return out

    def calc_trade_potential_loss_usdt(one_inst_id: str, trade: Dict[str, Any]) -> float:
        if not is_script_trade_state(trade):
            return 0.0
        side_l = str(trade.get("side", "") or "").strip().lower()
        if side_l not in {"long", "short"}:
            return 0.0
        entry_px = float(trade.get("entry_price", 0.0) or 0.0)
        stop_px = float(trade.get("hard_stop", trade.get("stop", 0.0)) or 0.0)
        if entry_px <= 0 or stop_px <= 0:
            return 0.0
        remain_size = float(trade.get("remaining_size", trade.get("open_size", 0.0)) or 0.0)
        if remain_size <= 0:
            return 0.0
        pnl_at_stop = compute_trade_pnl_usdt_for_inst(
            str(one_inst_id).strip().upper(),
            side_l,
            entry_px,
            stop_px,
            remain_size,
        )
        if pnl_at_stop >= 0:
            return 0.0
        return abs(float(pnl_at_stop))

    def sum_open_potential_loss_usdt() -> float:
        total = 0.0
        for one_inst_id, one_trade in iter_script_trade_states():
            try:
                total += max(0.0, calc_trade_potential_loss_usdt(one_inst_id, one_trade))
            except Exception:
                continue
        return total

    def get_daily_loss_budget_snapshot() -> Optional[Dict[str, float]]:
        limit_ratio = float(getattr(cfg.params, "daily_loss_limit_pct", 0.0) or 0.0)
        if limit_ratio <= 0:
            return None
        base_fixed_usdt = float(getattr(cfg.params, "daily_loss_base_usdt", 0.0) or 0.0)
        base_mode = normalize_loss_base_mode(str(getattr(cfg.params, "daily_loss_base_mode", "current") or "current"))

        eq_now: Optional[float] = None
        if base_mode in {"current", "min"} and (not cfg.alert_only):
            has_creds = bool(cfg.api_key and cfg.secret_key and cfg.passphrase)
            if has_creds:
                eq_now = client.get_account_equity()
        base_usdt = resolve_loss_base(base_mode, eq_now, base_fixed_usdt)
        if base_usdt <= 0:
            return None

        events = prune_script_loss_events()
        loss_sum = rolling_loss_sum(events)
        open_potential_loss = sum_open_potential_loss_usdt()
        limit_usdt = base_usdt * limit_ratio
        return {
            "limit_ratio": float(limit_ratio),
            "base_usdt": float(base_usdt),
            "base_mode": str(base_mode),
            "loss_sum": float(loss_sum),
            "open_potential_loss": float(open_potential_loss),
            "limit_usdt": float(limit_usdt),
        }

    def _log_daily_loss_halt_once(log_key: str, message: str) -> None:
        now_ts = int(sig["signal_ts_ms"])
        guard = global_state.get("daily_loss_guard")
        if not isinstance(guard, dict):
            guard = {}
            global_state["daily_loss_guard"] = guard
        key_name = f"last_{log_key}_signal_ts_ms"
        last_log_signal_ts = int(guard.get(key_name, 0) or 0)
        if last_log_signal_ts == now_ts:
            return
        guard[key_name] = now_ts
        log(message)

    def can_open_by_daily_loss() -> bool:
        snap = get_daily_loss_budget_snapshot()
        if not isinstance(snap, dict):
            return True
        projected_without_new = float(snap["loss_sum"]) + float(snap["open_potential_loss"])
        limit_usdt = float(snap["limit_usdt"])
        if projected_without_new < limit_usdt:
            return True
        _log_daily_loss_halt_once(
            "halt_log",
            "RiskGuard: projected 24h loss halt active "
            "(realized_loss={}usdt + open_risk={}usdt >= limit={}usdt, "
            "base={}usdt mode={} limit_pct={:.2f}%), skip entry.".format(
                round(float(snap["loss_sum"]), 6),
                round(float(snap["open_potential_loss"]), 6),
                round(limit_usdt, 6),
                round(float(snap["base_usdt"]), 6),
                str(snap["base_mode"]),
                float(snap["limit_ratio"]) * 100.0,
            ),
        )
        return False

    def can_open_by_projected_loss(entry_side: str, planned_stop: float, planned_size: float) -> bool:
        snap = get_daily_loss_budget_snapshot()
        if not isinstance(snap, dict):
            return True
        candidate_loss = 0.0
        try:
            candidate_loss = abs(
                min(
                    0.0,
                    compute_trade_pnl_usdt(
                        side=entry_side,
                        entry_px=float(sig["close"]),
                        exit_px=float(planned_stop),
                        close_size=float(planned_size),
                    ),
                )
            )
        except Exception:
            candidate_loss = 0.0
        projected_with_new = float(snap["loss_sum"]) + float(snap["open_potential_loss"]) + float(candidate_loss)
        limit_usdt = float(snap["limit_usdt"])
        if projected_with_new <= limit_usdt + 1e-12:
            return True
        _log_daily_loss_halt_once(
            "projected_halt_log",
            "RiskGuard: projected 24h loss would exceed limit after new entry "
            "(realized_loss={}usdt + open_risk={}usdt + new_risk={}usdt > limit={}usdt), "
            "skip entry.".format(
                round(float(snap["loss_sum"]), 6),
                round(float(snap["open_potential_loss"]), 6),
                round(float(candidate_loss), 6),
                round(limit_usdt, 6),
            ),
        )
        return False

    def can_open_by_stop_guard(entry_side: str, planned_level: int) -> bool:
        key = _side_key(entry_side)
        if key not in {"long", "short"}:
            return True
        now_ts = int(sig["signal_ts_ms"])
        level = int(planned_level or 0)
        bucket = _stop_guard_bucket(key)

        cooldown_min = int(max(0, cfg.params.stop_reentry_cooldown_minutes))
        last_stop_ts = int(bucket.get("last_stop_ts_ms", 0) or 0)
        if cooldown_min > 0 and last_stop_ts > 0:
            cooldown_ms = cooldown_min * 60 * 1000
            gap_ms = now_ts - last_stop_ts
            if gap_ms < cooldown_ms:
                remain_ms = max(0, cooldown_ms - gap_ms)
                remain_min = max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))
                log(
                    f"[{inst_id}] RiskGuard: {key} stop cooldown active "
                    f"(need {cooldown_min}m, wait {remain_min}m), skip entry."
                )
                return False

        freeze_count = int(max(0, cfg.params.stop_streak_freeze_count))
        freeze_hours = int(max(0, cfg.params.stop_streak_freeze_hours))
        freeze_enabled = freeze_count > 0 and freeze_hours > 0
        freeze_until_ts = int(bucket.get("freeze_until_ts_ms", 0) or 0)
        if freeze_enabled and freeze_until_ts > now_ts:
            remain_ms = max(0, freeze_until_ts - now_ts)
            remain_min = max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))
            if cfg.params.stop_streak_l2_only:
                if level > 2:
                    log(
                        f"[{inst_id}] RiskGuard: {key} freeze active ({remain_min}m left), "
                        f"L3 blocked in freeze window."
                    )
                    return False
            else:
                log(
                    f"[{inst_id}] RiskGuard: {key} freeze active "
                    f"({remain_min}m left), skip entry."
                )
                return False
        return True

    def can_open_entry(entry_side: str, planned_level: int) -> bool:
        if not can_open_by_daily_loss():
            return False
        limit = int(cfg.params.max_open_entries)
        recent = prune_open_history(state, "open_entry_ts_ms")
        if is_open_limit_reached(len(recent), limit):
            log(
                "RiskGuard: open limit reached ({}/{} in {}h), skip entry.".format(
                    len(recent), limit, cfg.params.open_window_hours
                )
            )
            return False

        global_limit = int(cfg.params.max_open_entries_global)
        global_recent = prune_open_history(global_state, "global_open_entry_ts_ms")
        if is_open_limit_reached(len(global_recent), global_limit):
            log(
                "RiskGuard: global open limit reached ({}/{} in {}h), skip entry.".format(
                    len(global_recent), global_limit, cfg.params.open_window_hours
                )
            )
            return False

        min_gap_min = int(max(0, cfg.params.min_open_interval_minutes))
        if min_gap_min > 0 and recent:
            now_ts = int(sig["signal_ts_ms"])
            remain_min = min_open_gap_remaining_minutes(now_ts, max(recent), min_gap_min)
            if remain_min > 0:
                log(
                    "RiskGuard: min open interval not reached (need {}m, wait {}m), skip entry.".format(
                        min_gap_min, remain_min
                    )
                )
                return False
        if not can_open_by_stop_guard(entry_side, planned_level):
            return False
        return True

    def mark_open_entry() -> None:
        now_signal_ts = int(sig["signal_ts_ms"])
        recent = prune_open_history(state, "open_entry_ts_ms")
        recent.append(now_signal_ts)
        state["open_entry_ts_ms"] = recent
        if cfg.params.max_open_entries > 0:
            log(
                "RiskGuard: open usage {}/{} in {}h.".format(
                    len(recent), cfg.params.max_open_entries, cfg.params.open_window_hours
                )
            )
        global_recent = prune_open_history(global_state, "global_open_entry_ts_ms")
        global_recent.append(now_signal_ts)
        global_state["global_open_entry_ts_ms"] = global_recent
        global_state["last_open_signal_ts_ms"] = now_signal_ts
        global_state["last_open_inst_id"] = inst_id
        global_state["last_open_side"] = str(entry_side).strip().lower()
        if cfg.params.max_open_entries_global > 0:
            log(
                "RiskGuard: global open usage {}/{} in {}h.".format(
                    len(global_recent), cfg.params.max_open_entries_global, cfg.params.open_window_hours
                )
            )

    def min_risk(price: float, atr_value: float) -> float:
        return max(atr_value * cfg.params.min_risk_atr_mult, price * cfg.params.min_risk_pct)

    def normalize_entry_stop_for_attach(entry_side: str, planned_stop: float) -> float:
        """Normalize attach stop so exchange-side TP/SL won't be too tight in close-disabled mode."""
        stop_px = float(planned_stop)
        entry_px = float(sig["close"])
        if entry_px <= 0 or stop_px <= 0:
            return stop_px
        if cfg.params.enable_close:
            return stop_px
        min_gap = min_risk(entry_px, float(sig["atr"]))
        if entry_side == "long":
            return min(stop_px, entry_px - min_gap)
        return max(stop_px, entry_px + min_gap)

    def init_trade_state(side: str, entry_price: float, suggested_stop: float, opened_size: float = 0.0) -> None:
        atr_value = float(sig["atr"])
        min_gap = min_risk(entry_price, atr_value)
        size_val = max(0.0, float(opened_size or 0.0))

        if side == "long":
            stop = min(float(suggested_stop), entry_price - min_gap)
            risk = max(entry_price - stop, min_gap)
            state["trade"] = {
                "side": "long",
                "entry_price": entry_price,
                "hard_stop": stop,
                "risk": risk,
                "tp1_done": False,
                "be_armed": False,
                "peak_price": entry_price,
                "trough_price": entry_price,
                "created_ts_ms": int(sig["signal_ts_ms"]),
                "inst_id": inst_id,
                "managed_by": "script",
                "open_size": size_val,
                "remaining_size": size_val,
                "realized_size": 0.0,
                "realized_pnl_usdt": 0.0,
            }
            return

        stop = max(float(suggested_stop), entry_price + min_gap)
        risk = max(stop - entry_price, min_gap)
        state["trade"] = {
            "side": "short",
            "entry_price": entry_price,
            "hard_stop": stop,
            "risk": risk,
            "tp1_done": False,
            "be_armed": False,
            "peak_price": entry_price,
            "trough_price": entry_price,
            "created_ts_ms": int(sig["signal_ts_ms"]),
            "inst_id": inst_id,
            "managed_by": "script",
            "open_size": size_val,
            "remaining_size": size_val,
            "realized_size": 0.0,
            "realized_pnl_usdt": 0.0,
        }

    def ensure_trade_state_for_position() -> None:
        current = state.get("trade")
        if pos.side not in {"long", "short"}:
            return
        if isinstance(current, dict) and current.get("side") == pos.side:
            return

        if pos.side == "long":
            init_trade_state("long", float(sig["close"]), float(sig["long_stop"]), opened_size=float(pos.size))
        else:
            init_trade_state("short", float(sig["close"]), float(sig["short_stop"]), opened_size=float(pos.size))
        state["trade"]["bootstrapped"] = True
        log(f"Management: bootstrapped {pos.side} trade state from current position.")

    def prepare_new_entry(entry_side: str, planned_stop: float) -> float:
        lev_pos_side: Optional[str] = None
        if client.use_pos_side(inst_id):
            lev_pos_side = "long" if entry_side == "long" else "short"
        client.ensure_leverage(inst_id, lev_pos_side, entry_side=entry_side)
        calc_fn = getattr(client, "calc_order_size", None)
        if callable(calc_fn):
            return calc_fn(
                cfg,
                inst_id,
                float(sig["close"]),
                stop_price=float(planned_stop),
                entry_side=entry_side,
            )
        return okx_calc_order_size(
            client,
            cfg,
            inst_id,
            float(sig["close"]),
            stop_price=float(planned_stop),
            entry_side=entry_side,
        )

    def build_entry_attach_plan(
        entry_side: str,
        planned_stop: float,
        *,
        requested_size: float,
        action_tag: str,
        tp_r_override: Optional[float] = None,
    ) -> Dict[str, Any]:
        plan: Dict[str, Any] = {"ords": None, "meta": {}}
        client_managed_open_tpsl = bool(getattr(client, "_client_managed_tpsl_on_open", False))
        client_managed_open_sl = bool(getattr(client, "_client_managed_sl_on_open", False))
        if (not cfg.attach_tpsl_on_entry) and (not client_managed_open_tpsl):
            return plan
        entry_price = float(sig["close"])
        if planned_stop <= 0 or entry_price <= 0 or float(requested_size) <= 0:
            return plan
        target_side = "LONG" if entry_side == "long" else "SHORT"

        if (tp_r_override is None) and should_split_tp_on_entry():
            try:
                risk, tp1, tp2 = compute_alert_targets(
                    side=target_side,
                    entry_price=entry_price,
                    stop_price=planned_stop,
                    tp1_r=cfg.params.tp1_r_mult,
                    tp2_r=cfg.params.tp2_r_mult,
                )
            except Exception as e:
                log(f"[{inst_id}] Split TP build skipped: {e}")
            else:
                if risk > 0 and tp1 > 0 and tp2 > 0:
                    leg_sizes = calc_split_entry_sizes(float(requested_size))
                    if leg_sizes:
                        tp1_size, tp2_size = leg_sizes
                        tp1_size, tp1_size_txt = client.normalize_order_size(inst_id, tp1_size, reduce_only=False)
                        tp2_size, _ = client.normalize_order_size(inst_id, tp2_size, reduce_only=False)
                        sl_attach_algo_cl_ord_id = build_cl_ord_id(entry_side, f"{action_tag}_slalgo")
                        if can_use_native_split_tp_on_entry():
                            tp2_size_txt = client.normalize_order_size(inst_id, tp2_size, reduce_only=False)[1]
                            tp1_attach_algo_cl_ord_id = build_cl_ord_id(entry_side, f"{action_tag}_tp1algo")
                            tp2_attach_algo_cl_ord_id = build_cl_ord_id(entry_side, f"{action_tag}_tp2algo")
                            ords = client.build_split_tp_attach_algo_ords(
                                tp1_price=float(tp1),
                                tp1_size=tp1_size_txt,
                                tp2_price=float(tp2),
                                tp2_size=tp2_size_txt,
                                sl_price=float(planned_stop),
                                tp1_attach_algo_cl_ord_id=tp1_attach_algo_cl_ord_id,
                                tp2_attach_algo_cl_ord_id=tp2_attach_algo_cl_ord_id,
                                sl_attach_algo_cl_ord_id=sl_attach_algo_cl_ord_id,
                                move_sl_to_avg_px_on_tp1=True,
                            )
                            if ords:
                                log(
                                    f"[{inst_id}] Attach TP/SL on entry (native split): side={target_side} "
                                    f"sl={planned_stop:.6f} tp1={float(tp1):.6f} sz1={round_size(tp1_size)} "
                                    f"tp2={float(tp2):.6f} sz2={round_size(tp2_size)}"
                                )
                                plan["ords"] = ords
                                plan["meta"] = {
                                    "exchange_split_tp_enabled": True,
                                    "exchange_tp1_size": float(tp1_size),
                                    "exchange_tp2_size": float(tp2_size),
                                    "exchange_tp1_px": float(tp1),
                                    "exchange_tp2_px": float(tp2),
                                    "exchange_tp1_attach_algo_cl_ord_id": tp1_attach_algo_cl_ord_id,
                                    "exchange_tp2_attach_algo_cl_ord_id": tp2_attach_algo_cl_ord_id,
                                    "exchange_sl_attach_algo_cl_ord_id": sl_attach_algo_cl_ord_id,
                                    "attach_algo_cl_ord_id": sl_attach_algo_cl_ord_id,
                                    "planned_stop": float(planned_stop),
                                }
                                return plan
                        else:
                            ords = client.build_sl_attach_algo_ords(
                                sl_price=float(planned_stop),
                                attach_algo_cl_ord_id=sl_attach_algo_cl_ord_id,
                            )
                            if ords or client_managed_open_tpsl:
                                if ords:
                                    log(
                                        f"[{inst_id}] Attach SL on entry (managed TP1/TP2): side={target_side} "
                                        f"sl={planned_stop:.6f} tp1={float(tp1):.6f} sz1={round_size(tp1_size)} "
                                        f"tp2={float(tp2):.6f} sz2={round_size(tp2_size)}"
                                    )
                                else:
                                    log(
                                        f"[{inst_id}] Client-managed SL/TP on open: side={target_side} "
                                        f"sl={planned_stop:.6f} tp1={float(tp1):.6f} sz1={round_size(tp1_size)} "
                                        f"tp2={float(tp2):.6f} sz2={round_size(tp2_size)}"
                                    )
                                plan["ords"] = ords or None
                                plan["meta"] = {
                                    "managed_tp1_enabled": True,
                                    "managed_tp2_enabled": True,
                                    "exchange_tp1_size": float(tp1_size),
                                    "exchange_tp2_size": float(tp2_size),
                                    "exchange_tp1_px": float(tp1),
                                    "exchange_tp2_px": float(tp2),
                                    "managed_tp1_target_px": float(tp1),
                                    "managed_tp1_target_size": float(tp1_size),
                                    "managed_tp2_target_px": float(tp2),
                                    "managed_tp2_target_size": float(tp2_size),
                                    "planned_stop": float(planned_stop),
                                    "exchange_sl_post_open_enabled": bool(client_managed_open_sl),
                                }
                                if ords:
                                    plan["meta"]["exchange_sl_attach_algo_cl_ord_id"] = sl_attach_algo_cl_ord_id
                                    plan["meta"]["attach_algo_cl_ord_id"] = sl_attach_algo_cl_ord_id
                                return plan
                    else:
                        log(
                            f"[{inst_id}] Split TP skipped: size={round_size(requested_size)} can't be split by lot/min constraints.",
                            level="WARN",
                        )

        attach_tp_r = float(tp_r_override) if tp_r_override is not None else float(cfg.attach_tpsl_tp_r)
        if not cfg.params.enable_close:
            attach_tp_r = float(cfg.params.tp1_r_mult)
        try:
            risk, _, tp = compute_alert_targets(
                side=target_side,
                entry_price=entry_price,
                stop_price=planned_stop,
                tp1_r=cfg.params.tp1_r_mult,
                tp2_r=attach_tp_r,
            )
        except Exception as e:
            log(f"[{inst_id}] Attach TP/SL build skipped: {e}")
            return plan
        if risk <= 0 or tp <= 0:
            return plan
        attach_algo_cl_ord_id = build_cl_ord_id(entry_side, f"{action_tag}_algo")
        ords = client.build_attach_tpsl_ords(
            tp_price=float(tp),
            sl_price=float(planned_stop),
            attach_algo_cl_ord_id=attach_algo_cl_ord_id,
        )
        if ords:
            log(
                f"[{inst_id}] Attach TP/SL on entry: side={target_side} "
                f"sl={planned_stop:.6f} tp={float(tp):.6f} tp_r={attach_tp_r}"
            )
            plan["ords"] = ords
            plan["meta"] = {
                "attach_algo_cl_ord_id": attach_algo_cl_ord_id,
                "exchange_sl_attach_algo_cl_ord_id": attach_algo_cl_ord_id,
                "planned_stop": float(planned_stop),
                "exchange_tp_px": float(tp),
            }
            return plan
        if client_managed_open_tpsl:
            plan["meta"] = {
                "planned_stop": float(planned_stop),
                "exchange_tp_px": float(tp),
                "exchange_sl_post_open_enabled": bool(client_managed_open_sl),
            }
        return plan

    def should_split_tp_on_entry() -> bool:
        if not bool(getattr(cfg.params, "split_tp_on_entry", False)):
            return False
        if (not cfg.attach_tpsl_on_entry) and (not bool(getattr(client, "_client_managed_tpsl_on_open", False))):
            return False
        if not cfg.params.enable_close:
            return False
        pct = float(cfg.params.tp1_close_pct)
        return 0.0 < pct < 1.0

    def can_use_native_split_tp_on_entry() -> bool:
        if not should_split_tp_on_entry():
            return False
        return getattr(client, "_native_split_tp_supported", True) is not False

    def is_native_split_tp_restricted_error(err: Exception) -> bool:
        msg = str(err or "").strip().lower()
        if not msg:
            return False
        if "51078" in msg:
            return True
        if ("multiple tps" in msg) and ("lead trader" in msg):
            return True
        return "you can't set multiple tps as a lead trader" in msg

    def disable_native_split_tp_for_runtime(err: Exception) -> None:
        setattr(client, "_native_split_tp_supported", False)
        setattr(client, "_native_split_tp_block_reason", str(err or "").strip())

    def calc_split_entry_sizes(total_size: float) -> Optional[tuple[float, float]]:
        try:
            info = client.get_instrument(inst_id)
            lot_sz = Decimal(str(info.get("lotSz", "0") or "0"))
            min_sz = Decimal(str(info.get("minSz", "0") or "0"))
            d_total = Decimal(str(total_size))
            d_pct = Decimal(str(float(cfg.params.tp1_close_pct)))
        except Exception:
            return None
        if d_total <= 0 or d_pct <= 0 or d_pct >= 1:
            return None
        if lot_sz <= 0:
            return None
        if min_sz <= 0:
            min_sz = lot_sz

        d_tp1 = (d_total * d_pct / lot_sz).to_integral_value(rounding=ROUND_DOWN) * lot_sz
        d_tp2 = d_total - d_tp1
        if d_tp1 <= 0 or d_tp2 <= 0:
            return None
        if d_tp1 < min_sz or d_tp2 < min_sz:
            return None

        try:
            tp1_size, _ = client.normalize_order_size(inst_id, float(d_tp1), reduce_only=False)
            tp2_size, _ = client.normalize_order_size(inst_id, float(d_tp2), reduce_only=False)
        except Exception:
            return None
        if tp1_size <= 0 or tp2_size <= 0:
            return None
        return float(tp1_size), float(tp2_size)

    def extract_order_meta(order_resp: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if not isinstance(order_resp, dict):
            return out
        try:
            rows = order_resp.get("data")
            if not isinstance(rows, list) or (not rows) or (not isinstance(rows[0], dict)):
                return out
            row = rows[0]
            ord_id = str(row.get("ordId", "") or "").strip()
            cl_ord_id = str(row.get("clOrdId", "") or "").strip()
            if ord_id:
                out["entry_ord_id"] = ord_id
            if cl_ord_id:
                out["entry_cl_ord_id"] = cl_ord_id

            attach_rows = row.get("attachAlgoOrds")
            if isinstance(attach_rows, list) and attach_rows and isinstance(attach_rows[0], dict):
                sl_ar = client._extract_attach_algo(row, prefer="sl")
                tp_ar = client._extract_attach_algo(row, prefer="tp")
                attach_algo_id = str(sl_ar.get("attachAlgoId", "") or "").strip()
                attach_algo_cl_id = str(sl_ar.get("attachAlgoClOrdId", "") or "").strip()
                if attach_algo_id:
                    out["attach_algo_id"] = attach_algo_id
                    out["exchange_sl_attach_algo_id"] = attach_algo_id
                if attach_algo_cl_id:
                    out["attach_algo_cl_ord_id"] = attach_algo_cl_id
                    out["exchange_sl_attach_algo_cl_ord_id"] = attach_algo_cl_id
                try:
                    sl_px = float(sl_ar.get("slTriggerPx", "0") or "0")
                except Exception:
                    sl_px = 0.0
                try:
                    tp_px = float(tp_ar.get("tpTriggerPx", "0") or "0")
                except Exception:
                    tp_px = 0.0
                if sl_px > 0:
                    out["exchange_sl_px"] = sl_px
                    out["exchange_sl_independent"] = False
                if tp_px > 0:
                    out["exchange_tp_px"] = tp_px
        except Exception:
            return {}
        return out

    def sync_exchange_attached_sl(
        trade: Dict[str, Any],
        *,
        side: str,
        target_sl: float,
        reason: str,
    ) -> None:
        def _should_retry_with_amend_algos(err: Exception) -> bool:
            msg = str(err or "").strip().lower()
            if not msg:
                return False
            if "51503" in msg:
                return True
            return "already been filled or canceled" in msg

        if not cfg.attach_tpsl_on_entry:
            return
        if not cfg.params.enable_close:
            return
        if not isinstance(trade, dict):
            return

        ord_id = str(trade.get("entry_ord_id", "") or "").strip()
        cl_ord_id = str(trade.get("entry_cl_ord_id", "") or "").strip()
        if not ord_id and not cl_ord_id:
            return

        try:
            new_sl = float(target_sl)
        except Exception:
            return
        if new_sl <= 0:
            return

        try:
            old_sl = float(trade.get("exchange_sl_px", "0") or "0")
        except Exception:
            old_sl = 0.0
        eps = max(abs(new_sl) * 1e-6, 1e-9)
        side_l = str(side or "").strip().lower()
        if side_l == "long" and old_sl > 0 and new_sl <= old_sl + eps:
            return
        if side_l == "short" and old_sl > 0 and new_sl >= old_sl - eps:
            return

        now_signal_ts = int(sig["signal_ts_ms"])
        now_wall_ts = int(time.time() * 1000)
        try:
            last_sync_ts = int(trade.get("exchange_sl_last_sync_ts_ms", 0) or 0)
        except Exception:
            last_sync_ts = 0
        if last_sync_ts == now_signal_ts:
            return

        try:
            last_fail_ts = int(trade.get("exchange_sl_last_fail_ts_ms", 0) or 0)
        except Exception:
            last_fail_ts = 0
        fail_cooldown_ms = max(60_000, int(bar_to_seconds(cfg.ltf_bar) * 500))
        if last_fail_ts > 0 and (now_wall_ts - last_fail_ts) < fail_cooldown_ms:
            return

        attach_algo_id = str(trade.get("exchange_sl_attach_algo_id", trade.get("attach_algo_id", "")) or "").strip()
        attach_algo_cl_id = str(trade.get("exchange_sl_attach_algo_cl_ord_id", trade.get("attach_algo_cl_ord_id", "")) or "").strip()
        sl_independent = bool(trade.get("exchange_sl_independent", False))
        if sl_independent and (attach_algo_id or attach_algo_cl_id):
            try:
                amend_resp = client.amend_algo_sl(
                    inst_id=inst_id,
                    algo_id=attach_algo_id,
                    algo_cl_ord_id=attach_algo_cl_id,
                    new_sl_trigger_px=new_sl,
                )
                amend_row = {}
                if isinstance(amend_resp, dict):
                    amend_rows = amend_resp.get("data")
                    if isinstance(amend_rows, list) and amend_rows and isinstance(amend_rows[0], dict):
                        amend_row = amend_rows[0]
                if amend_row:
                    new_attach_id = str(amend_row.get("attachAlgoId", amend_row.get("algoId", amend_row.get("ordId", ""))) or "").strip()
                    new_attach_cl_id = str(amend_row.get("attachAlgoClOrdId", amend_row.get("algoClOrdId", amend_row.get("clOrdId", ""))) or "").strip()
                    if new_attach_id:
                        trade["exchange_sl_attach_algo_id"] = new_attach_id
                        trade["attach_algo_id"] = new_attach_id
                    if new_attach_cl_id:
                        trade["exchange_sl_attach_algo_cl_ord_id"] = new_attach_cl_id
                        trade["attach_algo_cl_ord_id"] = new_attach_cl_id
                trade["exchange_sl_px"] = float(new_sl)
                trade["exchange_sl_last_sync_ts_ms"] = now_signal_ts
                trade["exchange_sl_last_reason"] = f"{reason}:independent"
                trade.pop("exchange_sl_last_fail_ts_ms", None)
                log(
                    f"[{inst_id}] Exchange SL synced ({side_l}) -> {new_sl:.6f} "
                    f"(reason={reason}, old={old_sl:.6f}, via=independent)"
                )
                return
            except Exception as e:
                trade["exchange_sl_last_fail_ts_ms"] = now_wall_ts
                log(
                    f"[{inst_id}] Exchange SL sync failed ({side_l}, reason={reason}, independent=true): {e}",
                    level="WARN",
                )
                return
        try:
            amend_resp = client.amend_order_attached_sl(
                inst_id=inst_id,
                ord_id=ord_id,
                cl_ord_id=cl_ord_id,
                attach_algo_id=attach_algo_id,
                attach_algo_cl_ord_id=attach_algo_cl_id,
                new_sl_trigger_px=new_sl,
            )
            amend_row = {}
            if isinstance(amend_resp, dict):
                amend_rows = amend_resp.get("data")
                if isinstance(amend_rows, list) and amend_rows and isinstance(amend_rows[0], dict):
                    amend_row = amend_rows[0]
            if amend_row:
                new_attach_id = str(amend_row.get("attachAlgoId", amend_row.get("ordId", "")) or "").strip()
                new_attach_cl_id = str(amend_row.get("attachAlgoClOrdId", amend_row.get("clOrdId", "")) or "").strip()
                if new_attach_id:
                    trade["exchange_sl_attach_algo_id"] = new_attach_id
                    trade["attach_algo_id"] = new_attach_id
                if new_attach_cl_id:
                    trade["exchange_sl_attach_algo_cl_ord_id"] = new_attach_cl_id
                    trade["attach_algo_cl_ord_id"] = new_attach_cl_id
            trade["exchange_sl_px"] = float(new_sl)
            trade["exchange_sl_last_sync_ts_ms"] = now_signal_ts
            trade["exchange_sl_last_reason"] = reason
            trade.pop("exchange_sl_last_fail_ts_ms", None)
            log(
                f"[{inst_id}] Exchange SL synced ({side_l}) -> {new_sl:.6f} "
                f"(reason={reason}, old={old_sl:.6f})"
            )
        except Exception as e:
            if (
                (attach_algo_id or attach_algo_cl_id)
                and _should_retry_with_amend_algos(e)
            ):
                try:
                    amend_resp = client.amend_algo_sl(
                        inst_id=inst_id,
                        algo_id=attach_algo_id,
                        algo_cl_ord_id=attach_algo_cl_id,
                        new_sl_trigger_px=new_sl,
                    )
                    amend_row = {}
                    if isinstance(amend_resp, dict):
                        amend_rows = amend_resp.get("data")
                        if isinstance(amend_rows, list) and amend_rows and isinstance(amend_rows[0], dict):
                            amend_row = amend_rows[0]
                    if amend_row:
                        new_attach_id = str(amend_row.get("attachAlgoId", amend_row.get("ordId", "")) or "").strip()
                        new_attach_cl_id = str(amend_row.get("attachAlgoClOrdId", amend_row.get("clOrdId", "")) or "").strip()
                        if new_attach_id:
                            trade["exchange_sl_attach_algo_id"] = new_attach_id
                            trade["attach_algo_id"] = new_attach_id
                        if new_attach_cl_id:
                            trade["exchange_sl_attach_algo_cl_ord_id"] = new_attach_cl_id
                            trade["attach_algo_cl_ord_id"] = new_attach_cl_id
                    trade["exchange_sl_px"] = float(new_sl)
                    trade["exchange_sl_last_sync_ts_ms"] = now_signal_ts
                    trade["exchange_sl_last_reason"] = f"{reason}:amend_algos_fallback"
                    trade.pop("exchange_sl_last_fail_ts_ms", None)
                    log(
                        f"[{inst_id}] Exchange SL synced ({side_l}) -> {new_sl:.6f} "
                        f"(reason={reason}, old={old_sl:.6f}, via=amend-algos)"
                    )
                    return
                except Exception as e2:
                    trade["exchange_sl_last_fail_ts_ms"] = now_wall_ts
                    log(
                        f"[{inst_id}] Exchange SL sync failed after amend-algos fallback "
                        f"({side_l}, reason={reason}): amend-order={e} | amend-algos={e2}",
                        level="WARN",
                    )
                    return
            trade["exchange_sl_last_fail_ts_ms"] = now_wall_ts
            log(
                f"[{inst_id}] Exchange SL sync failed ({side_l}, reason={reason}): {e}",
                level="WARN",
            )

    entry_order_meta: Dict[str, Any] = {}
    entry_order_resp: Optional[Dict[str, Any]] = None

    def build_cl_ord_id(entry_side: str, action_tag: str) -> str:
        return build_runtime_order_cl_id(
            inst_id=inst_id,
            side=entry_side,
            signal_ts_ms=int(sig.get("signal_ts_ms", 0) or 0),
            action_tag=action_tag,
            level=int(entry_level or 0),
            extra=str(sig.get("strategy_variant", "") or ""),
        )

    def _to_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    def _resolve_entry_exec_mode(planned_level: int) -> str:
        return resolve_entry_exec_mode_for_params(cfg.params, int(planned_level))

    def _parse_filled_size(order_row: Dict[str, Any], fallback: float = 0.0) -> float:
        if not isinstance(order_row, dict):
            return max(0.0, float(fallback))
        for k in ("accFillSz", "fillSz", "fill_size", "filledSize"):
            val = str(order_row.get(k, "") or "").strip()
            if not val:
                continue
            try:
                return max(0.0, float(val))
            except Exception:
                continue
        return max(0.0, float(fallback))

    def _get_best_bid_ask() -> tuple[float, float]:
        try:
            row = client.get_ticker(inst_id)
            bid = _to_float(row.get("bidPx"), 0.0)
            ask = _to_float(row.get("askPx"), 0.0)
            return bid, ask
        except Exception:
            return 0.0, 0.0

    def _build_limit_px(entry_side: str, reprice_idx: int = 0) -> float:
        base_bps = float(getattr(cfg.params, "entry_limit_offset_bps", 1.0) or 1.0)
        bps = max(0.0, base_bps) * max(0.20, (1.0 - 0.25 * max(0, int(reprice_idx))))
        bid, ask = _get_best_bid_ask()
        close_px = _to_float(sig.get("close"), 0.0)
        if entry_side == "long":
            ref = ask if ask > 0 else close_px
            if ref <= 0 and bid > 0:
                ref = bid
            px = ref * (1.0 - bps / 10000.0)
        else:
            ref = bid if bid > 0 else close_px
            if ref <= 0 and ask > 0:
                ref = ask
            px = ref * (1.0 + bps / 10000.0)
        return max(px, 1e-10)

    def _wait_limit_fill(
        *,
        ord_id: str,
        cl_ord_id: str,
        requested_sz: float,
        planned_level: int,
    ) -> tuple[float, Dict[str, Any], str]:
        if cfg.dry_run:
            return float(requested_sz), {}, "filled"
        ttl_sec = resolve_entry_limit_ttl_sec_for_params(cfg.params, int(planned_level))
        poll_ms = max(100, int(getattr(cfg.params, "entry_limit_poll_ms", 500) or 500))
        if ttl_sec <= 0:
            return 0.0, {}, "timeout"
        started = time.monotonic()
        last_row: Dict[str, Any] = {}
        while (time.monotonic() - started) < float(ttl_sec):
            try:
                row = client.get_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
            except Exception:
                time.sleep(poll_ms / 1000.0)
                continue
            if isinstance(row, dict):
                last_row = row
            state = str(last_row.get("state", "") or "").strip().lower()
            filled_sz = _parse_filled_size(last_row, fallback=0.0)
            if state == "filled":
                return min(float(requested_sz), float(filled_sz) if filled_sz > 0 else float(requested_sz)), last_row, "filled"
            if state in {"canceled", "mmp_canceled", "order_failed"}:
                return min(float(requested_sz), float(filled_sz)), last_row, "closed"
            time.sleep(poll_ms / 1000.0)
        filled_sz = _parse_filled_size(last_row, fallback=0.0)
        return min(float(requested_sz), float(filled_sz)), last_row, "timeout"

    def place_open_leg(
        *,
        entry_side: str,
        size: float,
        planned_stop: float,
        planned_level: int,
        action_tag: str,
        tp_r_override: Optional[float] = None,
    ) -> tuple[float, Dict[str, Any], Dict[str, Any], str]:
        req_size = float(size)
        norm_size, _ = client.normalize_order_size(inst_id, req_size, reduce_only=False)
        cl_ord_id = build_cl_ord_id(entry_side, action_tag)
        attach_plan = build_entry_attach_plan(
            entry_side,
            planned_stop,
            requested_size=float(norm_size),
            action_tag=action_tag,
            tp_r_override=tp_r_override,
        )
        attach_algo_ords = attach_plan.get("ords")
        attach_meta = dict(attach_plan.get("meta") or {})
        if entry_side == "long":
            side = "buy"
            pos_side = "long"
        else:
            side = "sell"
            pos_side = "short"
        resolved_mode = _resolve_entry_exec_mode(int(planned_level))

        def _place_market_leg(*, open_size: float, tag: str) -> tuple[float, Dict[str, Any], Dict[str, Any], str]:
            use_cl_ord_id = build_cl_ord_id(entry_side, tag)
            if cfg.pos_mode == "net":
                resp_m = client.place_order(
                    inst_id,
                    side,
                    open_size,
                    pos_side=None,
                    reduce_only=False,
                    attach_algo_ords=attach_algo_ords,
                    cl_ord_id=use_cl_ord_id,
                    ord_type="market",
                )
            else:
                resp_m = client.place_order(
                    inst_id,
                    side,
                    open_size,
                    pos_side=pos_side,
                    reduce_only=False,
                    attach_algo_ords=attach_algo_ords,
                    cl_ord_id=use_cl_ord_id,
                    ord_type="market",
                )
            meta_m = extract_order_meta(resp_m)
            if use_cl_ord_id and "entry_cl_ord_id" not in meta_m:
                meta_m["entry_cl_ord_id"] = use_cl_ord_id
            if attach_meta:
                meta_m.update(attach_meta)
            meta_m["entry_exec_mode"] = "market"
            return float(open_size), resp_m, meta_m, use_cl_ord_id

        if resolved_mode == "market":
            return _place_market_leg(open_size=float(norm_size), tag=action_tag)

        # limit mode: wait for fill, optional reprice, then fallback market/skip.
        fallback_mode = resolve_entry_limit_fallback_mode_for_params(cfg.params, int(planned_level))
        max_reprice = max(0, int(getattr(cfg.params, "entry_limit_reprice_max", 0) or 0))
        remaining = float(norm_size)
        opened_total = 0.0
        first_resp: Dict[str, Any] = {}
        merged_meta: Dict[str, Any] = {}
        first_cl_ord_id = cl_ord_id
        used_fallback_market = False
        partial_limit_fill_canceled = False

        for reprice_idx in range(max_reprice + 1):
            if remaining <= 0:
                break
            leg_requested = float(remaining)
            leg_tag = action_tag if reprice_idx == 0 else f"{action_tag}_rp{reprice_idx}"
            leg_cl_ord_id = build_cl_ord_id(entry_side, leg_tag)
            if reprice_idx == 0:
                first_cl_ord_id = leg_cl_ord_id
            limit_px = _build_limit_px(entry_side, reprice_idx)
            if cfg.pos_mode == "net":
                resp_l = client.place_order(
                    inst_id,
                    side,
                    remaining,
                    pos_side=None,
                    reduce_only=False,
                    attach_algo_ords=attach_algo_ords,
                    cl_ord_id=leg_cl_ord_id,
                    ord_type="limit",
                    px=limit_px,
                )
            else:
                resp_l = client.place_order(
                    inst_id,
                    side,
                    remaining,
                    pos_side=pos_side,
                    reduce_only=False,
                    attach_algo_ords=attach_algo_ords,
                    cl_ord_id=leg_cl_ord_id,
                    ord_type="limit",
                    px=limit_px,
                )
            if not first_resp:
                first_resp = resp_l if isinstance(resp_l, dict) else {}
            meta_l = extract_order_meta(resp_l)
            ord_id = str(meta_l.get("entry_ord_id", "") or "").strip()
            if not ord_id:
                ord_id = str((resp_l.get("data", [{}])[0] if isinstance(resp_l, dict) else {}).get("ordId", "") or "").strip()
            filled_sz, _, _ = _wait_limit_fill(ord_id=ord_id, cl_ord_id=leg_cl_ord_id, requested_sz=remaining, planned_level=int(planned_level))
            if filled_sz < leg_requested - 1e-12:
                cancel_ok = False
                try:
                    client.cancel_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=leg_cl_ord_id)
                    cancel_ok = True
                except Exception as e:
                    log(f"[{inst_id}] Limit entry cancel warning clOrdId={leg_cl_ord_id}: {e}", level="WARN")
                # Safety reconciliation:
                # If cancel status is uncertain and order might still be live, do not fallback to market
                # to avoid accidental overfill.
                row_after: Dict[str, Any] = {}
                try:
                    row_after = client.get_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=leg_cl_ord_id)
                except Exception as e:
                    log(
                        f"[{inst_id}] Limit entry post-cancel state check failed clOrdId={leg_cl_ord_id}: {e}",
                        level="WARN",
                    )
                if row_after:
                    state_after = str(row_after.get("state", "") or "").strip().lower()
                    filled_after = _parse_filled_size(row_after, fallback=filled_sz)
                    if filled_after > filled_sz:
                        filled_sz = min(float(remaining), float(filled_after))
                    if state_after in {"live", "partially_filled"}:
                        raise RuntimeError(
                            f"limit entry cancel not confirmed (order still {state_after}) clOrdId={leg_cl_ord_id}"
                        )
                elif not cancel_ok:
                    raise RuntimeError(
                        f"limit entry cancel not confirmed (no post-cancel snapshot) clOrdId={leg_cl_ord_id}"
                    )
            if filled_sz > 0:
                if filled_sz < leg_requested - 1e-12:
                    partial_limit_fill_canceled = True
                    if bool(getattr(client, "_client_managed_sl_on_open", False)):
                        attach_algo_ords = None
                        attach_meta = dict(attach_meta)
                        for stale_key in (
                            "attach_algo_id",
                            "exchange_sl_attach_algo_id",
                            "attach_algo_cl_ord_id",
                            "exchange_sl_attach_algo_cl_ord_id",
                            "exchange_sl_px",
                            "exchange_sl_independent",
                        ):
                            attach_meta.pop(stale_key, None)
                        attach_meta["exchange_sl_post_open_enabled"] = True
                opened_total += float(filled_sz)
                remaining = max(0.0, remaining - float(filled_sz))
                merged_meta = dict(meta_l)
                if attach_meta:
                    merged_meta.update(attach_meta)
                merged_meta["entry_exec_mode"] = "limit"
                merged_meta["entry_limit_px"] = float(limit_px)
            if remaining <= 0:
                break

        if remaining > 0 and fallback_mode == "market":
            opened_m, resp_m, meta_m, _ = _place_market_leg(open_size=float(remaining), tag=f"{action_tag}_mktfallback")
            if opened_m > 0:
                used_fallback_market = True
                opened_total += float(opened_m)
                if not first_resp:
                    first_resp = resp_m if isinstance(resp_m, dict) else {}
                merged_meta.update(meta_m)

        if opened_total <= 0:
            raise RuntimeError(
                f"entry {entry_side} not filled in limit mode (fallback={fallback_mode}, ttl={resolve_entry_limit_ttl_sec_for_params(cfg.params, int(planned_level))}s)"
            )
        if attach_meta:
            merged_meta.update(attach_meta)
        merged_meta["entry_exec_mode"] = "limit_fallback_market" if used_fallback_market else "limit"
        if first_cl_ord_id and "entry_cl_ord_id" not in merged_meta:
            merged_meta["entry_cl_ord_id"] = first_cl_ord_id
        return float(opened_total), first_resp, merged_meta, first_cl_ord_id

    def finalize_standard_open_entry(
        entry_side: str,
        *,
        planned_stop: float,
        opened_size: float,
        resp: Dict[str, Any],
        meta: Dict[str, Any],
        cl_ord_id: str,
    ) -> float:
        nonlocal entry_order_meta, entry_order_resp
        side_upper = "LONG" if entry_side == "long" else "SHORT"
        mark_open_entry()
        entry_order_meta = dict(meta)
        entry_order_meta["planned_stop"] = float(planned_stop)
        entry_order_resp = resp
        if bool(meta.get("exchange_split_tp_enabled", False)):
            log(
                f"[{inst_id}] Action: OPEN {side_upper} NATIVE_SPLIT | size={round_size(opened_size)} "
                f"tp1={round_size(float(meta.get('exchange_tp1_size', 0.0) or 0.0))} "
                f"tp2={round_size(float(meta.get('exchange_tp2_size', 0.0) or 0.0))} "
                f"clOrdId={cl_ord_id} entry_exec={str(meta.get('entry_exec_mode', 'market'))}"
            )
        elif bool(meta.get("managed_tp1_enabled", False)):
            log(
                f"[{inst_id}] Action: OPEN {side_upper} SL_ATTACH_MANAGED_TP | size={round_size(opened_size)} "
                f"tp1={round_size(float(meta.get('exchange_tp1_size', 0.0) or 0.0))} "
                f"tp2={round_size(float(meta.get('exchange_tp2_size', 0.0) or 0.0))} "
                f"clOrdId={cl_ord_id} entry_exec={str(meta.get('entry_exec_mode', 'market'))}"
            )
        elif bool(meta.get("managed_tp2_enabled", False)):
            log(
                f"[{inst_id}] Action: OPEN {side_upper} MANAGED_TP2 | size={round_size(opened_size)} "
                f"tp2={round_size(float(meta.get('exchange_tp2_size', 0.0) or 0.0))} "
                f"clOrdId={cl_ord_id} entry_exec={str(meta.get('entry_exec_mode', 'market'))}"
            )
        else:
            log(
                f"[{inst_id}] Action: OPEN {side_upper} | size={round_size(opened_size)} clOrdId={cl_ord_id} "
                f"entry_exec={str(meta.get('entry_exec_mode', 'market'))}"
            )
        notify_trade_execution(
            cfg,
            inst_id,
            side_upper,
            opened_size,
            sig,
            order_resp=resp,
            planned_stop=planned_stop,
            entry_level=entry_level,
        )
        return float(opened_size)

    def ensure_post_open_managed_tp1(trade: Dict[str, Any], *, reason: str) -> None:
        if not isinstance(trade, dict):
            return
        if not bool(trade.get("managed_tp1_enabled", False)):
            return
        ok_tp1, err_tp1 = ensure_managed_tp1_limit_order(
            cfg=cfg,
            client=client,
            inst_id=inst_id,
            trade=trade,
            signal_ts_ms=int(sig["signal_ts_ms"]),
            level=int(trade.get("entry_level", 0) or 0),
            reason=reason,
        )
        if not ok_tp1 and err_tp1 not in {"filled", "live", "partially_filled", "tp1_already_done"}:
            log(f"[{inst_id}] Managed TP1 ensure warning ({reason}): {err_tp1}", level="WARN")

    def ensure_post_open_exchange_sl(trade: Dict[str, Any], *, reason: str) -> None:
        if not isinstance(trade, dict):
            return
        post_open_enabled = bool(trade.get("exchange_sl_post_open_enabled", False))
        if (not post_open_enabled) and bool(getattr(client, "_client_managed_sl_on_open", False)):
            legacy_should_manage = (
                bool(trade.get("managed_tp1_enabled", False))
                or bool(trade.get("managed_tp2_enabled", False))
                or bool(trade.get("exchange_sl_independent", False))
            )
            if legacy_should_manage:
                post_open_enabled = True
                trade["exchange_sl_post_open_enabled"] = True
        if not post_open_enabled:
            return
        side_txt = str(trade.get("side", "") or "").strip().lower()
        if side_txt not in {"long", "short"}:
            return
        stop_px = float(trade.get("planned_stop", trade.get("hard_stop", 0.0)) or 0.0)
        if stop_px <= 0:
            return
        desired_stop_size = float(trade.get("remaining_size", trade.get("open_size", 0.0)) or 0.0)

        ord_id = str(trade.get("entry_ord_id", "") or "").strip()
        cl_ord_id = str(trade.get("entry_cl_ord_id", "") or "").strip()

        def _maybe_cleanup_duplicate_sl_orders(*, max_cancel: int = 1) -> None:
            cleanup_stop_fn = getattr(client, "cleanup_pending_stop_loss_orders", None)
            if not callable(cleanup_stop_fn):
                return
            keep_id = str(trade.get("exchange_sl_attach_algo_id", trade.get("attach_algo_id", "")) or "").strip()
            keep_cl_id = str(trade.get("exchange_sl_attach_algo_cl_ord_id", trade.get("attach_algo_cl_ord_id", "")) or "").strip()
            if not keep_id and not keep_cl_id:
                return
            try:
                canceled = cleanup_stop_fn(
                    inst_id=inst_id,
                    side=side_txt,
                    keep_algo_id=keep_id,
                    keep_algo_cl_ord_id=keep_cl_id,
                    max_cancel=int(max(0, max_cancel)),
                )
            except Exception as e:
                log(f"[{inst_id}] Exchange SL cleanup warning ({reason}): {e}", level="WARN")
                return
            if int(canceled or 0) > 0:
                log(f"[{inst_id}] Exchange SL duplicate cleanup: canceled={int(canceled)} side={side_txt}")

        def _adopt_sl_row(row: Dict[str, Any], *, independent: bool, adopt_reason: str) -> bool:
            if not isinstance(row, dict) or not row:
                return False
            attach_id = str(row.get("attachAlgoId", row.get("algoId", row.get("ordId", ""))) or "").strip()
            attach_cl_id = str(row.get("attachAlgoClOrdId", row.get("algoClOrdId", row.get("clOrdId", ""))) or "").strip()
            try:
                live_sl_px = float(row.get("slTriggerPx", row.get("newSlTriggerPx", 0.0)) or 0.0)
            except Exception:
                live_sl_px = 0.0
            if attach_id:
                trade["exchange_sl_attach_algo_id"] = attach_id
                trade["attach_algo_id"] = attach_id
            if attach_cl_id:
                trade["exchange_sl_attach_algo_cl_ord_id"] = attach_cl_id
                trade["attach_algo_cl_ord_id"] = attach_cl_id
            if live_sl_px > 0:
                trade["exchange_sl_px"] = live_sl_px
            trade["exchange_sl_independent"] = bool(independent)
            trade["exchange_sl_last_sync_ts_ms"] = int(sig["signal_ts_ms"])
            trade["exchange_sl_last_reason"] = adopt_reason
            trade.pop("exchange_sl_last_fail_ts_ms", None)
            log(
                f"[{inst_id}] Exchange SL adopted ({side_txt}) -> {float(trade.get('exchange_sl_px', stop_px) or stop_px):.6f} "
                f"reason={adopt_reason} independent={bool(independent)}"
            )
            return True

        def _lookup_pending_stop(**kwargs: Any) -> Dict[str, Any]:
            lookup_stop_fn = getattr(client, "get_pending_stop_loss_order", None)
            if not callable(lookup_stop_fn):
                return {}
            try:
                return lookup_stop_fn(**kwargs)
            except TypeError:
                kwargs.pop("size", None)
                kwargs.pop("stop_price", None)
                return lookup_stop_fn(**kwargs)

        extract_attach = getattr(client, "_extract_attach_algo", None)
        if ord_id or cl_ord_id:
            try:
                entry_row = client.get_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
            except Exception:
                entry_row = {}
            if isinstance(entry_row, dict) and entry_row and callable(extract_attach):
                try:
                    attach_row = extract_attach(entry_row, prefer="sl")
                except Exception:
                    attach_row = {}
                if isinstance(attach_row, dict) and attach_row and str(attach_row.get("slTriggerPx", "") or "").strip():
                    if _adopt_sl_row(attach_row, independent=False, adopt_reason=f"{reason}:attached_detected"):
                        return

        needs_repair = False
        duplicate_cleanup_needed = False
        existing_id = str(trade.get("exchange_sl_attach_algo_id", trade.get("attach_algo_id", "")) or "").strip()
        existing_cl_id = str(trade.get("exchange_sl_attach_algo_cl_ord_id", trade.get("attach_algo_cl_ord_id", "")) or "").strip()
        lookup_stop_fn = getattr(client, "get_pending_stop_loss_order", None)
        if callable(lookup_stop_fn):
            try:
                existing_row = {}
                if existing_id or existing_cl_id:
                    existing_row = _lookup_pending_stop(
                        inst_id=inst_id,
                        side=side_txt,
                        algo_id=existing_id,
                        algo_cl_ord_id=existing_cl_id,
                        size=desired_stop_size,
                        stop_price=float(stop_px),
                    )
                if existing_row:
                    if _adopt_sl_row(existing_row, independent=True, adopt_reason=f"{reason}:existing_detected"):
                        duplicate_cleanup_needed = int(existing_row.get("_extra_count", 0) or 0) > 0
                        if bool(existing_row.get("_qty_match", True)):
                            if duplicate_cleanup_needed:
                                _maybe_cleanup_duplicate_sl_orders(max_cancel=1)
                            return
                        needs_repair = True
                elif existing_id or existing_cl_id:
                    for stale_key in (
                        "exchange_sl_attach_algo_id",
                        "attach_algo_id",
                        "exchange_sl_attach_algo_cl_ord_id",
                        "attach_algo_cl_ord_id",
                    ):
                        trade.pop(stale_key, None)
                any_row = _lookup_pending_stop(
                    inst_id=inst_id,
                    side=side_txt,
                    size=desired_stop_size,
                    stop_price=float(stop_px),
                )
                if any_row:
                    if _adopt_sl_row(any_row, independent=True, adopt_reason=f"{reason}:existing_any"):
                        duplicate_cleanup_needed = int(any_row.get("_extra_count", 0) or 0) > 0
                        if bool(any_row.get("_qty_match", True)):
                            if duplicate_cleanup_needed:
                                _maybe_cleanup_duplicate_sl_orders(max_cancel=1)
                            return
                        needs_repair = True
            except Exception as e:
                log(f"[{inst_id}] Exchange SL lookup warning ({reason}): {e}", level="WARN")

        try:
            last_fail_ts = int(trade.get("exchange_sl_last_fail_ts_ms", 0) or 0)
        except Exception:
            last_fail_ts = 0
        initial_fail_cooldown_ms = max(15_000, min(45_000, int(bar_to_seconds(cfg.ltf_bar) * 1000 * 0.02)))
        if last_fail_ts > 0 and (int(time.time() * 1000) - last_fail_ts) < initial_fail_cooldown_ms:
            return

        if needs_repair:
            amend_fn = getattr(client, "amend_algo_sl", None)
            repair_algo_id = str(trade.get("exchange_sl_attach_algo_id", trade.get("attach_algo_id", "")) or "").strip()
            repair_algo_cl_id = str(trade.get("exchange_sl_attach_algo_cl_ord_id", trade.get("attach_algo_cl_ord_id", "")) or "").strip()
            if callable(amend_fn) and (repair_algo_id or repair_algo_cl_id):
                amend_kwargs: Dict[str, Any] = {
                    "inst_id": inst_id,
                    "algo_id": repair_algo_id,
                    "algo_cl_ord_id": repair_algo_cl_id,
                    "new_sl_trigger_px": float(stop_px),
                }
                if desired_stop_size > 0:
                    amend_kwargs["size"] = float(desired_stop_size)
                try:
                    try:
                        resp = amend_fn(**amend_kwargs)
                    except TypeError:
                        amend_kwargs.pop("size", None)
                        resp = amend_fn(**amend_kwargs)
                except Exception as e:
                    trade["exchange_sl_last_fail_ts_ms"] = int(time.time() * 1000)
                    log(f"[{inst_id}] Exchange SL repair failed ({reason}): {e}", level="WARN")
                    return
                row = {}
                if isinstance(resp, dict):
                    rows = resp.get("data")
                    if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                        row = rows[0]
                attach_id = str(row.get("attachAlgoId", row.get("algoId", row.get("ordId", repair_algo_id))) or repair_algo_id).strip()
                attach_cl_id = str(row.get("attachAlgoClOrdId", row.get("algoClOrdId", row.get("clOrdId", repair_algo_cl_id))) or repair_algo_cl_id).strip()
                if attach_id:
                    trade["exchange_sl_attach_algo_id"] = attach_id
                    trade["attach_algo_id"] = attach_id
                if attach_cl_id:
                    trade["exchange_sl_attach_algo_cl_ord_id"] = attach_cl_id
                    trade["attach_algo_cl_ord_id"] = attach_cl_id
                trade["exchange_sl_independent"] = True
                trade["exchange_sl_px"] = float(stop_px)
                trade["exchange_sl_last_sync_ts_ms"] = int(sig["signal_ts_ms"])
                trade["exchange_sl_last_reason"] = f"{reason}:repair"
                trade.pop("exchange_sl_last_fail_ts_ms", None)
                log(f"[{inst_id}] Exchange SL repaired ({side_txt}) -> {float(stop_px):.6f} reason={reason}")
                if duplicate_cleanup_needed:
                    _maybe_cleanup_duplicate_sl_orders(max_cancel=1)
            return

        place_stop_fn = getattr(client, "place_stop_loss_order", None)
        if not callable(place_stop_fn):
            return
        cl_id = build_cl_ord_id(side_txt, f"{reason}_sl")
        try:
            resp = place_stop_fn(
                inst_id=inst_id,
                side=side_txt,
                stop_price=float(stop_px),
                cl_ord_id=cl_id,
                size=float(desired_stop_size or 0.0),
            )
        except Exception as e:
            trade["exchange_sl_last_fail_ts_ms"] = int(time.time() * 1000)
            log(f"[{inst_id}] Exchange SL initial place failed ({reason}): {e}", level="WARN")
            return
        row = {}
        if isinstance(resp, dict):
            rows = resp.get("data")
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                row = rows[0]
        attach_id = str(row.get("attachAlgoId", row.get("algoId", row.get("ordId", ""))) or "").strip()
        attach_cl_id = str(row.get("attachAlgoClOrdId", row.get("algoClOrdId", row.get("clOrdId", cl_id))) or cl_id).strip()
        if attach_id:
            trade["exchange_sl_attach_algo_id"] = attach_id
            trade["attach_algo_id"] = attach_id
        if attach_cl_id:
            trade["exchange_sl_attach_algo_cl_ord_id"] = attach_cl_id
            trade["attach_algo_cl_ord_id"] = attach_cl_id
        trade["exchange_sl_independent"] = True
        trade["exchange_sl_px"] = float(stop_px)
        trade["exchange_sl_last_sync_ts_ms"] = int(sig["signal_ts_ms"])
        trade["exchange_sl_last_reason"] = reason
        trade.pop("exchange_sl_last_fail_ts_ms", None)
        log(f"[{inst_id}] Exchange SL armed ({side_txt}) -> {float(stop_px):.6f} reason={reason}")
        _maybe_cleanup_duplicate_sl_orders(max_cancel=1)

    def _open_entry_once(
        *,
        entry_side: str,
        size: float,
        planned_stop: float,
        planned_level: int,
    ) -> tuple[float, Dict[str, Any], Dict[str, Any], str]:
        return place_open_leg(
            entry_side=entry_side,
            size=size,
            planned_stop=planned_stop,
            planned_level=planned_level,
            action_tag="open",
            tp_r_override=None,
        )

    def open_long() -> float:
        nonlocal entry_order_meta, entry_order_resp
        entry_order_meta = {}
        entry_order_resp = None
        if not can_open_entry("long", entry_level):
            return 0.0
        raw_stop = float(entry_stop or float(sig["long_stop"]))
        planned_stop = normalize_entry_stop_for_attach("long", raw_stop)
        if planned_stop != raw_stop:
            log(
                f"[{inst_id}] Attach stop adjusted (long): raw={raw_stop:.6f} -> normalized={planned_stop:.6f}"
            )
        size = prepare_new_entry("long", planned_stop)
        size, _ = client.normalize_order_size(inst_id, size, reduce_only=False)
        if not can_open_by_projected_loss("long", float(planned_stop), float(size)):
            return 0.0
        try:
            opened_size, resp, meta, cl_ord_id = _open_entry_once(
                entry_side="long",
                size=size,
                planned_stop=planned_stop,
                planned_level=entry_level,
            )
        except Exception as e:
            if should_split_tp_on_entry() and is_native_split_tp_restricted_error(e):
                disable_native_split_tp_for_runtime(e)
                log(
                    f"[{inst_id}] Native split TP rejected by exchange, fallback to SL attach + managed TP1/TP2 for this runtime: {e}",
                    level="WARN",
                )
                opened_size, resp, meta, cl_ord_id = _open_entry_once(
                    entry_side="long",
                    size=size,
                    planned_stop=planned_stop,
                    planned_level=entry_level,
                )
            else:
                raise
        return finalize_standard_open_entry(
            "long",
            planned_stop=planned_stop,
            opened_size=opened_size,
            resp=resp,
            meta=meta,
            cl_ord_id=cl_ord_id,
        )

    def open_short() -> float:
        nonlocal entry_order_meta, entry_order_resp
        entry_order_meta = {}
        entry_order_resp = None
        if not can_open_entry("short", entry_level):
            return 0.0
        raw_stop = float(entry_stop or float(sig["short_stop"]))
        planned_stop = normalize_entry_stop_for_attach("short", raw_stop)
        if planned_stop != raw_stop:
            log(
                f"[{inst_id}] Attach stop adjusted (short): raw={raw_stop:.6f} -> normalized={planned_stop:.6f}"
            )
        size = prepare_new_entry("short", planned_stop)
        size, _ = client.normalize_order_size(inst_id, size, reduce_only=False)
        if not can_open_by_projected_loss("short", float(planned_stop), float(size)):
            return 0.0
        try:
            opened_size, resp, meta, cl_ord_id = _open_entry_once(
                entry_side="short",
                size=size,
                planned_stop=planned_stop,
                planned_level=entry_level,
            )
        except Exception as e:
            if should_split_tp_on_entry() and is_native_split_tp_restricted_error(e):
                disable_native_split_tp_for_runtime(e)
                log(
                    f"[{inst_id}] Native split TP rejected by exchange, fallback to SL attach + managed TP1/TP2 for this runtime: {e}",
                    level="WARN",
                )
                opened_size, resp, meta, cl_ord_id = _open_entry_once(
                    entry_side="short",
                    size=size,
                    planned_stop=planned_stop,
                    planned_level=entry_level,
                )
            else:
                raise
        return finalize_standard_open_entry(
            "short",
            planned_stop=planned_stop,
            opened_size=opened_size,
            resp=resp,
            meta=meta,
            cl_ord_id=cl_ord_id,
        )

    def close_long(size: float, action_tag: str = "close") -> tuple[float, Dict[str, Any]]:
        if size <= 0:
            return 0.0, {}
        trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if isinstance(trade, dict):
            cancel_managed_tp1_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=f"{action_tag}_preclose",
                quiet=True,
            )
            cancel_managed_tp2_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=f"{action_tag}_preclose",
                quiet=True,
            )
        try:
            size, _ = client.normalize_order_size(inst_id, size, reduce_only=True)
        except Exception:
            size = float(size)
        cl_ord_id = build_cl_ord_id("long", action_tag)
        if cfg.pos_mode == "net":
            resp = client.place_order(inst_id, "sell", size, pos_side=None, reduce_only=True, cl_ord_id=cl_ord_id)
        else:
            resp = client.place_order(inst_id, "sell", size, pos_side="long", reduce_only=True, cl_ord_id=cl_ord_id)
        log(f"[{inst_id}] Action: CLOSE LONG clOrdId={cl_ord_id}")
        return float(size), resp if isinstance(resp, dict) else {}

    def close_short(size: float, action_tag: str = "close") -> tuple[float, Dict[str, Any]]:
        if size <= 0:
            return 0.0, {}
        trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if isinstance(trade, dict):
            cancel_managed_tp1_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=f"{action_tag}_preclose",
                quiet=True,
            )
            cancel_managed_tp2_order(
                client=client,
                inst_id=inst_id,
                trade=trade,
                reason=f"{action_tag}_preclose",
                quiet=True,
            )
        try:
            size, _ = client.normalize_order_size(inst_id, size, reduce_only=True)
        except Exception:
            size = float(size)
        cl_ord_id = build_cl_ord_id("short", action_tag)
        if cfg.pos_mode == "net":
            resp = client.place_order(inst_id, "buy", size, pos_side=None, reduce_only=True, cl_ord_id=cl_ord_id)
        else:
            resp = client.place_order(inst_id, "buy", size, pos_side="short", reduce_only=True, cl_ord_id=cl_ord_id)
        log(f"[{inst_id}] Action: CLOSE SHORT clOrdId={cl_ord_id}")
        return float(size), resp if isinstance(resp, dict) else {}

    if pos.side == "flat":
        prev_trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if is_script_trade_state(prev_trade):
            prev_side = str(prev_trade.get("side", "")).strip().lower()
            try:
                prev_entry = float(prev_trade.get("entry_price", sig["close"]))
            except Exception:
                prev_entry = float(sig["close"])
            try:
                prev_rem = float(prev_trade.get("remaining_size", prev_trade.get("open_size", 0.0)) or 0.0)
            except Exception:
                prev_rem = 0.0
            prev_open_size = _trade_open_size(prev_trade)
            prev_unclosed_size = _trade_unclosed_size(prev_trade, fallback_remaining=prev_rem)
            if prev_side in {"long", "short"} and prev_unclosed_size > 0:
                close_size = float(prev_unclosed_size)
                close_entry_px = float(prev_entry)
                close_exit_px = float(sig["close"])
                used_hist = False

                hist = _fetch_closed_position_history_row(
                    prev_side,
                    _safe_int(prev_trade.get("created_ts_ms"), 0),
                )
                if isinstance(hist, dict):
                    hist_close_size = max(0.0, float(hist.get("close_total_pos", 0.0) or 0.0))
                    hist_open_size = max(0.0, float(hist.get("open_max_pos", 0.0) or 0.0))
                    if hist_close_size > 0:
                        if prev_open_size > 0 and hist_open_size > 0:
                            if abs(hist_open_size - prev_open_size) <= max(1e-9, prev_open_size * 0.30):
                                close_size = max(close_size, hist_close_size)
                                used_hist = True
                        elif hist_close_size >= close_size * 0.95:
                            close_size = max(close_size, hist_close_size)
                            used_hist = True
                    hist_open_px = float(hist.get("open_avg_px", 0.0) or 0.0)
                    hist_close_px = float(hist.get("close_avg_px", 0.0) or 0.0)
                    if used_hist and hist_open_px > 0:
                        close_entry_px = hist_open_px
                    if used_hist and hist_close_px > 0:
                        close_exit_px = hist_close_px
                    if used_hist:
                        _mark_seen_closed_pos_id(str(hist.get("pos_id", "") or ""))

                if used_hist:
                    pnl_usdt = float(hist.get("realized_pnl", 0.0) or 0.0)
                    if pnl_usdt < 0:
                        loss_usdt = abs(pnl_usdt)
                        events = prune_script_loss_events()
                        events.append(
                            {
                                "ts_ms": int(sig["signal_ts_ms"]),
                                "inst_id": inst_id,
                                "loss_usdt": float(loss_usdt),
                                "reason": "external_close_or_attached_tpsl",
                            }
                        )
                        global_state["script_loss_events"] = events
                        log(
                            "[{}] RiskGuard: script loss recorded (history) loss={}usdt side={} close_size={} reason={}".format(
                                inst_id,
                                round(loss_usdt, 6),
                                prev_side,
                                round_size(close_size),
                                "external_close_or_attached_tpsl",
                            )
                        )
                else:
                    pnl_usdt = record_script_realized_loss(
                        side=prev_side,
                        entry_px=close_entry_px,
                        exit_px=close_exit_px,
                        close_size=close_size,
                        reason="external_close_or_attached_tpsl",
                    )
                prev_stop = None
                try:
                    prev_stop = float(prev_trade.get("hard_stop", 0.0) or 0.0)
                except Exception:
                    prev_stop = None
                journal_trade_event(
                    event_type="EXTERNAL_CLOSE",
                    side=prev_side,
                    size=close_size,
                    reason="external_close_or_attached_tpsl",
                    entry_px=close_entry_px,
                    exit_px=close_exit_px,
                    stop_px=prev_stop if prev_stop and prev_stop > 0 else None,
                    pnl_usdt=float(pnl_usdt),
                    entry_level_value=int(prev_trade.get("entry_level", 0) or 0),
                    trade_ref=prev_trade,
                )
                stop_hit = _infer_external_close_is_stop(prev_side, close_exit_px, float(prev_stop or 0.0))
                _record_stop_guard(
                    prev_side,
                    is_stop_event=bool(stop_hit),
                    reason="external_close_or_attached_tpsl",
                )
        clear_trade_state()
        if exec_long_entry:
            opened_size = open_long()
            if opened_size > 0:
                init_trade_state(
                    "long",
                    float(sig["close"]),
                    entry_stop or float(sig["long_stop"]),
                    opened_size=opened_size,
                )
                if isinstance(state.get("trade"), dict) and entry_order_meta:
                    state["trade"].update(entry_order_meta)
                trade_ref = state.get("trade") if isinstance(state.get("trade"), dict) else None
                trade_id = _build_trade_id("long", trade_ref)
                if isinstance(trade_ref, dict):
                    trade_ref["journal_trade_id"] = trade_id
                    trade_ref["entry_level"] = int(entry_level)
                    ensure_post_open_exchange_sl(trade_ref, reason="open_long")
                    ensure_post_open_managed_tp1(trade_ref, reason="open_long")
                entry_px = float(sig["close"])
                stop_px = float(entry_order_meta.get("planned_stop", entry_stop or float(sig["long_stop"])))
                _risk, tp1_px, tp2_px = compute_alert_targets(
                    "LONG",
                    entry_price=entry_px,
                    stop_price=stop_px,
                    tp1_r=cfg.params.tp1_r_mult,
                    tp2_r=cfg.params.tp2_r_mult,
                )
                journal_trade_event(
                    event_type="OPEN",
                    side="long",
                    size=opened_size,
                    reason="open_long",
                    entry_px=entry_px,
                    stop_px=stop_px,
                    tp1_px=tp1_px,
                    tp2_px=tp2_px,
                    entry_level_value=int(entry_level),
                    trade_ref=trade_ref,
                    order_resp=entry_order_resp,
                    trade_id=trade_id,
                )
        elif exec_short_entry:
            opened_size = open_short()
            if opened_size > 0:
                init_trade_state(
                    "short",
                    float(sig["close"]),
                    entry_stop or float(sig["short_stop"]),
                    opened_size=opened_size,
                )
                if isinstance(state.get("trade"), dict) and entry_order_meta:
                    state["trade"].update(entry_order_meta)
                trade_ref = state.get("trade") if isinstance(state.get("trade"), dict) else None
                trade_id = _build_trade_id("short", trade_ref)
                if isinstance(trade_ref, dict):
                    trade_ref["journal_trade_id"] = trade_id
                    trade_ref["entry_level"] = int(entry_level)
                    ensure_post_open_exchange_sl(trade_ref, reason="open_short")
                    ensure_post_open_managed_tp1(trade_ref, reason="open_short")
                entry_px = float(sig["close"])
                stop_px = float(entry_order_meta.get("planned_stop", entry_stop or float(sig["short_stop"])))
                _risk, tp1_px, tp2_px = compute_alert_targets(
                    "SHORT",
                    entry_price=entry_px,
                    stop_price=stop_px,
                    tp1_r=cfg.params.tp1_r_mult,
                    tp2_r=cfg.params.tp2_r_mult,
                )
                journal_trade_event(
                    event_type="OPEN",
                    side="short",
                    size=opened_size,
                    reason="open_short",
                    entry_px=entry_px,
                    stop_px=stop_px,
                    tp1_px=tp1_px,
                    tp2_px=tp2_px,
                    entry_level_value=int(entry_level),
                    trade_ref=trade_ref,
                    order_resp=entry_order_resp,
                    trade_id=trade_id,
                )
        else:
            log("Action: NONE (flat, no entry)")
        return

    if cfg.params.manage_only_script_positions:
        current_trade = state.get("trade") if isinstance(state.get("trade"), dict) else None
        if not is_script_trade_state(current_trade, pos.side):
            if is_script_trade_state(current_trade) and str(current_trade.get("side", "")).strip().lower() != pos.side:
                clear_trade_state()
                log(f"[{inst_id}] Tracked script trade side mismatch with account position. Cleared local trade state.")
            log(f"[{inst_id}] Detected untracked {pos.side} position. Skip management by policy.")
            return
    else:
        ensure_trade_state_for_position()

    trade_state = state.get("trade") if isinstance(state.get("trade"), dict) else None
    if not trade_state:
        log("Action: HOLD (no trade state available)")
        return
    if isinstance(trade_state, dict):
        if "realized_size" not in trade_state:
            inferred = 0.0
            if bool(trade_state.get("tp1_done", False)):
                open_sz = _trade_open_size(trade_state)
                rem_sz = max(0.0, _safe_float(trade_state.get("remaining_size", open_sz), open_sz))
                if open_sz > 0 and rem_sz <= open_sz:
                    inferred = max(0.0, open_sz - rem_sz)
            trade_state["realized_size"] = float(inferred)
        if "realized_pnl_usdt" not in trade_state:
            trade_state["realized_pnl_usdt"] = 0.0
    try:
        prev_remaining_size = float(trade_state.get("remaining_size", pos.size) or 0.0)
    except Exception:
        prev_remaining_size = float(pos.size)
    try:
        current_pos_size = float(pos.size)
    except Exception:
        current_pos_size = 0.0
    try:
        trade_state["remaining_size"] = float(current_pos_size)
    except Exception:
        pass
    if not cfg.params.enable_close:
        log(f"[{inst_id}] Close actions disabled by STRAT_ENABLE_CLOSE=0, keep {pos.side} position unchanged.")
        return

    if pos.side == "long":
        entry = float(trade_state.get("entry_price", sig["close"]))
        risk = float(trade_state.get("risk", max(float(sig["atr"]), entry * 0.001)))
        peak = max(float(trade_state.get("peak_price", entry)), float(sig["close"]))
        trade_state["peak_price"] = peak
        managed_tp1_enabled = bool(trade_state.get("managed_tp1_enabled", False))
        managed_tp2_enabled = bool(trade_state.get("managed_tp2_enabled", False))
        external_tp1_mode = has_external_tp1_fill_mode(trade_state)
        ensure_post_open_exchange_sl(trade_state, reason="loop_long")

        if external_tp1_mode and (not trade_state.get("tp1_done", False)):
            expected_tp2_size = float(trade_state.get("exchange_tp2_size", 0.0) or 0.0)
            size_tol = max(1e-9, expected_tp2_size * 0.002)
            if (
                expected_tp2_size > 0
                and current_pos_size > 0
                and prev_remaining_size > expected_tp2_size + size_tol
                and current_pos_size <= expected_tp2_size + size_tol
            ):
                trade_state["tp1_done"] = True
                trade_state["be_armed"] = True
                clear_managed_tp1_order_state(trade_state)
                be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
                be_stop = entry * (1.0 + be_total_offset)
                trade_state["hard_stop"] = max(float(trade_state.get("hard_stop", be_stop)), be_stop)
                sync_exchange_attached_sl(
                    trade_state,
                    side="long",
                    target_sl=float(trade_state["hard_stop"]),
                    reason="tp1_be_inferred",
                )
                closed_est = max(0.0, float(prev_remaining_size) - float(current_pos_size))
                if closed_est > 0:
                    partial_pnl = compute_trade_pnl_usdt(
                        side="long",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_est,
                    )
                    tp1_px = entry + risk * cfg.params.tp1_r_mult
                    journal_trade_event(
                        event_type="PARTIAL_CLOSE",
                        side="long",
                        size=closed_est,
                        reason="tp1_external_fill",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["long_stop"])),
                        tp1_px=tp1_px,
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                    )
                    _mark_trade_realized(trade_state, size_delta=closed_est, pnl_delta=partial_pnl)
                if managed_tp2_enabled:
                    ok_tp2, err_tp2 = ensure_managed_tp2_limit_order(
                        cfg=cfg,
                        client=client,
                        inst_id=inst_id,
                        trade=trade_state,
                        signal_ts_ms=int(sig["signal_ts_ms"]),
                        level=int(trade_state.get("entry_level", 0) or 0),
                        reason="tp1_external_fill_long",
                    )
                    if not ok_tp2 and (not str(err_tp2).startswith("live")) and (err_tp2 != "filled"):
                        log(f"[{inst_id}] Managed TP2 ensure warning (long): {err_tp2}", level="WARN")
                log(
                    f"[{inst_id}] Management: TP1 inferred by external partial fill (long). "
                    f"remain={round_size(current_pos_size)} be_sl={float(trade_state['hard_stop']):.6f}"
                )

        if managed_tp1_enabled and (not trade_state.get("tp1_done", False)) and current_pos_size > 0:
            ok_tp1, err_tp1 = ensure_managed_tp1_limit_order(
                cfg=cfg,
                client=client,
                inst_id=inst_id,
                trade=trade_state,
                signal_ts_ms=int(sig["signal_ts_ms"]),
                level=int(trade_state.get("entry_level", 0) or 0),
                reason="loop_long",
            )
            if not ok_tp1 and err_tp1 not in {"filled", "live", "partially_filled", "tp1_already_done"}:
                log(f"[{inst_id}] Managed TP1 ensure warning (long loop): {err_tp1}", level="WARN")

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) >= entry + risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (long).")

        if (not external_tp1_mode) and (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry + risk * cfg.params.tp1_r_mult
            if float(sig["close"]) >= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    closed_sz, close_resp = close_long(pos.size, action_tag="tp1_full")
                    pnl_usdt = record_script_realized_loss(
                        side="long",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_sz,
                        reason="tp1_full",
                    )
                    journal_trade_event(
                        event_type="CLOSE",
                        side="long",
                        size=closed_sz,
                        reason="tp1_full",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["long_stop"])),
                        tp1_px=tp1_price,
                        pnl_usdt=float(pnl_usdt),
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                        order_resp=close_resp,
                    )
                    _record_stop_guard("long", is_stop_event=False, reason="tp1_full")
                    clear_trade_state()
                    return
                if close_size > 0:
                    try:
                        close_size, _ = client.normalize_order_size(inst_id, close_size, reduce_only=True)
                    except Exception as e:
                        log(
                            f"[{inst_id}] Management: TP1 partial close not tradable ({e}), fallback to full close."
                        )
                        closed_sz, close_resp = close_long(pos.size, action_tag="tp1_full_fallback")
                        pnl_usdt = record_script_realized_loss(
                            side="long",
                            entry_px=entry,
                            exit_px=float(sig["close"]),
                            close_size=closed_sz,
                            reason="tp1_full_fallback",
                        )
                        journal_trade_event(
                            event_type="CLOSE",
                            side="long",
                            size=closed_sz,
                            reason="tp1_full_fallback",
                            entry_px=entry,
                            exit_px=float(sig["close"]),
                            stop_px=float(trade_state.get("hard_stop", sig["long_stop"])),
                            tp1_px=tp1_price,
                            pnl_usdt=float(pnl_usdt),
                            entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                            trade_ref=trade_state,
                            order_resp=close_resp,
                        )
                        _record_stop_guard("long", is_stop_event=False, reason="tp1_full_fallback")
                        clear_trade_state()
                        return
                    closed_sz, close_resp = close_long(close_size, action_tag="tp1_partial")
                    pnl_usdt = record_script_realized_loss(
                        side="long",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_sz,
                        reason="tp1_partial",
                    )
                    journal_trade_event(
                        event_type="PARTIAL_CLOSE",
                        side="long",
                        size=closed_sz,
                        reason="tp1_partial",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["long_stop"])),
                        tp1_px=tp1_price,
                        pnl_usdt=float(pnl_usdt),
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                        order_resp=close_resp,
                    )
                    _mark_trade_realized(trade_state, size_delta=closed_sz, pnl_delta=float(pnl_usdt))
                    trade_state["tp1_done"] = True
                    trade_state["be_armed"] = True
                    remain = max(0.0, float(trade_state.get("remaining_size", pos.size)) - float(closed_sz))
                    trade_state["remaining_size"] = remain
                    be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
                    be_stop = entry * (1.0 + be_total_offset)
                    trade_state["hard_stop"] = max(float(trade_state.get("hard_stop", be_stop)), be_stop)
                    sync_exchange_attached_sl(
                        trade_state,
                        side="long",
                        target_sl=float(trade_state["hard_stop"]),
                        reason="tp1_be",
                    )
                    log(
                        "Management: TP1 hit (long). partial_close={:.4f}, tp1={:.2f}".format(
                            closed_sz, tp1_price
                        )
                    )
                    return

        if cfg.params.tp2_close_rest and trade_state.get("tp1_done", False):
            if managed_tp2_enabled and pos.size > 0:
                ok_tp2, err_tp2 = ensure_managed_tp2_limit_order(
                    cfg=cfg,
                    client=client,
                    inst_id=inst_id,
                    trade=trade_state,
                    signal_ts_ms=int(sig["signal_ts_ms"]),
                    level=int(trade_state.get("entry_level", 0) or 0),
                    reason="loop_long",
                )
                if not ok_tp2 and err_tp2 not in {"filled", "live", "partially_filled"}:
                    log(f"[{inst_id}] Managed TP2 ensure warning (long loop): {err_tp2}", level="WARN")
            tp2_price = entry + risk * cfg.params.tp2_r_mult
            if float(sig["close"]) >= tp2_price and pos.size > 0:
                closed_sz, close_resp = close_long(pos.size, action_tag="tp2_full")
                pnl_usdt = record_script_realized_loss(
                    side="long",
                    entry_px=entry,
                    exit_px=float(sig["close"]),
                    close_size=closed_sz,
                    reason="tp2_full",
                )
                journal_trade_event(
                    event_type="CLOSE",
                    side="long",
                    size=closed_sz,
                    reason="tp2_full",
                    entry_px=entry,
                    exit_px=float(sig["close"]),
                    stop_px=float(trade_state.get("hard_stop", sig["long_stop"])),
                    tp2_px=tp2_price,
                    pnl_usdt=float(pnl_usdt),
                    entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                    trade_ref=trade_state,
                    order_resp=close_resp,
                )
                _record_stop_guard("long", is_stop_event=False, reason="tp2_full")
                clear_trade_state()
                return

        dynamic_stop = float(trade_state.get("hard_stop", sig["long_stop"]))
        dynamic_stop = max(dynamic_stop, float(sig["long_stop"]))
        if trade_state.get("be_armed", False):
            be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
            dynamic_stop = max(dynamic_stop, entry * (1.0 + be_total_offset))
        if (not cfg.params.trail_after_tp1) or trade_state.get("tp1_done", False):
            trail_stop = peak - float(sig["atr"]) * cfg.params.trail_atr_mult
            dynamic_stop = max(dynamic_stop, trail_stop)
        trade_state["hard_stop"] = dynamic_stop
        sync_exchange_attached_sl(
            trade_state,
            side="long",
            target_sl=float(dynamic_stop),
            reason="dynamic_long",
        )

        stop_hit = float(sig["close"]) <= dynamic_stop
        signal_exit_hit = bool(cfg.params.signal_exit_enabled) and bool(sig.get("long_exit", False))
        if signal_exit_hit or stop_hit:
            closed_sz, close_resp = close_long(pos.size, action_tag="close_exit")
            close_reason = "long_stop" if stop_hit else "long_exit"
            pnl_usdt = record_script_realized_loss(
                side="long",
                entry_px=entry,
                exit_px=float(sig["close"]),
                close_size=closed_sz,
                reason=close_reason,
            )
            journal_trade_event(
                event_type="CLOSE",
                side="long",
                size=closed_sz,
                reason=close_reason,
                entry_px=entry,
                exit_px=float(sig["close"]),
                stop_px=float(dynamic_stop),
                pnl_usdt=float(pnl_usdt),
                entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                trade_ref=trade_state,
                order_resp=close_resp,
            )
            _record_stop_guard("long", is_stop_event=stop_hit, reason=close_reason)
            clear_trade_state()
            if cfg.params.allow_reverse and exec_short_entry:
                opened_size = open_short()
                if opened_size > 0:
                    init_trade_state(
                        "short",
                        float(sig["close"]),
                        entry_stop or float(sig["short_stop"]),
                        opened_size=opened_size,
                    )
                    if isinstance(state.get("trade"), dict) and entry_order_meta:
                        state["trade"].update(entry_order_meta)
                    trade_ref = state.get("trade") if isinstance(state.get("trade"), dict) else None
                    trade_id = _build_trade_id("short", trade_ref)
                    if isinstance(trade_ref, dict):
                        trade_ref["journal_trade_id"] = trade_id
                        trade_ref["entry_level"] = int(entry_level)
                        ensure_post_open_managed_tp1(trade_ref, reason="reverse_open_short")
                    entry_px = float(sig["close"])
                    stop_px = float(entry_order_meta.get("planned_stop", entry_stop or float(sig["short_stop"])))
                    _risk, tp1_px, tp2_px = compute_alert_targets(
                        "SHORT",
                        entry_price=entry_px,
                        stop_price=stop_px,
                        tp1_r=cfg.params.tp1_r_mult,
                        tp2_r=cfg.params.tp2_r_mult,
                    )
                    journal_trade_event(
                        event_type="OPEN",
                        side="short",
                        size=opened_size,
                        reason="reverse_open_short",
                        entry_px=entry_px,
                        stop_px=stop_px,
                        tp1_px=tp1_px,
                        tp2_px=tp2_px,
                        entry_level_value=int(entry_level),
                        trade_ref=trade_ref,
                        order_resp=entry_order_resp,
                        trade_id=trade_id,
                    )
        else:
            log(
                "Action: HOLD LONG | stop={:.2f} entry={:.2f} risk={:.2f} tp1_done={} be={}".format(
                    dynamic_stop,
                    entry,
                    risk,
                    trade_state.get("tp1_done", False),
                    trade_state.get("be_armed", False),
                )
            )
        return

    if pos.side == "short":
        entry = float(trade_state.get("entry_price", sig["close"]))
        risk = float(trade_state.get("risk", max(float(sig["atr"]), entry * 0.001)))
        trough = min(float(trade_state.get("trough_price", entry)), float(sig["close"]))
        trade_state["trough_price"] = trough
        managed_tp1_enabled = bool(trade_state.get("managed_tp1_enabled", False))
        managed_tp2_enabled = bool(trade_state.get("managed_tp2_enabled", False))
        external_tp1_mode = has_external_tp1_fill_mode(trade_state)
        ensure_post_open_exchange_sl(trade_state, reason="loop_short")

        if external_tp1_mode and (not trade_state.get("tp1_done", False)):
            expected_tp2_size = float(trade_state.get("exchange_tp2_size", 0.0) or 0.0)
            size_tol = max(1e-9, expected_tp2_size * 0.002)
            if (
                expected_tp2_size > 0
                and current_pos_size > 0
                and prev_remaining_size > expected_tp2_size + size_tol
                and current_pos_size <= expected_tp2_size + size_tol
            ):
                trade_state["tp1_done"] = True
                trade_state["be_armed"] = True
                clear_managed_tp1_order_state(trade_state)
                be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
                be_stop = entry * (1.0 - be_total_offset)
                trade_state["hard_stop"] = min(float(trade_state.get("hard_stop", be_stop)), be_stop)
                sync_exchange_attached_sl(
                    trade_state,
                    side="short",
                    target_sl=float(trade_state["hard_stop"]),
                    reason="tp1_be_inferred",
                )
                closed_est = max(0.0, float(prev_remaining_size) - float(current_pos_size))
                if closed_est > 0:
                    partial_pnl = compute_trade_pnl_usdt(
                        side="short",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_est,
                    )
                    tp1_px = entry - risk * cfg.params.tp1_r_mult
                    journal_trade_event(
                        event_type="PARTIAL_CLOSE",
                        side="short",
                        size=closed_est,
                        reason="tp1_external_fill",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["short_stop"])),
                        tp1_px=tp1_px,
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                    )
                    _mark_trade_realized(trade_state, size_delta=closed_est, pnl_delta=partial_pnl)
                if managed_tp2_enabled:
                    ok_tp2, err_tp2 = ensure_managed_tp2_limit_order(
                        cfg=cfg,
                        client=client,
                        inst_id=inst_id,
                        trade=trade_state,
                        signal_ts_ms=int(sig["signal_ts_ms"]),
                        level=int(trade_state.get("entry_level", 0) or 0),
                        reason="tp1_external_fill_short",
                    )
                    if not ok_tp2 and (not str(err_tp2).startswith("live")) and (err_tp2 != "filled"):
                        log(f"[{inst_id}] Managed TP2 ensure warning (short): {err_tp2}", level="WARN")
                log(
                    f"[{inst_id}] Management: TP1 inferred by external partial fill (short). "
                    f"remain={round_size(current_pos_size)} be_sl={float(trade_state['hard_stop']):.6f}"
                )

        if managed_tp1_enabled and (not trade_state.get("tp1_done", False)) and current_pos_size > 0:
            ok_tp1, err_tp1 = ensure_managed_tp1_limit_order(
                cfg=cfg,
                client=client,
                inst_id=inst_id,
                trade=trade_state,
                signal_ts_ms=int(sig["signal_ts_ms"]),
                level=int(trade_state.get("entry_level", 0) or 0),
                reason="loop_short",
            )
            if not ok_tp1 and err_tp1 not in {"filled", "live", "partially_filled", "tp1_already_done"}:
                log(f"[{inst_id}] Managed TP1 ensure warning (short loop): {err_tp1}", level="WARN")

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) <= entry - risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (short).")

        if (not external_tp1_mode) and (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry - risk * cfg.params.tp1_r_mult
            if float(sig["close"]) <= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    closed_sz, close_resp = close_short(pos.size, action_tag="tp1_full")
                    pnl_usdt = record_script_realized_loss(
                        side="short",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_sz,
                        reason="tp1_full",
                    )
                    journal_trade_event(
                        event_type="CLOSE",
                        side="short",
                        size=closed_sz,
                        reason="tp1_full",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["short_stop"])),
                        tp1_px=tp1_price,
                        pnl_usdt=float(pnl_usdt),
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                        order_resp=close_resp,
                    )
                    _record_stop_guard("short", is_stop_event=False, reason="tp1_full")
                    clear_trade_state()
                    return
                if close_size > 0:
                    try:
                        close_size, _ = client.normalize_order_size(inst_id, close_size, reduce_only=True)
                    except Exception as e:
                        log(
                            f"[{inst_id}] Management: TP1 partial close not tradable ({e}), fallback to full close."
                        )
                        closed_sz, close_resp = close_short(pos.size, action_tag="tp1_full_fallback")
                        pnl_usdt = record_script_realized_loss(
                            side="short",
                            entry_px=entry,
                            exit_px=float(sig["close"]),
                            close_size=closed_sz,
                            reason="tp1_full_fallback",
                        )
                        journal_trade_event(
                            event_type="CLOSE",
                            side="short",
                            size=closed_sz,
                            reason="tp1_full_fallback",
                            entry_px=entry,
                            exit_px=float(sig["close"]),
                            stop_px=float(trade_state.get("hard_stop", sig["short_stop"])),
                            tp1_px=tp1_price,
                            pnl_usdt=float(pnl_usdt),
                            entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                            trade_ref=trade_state,
                            order_resp=close_resp,
                        )
                        _record_stop_guard("short", is_stop_event=False, reason="tp1_full_fallback")
                        clear_trade_state()
                        return
                    closed_sz, close_resp = close_short(close_size, action_tag="tp1_partial")
                    pnl_usdt = record_script_realized_loss(
                        side="short",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        close_size=closed_sz,
                        reason="tp1_partial",
                    )
                    journal_trade_event(
                        event_type="PARTIAL_CLOSE",
                        side="short",
                        size=closed_sz,
                        reason="tp1_partial",
                        entry_px=entry,
                        exit_px=float(sig["close"]),
                        stop_px=float(trade_state.get("hard_stop", sig["short_stop"])),
                        tp1_px=tp1_price,
                        pnl_usdt=float(pnl_usdt),
                        entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                        trade_ref=trade_state,
                        order_resp=close_resp,
                    )
                    _mark_trade_realized(trade_state, size_delta=closed_sz, pnl_delta=float(pnl_usdt))
                    trade_state["tp1_done"] = True
                    trade_state["be_armed"] = True
                    remain = max(0.0, float(trade_state.get("remaining_size", pos.size)) - float(closed_sz))
                    trade_state["remaining_size"] = remain
                    be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
                    be_stop = entry * (1.0 - be_total_offset)
                    trade_state["hard_stop"] = min(float(trade_state.get("hard_stop", be_stop)), be_stop)
                    sync_exchange_attached_sl(
                        trade_state,
                        side="short",
                        target_sl=float(trade_state["hard_stop"]),
                        reason="tp1_be",
                    )
                    log(
                        "Management: TP1 hit (short). partial_close={:.4f}, tp1={:.2f}".format(
                            closed_sz, tp1_price
                        )
                    )
                    return

        if cfg.params.tp2_close_rest and trade_state.get("tp1_done", False):
            if managed_tp2_enabled and pos.size > 0:
                ok_tp2, err_tp2 = ensure_managed_tp2_limit_order(
                    cfg=cfg,
                    client=client,
                    inst_id=inst_id,
                    trade=trade_state,
                    signal_ts_ms=int(sig["signal_ts_ms"]),
                    level=int(trade_state.get("entry_level", 0) or 0),
                    reason="loop_short",
                )
                if not ok_tp2 and err_tp2 not in {"filled", "live", "partially_filled"}:
                    log(f"[{inst_id}] Managed TP2 ensure warning (short loop): {err_tp2}", level="WARN")
            tp2_price = entry - risk * cfg.params.tp2_r_mult
            if float(sig["close"]) <= tp2_price and pos.size > 0:
                closed_sz, close_resp = close_short(pos.size, action_tag="tp2_full")
                pnl_usdt = record_script_realized_loss(
                    side="short",
                    entry_px=entry,
                    exit_px=float(sig["close"]),
                    close_size=closed_sz,
                    reason="tp2_full",
                )
                journal_trade_event(
                    event_type="CLOSE",
                    side="short",
                    size=closed_sz,
                    reason="tp2_full",
                    entry_px=entry,
                    exit_px=float(sig["close"]),
                    stop_px=float(trade_state.get("hard_stop", sig["short_stop"])),
                    tp2_px=tp2_price,
                    pnl_usdt=float(pnl_usdt),
                    entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                    trade_ref=trade_state,
                    order_resp=close_resp,
                )
                _record_stop_guard("short", is_stop_event=False, reason="tp2_full")
                clear_trade_state()
                return

        dynamic_stop = float(trade_state.get("hard_stop", sig["short_stop"]))
        dynamic_stop = min(dynamic_stop, float(sig["short_stop"]))
        if trade_state.get("be_armed", False):
            be_total_offset = max(0.0, float(cfg.params.be_offset_pct) + float(cfg.params.be_fee_buffer_pct))
            dynamic_stop = min(dynamic_stop, entry * (1.0 - be_total_offset))
        if (not cfg.params.trail_after_tp1) or trade_state.get("tp1_done", False):
            trail_stop = trough + float(sig["atr"]) * cfg.params.trail_atr_mult
            dynamic_stop = min(dynamic_stop, trail_stop)
        trade_state["hard_stop"] = dynamic_stop
        sync_exchange_attached_sl(
            trade_state,
            side="short",
            target_sl=float(dynamic_stop),
            reason="dynamic_short",
        )

        stop_hit = float(sig["close"]) >= dynamic_stop
        signal_exit_hit = bool(cfg.params.signal_exit_enabled) and bool(sig.get("short_exit", False))
        if signal_exit_hit or stop_hit:
            closed_sz, close_resp = close_short(pos.size, action_tag="close_exit")
            close_reason = "short_stop" if stop_hit else "short_exit"
            pnl_usdt = record_script_realized_loss(
                side="short",
                entry_px=entry,
                exit_px=float(sig["close"]),
                close_size=closed_sz,
                reason=close_reason,
            )
            journal_trade_event(
                event_type="CLOSE",
                side="short",
                size=closed_sz,
                reason=close_reason,
                entry_px=entry,
                exit_px=float(sig["close"]),
                stop_px=float(dynamic_stop),
                pnl_usdt=float(pnl_usdt),
                entry_level_value=int(trade_state.get("entry_level", 0) or 0),
                trade_ref=trade_state,
                order_resp=close_resp,
            )
            _record_stop_guard("short", is_stop_event=stop_hit, reason=close_reason)
            clear_trade_state()
            if cfg.params.allow_reverse and exec_long_entry:
                opened_size = open_long()
                if opened_size > 0:
                    init_trade_state(
                        "long",
                        float(sig["close"]),
                        entry_stop or float(sig["long_stop"]),
                        opened_size=opened_size,
                    )
                    if isinstance(state.get("trade"), dict) and entry_order_meta:
                        state["trade"].update(entry_order_meta)
                    trade_ref = state.get("trade") if isinstance(state.get("trade"), dict) else None
                    trade_id = _build_trade_id("long", trade_ref)
                    if isinstance(trade_ref, dict):
                        trade_ref["journal_trade_id"] = trade_id
                        trade_ref["entry_level"] = int(entry_level)
                        ensure_post_open_managed_tp1(trade_ref, reason="reverse_open_long")
                    entry_px = float(sig["close"])
                    stop_px = float(entry_order_meta.get("planned_stop", entry_stop or float(sig["long_stop"])))
                    _risk, tp1_px, tp2_px = compute_alert_targets(
                        "LONG",
                        entry_price=entry_px,
                        stop_price=stop_px,
                        tp1_r=cfg.params.tp1_r_mult,
                        tp2_r=cfg.params.tp2_r_mult,
                    )
                    journal_trade_event(
                        event_type="OPEN",
                        side="long",
                        size=opened_size,
                        reason="reverse_open_long",
                        entry_px=entry_px,
                        stop_px=stop_px,
                        tp1_px=tp1_px,
                        tp2_px=tp2_px,
                        entry_level_value=int(entry_level),
                        trade_ref=trade_ref,
                        order_resp=entry_order_resp,
                        trade_id=trade_id,
                    )
        else:
            log(
                "Action: HOLD SHORT | stop={:.2f} entry={:.2f} risk={:.2f} tp1_done={} be={}".format(
                    dynamic_stop,
                    entry,
                    risk,
                    trade_state.get("tp1_done", False),
                    trade_state.get("be_armed", False),
                )
            )
