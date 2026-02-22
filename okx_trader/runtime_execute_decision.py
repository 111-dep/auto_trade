from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from .alerts import handle_entry_alert, notify_trade_execution
from .common import bar_to_seconds, log, round_size
from .decision_core import resolve_entry_decision
from .models import Config, PositionState
from .okx_client import OKXClient, calc_order_size
from .risk_guard import (
    is_daily_loss_halted,
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
from .trade_journal import append_trade_journal
from .runtime_order_id import build_runtime_order_cl_id

def execute_decision(
    client: OKXClient,
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

    def clear_trade_state() -> None:
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

    def compute_trade_pnl_usdt(side: str, entry_px: float, exit_px: float, close_size: float) -> float:
        if close_size <= 0 or entry_px <= 0 or exit_px <= 0:
            return 0.0
        side_l = str(side).strip().lower()
        if side_l not in {"long", "short"}:
            return 0.0
        sign = 1.0 if side_l == "long" else -1.0

        try:
            info = client.get_instrument(inst_id)
            ct_val = float(info.get("ctVal", "0") or "0")
            ct_val_ccy = str(info.get("ctValCcy", "")).strip().upper()
        except Exception:
            ct_val = 0.0
            ct_val_ccy = ""

        if ct_val <= 0:
            return 0.0
        parts = inst_id.split("-")
        quote_ccy = parts[1].upper() if len(parts) >= 2 else ""
        if ct_val_ccy and quote_ccy and ct_val_ccy == quote_ccy:
            return sign * ((exit_px - entry_px) / entry_px) * close_size * ct_val
        return sign * (exit_px - entry_px) * close_size * ct_val

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
        if ord_id_new:
            entry_ord_id = ord_id_new
        if cl_id_new:
            entry_cl_ord_id = cl_id_new

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
        if pnl_usdt >= 0:
            return float(pnl_usdt)
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

    def can_open_by_daily_loss() -> bool:
        limit_ratio = float(getattr(cfg.params, "daily_loss_limit_pct", 0.0) or 0.0)
        if limit_ratio <= 0:
            return True
        base_fixed_usdt = float(getattr(cfg.params, "daily_loss_base_usdt", 0.0) or 0.0)
        base_mode = normalize_loss_base_mode(str(getattr(cfg.params, "daily_loss_base_mode", "current") or "current"))

        eq_now: Optional[float] = None
        if base_mode in {"current", "min"} and (not cfg.alert_only):
            has_creds = bool(cfg.api_key and cfg.secret_key and cfg.passphrase)
            if has_creds:
                eq_now = client.get_account_equity()

        base_usdt = resolve_loss_base(base_mode, eq_now, base_fixed_usdt)

        if base_usdt <= 0:
            return True

        events = prune_script_loss_events()
        loss_sum = rolling_loss_sum(events)
        limit_usdt = base_usdt * limit_ratio
        if not is_daily_loss_halted(loss_sum, base_usdt, limit_ratio):
            return True

        now_ts = int(sig["signal_ts_ms"])
        guard = global_state.get("daily_loss_guard")
        if not isinstance(guard, dict):
            guard = {}
            global_state["daily_loss_guard"] = guard
        last_log_signal_ts = int(guard.get("last_halt_log_signal_ts_ms", 0) or 0)
        if last_log_signal_ts != now_ts:
            guard["last_halt_log_signal_ts_ms"] = now_ts
            log(
                "RiskGuard: script-only 24h loss halt active (loss={}usdt >= limit={}usdt, "
                "base={}usdt mode={} limit_pct={:.2f}%), skip entry.".format(
                    round(loss_sum, 6),
                    round(limit_usdt, 6),
                    round(base_usdt, 6),
                    base_mode,
                    limit_ratio * 100.0,
                )
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

        freeze_until_ts = int(bucket.get("freeze_until_ts_ms", 0) or 0)
        if freeze_until_ts > now_ts:
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
        return calc_order_size(
            client,
            cfg,
            inst_id,
            float(sig["close"]),
            stop_price=float(planned_stop),
            entry_side=entry_side,
        )

    def build_entry_attach_ords(entry_side: str, planned_stop: float) -> Optional[List[Dict[str, Any]]]:
        if not cfg.attach_tpsl_on_entry:
            return None
        entry_price = float(sig["close"])
        if planned_stop <= 0 or entry_price <= 0:
            return None
        target_side = "LONG" if entry_side == "long" else "SHORT"
        attach_tp_r = float(cfg.attach_tpsl_tp_r)
        if not cfg.params.enable_close:
            # When script close-management is disabled, use TP1 as exchange TP target.
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
            return None
        if risk <= 0 or tp <= 0:
            return None
        ords = client.build_attach_tpsl_ords(tp_price=float(tp), sl_price=float(planned_stop))
        if ords:
            log(
                f"[{inst_id}] Attach TP/SL on entry: side={target_side} "
                f"sl={planned_stop:.6f} tp={float(tp):.6f} tp_r={attach_tp_r}"
            )
            return ords
        return None

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
                ar = attach_rows[0]
                attach_algo_id = str(ar.get("attachAlgoId", "") or "").strip()
                attach_algo_cl_id = str(ar.get("attachAlgoClOrdId", "") or "").strip()
                if attach_algo_id:
                    out["attach_algo_id"] = attach_algo_id
                if attach_algo_cl_id:
                    out["attach_algo_cl_ord_id"] = attach_algo_cl_id
                try:
                    sl_px = float(ar.get("slTriggerPx", "0") or "0")
                except Exception:
                    sl_px = 0.0
                try:
                    tp_px = float(ar.get("tpTriggerPx", "0") or "0")
                except Exception:
                    tp_px = 0.0
                if sl_px > 0:
                    out["exchange_sl_px"] = sl_px
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

        now_ts = int(sig["signal_ts_ms"])
        try:
            last_sync_ts = int(trade.get("exchange_sl_last_sync_ts_ms", 0) or 0)
        except Exception:
            last_sync_ts = 0
        if last_sync_ts == now_ts:
            return

        try:
            last_fail_ts = int(trade.get("exchange_sl_last_fail_ts_ms", 0) or 0)
        except Exception:
            last_fail_ts = 0
        fail_cooldown_ms = max(60_000, int(bar_to_seconds(cfg.ltf_bar) * 500))
        if last_fail_ts > 0 and (now_ts - last_fail_ts) < fail_cooldown_ms:
            return

        attach_algo_id = str(trade.get("attach_algo_id", "") or "").strip()
        attach_algo_cl_id = str(trade.get("attach_algo_cl_ord_id", "") or "").strip()
        try:
            client.amend_order_attached_sl(
                inst_id=inst_id,
                ord_id=ord_id,
                cl_ord_id=cl_ord_id,
                attach_algo_id=attach_algo_id,
                attach_algo_cl_ord_id=attach_algo_cl_id,
                new_sl_trigger_px=new_sl,
            )
            trade["exchange_sl_px"] = float(new_sl)
            trade["exchange_sl_last_sync_ts_ms"] = now_ts
            trade["exchange_sl_last_reason"] = reason
            log(
                f"[{inst_id}] Exchange SL synced ({side_l}) -> {new_sl:.6f} "
                f"(reason={reason}, old={old_sl:.6f})"
            )
        except Exception as e:
            trade["exchange_sl_last_fail_ts_ms"] = now_ts
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

    def open_long() -> float:
        nonlocal entry_order_meta, entry_order_resp
        entry_order_meta = {}
        entry_order_resp = None
        if not can_open_entry("long", entry_level):
            return 0.0
        cl_ord_id = build_cl_ord_id("long", "open")
        raw_stop = float(entry_stop or float(sig["long_stop"]))
        planned_stop = normalize_entry_stop_for_attach("long", raw_stop)
        if planned_stop != raw_stop:
            log(
                f"[{inst_id}] Attach stop adjusted (long): raw={raw_stop:.6f} -> normalized={planned_stop:.6f}"
            )
        size = prepare_new_entry("long", planned_stop)
        size, _ = client.normalize_order_size(inst_id, size, reduce_only=False)
        attach_algo_ords = build_entry_attach_ords("long", planned_stop)
        if cfg.pos_mode == "net":
            resp = client.place_order(
                inst_id,
                "buy",
                size,
                pos_side=None,
                reduce_only=False,
                attach_algo_ords=attach_algo_ords,
                cl_ord_id=cl_ord_id,
            )
        else:
            resp = client.place_order(
                inst_id,
                "buy",
                size,
                pos_side="long",
                reduce_only=False,
                attach_algo_ords=attach_algo_ords,
                cl_ord_id=cl_ord_id,
            )
        mark_open_entry()
        entry_order_meta = extract_order_meta(resp)
        if cl_ord_id and "entry_cl_ord_id" not in entry_order_meta:
            entry_order_meta["entry_cl_ord_id"] = cl_ord_id
        entry_order_meta["planned_stop"] = float(planned_stop)
        entry_order_resp = resp
        log(f"[{inst_id}] Action: OPEN LONG | size={round_size(size)} clOrdId={cl_ord_id}")
        notify_trade_execution(
            cfg,
            inst_id,
            "LONG",
            size,
            sig,
            order_resp=resp,
            planned_stop=planned_stop,
            entry_level=entry_level,
        )
        return float(size)

    def open_short() -> float:
        nonlocal entry_order_meta, entry_order_resp
        entry_order_meta = {}
        entry_order_resp = None
        if not can_open_entry("short", entry_level):
            return 0.0
        cl_ord_id = build_cl_ord_id("short", "open")
        raw_stop = float(entry_stop or float(sig["short_stop"]))
        planned_stop = normalize_entry_stop_for_attach("short", raw_stop)
        if planned_stop != raw_stop:
            log(
                f"[{inst_id}] Attach stop adjusted (short): raw={raw_stop:.6f} -> normalized={planned_stop:.6f}"
            )
        size = prepare_new_entry("short", planned_stop)
        size, _ = client.normalize_order_size(inst_id, size, reduce_only=False)
        attach_algo_ords = build_entry_attach_ords("short", planned_stop)
        if cfg.pos_mode == "net":
            resp = client.place_order(
                inst_id,
                "sell",
                size,
                pos_side=None,
                reduce_only=False,
                attach_algo_ords=attach_algo_ords,
                cl_ord_id=cl_ord_id,
            )
        else:
            resp = client.place_order(
                inst_id,
                "sell",
                size,
                pos_side="short",
                reduce_only=False,
                attach_algo_ords=attach_algo_ords,
                cl_ord_id=cl_ord_id,
            )
        mark_open_entry()
        entry_order_meta = extract_order_meta(resp)
        if cl_ord_id and "entry_cl_ord_id" not in entry_order_meta:
            entry_order_meta["entry_cl_ord_id"] = cl_ord_id
        entry_order_meta["planned_stop"] = float(planned_stop)
        entry_order_resp = resp
        log(f"[{inst_id}] Action: OPEN SHORT | size={round_size(size)} clOrdId={cl_ord_id}")
        notify_trade_execution(
            cfg,
            inst_id,
            "SHORT",
            size,
            sig,
            order_resp=resp,
            planned_stop=planned_stop,
            entry_level=entry_level,
        )
        return float(size)

    def close_long(size: float, action_tag: str = "close") -> float:
        if size <= 0:
            return 0.0
        try:
            size, _ = client.normalize_order_size(inst_id, size, reduce_only=True)
        except Exception:
            size = float(size)
        cl_ord_id = build_cl_ord_id("long", action_tag)
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "sell", size, pos_side=None, reduce_only=True, cl_ord_id=cl_ord_id)
        else:
            client.place_order(inst_id, "sell", size, pos_side="long", reduce_only=True, cl_ord_id=cl_ord_id)
        log(f"[{inst_id}] Action: CLOSE LONG clOrdId={cl_ord_id}")
        return float(size)

    def close_short(size: float, action_tag: str = "close") -> float:
        if size <= 0:
            return 0.0
        try:
            size, _ = client.normalize_order_size(inst_id, size, reduce_only=True)
        except Exception:
            size = float(size)
        cl_ord_id = build_cl_ord_id("short", action_tag)
        if cfg.pos_mode == "net":
            client.place_order(inst_id, "buy", size, pos_side=None, reduce_only=True, cl_ord_id=cl_ord_id)
        else:
            client.place_order(inst_id, "buy", size, pos_side="short", reduce_only=True, cl_ord_id=cl_ord_id)
        log(f"[{inst_id}] Action: CLOSE SHORT clOrdId={cl_ord_id}")
        return float(size)

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
            if prev_side in {"long", "short"} and prev_rem > 0:
                pnl_usdt = record_script_realized_loss(
                    side=prev_side,
                    entry_px=prev_entry,
                    exit_px=float(sig["close"]),
                    close_size=prev_rem,
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
                    size=prev_rem,
                    reason="external_close_or_attached_tpsl",
                    entry_px=prev_entry,
                    exit_px=float(sig["close"]),
                    stop_px=prev_stop if prev_stop and prev_stop > 0 else None,
                    pnl_usdt=float(pnl_usdt),
                    entry_level_value=int(prev_trade.get("entry_level", 0) or 0),
                    trade_ref=prev_trade,
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
    try:
        trade_state["remaining_size"] = float(pos.size)
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

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) >= entry + risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (long).")

        if (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry + risk * cfg.params.tp1_r_mult
            if float(sig["close"]) >= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    closed_sz = close_long(pos.size, action_tag="tp1_full")
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
                        closed_sz = close_long(pos.size, action_tag="tp1_full_fallback")
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
                        )
                        _record_stop_guard("long", is_stop_event=False, reason="tp1_full_fallback")
                        clear_trade_state()
                        return
                    closed_sz = close_long(close_size, action_tag="tp1_partial")
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
                    )
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
            tp2_price = entry + risk * cfg.params.tp2_r_mult
            if float(sig["close"]) >= tp2_price and pos.size > 0:
                closed_sz = close_long(pos.size, action_tag="tp2_full")
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
            closed_sz = close_long(pos.size, action_tag="close_exit")
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

        if (not trade_state.get("be_armed", False)) and float(sig["close"]) <= entry - risk * cfg.params.be_trigger_r_mult:
            trade_state["be_armed"] = True
            log("Management: BE armed (short).")

        if (not trade_state.get("tp1_done", False)) and cfg.params.tp1_close_pct > 0:
            tp1_price = entry - risk * cfg.params.tp1_r_mult
            if float(sig["close"]) <= tp1_price:
                pct = min(max(cfg.params.tp1_close_pct, 0.0), 1.0)
                close_size = pos.size * pct
                if close_size >= pos.size * 0.999:
                    closed_sz = close_short(pos.size, action_tag="tp1_full")
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
                        closed_sz = close_short(pos.size, action_tag="tp1_full_fallback")
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
                        )
                        _record_stop_guard("short", is_stop_event=False, reason="tp1_full_fallback")
                        clear_trade_state()
                        return
                    closed_sz = close_short(close_size, action_tag="tp1_partial")
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
                    )
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
            tp2_price = entry - risk * cfg.params.tp2_r_mult
            if float(sig["close"]) <= tp2_price and pos.size > 0:
                closed_sz = close_short(pos.size, action_tag="tp2_full")
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
            closed_sz = close_short(pos.size, action_tag="close_exit")
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
