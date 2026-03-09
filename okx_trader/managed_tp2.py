from __future__ import annotations

import time
from typing import Any, Dict, Tuple

from .common import log, round_size
from .models import Config
from .okx_client import OKXClient
from .runtime_order_id import build_runtime_order_cl_id


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def has_external_tp1_fill_mode(trade: Dict[str, Any]) -> bool:
    if not isinstance(trade, dict):
        return False
    if bool(trade.get("exchange_split_tp_enabled", False)):
        return True
    if bool(trade.get("exchange_tp1_on_entry_enabled", False)):
        return True
    if bool(trade.get("managed_tp1_enabled", False)):
        return True
    return bool(trade.get("managed_tp2_enabled", False))


def _is_managed_tp_enabled(trade: Dict[str, Any], stage: str) -> bool:
    if not isinstance(trade, dict):
        return False
    stage_norm = str(stage or "").strip().lower()
    if stage_norm not in {"tp1", "tp2"}:
        return False
    return bool(trade.get(f"managed_{stage_norm}_enabled", False))


def is_managed_tp1_enabled(trade: Dict[str, Any]) -> bool:
    return _is_managed_tp_enabled(trade, "tp1")


def is_managed_tp2_enabled(trade: Dict[str, Any]) -> bool:
    return _is_managed_tp_enabled(trade, "tp2")


def _clear_managed_tp_order_state(trade: Dict[str, Any], stage: str) -> None:
    if not isinstance(trade, dict):
        return
    stage_norm = str(stage or "").strip().lower()
    prefix = f"managed_{stage_norm}_"
    for key in (
        f"{prefix}ord_id",
        f"{prefix}cl_ord_id",
        f"{prefix}order_px",
        f"{prefix}order_size",
        f"{prefix}order_state",
    ):
        trade.pop(key, None)


def clear_managed_tp1_order_state(trade: Dict[str, Any]) -> None:
    _clear_managed_tp_order_state(trade, "tp1")


def clear_managed_tp2_order_state(trade: Dict[str, Any]) -> None:
    _clear_managed_tp_order_state(trade, "tp2")


def _cancel_managed_tp_order(
    *,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    stage: str,
    reason: str = "",
    quiet: bool = False,
) -> bool:
    stage_norm = str(stage or "").strip().lower()
    if stage_norm not in {"tp1", "tp2"}:
        return False
    if not _is_managed_tp_enabled(trade, stage_norm):
        return False
    ord_id = str(trade.get(f"managed_{stage_norm}_ord_id", "") or "").strip()
    cl_ord_id = str(trade.get(f"managed_{stage_norm}_cl_ord_id", "") or "").strip()
    if not ord_id and not cl_ord_id:
        return False
    now_ms = int(time.time() * 1000)
    label = stage_norm.upper()
    try:
        client.cancel_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
    except Exception as e:
        if not quiet:
            log(
                f"[{inst_id}] Managed {label} cancel warning: ordId={ord_id or '-'} clOrdId={cl_ord_id or '-'} reason={reason or '-'} err={e}",
                level="WARN",
            )
        return False
    trade[f"managed_{stage_norm}_last_cancel_ts_ms"] = now_ms
    trade[f"managed_{stage_norm}_last_cancel_reason"] = str(reason or "").strip()
    _clear_managed_tp_order_state(trade, stage_norm)
    if not quiet:
        log(
            f"[{inst_id}] Managed {label} canceled: ordId={ord_id or '-'} clOrdId={cl_ord_id or '-'} reason={reason or '-'}"
        )
    return True


def cancel_managed_tp1_order(
    *,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    reason: str = "",
    quiet: bool = False,
) -> bool:
    return _cancel_managed_tp_order(
        client=client,
        inst_id=inst_id,
        trade=trade,
        stage="tp1",
        reason=reason,
        quiet=quiet,
    )


def cancel_managed_tp2_order(
    *,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    reason: str = "",
    quiet: bool = False,
) -> bool:
    return _cancel_managed_tp_order(
        client=client,
        inst_id=inst_id,
        trade=trade,
        stage="tp2",
        reason=reason,
        quiet=quiet,
    )


def _managed_target_size(trade: Dict[str, Any], stage: str) -> float:
    stage_norm = str(stage or "").strip().lower()
    size_keys = [f"managed_{stage_norm}_target_size", f"exchange_{stage_norm}_size"]
    if stage_norm == "tp2":
        size_keys = ["remaining_size", *size_keys, "open_size"]
    else:
        size_keys = [*size_keys, "open_size"]
    for key in size_keys:
        size_val = max(0.0, _safe_float(trade.get(key, 0.0), 0.0))
        if size_val > 0:
            return size_val
    return 0.0


def _managed_target_px(trade: Dict[str, Any], stage: str) -> float:
    stage_norm = str(stage or "").strip().lower()
    return _safe_float(trade.get(f"managed_{stage_norm}_target_px", trade.get(f"exchange_{stage_norm}_px", 0.0)), 0.0)


def _ensure_managed_tp_limit_order(
    *,
    cfg: Config,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    signal_ts_ms: int,
    level: int = 0,
    reason: str = "",
    stage: str,
) -> Tuple[bool, str]:
    stage_norm = str(stage or "").strip().lower()
    if stage_norm not in {"tp1", "tp2"}:
        return False, "invalid_stage"
    if not _is_managed_tp_enabled(trade, stage_norm):
        return False, f"managed_{stage_norm}_disabled"
    if (stage_norm == "tp1") and bool(trade.get("tp1_done", False)):
        return False, "tp1_already_done"
    if (stage_norm == "tp2") and (not bool(trade.get("tp1_done", False))):
        return False, "tp1_not_done"

    side = str(trade.get("side", "") or "").strip().lower()
    if side not in {"long", "short"}:
        return False, "invalid_side"

    target_px = _managed_target_px(trade, stage_norm)
    if target_px <= 0:
        return False, f"invalid_{stage_norm}_px"

    target_size = _managed_target_size(trade, stage_norm)
    if target_size <= 0:
        return False, "no_remaining_size" if stage_norm == "tp2" else f"invalid_{stage_norm}_size"

    ord_id = str(trade.get(f"managed_{stage_norm}_ord_id", "") or "").strip()
    cl_ord_id = str(trade.get(f"managed_{stage_norm}_cl_ord_id", "") or "").strip()
    if ord_id or cl_ord_id:
        try:
            row = client.get_order(inst_id=inst_id, ord_id=ord_id, cl_ord_id=cl_ord_id)
        except Exception as e:
            return False, f"{stage_norm}_get_order_failed:{e}"
        if isinstance(row, dict) and row:
            state = str(row.get("state", "") or "").strip().lower()
            row_ord_id = str(row.get("ordId", "") or "").strip()
            row_cl_ord_id = str(row.get("clOrdId", "") or "").strip()
            if row_ord_id:
                trade[f"managed_{stage_norm}_ord_id"] = row_ord_id
            if row_cl_ord_id:
                trade[f"managed_{stage_norm}_cl_ord_id"] = row_cl_ord_id
            trade[f"managed_{stage_norm}_order_state"] = state
            if state in {"live", "partially_filled"}:
                return True, state or "live"
            if state == "filled":
                return True, "filled"
        _clear_managed_tp_order_state(trade, stage_norm)

    order_side = "sell" if side == "long" else "buy"
    pos_side = None if cfg.pos_mode == "net" else side
    signal_ts = int(signal_ts_ms or int(time.time() * 1000))
    cl_ord_id = build_runtime_order_cl_id(
        inst_id=inst_id,
        side=side,
        signal_ts_ms=signal_ts,
        action_tag=f"{stage_norm}_limit",
        level=int(level or 0),
        extra=str(reason or stage_norm),
    )
    try:
        resp = client.place_order(
            inst_id,
            order_side,
            target_size,
            pos_side=pos_side,
            reduce_only=True,
            cl_ord_id=cl_ord_id,
            ord_type="limit",
            px=target_px,
        )
    except Exception as e:
        trade[f"managed_{stage_norm}_last_fail_ts_ms"] = int(time.time() * 1000)
        trade[f"managed_{stage_norm}_last_fail_reason"] = str(reason or "").strip()
        return False, str(e)

    row = {}
    if isinstance(resp, dict):
        rows = resp.get("data")
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            row = rows[0]
    trade[f"managed_{stage_norm}_ord_id"] = str(row.get("ordId", "") or "").strip()
    trade[f"managed_{stage_norm}_cl_ord_id"] = str(row.get("clOrdId", cl_ord_id) or cl_ord_id).strip()
    trade[f"managed_{stage_norm}_order_px"] = float(target_px)
    trade[f"managed_{stage_norm}_order_size"] = float(target_size)
    trade[f"managed_{stage_norm}_order_state"] = "live"
    trade[f"managed_{stage_norm}_last_place_ts_ms"] = int(time.time() * 1000)
    trade[f"managed_{stage_norm}_last_place_reason"] = str(reason or "").strip()
    log(
        f"[{inst_id}] Managed {stage_norm.upper()} placed ({side}) size={round_size(target_size)} px={target_px:.6f} clOrdId={trade[f'managed_{stage_norm}_cl_ord_id'] or '-'}"
    )
    return True, "placed"


def ensure_managed_tp1_limit_order(
    *,
    cfg: Config,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    signal_ts_ms: int,
    level: int = 0,
    reason: str = "",
) -> Tuple[bool, str]:
    return _ensure_managed_tp_limit_order(
        cfg=cfg,
        client=client,
        inst_id=inst_id,
        trade=trade,
        signal_ts_ms=signal_ts_ms,
        level=level,
        reason=reason,
        stage="tp1",
    )


def ensure_managed_tp2_limit_order(
    *,
    cfg: Config,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    signal_ts_ms: int,
    level: int = 0,
    reason: str = "",
) -> Tuple[bool, str]:
    return _ensure_managed_tp_limit_order(
        cfg=cfg,
        client=client,
        inst_id=inst_id,
        trade=trade,
        signal_ts_ms=signal_ts_ms,
        level=level,
        reason=reason,
        stage="tp2",
    )
