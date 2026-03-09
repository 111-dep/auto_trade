from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Dict, Optional, Tuple

from .common import log
from .config import get_strategy_params
from .managed_tp2 import clear_managed_tp1_order_state, ensure_managed_tp2_limit_order, has_external_tp1_fill_mode
from .models import Config
from .okx_client import OKXClient
from .state_store import _get_inst_state

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover
    websockets = None  # type: ignore


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _sync_ws_be_stop(
    *,
    client: OKXClient,
    inst_id: str,
    trade: Dict[str, Any],
    new_sl: float,
) -> Tuple[bool, str]:
    attach_algo_id = str(trade.get("exchange_sl_attach_algo_id", trade.get("attach_algo_id", "")) or "").strip()
    attach_algo_cl_id = str(trade.get("exchange_sl_attach_algo_cl_ord_id", trade.get("attach_algo_cl_ord_id", "")) or "").strip()
    entry_ord_id = str(trade.get("entry_ord_id", "") or "").strip()
    entry_cl_ord_id = str(trade.get("entry_cl_ord_id", "") or "").strip()

    if new_sl <= 0:
        return False, "invalid new_sl"

    # Split TP mode should have attach algo id on TP2 leg; try amend-algos first.
    if attach_algo_id or attach_algo_cl_id:
        try:
            client.amend_algo_sl(
                inst_id=inst_id,
                algo_id=attach_algo_id,
                algo_cl_ord_id=attach_algo_cl_id,
                new_sl_trigger_px=float(new_sl),
            )
            return True, ""
        except Exception as e_algo:
            if entry_ord_id or entry_cl_ord_id:
                try:
                    client.amend_order_attached_sl(
                        inst_id=inst_id,
                        ord_id=entry_ord_id,
                        cl_ord_id=entry_cl_ord_id,
                        attach_algo_id=attach_algo_id,
                        attach_algo_cl_ord_id=attach_algo_cl_id,
                        new_sl_trigger_px=float(new_sl),
                    )
                    return True, ""
                except Exception as e_order:
                    return False, f"amend-algos={e_algo} | amend-order={e_order}"
            return False, f"amend-algos={e_algo}"

    if entry_ord_id or entry_cl_ord_id:
        try:
            client.amend_order_attached_sl(
                inst_id=inst_id,
                ord_id=entry_ord_id,
                cl_ord_id=entry_cl_ord_id,
                new_sl_trigger_px=float(new_sl),
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    return False, "missing attach algo id and entry order id"


def handle_tp1_fill_from_position(
    *,
    cfg: Config,
    client: OKXClient,
    inst_id: str,
    inst_state: Dict[str, Any],
    pos_side: str,
    pos_size: float,
    event_ts_ms: Optional[int] = None,
) -> bool:
    side = str(pos_side or "").strip().lower()
    if side not in {"long", "short"}:
        return False

    trade = inst_state.get("trade")
    if not isinstance(trade, dict):
        return False
    if not has_external_tp1_fill_mode(trade):
        return False
    if bool(trade.get("tp1_done", False)):
        return False
    trade_side = str(trade.get("side", "")).strip().lower()
    if trade_side != side:
        return False

    current_pos_size = max(0.0, _safe_float(pos_size, 0.0))
    prev_remaining = _safe_float(trade.get("remaining_size", 0.0), 0.0)
    if prev_remaining <= 0:
        prev_remaining = _safe_float(trade.get("open_size", 0.0), 0.0)
    trade["remaining_size"] = float(current_pos_size)

    expected_tp2_size = _safe_float(trade.get("exchange_tp2_size", 0.0), 0.0)
    if expected_tp2_size <= 0 or current_pos_size <= 0:
        return False

    size_tol = max(1e-9, expected_tp2_size * 0.002)
    tp1_inferred = (
        prev_remaining > expected_tp2_size + size_tol
        and current_pos_size <= expected_tp2_size + size_tol
    )
    if not tp1_inferred:
        return False

    entry = _safe_float(trade.get("entry_price", 0.0), 0.0)
    if entry <= 0:
        return False
    params = get_strategy_params(cfg, inst_id)
    be_total_offset = max(0.0, float(params.be_offset_pct) + float(params.be_fee_buffer_pct))

    if side == "long":
        be_stop = entry * (1.0 + be_total_offset)
        trade["hard_stop"] = max(_safe_float(trade.get("hard_stop", be_stop), be_stop), be_stop)
    else:
        be_stop = entry * (1.0 - be_total_offset)
        trade["hard_stop"] = min(_safe_float(trade.get("hard_stop", be_stop), be_stop), be_stop)

    trade["tp1_done"] = True
    trade["be_armed"] = True
    clear_managed_tp1_order_state(trade)

    now_ms = int(event_ts_ms or int(time.time() * 1000))
    target_sl = _safe_float(trade.get("hard_stop", 0.0), 0.0)
    ok, err = _sync_ws_be_stop(
        client=client,
        inst_id=inst_id,
        trade=trade,
        new_sl=target_sl,
    )
    if ok:
        trade["exchange_sl_px"] = float(target_sl)
        trade["exchange_sl_last_sync_ts_ms"] = now_ms
        trade["exchange_sl_last_reason"] = "tp1_be_ws"
        log(
            f"[{inst_id}] WS fast-manage: TP1 filled -> BE SL synced ({side}) "
            f"remain={current_pos_size:.6f} sl={target_sl:.6f}"
        )
    else:
        trade["exchange_sl_last_fail_ts_ms"] = now_ms
        log(
            f"[{inst_id}] WS fast-manage: TP1 filled but BE SL sync failed ({side}): {err}",
            level="WARN",
        )

    if bool(trade.get("managed_tp2_enabled", False)) and bool(params.tp2_close_rest) and current_pos_size > 0:
        ok_tp2, err_tp2 = ensure_managed_tp2_limit_order(
            cfg=cfg,
            client=client,
            inst_id=inst_id,
            trade=trade,
            signal_ts_ms=now_ms,
            level=int(trade.get("entry_level", 0) or 0),
            reason="tp1_be_ws",
        )
        if (not ok_tp2) and err_tp2 not in {"filled", "live", "partially_filled"}:
            log(
                f"[{inst_id}] WS fast-manage: TP1 filled but managed TP2 place failed ({side}): {err_tp2}",
                level="WARN",
            )
    return True


class OKXWsTp1BeWorker:
    def __init__(self, cfg: Config, client: OKXClient, state: Dict[str, Any]):
        self.cfg = cfg
        self.client = client
        self.state = state
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

    def _enabled(self) -> bool:
        if not bool(self.cfg.ws_tp1_be_enabled):
            return False
        if websockets is None:
            log("[WS] websockets package not found; WS TP1 fast-manage disabled.", level="WARN")
            return False
        if self.cfg.alert_only or self.cfg.dry_run:
            return False
        if not self.cfg.attach_tpsl_on_entry:
            return False
        if not (self.cfg.api_key and self.cfg.secret_key and self.cfg.passphrase):
            return False
        return True

    def start(self) -> None:
        if not self._enabled():
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run, name="okx-ws-tp1-be", daemon=True)
        self._thread.start()
        log(f"[WS] TP1 fast-manage worker started | url={self.cfg.ws_private_url}")

    def stop(self, timeout_sec: float = 3.0) -> None:
        self._stop_evt.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(0.2, float(timeout_sec)))
        self._thread = None

    def _run(self) -> None:
        asyncio.run(self._run_loop())

    def _build_login_payload(self) -> Dict[str, Any]:
        ts = str(time.time())
        prehash = f"{ts}GET/users/self/verify"
        sign = self.client._sign(prehash)  # Reuse REST signer.
        return {
            "op": "login",
            "args": [
                {
                    "apiKey": self.cfg.api_key,
                    "passphrase": self.cfg.passphrase,
                    "timestamp": ts,
                    "sign": sign,
                }
            ],
        }

    async def _expect_event(
        self,
        ws: Any,
        *,
        event_name: str,
        channel: str = "",
    ) -> None:
        deadline = time.time() + 15.0
        while not self._stop_evt.is_set():
            if time.time() > deadline:
                raise RuntimeError(f"WS wait event timeout: {event_name}")
            raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
            msg = self._decode_msg(raw)
            if not msg:
                continue
            event = str(msg.get("event", "") or "").strip().lower()
            if event == "error":
                raise RuntimeError(f"WS error event: {msg}")
            if event != event_name:
                continue
            if channel:
                arg = msg.get("arg")
                ch = str(arg.get("channel", "") if isinstance(arg, dict) else "").strip().lower()
                if ch != channel:
                    continue
            code = str(msg.get("code", "0") or "0").strip()
            if code not in {"", "0"}:
                raise RuntimeError(f"WS {event_name} failed: {msg}")
            return
        raise RuntimeError(f"WS stopped while waiting event: {event_name}")

    @staticmethod
    def _decode_msg(raw: Any) -> Dict[str, Any]:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")
            return json.loads(str(raw))
        except Exception:
            return {}

    def _parse_position_update(self, row: Dict[str, Any]) -> Tuple[str, str, float]:
        inst_id = str(row.get("instId", "") or "").strip().upper()
        if not inst_id:
            return "", "flat", 0.0
        mgn_mode = str(row.get("mgnMode", "") or "").strip().lower()
        if mgn_mode and mgn_mode != str(self.cfg.td_mode).strip().lower():
            return "", "flat", 0.0

        pos_side_raw = str(row.get("posSide", "") or "").strip().lower()
        pos_val = _safe_float(row.get("pos", "0"), 0.0)
        size = abs(float(pos_val))

        if pos_side_raw in {"long", "short"}:
            return inst_id, pos_side_raw, size
        if pos_val > 0:
            return inst_id, "long", size
        if pos_val < 0:
            return inst_id, "short", size
        return inst_id, "flat", 0.0

    def _handle_ws_msg(self, msg: Dict[str, Any]) -> None:
        arg = msg.get("arg")
        if not isinstance(arg, dict):
            return
        if str(arg.get("channel", "") or "").strip().lower() != "positions":
            return
        rows = msg.get("data")
        if not isinstance(rows, list):
            return

        for row in rows:
            if not isinstance(row, dict):
                continue
            inst_id, side, size = self._parse_position_update(row)
            if not inst_id or inst_id not in self.cfg.inst_ids:
                continue
            if side not in {"long", "short"}:
                continue
            event_ts_ms = int(_safe_float(row.get("uTime", "0"), 0.0))
            if event_ts_ms <= 0:
                event_ts_ms = int(time.time() * 1000)
            inst_state = _get_inst_state(self.state, inst_id)
            handle_tp1_fill_from_position(
                cfg=self.cfg,
                client=self.client,
                inst_id=inst_id,
                inst_state=inst_state,
                pos_side=side,
                pos_size=size,
                event_ts_ms=event_ts_ms,
            )

    async def _run_once(self) -> None:
        assert websockets is not None
        async with websockets.connect(
            self.cfg.ws_private_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_queue=2048,
        ) as ws:
            await ws.send(json.dumps(self._build_login_payload(), separators=(",", ":")))
            await self._expect_event(ws, event_name="login")

            sub_payload = {"op": "subscribe", "args": [{"channel": "positions", "instType": "SWAP"}]}
            await ws.send(json.dumps(sub_payload, separators=(",", ":")))
            await self._expect_event(ws, event_name="subscribe", channel="positions")
            log("[WS] positions subscription ready.")

            while not self._stop_evt.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3.0)
                except asyncio.TimeoutError:
                    continue
                msg = self._decode_msg(raw)
                if not msg:
                    continue
                self._handle_ws_msg(msg)

    async def _run_loop(self) -> None:
        while not self._stop_evt.is_set():
            try:
                await self._run_once()
            except Exception as e:
                if self._stop_evt.is_set():
                    break
                log(f"[WS] TP1 fast-manage disconnected, reconnect in {self.cfg.ws_reconnect_seconds}s: {e}", level="WARN")
                await asyncio.sleep(float(max(1, int(self.cfg.ws_reconnect_seconds))))
