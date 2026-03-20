from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.models import PositionState
from okx_trader.runtime_execute_decision import execute_decision


class _BaseStopClient:
    def __init__(self) -> None:
        self.amend_calls: list[dict] = []

    def get_instrument(self, inst_id: str) -> dict:
        return {"lotSz": "0.1", "minSz": "0.1", "tickSz": "0.001", "ctVal": "1", "ctValCcy": "USDT"}

    def amend_algo_sl(self, **kwargs):
        self.amend_calls.append(dict(kwargs))
        return {"data": [{"algoId": kwargs.get("algo_id", "ALG-NEW"), "algoClOrdId": kwargs.get("algo_cl_ord_id", "ALGCL-NEW")}]}


class _BinanceStyleStopClient(_BaseStopClient):
    def get_pending_stop_loss_order(self, **kwargs):
        stop_price = float(kwargs.get("stop_price", 0.0) or 0.0)
        live_stop = 105.0
        tol = max(abs(stop_price) * 1e-6, 1e-9)
        return {
            "attachAlgoId": "ALG-SL-1",
            "attachAlgoClOrdId": "ALGCL-SL-1",
            "slTriggerPx": f"{live_stop:.3f}",
            "_qty_match": True,
            "_stop_match": abs(live_stop - stop_price) <= tol,
            "_extra_count": 0,
        }


class _OkxStyleAttachedStopClient(_BaseStopClient):
    @staticmethod
    def _extract_attach_algo(order_row: dict, *, prefer: str = "") -> dict:
        rows = order_row.get("attachAlgoOrds")
        if not isinstance(rows, list):
            return {}
        prefer_norm = str(prefer or "").strip().lower()
        if prefer_norm == "sl":
            for item in rows:
                if isinstance(item, dict) and str(item.get("slTriggerPx", "") or "").strip():
                    return dict(item)
        for item in rows:
            if isinstance(item, dict):
                return dict(item)
        return {}

    def get_order(self, *, inst_id: str, ord_id: str = "", cl_ord_id: str = ""):
        return {
            "ordId": ord_id or "ENTRY-1",
            "clOrdId": cl_ord_id or "ENTRYCL-1",
            "attachAlgoOrds": [
                {
                    "attachAlgoId": "ALG-SL-OKX-1",
                    "attachAlgoClOrdId": "ALGCL-SL-OKX-1",
                    "slTriggerPx": "105.000",
                }
            ],
        }


class ExchangeStopSyncTests(unittest.TestCase):
    def _cfg(self, provider: str) -> SimpleNamespace:
        return SimpleNamespace(
            exchange_provider=provider,
            alert_only=False,
            paper=True,
            dry_run=False,
            attach_tpsl_on_entry=True,
            attach_tpsl_trigger_px_type="mark",
            ltf_bar="15m",
            trade_journal_enabled=False,
            trade_order_link_enabled=False,
            td_mode="isolated",
            pos_mode="net",
            params=SimpleNamespace(
                exec_l3_inst_ids=[],
                exec_max_level=3,
                tp1_r_mult=1.5,
                tp2_r_mult=2.5,
                tp1_close_pct=0.5,
                tp2_close_rest=True,
                be_trigger_r_mult=1.0,
                be_offset_pct=0.0005,
                be_fee_buffer_pct=0.0008,
                auto_tighten_stop=False,
                trail_after_tp1=True,
                trail_atr_mult=0.0,
                open_window_hours=24,
                stop_streak_freeze_count=0,
                stop_streak_freeze_hours=0,
                stop_streak_l2_only=False,
                enable_close=True,
                signal_exit_enabled=False,
                split_tp_on_entry=True,
                allow_reverse=False,
                manage_only_script_positions=False,
            ),
        )

    def _sig(self) -> dict:
        return {
            "bias": "SHORT",
            "close": 95.0,
            "ema": 96.0,
            "rsi": 45.0,
            "macd_hist": -0.1,
            "bb_width": 0.02,
            "bb_width_avg": 0.01,
            "htf_close": 96.0,
            "htf_ema_fast": 95.0,
            "htf_ema_slow": 97.0,
            "htf_rsi": 44.0,
            "strategy_variant": "classic",
            "location_long_ok": False,
            "location_short_ok": True,
            "fib_touch_long": False,
            "fib_touch_short": True,
            "retest_long": False,
            "retest_short": True,
            "smc_sweep_long": False,
            "smc_sweep_short": False,
            "smc_bullish_fvg": False,
            "smc_bearish_fvg": False,
            "long_entry": False,
            "short_entry": False,
            "long_entry_l2": False,
            "short_entry_l2": False,
            "long_entry_l3": False,
            "short_entry_l3": False,
            "long_level": 0,
            "short_level": 0,
            "long_exit": False,
            "short_exit": False,
            "long_stop": 96.0,
            "short_stop": 104.0,
            "atr": 2.0,
            "signal_ts_ms": 1_700_000_000_000,
        }

    def _trade_state(self) -> dict:
        return {
            "side": "short",
            "managed_by": "script",
            "entry_price": 100.0,
            "hard_stop": 99.87,
            "planned_stop": 105.0,
            "risk": 5.0,
            "tp1_done": True,
            "be_armed": True,
            "open_size": 5.0,
            "remaining_size": 5.0,
            "realized_size": 2.5,
            "realized_pnl_usdt": 3.0,
            "created_ts_ms": 1_699_999_900_000,
            "exchange_sl_post_open_enabled": True,
            "exchange_sl_independent": True,
            "exchange_sl_attach_algo_id": "ALG-SL-1",
            "exchange_sl_attach_algo_cl_ord_id": "ALGCL-SL-1",
        }

    def test_binance_style_post_open_stop_repairs_to_hard_stop_after_tp1(self) -> None:
        client = _BinanceStyleStopClient()
        state = {"trade": self._trade_state()}

        with patch("okx_trader.runtime_execute_decision.resolve_entry_decision", return_value=None), patch(
            "okx_trader.runtime_execute_decision.log"
        ):
            execute_decision(
                client=client,
                cfg=self._cfg("binance"),
                inst_id="FIL-USDT-SWAP",
                sig=self._sig(),
                pos=PositionState("short", 5.0),
                state=state,
                root_state={},
                profile_id="DEFAULT",
            )

        self.assertEqual(len(client.amend_calls), 1)
        self.assertAlmostEqual(float(client.amend_calls[0]["new_sl_trigger_px"]), 99.87, places=6)
        self.assertAlmostEqual(float(state["trade"]["exchange_sl_px"]), 99.87, places=6)
        self.assertEqual(str(state["trade"].get("exchange_sl_last_reason", "")), "loop_short:repair")

    def test_okx_style_attached_stop_repairs_to_hard_stop_after_tp1(self) -> None:
        client = _OkxStyleAttachedStopClient()
        trade = self._trade_state()
        trade["exchange_sl_independent"] = False
        trade["entry_ord_id"] = "ENTRY-1"
        trade["entry_cl_ord_id"] = "ENTRYCL-1"
        trade.pop("exchange_sl_attach_algo_id", None)
        trade.pop("exchange_sl_attach_algo_cl_ord_id", None)
        state = {"trade": trade}

        with patch("okx_trader.runtime_execute_decision.resolve_entry_decision", return_value=None), patch(
            "okx_trader.runtime_execute_decision.log"
        ):
            execute_decision(
                client=client,
                cfg=self._cfg("okx"),
                inst_id="FIL-USDT-SWAP",
                sig=self._sig(),
                pos=PositionState("short", 5.0),
                state=state,
                root_state={},
                profile_id="DEFAULT",
            )

        self.assertEqual(len(client.amend_calls), 1)
        self.assertAlmostEqual(float(client.amend_calls[0]["new_sl_trigger_px"]), 99.87, places=6)
        self.assertAlmostEqual(float(state["trade"]["exchange_sl_px"]), 99.87, places=6)
        self.assertEqual(str(state["trade"].get("exchange_sl_last_reason", "")), "loop_short:repair")


if __name__ == "__main__":
    unittest.main()
