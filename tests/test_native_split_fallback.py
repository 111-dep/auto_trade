from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.decision_core import EntryDecision
from okx_trader.models import PositionState
from okx_trader.runtime_execute_decision import execute_decision


class _FakeClient:
    def __init__(self) -> None:
        self.place_calls: list[dict] = []

    def use_pos_side(self, inst_id: str) -> bool:
        return False

    def ensure_leverage(self, inst_id: str, pos_side: str | None, entry_side: str = "") -> None:
        return None

    def get_instrument(self, inst_id: str) -> dict:
        return {"lotSz": "1", "minSz": "1", "ctVal": "1", "ctValCcy": "USDT"}

    def normalize_order_size(self, inst_id: str, sz: float, reduce_only: bool = False) -> tuple[float, str]:
        val = float(int(float(sz))) if float(sz) >= 1 else float(sz)
        if val <= 0:
            val = float(sz)
        return val, f"{val:g}"

    def build_attach_tpsl_ords(self, *, tp_price: float, sl_price: float, attach_algo_cl_ord_id: str = "") -> list[dict]:
        return [
            {"tpTriggerPx": f"{float(tp_price):g}", "tpOrdPx": "-1", "attachAlgoClOrdId": f"{attach_algo_cl_ord_id}_tp"},
            {"slTriggerPx": f"{float(sl_price):g}", "slOrdPx": "-1", "attachAlgoClOrdId": attach_algo_cl_ord_id},
        ]

    def build_partial_tp_attach_algo_ords(
        self,
        *,
        tp_price: float,
        tp_size: str,
        sl_price: float,
        tp_attach_algo_cl_ord_id: str = "",
        sl_attach_algo_cl_ord_id: str = "",
    ) -> list[dict]:
        return [
            {
                "tpTriggerPx": f"{float(tp_price):g}",
                "tpOrdPx": "-1",
                "sz": str(tp_size),
                "attachAlgoClOrdId": tp_attach_algo_cl_ord_id,
            },
            {
                "slTriggerPx": f"{float(sl_price):g}",
                "slOrdPx": "-1",
                "attachAlgoClOrdId": sl_attach_algo_cl_ord_id,
            },
        ]

    def build_sl_attach_algo_ords(self, *, sl_price: float, attach_algo_cl_ord_id: str = "") -> list[dict]:
        return [
            {
                "slTriggerPx": f"{float(sl_price):g}",
                "slOrdPx": "-1",
                "attachAlgoClOrdId": attach_algo_cl_ord_id,
            }
        ]

    def build_split_tp_attach_algo_ords(
        self,
        *,
        tp1_price: float,
        tp1_size: str,
        tp2_price: float,
        tp2_size: str,
        sl_price: float,
        tp1_attach_algo_cl_ord_id: str = "",
        tp2_attach_algo_cl_ord_id: str = "",
        sl_attach_algo_cl_ord_id: str = "",
        move_sl_to_avg_px_on_tp1: bool = False,
    ) -> list[dict]:
        return [
            {
                "tpTriggerPx": f"{float(tp1_price):g}",
                "tpOrdPx": "-1",
                "sz": str(tp1_size),
                "attachAlgoClOrdId": tp1_attach_algo_cl_ord_id,
            },
            {
                "tpTriggerPx": f"{float(tp2_price):g}",
                "tpOrdPx": "-1",
                "sz": str(tp2_size),
                "attachAlgoClOrdId": tp2_attach_algo_cl_ord_id,
            },
            {
                "slTriggerPx": f"{float(sl_price):g}",
                "slOrdPx": "-1",
                "attachAlgoClOrdId": sl_attach_algo_cl_ord_id,
                "amendPxOnTriggerType": "1" if move_sl_to_avg_px_on_tp1 else "0",
            },
        ]

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
        if prefer_norm == "tp":
            for item in rows:
                if isinstance(item, dict) and str(item.get("tpTriggerPx", "") or "").strip():
                    return dict(item)
        for item in rows:
            if isinstance(item, dict):
                return dict(item)
        return {}

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side: str | None = None,
        reduce_only: bool = False,
        attach_algo_ords: list[dict] | None = None,
        cl_ord_id: str = "",
        ord_type: str = "market",
        px: float = 0.0,
        post_only: bool = False,
    ) -> dict:
        call = {
            "inst_id": inst_id,
            "side": side,
            "sz": float(sz),
            "attach_count": len(attach_algo_ords or []),
            "cl_ord_id": cl_ord_id,
            "ord_type": ord_type,
            "reduce_only": bool(reduce_only),
            "px": float(px),
        }
        self.place_calls.append(call)
        if len(attach_algo_ords or []) == 3:
            raise RuntimeError(
                "OKX API error: code=1 msg=Order failed data=[{'sCode':'51078','sMsg':'You can\'t set multiple TPs as a lead trader'}]"
            )
        idx = len(self.place_calls)
        return {
            "data": [
                {
                    "ordId": f"ord{idx}",
                    "clOrdId": cl_ord_id,
                    "attachAlgoOrds": attach_algo_ords or [],
                }
            ]
        }


class NativeSplitFallbackTests(unittest.TestCase):
    def _cfg(self) -> SimpleNamespace:
        params = SimpleNamespace(
            exec_l3_inst_ids=[],
            exec_max_level=3,
            tp1_r_mult=1.5,
            tp2_r_mult=2.5,
            tp1_close_pct=0.5,
            tp2_close_rest=True,
            be_trigger_r_mult=1.0,
            be_offset_pct=0.0,
            be_fee_buffer_pct=0.0,
            trail_after_tp1=True,
            trail_atr_mult=1.5,
            max_open_entries=100,
            max_open_entries_global=100,
            open_window_hours=24,
            min_open_interval_minutes=0,
            daily_loss_limit_pct=0.0,
            daily_loss_base_usdt=0.0,
            daily_loss_base_mode="current",
            stop_reentry_cooldown_minutes=0,
            stop_streak_freeze_count=0,
            stop_streak_freeze_hours=0,
            stop_streak_l2_only=False,
            enable_close=True,
            signal_exit_enabled=True,
            split_tp_on_entry=True,
            allow_reverse=False,
            entry_exec_mode="market",
            entry_exec_mode_l1="",
            entry_exec_mode_l2="",
            entry_exec_mode_l3="",
            entry_auto_market_level_min=0,
            entry_auto_market_level_max=0,
            entry_limit_offset_bps=0.0,
            entry_limit_ttl_sec=5,
            entry_limit_ttl_sec_l1=0,
            entry_limit_ttl_sec_l2=0,
            entry_limit_ttl_sec_l3=0,
            entry_limit_poll_ms=100,
            entry_limit_reprice_max=0,
            entry_limit_fallback_mode="market",
            entry_limit_fallback_mode_l1="",
            entry_limit_fallback_mode_l2="",
            entry_limit_fallback_mode_l3="",
            manage_only_script_positions=False,
            skip_on_foreign_mgnmode_pos=False,
            min_risk_atr_mult=1.0,
            min_risk_pct=0.01,
            risk_frac=0.0,
            risk_max_margin_frac=0.0,
        )
        return SimpleNamespace(
            params=params,
            attach_tpsl_on_entry=True,
            attach_tpsl_tp_r=2.5,
            attach_tpsl_trigger_px_type="last",
            alert_only=False,
            paper=True,
            dry_run=False,
            sizing_mode="fixed",
            order_size=10.0,
            margin_usdt=25.0,
            leverage=5.0,
            td_mode="cross",
            pos_mode="net",
            ltf_bar="15m",
            api_key="",
            secret_key="",
            passphrase="",
            trade_journal_enabled=False,
            trade_order_link_enabled=False,
            alert_email_enabled=False,
            alert_tg_trade_exec_enabled=False,
            alert_local_sound=False,
            alert_local_file=False,
            alert_local_file_path="",
        )

    def _sig(self, signal_ts_ms: int) -> dict:
        return {
            "bias": "LONG",
            "close": 100.0,
            "ema": 99.0,
            "rsi": 55.0,
            "macd_hist": 0.1,
            "bb_width": 0.02,
            "bb_width_avg": 0.015,
            "htf_close": 100.0,
            "htf_ema_fast": 99.0,
            "htf_ema_slow": 98.0,
            "htf_rsi": 56.0,
            "strategy_variant": "classic",
            "location_long_ok": True,
            "location_short_ok": False,
            "fib_touch_long": True,
            "fib_touch_short": False,
            "retest_long": True,
            "retest_short": False,
            "smc_sweep_long": False,
            "smc_sweep_short": False,
            "smc_bullish_fvg": False,
            "smc_bearish_fvg": False,
            "long_entry": True,
            "short_entry": False,
            "long_entry_l2": False,
            "short_entry_l2": False,
            "long_entry_l3": True,
            "short_entry_l3": False,
            "long_level": 3,
            "short_level": 0,
            "long_exit": False,
            "short_exit": False,
            "long_stop": 95.0,
            "short_stop": 105.0,
            "atr": 2.0,
            "signal_ts_ms": signal_ts_ms,
        }

    def test_native_split_rejection_falls_back_to_sl_attach_and_managed_tp_orders(self) -> None:
        client = _FakeClient()
        cfg = self._cfg()
        decision = EntryDecision(
            side="LONG",
            level=3,
            entry=100.0,
            stop=95.0,
            risk=5.0,
            tp1=107.5,
            tp2=112.5,
        )

        with patch("okx_trader.runtime_execute_decision.resolve_entry_decision", return_value=decision), patch(
            "okx_trader.runtime_execute_decision.notify_trade_execution"
        ):
            state_one: dict = {}
            root_one: dict = {}
            execute_decision(
                client=client,
                cfg=cfg,
                inst_id="BTC-USDT-SWAP",
                sig=self._sig(1_700_000_000_000),
                pos=PositionState("flat", 0.0),
                state=state_one,
                root_state=root_one,
                profile_id="DEFAULT",
            )

            self.assertEqual([c["attach_count"] for c in client.place_calls], [3, 1, 0])
            self.assertFalse(getattr(client, "_native_split_tp_supported", True))
            self.assertTrue(state_one["trade"]["managed_tp1_enabled"])
            self.assertTrue(state_one["trade"]["managed_tp2_enabled"])
            self.assertEqual(state_one["trade"]["exchange_tp1_size"], 5.0)
            self.assertEqual(state_one["trade"]["exchange_tp2_size"], 5.0)
            self.assertEqual(str(state_one["trade"].get("managed_tp1_order_state", "")), "live")
            self.assertTrue(str(state_one["trade"].get("managed_tp1_cl_ord_id", "")))
            self.assertTrue(bool(client.place_calls[-1].get("reduce_only")))
            self.assertEqual(client.place_calls[-1].get("ord_type"), "limit")
            self.assertAlmostEqual(float(client.place_calls[-1].get("px", 0.0)), 107.5, places=6)

            client.place_calls.clear()
            state_two: dict = {}
            root_two: dict = {}
            execute_decision(
                client=client,
                cfg=cfg,
                inst_id="BTC-USDT-SWAP",
                sig=self._sig(1_700_000_900_000),
                pos=PositionState("flat", 0.0),
                state=state_two,
                root_state=root_two,
                profile_id="DEFAULT",
            )

            self.assertEqual([c["attach_count"] for c in client.place_calls], [1, 0])
            self.assertTrue(state_two["trade"]["managed_tp1_enabled"])
            self.assertTrue(state_two["trade"]["managed_tp2_enabled"])
            self.assertEqual(str(state_two["trade"].get("managed_tp1_order_state", "")), "live")


if __name__ == "__main__":
    unittest.main()
