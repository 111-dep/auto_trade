from __future__ import annotations

import io
import json
import os
import unittest
import urllib.error
from types import SimpleNamespace
from unittest.mock import patch

from okx_trader.okx_client import OKXClient


class _Resp:
    def __init__(self, payload: dict):
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._raw

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _cfg() -> SimpleNamespace:
    return SimpleNamespace(
        base_url="https://www.okx.com",
        api_key="",
        secret_key="",
        passphrase="",
        paper=False,
        dry_run=False,
        user_agent="ua-test",
        td_mode="isolated",
        pos_mode="net",
        leverage=10.0,
        history_cache_enabled=False,
        history_cache_dir="/tmp",
        history_cache_ttl_seconds=0,
        compound_cache_seconds=0,
        compound_balance_ccy="USDT",
        margin_usdt=10.0,
        compound_enabled=False,
        attach_tpsl_trigger_px_type="mark",
    )


class _RecoverClient(OKXClient):
    def __init__(self, cfg: SimpleNamespace, request_error: Exception):
        super().__init__(cfg)
        self._request_error = request_error
        self.place_calls = 0

    def normalize_order_size(self, inst_id: str, sz: float, reduce_only: bool = False):
        return float(sz), str(float(sz))

    def _request(self, method, path, params=None, body=None, private=False):  # type: ignore[override]
        self.place_calls += 1
        raise self._request_error

    def get_order(self, inst_id: str, ord_id: str = "", cl_ord_id: str = ""):
        return {"ordId": "123456", "instId": inst_id, "clOrdId": cl_ord_id}


class OKXClientResilienceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "OKX_HTTP_MAX_RETRIES": os.getenv("OKX_HTTP_MAX_RETRIES"),
            "OKX_HTTP_RETRY_BASE_SECONDS": os.getenv("OKX_HTTP_RETRY_BASE_SECONDS"),
            "OKX_HTTP_RETRY_MAX_SECONDS": os.getenv("OKX_HTTP_RETRY_MAX_SECONDS"),
        }
        os.environ["OKX_HTTP_MAX_RETRIES"] = "2"
        os.environ["OKX_HTTP_RETRY_BASE_SECONDS"] = "0.001"
        os.environ["OKX_HTTP_RETRY_MAX_SECONDS"] = "0.005"

    def tearDown(self) -> None:
        for k, v in self._env_backup.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_request_retries_on_http_transient_code(self) -> None:
        client = OKXClient(_cfg())
        http_err = urllib.error.HTTPError(
            url="https://www.okx.com/api/v5/public/time",
            code=503,
            msg="Service Unavailable",
            hdrs={},
            fp=io.BytesIO(b'{"code":"50011","msg":"busy"}'),
        )
        with patch("urllib.request.urlopen", side_effect=[http_err, _Resp({"code": "0", "data": []})]) as mocked:
            with patch("time.sleep", return_value=None):
                data = client._request("GET", "/api/v5/public/time", private=False)
        self.assertEqual(data.get("code"), "0")
        self.assertEqual(mocked.call_count, 2)

    def test_request_retries_on_okx_retryable_code(self) -> None:
        client = OKXClient(_cfg())
        with patch(
            "urllib.request.urlopen",
            side_effect=[
                _Resp({"code": "50011", "msg": "too many requests", "data": []}),
                _Resp({"code": "0", "data": [{"ok": 1}]}),
            ],
        ) as mocked:
            with patch("time.sleep", return_value=None):
                data = client._request("GET", "/api/v5/public/time", private=False)
        self.assertEqual(data.get("code"), "0")
        self.assertEqual(mocked.call_count, 2)

    def test_place_order_recovers_by_cl_ord_id_for_duplicate(self) -> None:
        client = _RecoverClient(
            _cfg(),
            RuntimeError("OKX API error: code=51603 msg=clOrdId already exists data=[]"),
        )
        data = client.place_order(
            inst_id="BTC-USDT-SWAP",
            side="buy",
            sz=1.0,
            cl_ord_id="T-CLID-1",
        )
        self.assertEqual(client.place_calls, 1)
        self.assertEqual(data["data"][0].get("ordId"), "123456")
        self.assertEqual(data["data"][0].get("clOrdId"), "T-CLID-1")

    def test_place_order_recovers_by_cl_ord_id_for_transient_error(self) -> None:
        client = _RecoverClient(
            _cfg(),
            RuntimeError("HTTP request failed: POST /api/v5/trade/order | timed out"),
        )
        data = client.place_order(
            inst_id="ETH-USDT-SWAP",
            side="sell",
            sz=2.0,
            cl_ord_id="T-CLID-2",
        )
        self.assertEqual(client.place_calls, 1)
        self.assertEqual(data["data"][0].get("ordId"), "123456")
        self.assertEqual(data["data"][0].get("clOrdId"), "T-CLID-2")

    def test_amend_algo_sl_requires_algo_identifier(self) -> None:
        client = OKXClient(_cfg())
        with self.assertRaises(RuntimeError):
            client.amend_algo_sl(
                inst_id="BTC-USDT-SWAP",
                new_sl_trigger_px=100.0,
            )

    def test_amend_algo_sl_posts_expected_payload(self) -> None:
        client = OKXClient(_cfg())
        with patch.object(client, "_request", return_value={"code": "0", "data": []}) as mocked:
            client.amend_algo_sl(
                inst_id="SUI-USDT-SWAP",
                new_sl_trigger_px=0.939,
                algo_id="3330260971104681984",
            )
        self.assertEqual(mocked.call_count, 1)
        args, kwargs = mocked.call_args
        self.assertEqual(args[0], "POST")
        self.assertEqual(args[1], "/api/v5/trade/amend-algos")
        self.assertTrue(kwargs.get("private"))
        body = kwargs.get("body") or {}
        self.assertEqual(body.get("instId"), "SUI-USDT-SWAP")
        self.assertEqual(body.get("algoId"), "3330260971104681984")
        self.assertEqual(body.get("newSlOrdPx"), "-1")
        self.assertEqual(body.get("newSlTriggerPxType"), "mark")

    def test_build_attach_tpsl_ords_keeps_attach_algo_cl_id(self) -> None:
        client = OKXClient(_cfg())
        ords = client.build_attach_tpsl_ords(
            tp_price=1.23,
            sl_price=1.11,
            attach_algo_cl_ord_id="ALG-CL-001",
        )
        self.assertEqual(len(ords), 1)
        row = ords[0]
        self.assertEqual(row.get("attachAlgoClOrdId"), "ALG-CL-001")
        self.assertEqual(row.get("tpOrdPx"), "-1")
        self.assertEqual(row.get("slOrdPx"), "-1")

    def test_build_sl_attach_algo_ords_keeps_attach_algo_cl_id(self) -> None:
        client = OKXClient(_cfg())
        ords = client.build_sl_attach_algo_ords(
            sl_price=1.11,
            attach_algo_cl_ord_id="ALG-SL-ONLY-001",
        )
        self.assertEqual(len(ords), 1)
        row = ords[0]
        self.assertEqual(row.get("attachAlgoClOrdId"), "ALG-SL-ONLY-001")
        self.assertEqual(row.get("slOrdPx"), "-1")
        self.assertEqual(row.get("slTriggerPxType"), "mark")

    def test_build_split_tp_attach_algo_ords_builds_two_tp_and_one_sl(self) -> None:
        client = OKXClient(_cfg())
        ords = client.build_split_tp_attach_algo_ords(
            tp1_price=1.23,
            tp1_size="3",
            tp2_price=1.45,
            tp2_size="7",
            sl_price=1.11,
            tp1_attach_algo_cl_ord_id="ALG-TP1-001",
            tp2_attach_algo_cl_ord_id="ALG-TP2-001",
            sl_attach_algo_cl_ord_id="ALG-SL-001",
            move_sl_to_avg_px_on_tp1=True,
        )
        self.assertEqual(len(ords), 3)
        tp1, tp2, sl = ords
        self.assertEqual(tp1.get("attachAlgoClOrdId"), "ALG-TP1-001")
        self.assertEqual(tp1.get("tpOrdPx"), "-1")
        self.assertEqual(tp1.get("sz"), "3")
        self.assertEqual(tp2.get("attachAlgoClOrdId"), "ALG-TP2-001")
        self.assertEqual(tp2.get("tpOrdPx"), "-1")
        self.assertEqual(tp2.get("sz"), "7")
        self.assertEqual(sl.get("attachAlgoClOrdId"), "ALG-SL-001")
        self.assertEqual(sl.get("slOrdPx"), "-1")
        self.assertEqual(sl.get("amendPxOnTriggerType"), "1")

    def test_build_partial_tp_attach_algo_ords_builds_one_tp_and_one_sl(self) -> None:
        client = OKXClient(_cfg())
        ords = client.build_partial_tp_attach_algo_ords(
            tp_price=1.23,
            tp_size="3",
            sl_price=1.11,
            tp_attach_algo_cl_ord_id="ALG-TP1-ONLY",
            sl_attach_algo_cl_ord_id="ALG-SL-ONLY",
        )
        self.assertEqual(len(ords), 2)
        tp1, sl = ords
        self.assertEqual(tp1.get("attachAlgoClOrdId"), "ALG-TP1-ONLY")
        self.assertEqual(tp1.get("tpOrdPx"), "-1")
        self.assertEqual(tp1.get("sz"), "3")
        self.assertEqual(sl.get("attachAlgoClOrdId"), "ALG-SL-ONLY")
        self.assertEqual(sl.get("slOrdPx"), "-1")

    def test_amend_order_attached_sl_prefers_sl_attach_algo(self) -> None:
        client = OKXClient(_cfg())
        order_row = {
            "ordId": "111",
            "clOrdId": "ENTRY-CL-1",
            "attachAlgoOrds": [
                {"attachAlgoClOrdId": "ALG-TP1", "tpTriggerPx": "101", "tpOrdPx": "-1"},
                {"attachAlgoClOrdId": "ALG-TP2", "tpTriggerPx": "102", "tpOrdPx": "-1"},
                {"attachAlgoClOrdId": "ALG-SL", "slTriggerPx": "95", "slOrdPx": "-1"},
            ],
        }
        with patch.object(client, "get_order", return_value=order_row):
            with patch.object(client, "_request", return_value={"code": "0", "data": []}) as mocked:
                client.amend_order_attached_sl(
                    inst_id="BTC-USDT-SWAP",
                    ord_id="111",
                    cl_ord_id="ENTRY-CL-1",
                    new_sl_trigger_px=96.0,
                )
        self.assertEqual(mocked.call_count, 1)
        _, kwargs = mocked.call_args
        body = kwargs.get("body") or {}
        attach_rows = body.get("attachAlgoOrds") or []
        self.assertEqual(len(attach_rows), 1)
        self.assertEqual(attach_rows[0].get("attachAlgoClOrdId"), "ALG-SL")


if __name__ == "__main__":
    unittest.main()
