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


if __name__ == "__main__":
    unittest.main()
