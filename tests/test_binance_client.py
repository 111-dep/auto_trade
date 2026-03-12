from __future__ import annotations

import os
import unittest
import urllib.error
import urllib.parse
from unittest.mock import patch

from okx_trader.binance_um_client import BinanceUMClient
from okx_trader.client_factory import create_client
from okx_trader.config import read_config


class BinanceConfigAndClientTests(unittest.TestCase):
    class _FakeHTTPResponse:
        def __init__(self, body: bytes):
            self._body = body

        def read(self) -> bytes:
            return self._body

        def close(self) -> None:
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _build_cfg(self, extra_env: dict[str, str] | None = None):
        env = {
            "EXCHANGE_PROVIDER": "binance",
            "BINANCE_INST_IDS": "BTC-USDT-SWAP,ETH-USDT-SWAP",
            "BINANCE_API_KEY": "binance-key",
            "BINANCE_SECRET_KEY": "binance-secret",
            "BINANCE_BASE_URL": "https://fapi.binance.com",
            "BINANCE_QUOTE_ASSET": "USDC",
            "BINANCE_RECV_WINDOW": "7000",
            "OKX_SIZING_MODE": "margin",
            "OKX_MARGIN_USDT": "25",
            "OKX_LEVERAGE": "5",
        }
        if extra_env:
            env.update(extra_env)
        with patch.dict(os.environ, env, clear=True):
            return read_config(None)

    def test_read_config_uses_binance_provider_values(self) -> None:
        cfg = self._build_cfg()
        self.assertEqual(cfg.exchange_provider, "binance")
        self.assertEqual(cfg.base_url, "https://fapi.binance.com")
        self.assertEqual(cfg.api_key, "binance-key")
        self.assertEqual(cfg.secret_key, "binance-secret")
        self.assertEqual(cfg.passphrase, "")
        self.assertEqual(cfg.inst_ids, ["BTC-USDT-SWAP", "ETH-USDT-SWAP"])
        self.assertEqual(cfg.binance_quote_asset, "USDC")
        self.assertEqual(cfg.binance_recv_window, 7000)
        self.assertEqual(cfg.compound_balance_ccy, "USDC")

    def test_binance_requires_net_position_mode(self) -> None:
        with self.assertRaises(ValueError):
            self._build_cfg({"OKX_POS_MODE": "long_short"})

    def test_client_factory_selects_binance(self) -> None:
        cfg = self._build_cfg()
        client = create_client(cfg)
        self.assertIsInstance(client, BinanceUMClient)
        self.assertEqual(client.provider_name, "binance")
        self.assertFalse(client.supports_attached_tpsl_on_entry)

    def test_resolve_symbol_uses_default_usdc_mapping(self) -> None:
        cfg = self._build_cfg()
        client = BinanceUMClient(cfg)
        client._exchange_info_by_symbol = {
            "BTCUSDC": {
                "symbol": "BTCUSDC",
                "quoteAsset": "USDC",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                ],
            }
        }
        self.assertEqual(client._resolve_symbol("BTC-USDT-SWAP"), "BTCUSDC")

    def test_resolve_symbol_honors_explicit_symbol_map(self) -> None:
        cfg = self._build_cfg({"BINANCE_SYMBOL_MAP": "DOGE-USDT-SWAP:DOGEUSDC"})
        client = BinanceUMClient(cfg)
        client._exchange_info_by_symbol = {
            "DOGEUSDC": {
                "symbol": "DOGEUSDC",
                "quoteAsset": "USDC",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.00001"},
                ],
            }
        }
        self.assertEqual(client._resolve_symbol("DOGE-USDT-SWAP"), "DOGEUSDC")

    def test_normalize_order_size_uses_step_and_min_qty(self) -> None:
        cfg = self._build_cfg()
        client = BinanceUMClient(cfg)
        client._instrument_cache["BTC-USDT-SWAP"] = {
            "symbol": "BTCUSDC",
            "quoteAsset": "USDC",
            "lotSz": "0.001",
            "minSz": "0.001",
            "tickSz": "0.1",
        }
        qty, qty_txt = client.normalize_order_size("BTC-USDT-SWAP", 0.00149, reduce_only=False)
        self.assertAlmostEqual(qty, 0.001, places=9)
        self.assertEqual(qty_txt, "0.001")

    def test_calc_order_size_risk_mode_for_linear_usdc(self) -> None:
        cfg = self._build_cfg(
            {
                "STRAT_RISK_FRAC": "2%",
                "OKX_MARGIN_USDT": "0",
                "OKX_SIZING_MODE": "margin",
                "OKX_LEVERAGE": "5",
            }
        )
        client = BinanceUMClient(cfg)
        client._instrument_cache["BTC-USDT-SWAP"] = {
            "symbol": "BTCUSDC",
            "quoteAsset": "USDC",
            "lotSz": "0.001",
            "minSz": "0.001",
            "tickSz": "0.1",
        }
        client.get_account_equity = lambda force_refresh=False: 1000.0
        size = client.calc_order_size(
            cfg,
            "BTC-USDT-SWAP",
            entry_price=100.0,
            stop_price=90.0,
            entry_side="long",
        )
        self.assertAlmostEqual(size, 2.0, places=9)

    def _prime_btc_usdc(self, client: BinanceUMClient) -> None:
        client._exchange_info_by_symbol = {
            "BTCUSDC": {
                "symbol": "BTCUSDC",
                "quoteAsset": "USDC",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                ],
            }
        }
        client._instrument_cache["BTC-USDT-SWAP"] = {
            "symbol": "BTCUSDC",
            "quoteAsset": "USDC",
            "lotSz": "0.001",
            "minSz": "0.001",
            "tickSz": "0.1",
        }

    def test_private_api_mode_auto_falls_back_to_papi(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "auto"})
        client = BinanceUMClient(cfg)
        calls: list[tuple[str, str, str, bool]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, base_url, signed))
            if path == "/fapi/v3/balance":
                raise RuntimeError("Binance API error: code=-2015 msg=Invalid API-key, IP, or permissions for action")
            if path == "/papi/v1/um/account":
                return {"assets": [], "positions": []}
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            self.assertEqual(client._private_api_mode(), "papi")
            self.assertEqual(client._private_api_mode(), "papi")

        self.assertEqual([item[1] for item in calls], ["/fapi/v3/balance", "/papi/v1/um/account"])

    def test_get_positions_uses_papi_endpoint_when_pinned(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)
        calls: list[tuple[str, str, str]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, base_url))
            return []

        with patch.object(client, "_request", side_effect=fake_request):
            rows = client.get_positions("BTC-USDT-SWAP")

        self.assertEqual(rows, [])
        self.assertEqual(calls, [("GET", "/papi/v1/um/positionRisk", "https://papi.binance.com")])

    def test_get_account_equity_uses_papi_balances(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            if path == "/papi/v1/balance":
                return [{"asset": "USDC", "umWalletBalance": "123.45", "totalWalletBalance": "123.45"}]
            if path == "/papi/v1/um/account":
                return {"assets": [{"asset": "USDC", "crossWalletBalance": "120.00"}], "positions": []}
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            equity = client.get_account_equity(force_refresh=True)

        self.assertAlmostEqual(equity or 0.0, 123.45, places=9)

    def test_get_account_equity_papi_prefers_total_wallet_balance_for_unified_account(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            if path == "/papi/v1/balance":
                return [{
                    "asset": "USDC",
                    "totalWalletBalance": "509.88720342",
                    "crossMarginAsset": "509.86687824",
                    "crossMarginFree": "509.86687824",
                    "umWalletBalance": "0.02032518",
                }]
            if path == "/papi/v1/um/account":
                return {"assets": [{"asset": "USDC", "crossWalletBalance": "0.02032518"}], "positions": []}
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            equity = client.get_account_equity(force_refresh=True)

        self.assertAlmostEqual(equity or 0.0, 509.88720342, places=9)

    def test_place_stop_loss_order_papi_sends_quantity_and_reduce_only(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi", "OKX_DRY_RUN": "0"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)
        calls: list[tuple[str, str, dict[str, str] | None]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, dict(params or {})))
            if (method, path) == ("POST", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": 789,
                    "newClientStrategyId": "sl-2",
                    "strategyStatus": "NEW",
                    "side": "SELL",
                    "stopPrice": "95",
                    "origQty": "0.536",
                }
            raise AssertionError(f"unexpected request: {method} {path}")

        resp = None
        with patch.object(client, "_request", side_effect=fake_request):
            resp = client.place_stop_loss_order(
                inst_id="BTC-USDT-SWAP",
                side="long",
                stop_price=95.0,
                cl_ord_id="sl-2",
                size=0.536,
            )

        row = resp["data"][0]
        self.assertEqual(calls[-1][1], "/papi/v1/um/conditional/order")
        params = calls[-1][2] or {}
        self.assertEqual(params.get("strategyType"), "STOP_MARKET")
        self.assertEqual(params.get("quantity"), "0.536")
        self.assertEqual(params.get("reduceOnly"), "true")
        self.assertNotIn("closePosition", params)
        self.assertEqual(row["attachAlgoId"], "789")
        self.assertEqual(row["attachAlgoClOrdId"], "sl-2")
        self.assertEqual(row["slTriggerPx"], "95")

    def test_place_stop_loss_order_papi_dry_run_uses_strategy_ids(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi", "OKX_DRY_RUN": "1"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)

        resp = client.place_stop_loss_order(
            inst_id="BTC-USDT-SWAP",
            side="long",
            stop_price=95.0,
            cl_ord_id="sl-1",
            size=1.0,
        )
        row = resp["data"][0]
        self.assertEqual(row["attachAlgoId"], "DRY_RUN_STOP")
        self.assertEqual(row["attachAlgoClOrdId"], "sl-1")
        self.assertEqual(row["slTriggerPx"], "95")
        self.assertEqual(row["side"], "sell")

    def test_public_request_falls_back_to_backup_domain_on_transient_error(self) -> None:
        cfg = self._build_cfg(
            {
                "BINANCE_HTTP_MAX_RETRIES": "0",
                "BINANCE_PUBLIC_FALLBACK_URLS": "https://fapi1.binance.com,https://fapi2.binance.com",
            }
        )
        client = BinanceUMClient(cfg)
        client._http_max_retries = 0
        client._public_base_urls = [
            "https://fapi.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
        ]
        client._active_public_base_url = client._public_base_urls[0]
        urls: list[str] = []

        def fake_urlopen(req, timeout=15):
            urls.append(req.full_url)
            if req.full_url.startswith("https://fapi.binance.com/"):
                raise urllib.error.URLError("[Errno 104] Connection reset by peer")
            return self._FakeHTTPResponse(b'{"symbols": []}')

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            payload = client._request("GET", "/fapi/v1/exchangeInfo")

        self.assertEqual(payload, {"symbols": []})
        self.assertEqual(
            urls,
            [
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "https://fapi1.binance.com/fapi/v1/exchangeInfo",
            ],
        )
        self.assertEqual(client._active_public_base_url, "https://fapi1.binance.com")

    def test_signed_request_does_not_use_public_fallback_domains(self) -> None:
        cfg = self._build_cfg(
            {
                "BINANCE_HTTP_MAX_RETRIES": "0",
                "BINANCE_PUBLIC_FALLBACK_URLS": "https://fapi1.binance.com",
            }
        )
        client = BinanceUMClient(cfg)
        client._http_max_retries = 0
        client._public_base_urls = [
            "https://fapi.binance.com",
            "https://fapi1.binance.com",
        ]
        client._active_public_base_url = client._public_base_urls[0]
        urls: list[str] = []

        def fake_urlopen(req, timeout=15):
            urls.append(req.full_url)
            raise urllib.error.URLError("[Errno 104] Connection reset by peer")

        with patch.object(client, "_sync_server_time", return_value=0):
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                with self.assertRaises(RuntimeError):
                    client._request("GET", "/fapi/v3/balance", signed=True, base_url="https://fapi.binance.com")

        self.assertEqual(len(urls), 1)
        self.assertTrue(urls[0].startswith("https://fapi.binance.com/fapi/v3/balance?"))

    def test_exchange_info_uses_static_fallback_when_public_api_unavailable(self) -> None:
        cfg = self._build_cfg()
        client = BinanceUMClient(cfg)

        with patch.object(client, "_request", side_effect=RuntimeError("waf challenge")):
            info = client.get_instrument("FIL-USDT-SWAP")

        self.assertEqual(info["symbol"], "FILUSDC")
        self.assertEqual(info["lotSz"], "0.1")
        self.assertEqual(info["minSz"], "0.1")
        self.assertEqual(info["tickSz"], "0.0001")

    def test_public_request_invalid_json_falls_back_to_next_domain(self) -> None:
        cfg = self._build_cfg(
            {
                "BINANCE_HTTP_MAX_RETRIES": "0",
                "BINANCE_PUBLIC_FALLBACK_URLS": "https://fapi1.binance.com,https://fapi2.binance.com",
            }
        )
        client = BinanceUMClient(cfg)
        client._http_max_retries = 0
        client._public_base_urls = [
            "https://fapi.binance.com",
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
        ]
        client._active_public_base_url = "https://fapi1.binance.com"
        urls: list[str] = []

        def fake_urlopen(req, timeout=15):
            urls.append(req.full_url)
            if req.full_url.startswith("https://fapi1.binance.com/"):
                return self._FakeHTTPResponse(b"")
            if req.full_url.startswith("https://fapi.binance.com/"):
                return self._FakeHTTPResponse(b"")
            if req.full_url.startswith("https://fapi2.binance.com/"):
                return self._FakeHTTPResponse(b'{"symbols": []}')
            raise AssertionError(f"unexpected url: {req.full_url}")

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            payload = client._request("GET", "/fapi/v1/exchangeInfo")

        self.assertEqual(payload, {"symbols": []})
        self.assertEqual(
            urls,
            [
                "https://fapi1.binance.com/fapi/v1/exchangeInfo",
                "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "https://fapi2.binance.com/fapi/v1/exchangeInfo",
            ],
        )
        self.assertEqual(client._active_public_base_url, "https://fapi2.binance.com")

    def test_get_candles_uses_recent_cache_when_public_fetch_fails(self) -> None:
        cfg = self._build_cfg()
        client = BinanceUMClient(cfg)
        client._exchange_info_by_symbol = {
            "BTCUSDC": {
                "symbol": "BTCUSDC",
                "quoteAsset": "USDC",
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001"},
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                ],
            }
        }
        rows = [
            [1700000000000, "100", "110", "90", "105", "123", 1700000899999],
            [1700000900000, "105", "112", "101", "111", "88", 1700001799999],
        ]

        with patch.object(client, "_request", side_effect=[rows, RuntimeError("waf challenge")]):
            first = client.get_candles("BTC-USDT-SWAP", "15m", 2)
            second = client.get_candles("BTC-USDT-SWAP", "15m", 2)

        self.assertEqual(len(first), 2)
        self.assertEqual(len(second), 2)
        self.assertEqual([c.ts_ms for c in second], [1700000000000, 1700000900000])
        self.assertEqual([c.close for c in second], [105.0, 111.0])

    def test_conditional_order_rate_limit_arms_request_cooldown(self) -> None:
        cfg = self._build_cfg({"BINANCE_HTTP_MAX_RETRIES": "2", "BINANCE_CONDITIONAL_ORDER_COOLDOWN_SECONDS": "15"})
        client = BinanceUMClient(cfg)
        calls: list[str] = []

        def fake_urlopen(req, timeout=15):
            calls.append(req.full_url)
            raise urllib.error.HTTPError(
                req.full_url,
                429,
                "Too Many Requests",
                hdrs=None,
                fp=self._FakeHTTPResponse(b'{"code":-1015,"msg":"Too many new orders."}'),
            )

        with patch.object(client, "_sync_server_time", return_value=0):
            with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                with self.assertRaises(RuntimeError) as first_err:
                    client._request(
                        "POST",
                        "/papi/v1/um/conditional/order",
                        params={"symbol": "BTCUSDC", "strategyType": "STOP_MARKET"},
                        signed=True,
                        base_url="https://papi.binance.com",
                    )
                with self.assertRaises(RuntimeError) as second_err:
                    client._request(
                        "POST",
                        "/papi/v1/um/conditional/order",
                        params={"symbol": "BTCUSDC", "strategyType": "STOP_MARKET"},
                        signed=True,
                        base_url="https://papi.binance.com",
                    )

        self.assertIn("cooldown", str(first_err.exception).lower())
        self.assertIn("cooldown active", str(second_err.exception).lower())
        self.assertEqual(len(calls), 1)

    def test_signed_request_resyncs_timestamp_after_1021(self) -> None:
        cfg = self._build_cfg({"BINANCE_HTTP_MAX_RETRIES": "1"})
        client = BinanceUMClient(cfg)
        client._http_max_retries = 1
        urls: list[str] = []

        def fake_sync(*, force: bool = False):
            if force:
                client._server_time_offset_ms = 8000
                client._server_time_offset_synced_at = 1000.0
            return int(client._server_time_offset_ms)

        def fake_urlopen(req, timeout=15):
            urls.append(req.full_url)
            if len(urls) == 1:
                raise urllib.error.HTTPError(
                    req.full_url,
                    400,
                    "Bad Request",
                    hdrs=None,
                    fp=self._FakeHTTPResponse(
                        b'{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'
                    ),
                )
            return self._FakeHTTPResponse(b"[]")

        with patch.object(client, "_sync_server_time", side_effect=fake_sync):
            with patch("time.time", return_value=1000.0):
                with patch("urllib.request.urlopen", side_effect=fake_urlopen):
                    payload = client._request("GET", "/fapi/v3/balance", signed=True, base_url="https://fapi.binance.com")

        self.assertEqual(payload, [])
        self.assertEqual(len(urls), 2)
        first_qs = urllib.parse.parse_qs(urllib.parse.urlparse(urls[0]).query)
        second_qs = urllib.parse.parse_qs(urllib.parse.urlparse(urls[1]).query)
        self.assertEqual(first_qs.get("recvWindow"), ["7000"])
        self.assertEqual(second_qs.get("recvWindow"), ["7000"])
        self.assertEqual(int(second_qs["timestamp"][0]) - int(first_qs["timestamp"][0]), 8000)

    def test_amend_algo_sl_papi_uses_conditional_order_endpoints(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi", "OKX_DRY_RUN": "0"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)
        calls: list[tuple[str, str, dict[str, str] | None]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, dict(params or {})))
            if (method, path) == ("GET", "/papi/v1/um/conditional/openOrder"):
                return {
                    "strategyId": 123,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "NEW",
                    "side": "SELL",
                    "stopPrice": "90",
                    "origQty": "0.5",
                }
            if (method, path) == ("DELETE", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": 123,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "CANCELED",
                    "side": "SELL",
                    "stopPrice": "90",
                }
            if (method, path) == ("POST", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": 456,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "NEW",
                    "side": "SELL",
                    "stopPrice": "95",
                }
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            resp = client.amend_algo_sl(
                inst_id="BTC-USDT-SWAP",
                new_sl_trigger_px=95.0,
                algo_id="123",
                algo_cl_ord_id="sl-1",
            )

        row = resp["data"][0]
        self.assertEqual([item[1] for item in calls], [
            "/papi/v1/um/conditional/openOrder",
            "/papi/v1/um/conditional/order",
            "/papi/v1/um/conditional/order",
        ])
        post_params = calls[-1][2] or {}
        self.assertEqual(post_params.get("strategyType"), "STOP_MARKET")
        self.assertEqual(post_params.get("quantity"), "0.5")
        self.assertEqual(post_params.get("reduceOnly"), "true")
        self.assertNotIn("type", post_params)
        self.assertNotIn("closePosition", post_params)
        self.assertEqual(row["attachAlgoId"], "456")
        self.assertEqual(row["attachAlgoClOrdId"], "sl-1")
        self.assertEqual(row["slTriggerPx"], "95")

    def test_get_pending_stop_loss_order_prefers_qty_match_then_latest(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            if (method, path) == ("GET", "/papi/v1/um/conditional/openOrders"):
                return [
                    {
                        "strategyId": 101,
                        "newClientStrategyId": "sl-old",
                        "strategyStatus": "NEW",
                        "strategyType": "STOP_MARKET",
                        "side": "SELL",
                        "stopPrice": "90",
                        "origQty": "0.7",
                        "reduceOnly": True,
                    },
                    {
                        "strategyId": 202,
                        "newClientStrategyId": "sl-good-old",
                        "strategyStatus": "NEW",
                        "strategyType": "STOP_MARKET",
                        "side": "SELL",
                        "stopPrice": "95",
                        "origQty": "0.5",
                        "reduceOnly": True,
                    },
                    {
                        "strategyId": 303,
                        "newClientStrategyId": "sl-good-new",
                        "strategyStatus": "NEW",
                        "strategyType": "STOP_MARKET",
                        "side": "SELL",
                        "stopPrice": "95",
                        "origQty": "0.5",
                        "reduceOnly": True,
                    },
                ]
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            row = client.get_pending_stop_loss_order(
                inst_id="BTC-USDT-SWAP",
                side="long",
                size=0.5,
                stop_price=95.0,
            )

        self.assertEqual(str(row.get("attachAlgoId", "")), "303")
        self.assertEqual(str(row.get("attachAlgoClOrdId", "")), "sl-good-new")
        self.assertTrue(bool(row.get("_qty_match", False)))
        self.assertTrue(bool(row.get("_stop_match", False)))
        self.assertEqual(int(row.get("_extra_count", 0) or 0), 2)

    def test_amend_algo_sl_papi_uses_override_size_when_provided(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi", "OKX_DRY_RUN": "0"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)
        calls: list[tuple[str, str, dict[str, str] | None]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, dict(params or {})))
            if (method, path) == ("GET", "/papi/v1/um/conditional/openOrder"):
                return {
                    "strategyId": 123,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "NEW",
                    "strategyType": "STOP_MARKET",
                    "side": "SELL",
                    "stopPrice": "90",
                    "origQty": "0.9",
                    "reduceOnly": True,
                }
            if (method, path) == ("DELETE", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": 123,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "CANCELED",
                    "side": "SELL",
                    "stopPrice": "90",
                }
            if (method, path) == ("POST", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": 456,
                    "newClientStrategyId": "sl-1",
                    "strategyStatus": "NEW",
                    "side": "SELL",
                    "stopPrice": "95",
                    "origQty": "0.5",
                }
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            client.amend_algo_sl(
                inst_id="BTC-USDT-SWAP",
                new_sl_trigger_px=95.0,
                algo_id="123",
                algo_cl_ord_id="sl-1",
                size=0.5,
            )

        post_params = calls[-1][2] or {}
        self.assertEqual(post_params.get("quantity"), "0.5")

    def test_cleanup_pending_stop_loss_orders_keeps_best_and_cancels_one_extra(self) -> None:
        cfg = self._build_cfg({"BINANCE_PRIVATE_API_MODE": "papi", "OKX_DRY_RUN": "0"})
        client = BinanceUMClient(cfg)
        client._binance_private_api_mode = "papi"
        self._prime_btc_usdc(client)
        calls: list[tuple[str, str, dict[str, str] | None]] = []

        def fake_request(method: str, path: str, params=None, *, signed: bool = False, base_url: str = ""):
            calls.append((method, path, dict(params or {})))
            if (method, path) == ("GET", "/papi/v1/um/conditional/openOrders"):
                return [
                    {
                        "strategyId": 101,
                        "newClientStrategyId": "sl-old",
                        "strategyStatus": "NEW",
                        "strategyType": "STOP_MARKET",
                        "side": "SELL",
                        "stopPrice": "95",
                        "origQty": "0.5",
                        "reduceOnly": True,
                    },
                    {
                        "strategyId": 303,
                        "newClientStrategyId": "sl-keep",
                        "strategyStatus": "NEW",
                        "strategyType": "STOP_MARKET",
                        "side": "SELL",
                        "stopPrice": "95",
                        "origQty": "0.5",
                        "reduceOnly": True,
                    },
                ]
            if (method, path) == ("DELETE", "/papi/v1/um/conditional/order"):
                return {
                    "strategyId": params.get("strategyId", "101"),
                    "newClientStrategyId": params.get("newClientStrategyId", "sl-old"),
                    "strategyStatus": "CANCELED",
                    "side": "SELL",
                    "stopPrice": "95",
                }
            raise AssertionError(f"unexpected request: {method} {path}")

        with patch.object(client, "_request", side_effect=fake_request):
            canceled = client.cleanup_pending_stop_loss_orders(
                inst_id="BTC-USDT-SWAP",
                side="long",
                keep_algo_id="303",
                keep_algo_cl_ord_id="sl-keep",
                max_cancel=1,
            )

        self.assertEqual(canceled, 1)
        delete_calls = [item for item in calls if item[1] == "/papi/v1/um/conditional/order"]
        self.assertEqual(len(delete_calls), 1)
        self.assertEqual((delete_calls[0][2] or {}).get("strategyId"), "101")


if __name__ == "__main__":
    unittest.main()
