from __future__ import annotations
import hashlib
import hmac
import json
import math
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
from .common import log, parse_inst_parts, round_size
from .models import Candle, Config, PositionState


_STATIC_BINANCE_USDC_SPECS: Dict[str, Dict[str, str]] = {
    "BTCUSDC": {"quoteAsset": "USDC", "lotSz": "0.001", "minSz": "0.001", "tickSz": "0.1"},
    "ETHUSDC": {"quoteAsset": "USDC", "lotSz": "0.001", "minSz": "0.001", "tickSz": "0.01"},
    "SOLUSDC": {"quoteAsset": "USDC", "lotSz": "0.01", "minSz": "0.01", "tickSz": "0.01"},
    "DOGEUSDC": {"quoteAsset": "USDC", "lotSz": "1", "minSz": "1", "tickSz": "0.00001"},
    "SUIUSDC": {"quoteAsset": "USDC", "lotSz": "1", "minSz": "1", "tickSz": "0.0001"},
    "BCHUSDC": {"quoteAsset": "USDC", "lotSz": "0.001", "minSz": "0.001", "tickSz": "0.01"},
    "LTCUSDC": {"quoteAsset": "USDC", "lotSz": "0.01", "minSz": "0.01", "tickSz": "0.01"},
    "NEARUSDC": {"quoteAsset": "USDC", "lotSz": "1", "minSz": "1", "tickSz": "0.0001"},
    "FILUSDC": {"quoteAsset": "USDC", "lotSz": "0.1", "minSz": "0.1", "tickSz": "0.0001"},
    "UNIUSDC": {"quoteAsset": "USDC", "lotSz": "0.1", "minSz": "0.1", "tickSz": "0.001"},
}
class BinanceUMClient:
    provider_name = "binance"
    supports_attached_tpsl_on_entry = False
    supports_private_ws_tp1_be = False
    _client_managed_tpsl_on_open = True
    _client_managed_sl_on_open = True
    _native_split_tp_supported = False
    def __init__(self, config: Config):
        self.cfg = config
        self._instrument_cache: Dict[str, Dict[str, Any]] = {}
        self._symbol_alias_cache: Dict[str, str] = {}
        self._exchange_info_by_symbol: Optional[Dict[str, Dict[str, Any]]] = None
        self._leverage_ready: Dict[str, bool] = {}
        self._equity_cache_ts: float = 0.0
        self._equity_cache_value: Optional[float] = None
        self._equity_peak: Optional[float] = None
        self._public_base_url = str(getattr(self.cfg, "base_url", "") or "https://fapi.binance.com").rstrip("/")
        self._public_base_urls = self._build_public_base_urls(self._public_base_url)
        self._active_public_base_url = self._public_base_urls[0] if self._public_base_urls else self._public_base_url
        self._fapi_private_base_url = str(os.getenv("BINANCE_FAPI_BASE_URL", self._public_base_url)).rstrip("/")
        self._papi_private_base_url = str(os.getenv("BINANCE_PAPI_BASE_URL", "https://papi.binance.com")).rstrip("/")
        private_mode = str(os.getenv("BINANCE_PRIVATE_API_MODE", "auto") or "auto").strip().lower()
        self._binance_private_api_mode = private_mode if private_mode in {"auto", "fapi", "papi"} else "auto"
        self._detected_private_api_mode: Optional[str] = None
        self._http_max_retries = max(0, int(os.getenv("BINANCE_HTTP_MAX_RETRIES", "2")))
        self._http_retry_base_seconds = max(0.05, float(os.getenv("BINANCE_HTTP_RETRY_BASE_SECONDS", "0.35")))
        self._http_retry_max_seconds = max(
            self._http_retry_base_seconds,
            float(os.getenv("BINANCE_HTTP_RETRY_MAX_SECONDS", "3.0")),
        )
        self._public_http_timeout_seconds = max(
            2.0,
            float(os.getenv("BINANCE_PUBLIC_HTTP_TIMEOUT_SECONDS", "6.0")),
        )
        self._private_http_timeout_seconds = max(
            3.0,
            float(os.getenv("BINANCE_PRIVATE_HTTP_TIMEOUT_SECONDS", "15.0")),
        )
        self._server_time_offset_ms = 0
        self._server_time_offset_synced_at = 0.0
        self._server_time_offset_ttl_seconds = max(
            0.0,
            float(os.getenv("BINANCE_TIME_OFFSET_TTL_SECONDS", "30")),
        )
        self._recent_candle_cache: Dict[Tuple[str, str, bool], Tuple[float, List[Candle]]] = {}
        self._recent_candle_cache_ttl_seconds = max(
            30.0,
            float(os.getenv("BINANCE_RECENT_CANDLE_CACHE_TTL_SECONDS", "1800")),
        )
        self._signed_endpoint_cooldown_until: Dict[str, float] = {}
        self._conditional_order_cooldown_seconds = max(
            1.0,
            float(os.getenv("BINANCE_CONDITIONAL_ORDER_COOLDOWN_SECONDS", "15")),
        )
    @staticmethod
    def _safe_cache_key(value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "_"
        out = []
        for ch in raw:
            if ch.isalnum() or ch in {"_", "-", "."}:
                out.append(ch)
            else:
                out.append("_")
        return "".join(out)
    def _history_cache_path(self, inst_id: str, bar: str) -> str:
        inst_key = self._safe_cache_key(inst_id.upper())
        bar_key = self._safe_cache_key(bar)
        filename = f"{inst_key}__{bar_key}.json"
        return os.path.join(self.cfg.history_cache_dir, filename)
    def _load_history_cache(self, inst_id: str, bar: str, need: int) -> Optional[List[Candle]]:
        if not self.cfg.history_cache_enabled:
            return None
        path = self._history_cache_path(inst_id, bar)
        try:
            st = os.stat(path)
        except Exception:
            return None
        ttl = int(max(0, self.cfg.history_cache_ttl_seconds))
        age = time.time() - float(st.st_mtime)
        if ttl > 0 and age > ttl:
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return None
        rows = payload.get("candles") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return None
        out: List[Candle] = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 6:
                continue
            try:
                vol = 0.0
                conf = False
                if len(row) >= 7:
                    vol = float(row[5])
                    conf = bool(row[6]) if isinstance(row[6], bool) else (str(row[6]) == "1")
                else:
                    conf = bool(row[5]) if isinstance(row[5], bool) else (str(row[5]) == "1")
                out.append(
                    Candle(
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        confirm=conf,
                        volume=vol,
                    )
                )
            except Exception:
                continue
        out.sort(key=lambda c: c.ts_ms)
        out = [c for c in out if c.confirm]
        if len(out) >= int(max(1, need)):
            log(
                f"[{inst_id}] history cache hit | bar={bar} candles={len(out)} "
                f"age={int(max(0.0, age))}s"
            )
        return out or None
    def _save_history_cache(self, inst_id: str, bar: str, candles: List[Candle]) -> None:
        if not self.cfg.history_cache_enabled:
            return
        if not candles:
            return
        path = self._history_cache_path(inst_id, bar)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {
            "inst_id": inst_id.upper(),
            "bar": str(bar),
            "saved_ts": int(time.time()),
            "candles": [[c.ts_ms, c.open, c.high, c.low, c.close, c.volume, bool(c.confirm)] for c in candles],
        }
        tmp = f"{path}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, path)
        except Exception as e:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            log(f"[{inst_id}] history cache save skipped: {e}", level="WARN")
    @staticmethod
    def _is_transient_http_code(code: int) -> bool:
        return int(code) in {408, 409, 418, 425, 429, 500, 502, 503, 504}
    @staticmethod
    def _is_transient_error_text(text: str) -> bool:
        low = str(text or "").lower()
        tokens = (
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "connection refused",
            "broken pipe",
            "network is unreachable",
            "try again",
            "temporary failure",
            "service unavailable",
            "too many requests",
        )
        return any(tok in low for tok in tokens)
    @staticmethod
    def _is_transient_exception(exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        if isinstance(exc, socket.timeout):
            return True
        if isinstance(exc, urllib.error.URLError):
            return BinanceUMClient._is_transient_error_text(str(exc.reason))
        return BinanceUMClient._is_transient_error_text(str(exc))
    def _retry_backoff_seconds(self, attempt_idx: int) -> float:
        sec = self._http_retry_base_seconds * (2 ** max(0, int(attempt_idx)))
        return min(self._http_retry_max_seconds, max(0.0, sec))
    @staticmethod
    def _is_duplicate_cl_ord_id_error(exc: Exception) -> bool:
        low = str(exc or "").lower()
        if ("duplicate" in low and "order" in low) or ("client order id" in low and "used" in low):
            return True
        return "-2010" in low
    @staticmethod
    def _append_unique_base_url(items: List[str], value: str, seen: set[str]) -> None:
        txt = str(value or "").strip().rstrip("/")
        if (not txt) or txt in seen:
            return
        seen.add(txt)
        items.append(txt)
    def _build_public_base_urls(self, primary: str) -> List[str]:
        seen: set[str] = set()
        out: List[str] = []
        self._append_unique_base_url(out, primary, seen)
        raw_env = str(os.getenv("BINANCE_PUBLIC_FALLBACK_URLS", "") or "").strip()
        if raw_env:
            for part in raw_env.split(","):
                self._append_unique_base_url(out, part, seen)
            return out
        for default_url in (
            "https://fapi1.binance.com",
            "https://fapi2.binance.com",
            "https://fapi3.binance.com",
        ):
            self._append_unique_base_url(out, default_url, seen)
        return out
    def _request_base_url_candidates(self, base_url: str, *, signed: bool) -> List[str]:
        current = str(base_url or self._public_base_url or self.cfg.base_url).strip().rstrip("/")
        if signed:
            return [current]
        if current not in self._public_base_urls:
            return [current]
        active = str(self._active_public_base_url or current).strip().rstrip("/")
        seen: set[str] = set()
        out: List[str] = []
        self._append_unique_base_url(out, active, seen)
        self._append_unique_base_url(out, current, seen)
        for item in self._public_base_urls:
            self._append_unique_base_url(out, item, seen)
        return out
    @staticmethod
    def _is_timestamp_skew_error(code: Any, msg: Any) -> bool:
        txt = f"{code} {msg}".lower()
        return ("-1021" in txt) or ("outside of the recvwindow" in txt) or ("ahead of the server" in txt)
    def _signed_recv_window_ms(self) -> int:
        return int(max(1000, getattr(self.cfg, "binance_recv_window", 5000) or 5000))
    def _signed_timestamp_ms(self) -> int:
        return int(time.time() * 1000) + int(self._server_time_offset_ms)
    def _sync_server_time(self, *, force: bool = False) -> int:
        now = time.time()
        if (not force) and self._server_time_offset_synced_at > 0:
            if now - float(self._server_time_offset_synced_at) <= float(self._server_time_offset_ttl_seconds):
                return int(self._server_time_offset_ms)
        payload = self._request("GET", "/fapi/v1/time", base_url=self._public_base_url)
        server_ms = 0
        if isinstance(payload, dict):
            try:
                server_ms = int(payload.get("serverTime", 0) or 0)
            except Exception:
                server_ms = 0
        if server_ms <= 0:
            raise RuntimeError(f"Binance time sync failed: unexpected payload={payload}")
        local_ms = int(time.time() * 1000)
        prev_offset = int(self._server_time_offset_ms)
        new_offset = int(server_ms - local_ms)
        self._server_time_offset_ms = new_offset
        self._server_time_offset_synced_at = now
        if force or abs(new_offset - prev_offset) >= 250:
            log(f"Binance server time sync offset={new_offset}ms", level="WARN" if abs(new_offset) >= 1000 else "INFO")
        return new_offset
    def _static_exchange_info_by_symbol(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for symbol, spec in _STATIC_BINANCE_USDC_SPECS.items():
            row = {
                "symbol": symbol,
                "quoteAsset": str(spec.get("quoteAsset", "USDC") or "USDC"),
                "filters": [
                    {"filterType": "LOT_SIZE", "stepSize": str(spec.get("lotSz", "1") or "1"), "minQty": str(spec.get("minSz", "0") or "0")},
                    {"filterType": "PRICE_FILTER", "tickSize": str(spec.get("tickSz", "0.01") or "0.01")},
                ],
                "status": "TRADING",
            }
            out[symbol] = row
        return out
    @staticmethod
    def _decimal_text(v: Decimal) -> str:
        txt = format(v, "f").rstrip("0").rstrip(".")
        if not txt:
            return "0"
        if txt == "-0":
            return "0"
        return txt
    @staticmethod
    def _fmt_price(v: float) -> str:
        txt = f"{float(v):.12f}".rstrip("0").rstrip(".")
        return txt if txt else "0"
    @staticmethod
    def _to_float(v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0
    def mark_force_pos_side(self, inst_id: str) -> None:
        return
    def use_pos_side(self, inst_id: str) -> bool:
        return False
    def _sign(self, payload: str) -> str:
        return hmac.new(
            self.cfg.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        signed: bool = False,
        base_url: str = "",
    ) -> Any:
        method_up = str(method or "GET").upper()
        use_base_url = str(base_url or self._public_base_url or self.cfg.base_url).rstrip("/")
        headers = {
            "User-Agent": self.cfg.user_agent,
            "Accept": "application/json, text/plain, */*",
        }
        if method_up in {"POST", "PUT", "DELETE"}:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if signed:
            if not self.cfg.api_key or not self.cfg.secret_key:
                raise RuntimeError("Missing Binance credentials in env")
            headers["X-MBX-APIKEY"] = self.cfg.api_key
        max_retry = int(max(0, self._http_max_retries))
        base_candidates = self._request_base_url_candidates(use_base_url, signed=signed)

        def _build_encoded_params() -> str:
            clean: Dict[str, str] = {}
            if params:
                for key, value in params.items():
                    if value is None:
                        continue
                    value_txt = str(value).strip()
                    if value_txt == "":
                        continue
                    clean[str(key)] = value_txt
            if signed:
                try:
                    self._sync_server_time(force=False)
                except Exception:
                    pass
                clean["timestamp"] = str(self._signed_timestamp_ms())
                clean["recvWindow"] = str(self._signed_recv_window_ms())
            encoded_local = urllib.parse.urlencode(clean)
            if signed:
                signature = self._sign(encoded_local)
                encoded_local = f"{encoded_local}&signature={signature}" if encoded_local else f"signature={signature}"
            return encoded_local

        if signed:
            self._ensure_endpoint_not_cooled_down(method_up, path)

        for base_idx, candidate_base_url in enumerate(base_candidates):
            for attempt in range(max_retry + 1):
                encoded = _build_encoded_params()
                if method_up in {"GET", "DELETE"}:
                    url = candidate_base_url + path + (f"?{encoded}" if encoded else "")
                    data = None
                else:
                    url = candidate_base_url + path
                    data = encoded.encode("utf-8") if encoded else None
                req = urllib.request.Request(url=url, data=data, headers=headers, method=method_up)
                try:
                    timeout_s = self._private_http_timeout_seconds if signed else self._public_http_timeout_seconds
                    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                        raw = resp.read().decode("utf-8")
                except urllib.error.HTTPError as e:
                    body_txt = ""
                    try:
                        body_txt = e.read().decode("utf-8", errors="ignore")
                    except Exception:
                        body_txt = ""
                    snippet = body_txt[:300].replace("\n", " ").strip()
                    try:
                        payload = json.loads(body_txt) if body_txt else {}
                    except Exception:
                        payload = {}
                    code = payload.get("code") if isinstance(payload, dict) else None
                    msg = payload.get("msg") if isinstance(payload, dict) else None
                    http_code = int(getattr(e, "code", 0) or 0)
                    if signed and self._is_timestamp_skew_error(code, msg) and attempt < max_retry:
                        self._sync_server_time(force=True)
                        log(
                            f"Binance timestamp resync retry {attempt + 1}/{max_retry} for {method_up} {path}.",
                            level="WARN",
                        )
                        continue
                    if signed and self._is_conditional_order_rate_limit(path, code=code, msg=msg, http_code=http_code):
                        wait_s = self._arm_endpoint_cooldown(method_up, path)
                        raise RuntimeError(
                            f"Binance API error: code={code or -1015} msg={msg or 'Too many new orders'}"
                            f" (cooldown {wait_s:.2f}s)"
                        ) from e
                    transient_http = self._is_transient_http_code(http_code)
                    if attempt < max_retry and transient_http:
                        wait_s = self._retry_backoff_seconds(attempt)
                        log(
                            f"Binance HTTP transient retry {attempt + 1}/{max_retry} for {method_up} {path} after {wait_s:.2f}s (HTTP {e.code}).",
                            level="WARN",
                        )
                        time.sleep(wait_s)
                        continue
                    if transient_http and (not signed) and base_idx + 1 < len(base_candidates):
                        next_base = base_candidates[base_idx + 1]
                        log(
                            f"Binance public REST fallback for {method_up} {path}: {candidate_base_url} -> {next_base}",
                            level="WARN",
                        )
                        break
                    if code is not None or msg is not None:
                        raise RuntimeError(f"Binance API error: code={code} msg={msg}") from e
                    raise RuntimeError(
                        f"HTTP request failed: {method_up} {path} | HTTP {e.code} {e.reason}"
                        + (f" | body={snippet}" if snippet else "")
                    ) from e
                except Exception as e:
                    transient_exc = self._is_transient_exception(e)
                    if attempt < max_retry and transient_exc:
                        wait_s = self._retry_backoff_seconds(attempt)
                        log(
                            f"Binance HTTP transient retry {attempt + 1}/{max_retry} for {method_up} {path} after {wait_s:.2f}s.",
                            level="WARN",
                        )
                        time.sleep(wait_s)
                        continue
                    if transient_exc and (not signed) and base_idx + 1 < len(base_candidates):
                        next_base = base_candidates[base_idx + 1]
                        log(
                            f"Binance public REST fallback for {method_up} {path}: {candidate_base_url} -> {next_base}",
                            level="WARN",
                        )
                        break
                    raise RuntimeError(f"HTTP request failed: {method_up} {path} | {e}") from e
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as e:
                    snippet = raw[:300].replace("\n", " ").strip()
                    if attempt < max_retry:
                        wait_s = self._retry_backoff_seconds(attempt)
                        log(
                            f"Binance invalid JSON retry {attempt + 1}/{max_retry} for {method_up} {path} after {wait_s:.2f}s.",
                            level="WARN",
                        )
                        time.sleep(wait_s)
                        continue
                    if (not signed) and base_idx + 1 < len(base_candidates):
                        next_base = base_candidates[base_idx + 1]
                        log(
                            f"Binance public REST fallback for {method_up} {path}: {candidate_base_url} -> {next_base} (invalid JSON)",
                            level="WARN",
                        )
                        break
                    raise RuntimeError(f"Invalid JSON response: {snippet}") from e
                if isinstance(payload, dict):
                    code = payload.get("code")
                    msg = payload.get("msg")
                    if code is not None and str(code).strip() not in {"0", "200"}:
                        if signed and self._is_timestamp_skew_error(code, msg) and attempt < max_retry:
                            self._sync_server_time(force=True)
                            log(
                                f"Binance timestamp resync retry {attempt + 1}/{max_retry} for {method_up} {path}.",
                                level="WARN",
                            )
                            continue
                        if signed and self._is_conditional_order_rate_limit(path, code=code, msg=msg):
                            wait_s = self._arm_endpoint_cooldown(method_up, path)
                            raise RuntimeError(
                                f"Binance API error: code={code} msg={msg} (cooldown {wait_s:.2f}s)"
                            )
                        raise RuntimeError(f"Binance API error: code={code} msg={msg}")
                if (not signed) and candidate_base_url in self._public_base_urls:
                    self._active_public_base_url = candidate_base_url
                return payload
        raise RuntimeError(f"HTTP request failed: exhausted retries for {method_up} {path}")
    def _private_api_mode(self) -> str:
        if self._detected_private_api_mode in {"fapi", "papi"}:
            return str(self._detected_private_api_mode)
        configured = str(self._binance_private_api_mode or "auto").strip().lower()
        if configured in {"fapi", "papi"}:
            self._detected_private_api_mode = configured
            log(f"Binance private API mode pinned: {configured.upper()}")
            return configured
        return self._detect_private_api_mode()
    def _detect_private_api_mode(self) -> str:
        if self._detected_private_api_mode in {"fapi", "papi"}:
            return str(self._detected_private_api_mode)
        probe_errors: List[str] = []
        probes = [
            ("fapi", self._fapi_private_base_url, "GET", "/fapi/v3/balance"),
            ("papi", self._papi_private_base_url, "GET", "/papi/v1/um/account"),
        ]
        for mode, base_url_txt, method_txt, path_txt in probes:
            try:
                self._request(method_txt, path_txt, signed=True, base_url=base_url_txt)
            except Exception as e:
                probe_errors.append(f"{mode}:{e}")
                continue
            self._detected_private_api_mode = mode
            log(f"Binance private API mode detected: {mode.upper()}")
            return mode
        raise RuntimeError("Binance private API mode detection failed: " + " | ".join(probe_errors))
    def _um_private_request(
        self,
        method: str,
        fapi_path: str,
        papi_path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        signed: bool = True,
    ) -> Any:
        mode = self._private_api_mode()
        if mode == "papi":
            return self._request(method, papi_path, params=params, signed=signed, base_url=self._papi_private_base_url)
        return self._request(method, fapi_path, params=params, signed=signed, base_url=self._fapi_private_base_url)

    def _load_exchange_info(self) -> Dict[str, Dict[str, Any]]:
        if self._exchange_info_by_symbol is not None:
            return self._exchange_info_by_symbol
        try:
            data = self._request("GET", "/fapi/v1/exchangeInfo")
        except Exception as e:
            static_rows = self._static_exchange_info_by_symbol()
            if static_rows:
                log(f"Binance exchangeInfo unavailable; using static symbol specs fallback: {e}", level="WARN")
                self._exchange_info_by_symbol = static_rows
                return static_rows
            raise
        by_symbol: Dict[str, Dict[str, Any]] = {}
        rows = data.get("symbols", []) if isinstance(data, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "") or "").strip().upper()
            if not symbol:
                continue
            by_symbol[symbol] = row
        if not by_symbol:
            static_rows = self._static_exchange_info_by_symbol()
            if static_rows:
                log("Binance exchangeInfo returned empty symbols; using static symbol specs fallback.", level="WARN")
                by_symbol = static_rows
        self._exchange_info_by_symbol = by_symbol
        return by_symbol

    def _candidate_symbols(self, inst_id: str) -> List[str]:
        raw = str(inst_id or "").strip().upper()
        quote_asset = str(getattr(self.cfg, "binance_quote_asset", "USDC") or "USDC").strip().upper()
        out: List[str] = []
        seen: set[str] = set()

        def _push(value: str) -> None:
            txt = str(value or "").strip().upper()
            if not txt or txt in seen:
                return
            seen.add(txt)
            out.append(txt)

        symbol_map = getattr(self.cfg, "binance_symbol_map", {}) or {}
        if raw in symbol_map:
            _push(symbol_map[raw])
        _push(raw)
        if "-" not in raw:
            _push(raw.replace("/", ""))
        base, quote, inst_type = parse_inst_parts(raw)
        if base:
            _push(f"{base}{quote_asset}")
            if quote:
                _push(f"{base}{str(quote).strip().upper()}")
        compact = raw.replace("-", "").replace("/", "")
        _push(compact)
        return out

    def _resolve_symbol(self, inst_id: str) -> str:
        raw = str(inst_id or "").strip().upper()
        if raw in self._symbol_alias_cache:
            return self._symbol_alias_cache[raw]
        by_symbol = self._load_exchange_info()
        for candidate in self._candidate_symbols(raw):
            if candidate in by_symbol:
                self._symbol_alias_cache[raw] = candidate
                return candidate
        raise RuntimeError(f"Binance symbol not found for instId={inst_id}")

    def _interval(self, bar: str) -> str:
        text = str(bar or "").strip().lower()
        if text.endswith("m") and text[:-1].isdigit():
            return f"{int(text[:-1])}m"
        if text.endswith("h") and text[:-1].isdigit():
            return f"{int(text[:-1])}h"
        if text.endswith("d") and text[:-1].isdigit():
            return f"{int(text[:-1])}d"
        raise RuntimeError(f"Unsupported Binance bar format: {bar}")

    @staticmethod
    def _clone_candles(candles: List[Candle]) -> List[Candle]:
        out: List[Candle] = []
        for row in candles:
            if not isinstance(row, Candle):
                continue
            out.append(
                Candle(
                    ts_ms=int(row.ts_ms),
                    open=float(row.open),
                    high=float(row.high),
                    low=float(row.low),
                    close=float(row.close),
                    confirm=bool(row.confirm),
                    volume=float(row.volume),
                )
            )
        return out

    def _recent_candle_cache_key(self, inst_id: str, bar: str, include_unconfirmed: bool) -> Tuple[str, str, bool]:
        return (str(inst_id or "").strip().upper(), str(bar or "").strip().lower(), bool(include_unconfirmed))

    def _remember_recent_candles(
        self,
        inst_id: str,
        bar: str,
        *,
        include_unconfirmed: bool,
        candles: List[Candle],
    ) -> None:
        if not candles:
            return
        key = self._recent_candle_cache_key(inst_id, bar, include_unconfirmed)
        self._recent_candle_cache[key] = (time.time(), self._clone_candles(candles))

    def _load_recent_candles(
        self,
        inst_id: str,
        bar: str,
        *,
        include_unconfirmed: bool,
    ) -> Tuple[List[Candle], float] | Tuple[None, None]:
        key = self._recent_candle_cache_key(inst_id, bar, include_unconfirmed)
        cached = self._recent_candle_cache.get(key)
        if not cached:
            return None, None
        cached_at, candles = cached
        age_s = max(0.0, time.time() - float(cached_at))
        if self._recent_candle_cache_ttl_seconds > 0 and age_s > self._recent_candle_cache_ttl_seconds:
            return None, None
        return self._clone_candles(candles), age_s

    @staticmethod
    def _is_conditional_order_rate_limit(path: str, code: Any = None, msg: Any = None, http_code: int = 0) -> bool:
        if str(path or "").strip() != "/papi/v1/um/conditional/order":
            return False
        txt = f"{code} {msg}".lower()
        if int(http_code or 0) == 429:
            return True
        return ("-1015" in txt) or ("too many new orders" in txt)

    @staticmethod
    def _endpoint_cooldown_key(method: str, path: str) -> str:
        return f"{str(method or '').strip().upper()} {str(path or '').strip()}"

    def _arm_endpoint_cooldown(self, method: str, path: str, *, seconds: float = 0.0) -> float:
        key = self._endpoint_cooldown_key(method, path)
        wait_s = max(0.0, float(seconds or 0.0) or self._conditional_order_cooldown_seconds)
        until_ts = time.time() + wait_s
        prev_until_ts = float(self._signed_endpoint_cooldown_until.get(key, 0.0) or 0.0)
        if until_ts > prev_until_ts:
            self._signed_endpoint_cooldown_until[key] = until_ts
        else:
            until_ts = prev_until_ts
            wait_s = max(0.0, until_ts - time.time())
        return wait_s

    def _ensure_endpoint_not_cooled_down(self, method: str, path: str) -> None:
        key = self._endpoint_cooldown_key(method, path)
        until_ts = float(self._signed_endpoint_cooldown_until.get(key, 0.0) or 0.0)
        now_ts = time.time()
        if until_ts <= now_ts:
            return
        remain_s = max(0.0, until_ts - now_ts)
        raise RuntimeError(f"Binance request cooldown active for {method} {path}: wait {remain_s:.2f}s")

    def get_candles(self, inst_id: str, bar: str, limit: int, include_unconfirmed: bool = False) -> List[Candle]:
        symbol = self._resolve_symbol(inst_id)
        interval = self._interval(bar)
        try:
            rows = self._request(
                "GET",
                "/fapi/v1/klines",
                params={"symbol": symbol, "interval": interval, "limit": str(max(1, int(limit)))},
            )
        except Exception as e:
            cached_rows, age_s = self._load_recent_candles(
                inst_id,
                bar,
                include_unconfirmed=include_unconfirmed,
            )
            if cached_rows:
                log(
                    f"[{inst_id}] Binance candle cache fallback for {bar}: {e} (cache_age={float(age_s or 0.0):.1f}s)",
                    level="WARN",
                )
                return cached_rows[-max(1, int(limit)):]
            raise
        out: List[Candle] = []
        now_ms = int(time.time() * 1000)
        if not isinstance(rows, list):
            rows = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 7:
                continue
            try:
                open_ts = int(row[0])
                close_ts = int(row[6])
                confirm = bool(close_ts <= now_ms)
                out.append(
                    Candle(
                        ts_ms=open_ts,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        confirm=confirm,
                        volume=float(row[5]),
                    )
                )
            except Exception:
                continue
        out.sort(key=lambda c: c.ts_ms)
        if not include_unconfirmed:
            out = [c for c in out if c.confirm]
        self._remember_recent_candles(
            inst_id,
            bar,
            include_unconfirmed=include_unconfirmed,
            candles=out,
        )
        return out

    def get_candles_history(self, inst_id: str, bar: str, total_limit: int) -> List[Candle]:
        need = int(max(1, total_limit))
        cached = self._load_history_cache(inst_id, bar, need)
        if cached is not None and len(cached) >= need:
            return cached[-need:]

        symbol = self._resolve_symbol(inst_id)
        interval = self._interval(bar)
        page_limit = min(1500, need)
        cursor_end: Optional[int] = None
        seen: Dict[int, Candle] = {}
        stall = 0
        while len(seen) < need + page_limit:
            params: Dict[str, str] = {
                "symbol": symbol,
                "interval": interval,
                "limit": str(page_limit),
            }
            if cursor_end is not None:
                params["endTime"] = str(cursor_end)
            rows = self._request("GET", "/fapi/v1/klines", params=params)
            if not isinstance(rows, list) or not rows:
                break
            added = 0
            next_cursor: Optional[int] = None
            for row in rows:
                if not isinstance(row, list) or len(row) < 7:
                    continue
                try:
                    open_ts = int(row[0])
                    close_ts = int(row[6])
                    candle = Candle(
                        ts_ms=open_ts,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        confirm=True,
                        volume=float(row[5]),
                    )
                except Exception:
                    continue
                if open_ts not in seen:
                    seen[open_ts] = candle
                    added += 1
                if next_cursor is None:
                    next_cursor = open_ts - 1
            if added <= 0:
                stall += 1
                if stall >= 2:
                    break
            else:
                stall = 0
            cursor_end = next_cursor
            if len(rows) < page_limit:
                break
        out = sorted(seen.values(), key=lambda x: x.ts_ms)
        if len(out) > need:
            out = out[-need:]
        self._save_history_cache(inst_id, bar, out)
        return out

    def get_positions(self, inst_id: str) -> List[Dict[str, Any]]:
        symbol = self._resolve_symbol(inst_id)
        rows = self._um_private_request(
            "GET",
            "/fapi/v3/positionRisk",
            "/papi/v1/um/positionRisk",
            params={"symbol": symbol},
            signed=True,
        )
        if not isinstance(rows, list):
            return []
        out: List[Dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            amt = self._to_float(row.get("positionAmt"))
            if abs(amt) <= 1e-12:
                continue
            item = dict(row)
            item["instId"] = inst_id
            item["symbol"] = symbol
            item["mgnMode"] = self.cfg.td_mode
            pos_side = str(row.get("positionSide", "BOTH") or "BOTH").strip().upper()
            if pos_side == "LONG":
                item["posSide"] = "long"
                item["pos"] = str(abs(amt))
            elif pos_side == "SHORT":
                item["posSide"] = "short"
                item["pos"] = str(abs(amt))
            else:
                item["posSide"] = ""
                item["pos"] = str(amt)
            out.append(item)
        return out

    def parse_position(self, rows: List[Dict[str, Any]], pos_mode: str) -> PositionState:
        if not rows:
            return PositionState("flat", 0.0)
        if pos_mode == "net":
            net = 0.0
            for row in rows:
                if "positionAmt" in row:
                    net += self._to_float(row.get("positionAmt"))
                else:
                    net += self._to_float(row.get("pos"))
            if net > 0:
                return PositionState("long", abs(net))
            if net < 0:
                return PositionState("short", abs(net))
            return PositionState("flat", 0.0)
        long_sz = 0.0
        short_sz = 0.0
        for row in rows:
            pos_side = str(row.get("positionSide", row.get("posSide", "")) or "").strip().lower()
            amt = abs(self._to_float(row.get("positionAmt", row.get("pos", 0.0))))
            if pos_side == "long":
                long_sz += amt
            elif pos_side == "short":
                short_sz += amt
        if long_sz > 0 and short_sz > 0:
            return PositionState("mixed", max(long_sz, short_sz))
        if long_sz > 0:
            return PositionState("long", long_sz)
        if short_sz > 0:
            return PositionState("short", short_sz)
        return PositionState("flat", 0.0)

    def split_positions_by_mgn_mode(
        self,
        rows: List[Dict[str, Any]],
        td_mode: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        matched: List[Dict[str, Any]] = []
        foreign: List[Dict[str, Any]] = []
        target = str(td_mode or "").strip().lower()
        for row in rows:
            mode = str(row.get("mgnMode", target) or "").strip().lower()
            if (not target) or (not mode) or mode == target:
                matched.append(row)
            else:
                foreign.append(row)
        return matched, foreign

    def _extract_balance_value(self, row: Dict[str, Any]) -> float:
        for key in (
            "totalWalletBalance",
            "crossMarginAsset",
            "crossMarginFree",
            "walletBalance",
            "crossWalletBalance",
            "balance",
            "availableBalance",
            "umWalletBalance",
            "cmWalletBalance",
        ):
            val = self._to_float(row.get(key))
            if val > 0:
                return val
        return 0.0

    def get_account_equity(self, force_refresh: bool = False) -> Optional[float]:
        ttl = max(0, int(self.cfg.compound_cache_seconds))
        now = time.time()
        if (not force_refresh) and ttl > 0 and self._equity_cache_value is not None:
            if now - self._equity_cache_ts <= ttl:
                return self._equity_cache_value
        try:
            prefer_asset = (
                str(self.cfg.compound_balance_ccy or "").strip().upper()
                or str(getattr(self.cfg, "binance_quote_asset", "USDC") or "USDC").strip().upper()
            )
            mode = self._private_api_mode()
            rows_to_scan: List[Dict[str, Any]] = []
            if mode == "papi":
                balance_rows = self._request("GET", "/papi/v1/balance", signed=True, base_url=self._papi_private_base_url)
                if isinstance(balance_rows, list):
                    rows_to_scan.extend([dict(row) for row in balance_rows if isinstance(row, dict)])
                account = self._request("GET", "/papi/v1/um/account", signed=True, base_url=self._papi_private_base_url)
                assets = account.get("assets", []) if isinstance(account, dict) else []
                if isinstance(assets, list):
                    rows_to_scan.extend([dict(row) for row in assets if isinstance(row, dict)])
            else:
                balance_rows = self._request("GET", "/fapi/v3/balance", signed=True, base_url=self._fapi_private_base_url)
                if isinstance(balance_rows, list):
                    rows_to_scan.extend([dict(row) for row in balance_rows if isinstance(row, dict)])
            if not rows_to_scan:
                return None
            asset_values: Dict[str, float] = {}
            for row in rows_to_scan:
                asset = str(row.get("asset", "") or "").strip().upper()
                if not asset:
                    continue
                value = self._extract_balance_value(row)
                if value > asset_values.get(asset, 0.0):
                    asset_values[asset] = value
            equity = asset_values.get(prefer_asset, 0.0)
            if equity <= 0:
                equity = sum(val for val in asset_values.values() if val > 0)
            if equity <= 0:
                return None
            self._equity_cache_value = equity
            self._equity_cache_ts = now
            return equity
        except Exception as e:
            log(f"[Account] Binance equity fetch failed: {e}")
            return None

    def resolve_margin_usdt(self) -> float:
        base_margin = float(self.cfg.margin_usdt)
        if base_margin <= 0:
            raise RuntimeError("OKX_MARGIN_USDT must be > 0 when OKX_SIZING_MODE=margin")
        if not self.cfg.compound_enabled:
            return base_margin
        eq = self.get_account_equity()
        if eq is None or eq <= 0:
            return base_margin
        if self._equity_peak is None or eq > self._equity_peak:
            self._equity_peak = eq
        peak = max(eq, self._equity_peak or eq)
        dd_ratio = 0.0 if peak <= 0 else max(0.0, (peak - eq) / peak)
        mode = self.cfg.compound_mode
        if mode == "ratio":
            base_eq = max(1e-9, float(self.cfg.compound_base_equity))
            ratio = max(0.0, eq / base_eq)
            scaled_margin = float(self.cfg.compound_base_margin) * (ratio ** float(self.cfg.compound_ratio_power))
        else:
            step_eq = max(1e-9, float(self.cfg.compound_step_equity))
            steps = math.floor((eq - float(self.cfg.compound_base_equity)) / step_eq)
            scaled_margin = float(self.cfg.compound_base_margin) + steps * float(self.cfg.compound_step_margin)
        guard_applied = False
        if self.cfg.compound_dd_guard_pct > 0 and dd_ratio >= float(self.cfg.compound_dd_guard_pct):
            scaled_margin = scaled_margin * float(self.cfg.compound_dd_factor)
            guard_applied = True
        scaled_margin = max(float(self.cfg.compound_min_margin), scaled_margin)
        scaled_margin = min(float(self.cfg.compound_max_margin), scaled_margin)
        scaled_margin = max(0.01, scaled_margin)
        log(
            "[Sizing] compound margin={} eq={} peak={} dd={:.1f}% mode={} guard={}".format(
                round(scaled_margin, 4),
                round(eq, 4),
                round(peak, 4),
                dd_ratio * 100.0,
                mode,
                "on" if guard_applied else "off",
            )
        )
        return scaled_margin

    def get_instrument(self, inst_id: str) -> Dict[str, Any]:
        if inst_id in self._instrument_cache:
            return self._instrument_cache[inst_id]
        symbol = self._resolve_symbol(inst_id)
        by_symbol = self._load_exchange_info()
        info = dict(by_symbol.get(symbol, {}))
        if not info:
            raise RuntimeError(f"Instrument not found: {inst_id}")
        filters = {str(item.get("filterType", "") or ""): item for item in info.get("filters", []) if isinstance(item, dict)}
        lot = filters.get("LOT_SIZE", {})
        price_filter = filters.get("PRICE_FILTER", {})
        info["instId"] = inst_id
        info["symbol"] = symbol
        info["lotSz"] = str(lot.get("stepSize", "1") or "1")
        info["minSz"] = str(lot.get("minQty", "0") or "0")
        info["tickSz"] = str(price_filter.get("tickSize", "0.01") or "0.01")
        info["ctVal"] = "1"
        info["ctValCcy"] = str(info.get("quoteAsset", getattr(self.cfg, "binance_quote_asset", "USDC")) or "")
        self._instrument_cache[inst_id] = info
        return info

    def _normalize_price(self, inst_id: str, px: float) -> str:
        if px <= 0:
            raise RuntimeError("Price must be > 0")
        info = self.get_instrument(inst_id)
        tick_raw = str(info.get("tickSz", "0") or "0")
        try:
            d_px = Decimal(str(px))
            d_tick = Decimal(tick_raw)
        except Exception as e:
            raise RuntimeError(f"Invalid price precision config for {inst_id}: tickSz={tick_raw}") from e
        if d_tick > 0:
            d_px = (d_px / d_tick).to_integral_value(rounding=ROUND_HALF_UP) * d_tick
        if d_px <= 0:
            raise RuntimeError(f"Price too small after tick normalization: instId={inst_id}, px={px}")
        return self._decimal_text(d_px)

    def normalize_order_size(self, inst_id: str, sz: float, reduce_only: bool = False) -> Tuple[float, str]:
        if sz <= 0:
            raise RuntimeError("Order size must be > 0")
        info = self.get_instrument(inst_id)
        lot_raw = str(info.get("lotSz", "0") or "0")
        min_raw = str(info.get("minSz", "0") or "0")
        try:
            d_sz = Decimal(str(sz))
            d_lot = Decimal(lot_raw)
            d_min = Decimal(min_raw)
        except Exception as e:
            raise RuntimeError(f"Invalid size precision config for {inst_id}: lotSz={lot_raw} minSz={min_raw}") from e
        if d_lot > 0:
            d_sz = (d_sz / d_lot).to_integral_value(rounding=ROUND_DOWN) * d_lot
        if (not reduce_only) and d_min > 0 and d_sz < d_min:
            d_sz = d_min
            if d_lot > 0:
                d_sz = (d_sz / d_lot).to_integral_value(rounding=ROUND_DOWN) * d_lot
        if d_sz <= 0:
            raise RuntimeError(f"Order size too small after lot normalization: instId={inst_id}, sz={sz}")
        return float(d_sz), self._decimal_text(d_sz)

    def ensure_leverage(
        self,
        inst_id: str,
        pos_side: Optional[str] = None,
        entry_side: Optional[str] = None,
    ) -> None:
        if self.cfg.leverage <= 0:
            return
        symbol = self._resolve_symbol(inst_id)
        lev_int = max(1, min(125, int(round(float(self.cfg.leverage)))))
        private_mode = self._private_api_mode()
        cache_key = f"{private_mode}:{symbol}:{self.cfg.td_mode}:{lev_int}"
        if self._leverage_ready.get(cache_key):
            return
        margin_type = "ISOLATED" if str(self.cfg.td_mode or "cross").strip().lower() == "isolated" else "CROSSED"
        if private_mode == "fapi":
            try:
                if not self.cfg.dry_run:
                    self._request(
                        "POST",
                        "/fapi/v1/marginType",
                        params={"symbol": symbol, "marginType": margin_type},
                        signed=True,
                        base_url=self._fapi_private_base_url,
                    )
            except Exception as e:
                msg = str(e).lower()
                if "no need to change margin type" not in msg:
                    log(f"[{inst_id}] Binance marginType setup warning: {e}", level="WARN")
        if self.cfg.dry_run:
            log(f'[DRY-RUN] binance leverage payload={{"symbol":"{symbol}","leverage":"{lev_int}","mode":"{private_mode}"}}')
            self._leverage_ready[cache_key] = True
            return
        self._um_private_request(
            "POST",
            "/fapi/v1/leverage",
            "/papi/v1/um/leverage",
            params={"symbol": symbol, "leverage": str(lev_int)},
            signed=True,
        )
        self._leverage_ready[cache_key] = True
        log(f"[{inst_id}] Binance leverage ready: symbol={symbol} leverage={lev_int} marginType={margin_type} mode={private_mode}")

    def calc_order_size(
        self,
        cfg: Config,
        inst_id: str,
        entry_price: float,
        stop_price: Optional[float] = None,
        entry_side: str = "",
    ) -> float:
        if cfg.sizing_mode == "fixed":
            return cfg.order_size
        if cfg.sizing_mode != "margin":
            raise RuntimeError(f"Unsupported OKX_SIZING_MODE: {cfg.sizing_mode}")
        if cfg.leverage <= 0:
            raise RuntimeError("OKX_LEVERAGE must be > 0 when OKX_SIZING_MODE=margin")
        if entry_price <= 0:
            raise RuntimeError("Entry price must be > 0 to calculate margin-based size")
        info = self.get_instrument(inst_id)
        try:
            lot_sz = float(info.get("lotSz", "1") or "1")
        except Exception:
            lot_sz = 1.0
        try:
            min_sz = float(info.get("minSz", "0") or "0")
        except Exception:
            min_sz = 0.0
        if min_sz <= 0:
            min_sz = lot_sz

        def _floor_to_step(value: float, step: float) -> float:
            if step <= 0:
                return value
            return math.floor(value / step) * step

        def size_by_margin() -> float:
            margin_usdt = self.resolve_margin_usdt()
            if margin_usdt <= 0:
                raise RuntimeError("Resolved margin must be > 0 when OKX_SIZING_MODE=margin")
            target_notional = margin_usdt * cfg.leverage
            raw_sz = target_notional / entry_price
            sized = _floor_to_step(raw_sz, lot_sz)
            if sized < min_sz:
                sized = min_sz
            log(
                "Sizing: mode=margin margin={} base_margin={} lever={} target_notional={} entry_price={} raw_sz={} final_sz={}".format(
                    round(margin_usdt, 6),
                    round(cfg.margin_usdt, 6),
                    cfg.leverage,
                    round(target_notional, 6),
                    round(entry_price, 6),
                    round(raw_sz, 6),
                    round_size(sized),
                )
            )
            return sized

        risk_frac = float(getattr(cfg.params, "risk_frac", 0.0) or 0.0)
        margin_cfg = float(getattr(cfg, "margin_usdt", 0.0) or 0.0)
        risk_mode_enabled = (risk_frac > 0) and (margin_cfg <= 0)
        if risk_frac > 0 and not risk_mode_enabled:
            log(
                f"[{inst_id}] Sizing: STRAT_RISK_FRAC={round(risk_frac, 6)} ignored because OKX_MARGIN_USDT={round(margin_cfg, 6)} > 0.",
                level="WARN",
            )
        if risk_mode_enabled:
            if stop_price is None or float(stop_price) <= 0:
                raise RuntimeError(f"[{inst_id}] Risk-based sizing requires valid stop_price")
            equity = self.get_account_equity()
            if equity is None or float(equity) <= 0:
                raise RuntimeError(f"[{inst_id}] Risk-based sizing requires positive account equity")
            stop_pct = abs(float(entry_price) - float(stop_price)) / max(float(entry_price), 1e-12)
            loss_per_unit = abs(float(entry_price) - float(stop_price))
            if loss_per_unit <= 0:
                raise RuntimeError(f"[{inst_id}] Risk-based sizing requires entry_price != stop_price")
            risk_usdt = float(equity) * float(risk_frac)
            raw_sz = risk_usdt / loss_per_unit
            sized = _floor_to_step(raw_sz, lot_sz)
            if sized < min_sz:
                sized = min_sz
            est_notional = sized * float(entry_price)
            est_margin = est_notional / max(float(cfg.leverage), 1e-9)
            max_margin_frac = float(getattr(cfg.params, "risk_max_margin_frac", 0.0) or 0.0)
            max_margin_allowed = float(equity) * max_margin_frac if max_margin_frac > 0 else float("inf")
            log(
                "Sizing: mode=risk risk_frac={} side={} equity={} risk_usdt={} stop_pct={:.5f} loss_per_unit={} lever={} est_margin={} est_notional={} max_margin_frac={} max_margin={} raw_sz={} final_sz={}".format(
                    round(risk_frac, 6),
                    str(entry_side or "").strip().lower() or "na",
                    round(float(equity), 6),
                    round(risk_usdt, 6),
                    stop_pct,
                    round(loss_per_unit, 8),
                    cfg.leverage,
                    round(est_margin, 6),
                    round(est_notional, 6),
                    round(max_margin_frac, 6),
                    round(max_margin_allowed, 6) if math.isfinite(max_margin_allowed) else "inf",
                    round(raw_sz, 6),
                    round_size(sized),
                )
            )
            if max_margin_frac > 0 and est_margin > max_margin_allowed:
                raise RuntimeError(
                    f"[{inst_id}] Risk guard blocked order: estimated margin {round(est_margin, 6)} > cap {round(max_margin_allowed, 6)} (equity={round(float(equity), 6)}, cap_frac={round(max_margin_frac, 6)})."
                )
            if est_margin > max(float(equity), 0.0):
                log(
                    f"[{inst_id}] Sizing: risk-based estimated margin ({round(est_margin, 6)}) > equity ({round(float(equity), 6)}).",
                    level="WARN",
                )
            return sized
        return size_by_margin()

    def _normalize_order_row(self, raw: Dict[str, Any], *, fallback_cl_ord_id: str = "") -> Dict[str, Any]:
        row = dict(raw or {})
        ord_id = str(raw.get("orderId", raw.get("ordId", "")) or "").strip()
        cl_ord_id = str(raw.get("clientOrderId", raw.get("origClientOrderId", fallback_cl_ord_id)) or fallback_cl_ord_id).strip()
        status = str(raw.get("status", raw.get("state", "")) or "").strip().upper()
        state_map = {
            "NEW": "live",
            "PARTIALLY_FILLED": "partially_filled",
            "FILLED": "filled",
            "CANCELED": "canceled",
            "CANCELLED": "canceled",
            "REJECTED": "order_failed",
            "EXPIRED": "canceled",
            "PENDING_CANCEL": "live",
        }
        state = state_map.get(status, str(raw.get("state", "") or "").strip().lower())
        row["ordId"] = ord_id
        if cl_ord_id:
            row["clOrdId"] = cl_ord_id
        if state:
            row["state"] = state
        executed_qty = raw.get("executedQty", raw.get("cumQty", raw.get("accFillSz", "0")))
        row["accFillSz"] = str(executed_qty or "0")
        avg_price = self._to_float(raw.get("avgPrice", raw.get("avgPx", 0.0)))
        if avg_price > 0:
            row["avgPx"] = self._fmt_price(avg_price)
        if str(raw.get("side", "") or "").strip():
            row["side"] = str(raw.get("side", "") or "").strip().lower()
        stop_px = self._to_float(raw.get("stopPrice", raw.get("slTriggerPx", 0.0)))
        if stop_px > 0:
            row["slTriggerPx"] = self._fmt_price(stop_px)
        return row

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side: Optional[str] = None,
        reduce_only: bool = False,
        attach_algo_ords: Optional[List[Dict[str, Any]]] = None,
        cl_ord_id: str = "",
        ord_type: str = "market",
        px: float = 0.0,
        post_only: bool = False,
    ) -> Dict[str, Any]:
        cl_ord_id_txt = str(cl_ord_id or "").strip()
        normalized_sz, normalized_sz_txt = self.normalize_order_size(inst_id, sz, reduce_only=reduce_only)
        if abs(float(sz) - normalized_sz) > 1e-12:
            log(f"[{inst_id}] Size normalized by lot/min rule: raw={round_size(float(sz))} -> final={normalized_sz_txt}")
        if attach_algo_ords:
            log(f"[{inst_id}] Binance ignored attach_algo_ords on entry; using client-managed exits.", level="WARN")
        ord_type_norm = str(ord_type or "market").strip().lower()
        if ord_type_norm not in {"market", "limit", "post_only"}:
            ord_type_norm = "market"
        if post_only and ord_type_norm == "limit":
            ord_type_norm = "post_only"
        symbol = self._resolve_symbol(inst_id)
        params: Dict[str, str] = {
            "symbol": symbol,
            "side": str(side or "").strip().upper(),
            "quantity": normalized_sz_txt,
        }
        if cl_ord_id_txt:
            params["newClientOrderId"] = cl_ord_id_txt
        if reduce_only:
            params["reduceOnly"] = "true"
        if ord_type_norm == "market":
            params["type"] = "MARKET"
            params["newOrderRespType"] = "RESULT"
        else:
            params["type"] = "LIMIT"
            params["price"] = self._normalize_price(inst_id, float(px))
            params["timeInForce"] = "GTX" if ord_type_norm == "post_only" else "GTC"
            params["newOrderRespType"] = "ACK"
        if self.cfg.dry_run:
            log(f"[DRY-RUN] binance place_order payload={json.dumps(params, ensure_ascii=False)}")
            row: Dict[str, Any] = {
                "ordId": "DRY_RUN",
                "state": "live" if ord_type_norm != "market" else "filled",
                "accFillSz": normalized_sz_txt if ord_type_norm == "market" else "0",
            }
            if cl_ord_id_txt:
                row["clOrdId"] = cl_ord_id_txt
            return {"data": [row]}

        def _recover_by_cl_ord_id(exc: Exception) -> Optional[Dict[str, Any]]:
            if not cl_ord_id_txt:
                return None
            allow_probe = self._is_duplicate_cl_ord_id_error(exc) or self._is_transient_error_text(str(exc))
            if not allow_probe:
                return None
            try:
                row_probe = self.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id_txt)
            except Exception:
                return None
            if not isinstance(row_probe, dict) or not row_probe:
                return None
            ord_id_probe = str(row_probe.get("ordId", "") or "").strip()
            if not ord_id_probe:
                return None
            log(f"[{inst_id}] Idempotency hit: clOrdId={cl_ord_id_txt} already exists, reuse ordId={ord_id_probe}.")
            return {"data": [row_probe]}

        try:
            raw = self._um_private_request(
                "POST",
                "/fapi/v1/order",
                "/papi/v1/um/order",
                params=params,
                signed=True,
            )
            return {"data": [self._normalize_order_row(raw, fallback_cl_ord_id=cl_ord_id_txt)]}
        except Exception as e:
            recovered = _recover_by_cl_ord_id(e)
            if recovered is not None:
                return recovered
            raise

    def get_ticker(self, inst_id: str) -> Dict[str, Any]:
        symbol = self._resolve_symbol(inst_id)
        raw = self._request("GET", "/fapi/v1/ticker/bookTicker", params={"symbol": symbol})
        if not isinstance(raw, dict):
            return {}
        out = dict(raw)
        out["bidPx"] = str(raw.get("bidPrice", raw.get("bidPx", "")) or "")
        out["askPx"] = str(raw.get("askPrice", raw.get("askPx", "")) or "")
        return out

    def build_attach_tpsl_ords(self, tp_price: float, sl_price: float, *, attach_algo_cl_ord_id: str = "") -> List[Dict[str, str]]:
        return []

    def build_sl_attach_algo_ords(self, *, sl_price: float, attach_algo_cl_ord_id: str = "") -> List[Dict[str, str]]:
        return []

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
        move_sl_to_avg_px_on_tp1: bool = True,
    ) -> List[Dict[str, str]]:
        return []

    def get_order(self, inst_id: str, ord_id: str = "", cl_ord_id: str = "") -> Dict[str, Any]:
        symbol = self._resolve_symbol(inst_id)
        params: Dict[str, str] = {"symbol": symbol}
        if str(ord_id or "").strip():
            params["orderId"] = str(ord_id).strip()
        if str(cl_ord_id or "").strip():
            params["origClientOrderId"] = str(cl_ord_id).strip()
        raw = self._um_private_request(
            "GET",
            "/fapi/v1/order",
            "/papi/v1/um/order",
            params=params,
            signed=True,
        )
        if isinstance(raw, dict):
            return self._normalize_order_row(raw, fallback_cl_ord_id=str(cl_ord_id or "").strip())
        return {}

    def cancel_order(self, inst_id: str, ord_id: str = "", cl_ord_id: str = "") -> Dict[str, Any]:
        ord_id_txt = str(ord_id or "").strip()
        cl_ord_id_txt = str(cl_ord_id or "").strip()
        if not ord_id_txt and not cl_ord_id_txt:
            raise RuntimeError("ord_id or cl_ord_id is required for cancel_order")
        symbol = self._resolve_symbol(inst_id)
        params: Dict[str, str] = {"symbol": symbol}
        if ord_id_txt:
            params["orderId"] = ord_id_txt
        if cl_ord_id_txt:
            params["origClientOrderId"] = cl_ord_id_txt
        if self.cfg.dry_run:
            log(f"[DRY-RUN] binance cancel_order payload={json.dumps(params, ensure_ascii=False)}")
            return {"data": [{"ordId": ord_id_txt or "DRY_RUN", "clOrdId": cl_ord_id_txt, "state": "canceled"}]}
        raw = self._um_private_request(
            "DELETE",
            "/fapi/v1/order",
            "/papi/v1/um/order",
            params=params,
            signed=True,
        )
        if isinstance(raw, dict):
            return {"data": [self._normalize_order_row(raw, fallback_cl_ord_id=cl_ord_id_txt)]}
        return {"data": []}

    @staticmethod
    def _extract_attach_algo(order_row: Dict[str, Any], *, prefer: str = "") -> Dict[str, Any]:
        attach = order_row.get("attachAlgoOrds")
        if isinstance(attach, list) and attach:
            rows = [dict(item) for item in attach if isinstance(item, dict)]
            if rows:
                return rows[0]
        if any(str(order_row.get(key, "") or "").strip() for key in ("attachAlgoId", "attachAlgoClOrdId", "slTriggerPx")):
            return {
                "attachAlgoId": str(order_row.get("attachAlgoId", order_row.get("ordId", "")) or "").strip(),
                "attachAlgoClOrdId": str(order_row.get("attachAlgoClOrdId", order_row.get("clOrdId", "")) or "").strip(),
                "slTriggerPx": str(order_row.get("slTriggerPx", "") or "").strip(),
            }
        return {}

    def _working_type(self) -> str:
        px_type = str(self.cfg.attach_tpsl_trigger_px_type or "last").strip().lower()
        if px_type in {"mark", "index"}:
            return "MARK_PRICE"
        return "CONTRACT_PRICE"

    def _normalize_close_order_side(self, side: str) -> str:
        side_txt = str(side or "").strip().lower()
        if side_txt in {"long", "sell"}:
            return "SELL"
        if side_txt in {"short", "buy"}:
            return "BUY"
        raise RuntimeError(f"Unsupported stop order side: {side}")

    def _normalize_stop_order_row(
        self,
        raw: Dict[str, Any],
        *,
        fallback_cl_ord_id: str = "",
        fallback_side: str = "",
    ) -> Dict[str, Any]:
        row = dict(raw or {})
        ord_id = str(raw.get("strategyId", raw.get("orderId", raw.get("ordId", ""))) or "").strip()
        cl_ord_id = str(
            raw.get(
                "newClientStrategyId",
                raw.get(
                    "clientStrategyId",
                    raw.get("clientOrderId", raw.get("origClientOrderId", fallback_cl_ord_id)),
                ),
            ) or fallback_cl_ord_id
        ).strip()
        status = str(raw.get("strategyStatus", raw.get("status", raw.get("state", ""))) or "").strip().upper()
        state_map = {
            "NEW": "live",
            "PARTIALLY_FILLED": "partially_filled",
            "FILLED": "filled",
            "TRIGGERED": "filled",
            "FINISHED": "filled",
            "CANCELED": "canceled",
            "CANCELLED": "canceled",
            "EXPIRED": "canceled",
            "REJECTED": "order_failed",
        }
        state = state_map.get(status, str(raw.get("state", "") or "").strip().lower())
        row["ordId"] = ord_id
        if cl_ord_id:
            row["clOrdId"] = cl_ord_id
        if state:
            row["state"] = state
        executed_qty = raw.get("executedQty", raw.get("cumQty", raw.get("accFillSz", raw.get("origQty", "0"))))
        row["accFillSz"] = str(executed_qty or "0")
        side_txt = str(raw.get("side", fallback_side) or fallback_side).strip().lower()
        if side_txt:
            row["side"] = side_txt
        stop_px = self._to_float(raw.get("stopPrice", raw.get("triggerPrice", raw.get("slTriggerPx", 0.0))))
        if stop_px > 0:
            row["slTriggerPx"] = self._fmt_price(stop_px)
        row["attachAlgoId"] = ord_id
        if cl_ord_id:
            row["attachAlgoClOrdId"] = cl_ord_id
        return row

    def _get_papi_conditional_order(
        self,
        *,
        inst_id: str,
        strategy_id: str = "",
        client_strategy_id: str = "",
    ) -> Dict[str, Any]:
        strategy_id_txt = str(strategy_id or "").strip()
        client_strategy_id_txt = str(client_strategy_id or "").strip()
        if not strategy_id_txt and not client_strategy_id_txt:
            raise RuntimeError("strategy_id or client_strategy_id is required")
        symbol = self._resolve_symbol(inst_id)
        params: Dict[str, str] = {"symbol": symbol}
        if strategy_id_txt:
            params["strategyId"] = strategy_id_txt
        if client_strategy_id_txt:
            params["newClientStrategyId"] = client_strategy_id_txt
        raw = self._request(
            "GET",
            "/papi/v1/um/conditional/openOrder",
            params=params,
            signed=True,
            base_url=self._papi_private_base_url,
        )
        if isinstance(raw, dict):
            return self._normalize_stop_order_row(raw, fallback_cl_ord_id=client_strategy_id_txt)
        return {}

    @staticmethod
    def _is_live_pending_stop_order_row(row: Dict[str, Any]) -> bool:
        if not isinstance(row, dict) or not row:
            return False
        strategy_type = str(row.get("strategyType", row.get("type", row.get("origType", ""))) or "").strip().upper()
        if strategy_type and strategy_type != "STOP_MARKET":
            return False
        if not str(row.get("slTriggerPx", "") or "").strip():
            return False
        state_txt = str(row.get("state", "") or "").strip().lower()
        if state_txt and state_txt not in {"live", "partially_filled"}:
            return False
        reduce_only_txt = str(row.get("reduceOnly", "") or "").strip().lower()
        if reduce_only_txt and reduce_only_txt not in {"true", "1"}:
            return False
        return True

    def _pending_stop_close_side(self, side: str) -> str:
        side_txt = str(side or "").strip()
        if not side_txt:
            return ""
        return self._normalize_close_order_side(side_txt).lower()

    @staticmethod
    def _pending_stop_sort_key(row: Dict[str, Any]) -> Tuple[int, int, int]:
        qty_rank = 1 if bool(row.get("_qty_match", False)) else 0
        stop_rank = 1 if bool(row.get("_stop_match", False)) else 0
        raw_id = str(row.get("strategyId", row.get("attachAlgoId", row.get("ordId", ""))) or "").strip()
        try:
            numeric_id = int(raw_id)
        except Exception:
            numeric_id = -1
        return (qty_rank, stop_rank, numeric_id)

    def _annotate_pending_stop_row(
        self,
        inst_id: str,
        row: Dict[str, Any],
        *,
        size: float = 0.0,
        stop_price: float = 0.0,
    ) -> Dict[str, Any]:
        out = dict(row or {})
        qty_val = self._to_float(out.get("origQty", out.get("quantity", out.get("origSz", 0.0))))
        stop_val = self._to_float(out.get("slTriggerPx", out.get("stopPrice", 0.0)))
        qty_match = True
        if float(size or 0.0) > 0:
            try:
                inst = self.get_instrument(inst_id)
            except Exception:
                inst = {}
            step = self._to_float(inst.get("lotSz", inst.get("minSz", 0.0)))
            qty_tol = max((step * 0.5) if step > 0 else 0.0, abs(float(size)) * 1e-6, 1e-9)
            qty_match = abs(qty_val - float(size)) <= qty_tol
        stop_match = True
        if float(stop_price or 0.0) > 0:
            try:
                inst = self.get_instrument(inst_id)
            except Exception:
                inst = {}
            tick = self._to_float(inst.get("tickSz", 0.0))
            stop_tol = max((tick * 0.5) if tick > 0 else 0.0, abs(float(stop_price)) * 1e-6, 1e-9)
            stop_match = abs(stop_val - float(stop_price)) <= stop_tol
        out["_qty"] = qty_val
        out["_stop"] = stop_val
        out["_qty_match"] = bool(qty_match)
        out["_stop_match"] = bool(stop_match)
        return out

    def _list_papi_pending_stop_orders(self, *, inst_id: str, side: str = "") -> List[Dict[str, Any]]:
        symbol = self._resolve_symbol(inst_id)
        want_close_side = self._pending_stop_close_side(side)
        raw = self._request(
            "GET",
            "/papi/v1/um/conditional/openOrders",
            params={"symbol": symbol},
            signed=True,
            base_url=self._papi_private_base_url,
        )
        rows = raw if isinstance(raw, list) else []
        out: List[Dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            row = self._normalize_stop_order_row(item)
            if not self._is_live_pending_stop_order_row(row):
                continue
            if want_close_side and str(row.get("side", "") or "").strip().lower() != want_close_side:
                continue
            out.append(row)
        return out

    def _list_fapi_pending_stop_orders(self, *, inst_id: str, side: str = "") -> List[Dict[str, Any]]:
        symbol = self._resolve_symbol(inst_id)
        want_close_side = self._pending_stop_close_side(side)
        raw = self._request(
            "GET",
            "/fapi/v1/openOrders",
            params={"symbol": symbol},
            signed=True,
            base_url=self._fapi_private_base_url,
        )
        rows = raw if isinstance(raw, list) else []
        out: List[Dict[str, Any]] = []
        for item in rows:
            if not isinstance(item, dict):
                continue
            row = self._normalize_order_row(item)
            if not self._is_live_pending_stop_order_row(row):
                continue
            if want_close_side and str(row.get("side", "") or "").strip().lower() != want_close_side:
                continue
            out.append(row)
        return out

    def list_pending_stop_loss_orders(
        self,
        *,
        inst_id: str,
        side: str = "",
        size: float = 0.0,
        stop_price: float = 0.0,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]]
        if self._private_api_mode() == "papi":
            rows = self._list_papi_pending_stop_orders(inst_id=inst_id, side=side)
        else:
            rows = self._list_fapi_pending_stop_orders(inst_id=inst_id, side=side)
        annotated = [
            self._annotate_pending_stop_row(inst_id, row, size=float(size or 0.0), stop_price=float(stop_price or 0.0))
            for row in rows
        ]
        annotated.sort(key=self._pending_stop_sort_key, reverse=True)
        live_count = len(annotated)
        extra_count = max(0, live_count - 1)
        for row in annotated:
            row["_live_count"] = live_count
            row["_extra_count"] = extra_count
        return annotated

    def get_pending_stop_loss_order(
        self,
        *,
        inst_id: str,
        side: str = "",
        algo_id: str = "",
        algo_cl_ord_id: str = "",
        size: float = 0.0,
        stop_price: float = 0.0,
    ) -> Dict[str, Any]:
        algo_id_txt = str(algo_id or "").strip()
        algo_cl_id_txt = str(algo_cl_ord_id or "").strip()
        want_close_side = self._pending_stop_close_side(side)
        if algo_id_txt or algo_cl_id_txt:
            row: Dict[str, Any] = {}
            try:
                if self._private_api_mode() == "papi":
                    row = self._get_papi_conditional_order(
                        inst_id=inst_id,
                        strategy_id=algo_id_txt,
                        client_strategy_id=algo_cl_id_txt,
                    )
                else:
                    row = self.get_order(inst_id=inst_id, ord_id=algo_id_txt, cl_ord_id=algo_cl_id_txt)
            except Exception:
                row = {}
            if not self._is_live_pending_stop_order_row(row):
                return {}
            if want_close_side and str(row.get("side", "") or "").strip().lower() != want_close_side:
                return {}
            out = self._annotate_pending_stop_row(
                inst_id,
                row,
                size=float(size or 0.0),
                stop_price=float(stop_price or 0.0),
            )
            out["_live_count"] = 1
            out["_extra_count"] = 0
            return out
        rows = self.list_pending_stop_loss_orders(
            inst_id=inst_id,
            side=side,
            size=float(size or 0.0),
            stop_price=float(stop_price or 0.0),
        )
        if not rows:
            return {}
        return dict(rows[0])

    def cancel_pending_stop_loss_orders(
        self,
        *,
        inst_id: str,
        side: str = "",
        max_cancel: int = 20,
    ) -> int:
        limit = int(max(0, max_cancel))
        if limit <= 0:
            return 0
        rows = self.list_pending_stop_loss_orders(inst_id=inst_id, side=side)
        canceled = 0
        for row in rows:
            if canceled >= limit:
                break
            row_algo_id = str(row.get("attachAlgoId", row.get("ordId", "")) or "").strip()
            row_algo_cl_id = str(row.get("attachAlgoClOrdId", row.get("clOrdId", "")) or "").strip()
            try:
                if self._private_api_mode() == "papi":
                    self._cancel_papi_conditional_order(
                        inst_id=inst_id,
                        strategy_id=row_algo_id,
                        client_strategy_id=row_algo_cl_id,
                    )
                else:
                    self.cancel_order(inst_id=inst_id, ord_id=row_algo_id, cl_ord_id=row_algo_cl_id)
            except Exception as e:
                log(f"[{inst_id}] Binance pending stop cleanup warning: {e}", level="WARN")
                break
            canceled += 1
        return canceled

    def cleanup_pending_stop_loss_orders(
        self,
        *,
        inst_id: str,
        side: str = "",
        keep_algo_id: str = "",
        keep_algo_cl_ord_id: str = "",
        max_cancel: int = 1,
    ) -> int:
        limit = int(max(0, max_cancel))
        if limit <= 0:
            return 0
        rows = self.list_pending_stop_loss_orders(inst_id=inst_id, side=side)
        if len(rows) <= 1:
            return 0
        keep_algo_id_txt = str(keep_algo_id or "").strip()
        keep_algo_cl_id_txt = str(keep_algo_cl_ord_id or "").strip()
        if (not keep_algo_id_txt) and (not keep_algo_cl_id_txt):
            keep_algo_id_txt = str(rows[0].get("attachAlgoId", rows[0].get("ordId", "")) or "").strip()
            keep_algo_cl_id_txt = str(rows[0].get("attachAlgoClOrdId", rows[0].get("clOrdId", "")) or "").strip()
        canceled = 0
        cancel_rows = list(rows)
        cancel_rows.sort(key=self._pending_stop_sort_key)
        for row in cancel_rows:
            if canceled >= limit:
                break
            row_algo_id = str(row.get("attachAlgoId", row.get("ordId", "")) or "").strip()
            row_algo_cl_id = str(row.get("attachAlgoClOrdId", row.get("clOrdId", "")) or "").strip()
            if (keep_algo_id_txt and row_algo_id == keep_algo_id_txt) or (
                keep_algo_cl_id_txt and row_algo_cl_id == keep_algo_cl_id_txt
            ):
                continue
            try:
                if self._private_api_mode() == "papi":
                    self._cancel_papi_conditional_order(
                        inst_id=inst_id,
                        strategy_id=row_algo_id,
                        client_strategy_id=row_algo_cl_id,
                    )
                else:
                    self.cancel_order(inst_id=inst_id, ord_id=row_algo_id, cl_ord_id=row_algo_cl_id)
            except Exception as e:
                log(f"[{inst_id}] Binance duplicate stop cleanup warning: {e}", level="WARN")
                break
            canceled += 1
        return canceled

    def _cancel_papi_conditional_order(
        self,
        *,
        inst_id: str,
        strategy_id: str = "",
        client_strategy_id: str = "",
    ) -> Dict[str, Any]:
        strategy_id_txt = str(strategy_id or "").strip()
        client_strategy_id_txt = str(client_strategy_id or "").strip()
        if not strategy_id_txt and not client_strategy_id_txt:
            raise RuntimeError("strategy_id or client_strategy_id is required")
        symbol = self._resolve_symbol(inst_id)
        params: Dict[str, str] = {"symbol": symbol}
        if strategy_id_txt:
            params["strategyId"] = strategy_id_txt
        if client_strategy_id_txt:
            params["newClientStrategyId"] = client_strategy_id_txt
        if self.cfg.dry_run:
            log(f"[DRY-RUN] binance papi cancel_stop payload={json.dumps(params, ensure_ascii=False)}")
            return {"data": [{"ordId": strategy_id_txt or "DRY_RUN_STOP", "clOrdId": client_strategy_id_txt, "state": "canceled"}]}
        raw = self._request(
            "DELETE",
            "/papi/v1/um/conditional/order",
            params=params,
            signed=True,
            base_url=self._papi_private_base_url,
        )
        if isinstance(raw, dict):
            return {"data": [self._normalize_stop_order_row(raw, fallback_cl_ord_id=client_strategy_id_txt)]}
        return {"data": []}

    def place_stop_loss_order(
        self,
        *,
        inst_id: str,
        side: str,
        stop_price: float,
        cl_ord_id: str = "",
        size: float = 0.0,
    ) -> Dict[str, Any]:
        if stop_price <= 0:
            raise RuntimeError("stop_price must be > 0")
        symbol = self._resolve_symbol(inst_id)
        close_side = self._normalize_close_order_side(side)
        cl_ord_id_txt = str(cl_ord_id or "").strip()
        mode = self._private_api_mode()
        if mode == "papi":
            stop_size = float(size or 0.0)
            if stop_size <= 0:
                pos_state = self.parse_position(self.get_positions(inst_id), "net")
                stop_size = float(pos_state.size or 0.0)
            if stop_size <= 0:
                raise RuntimeError(f"[{inst_id}] Binance PAPI stop order requires positive position size")
            _, stop_size_txt = self.normalize_order_size(inst_id, float(stop_size), reduce_only=True)
            params: Dict[str, str] = {
                "symbol": symbol,
                "side": close_side,
                "strategyType": "STOP_MARKET",
                "quantity": stop_size_txt,
                "reduceOnly": "true",
                "stopPrice": self._normalize_price(inst_id, float(stop_price)),
                "workingType": self._working_type(),
                "priceProtect": "FALSE",
            }
            if cl_ord_id_txt:
                params["newClientStrategyId"] = cl_ord_id_txt
            if self.cfg.dry_run:
                log(f"[DRY-RUN] binance papi place_stop payload={json.dumps(params, ensure_ascii=False)}")
                row = {
                    "ordId": "DRY_RUN_STOP",
                    "clOrdId": cl_ord_id_txt,
                    "state": "live",
                    "attachAlgoId": "DRY_RUN_STOP",
                    "attachAlgoClOrdId": cl_ord_id_txt,
                    "slTriggerPx": self._fmt_price(float(stop_price)),
                    "side": close_side.lower(),
                }
                return {"data": [row]}
            raw = self._request(
                "POST",
                "/papi/v1/um/conditional/order",
                params=params,
                signed=True,
                base_url=self._papi_private_base_url,
            )
            row = self._normalize_stop_order_row(raw, fallback_cl_ord_id=cl_ord_id_txt, fallback_side=close_side)
            row["attachAlgoId"] = str(row.get("attachAlgoId", row.get("ordId", "")) or "").strip()
            row["attachAlgoClOrdId"] = str(row.get("attachAlgoClOrdId", row.get("clOrdId", cl_ord_id_txt)) or cl_ord_id_txt).strip()
            row["slTriggerPx"] = self._fmt_price(float(stop_price))
            if "side" not in row:
                row["side"] = close_side.lower()
            return {"data": [row]}
        params = {
            "symbol": symbol,
            "side": close_side,
            "type": "STOP_MARKET",
            "stopPrice": self._normalize_price(inst_id, float(stop_price)),
            "closePosition": "true",
            "workingType": self._working_type(),
            "priceProtect": "FALSE",
            "newOrderRespType": "ACK",
        }
        if cl_ord_id_txt:
            params["newClientOrderId"] = cl_ord_id_txt
        if self.cfg.dry_run:
            log(f"[DRY-RUN] binance place_stop payload={json.dumps(params, ensure_ascii=False)}")
            row = {
                "ordId": "DRY_RUN_STOP",
                "clOrdId": cl_ord_id_txt,
                "state": "live",
                "attachAlgoId": "DRY_RUN_STOP",
                "attachAlgoClOrdId": cl_ord_id_txt,
                "slTriggerPx": self._fmt_price(float(stop_price)),
                "side": close_side.lower(),
            }
            return {"data": [row]}
        raw = self._request("POST", "/fapi/v1/order", params=params, signed=True, base_url=self._fapi_private_base_url)
        row = self._normalize_order_row(raw, fallback_cl_ord_id=cl_ord_id_txt)
        row["attachAlgoId"] = str(row.get("ordId", "") or "").strip()
        row["attachAlgoClOrdId"] = str(row.get("clOrdId", cl_ord_id_txt) or cl_ord_id_txt).strip()
        row["slTriggerPx"] = self._fmt_price(float(stop_price))
        if "side" not in row:
            row["side"] = close_side.lower()
        return {"data": [row]}

    def _replace_close_position_stop_order(
        self,
        *,
        inst_id: str,
        new_sl_trigger_px: float,
        algo_id: str = "",
        algo_cl_ord_id: str = "",
        size: float = 0.0,
    ) -> Dict[str, Any]:
        if new_sl_trigger_px <= 0:
            raise RuntimeError("new_sl_trigger_px must be > 0")
        algo_id_txt = str(algo_id or "").strip()
        algo_cl_id_txt = str(algo_cl_ord_id or "").strip()
        if not algo_id_txt and not algo_cl_id_txt:
            raise RuntimeError("algo_id or algo_cl_ord_id is required")
        if self._private_api_mode() == "papi":
            current: Dict[str, Any] = {}
            try:
                current = self._get_papi_conditional_order(
                    inst_id=inst_id,
                    strategy_id=algo_id_txt,
                    client_strategy_id=algo_cl_id_txt,
                )
            except Exception:
                current = {}
            close_side = str(current.get("side", "") or "").strip().lower()
            if close_side not in {"buy", "sell"}:
                raise RuntimeError("Binance PAPI stop amend requires existing stop order side")
            use_cl_ord_id = algo_cl_id_txt or str(current.get("attachAlgoClOrdId", current.get("clOrdId", "")) or "").strip()
            use_ord_id = algo_id_txt or str(current.get("attachAlgoId", current.get("ordId", "")) or "").strip()
            try:
                self._cancel_papi_conditional_order(
                    inst_id=inst_id,
                    strategy_id=use_ord_id,
                    client_strategy_id=use_cl_ord_id,
                )
            except Exception as e:
                msg = str(e).lower()
                if (
                    ("unknown order" not in msg)
                    and ("unknown strategy" not in msg)
                    and ("does not exist" not in msg)
                    and ("order not found" not in msg)
                    and ("not found" not in msg)
                ):
                    log(f"[{inst_id}] Binance PAPI stop replace cancel warning: {e}", level="WARN")
            current_qty = float(size or 0.0)
            if current_qty <= 0:
                current_qty = self._to_float(current.get("origQty", current.get("accFillSz", 0.0)))
            if current_qty <= 0:
                pos_state = self.parse_position(self.get_positions(inst_id), "net")
                current_qty = float(pos_state.size or 0.0)
            return self.place_stop_loss_order(
                inst_id=inst_id,
                side=close_side,
                stop_price=float(new_sl_trigger_px),
                cl_ord_id=use_cl_ord_id,
                size=float(current_qty or 0.0),
            )
        current: Dict[str, Any] = {}
        try:
            current = self.get_order(inst_id=inst_id, ord_id=algo_id_txt, cl_ord_id=algo_cl_id_txt)
        except Exception:
            current = {}
        close_side = str(current.get("side", "") or "").strip().lower()
        if close_side not in {"buy", "sell"}:
            raise RuntimeError("Binance stop amend requires existing stop order side")
        use_cl_ord_id = algo_cl_id_txt or str(current.get("clOrdId", "") or "").strip()
        use_ord_id = algo_id_txt or str(current.get("ordId", "") or "").strip()
        try:
            self.cancel_order(inst_id=inst_id, ord_id=use_ord_id, cl_ord_id=use_cl_ord_id)
        except Exception as e:
            msg = str(e).lower()
            if ("unknown order" not in msg) and ("does not exist" not in msg) and ("order not found" not in msg):
                log(f"[{inst_id}] Binance stop replace cancel warning: {e}", level="WARN")
        return self.place_stop_loss_order(
            inst_id=inst_id,
            side=close_side,
            stop_price=float(new_sl_trigger_px),
            cl_ord_id=use_cl_ord_id,
            size=float(size or 0.0),
        )
    def amend_order_attached_sl(
        self,
        *,
        inst_id: str,
        ord_id: str,
        new_sl_trigger_px: float,
        attach_algo_id: str = "",
        attach_algo_cl_ord_id: str = "",
        cl_ord_id: str = "",
        size: float = 0.0,
    ) -> Dict[str, Any]:
        return self._replace_close_position_stop_order(
            inst_id=inst_id,
            new_sl_trigger_px=new_sl_trigger_px,
            algo_id=attach_algo_id,
            algo_cl_ord_id=attach_algo_cl_ord_id,
            size=float(size or 0.0),
        )
    def amend_algo_sl(
        self,
        *,
        inst_id: str,
        new_sl_trigger_px: float,
        algo_id: str = "",
        algo_cl_ord_id: str = "",
        size: float = 0.0,
    ) -> Dict[str, Any]:
        return self._replace_close_position_stop_order(
            inst_id=inst_id,
            new_sl_trigger_px=new_sl_trigger_px,
            algo_id=algo_id,
            algo_cl_ord_id=algo_cl_ord_id,
            size=float(size or 0.0),
        )
