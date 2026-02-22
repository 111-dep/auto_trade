from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import json
import math
import os
import time
from decimal import Decimal, ROUND_DOWN
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from .common import floor_to_step, infer_inst_type, log, parse_inst_parts, round_size
from .models import Candle, Config, PositionState


class OKXClient:
    def __init__(self, config: Config):
        self.cfg = config
        self._instrument_cache: Dict[str, Dict[str, Any]] = {}
        self._leverage_ready: Dict[str, bool] = {}
        self._force_pos_side: Dict[str, bool] = {}
        self._equity_cache_ts: float = 0.0
        self._equity_cache_value: Optional[float] = None
        self._equity_peak: Optional[float] = None
        self._http_max_retries = max(0, int(os.getenv("OKX_HTTP_MAX_RETRIES", "2")))
        self._http_retry_base_seconds = max(0.05, float(os.getenv("OKX_HTTP_RETRY_BASE_SECONDS", "0.35")))
        self._http_retry_max_seconds = max(
            self._http_retry_base_seconds,
            float(os.getenv("OKX_HTTP_RETRY_MAX_SECONDS", "3.0")),
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
    def _is_pos_side_error(exc: Exception) -> bool:
        txt = str(exc)
        return ("51000" in txt) and ("posSide" in txt or "posside" in txt.lower())

    @staticmethod
    def _is_transient_http_code(code: int) -> bool:
        return int(code) in {408, 409, 425, 429, 500, 502, 503, 504}

    @staticmethod
    def _is_retryable_okx_code(code: str) -> bool:
        c = str(code or "").strip()
        return c in {"50011", "50040", "50061", "51149"}

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
            "name or service not known",
            "temporary failure",
        )
        return any(tok in low for tok in tokens)

    @staticmethod
    def _is_transient_exception(exc: Exception) -> bool:
        if isinstance(exc, TimeoutError):
            return True
        if isinstance(exc, socket.timeout):
            return True
        if isinstance(exc, urllib.error.URLError):
            return OKXClient._is_transient_error_text(str(exc.reason))
        return OKXClient._is_transient_error_text(str(exc))

    def _retry_backoff_seconds(self, attempt_idx: int) -> float:
        sec = self._http_retry_base_seconds * (2 ** max(0, int(attempt_idx)))
        return min(self._http_retry_max_seconds, max(0.0, sec))

    @staticmethod
    def _is_duplicate_cl_ord_id_error(exc: Exception) -> bool:
        txt = str(exc or "")
        low = txt.lower()
        if "clordid" not in low:
            return False
        duplicate_tokens = ("exist", "exists", "repeated", "duplicate", "duplicated", "already")
        if any(tok in low for tok in duplicate_tokens):
            return True
        # Keep common code-style fallback broad for exchange text variations.
        return ("51000" in low) or ("51603" in low)

    @staticmethod
    def _fmt_price(v: float) -> str:
        txt = f"{float(v):.12f}".rstrip("0").rstrip(".")
        return txt if txt else "0"

    @staticmethod
    def _infer_pos_side(side: str, reduce_only: bool) -> str:
        s = side.strip().lower()
        if s == "buy":
            return "short" if reduce_only else "long"
        if s == "sell":
            return "long" if reduce_only else "short"
        raise RuntimeError(f"Unsupported side for posSide inference: {side}")

    def mark_force_pos_side(self, inst_id: str) -> None:
        self._force_pos_side[inst_id] = True

    def use_pos_side(self, inst_id: str) -> bool:
        return self.cfg.pos_mode == "long_short" or self._force_pos_side.get(inst_id, False)

    def _timestamp(self) -> str:
        return dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    def _sign(self, prehash: str) -> str:
        digest = hmac.new(
            self.cfg.secret_key.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        private: bool = False,
    ) -> Dict[str, Any]:
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)
        request_path = path + query
        payload = json.dumps(body, separators=(",", ":")) if body else ""
        url = self.cfg.base_url + request_path
        max_retry = int(max(0, self._http_max_retries))

        for attempt in range(max_retry + 1):
            headers = {"Content-Type": "application/json"}
            headers["User-Agent"] = self.cfg.user_agent
            headers["Accept"] = "application/json, text/plain, */*"
            if self.cfg.paper:
                headers["x-simulated-trading"] = "1"

            if private:
                if not self.cfg.api_key or not self.cfg.secret_key or not self.cfg.passphrase:
                    raise RuntimeError("Missing OKX credentials in env")
                ts = self._timestamp()
                prehash = f"{ts}{method.upper()}{request_path}{payload}"
                headers.update(
                    {
                        "OK-ACCESS-KEY": self.cfg.api_key,
                        "OK-ACCESS-SIGN": self._sign(prehash),
                        "OK-ACCESS-TIMESTAMP": ts,
                        "OK-ACCESS-PASSPHRASE": self.cfg.passphrase,
                    }
                )

            req = urllib.request.Request(
                url=url,
                data=payload.encode("utf-8") if payload else None,
                headers=headers,
                method=method.upper(),
            )

            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    raw = resp.read().decode("utf-8")
            except urllib.error.HTTPError as e:
                body_txt = ""
                try:
                    body_txt = e.read().decode("utf-8", errors="ignore")
                except Exception:
                    body_txt = ""
                snippet = body_txt[:240].replace("\n", " ").strip()
                err = RuntimeError(
                    f"HTTP request failed: {method} {request_path} | HTTP {e.code} {e.reason}"
                    + (f" | body={snippet}" if snippet else "")
                )
                if attempt < max_retry and self._is_transient_http_code(int(getattr(e, "code", 0) or 0)):
                    wait_s = self._retry_backoff_seconds(attempt)
                    log(
                        f"HTTP transient error retry {attempt + 1}/{max_retry} "
                        f"for {method} {request_path} after {wait_s:.2f}s (HTTP {e.code}).",
                        level="WARN",
                    )
                    time.sleep(wait_s)
                    continue
                raise err from e
            except Exception as e:
                err = RuntimeError(f"HTTP request failed: {method} {request_path} | {e}")
                if attempt < max_retry and self._is_transient_exception(e):
                    wait_s = self._retry_backoff_seconds(attempt)
                    log(
                        f"HTTP transient exception retry {attempt + 1}/{max_retry} "
                        f"for {method} {request_path} after {wait_s:.2f}s.",
                        level="WARN",
                    )
                    time.sleep(wait_s)
                    continue
                raise err from e

            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid JSON response: {raw[:500]}") from e

            if data.get("code") == "0":
                return data

            code = str(data.get("code") or "")
            msg = str(data.get("msg") or "")
            if attempt < max_retry and self._is_retryable_okx_code(code):
                wait_s = self._retry_backoff_seconds(attempt)
                log(
                    f"OKX retryable code={code} msg={msg} retry {attempt + 1}/{max_retry} "
                    f"for {method} {request_path} after {wait_s:.2f}s.",
                    level="WARN",
                )
                time.sleep(wait_s)
                continue
            raise RuntimeError(f"OKX API error: code={code} msg={msg} data={data.get('data')}")

        raise RuntimeError(f"HTTP request failed: exhausted retries for {method} {request_path}")

    def get_candles(self, inst_id: str, bar: str, limit: int, include_unconfirmed: bool = False) -> List[Candle]:
        data = self._request(
            "GET",
            "/api/v5/market/candles",
            params={"instId": inst_id, "bar": bar, "limit": str(limit)},
            private=False,
        )
        out: List[Candle] = []
        for row in data.get("data", []):
            # [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
            try:
                out.append(
                    Candle(
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        confirm=(str(row[8]) == "1"),
                    )
                )
            except Exception:
                continue
        out.sort(key=lambda c: c.ts_ms)
        if include_unconfirmed:
            return out
        return [c for c in out if c.confirm]

    def get_candles_history(self, inst_id: str, bar: str, total_limit: int) -> List[Candle]:
        need = max(1, int(total_limit))
        cached = self._load_history_cache(inst_id, bar, need)
        if cached is not None and len(cached) >= need:
            return cached[-need:]

        page_limit = 300
        seen: Dict[int, Candle] = {}
        if cached:
            for c in cached:
                seen[c.ts_ms] = c
        cursor_after: Optional[int] = None
        stall = 0

        while len(seen) < need:
            params: Dict[str, str] = {"instId": inst_id, "bar": bar, "limit": str(page_limit)}
            if cursor_after is not None:
                params["after"] = str(cursor_after)
            data = self._request("GET", "/api/v5/market/history-candles", params=params, private=False)
            rows = data.get("data", [])
            if not rows:
                break

            added = 0
            oldest_ts: Optional[int] = None
            for row in rows:
                try:
                    c = Candle(
                        ts_ms=int(row[0]),
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        confirm=(str(row[8]) == "1"),
                    )
                except Exception:
                    continue
                if not c.confirm:
                    continue
                if c.ts_ms not in seen:
                    seen[c.ts_ms] = c
                    added += 1
                if oldest_ts is None or c.ts_ms < oldest_ts:
                    oldest_ts = c.ts_ms

            if oldest_ts is None:
                break
            next_cursor = oldest_ts - 1
            if cursor_after is not None and next_cursor >= cursor_after:
                stall += 1
                if stall >= 2:
                    break
            else:
                stall = 0
            cursor_after = next_cursor

            if len(rows) < page_limit:
                break

        out = sorted(seen.values(), key=lambda x: x.ts_ms)
        if len(out) > need:
            out = out[-need:]
        self._save_history_cache(inst_id, bar, out)
        return out

    def get_positions(self, inst_id: str) -> List[Dict[str, Any]]:
        data = self._request(
            "GET",
            "/api/v5/account/positions",
            params={"instId": inst_id},
            private=True,
        )
        return data.get("data", [])

    def _to_float(self, v: Any) -> float:
        try:
            return float(v)
        except Exception:
            return 0.0

    def _extract_account_equity(self, row: Dict[str, Any], prefer_ccy: str) -> float:
        ccy = (prefer_ccy or "").strip().upper()
        details = row.get("details")
        if ccy and isinstance(details, list):
            for d in details:
                if str(d.get("ccy", "")).strip().upper() != ccy:
                    continue
                for key in ("eq", "cashBal", "availEq"):
                    val = self._to_float(d.get(key))
                    if val > 0:
                        return val

        total_eq = self._to_float(row.get("totalEq"))
        if total_eq > 0:
            return total_eq

        if isinstance(details, list):
            summed = 0.0
            for d in details:
                v = self._to_float(d.get("eq"))
                if v > 0:
                    summed += v
            if summed > 0:
                return summed
        return 0.0

    def get_account_equity(self, force_refresh: bool = False) -> Optional[float]:
        ttl = max(0, int(self.cfg.compound_cache_seconds))
        now = time.time()
        if (not force_refresh) and ttl > 0 and self._equity_cache_value is not None:
            if now - self._equity_cache_ts <= ttl:
                return self._equity_cache_value

        try:
            ccy = (self.cfg.compound_balance_ccy or "").strip().upper()
            params = {"ccy": ccy} if ccy else None
            data = self._request("GET", "/api/v5/account/balance", params=params, private=True)
            rows = data.get("data", [])
            if not rows:
                return None
            equity = self._extract_account_equity(rows[0], ccy)
            if equity <= 0:
                return None
            self._equity_cache_value = equity
            self._equity_cache_ts = now
            return equity
        except Exception as e:
            log(f"[Account] equity fetch failed: {e}")
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

        inst_type = infer_inst_type(inst_id)
        data = self._request(
            "GET",
            "/api/v5/public/instruments",
            params={"instType": inst_type, "instId": inst_id},
            private=False,
        )
        rows = data.get("data", [])
        if not rows:
            raise RuntimeError(f"Instrument not found: {inst_id}")
        info = rows[0]
        self._instrument_cache[inst_id] = info
        return info

    @staticmethod
    def _decimal_text(v: Decimal) -> str:
        txt = format(v, "f").rstrip("0").rstrip(".")
        if not txt:
            return "0"
        if txt == "-0":
            return "0"
        return txt

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
        if self.cfg.td_mode != "isolated":
            return

        cache_key = f"{inst_id}:{self.cfg.td_mode}:{self.cfg.leverage}:{pos_side or 'net'}"
        if self._leverage_ready.get(cache_key):
            return

        body: Dict[str, Any] = {
            "instId": inst_id,
            "mgnMode": self.cfg.td_mode,
            "lever": str(self.cfg.leverage),
        }
        if pos_side:
            body["posSide"] = pos_side

        if self.cfg.dry_run:
            log(f"[DRY-RUN] set_leverage payload={json.dumps(body, ensure_ascii=False)}")
            self._leverage_ready[cache_key] = True
            return
        try:
            self._request("POST", "/api/v5/account/set-leverage", body=body, private=True)
            self._leverage_ready[cache_key] = True
            log(
                "Leverage set: instId={} mgnMode={} lever={}{}".format(
                    inst_id,
                    self.cfg.td_mode,
                    self.cfg.leverage,
                    f" posSide={pos_side}" if pos_side else "",
                )
            )
            return
        except Exception as e:
            if pos_side or (not self._is_pos_side_error(e)):
                raise
            side = (entry_side or "").strip().lower()
            if side not in {"long", "short"}:
                raise

            retry_pos_side = "long" if side == "long" else "short"
            retry_key = f"{inst_id}:{self.cfg.td_mode}:{self.cfg.leverage}:{retry_pos_side}"
            if self._leverage_ready.get(retry_key):
                self.mark_force_pos_side(inst_id)
                return

            retry_body = dict(body)
            retry_body["posSide"] = retry_pos_side
            self._request("POST", "/api/v5/account/set-leverage", body=retry_body, private=True)
            self._leverage_ready[retry_key] = True
            self.mark_force_pos_side(inst_id)
            log(
                f"[{inst_id}] Leverage set with posSide fallback: mgnMode={self.cfg.td_mode} "
                f"lever={self.cfg.leverage} posSide={retry_pos_side}"
            )
            return

    def place_order(
        self,
        inst_id: str,
        side: str,
        sz: float,
        pos_side: Optional[str] = None,
        reduce_only: bool = False,
        attach_algo_ords: Optional[List[Dict[str, Any]]] = None,
        cl_ord_id: str = "",
    ) -> Dict[str, Any]:
        effective_pos_side = pos_side
        if not effective_pos_side and self.use_pos_side(inst_id):
            effective_pos_side = self._infer_pos_side(side, reduce_only)

        cl_ord_id_txt = str(cl_ord_id or "").strip()
        normalized_sz, normalized_sz_txt = self.normalize_order_size(inst_id, sz, reduce_only=reduce_only)
        if abs(float(sz) - normalized_sz) > 1e-12:
            log(
                f"[{inst_id}] Size normalized by lot/min rule: raw={round_size(float(sz))} -> final={normalized_sz_txt}"
            )
        body: Dict[str, Any] = {
            "instId": inst_id,
            "tdMode": self.cfg.td_mode,
            "side": side,
            "ordType": "market",
            "sz": normalized_sz_txt,
        }
        if effective_pos_side:
            body["posSide"] = effective_pos_side
        if reduce_only:
            body["reduceOnly"] = "true"
        if attach_algo_ords:
            body["attachAlgoOrds"] = attach_algo_ords
        if cl_ord_id_txt:
            body["clOrdId"] = cl_ord_id_txt

        if self.cfg.dry_run:
            log(f"[DRY-RUN] place_order payload={json.dumps(body, ensure_ascii=False)}")
            row: Dict[str, Any] = {"ordId": "DRY_RUN"}
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
                row = self.get_order(inst_id=inst_id, cl_ord_id=cl_ord_id_txt)
            except Exception:
                return None
            if not isinstance(row, dict) or not row:
                return None
            ord_id = str(row.get("ordId", "") or "").strip()
            if not ord_id:
                return None
            log(
                f"[{inst_id}] Idempotency hit: clOrdId={cl_ord_id_txt} already exists, reuse ordId={ord_id}."
            )
            return {"data": [row]}

        try:
            data = self._request("POST", "/api/v5/trade/order", body=body, private=True)
            return data
        except Exception as e:
            recovered = _recover_by_cl_ord_id(e)
            if recovered is not None:
                return recovered
            if effective_pos_side or not self._is_pos_side_error(e):
                raise

            inferred_pos_side = self._infer_pos_side(side, reduce_only)
            self.mark_force_pos_side(inst_id)
            log(
                f"[{inst_id}] Detected posSide requirement, retry with posSide={inferred_pos_side}. "
                "Set OKX_POS_MODE=long_short to match account mode."
            )

            if self.cfg.td_mode == "isolated":
                try:
                    self.ensure_leverage(inst_id, inferred_pos_side)
                except Exception as lev_err:
                    log(f"[{inst_id}] Leverage retry warning: {lev_err}")

            retry_body = dict(body)
            retry_body["posSide"] = inferred_pos_side
            try:
                data = self._request("POST", "/api/v5/trade/order", body=retry_body, private=True)
                return data
            except Exception as e2:
                recovered = _recover_by_cl_ord_id(e2)
                if recovered is not None:
                    return recovered
                raise

    def build_attach_tpsl_ords(self, tp_price: float, sl_price: float) -> List[Dict[str, str]]:
        if tp_price <= 0 or sl_price <= 0:
            return []
        px_type = self.cfg.attach_tpsl_trigger_px_type
        ord_item: Dict[str, str] = {
            "tpTriggerPx": self._fmt_price(tp_price),
            "tpOrdPx": "-1",
            "slTriggerPx": self._fmt_price(sl_price),
            "slOrdPx": "-1",
            "tpTriggerPxType": px_type,
            "slTriggerPxType": px_type,
        }
        return [ord_item]

    def get_order(self, inst_id: str, ord_id: str = "", cl_ord_id: str = "") -> Dict[str, Any]:
        params: Dict[str, str] = {"instId": inst_id}
        if str(ord_id or "").strip():
            params["ordId"] = str(ord_id).strip()
        if str(cl_ord_id or "").strip():
            params["clOrdId"] = str(cl_ord_id).strip()
        data = self._request("GET", "/api/v5/trade/order", params=params, private=True)
        rows = data.get("data", [])
        if isinstance(rows, list) and rows and isinstance(rows[0], dict):
            return rows[0]
        return {}

    @staticmethod
    def _extract_first_attach_algo(order_row: Dict[str, Any]) -> Dict[str, Any]:
        attach = order_row.get("attachAlgoOrds")
        if isinstance(attach, list) and attach and isinstance(attach[0], dict):
            return dict(attach[0])
        return {}

    def amend_order_attached_sl(
        self,
        *,
        inst_id: str,
        ord_id: str,
        new_sl_trigger_px: float,
        attach_algo_id: str = "",
        attach_algo_cl_ord_id: str = "",
        cl_ord_id: str = "",
    ) -> Dict[str, Any]:
        if new_sl_trigger_px <= 0:
            raise RuntimeError("new_sl_trigger_px must be > 0")
        ord_id_txt = str(ord_id or "").strip()
        cl_ord_txt = str(cl_ord_id or "").strip()
        if not ord_id_txt and not cl_ord_txt:
            raise RuntimeError("ord_id or cl_ord_id is required for amend_order_attached_sl")

        algo_id = str(attach_algo_id or "").strip()
        algo_cl_id = str(attach_algo_cl_ord_id or "").strip()
        if not algo_id and not algo_cl_id:
            row = self.get_order(inst_id=inst_id, ord_id=ord_id_txt, cl_ord_id=cl_ord_txt)
            first = self._extract_first_attach_algo(row)
            algo_id = str(first.get("attachAlgoId", "") or "").strip()
            algo_cl_id = str(first.get("attachAlgoClOrdId", "") or "").strip()
        if not algo_id and not algo_cl_id:
            raise RuntimeError("No attach algo id available for amend-order")

        item: Dict[str, str] = {
            "newSlTriggerPx": self._fmt_price(float(new_sl_trigger_px)),
            "newSlOrdPx": "-1",
            "newSlTriggerPxType": self.cfg.attach_tpsl_trigger_px_type,
        }
        if algo_id:
            item["attachAlgoId"] = algo_id
        if algo_cl_id:
            item["attachAlgoClOrdId"] = algo_cl_id

        body: Dict[str, Any] = {
            "instId": inst_id,
            "attachAlgoOrds": [item],
        }
        if ord_id_txt:
            body["ordId"] = ord_id_txt
        if cl_ord_txt:
            body["clOrdId"] = cl_ord_txt

        if self.cfg.dry_run:
            log(f"[DRY-RUN] amend_order_attached_sl payload={json.dumps(body, ensure_ascii=False)}")
            return {"data": [{"sCode": "0", "sMsg": "", "reqId": "DRY_RUN"}]}

        return self._request("POST", "/api/v5/trade/amend-order", body=body, private=True)


def parse_position(rows: List[Dict[str, Any]], pos_mode: str) -> PositionState:
    if not rows:
        return PositionState("flat", 0.0)

    if pos_mode == "net":
        long_sz = 0.0
        short_sz = 0.0
        has_side_rows = False
        for r in rows:
            pos_side = str(r.get("posSide", "")).strip().lower()
            if pos_side not in {"long", "short"}:
                continue
            has_side_rows = True
            try:
                pos = abs(float(r.get("pos", "0") or "0"))
            except Exception:
                pos = 0.0
            if pos_side == "long":
                long_sz += pos
            else:
                short_sz += pos

        if has_side_rows:
            if long_sz > 0 and short_sz > 0:
                return PositionState("mixed", max(long_sz, short_sz))
            if long_sz > 0:
                return PositionState("long", long_sz)
            if short_sz > 0:
                return PositionState("short", short_sz)
            return PositionState("flat", 0.0)

        net = 0.0
        for r in rows:
            try:
                net += float(r.get("pos", "0") or "0")
            except Exception:
                continue
        if net > 0:
            return PositionState("long", abs(net))
        if net < 0:
            return PositionState("short", abs(net))
        return PositionState("flat", 0.0)

    long_sz = 0.0
    short_sz = 0.0
    for r in rows:
        pos_side = str(r.get("posSide", "")).lower()
        try:
            pos = abs(float(r.get("pos", "0") or "0"))
        except Exception:
            pos = 0.0
        if pos_side == "long":
            long_sz += pos
        elif pos_side == "short":
            short_sz += pos

    if long_sz > 0 and short_sz > 0:
        return PositionState("mixed", max(long_sz, short_sz))
    if long_sz > 0:
        return PositionState("long", long_sz)
    if short_sz > 0:
        return PositionState("short", short_sz)
    return PositionState("flat", 0.0)


def split_positions_by_mgn_mode(
    rows: List[Dict[str, Any]],
    td_mode: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    target = (td_mode or "").strip().lower()
    if not target:
        return list(rows), []

    matched: List[Dict[str, Any]] = []
    foreign: List[Dict[str, Any]] = []
    for r in rows:
        mode = str(r.get("mgnMode", "")).strip().lower()
        if (not mode) or mode == target:
            matched.append(r)
        else:
            foreign.append(r)
    return matched, foreign


def calc_order_size(
    client: OKXClient,
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

    info = client.get_instrument(inst_id)
    try:
        ct_val = float(info.get("ctVal", "0") or "0")
    except Exception:
        ct_val = 0.0
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

    if ct_val <= 0:
        raise RuntimeError(f"Invalid ctVal for {inst_id}: {info.get('ctVal')}")

    _, quote, _ = parse_inst_parts(inst_id)
    quote = quote.upper()
    ct_val_ccy = str(info.get("ctValCcy", "")).upper()

    # For linear contracts (ctVal in base ccy), contract notional in quote = ctVal * price.
    # For quote-valued ctVal, contract notional in quote = ctVal.
    if quote and ct_val_ccy == quote:
        contract_notional_quote = ct_val
    else:
        contract_notional_quote = ct_val * entry_price

    if contract_notional_quote <= 0:
        raise RuntimeError("Computed contract notional <= 0")

    def size_by_margin() -> float:
        margin_usdt = client.resolve_margin_usdt()
        if margin_usdt <= 0:
            raise RuntimeError("Resolved margin must be > 0 when OKX_SIZING_MODE=margin")

        target_notional = margin_usdt * cfg.leverage
        raw_sz = target_notional / contract_notional_quote
        sized = floor_to_step(raw_sz, lot_sz)
        if sized < min_sz:
            sized = min_sz

        log(
            "Sizing: mode=margin margin={} base_margin={} lever={} target_notional={} contract_notional={} raw_sz={} final_sz={}".format(
                round(margin_usdt, 6),
                round(cfg.margin_usdt, 6),
                cfg.leverage,
                round(target_notional, 6),
                round(contract_notional_quote, 6),
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
            f"[{inst_id}] Sizing: STRAT_RISK_FRAC={round(risk_frac, 6)} ignored because "
            f"OKX_MARGIN_USDT={round(margin_cfg, 6)} > 0. Using margin sizing."
        )

    if not risk_mode_enabled:
        if risk_frac <= 0 and margin_cfg <= 0:
            raise RuntimeError("OKX_MARGIN_USDT must be > 0 when STRAT_RISK_FRAC is disabled.")
        return size_by_margin()

    stop_px = float(stop_price or 0.0)
    if stop_px <= 0:
        log(f"[{inst_id}] Sizing: STRAT_RISK_FRAC enabled but stop<=0, fallback to margin sizing.")
        return size_by_margin()

    stop_delta = abs(float(entry_price) - stop_px)
    if stop_delta <= 0:
        log(f"[{inst_id}] Sizing: STRAT_RISK_FRAC enabled but stop distance=0, fallback to margin sizing.")
        return size_by_margin()

    # Risk-per-contract should align with compute_trade_pnl_usdt() model in runtime.
    if quote and ct_val_ccy == quote:
        loss_per_contract = (stop_delta / entry_price) * ct_val
    else:
        loss_per_contract = stop_delta * ct_val

    if loss_per_contract <= 0:
        log(f"[{inst_id}] Sizing: STRAT_RISK_FRAC loss_per_contract<=0, fallback to margin sizing.")
        return size_by_margin()

    equity = client.get_account_equity()
    if equity is None or equity <= 0:
        fallback_eq = float(getattr(cfg, "compound_base_equity", 0.0) or 0.0)
        if fallback_eq <= 0:
            log(f"[{inst_id}] Sizing: STRAT_RISK_FRAC equity unavailable, fallback to margin sizing.")
            return size_by_margin()
        equity = fallback_eq
        log(
            f"[{inst_id}] Sizing: STRAT_RISK_FRAC equity unavailable, use fallback compound_base_equity={round(equity, 6)}."
        )

    risk_usdt = float(equity) * risk_frac
    if risk_usdt <= 0:
        log(f"[{inst_id}] Sizing: STRAT_RISK_FRAC risk_usdt<=0, fallback to margin sizing.")
        return size_by_margin()

    raw_sz = risk_usdt / loss_per_contract
    sized = floor_to_step(raw_sz, lot_sz)
    if sized < min_sz:
        sized = min_sz

    est_notional = sized * contract_notional_quote
    est_margin = est_notional / cfg.leverage if cfg.leverage > 0 else est_notional
    stop_pct = stop_delta / entry_price
    max_margin_frac = float(getattr(cfg.params, "risk_max_margin_frac", 0.0) or 0.0)
    max_margin_allowed = float(equity) * max_margin_frac if max_margin_frac > 0 else 0.0
    side_txt = str(entry_side or "").strip().lower() or "-"
    log(
        "Sizing: mode=risk risk_frac={} side={} equity={} risk_usdt={} stop_pct={:.5f} "
        "loss_per_contract={} lever={} est_margin={} est_notional={} max_margin_frac={} max_margin={} raw_sz={} final_sz={}".format(
            round(risk_frac, 6),
            side_txt,
            round(float(equity), 6),
            round(risk_usdt, 6),
            stop_pct,
            round(loss_per_contract, 8),
            cfg.leverage,
            round(est_margin, 6),
            round(est_notional, 6),
            round(max_margin_frac, 6),
            round(max_margin_allowed, 6),
            round(raw_sz, 6),
            round_size(sized),
        )
    )
    if max_margin_frac > 0 and est_margin > max_margin_allowed:
        raise RuntimeError(
            f"[{inst_id}] Risk guard blocked order: estimated margin {round(est_margin, 6)} "
            f"> cap {round(max_margin_allowed, 6)} (equity={round(float(equity), 6)}, "
            f"cap_frac={round(max_margin_frac, 6)})."
        )
    if est_margin > max(float(equity), 0.0):
        log(
            f"[{inst_id}] Sizing: risk-based estimated margin ({round(est_margin, 6)}) > equity ({round(float(equity), 6)}).",
            level="WARN",
        )

    return sized
