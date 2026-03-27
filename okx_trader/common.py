from __future__ import annotations

import datetime as dt
import hashlib
import math
import os
from typing import List, Optional, Tuple

_LOG_LEVEL_MAP = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "ERROR": 40,
}
_CURRENT_LOG_LEVEL = _LOG_LEVEL_MAP["INFO"]


def set_log_level(level: str) -> None:
    global _CURRENT_LOG_LEVEL
    key = str(level or "INFO").strip().upper()
    _CURRENT_LOG_LEVEL = _LOG_LEVEL_MAP.get(key, _LOG_LEVEL_MAP["INFO"])


def _normalize_log_level(level: str) -> str:
    key = str(level or "INFO").strip().upper()
    if key == "WARNING":
        key = "WARN"
    return key if key in _LOG_LEVEL_MAP else "INFO"


def log(msg: str, level: str = "INFO") -> None:
    lvl = _normalize_log_level(level)
    if _LOG_LEVEL_MAP[lvl] < _CURRENT_LOG_LEVEL:
        return
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if lvl == "INFO":
        # Keep INFO formatting backward-compatible for existing log parsers.
        print(f"[{now}] {msg}", flush=True)
    else:
        print(f"[{now}] [{lvl}] {msg}", flush=True)


def format_duration(seconds: float) -> str:
    sec = max(0.0, float(seconds))
    if sec < 60:
        return f"{sec:.1f}s"
    if sec < 3600:
        return f"{sec / 60.0:.1f}m"
    return f"{sec / 3600.0:.2f}h"


def make_progress_bar(done: int, total: int, width: int = 20) -> str:
    t = max(1, int(total))
    d = max(0, min(int(done), t))
    fill = int(round((d / t) * width))
    fill = max(0, min(fill, width))
    return "[" + ("#" * fill) + ("." * (width - fill)) + "]"


def truncate_text(text: str, limit: int = 3800) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n...（消息过长，已截断）"
    keep = max(0, limit - len(suffix))
    return text[:keep] + suffix


def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_inst_ids(value: str) -> List[str]:
    if value is None:
        return []
    out: List[str] = []
    for part in value.split(","):
        item = part.strip().upper()
        if item:
            out.append(item)
    return out


def parse_csv(value: str) -> List[str]:
    if value is None:
        return []
    out: List[str] = []
    for part in value.split(","):
        item = part.strip()
        if item:
            out.append(item)
    return out


def parse_backtest_levels(value: str) -> List[int]:
    out: List[int] = []
    seen: set = set()
    for raw in parse_csv(value):
        try:
            lv = int(raw)
        except Exception:
            continue
        if lv < 1 or lv > 3:
            continue
        if lv in seen:
            continue
        seen.add(lv)
        out.append(lv)
    return out


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value


def apply_backtest_env_overrides() -> List[str]:
    changes: List[str] = []

    ttl_raw = str(os.getenv("OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS", "") or "").strip()
    if ttl_raw:
        try:
            ttl = max(0, int(float(ttl_raw)))
        except Exception:
            log(
                f"Invalid OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS={ttl_raw!r}, ignore backtest TTL override.",
                level="WARN",
            )
        else:
            prev = str(os.getenv("OKX_HISTORY_CACHE_TTL_SECONDS", "") or "").strip()
            os.environ["OKX_HISTORY_CACHE_TTL_SECONDS"] = str(ttl)
            changes.append(f"history_cache_ttl={prev or '-'}->{ttl}")

    return changes


def resolve_backtest_live_window_signals(cli_value: Optional[bool] = None) -> bool:
    if cli_value is not None:
        return bool(cli_value)
    raw = str(os.getenv("OKX_BACKTEST_LIVE_WINDOW_SIGNALS", "") or "").strip()
    if raw:
        return parse_bool(raw, True)
    return True


def bar_to_seconds(bar: str) -> int:
    s = bar.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60
    if s.endswith("h"):
        return int(s[:-1]) * 3600
    if s.endswith("d"):
        return int(s[:-1]) * 86400
    if s.endswith("w"):
        return int(s[:-1]) * 7 * 86400
    raise ValueError(f"Unsupported bar format: {bar}")


def round_size(sz: float) -> str:
    if sz <= 0:
        raise ValueError("Order size must be > 0")
    txt = f"{sz:.8f}".rstrip("0").rstrip(".")
    return txt if txt else "0"


def infer_price_decimals(entry: float, stop: float, min_decimals: int = 2, max_decimals: int = 8) -> int:
    ref = max(abs(float(entry)), abs(float(stop)))
    if ref >= 10000:
        base = 2
    elif ref >= 1000:
        base = 3
    elif ref >= 100:
        base = 4
    elif ref >= 1:
        base = 5
    elif ref >= 0.1:
        base = 6
    elif ref >= 0.01:
        base = 7
    else:
        base = 8

    risk = abs(float(entry) - float(stop))
    diff_need = 0
    if risk > 0:
        # Need enough digits to keep entry/stop difference visible in alert text.
        diff_need = max(0, int(math.ceil(-math.log10(risk))) + 1)

    out = max(int(min_decimals), base, diff_need)
    return min(int(max_decimals), out)


def format_price(value: float, decimals: int) -> str:
    d = max(0, min(12, int(decimals)))
    txt = f"{float(value):.{d}f}".rstrip("0").rstrip(".")
    if not txt:
        return "0"
    if txt == "-0":
        return "0"
    return txt


def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step


def parse_inst_parts(inst_id: str) -> Tuple[str, str, str]:
    parts = inst_id.split("-")
    if len(parts) < 3:
        return "", "", ""
    return parts[0], parts[1], parts[2]


def infer_inst_type(inst_id: str) -> str:
    upper = inst_id.upper()
    if upper.endswith("-SWAP"):
        return "SWAP"
    if upper.endswith("-FUTURES"):
        return "FUTURES"
    if upper.endswith("-OPTION"):
        return "OPTION"
    return "MARGIN"


def _to_base36(value: int) -> str:
    v = int(abs(value))
    if v == 0:
        return "0"
    digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    out = []
    while v > 0:
        v, rem = divmod(v, 36)
        out.append(digits[rem])
    return "".join(reversed(out))


def build_client_order_id(
    *,
    prefix: str,
    inst_id: str,
    side: str,
    signal_ts_ms: int,
    salt: str = "",
) -> str:
    """Build a deterministic client-order-id (<=32, alnum only)."""
    pref = "".join(ch for ch in str(prefix or "").upper() if ch.isalnum())[:4] or "OD"
    inst = "".join(ch for ch in str(inst_id or "").upper() if ch.isalnum())[:8] or "INST"
    side_l = str(side or "").strip().lower()
    side_tag = "L" if side_l.startswith("l") else ("S" if side_l.startswith("s") else "N")
    ts_tag = _to_base36(int(signal_ts_ms or 0))[-7:].rjust(7, "0")
    raw = f"{inst_id}|{side}|{int(signal_ts_ms or 0)}|{salt}"
    hash_tag = hashlib.blake2b(raw.encode("utf-8"), digest_size=5).hexdigest().upper()
    out = f"{pref}{side_tag}{inst}{ts_tag}{hash_tag}"
    return out[:32]
