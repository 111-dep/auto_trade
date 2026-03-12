from __future__ import annotations

from .binance_um_client import BinanceUMClient
from .models import Config
from .okx_client import OKXClient


def create_client(cfg: Config):
    provider = str(getattr(cfg, "exchange_provider", "okx") or "okx").strip().lower()
    if provider == "okx":
        return OKXClient(cfg)
    if provider == "binance":
        return BinanceUMClient(cfg)
    raise ValueError(f"Unsupported EXCHANGE_PROVIDER: {provider}")
