from __future__ import annotations

from .common import build_client_order_id


def build_runtime_order_cl_id(
    *,
    inst_id: str,
    side: str,
    signal_ts_ms: int,
    action_tag: str,
    level: int = 0,
    extra: str = "",
) -> str:
    salt = f"{action_tag}|lv={int(level)}|{extra}"
    return build_client_order_id(
        prefix="AT",
        inst_id=inst_id,
        side=side,
        signal_ts_ms=int(signal_ts_ms),
        salt=salt,
    )
