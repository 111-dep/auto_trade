from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .signals import compute_alert_targets, select_signal_candidate


@dataclass(frozen=True)
class EntryDecision:
    side: str
    level: int
    entry: float
    stop: float
    risk: float
    tp1: float
    tp2: float


def resolve_entry_decision(
    sig: Mapping[str, Any],
    *,
    max_level: int,
    min_level: int = 1,
    exact_level: int = 0,
    tp1_r: float,
    tp2_r: float,
    tp1_only: bool = False,
) -> Optional[EntryDecision]:
    pick = select_signal_candidate(
        sig,
        max_level=max_level,
        min_level=min_level,
        exact_level=exact_level,
    )
    if not pick:
        return None

    side, level, stop = pick
    entry = float(sig["close"])
    stop = float(stop)

    risk, tp1, tp2 = compute_alert_targets(
        side=side,
        entry_price=entry,
        stop_price=stop,
        tp1_r=tp1_r,
        tp2_r=tp2_r,
    )
    if tp1_only:
        tp2 = tp1
    if risk <= 0:
        return None

    return EntryDecision(
        side=side,
        level=int(level),
        entry=entry,
        stop=stop,
        risk=float(risk),
        tp1=float(tp1),
        tp2=float(tp2),
    )
