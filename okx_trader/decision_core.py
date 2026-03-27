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
    entry_idx: int = 0
    include_start_bar: bool = False
    max_hold_bars: int = 0


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
    entry = float(sig.get("entry_price_override", sig["close"]))
    stop = float(stop)

    max_stop_pct = max(0.0, float(sig.get("max_stop_pct", 0.0) or 0.0))
    tp1_r_eff = float(sig.get("tp1_r_override", tp1_r) or tp1_r)
    tp2_r_eff = float(sig.get("tp2_r_override", tp2_r) or tp2_r)
    tp1_only_eff = bool(sig.get("tp1_only_override", tp1_only))
    risk = abs(entry - stop)
    if risk <= 0:
        risk = max(abs(entry) * 0.0005, 1e-8)

    tp1_override = sig.get("tp1_price_override")
    tp2_override = sig.get("tp2_price_override")
    if tp1_override is not None or tp2_override is not None:
        tp1 = float(tp1_override) if tp1_override is not None else float(entry + risk * tp1_r_eff if str(side).upper() == "LONG" else entry - risk * tp1_r_eff)
        tp2 = float(tp2_override) if tp2_override is not None else float(entry + risk * tp2_r_eff if str(side).upper() == "LONG" else entry - risk * tp2_r_eff)
    else:
        risk, tp1, tp2 = compute_alert_targets(
            side=side,
            entry_price=entry,
            stop_price=stop,
            tp1_r=tp1_r_eff,
            tp2_r=tp2_r_eff,
        )
    if max_stop_pct > 0 and entry > 0 and (risk / entry) > max_stop_pct:
        return None
    if tp1_only_eff:
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
        entry_idx=int(sig.get("entry_idx", 0) or 0),
        include_start_bar=bool(sig.get("entry_include_start_bar", False)),
        max_hold_bars=int(sig.get("max_hold_bars", 0) or 0),
    )
