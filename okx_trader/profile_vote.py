from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .decision_core import EntryDecision


def _vote_required(
    *,
    mode: str,
    total_profiles: int,
    total_votes: int,
    min_agree: int,
) -> int:
    need = max(1, int(min_agree))
    mode_k = str(mode or "majority").strip().lower()
    if mode_k == "any":
        return need
    if mode_k == "unanimous":
        return max(need, max(1, int(total_profiles)))
    # majority over active (non-neutral) votes.
    active = max(1, int(total_votes))
    return max(need, (active // 2) + 1)


def merge_entry_votes(
    *,
    base_signal: Dict[str, Any],
    profile_ids: List[str],
    signals_by_profile: Dict[str, Dict[str, Any]],
    decisions_by_profile: Dict[str, Optional[EntryDecision]],
    mode: str,
    min_agree: int,
    enforce_max_level: int,
    profile_score_map: Optional[Dict[str, float]] = None,
    level_weight: float = 0.0,
    fallback_profile_ids: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    sig = dict(base_signal)
    total_profiles = max(0, len(profile_ids))
    enforce_max_level = max(1, min(3, int(enforce_max_level)))

    # (order_idx, profile_id, decision)
    long_votes: List[Tuple[int, str, EntryDecision]] = []
    short_votes: List[Tuple[int, str, EntryDecision]] = []
    for idx, profile_id in enumerate(profile_ids):
        decision = decisions_by_profile.get(profile_id)
        if decision is None:
            continue
        if decision.side == "LONG":
            long_votes.append((idx, profile_id, decision))
        elif decision.side == "SHORT":
            short_votes.append((idx, profile_id, decision))

    fallback_set = {str(x).strip().upper() for x in (fallback_profile_ids or []) if str(x).strip()}
    long_votes_all = list(long_votes)
    short_votes_all = list(short_votes)
    fallback_mode = "disabled"
    if fallback_set:
        long_primary = [x for x in long_votes_all if str(x[1]).strip().upper() not in fallback_set]
        short_primary = [x for x in short_votes_all if str(x[1]).strip().upper() not in fallback_set]
        primary_total = len(long_primary) + len(short_primary)
        if primary_total > 0:
            long_votes = long_primary
            short_votes = short_primary
            fallback_mode = "suppressed"
        else:
            long_votes = long_votes_all
            short_votes = short_votes_all
            fallback_mode = "activated" if (len(long_votes) + len(short_votes)) > 0 else "idle"

    lv = len(long_votes)
    sv = len(short_votes)
    total_votes = lv + sv
    score_map = profile_score_map or {}
    use_weighted = bool(score_map) or float(level_weight) > 0.0
    level_w = max(0.0, float(level_weight))

    def _decision_score(pid: str, level: int) -> float:
        base = float(score_map.get(pid, 0.0))
        lv_clamped = max(1, min(3, int(level)))
        # Higher level strictness gets a small bonus (L1 > L2 > L3).
        strict_bonus = (4 - lv_clamped) * level_w
        return base + strict_bonus

    def _side_score(votes: List[Tuple[int, str, EntryDecision]]) -> float:
        out = 0.0
        for _idx, pid, d in votes:
            out += _decision_score(pid, int(d.level))
        return out

    required = _vote_required(
        mode=mode,
        total_profiles=total_profiles,
        total_votes=total_votes,
        min_agree=min_agree,
    )

    winner_side = ""
    long_ok = lv >= required
    short_ok = sv >= required
    if long_ok and not short_ok:
        winner_side = "LONG"
    elif short_ok and not long_ok:
        winner_side = "SHORT"
    elif long_ok and short_ok:
        if use_weighted:
            long_side_score = _side_score(long_votes)
            short_side_score = _side_score(short_votes)
            eps = 1e-12
            if long_side_score > short_side_score + eps:
                winner_side = "LONG"
            elif short_side_score > long_side_score + eps:
                winner_side = "SHORT"
        if not winner_side:
            if lv > sv:
                winner_side = "LONG"
            elif sv > lv:
                winner_side = "SHORT"
            else:
                best_long = min(int(x[2].level) for x in long_votes) if long_votes else 0
                best_short = min(int(x[2].level) for x in short_votes) if short_votes else 0
                if best_long > 0 and best_short > 0:
                    if best_long < best_short:
                        winner_side = "LONG"
                    elif best_short < best_long:
                        winner_side = "SHORT"

    winner_profile = ""
    winner_level = 0
    winner_stop = 0.0
    winner_decision: Optional[EntryDecision] = None
    if winner_side == "LONG":
        if use_weighted:
            long_votes.sort(
                key=lambda x: (
                    -_decision_score(x[1], int(x[2].level)),
                    int(x[2].level),
                    int(x[0]),
                )
            )
        else:
            long_votes.sort(key=lambda x: (int(x[2].level), int(x[0])))
        _, winner_profile, winner_decision = long_votes[0]
    elif winner_side == "SHORT":
        if use_weighted:
            short_votes.sort(
                key=lambda x: (
                    -_decision_score(x[1], int(x[2].level)),
                    int(x[2].level),
                    int(x[0]),
                )
            )
        else:
            short_votes.sort(key=lambda x: (int(x[2].level), int(x[0])))
        _, winner_profile, winner_decision = short_votes[0]

    if winner_decision is not None:
        winner_level = int(winner_decision.level)
        winner_stop = float(winner_decision.stop)
        if winner_level > enforce_max_level:
            winner_side = ""
            winner_profile = ""
            winner_level = 0
            winner_stop = 0.0
            winner_decision = None

    if winner_side == "LONG" and winner_decision is not None:
        sig["long_level"] = int(winner_level)
        sig["long_stop"] = float(winner_stop)
        sig["short_level"] = 0
        sig["long_entry"] = True
        sig["short_entry"] = False
        sig["long_entry_l2"] = bool(winner_level <= 2)
        sig["short_entry_l2"] = False
        sig["long_entry_l3"] = bool(winner_level <= 3)
        sig["short_entry_l3"] = False
    elif winner_side == "SHORT" and winner_decision is not None:
        sig["short_level"] = int(winner_level)
        sig["short_stop"] = float(winner_stop)
        sig["long_level"] = 0
        sig["long_entry"] = False
        sig["short_entry"] = True
        sig["long_entry_l2"] = False
        sig["short_entry_l2"] = bool(winner_level <= 2)
        sig["long_entry_l3"] = False
        sig["short_entry_l3"] = bool(winner_level <= 3)
    else:
        sig["long_level"] = 0
        sig["short_level"] = 0
        sig["long_entry"] = False
        sig["short_entry"] = False
        sig["long_entry_l2"] = False
        sig["short_entry_l2"] = False
        sig["long_entry_l3"] = False
        sig["short_entry_l3"] = False

    meta: Dict[str, Any] = {
        "enabled": bool(total_profiles > 1),
        "mode": str(mode or "majority").strip().lower(),
        "required": int(required),
        "min_agree": int(max(1, int(min_agree))),
        "profiles": list(profile_ids),
        "total_profiles": int(total_profiles),
        "total_votes": int(total_votes),
        "long_votes": int(lv),
        "short_votes": int(sv),
        "long_votes_all": int(len(long_votes_all)),
        "short_votes_all": int(len(short_votes_all)),
        "winner_side": winner_side or "NONE",
        "winner_profile": winner_profile,
        "winner_level": int(winner_level),
        "winner_decision": winner_decision,
        "weighted": bool(use_weighted),
        "level_weight": float(level_w),
        "score_map": dict(score_map),
        "fallback_profiles": sorted(fallback_set),
        "fallback_mode": fallback_mode,
        "long_side_score": float(_side_score(long_votes)) if use_weighted else 0.0,
        "short_side_score": float(_side_score(short_votes)) if use_weighted else 0.0,
    }
    sig["vote_enabled"] = bool(meta["enabled"])
    sig["vote_mode"] = str(meta["mode"])
    sig["vote_required"] = int(meta["required"])
    sig["vote_min_agree"] = int(meta["min_agree"])
    sig["vote_total_profiles"] = int(meta["total_profiles"])
    sig["vote_total_votes"] = int(meta["total_votes"])
    sig["vote_long_votes"] = int(meta["long_votes"])
    sig["vote_short_votes"] = int(meta["short_votes"])
    sig["vote_winner"] = str(meta["winner_side"])
    sig["vote_winner_profile"] = str(meta["winner_profile"])
    sig["vote_winner_level"] = int(meta["winner_level"])
    return sig, meta
