from __future__ import annotations

import time
from typing import Any, Dict, Optional

from .common import bar_to_seconds, log
from .config import (
    get_strategy_params,
    get_strategy_profile_id,
    get_strategy_profile_ids,
    resolve_exec_max_level,
)
from .decision_core import resolve_entry_decision
from .models import Config, PositionState, StrategyParams
from .okx_client import OKXClient, parse_position, split_positions_by_mgn_mode
from .profile_vote import merge_entry_votes
from .runtime_execute_decision import execute_decision
from .signals import build_signals
from .state_store import _record_opportunity


def _execute_decision_with_params(
    client: OKXClient,
    cfg: Config,
    inst_id: str,
    sig: Dict[str, Any],
    pos: PositionState,
    state: Dict[str, Any],
    params: StrategyParams,
    profile_id: str,
    root_state: Optional[Dict[str, Any]] = None,
) -> None:
    prev_params = cfg.params
    prev_leverage = float(cfg.leverage)
    cfg.params = params
    profile_leverage = float(getattr(params, "leverage", 0.0) or 0.0)
    if profile_leverage > 0:
        cfg.leverage = profile_leverage
    try:
        execute_decision(
            client=client,
            cfg=cfg,
            inst_id=inst_id,
            sig=sig,
            pos=pos,
            state=state,
            root_state=root_state,
            profile_id=profile_id,
        )
    finally:
        cfg.params = prev_params
        cfg.leverage = prev_leverage


def run_once_for_inst(
    client: OKXClient,
    cfg: Config,
    inst_id: str,
    inst_state: Dict[str, Any],
    root_state: Optional[Dict[str, Any]] = None,
) -> tuple[bool, str]:
    profile_ids = get_strategy_profile_ids(cfg, inst_id)
    if not profile_ids:
        profile_ids = [get_strategy_profile_id(cfg, inst_id)]
    profile_id = profile_ids[0]
    params = cfg.strategy_profiles.get(profile_id, get_strategy_params(cfg, inst_id))
    intrabar_mode = cfg.alert_only and cfg.alert_intrabar_enabled
    htf_candles = client.get_candles(inst_id, cfg.htf_bar, cfg.candle_limit)
    loc_candles = client.get_candles(inst_id, cfg.loc_bar, cfg.candle_limit)
    ltf_candles = client.get_candles(inst_id, cfg.ltf_bar, cfg.candle_limit, include_unconfirmed=intrabar_mode)
    if not htf_candles:
        log(f"[{inst_id}] No HTF candle data returned from OKX.", level="WARN")
        return False, "no_data"
    if not loc_candles:
        log(f"[{inst_id}] No LOC candle data returned from OKX.", level="WARN")
        return False, "no_data"
    if not ltf_candles:
        log(f"[{inst_id}] No LTF candle data returned from OKX.", level="WARN")
        return False, "no_data"

    sig = build_signals(htf_candles, loc_candles, ltf_candles, params)
    if len(profile_ids) > 1:
        signals_by_profile: Dict[str, Dict[str, Any]] = {profile_id: sig}
        decisions_by_profile: Dict[str, Optional[Any]] = {}

        primary_exec_max = resolve_exec_max_level(params, inst_id)
        decisions_by_profile[profile_id] = resolve_entry_decision(
            sig,
            max_level=primary_exec_max,
            min_level=1,
            exact_level=0,
            tp1_r=params.tp1_r_mult,
            tp2_r=params.tp2_r_mult,
            tp1_only=False,
        )

        for pid in profile_ids[1:]:
            p = cfg.strategy_profiles.get(pid, cfg.params)
            try:
                one_sig = build_signals(htf_candles, loc_candles, ltf_candles, p)
            except Exception as e:
                log(f"[{inst_id}] vote profile={pid} skipped: {e}", level="WARN")
                continue
            signals_by_profile[pid] = one_sig
            one_exec_max = resolve_exec_max_level(p, inst_id)
            decisions_by_profile[pid] = resolve_entry_decision(
                one_sig,
                max_level=one_exec_max,
                min_level=1,
                exact_level=0,
                tp1_r=p.tp1_r_mult,
                tp2_r=p.tp2_r_mult,
                tp1_only=False,
            )

        sig, vote_meta = merge_entry_votes(
            base_signal=sig,
            profile_ids=[pid for pid in profile_ids if pid in signals_by_profile],
            signals_by_profile=signals_by_profile,
            decisions_by_profile=decisions_by_profile,
            mode=cfg.strategy_profile_vote_mode,
            min_agree=cfg.strategy_profile_vote_min_agree,
            enforce_max_level=primary_exec_max,
            profile_score_map=cfg.strategy_profile_vote_score_map,
            level_weight=cfg.strategy_profile_vote_level_weight,
        )
        if bool(vote_meta.get("enabled")):
            log(
                f"[{inst_id}] vote mode={vote_meta.get('mode')} "
                f"agree={vote_meta.get('required')} long={vote_meta.get('long_votes')} "
                f"short={vote_meta.get('short_votes')} weighted={vote_meta.get('weighted')} "
                f"winner={vote_meta.get('winner_side')} "
                f"pick={vote_meta.get('winner_profile')}@L{vote_meta.get('winner_level')}"
            )
    last_ts_raw = inst_state.get("last_processed_ts_ms")
    try:
        last_ts = int(last_ts_raw) if last_ts_raw is not None else None
    except (TypeError, ValueError):
        last_ts = None

    signal_ts = int(sig["signal_ts_ms"])
    signal_confirm = bool(sig.get("signal_confirm", True))
    if last_ts is not None and last_ts == signal_ts:
        if (not intrabar_mode) or signal_confirm:
            last_no_new_logged_raw = inst_state.get("last_no_new_logged_ts_ms")
            try:
                last_no_new_logged = int(last_no_new_logged_raw) if last_no_new_logged_raw is not None else None
            except Exception:
                last_no_new_logged = None
            if last_no_new_logged != signal_ts:
                log(f"[{inst_id}] No new closed candle yet.")
                inst_state["last_no_new_logged_ts_ms"] = signal_ts
            return False, "no_new"

    now_ms = int(time.time() * 1000)
    bar_s = bar_to_seconds(cfg.ltf_bar)
    if now_ms - signal_ts > bar_s * 1000 * 2:
        last_stale_logged_raw = inst_state.get("last_stale_logged_ts_ms")
        try:
            last_stale_logged = int(last_stale_logged_raw) if last_stale_logged_raw is not None else None
        except Exception:
            last_stale_logged = None
        if last_stale_logged != signal_ts:
            log(f"[{inst_id}] Latest closed candle is stale. Skip trading this round.", level="WARN")
            inst_state["last_stale_logged_ts_ms"] = signal_ts
        inst_state["last_processed_ts_ms"] = signal_ts
        return False, "stale"

    _record_opportunity(
        cfg=cfg,
        inst_state=inst_state,
        signal_ts_ms=signal_ts,
        signal_confirm=signal_confirm,
        side="LONG",
        level=int(sig.get("long_level", 0) or 0),
    )
    _record_opportunity(
        cfg=cfg,
        inst_state=inst_state,
        signal_ts_ms=signal_ts,
        signal_confirm=signal_confirm,
        side="SHORT",
        level=int(sig.get("short_level", 0) or 0),
    )

    if cfg.alert_only:
        pos = PositionState("flat", 0.0)
    elif cfg.dry_run and (not cfg.api_key or not cfg.secret_key or not cfg.passphrase):
        log(f"[{inst_id}] Dry-run without API credentials: assume flat position for signal simulation.")
        pos = PositionState("flat", 0.0)
    else:
        rows_all = client.get_positions(inst_id)
        rows, foreign_rows = split_positions_by_mgn_mode(rows_all, cfg.td_mode)

        foreign_nonzero = 0
        for r in foreign_rows:
            try:
                if abs(float(r.get("pos", "0") or "0")) > 0:
                    foreign_nonzero += 1
            except Exception:
                continue
        if foreign_nonzero > 0:
            log(
                f"[{inst_id}] Found {foreign_nonzero} non-{cfg.td_mode} position(s) in account. "
                "Ignoring them for this strategy."
            )
            if params.skip_on_foreign_mgnmode_pos:
                log(
                    f"[{inst_id}] Safety skip: STRAT_SKIP_ON_FOREIGN_MGNMODE_POS=1, "
                    "skip trading this instrument to avoid touching other margin-mode positions.",
                    level="WARN",
                )
                inst_state["last_processed_ts_ms"] = signal_ts
                return False, "safety_skip"

        if cfg.pos_mode == "net":
            has_dual_side = False
            for row in rows:
                pos_side_raw = str(row.get("posSide", "")).strip().lower()
                if pos_side_raw in {"long", "short"}:
                    has_dual_side = True
                    break
            if has_dual_side:
                client.mark_force_pos_side(inst_id)
                if not inst_state.get("warned_pos_mode_mismatch"):
                    log(
                        f"[{inst_id}] Detected dual-side positions while OKX_POS_MODE=net. "
                        "Auto-posSide fallback enabled. Recommend setting OKX_POS_MODE=long_short."
                    )
                    inst_state["warned_pos_mode_mismatch"] = True
        pos = parse_position(rows, cfg.pos_mode)

    _execute_decision_with_params(
        client=client,
        cfg=cfg,
        inst_id=inst_id,
        sig=sig,
        pos=pos,
        state=inst_state,
        params=params,
        profile_id=profile_id,
        root_state=root_state,
    )
    inst_state["last_processed_ts_ms"] = signal_ts
    return True, "processed"
