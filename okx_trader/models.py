from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Candle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    confirm: bool
    volume: float = 0.0


@dataclass
class StrategyParams:
    strategy_variant: str
    htf_ema_fast_len: int
    htf_ema_slow_len: int
    htf_rsi_len: int
    htf_rsi_long_min: float
    htf_rsi_short_max: float
    loc_lookback: int
    loc_recent_bars: int
    loc_sr_lookback: int
    location_fib_low: float
    location_fib_high: float
    location_retest_tol: float

    break_len: int
    exit_len: int
    ltf_ema_len: int
    bb_len: int
    bb_mult: float
    bb_width_k: float
    rsi_len: int
    rsi_long_min: float
    rsi_short_max: float
    l2_rsi_relax: float
    l3_rsi_relax: float
    macd_fast: int
    macd_slow: int
    macd_signal: int
    pullback_lookback: int
    pullback_tolerance: float
    max_chase_from_ema: float
    atr_len: int
    atr_stop_mult: float
    min_risk_atr_mult: float
    min_risk_pct: float
    leverage: float
    risk_frac: float
    risk_max_margin_frac: float
    tp1_r_mult: float
    tp2_r_mult: float
    tp1_close_pct: float
    tp2_close_rest: bool
    be_trigger_r_mult: float
    be_offset_pct: float
    be_fee_buffer_pct: float
    trail_atr_mult: float
    trail_after_tp1: bool
    max_open_entries: int
    max_open_entries_global: int
    open_window_hours: int
    min_open_interval_minutes: int
    daily_loss_limit_pct: float
    daily_loss_base_usdt: float
    daily_loss_base_mode: str
    stop_reentry_cooldown_minutes: int
    stop_streak_freeze_count: int
    stop_streak_freeze_hours: int
    stop_streak_l2_only: bool
    exec_max_level: int
    exec_l3_inst_ids: List[str]
    enable_close: bool
    signal_exit_enabled: bool
    split_tp_on_entry: bool
    allow_reverse: bool
    entry_exec_mode: str
    entry_exec_mode_l1: str
    entry_exec_mode_l2: str
    entry_exec_mode_l3: str
    entry_auto_market_level_min: int
    entry_auto_market_level_max: int
    entry_limit_offset_bps: float
    entry_limit_ttl_sec: int
    entry_limit_ttl_sec_l1: int
    entry_limit_ttl_sec_l2: int
    entry_limit_ttl_sec_l3: int
    entry_limit_poll_ms: int
    entry_limit_reprice_max: int
    entry_limit_fallback_mode: str
    entry_limit_fallback_mode_l1: str
    entry_limit_fallback_mode_l2: str
    entry_limit_fallback_mode_l3: str
    manage_only_script_positions: bool
    skip_on_foreign_mgnmode_pos: bool


@dataclass
class Config:
    base_url: str
    api_key: str
    secret_key: str
    passphrase: str
    paper: bool
    dry_run: bool
    inst_ids: List[str]
    htf_bar: str
    loc_bar: str
    ltf_bar: str
    poll_seconds: int
    fast_ltf_gate: bool
    ws_tp1_be_enabled: bool
    ws_private_url: str
    ws_reconnect_seconds: int
    candle_limit: int
    history_cache_enabled: bool
    history_cache_dir: str
    history_cache_ttl_seconds: int
    td_mode: str
    pos_mode: str
    order_size: float
    sizing_mode: str
    margin_usdt: float
    leverage: float
    attach_tpsl_on_entry: bool
    attach_tpsl_tp_r: float
    attach_tpsl_trigger_px_type: str
    compound_enabled: bool
    compound_mode: str
    compound_base_equity: float
    compound_base_margin: float
    compound_step_equity: float
    compound_step_margin: float
    compound_ratio_power: float
    compound_min_margin: float
    compound_max_margin: float
    compound_dd_guard_pct: float
    compound_dd_factor: float
    compound_balance_ccy: str
    compound_cache_seconds: int
    state_file: str
    user_agent: str
    log_level: str
    log_heartbeat_seconds: int
    alert_only: bool
    alert_email_enabled: bool
    alert_smtp_host: str
    alert_smtp_port: int
    alert_smtp_user: str
    alert_smtp_pass: str
    alert_smtp_from: str
    alert_smtp_to: List[str]
    alert_smtp_use_ssl: bool
    alert_smtp_starttls: bool
    alert_tg_enabled: bool
    alert_tg_trade_exec_enabled: bool
    alert_tg_bot_token: str
    alert_tg_chat_id: str
    alert_tg_api_base: str
    alert_tg_parse_mode: str
    alert_max_level: int
    alert_intrabar_enabled: bool
    alert_stats_keep_days: int
    alert_no_open_hours: float
    alert_no_open_cooldown_hours: float
    alert_local_sound: bool
    alert_local_file: bool
    alert_local_file_path: str
    trade_journal_enabled: bool
    trade_journal_path: str
    trade_order_link_enabled: bool
    trade_order_link_path: str
    params: StrategyParams
    strategy_profile_map: Dict[str, str]
    strategy_profile_vote_map: Dict[str, List[str]]
    strategy_profile_vote_mode: str
    strategy_profile_vote_min_agree: int
    strategy_profile_vote_score_map: Dict[str, float]
    strategy_profile_vote_level_weight: float
    strategy_profile_vote_fallback_profiles: List[str]
    strategy_profiles: Dict[str, StrategyParams]


@dataclass
class PositionState:
    side: str  # flat | long | short | mixed
    size: float
