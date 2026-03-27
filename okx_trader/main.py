from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Optional

from .alerts import run_test_alert, send_telegram
from .backtest import build_backtest_telegram_summary, run_backtest, run_backtest_compare
from .common import (
    apply_backtest_env_overrides,
    load_dotenv,
    log,
    parse_backtest_levels,
    parse_bool,
    parse_inst_ids,
    resolve_backtest_live_window_signals,
    round_size,
    set_log_level,
)
from .client_factory import create_client
from .config import read_config
from .instance_lock import SingleInstanceLock
from .runtime import print_stats, run_once
from .state_store import load_state
from .ws_tp1_be import OKXWsTp1BeWorker


def main() -> int:
    parser = argparse.ArgumentParser(description="OKX adaptive auto trader")
    parser.add_argument("--env", default=os.path.join(os.path.dirname(__file__), "..", "okx_auto_trader.env"))
    parser.add_argument("--once", action="store_true", help="Run one iteration and exit")
    parser.add_argument("--test-alert", action="store_true", help="Send one test alert and exit")
    parser.add_argument("--test-inst", default="", help="Instrument name used by --test-alert")
    parser.add_argument("--stats", action="store_true", help="Print daily alert/opportunity stats and exit")
    parser.add_argument("--stats-days", default="3", help="How many recent days to print for --stats")
    parser.add_argument("--backtest", action="store_true", help="Run historical backtest and exit")
    parser.add_argument("--bt-bars", default="1200", help="LTF bars to evaluate in backtest")
    parser.add_argument(
        "--bt-horizon-bars",
        default="24",
        help="Forward bars for signal outcome evaluation; 0 means hold until TP/SL or data end",
    )
    parser.add_argument("--bt-min-level", default="1", help="Min alert level to include in backtest (1~3)")
    parser.add_argument(
        "--bt-max-level",
        default="0",
        help="Max alert level to evaluate in backtest (0 uses ALERT_MAX_LEVEL, 1~3 override)",
    )
    parser.add_argument(
        "--bt-exact-level",
        default="0",
        help="Exact alert level to include (1~3). When set, min/max are ignored.",
    )
    parser.add_argument(
        "--bt-compare-levels",
        default="",
        help="Comma-separated levels to compare sequentially (e.g. 1,2); overrides --bt-max-level",
    )
    parser.add_argument(
        "--bt-inst-ids",
        default="",
        help="Comma-separated instruments for backtest (empty uses OKX_INST_IDS/OKX_INST_ID)",
    )
    parser.add_argument(
        "--bt-min-open-interval-minutes",
        default="0",
        help="Min minutes between opens for each instrument in backtest path-mode (0=disable)",
    )
    parser.add_argument(
        "--bt-max-opens-per-day",
        default="0",
        help="Max opens per UTC day for each instrument in backtest path-mode (0=disable)",
    )
    parser.add_argument(
        "--bt-require-tp-sl",
        action="store_true",
        help="Only count trades that end with TP/SL (exclude unresolved NONE outcome)",
    )
    parser.add_argument(
        "--bt-tp1-only",
        action="store_true",
        help="Backtest exits at TP1 only (ignore TP2 target)",
    )
    parser.add_argument(
        "--bt-managed-exit",
        action="store_true",
        help="Backtest managed exit: TP1 partial + remaining TP2 + BE/fee-buffer stop",
    )
    parser.set_defaults(bt_live_window_signals=None)
    parser.add_argument(
        "--bt-live-window-signals",
        dest="bt_live_window_signals",
        action="store_true",
        help="Build backtest signals with the same rolling candle window as live runtime",
    )
    parser.add_argument(
        "--bt-fast-signals",
        dest="bt_live_window_signals",
        action="store_false",
        help="Use fast precomputed signal path in backtest (faster, but less parity-safe)",
    )
    parser.add_argument(
        "--bt-send-telegram",
        action="store_true",
        help="Send backtest summary to Telegram after run",
    )
    parser.add_argument("--bt-title", default="", help="Optional title for backtest summary message")
    parser.add_argument(
        "--no-instance-lock",
        action="store_true",
        help="Disable single-instance lock for live/once mode",
    )
    parser.add_argument("--state-file", default=None, help="Override state file path")
    args = parser.parse_args()

    load_dotenv(args.env)
    if args.backtest:
        override_notes = apply_backtest_env_overrides()
        if override_notes:
            log(f"Backtest env overrides: {'; '.join(override_notes)}")
    cfg = read_config(args.state_file)
    set_log_level(cfg.log_level)

    lock: Optional[SingleInstanceLock] = None
    ws_worker: Optional[OKXWsTp1BeWorker] = None
    lock_enabled = parse_bool(os.getenv("OKX_SINGLE_INSTANCE_LOCK", "1"), True) and (not args.no_instance_lock)
    lock_needed = (not args.backtest) and (not args.stats) and (not args.test_alert)
    if lock_needed and lock_enabled:
        lock_path = str(os.getenv("OKX_INSTANCE_LOCK_FILE", "") or "").strip() or (cfg.state_file + ".lock")
        lock = SingleInstanceLock(lock_path)
        if not lock.acquire():
            log(
                f"Another instance is running or lock is busy: {lock_path}. "
                "Stop old process or pass --no-instance-lock to bypass.",
                level="ERROR",
            )
            return 2
        log(f"Instance lock acquired: {lock_path}")

    try:
        if args.test_alert:
            return run_test_alert(cfg, args.test_inst)

        if args.stats:
            state = load_state(cfg.state_file)
            try:
                days = int(args.stats_days)
            except Exception:
                days = 3
            return print_stats(cfg, state, days)

        if args.backtest:
            client = create_client(cfg)
            try:
                bars = int(args.bt_bars)
            except Exception:
                bars = 1200
            try:
                horizon = int(args.bt_horizon_bars)
            except Exception:
                horizon = 24
            if horizon < 0:
                horizon = 0
            try:
                min_level = int(args.bt_min_level)
            except Exception:
                min_level = 1
            try:
                max_level_cli = int(args.bt_max_level)
            except Exception:
                max_level_cli = 0
            try:
                exact_level = int(args.bt_exact_level)
            except Exception:
                exact_level = 0
            try:
                bt_min_open_interval_minutes = int(args.bt_min_open_interval_minutes)
            except Exception:
                bt_min_open_interval_minutes = 0
            try:
                bt_max_opens_per_day = int(args.bt_max_opens_per_day)
            except Exception:
                bt_max_opens_per_day = 0
            bt_require_tp_sl = bool(args.bt_require_tp_sl)
            bt_tp1_only = bool(args.bt_tp1_only)
            bt_managed_exit = bool(args.bt_managed_exit)
            bt_live_window_signals = resolve_backtest_live_window_signals(args.bt_live_window_signals)
            inst_ids = parse_inst_ids(args.bt_inst_ids) or cfg.inst_ids
            compare_levels = parse_backtest_levels(args.bt_compare_levels)

            if exact_level not in {0, 1, 2, 3}:
                log(f"Invalid --bt-exact-level={exact_level}, fallback to 0")
                exact_level = 0
            if min_level < 1:
                min_level = 1
            if min_level > 3:
                min_level = 3

            results = []
            if compare_levels:
                if exact_level in {1, 2, 3}:
                    log("--bt-exact-level is set; ignore --bt-compare-levels and run single exact level.")
                    one = run_backtest(
                        client=client,
                        cfg=cfg,
                        inst_ids=inst_ids,
                        bars=bars,
                        horizon_bars=horizon,
                        max_level=exact_level,
                        min_level=exact_level,
                        exact_level=exact_level,
                        bt_min_open_interval_minutes=bt_min_open_interval_minutes,
                        bt_max_opens_per_day=bt_max_opens_per_day,
                        bt_require_tp_sl=bt_require_tp_sl,
                        bt_tp1_only=bt_tp1_only,
                        bt_managed_exit=bt_managed_exit,
                        bt_live_window_signals=bt_live_window_signals,
                    )
                    results = [one]
                else:
                    if min_level != 1:
                        log("--bt-compare-levels runs cumulative levels; ignore --bt-min-level and use 1.")
                    results = run_backtest_compare(
                        client,
                        cfg,
                        inst_ids,
                        bars,
                        horizon,
                        compare_levels,
                        min_level=1,
                        bt_min_open_interval_minutes=bt_min_open_interval_minutes,
                        bt_max_opens_per_day=bt_max_opens_per_day,
                        bt_require_tp_sl=bt_require_tp_sl,
                        bt_tp1_only=bt_tp1_only,
                        bt_managed_exit=bt_managed_exit,
                        bt_live_window_signals=bt_live_window_signals,
                    )
            else:
                max_level = cfg.alert_max_level if max_level_cli <= 0 else max_level_cli
                one = run_backtest(
                    client=client,
                    cfg=cfg,
                    inst_ids=inst_ids,
                    bars=bars,
                    horizon_bars=horizon,
                    max_level=max_level,
                    min_level=min_level,
                    exact_level=exact_level,
                    bt_min_open_interval_minutes=bt_min_open_interval_minutes,
                    bt_max_opens_per_day=bt_max_opens_per_day,
                    bt_require_tp_sl=bt_require_tp_sl,
                    bt_tp1_only=bt_tp1_only,
                    bt_managed_exit=bt_managed_exit,
                    bt_live_window_signals=bt_live_window_signals,
                )
                results = [one]

            if args.bt_send_telegram:
                text = build_backtest_telegram_summary(cfg, results, args.bt_title)
                sent = send_telegram(cfg, text)
                log(f"Backtest summary telegram_sent={sent}")
                if not sent:
                    return 1
            return 0

        client = create_client(cfg)
        state = load_state(cfg.state_file)

        insts_display = ",".join(cfg.inst_ids)
        profile_map_display = ",".join(f"{k}:{v}" for k, v in sorted(cfg.strategy_profile_map.items())) or "-"
        profile_vote_map_display = (
            ",".join(f"{k}:{'+'.join(v)}" for k, v in sorted(cfg.strategy_profile_vote_map.items()))
            if cfg.strategy_profile_vote_map
            else "-"
        )
        profile_vote_score_display = (
            ",".join(f"{k}={v:.4f}" for k, v in sorted(cfg.strategy_profile_vote_score_map.items()))
            if cfg.strategy_profile_vote_score_map
            else "-"
        )
        profile_vote_fallback_display = (
            ",".join(cfg.strategy_profile_vote_fallback_profiles)
            if cfg.strategy_profile_vote_fallback_profiles
            else "-"
        )
        profile_ids_display = ",".join(sorted(cfg.strategy_profiles.keys())) or "DEFAULT"
        log(
            f"Start | provider={cfg.exchange_provider} insts={insts_display} htf={cfg.htf_bar} loc={cfg.loc_bar} ltf={cfg.ltf_bar} dry_run={cfg.dry_run} "
            f"paper={cfg.paper} pos_mode={cfg.pos_mode} td_mode={cfg.td_mode} "
            f"hist_cache={cfg.history_cache_enabled} ttl={cfg.history_cache_ttl_seconds}s "
            f"fast_ltf_gate={cfg.fast_ltf_gate} "
            f"sizing_mode={cfg.sizing_mode} order_size={round_size(cfg.order_size)} "
            f"margin={cfg.margin_usdt} leverage={cfg.leverage} "
            f"attach_tpsl={cfg.attach_tpsl_on_entry} tp_r={cfg.attach_tpsl_tp_r} trig={cfg.attach_tpsl_trigger_px_type} "
            f"compound={cfg.compound_enabled}({cfg.compound_mode}) "
            f"cmp_base_eq={cfg.compound_base_equity} cmp_base_m={cfg.compound_base_margin} "
            f"cmp_range={cfg.compound_min_margin}-{cfg.compound_max_margin} "
            f"cmp_dd={cfg.compound_dd_guard_pct}/{cfg.compound_dd_factor} "
            f"log_level={cfg.log_level} heartbeat={cfg.log_heartbeat_seconds}s "
            f"open_limit={cfg.params.max_open_entries}/{cfg.params.open_window_hours}h "
            f"open_limit_global={cfg.params.max_open_entries_global}/{cfg.params.open_window_hours}h "
            f"open_min_interval={cfg.params.min_open_interval_minutes}m "
            f"risk_frac={cfg.params.risk_frac} "
            f"risk_max_margin_frac={cfg.params.risk_max_margin_frac} "
            f"risk_mode={'on' if (cfg.params.risk_frac > 0 and cfg.margin_usdt <= 0) else 'off'} "
            f"daily_loss_limit={cfg.params.daily_loss_limit_pct * 100:.2f}% "
            f"daily_loss_base={cfg.params.daily_loss_base_usdt} "
            f"daily_loss_mode={cfg.params.daily_loss_base_mode} "
            f"tp2_close_rest={cfg.params.tp2_close_rest} "
            f"be_fee_buf={cfg.params.be_fee_buffer_pct} "
            f"auto_tighten_stop={cfg.params.auto_tighten_stop} "
            f"stop_cooldown={cfg.params.stop_reentry_cooldown_minutes}m "
            f"tp2_cooldown={cfg.params.tp2_reentry_cooldown_hours:g}h "
            f"tp2_partial_until={cfg.params.tp2_reentry_partial_until_hours:g}h "
            f"tp2_partial_max_level={cfg.params.tp2_reentry_partial_max_level} "
            f"stop_freeze={cfg.params.stop_streak_freeze_count}/{cfg.params.stop_streak_freeze_hours}h "
            f"stop_l2_only={cfg.params.stop_streak_l2_only} "
            f"exec_max_level={cfg.params.exec_max_level} "
            f"exec_l3_inst_ids={','.join(cfg.params.exec_l3_inst_ids) if cfg.params.exec_l3_inst_ids else '-'} "
            f"profiles={profile_ids_display} "
            f"profile_map={profile_map_display} "
            f"profile_vote_map={profile_vote_map_display} "
            f"profile_vote_mode={cfg.strategy_profile_vote_mode}/{cfg.strategy_profile_vote_min_agree} "
            f"profile_vote_score_map={profile_vote_score_display} "
            f"profile_vote_level_weight={cfg.strategy_profile_vote_level_weight} "
            f"profile_vote_fallback={profile_vote_fallback_display} "
            f"journal={cfg.trade_journal_enabled} "
            f"journal_path={cfg.trade_journal_path} "
            f"order_link={cfg.trade_order_link_enabled} "
            f"order_link_path={cfg.trade_order_link_path} "
            f"close_enabled={cfg.params.enable_close} "
            f"signal_exit_enabled={cfg.params.signal_exit_enabled} "
            f"split_tp_on_entry={cfg.params.split_tp_on_entry} "
            f"entry_exec_mode={cfg.params.entry_exec_mode} "
            f"entry_auto_lv_min={cfg.params.entry_auto_market_level_min} "
            f"entry_auto_lv_max={cfg.params.entry_auto_market_level_max} "
            f"entry_limit={cfg.params.entry_limit_offset_bps}bps/{cfg.params.entry_limit_ttl_sec}s "
            f"fallback={cfg.params.entry_limit_fallback_mode} "
            f"ws_tp1_be={cfg.ws_tp1_be_enabled} "
            f"ws_url={cfg.ws_private_url} "
            f"skip_foreign_mgnmode={cfg.params.skip_on_foreign_mgnmode_pos} "
            f"alert_only={cfg.alert_only} email_enabled={cfg.alert_email_enabled} "
            f"tg_enabled={cfg.alert_tg_enabled} tg_trade_exec={cfg.alert_tg_trade_exec_enabled} "
            f"max_level={cfg.alert_max_level} "
            f"no_open_alert={cfg.alert_no_open_hours}h/{cfg.alert_no_open_cooldown_hours}h "
            f"intrabar={cfg.alert_intrabar_enabled} stats_keep_days={cfg.alert_stats_keep_days} "
            f"local_sound={cfg.alert_local_sound} local_file={cfg.alert_local_file}"
        )
        if (not args.once) and cfg.exchange_provider == "okx":
            ws_worker = OKXWsTp1BeWorker(cfg, client, state)
            ws_worker.start()
        elif (not args.once) and cfg.exchange_provider != "okx":
            log(f"[{cfg.exchange_provider}] private WS TP1/BE worker disabled; use polling management.")

        try:
            if args.once:
                try:
                    run_once(client, cfg, state)
                    return 0
                except Exception as e:
                    log(f"Run error: {e}", level="ERROR")
                    return 1

            while True:
                try:
                    run_once(client, cfg, state)
                except Exception as e:
                    log(f"Loop error: {e}", level="ERROR")
                time.sleep(cfg.poll_seconds)
        except KeyboardInterrupt:
            log("Stopped by user.")
            return 0
    finally:
        if ws_worker is not None:
            ws_worker.stop()
        if lock is not None:
            lock.release()
            log("Instance lock released.")


if __name__ == "__main__":
    sys.exit(main())
