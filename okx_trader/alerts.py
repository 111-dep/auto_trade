from __future__ import annotations

import datetime as dt
import json
import os
import smtplib
import time
import urllib.parse
import urllib.request
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

from .common import format_price, infer_price_decimals, log, round_size
from .models import Config
from .signals import compute_alert_targets
from .state_store import _mark_alert_sent, _record_alert, day_key_from_ts_ms


def format_ts_ms(ts_ms: int) -> str:
    try:
        return dt.datetime.utcfromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(ts_ms)


def send_email(cfg: Config, subject: str, body: str) -> bool:
    if not cfg.alert_email_enabled:
        log("[Alert] Email disabled by ALERT_EMAIL_ENABLED=0")
        return False
    if not cfg.alert_smtp_host:
        log("[Alert] Email skipped: ALERT_SMTP_HOST is empty")
        return False
    if not cfg.alert_smtp_to:
        log("[Alert] Email skipped: ALERT_EMAIL_TO is empty")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg.alert_smtp_from or cfg.alert_smtp_user or "okx-bot@localhost"
    msg["To"] = ", ".join(cfg.alert_smtp_to)
    msg.set_content(body)

    try:
        timeout = 20
        if cfg.alert_smtp_use_ssl:
            with smtplib.SMTP_SSL(cfg.alert_smtp_host, cfg.alert_smtp_port, timeout=timeout) as smtp:
                if cfg.alert_smtp_user:
                    smtp.login(cfg.alert_smtp_user, cfg.alert_smtp_pass)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(cfg.alert_smtp_host, cfg.alert_smtp_port, timeout=timeout) as smtp:
                smtp.ehlo()
                if cfg.alert_smtp_starttls:
                    smtp.starttls()
                    smtp.ehlo()
                if cfg.alert_smtp_user:
                    smtp.login(cfg.alert_smtp_user, cfg.alert_smtp_pass)
                smtp.send_message(msg)
        return True
    except Exception as e:
        log(f"[Alert] Email send failed: {e}")
        return False


def send_telegram(cfg: Config, text: str) -> bool:
    if not cfg.alert_tg_enabled:
        log("[Alert] Telegram disabled by ALERT_TG_ENABLED=0")
        return False
    if not cfg.alert_tg_bot_token:
        log("[Alert] Telegram skipped: ALERT_TG_BOT_TOKEN is empty")
        return False
    if not cfg.alert_tg_chat_id:
        log("[Alert] Telegram skipped: ALERT_TG_CHAT_ID is empty")
        return False

    base = (cfg.alert_tg_api_base or "https://api.telegram.org").rstrip("/")
    url = f"{base}/bot{cfg.alert_tg_bot_token}/sendMessage"
    form: Dict[str, str] = {
        "chat_id": cfg.alert_tg_chat_id,
        "text": text,
        "disable_web_page_preview": "true",
    }
    if cfg.alert_tg_parse_mode:
        form["parse_mode"] = cfg.alert_tg_parse_mode

    payload = urllib.parse.urlencode(form).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        if not bool(data.get("ok")):
            log(f"[Alert] Telegram send failed: {raw[:240]}")
            return False
        return True
    except Exception as e:
        log(f"[Alert] Telegram send failed: {e}")
        return False


def emit_local_alert(cfg: Config, subject: str, body: str) -> bool:
    # Always print a clear alert line in terminal.
    log(f"[ALERT] {subject}")

    if cfg.alert_local_sound:
        try:
            print("\a", end="", flush=True)
        except Exception:
            pass

    if not cfg.alert_local_file:
        return False

    path = cfg.alert_local_file_path.strip()
    if not path:
        return False

    try:
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write("=" * 72 + "\n")
            f.write(subject + "\n")
            f.write(body.rstrip() + "\n")
        return True
    except Exception as e:
        log(f"[Alert] Local file write failed: {e}")
        return False


def _prune_entry_alert_history(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
    key: str = "entry_alert_ts_ms",
) -> List[int]:
    raw = inst_state.get(key)
    if not isinstance(raw, list):
        raw = []

    try:
        window_ms = int(max(1, cfg.params.open_window_hours) * 3600 * 1000)
    except Exception:
        window_ms = 24 * 3600 * 1000

    kept: List[int] = []
    for item in raw:
        try:
            ts = int(item)
        except Exception:
            continue
        if int(signal_ts_ms) - ts <= window_ms:
            kept.append(ts)
    kept.sort()
    inst_state[key] = kept
    return kept


def _allow_entry_alert_by_rate(
    cfg: Config,
    inst_id: str,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
) -> bool:
    recent = _prune_entry_alert_history(cfg, inst_state, signal_ts_ms)

    limit = int(cfg.params.max_open_entries)
    if limit > 0 and len(recent) >= limit:
        log(
            f"[{inst_id}] Alert throttled: limit reached ({len(recent)}/{limit} in {cfg.params.open_window_hours}h)."
        )
        return False

    min_gap_min = int(max(0, cfg.params.min_open_interval_minutes))
    if min_gap_min > 0 and recent:
        last_ts = max(recent)
        gap_ms = int(signal_ts_ms) - int(last_ts)
        min_gap_ms = min_gap_min * 60 * 1000
        if gap_ms < min_gap_ms:
            remain_ms = max(0, min_gap_ms - gap_ms)
            remain_min = max(1, int((remain_ms + 60 * 1000 - 1) / (60 * 1000)))
            log(
                f"[{inst_id}] Alert throttled: min interval {min_gap_min}m not reached, wait {remain_min}m."
            )
            return False
    return True


def _mark_entry_alert_rate(
    cfg: Config,
    inst_state: Dict[str, Any],
    signal_ts_ms: int,
) -> None:
    recent = _prune_entry_alert_history(cfg, inst_state, signal_ts_ms)
    recent.append(int(signal_ts_ms))
    recent.sort()
    inst_state["entry_alert_ts_ms"] = recent


def handle_entry_alert(cfg: Config, inst_id: str, sig: Dict[str, Any], state: Dict[str, Any]) -> None:
    side: Optional[str] = None
    side_cn: Optional[str] = None
    stage_cn = "收线确认" if bool(sig.get("signal_confirm", True)) else "盘中预警"
    level = 0
    level_tag = ""
    stop: Optional[float] = None
    long_level = int(sig.get("long_level", 0) or 0)
    short_level = int(sig.get("short_level", 0) or 0)

    candidates: List[Tuple[str, str, float, int]] = []
    if long_level > 0 and long_level <= cfg.alert_max_level:
        candidates.append(("LONG", "做多", float(sig["long_stop"]), long_level))
    if short_level > 0 and short_level <= cfg.alert_max_level:
        candidates.append(("SHORT", "做空", float(sig["short_stop"]), short_level))

    if not candidates:
        log(
            f"[{inst_id}] Alert: NONE (no entry signal within max_level={cfg.alert_max_level}) "
            f"long_level={long_level} short_level={short_level}"
        )
        return

    # Pick stronger signal first: lower level number means stricter/higher confidence.
    candidates.sort(key=lambda x: x[3])
    side, side_cn, stop_val_raw, level = candidates[0]
    stop = float(stop_val_raw)
    level_map = {1: "1级-严格", 2: "2级-中等", 3: "3级-宽松"}
    level_tag = level_map.get(level, f"{level}级")

    signal_ts_ms = int(sig["signal_ts_ms"])
    if not _allow_entry_alert_by_rate(cfg, inst_id, state, signal_ts_ms):
        return

    stage_key = "C" if bool(sig.get("signal_confirm", True)) else "L"
    day = day_key_from_ts_ms(signal_ts_ms)
    alert_key = f"{day}:{signal_ts_ms}:{side}:{stage_key}:L{level}"
    if not _mark_alert_sent(cfg, state, alert_key):
        log(f"[{inst_id}] Alert: duplicate key={alert_key}, skip")
        return
    _mark_entry_alert_rate(cfg, state, signal_ts_ms)

    price_dp = infer_price_decimals(entry=float(sig["close"]), stop=float(stop))
    subject = (
        f"[交易信号][{stage_cn}][{level_tag}] {inst_id} {side_cn} "
        f"{format_price(float(sig['close']), price_dp)}（{cfg.ltf_bar}）"
    )
    entry = float(sig["close"])
    stop_val = float(stop)
    risk_val, tp1_val, tp2_val = compute_alert_targets(
        side,
        entry_price=entry,
        stop_price=stop_val,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=cfg.params.tp2_r_mult,
    )
    body = (
        f"信号时间：{format_ts_ms(int(sig['signal_ts_ms']))}\n"
        f"交易对：{inst_id}\n"
        f"方向：{side_cn}\n"
        f"信号类型：{stage_cn}\n"
        f"信号等级：{level_tag}（当前最大推送等级={cfg.alert_max_level}）\n"
        f"周期：{cfg.ltf_bar}\n"
        f"入场参考价（收盘）：{format_price(entry, price_dp)}\n"
        f"建议止损：{format_price(stop_val, price_dp)}\n"
        f"风险（1R）：{format_price(risk_val, price_dp)}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)}\n"
        f"止盈二（{cfg.params.tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        f"大周期方向：{sig['bias']}\n"
        f"HTF收盘/EMA50/EMA200：{format_price(float(sig['htf_close']), price_dp)} / "
        f"{format_price(float(sig['htf_ema_fast']), price_dp)} / {format_price(float(sig['htf_ema_slow']), price_dp)}\n"
        f"LTF EMA/RSI/MACD柱：{format_price(float(sig['ema']), price_dp)} / {float(sig['rsi']):.2f} / {float(sig['macd_hist']):.4f}\n"
        f"位置过滤：fibL={sig['fib_touch_long']} fibS={sig['fib_touch_short']} rtL={sig['retest_long']} rtS={sig['retest_short']}\n"
        f"触发标记：LE={sig['long_entry']} SE={sig['short_entry']}\n"
    )
    tg_text = (
        f"{subject}\n"
        f"信号时间：{format_ts_ms(int(sig['signal_ts_ms']))}\n"
        f"方向：{side_cn}\n"
        f"信号类型：{stage_cn} | 等级：{level_tag}\n"
        f"入场：{format_price(entry, price_dp)} | 止损：{format_price(stop_val, price_dp)}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)} | "
        f"止盈二（{cfg.params.tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        f"大周期方向：{sig['bias']} | L1E={sig['long_entry']} S1E={sig['short_entry']} "
        f"L2E={sig.get('long_entry_l2', False)} S2E={sig.get('short_entry_l2', False)} "
        f"L3E={sig.get('long_entry_l3', False)} S3E={sig.get('short_entry_l3', False)}"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = send_telegram(cfg, tg_text)
    file_written = emit_local_alert(cfg, subject, body)
    state["last_alert_key"] = alert_key
    _record_alert(
        cfg=cfg,
        inst_state=state,
        signal_ts_ms=int(sig["signal_ts_ms"]),
        signal_confirm=bool(sig.get("signal_confirm", True)),
        side=side,
        level=int(level),
    )
    log(
        f"[{inst_id}] Alert: {side} level={level} | email_sent={email_sent} telegram_sent={telegram_sent} "
        f"file_written={file_written}"
    )


def run_test_alert(cfg: Config, test_inst: Optional[str] = None) -> int:
    inst_id = (test_inst or "").strip().upper() or (cfg.inst_ids[0] if cfg.inst_ids else "TEST-USDT-SWAP")
    now_ms = int(time.time() * 1000)
    price_dp = infer_price_decimals(entry=99999.99, stop=99888.88)
    subject = (
        f"[交易信号][测试][2级-中等] {inst_id} 做多 "
        f"{format_price(99999.99, price_dp)}（{cfg.ltf_bar}）"
    )
    entry = 99999.99
    stop_val = 99888.88
    risk_val, tp1_val, tp2_val = compute_alert_targets(
        "LONG",
        entry_price=entry,
        stop_price=stop_val,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=cfg.params.tp2_r_mult,
    )
    body = (
        f"信号时间：{format_ts_ms(now_ms)}\n"
        f"交易对：{inst_id}\n"
        f"方向：做多（测试）\n"
        f"信号等级：2级-中等\n"
        f"周期：{cfg.ltf_bar}\n"
        f"入场参考价（收盘）：{format_price(entry, price_dp)}\n"
        f"建议止损：{format_price(stop_val, price_dp)}\n"
        f"风险（1R）：{format_price(risk_val, price_dp)}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)}\n"
        f"止盈二（{cfg.params.tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        f"大周期方向：test\n"
        f"触发标记：LE=True SE=False\n"
        f"说明：这是手动测试提醒。\n"
    )
    tg_text = (
        f"{subject}\n"
        f"信号时间：{format_ts_ms(now_ms)}\n"
        "方向：做多（测试） | 等级：2级-中等\n"
        f"入场：{format_price(entry, price_dp)} | 止损：{format_price(stop_val, price_dp)}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)} | "
        f"止盈二（{cfg.params.tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        "说明：这是手动测试提醒。"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = send_telegram(cfg, tg_text)
    file_written = emit_local_alert(cfg, subject, body)
    log(
        f"[{inst_id}] Test alert done | email_sent={email_sent} telegram_sent={telegram_sent} "
        f"file_written={file_written}"
    )

    if cfg.alert_email_enabled or cfg.alert_tg_enabled:
        return 0 if (email_sent or telegram_sent or file_written) else 1
    return 0 if file_written or cfg.alert_local_sound else 1


def notify_no_open_timeout(
    cfg: Config,
    *,
    now_ts_ms: int,
    threshold_hours: float,
    elapsed_hours: float,
    last_open_ts_ms: int = 0,
    last_open_inst_id: str = "",
) -> None:
    now_utc = format_ts_ms(int(now_ts_ms))
    last_open_utc = format_ts_ms(int(last_open_ts_ms)) if int(last_open_ts_ms) > 0 else "N/A"
    threshold_text = f"{float(threshold_hours):.2f}".rstrip("0").rstrip(".")
    elapsed_text = f"{float(elapsed_hours):.2f}".rstrip("0").rstrip(".")
    inst_text = str(last_open_inst_id or "").strip().upper() or "N/A"

    subject = f"[运行监控][无开仓超时] 已连续 {elapsed_text}h 无新开仓（阈值 {threshold_text}h）"
    body = (
        f"监控时间：{now_utc}\n"
        f"事件：无开仓超时提醒\n"
        f"阈值：{threshold_text} 小时\n"
        f"当前连续无开仓：{elapsed_text} 小时\n"
        f"最近一次开仓时间：{last_open_utc}\n"
        f"最近一次开仓标的：{inst_text}\n"
        f"说明：程序仍在运行，当前主要是信号未触发。"
    )
    tg_text = (
        f"【运行监控】无开仓超时提醒\n"
        f"时间：{now_utc}\n"
        f"连续无开仓：{elapsed_text}h（阈值 {threshold_text}h）\n"
        f"最近开仓：{last_open_utc}\n"
        f"标的：{inst_text}"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = send_telegram(cfg, tg_text)
    file_written = emit_local_alert(cfg, subject, body)
    log(
        f"[Runtime] No-open timeout alert sent | email_sent={email_sent} "
        f"telegram_sent={telegram_sent} file_written={file_written}"
    )


def notify_trade_execution(
    cfg: Config,
    inst_id: str,
    side: str,
    size: float,
    sig: Dict[str, Any],
    order_resp: Optional[Dict[str, Any]] = None,
    planned_stop: Optional[float] = None,
    entry_level: int = 0,
) -> None:
    side_u = side.strip().upper()
    side_cn = "做多" if side_u == "LONG" else "做空" if side_u == "SHORT" else side_u
    close_px = float(sig.get("close", 0.0) or 0.0)
    stop_px_default = float(
        sig.get("long_stop" if side_u == "LONG" else "short_stop", close_px) or close_px
    )
    stop_px = float(planned_stop) if planned_stop and float(planned_stop) > 0 else stop_px_default
    strategy_tp2_r_mult = cfg.params.tp2_r_mult
    risk_val, tp1_val, tp2_val = compute_alert_targets(
        side_u,
        entry_price=close_px,
        stop_price=stop_px,
        tp1_r=cfg.params.tp1_r_mult,
        tp2_r=strategy_tp2_r_mult,
    )
    attach_tp_r = 0.0
    attach_tp_val = 0.0
    if cfg.attach_tpsl_on_entry:
        attach_tp_r = cfg.attach_tpsl_tp_r if cfg.params.enable_close else cfg.params.tp1_r_mult
        _, _, attach_tp_val = compute_alert_targets(
            side_u,
            entry_price=close_px,
            stop_price=stop_px,
            tp1_r=cfg.params.tp1_r_mult,
            tp2_r=attach_tp_r,
        )
    price_dp = infer_price_decimals(close_px, stop_px)

    ord_id = ""
    attach_tp_px = ""
    attach_sl_px = ""
    if isinstance(order_resp, dict):
        try:
            rows = order_resp.get("data")
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                ord_id = str(rows[0].get("ordId", "")).strip()
                attach_rows = rows[0].get("attachAlgoOrds")
                if isinstance(attach_rows, list) and attach_rows and isinstance(attach_rows[0], dict):
                    attach_tp_px = str(attach_rows[0].get("tpTriggerPx", "")).strip()
                    attach_sl_px = str(attach_rows[0].get("slTriggerPx", "")).strip()
        except Exception:
            ord_id = ""
            attach_tp_px = ""
            attach_sl_px = ""

    subject = (
        f"[交易执行][开仓成功] {inst_id} {side_cn} "
        f"价={format_price(close_px, price_dp)} 数量={round_size(float(size))}"
    )
    level_text = f"L{int(entry_level)}" if int(entry_level) > 0 else "N/A"
    margin_text = (
        f"{cfg.margin_usdt:g} USDT（{cfg.sizing_mode}）"
        if cfg.sizing_mode == "margin"
        else f"固定张数（{cfg.sizing_mode}）"
    )
    attach_text = (
        f"启用（tp_r={attach_tp_r}, trig={cfg.attach_tpsl_trigger_px_type}）"
        if cfg.attach_tpsl_on_entry
        else "关闭（脚本管理）"
    )
    if cfg.attach_tpsl_on_entry:
        final_attach_tp = attach_tp_px or format_price(attach_tp_val, price_dp)
        final_attach_sl = attach_sl_px or format_price(stop_px, price_dp)
        exchange_tpsl_text = f"TP={final_attach_tp} | SL={final_attach_sl}"
    else:
        exchange_tpsl_text = "N/A"
    body = (
        f"执行时间：{format_ts_ms(int(time.time() * 1000))}\n"
        f"信号时间：{format_ts_ms(int(sig.get('signal_ts_ms', 0) or 0))}\n"
        f"交易对：{inst_id}\n"
        f"动作：开仓\n"
        f"方向：{side_cn}\n"
        f"执行等级：{level_text}\n"
        f"参考价：{format_price(close_px, price_dp)}\n"
        f"建议止损：{format_price(stop_px, price_dp)}\n"
        f"风险（1R）：{format_price(risk_val, price_dp)}\n"
        f"止盈一（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)}\n"
        f"止盈二（{strategy_tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        f"下单数量：{round_size(float(size))}\n"
        f"订单号：{ord_id or 'N/A'}\n"
        f"交易所附带止盈（R）：{attach_tp_r if cfg.attach_tpsl_on_entry else 'N/A'}\n"
        f"交易所附带TP/SL：{exchange_tpsl_text}\n"
        f"保证金：{margin_text}\n"
        f"杠杆：{cfg.leverage:g}x\n"
        f"附带TP/SL设置：{attach_text}\n"
        f"模式：paper={cfg.paper} dry_run={cfg.dry_run} td_mode={cfg.td_mode} pos_mode={cfg.pos_mode}\n"
    )
    tg_text = (
        f"{subject}\n"
        f"执行时间：{format_ts_ms(int(time.time() * 1000))}\n"
        f"方向：{side_cn} | 等级：{level_text} | 数量：{round_size(float(size))}\n"
        f"入场：{format_price(close_px, price_dp)} | 止损：{format_price(stop_px, price_dp)}\n"
        f"止盈1（{cfg.params.tp1_r_mult}R）：{format_price(tp1_val, price_dp)} | "
        f"止盈2（{strategy_tp2_r_mult}R）：{format_price(tp2_val, price_dp)}\n"
        f"交易所附带TP/SL：{exchange_tpsl_text}\n"
        f"交易所附带止盈R：{attach_tp_r if cfg.attach_tpsl_on_entry else 'N/A'}\n"
        f"保证金：{margin_text} | 杠杆：{cfg.leverage:g}x\n"
        f"订单号：{ord_id or 'N/A'}"
    )

    email_sent = send_email(cfg, subject, body) if cfg.alert_email_enabled else False
    telegram_sent = False
    if cfg.alert_tg_trade_exec_enabled:
        telegram_sent = send_telegram(cfg, tg_text)
    else:
        log(f"[{inst_id}] Trade notify telegram skipped by ALERT_TG_TRADE_EXEC_ENABLED=0")
    file_written = emit_local_alert(cfg, subject, body)
    log(
        f"[{inst_id}] Trade notify: OPEN {side_u} size={round_size(float(size))} "
        f"| email_sent={email_sent} telegram_sent={telegram_sent} file_written={file_written}"
    )
