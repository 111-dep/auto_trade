#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${ROOT_DIR}/run_interleaved_backtest_2y.py"

ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
BARS=70080
RISK_FRAC="0.005"
FEE_RATE="0.0010"
SLIPPAGE_BPS="3.0"
STOP_EXTRA_R="0.05"
TP_HAIRCUT_R="0.04"
MISS_PROB="0.06"
TITLE="2Y ManagedExit 实盘拟合(中严之间)"
INST_IDS=""
TRADES_CSV=""
SEND_TG=0
MANAGED_EXIT=1
PESSIMISTIC=0
CACHE_ONLY=1
CACHE_TTL_SECONDS=315360000
SAVE_TAG=""
SAVE_DIR="${ROOT_DIR}/logs/backtest_snapshots"

usage() {
  cat <<'EOF'
Usage:
  run_backtest_2y_cached.sh [options]

Options:
  --env PATH                 Env file path (default: ./okx_auto_trader.env)
  --inst-ids CSV             Override instruments (default: use env OKX_INST_IDS)
  --bars N                   LTF bars (default: 70080)
  --risk-frac X              Risk fraction (default: 0.005)
  --fee-rate X               Fee rate (default: 0.0010)
  --slippage-bps X           Slippage bps (default: 3.0)
  --stop-extra-r X           Stop extra R (default: 0.05)
  --tp-haircut-r X           TP haircut R (default: 0.04)
  --miss-prob X              Miss probability (default: 0.06)
  --title TEXT               Result title
  --dump-trades-csv PATH     Dump accepted trades to CSV (for Monte Carlo)
  --send-telegram            Enable telegram send (default: off)
  --tp1-only                 Disable managed exit (TP1 only)
  --managed-exit             Enable managed exit (default)
  --pessimistic              Use runner built-in pessimistic fallback
  --allow-fetch              Skip cache sufficiency precheck (may fetch from network)
  --cache-ttl-seconds N      Cache TTL override (default: 315360000)
  --save-tag NAME            Save this run as a snapshot (log+trades+summary index)
  --save-dir PATH            Snapshot output directory (default: ./logs/backtest_snapshots)
  -h, --help                 Show help

Notes:
  - Default is cache-only mode: script first verifies local cache is enough.
  - If cache is insufficient, it exits before running backtest (no long wait on fetch).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --inst-ids)
      INST_IDS="${2:-}"
      shift 2
      ;;
    --bars)
      BARS="${2:-}"
      shift 2
      ;;
    --risk-frac)
      RISK_FRAC="${2:-}"
      shift 2
      ;;
    --fee-rate)
      FEE_RATE="${2:-}"
      shift 2
      ;;
    --slippage-bps)
      SLIPPAGE_BPS="${2:-}"
      shift 2
      ;;
    --stop-extra-r)
      STOP_EXTRA_R="${2:-}"
      shift 2
      ;;
    --tp-haircut-r)
      TP_HAIRCUT_R="${2:-}"
      shift 2
      ;;
    --miss-prob)
      MISS_PROB="${2:-}"
      shift 2
      ;;
    --title)
      TITLE="${2:-}"
      shift 2
      ;;
    --dump-trades-csv)
      TRADES_CSV="${2:-}"
      shift 2
      ;;
    --send-telegram)
      SEND_TG=1
      shift
      ;;
    --tp1-only)
      MANAGED_EXIT=0
      shift
      ;;
    --managed-exit)
      MANAGED_EXIT=1
      shift
      ;;
    --pessimistic)
      PESSIMISTIC=1
      shift
      ;;
    --allow-fetch)
      CACHE_ONLY=0
      shift
      ;;
    --cache-ttl-seconds)
      CACHE_TTL_SECONDS="${2:-}"
      shift 2
      ;;
    --save-tag)
      SAVE_TAG="${2:-}"
      shift 2
      ;;
    --save-dir)
      SAVE_DIR="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ ! -f "${RUNNER}" ]]; then
  echo "Runner not found: ${RUNNER}" >&2
  exit 1
fi
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found: ${ENV_FILE}" >&2
  exit 1
fi
if ! [[ "${BARS}" =~ ^[0-9]+$ ]]; then
  echo "--bars must be integer, got: ${BARS}" >&2
  exit 1
fi
if ! [[ "${CACHE_TTL_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "--cache-ttl-seconds must be integer, got: ${CACHE_TTL_SECONDS}" >&2
  exit 1
fi
if [[ -n "${SAVE_TAG}" && -z "${SAVE_DIR}" ]]; then
  echo "--save-dir cannot be empty when --save-tag is set" >&2
  exit 1
fi

export OKX_HISTORY_CACHE_ENABLED=1
export OKX_HISTORY_CACHE_TTL_SECONDS="${CACHE_TTL_SECONDS}"
if [[ -n "${INST_IDS}" ]]; then
  export OKX_INST_IDS="${INST_IDS}"
fi
if [[ "${SEND_TG}" -eq 1 ]]; then
  export ALERT_TG_ENABLED=1
else
  export ALERT_TG_ENABLED=0
fi

if [[ "${CACHE_ONLY}" -eq 1 ]]; then
  echo "[Precheck] cache-only mode is ON, checking local history cache..."
  python3 - "${ENV_FILE}" "${BARS}" "${INST_IDS}" <<'PY'
import math
import sys
from typing import List

from okx_trader.common import bar_to_seconds, load_dotenv
from okx_trader.config import get_strategy_profile_id, get_strategy_profile_ids, read_config
from okx_trader.okx_client import OKXClient

env_file = sys.argv[1]
bars = max(1200, int(sys.argv[2]))
inst_override = str(sys.argv[3] or "").strip()

load_dotenv(env_file)
cfg = read_config(None)
if inst_override:
    cfg.inst_ids = [x.strip().upper() for x in inst_override.split(",") if x.strip()]
inst_ids: List[str] = list(cfg.inst_ids)
if not inst_ids:
    print("[Precheck] No instruments resolved.", flush=True)
    raise SystemExit(2)

profile_ids_by_inst = {inst: get_strategy_profile_ids(cfg, inst) for inst in inst_ids}
profile_by_inst = {
    inst: (ids[0] if ids else get_strategy_profile_id(cfg, inst))
    for inst, ids in profile_ids_by_inst.items()
}
all_params = []
for inst in inst_ids:
    ids = profile_ids_by_inst.get(inst) or [profile_by_inst.get(inst, "DEFAULT")]
    for pid in ids:
        all_params.append(cfg.strategy_profiles.get(pid, cfg.params))

ltf_s = bar_to_seconds(cfg.ltf_bar)
loc_s = bar_to_seconds(cfg.loc_bar)
htf_s = bar_to_seconds(cfg.htf_bar)
ratio_loc = max(1, int(math.ceil(loc_s / ltf_s)))
ratio_htf = max(1, int(math.ceil(htf_s / ltf_s)))
need_ltf = bars + 300
if all_params:
    max_loc_lookback = max(p.loc_lookback for p in all_params)
    max_htf_ema_slow = max(p.htf_ema_slow_len for p in all_params)
else:
    max_loc_lookback = cfg.params.loc_lookback
    max_htf_ema_slow = cfg.params.htf_ema_slow_len
need_loc = int(math.ceil(need_ltf / ratio_loc)) + max_loc_lookback + 120
need_htf = int(math.ceil(need_ltf / ratio_htf)) + max_htf_ema_slow + 120

client = OKXClient(cfg)
missing = []
for inst in inst_ids:
    for bar, need in ((cfg.htf_bar, need_htf), (cfg.loc_bar, need_loc), (cfg.ltf_bar, need_ltf)):
        got = client._load_history_cache(inst, bar, need)  # intentionally use same parsing/path logic
        n = len(got) if got is not None else 0
        if got is None or n < need:
            missing.append((inst, bar, need, n))

if missing:
    print("[Precheck] cache insufficient, abort (no backtest started):", flush=True)
    for inst, bar, need, got in missing:
        print(f"  - {inst} {bar}: need>={need}, cached={got}", flush=True)
    print("[Precheck] hint: use --allow-fetch to permit online补拉，或先补齐本地缓存。", flush=True)
    raise SystemExit(3)

print(
    f"[Precheck] cache OK for {len(inst_ids)} inst(s) | "
    f"need: {cfg.htf_bar}={need_htf}, {cfg.loc_bar}={need_loc}, {cfg.ltf_bar}={need_ltf}",
    flush=True,
)
PY
fi

RESULT_LOG=""
INDEX_CSV=""
if [[ -n "${SAVE_TAG}" ]]; then
  mkdir -p "${SAVE_DIR}"
  TS_UTC="$(date -u +%Y%m%d_%H%M%S)"
  SAFE_TAG="$(echo "${SAVE_TAG}" | sed 's/[^A-Za-z0-9._-]/_/g')"
  RESULT_LOG="${SAVE_DIR}/${TS_UTC}_${SAFE_TAG}.log"
  if [[ -z "${TRADES_CSV}" ]]; then
    TRADES_CSV="${SAVE_DIR}/${TS_UTC}_${SAFE_TAG}_trades.csv"
  fi
  INDEX_CSV="${SAVE_DIR}/index.csv"
  echo "[Snapshot] tag=${SAFE_TAG}"
  echo "[Snapshot] log=${RESULT_LOG}"
  echo "[Snapshot] trades=${TRADES_CSV}"
fi

CMD=(python3 -u "${RUNNER}" --env "${ENV_FILE}" --bars "${BARS}" --risk-frac "${RISK_FRAC}" --fee-rate "${FEE_RATE}" --slippage-bps "${SLIPPAGE_BPS}" --stop-extra-r "${STOP_EXTRA_R}" --tp-haircut-r "${TP_HAIRCUT_R}" --miss-prob "${MISS_PROB}" --title "${TITLE}")
if [[ "${MANAGED_EXIT}" -eq 1 ]]; then
  CMD+=(--managed-exit)
fi
if [[ "${PESSIMISTIC}" -eq 1 ]]; then
  CMD+=(--pessimistic)
fi
if [[ -n "${TRADES_CSV}" ]]; then
  CMD+=(--dump-trades-csv "${TRADES_CSV}")
fi

echo "[Run] ${CMD[*]}"
if [[ -n "${RESULT_LOG}" ]]; then
  "${CMD[@]}" | tee "${RESULT_LOG}"
else
  "${CMD[@]}"
fi

if [[ -n "${RESULT_LOG}" ]]; then
  SNAP_INST_IDS="${INST_IDS}"
  if [[ -z "${SNAP_INST_IDS}" ]]; then
    SNAP_INST_IDS="$(awk -F= '/^OKX_INST_IDS=/{print $2}' "${ENV_FILE}" | tail -n1 | tr -d '"' || true)"
  fi
  python3 - "${INDEX_CSV}" "${RESULT_LOG}" "${TRADES_CSV}" "${SAVE_TAG}" "${TITLE}" "${BARS}" "${RISK_FRAC}" "${FEE_RATE}" "${SLIPPAGE_BPS}" "${STOP_EXTRA_R}" "${TP_HAIRCUT_R}" "${MISS_PROB}" "${MANAGED_EXIT}" "${PESSIMISTIC}" "${SNAP_INST_IDS}" <<'PY'
import csv
import os
import re
import sys
from datetime import datetime, timezone

(
    index_csv,
    result_log,
    trades_csv,
    save_tag,
    title,
    bars,
    risk_frac,
    fee_rate,
    slippage_bps,
    stop_extra_r,
    tp_haircut_r,
    miss_prob,
    managed_exit,
    pessimistic,
    inst_ids,
) = sys.argv[1:]

with open(result_log, "r", encoding="utf-8", errors="ignore") as f:
    text = f.read()

def _find(pat: str):
    m = re.search(pat, text, flags=re.MULTILINE)
    return m.groups() if m else None

start_equity = ""
final_equity = ""
return_pct = ""
maxdd_pct = ""
signals = ""
wins = ""
stops = ""
win_rate_pct = ""
avg_r = ""

compound = _find(r"复利：risk=[^\n]*start=([0-9.]+)\s+final=([0-9.]+)\s+return=([+-]?[0-9.]+)%\s+maxDD=([0-9.]+)%")
if compound:
    start_equity, final_equity, return_pct, maxdd_pct = compound
res = _find(r"结果：signals=(\d+)\s+wins=(\d+)\s+stops=(\d+)\s+win_rate=([0-9.]+)%\s+avgR=([+-]?[0-9.]+)")
if res:
    signals, wins, stops, win_rate_pct, avg_r = res

rows = []
if os.path.exists(trades_csv):
    with open(trades_csv, "r", newline="", encoding="utf-8") as f:
        dr = csv.DictReader(f)
        for row in dr:
            try:
                row["r"] = float(row["r"])
            except Exception:
                continue
            rows.append(row)

payoff_r = ""
profit_factor_r = ""
if rows:
    wr = [x["r"] for x in rows if x["r"] > 0]
    lr = [x["r"] for x in rows if x["r"] < 0]
    if not avg_r:
        avg_r = f"{(sum(x['r'] for x in rows) / len(rows)):.6f}"
    if not signals:
        signals = str(len(rows))
    if not wins:
        wins = str(len(wr))
    if not stops:
        stops = str(len(lr))
    if wr and lr:
        avg_win = sum(wr) / len(wr)
        avg_loss = sum(lr) / len(lr)
        payoff_r = f"{(avg_win / abs(avg_loss)):.6f}" if avg_loss != 0 else ""
        gross_profit = sum(wr)
        gross_loss = abs(sum(lr))
        profit_factor_r = f"{(gross_profit / gross_loss):.6f}" if gross_loss != 0 else ""
        if not win_rate_pct:
            win_rate_pct = f"{(len(wr) / len(rows) * 100):.4f}"

row = {
    "saved_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "tag": save_tag,
    "title": title,
    "bars": bars,
    "inst_ids": inst_ids,
    "managed_exit": managed_exit,
    "pessimistic": pessimistic,
    "risk_frac": risk_frac,
    "fee_rate": fee_rate,
    "slippage_bps": slippage_bps,
    "stop_extra_r": stop_extra_r,
    "tp_haircut_r": tp_haircut_r,
    "miss_prob": miss_prob,
    "start_equity": start_equity,
    "final_equity": final_equity,
    "return_pct": return_pct,
    "maxdd_pct": maxdd_pct,
    "signals": signals,
    "wins": wins,
    "stops": stops,
    "win_rate_pct": win_rate_pct,
    "avg_r": avg_r,
    "payoff_r": payoff_r,
    "profit_factor_r": profit_factor_r,
    "result_log": result_log,
    "trades_csv": trades_csv,
}

fieldnames = list(row.keys())
need_header = (not os.path.exists(index_csv)) or os.path.getsize(index_csv) == 0
with open(index_csv, "a", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if need_header:
        w.writeheader()
    w.writerow(row)

print(f"[Snapshot] indexed: {index_csv}")
PY
fi
