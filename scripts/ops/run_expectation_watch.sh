#!/usr/bin/env bash
set -euo pipefail

resolve_root_dir() {
  if [[ -n "${OKX_SUITE_ROOT:-}" ]]; then
    printf '%s\n' "${OKX_SUITE_ROOT}"
    return
  fi
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [[ -f "${script_dir}/okx_auto_trader.py" ]]; then
    printf '%s\n' "${script_dir}"
    return
  fi
  if [[ -f "${script_dir}/../okx_auto_trader.py" ]]; then
    (cd "${script_dir}/.." && pwd)
    return
  fi
  if [[ -f "${script_dir}/../../okx_auto_trader.py" ]]; then
    (cd "${script_dir}/../.." && pwd)
    return
  fi
  printf '%s\n' "${script_dir}"
}

ROOT_DIR="$(resolve_root_dir)"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
ROLLING_HOURS="24"
PRIMARY_SOURCE="exchange_first"
TOP_N="5"
OUT_DIR="${ROOT_DIR}/logs/expectation_watch"
OUT_JSON="${OUT_DIR}/latest.json"
STATE_FILE="${OUT_DIR}/state.json"

WITH_BILLS=0
WITH_EXCHANGE_HISTORY=1
WITH_EQUITY=1
TELEGRAM=0
TELEGRAM_RECOVER=0
PRINT_STATUS=1
EXIT_ON_ALERT=0

COOLDOWN_MIN="30"
MAX_DRAWDOWN_PCT="23.0"
SEED_PEAK_EQUITY="0"
DRAWDOWN_ONLY=1
MAX_EXCH_LOSS_STREAK="10"
MAX_JOURNAL_LOSS_STREAK="8"
MAX_BATCH_LOSS_STREAK="5"
MIN_EQUITY_DELTA_PCT="-5.0"
MIN_EXCHANGE_PNL_USDT="-120.0"
MIN_EXCHANGE_ROWS="8"
MAX_RUNTIME_ERROR="2"
MAX_RUNTIME_LOOP_ERROR="2"
ENABLE_BILLS_HARD_ALERT=0

usage() {
  cat <<'EOF'
Usage:
  run_expectation_watch.sh [options]

Description:
  Build a rolling recap snapshot and detect expectation deviation.
  Telegram is sent only when deviation is triggered (with cooldown dedupe).

Options:
  --env PATH                     Env file path (default: ./okx_auto_trader.env)
  --tz-offset +08:00             Timezone for rolling window (default: +08:00)
  --rolling-hours N              Rolling window hours (default: 24)
  --primary-source MODE          bills_auto/journal/exchange_first (default: exchange_first)
  --top-n N                      Top winners/losers for recap generation (default: 5)
  --out-dir PATH                 Output dir for watch artifacts (default: ./logs/expectation_watch)
  --state-file PATH              Dedupe state file (default: <out-dir>/state.json)
  --with-bills                   Include bills reconcile in rolling recap
  --no-exchange-history          Disable exchange positions-history pull
  --no-equity                    Disable equity pull
  --telegram                     Send telegram on deviation
  --telegram-recover             Send telegram when status recovers
  --no-print                     Disable watch status line
  --exit-on-alert                Return exit code 2 when deviation detected

Deviation thresholds:
  --cooldown-min N               Alert cooldown minutes (default: 30)
  --max-drawdown-pct X           Trigger if live drawdown >= X% (default: 23.0)
  --seed-peak-equity X           Optional peak equity seed in USDT (default: 0)
  --disable-drawdown-only        Also enable legacy triggers (streak/pnl/runtime)
  --max-exch-loss-streak N       Trigger if exchange loss streak >= N (default: 10)
  --max-journal-loss-streak N    Trigger if journal loss streak >= N (default: 8)
  --max-batch-loss-streak N      Trigger if batch loss streak >= N (default: 5)
  --min-equity-delta-pct X       Trigger if equity delta pct <= X (default: -5.0)
  --min-exchange-pnl-usdt X      Trigger if exchange pnl <= X (default: -120.0)
  --min-exchange-rows N          Require at least N exchange rows (default: 8)
  --max-runtime-error N          Trigger if runtime error > N (default: 2)
  --max-runtime-loop-error N     Trigger if runtime loop_error > N (default: 2)
  --enable-bills-hard-alert      Treat bills hard alert as deviation trigger
  -h, --help                     Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --tz-offset)
      TZ_OFFSET="${2:-}"
      shift 2
      ;;
    --rolling-hours)
      ROLLING_HOURS="${2:-}"
      shift 2
      ;;
    --primary-source)
      PRIMARY_SOURCE="${2:-exchange_first}"
      shift 2
      ;;
    --top-n)
      TOP_N="${2:-5}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --state-file)
      STATE_FILE="${2:-}"
      shift 2
      ;;
    --with-bills)
      WITH_BILLS=1
      shift
      ;;
    --no-exchange-history)
      WITH_EXCHANGE_HISTORY=0
      shift
      ;;
    --no-equity)
      WITH_EQUITY=0
      shift
      ;;
    --telegram)
      TELEGRAM=1
      shift
      ;;
    --telegram-recover)
      TELEGRAM_RECOVER=1
      shift
      ;;
    --no-print)
      PRINT_STATUS=0
      shift
      ;;
    --exit-on-alert)
      EXIT_ON_ALERT=1
      shift
      ;;
    --cooldown-min)
      COOLDOWN_MIN="${2:-}"
      shift 2
      ;;
    --max-drawdown-pct)
      MAX_DRAWDOWN_PCT="${2:-}"
      shift 2
      ;;
    --seed-peak-equity)
      SEED_PEAK_EQUITY="${2:-}"
      shift 2
      ;;
    --disable-drawdown-only)
      DRAWDOWN_ONLY=0
      shift
      ;;
    --max-exch-loss-streak)
      MAX_EXCH_LOSS_STREAK="${2:-}"
      shift 2
      ;;
    --max-journal-loss-streak)
      MAX_JOURNAL_LOSS_STREAK="${2:-}"
      shift 2
      ;;
    --max-batch-loss-streak)
      MAX_BATCH_LOSS_STREAK="${2:-}"
      shift 2
      ;;
    --min-equity-delta-pct)
      MIN_EQUITY_DELTA_PCT="${2:-}"
      shift 2
      ;;
    --min-exchange-pnl-usdt)
      MIN_EXCHANGE_PNL_USDT="${2:-}"
      shift 2
      ;;
    --min-exchange-rows)
      MIN_EXCHANGE_ROWS="${2:-}"
      shift 2
      ;;
    --max-runtime-error)
      MAX_RUNTIME_ERROR="${2:-}"
      shift 2
      ;;
    --max-runtime-loop-error)
      MAX_RUNTIME_LOOP_ERROR="${2:-}"
      shift 2
      ;;
    --enable-bills-hard-alert)
      ENABLE_BILLS_HARD_ALERT=1
      shift
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

mkdir -p "${OUT_DIR}"
OUT_JSON="${OUT_DIR}/latest.json"
if [[ -z "${STATE_FILE}" ]]; then
  STATE_FILE="${OUT_DIR}/state.json"
fi

DAILY_CMD=(python3 -u "${ROOT_DIR}/daily_recap.py"
  --env "${ENV_FILE}"
  --tz-offset "${TZ_OFFSET}"
  --rolling-hours "${ROLLING_HOURS}"
  --primary-source "${PRIMARY_SOURCE}"
  --top-n "${TOP_N}"
  --out-json "${OUT_JSON}"
)
if [[ "${WITH_BILLS}" == "1" ]]; then
  DAILY_CMD+=(--with-bills)
fi
if [[ "${WITH_EXCHANGE_HISTORY}" == "1" ]]; then
  DAILY_CMD+=(--with-exchange-history)
fi
if [[ "${WITH_EQUITY}" == "1" ]]; then
  DAILY_CMD+=(--with-equity)
fi

WATCH_CMD=(python3 -u "${ROOT_DIR}/scripts/ops/expectation_watch.py"
  --report-json "${OUT_JSON}"
  --state-file "${STATE_FILE}"
  --env "${ENV_FILE}"
  --cooldown-min "${COOLDOWN_MIN}"
  --max-drawdown-pct "${MAX_DRAWDOWN_PCT}"
  --seed-peak-equity "${SEED_PEAK_EQUITY}"
  --max-exch-loss-streak "${MAX_EXCH_LOSS_STREAK}"
  --max-journal-loss-streak "${MAX_JOURNAL_LOSS_STREAK}"
  --max-batch-loss-streak "${MAX_BATCH_LOSS_STREAK}"
  --min-equity-delta-pct "${MIN_EQUITY_DELTA_PCT}"
  --min-exchange-pnl-usdt "${MIN_EXCHANGE_PNL_USDT}"
  --min-exchange-rows "${MIN_EXCHANGE_ROWS}"
  --max-runtime-error "${MAX_RUNTIME_ERROR}"
  --max-runtime-loop-error "${MAX_RUNTIME_LOOP_ERROR}"
)
if [[ "${TELEGRAM}" == "1" ]]; then
  WATCH_CMD+=(--telegram)
fi
if [[ "${TELEGRAM_RECOVER}" == "1" ]]; then
  WATCH_CMD+=(--telegram-recover)
fi
if [[ "${PRINT_STATUS}" == "1" ]]; then
  WATCH_CMD+=(--print)
fi
if [[ "${EXIT_ON_ALERT}" == "1" ]]; then
  WATCH_CMD+=(--exit-on-alert)
fi
if [[ "${ENABLE_BILLS_HARD_ALERT}" == "1" ]]; then
  WATCH_CMD+=(--enable-bills-hard-alert)
fi
if [[ "${DRAWDOWN_ONLY}" == "1" ]]; then
  WATCH_CMD+=(--drawdown-only)
fi

"${DAILY_CMD[@]}"
"${WATCH_CMD[@]}"
