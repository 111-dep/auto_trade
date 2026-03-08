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
RUNNER="${ROOT_DIR}/scripts/ops/run_expectation_watch.sh"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
ROLLING_HOURS="24"
INTERVAL_MIN="10"
OUT_DIR="${ROOT_DIR}/logs/expectation_watch"
STATE_FILE=""
WITH_BILLS=0
WITH_EXCHANGE_HISTORY=1
WITH_EQUITY=1
TELEGRAM=1
TELEGRAM_RECOVER=0

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

CRON_TAG="# OKX_EXPECTATION_WATCH"
PRINT_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  setup_expectation_watch_cron.sh [options]

Default:
  Install deviation watch cron every 10 minutes.

Options:
  --interval-min N               Cron interval in minutes (1-59, default: 10)
  --env PATH                     Env file path
  --tz-offset +08:00             Timezone used by rolling recap
  --rolling-hours N              Rolling window hours (default: 24)
  --out-dir PATH                 Output dir for watch logs/json/state
  --state-file PATH              State file override
  --with-bills                   Include bills reconcile
  --no-exchange-history          Disable exchange positions-history pull
  --no-equity                    Disable equity pull
  --telegram                     Enable telegram alerts (default: on)
  --no-telegram                  Disable telegram alerts
  --telegram-recover             Send telegram when deviation recovers

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
  --enable-bills-hard-alert      Treat bills hard alert as trigger
  --print-only                   Only print cron line, do not install
  -h, --help                     Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval-min)
      INTERVAL_MIN="${2:-}"
      shift 2
      ;;
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
    --no-telegram)
      TELEGRAM=0
      shift
      ;;
    --telegram-recover)
      TELEGRAM_RECOVER=1
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
    --print-only)
      PRINT_ONLY=1
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

if [[ ! -x "${RUNNER}" ]]; then
  echo "Expectation watch runner not found or not executable: ${RUNNER}" >&2
  exit 1
fi
if ! [[ "${INTERVAL_MIN}" =~ ^[0-9]+$ ]]; then
  echo "--interval-min must be integer, got: ${INTERVAL_MIN}" >&2
  exit 1
fi
if ((10#${INTERVAL_MIN} < 1 || 10#${INTERVAL_MIN} > 59)); then
  echo "--interval-min out of range [1,59], got: ${INTERVAL_MIN}" >&2
  exit 1
fi

mkdir -p "${OUT_DIR}"
if [[ -z "${STATE_FILE}" ]]; then
  STATE_FILE="${OUT_DIR}/state.json"
fi

ARGS=(
  --env "${ENV_FILE}"
  --tz-offset "${TZ_OFFSET}"
  --rolling-hours "${ROLLING_HOURS}"
  --out-dir "${OUT_DIR}"
  --state-file "${STATE_FILE}"
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
if [[ "${WITH_BILLS}" == "1" ]]; then
  ARGS+=(--with-bills)
fi
if [[ "${WITH_EXCHANGE_HISTORY}" == "0" ]]; then
  ARGS+=(--no-exchange-history)
fi
if [[ "${WITH_EQUITY}" == "0" ]]; then
  ARGS+=(--no-equity)
fi
if [[ "${TELEGRAM}" == "1" ]]; then
  ARGS+=(--telegram)
fi
if [[ "${TELEGRAM_RECOVER}" == "1" ]]; then
  ARGS+=(--telegram-recover)
fi
if [[ "${ENABLE_BILLS_HARD_ALERT}" == "1" ]]; then
  ARGS+=(--enable-bills-hard-alert)
fi
if [[ "${DRAWDOWN_ONLY}" == "1" ]]; then
  ARGS+=(--drawdown-only)
fi

CRON_CMD="${RUNNER} ${ARGS[*]} >> ${OUT_DIR}/cron.log 2>&1"
CRON_LINE="*/${INTERVAL_MIN} * * * * ${CRON_CMD} ${CRON_TAG}"

if [[ "${PRINT_ONLY}" == "1" ]]; then
  echo "${CRON_LINE}"
  exit 0
fi

TMP_FILE="$(mktemp)"
trap 'rm -f "${TMP_FILE}"' EXIT
(crontab -l 2>/dev/null || true) | sed "/${CRON_TAG//\//\\/}/d" > "${TMP_FILE}"
echo "${CRON_LINE}" >> "${TMP_FILE}"
crontab "${TMP_FILE}"

echo "Installed cron:"
echo "${CRON_LINE}"
echo "Check with: crontab -l | grep OKX_EXPECTATION_WATCH"
