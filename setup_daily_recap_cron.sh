#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${ROOT_DIR}/run_daily_recap.sh"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
HHMM="00:10"
WITH_BILLS=0
WITH_EXCHANGE_HISTORY=0
WITH_EQUITY=0
ROLLING_HOURS=""
TELEGRAM=0
CRON_TAG="# OKX_DAILY_RECAP"
PRINT_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  setup_daily_recap_cron.sh [options]

Default:
  Install a daily cron at 00:10 local machine time.

Options:
  --time HH:MM           Cron run time (default: 00:10)
  --env PATH             Env file path for recap runner
  --tz-offset +08:00     Recap timezone offset
  --rolling-hours N      Rolling window hours (e.g. 24)
  --with-bills           Enable bills reconcile in daily cron
  --with-exchange-history
                        Enable exchange positions-history stats in daily cron
  --with-equity          Include current account equity in recap
  --telegram             Send short summary to telegram
  --print-only           Only print cron line, do not install
  -h, --help             Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --time)
      HHMM="${2:-}"
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
    --with-bills)
      WITH_BILLS=1
      shift
      ;;
    --with-exchange-history)
      WITH_EXCHANGE_HISTORY=1
      shift
      ;;
    --with-equity)
      WITH_EQUITY=1
      shift
      ;;
    --telegram)
      TELEGRAM=1
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

if [[ ! "${HHMM}" =~ ^[0-9]{2}:[0-9]{2}$ ]]; then
  echo "--time must be HH:MM, got: ${HHMM}" >&2
  exit 1
fi

HOUR="${HHMM%:*}"
MINUTE="${HHMM#*:}"
if ((10#${HOUR} > 23)) || ((10#${MINUTE} > 59)); then
  echo "--time out of range: ${HHMM}" >&2
  exit 1
fi

ARGS=(--env "${ENV_FILE}" --tz-offset "${TZ_OFFSET}" --no-print)
if [[ -n "${ROLLING_HOURS}" ]]; then
  ARGS+=(--rolling-hours "${ROLLING_HOURS}")
fi
if [[ "${WITH_BILLS}" == "1" ]]; then
  ARGS+=(--with-bills)
fi
if [[ "${WITH_EXCHANGE_HISTORY}" == "1" ]]; then
  ARGS+=(--with-exchange-history)
fi
if [[ "${WITH_EQUITY}" == "1" ]]; then
  ARGS+=(--with-equity)
fi
if [[ "${TELEGRAM}" == "1" ]]; then
  ARGS+=(--telegram)
fi

CRON_CMD="${RUNNER} ${ARGS[*]} >> ${ROOT_DIR}/logs/daily_recap/cron.log 2>&1"
CRON_LINE="${MINUTE} ${HOUR} * * * ${CRON_CMD} ${CRON_TAG}"

if [[ "${PRINT_ONLY}" == "1" ]]; then
  echo "${CRON_LINE}"
  exit 0
fi

mkdir -p "${ROOT_DIR}/logs/daily_recap"
TMP_FILE="$(mktemp)"
trap 'rm -f "${TMP_FILE}"' EXIT
(crontab -l 2>/dev/null || true) | sed "/${CRON_TAG//\//\\/}/d" > "${TMP_FILE}"
echo "${CRON_LINE}" >> "${TMP_FILE}"
crontab "${TMP_FILE}"

echo "Installed cron:"
echo "${CRON_LINE}"
echo "Check with: crontab -l | grep OKX_DAILY_RECAP"
