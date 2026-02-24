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
RUNNER="${ROOT_DIR}/scripts/ops/run_weekly_recap.sh"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
HHMM="07:05"
DOW="1"
ROLLING_HOURS="168"
WITH_BILLS=0
WITH_EXCHANGE_HISTORY=0
WITH_EQUITY=0
TELEGRAM=0
BILLS_UNMAPPED_MAX_RATIO="0.35"
BILLS_ALERT_UNMAPPED_RATIO="0.50"
BILLS_ALERT_MIN_SELECTED="20"
CRON_TAG="# OKX_WEEKLY_RECAP"
PRINT_ONLY=0

usage() {
  cat <<'EOF'
Usage:
  setup_weekly_recap_cron.sh [options]

Default:
  Install weekly recap cron on Monday 07:05 local machine time.

Options:
  --time HH:MM                   Cron run time (default: 07:05)
  --dow D                        Day of week (0-7, 1=Mon, 0/7=Sun; default: 1)
  --env PATH                     Env file path for recap runner
  --tz-offset +08:00             Recap timezone offset
  --rolling-hours N              Rolling window hours (default: 168)
  --with-bills                   Enable bills reconcile in weekly cron
  --with-exchange-history        Enable exchange positions-history stats in weekly cron
  --with-equity                  Include current account equity in recap
  --bills-unmapped-max-ratio X   Fallback threshold (default: 0.35)
  --bills-alert-unmapped-ratio X Hard-alert threshold (default: 0.50)
  --bills-alert-min-selected N   Min selected rows before hard alert (default: 20)
  --telegram                     Send short summary to telegram
  --print-only                   Only print cron line, do not install
  -h, --help                     Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --time)
      HHMM="${2:-}"
      shift 2
      ;;
    --dow)
      DOW="${2:-}"
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
    --bills-unmapped-max-ratio)
      BILLS_UNMAPPED_MAX_RATIO="${2:-}"
      shift 2
      ;;
    --bills-alert-unmapped-ratio)
      BILLS_ALERT_UNMAPPED_RATIO="${2:-}"
      shift 2
      ;;
    --bills-alert-min-selected)
      BILLS_ALERT_MIN_SELECTED="${2:-}"
      shift 2
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

if [[ ! -x "${RUNNER}" ]]; then
  echo "Weekly recap runner not found or not executable: ${RUNNER}" >&2
  exit 1
fi
if [[ ! "${HHMM}" =~ ^[0-9]{2}:[0-9]{2}$ ]]; then
  echo "--time must be HH:MM, got: ${HHMM}" >&2
  exit 1
fi
if ! [[ "${DOW}" =~ ^[0-7]$ ]]; then
  echo "--dow must be 0-7, got: ${DOW}" >&2
  exit 1
fi

HOUR="${HHMM%:*}"
MINUTE="${HHMM#*:}"
if ((10#${HOUR} > 23)) || ((10#${MINUTE} > 59)); then
  echo "--time out of range: ${HHMM}" >&2
  exit 1
fi

ARGS=(--env "${ENV_FILE}" --tz-offset "${TZ_OFFSET}" --rolling-hours "${ROLLING_HOURS}" --no-print)
ARGS+=(--bills-unmapped-max-ratio "${BILLS_UNMAPPED_MAX_RATIO}")
ARGS+=(--bills-alert-unmapped-ratio "${BILLS_ALERT_UNMAPPED_RATIO}")
ARGS+=(--bills-alert-min-selected "${BILLS_ALERT_MIN_SELECTED}")
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

CRON_CMD="${RUNNER} ${ARGS[*]} >> ${ROOT_DIR}/logs/weekly_recap/cron.log 2>&1"
CRON_LINE="${MINUTE} ${HOUR} * * ${DOW} ${CRON_CMD} ${CRON_TAG}"

if [[ "${PRINT_ONLY}" == "1" ]]; then
  echo "${CRON_LINE}"
  exit 0
fi

mkdir -p "${ROOT_DIR}/logs/weekly_recap"
TMP_FILE="$(mktemp)"
trap 'rm -f "${TMP_FILE}"' EXIT
(crontab -l 2>/dev/null || true) | sed "/${CRON_TAG//\//\\/}/d" > "${TMP_FILE}"
echo "${CRON_LINE}" >> "${TMP_FILE}"
crontab "${TMP_FILE}"

echo "Installed cron:"
echo "${CRON_LINE}"
echo "Check with: crontab -l | grep OKX_WEEKLY_RECAP"
