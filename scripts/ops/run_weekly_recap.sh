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
RUNNER="${ROOT_DIR}/scripts/ops/run_daily_recap.sh"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
ROLLING_HOURS="168"
WITH_BILLS=0
WITH_EXCHANGE_HISTORY=0
WITH_EQUITY=0
TELEGRAM=0
PRIMARY_SOURCE="exchange_first"
TOP_N=8
OUT_DIR="${ROOT_DIR}/logs/weekly_recap"
PRINT_STDOUT=1
BILLS_UNMAPPED_MAX_RATIO="0.35"
BILLS_ALERT_UNMAPPED_RATIO="0.50"
BILLS_ALERT_MIN_SELECTED="20"

usage() {
  cat <<'EOF'
Usage:
  run_weekly_recap.sh [options]

Default:
  Generate weekly recap using rolling 168h window.

Options:
  --env PATH                      Env file path (default: ./okx_auto_trader.env)
  --rolling-hours N               Rolling window hours (default: 168)
  --tz-offset +08:00              Local timezone offset (default: +08:00)
  --with-bills                    Include bills reconcile (requires API connectivity)
  --with-exchange-history         Include exchange positions-history stats (requires API)
  --with-equity                   Include current account equity (requires API connectivity)
  --primary-source MODE           Recap primary source: bills_auto/journal/exchange_first
  --bills-unmapped-max-ratio X    Fallback threshold (default: 0.35)
  --bills-alert-unmapped-ratio X  Hard-alert threshold (default: 0.50)
  --bills-alert-min-selected N    Min selected rows before hard alert (default: 20)
  --telegram                      Push short summary to telegram
  --top-n N                       Top winners/losers count (default: 8)
  --out-dir PATH                  Output folder (default: ./logs/weekly_recap)
  --no-print                      Do not print full markdown to stdout
  -h, --help                      Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --rolling-hours)
      ROLLING_HOURS="${2:-}"
      shift 2
      ;;
    --tz-offset)
      TZ_OFFSET="${2:-}"
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
    --primary-source)
      PRIMARY_SOURCE="${2:-exchange_first}"
      shift 2
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
    --top-n)
      TOP_N="${2:-8}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
      shift 2
      ;;
    --no-print)
      PRINT_STDOUT=0
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
  echo "Daily recap runner missing or not executable: ${RUNNER}" >&2
  exit 1
fi

if [[ "${PRIMARY_SOURCE}" == "exchange_first" && "${WITH_EXCHANGE_HISTORY}" != "1" ]]; then
  WITH_EXCHANGE_HISTORY=1
fi

CMD=("${RUNNER}"
  --env "${ENV_FILE}"
  --rolling-hours "${ROLLING_HOURS}"
  --tz-offset "${TZ_OFFSET}"
  --top-n "${TOP_N}"
  --out-dir "${OUT_DIR}"
  --primary-source "${PRIMARY_SOURCE}"
  --bills-unmapped-max-ratio "${BILLS_UNMAPPED_MAX_RATIO}"
  --bills-alert-unmapped-ratio "${BILLS_ALERT_UNMAPPED_RATIO}"
  --bills-alert-min-selected "${BILLS_ALERT_MIN_SELECTED}"
)

if [[ "${WITH_BILLS}" == "1" ]]; then
  CMD+=(--with-bills)
fi
if [[ "${WITH_EXCHANGE_HISTORY}" == "1" ]]; then
  CMD+=(--with-exchange-history)
fi
if [[ "${WITH_EQUITY}" == "1" ]]; then
  CMD+=(--with-equity)
fi
if [[ "${TELEGRAM}" == "1" ]]; then
  CMD+=(--telegram)
fi
if [[ "${PRINT_STDOUT}" == "0" ]]; then
  CMD+=(--no-print)
fi

"${CMD[@]}"
