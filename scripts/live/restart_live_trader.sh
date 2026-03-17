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
TRADER="${ROOT_DIR}/okx_auto_trader.py"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
LOG_FILE="${ROOT_DIR}/runtime.log"
RECAP_CRON_SETUP="${ROOT_DIR}/scripts/ops/setup_daily_recap_cron.sh"

ACTION="restart"   # restart | start | stop | status
TAIL_LOG=1
WAIT_SEC=8
TG_TRADE_EXEC_OVERRIDE=""   # "", "0", "1"
SETUP_DAILY_RECAP_7AM=0
DAILY_RECAP_TZ="+08:00"

usage() {
  cat <<'EOF'
Usage:
  restart_live_trader.sh [options]

Default behavior:
  Restart live trader: stop old process -> start new process -> tail log

Options:
  --env PATH       Env file path (default: ./okx_auto_trader.env)
  --log PATH       Runtime log path (default: ./runtime.log)
  --start          Start only
  --stop           Stop only
  --status         Show process status only
  --no-tail        Do not tail log after start/restart
  --wait-sec N     Wait seconds after TERM before KILL (default: 8)
  --no-open-tg     Start with ALERT_TG_TRADE_EXEC_ENABLED=0（不发开仓TG）
  --setup-daily-recap-7am
                   Install/update 07:00 daily recap telegram cron (rolling 24h)
  --daily-recap-tz +08:00
                   Timezone for daily recap cron (default: +08:00)
  -h, --help       Show this help

Examples:
  ./scripts/live/restart_live_trader.sh
  ./scripts/live/restart_live_trader.sh --no-tail
  ./scripts/live/restart_live_trader.sh --stop
  ./scripts/live/restart_live_trader.sh --start --env /path/to/okx_auto_trader.env
  ./scripts/live/restart_live_trader.sh --start --no-open-tg
  ./scripts/live/restart_live_trader.sh --start --no-open-tg --setup-daily-recap-7am
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --log)
      LOG_FILE="${2:-}"
      shift 2
      ;;
    --start)
      ACTION="start"
      shift
      ;;
    --stop)
      ACTION="stop"
      shift
      ;;
    --status)
      ACTION="status"
      shift
      ;;
    --no-tail)
      TAIL_LOG=0
      shift
      ;;
    --wait-sec)
      WAIT_SEC="${2:-8}"
      shift 2
      ;;
    --no-open-tg)
      TG_TRADE_EXEC_OVERRIDE="0"
      shift
      ;;
    --setup-daily-recap-7am)
      SETUP_DAILY_RECAP_7AM=1
      shift
      ;;
    --daily-recap-tz)
      DAILY_RECAP_TZ="${2:-}"
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

if [[ ! -f "${TRADER}" ]]; then
  echo "Trader script not found: ${TRADER}" >&2
  exit 1
fi

if ! [[ "${WAIT_SEC}" =~ ^[0-9]+$ ]]; then
  echo "--wait-sec must be integer, got: ${WAIT_SEC}" >&2
  exit 1
fi

if [[ -n "${TG_TRADE_EXEC_OVERRIDE}" ]] && [[ "${TG_TRADE_EXEC_OVERRIDE}" != "0" && "${TG_TRADE_EXEC_OVERRIDE}" != "1" ]]; then
  echo "Invalid TG trade exec override: ${TG_TRADE_EXEC_OVERRIDE}" >&2
  exit 1
fi

find_pids() {
  local pattern
  pattern="python3 -u ${TRADER} --env ${ENV_FILE}"
  pgrep -f "${pattern}" || true
}

print_status() {
  local pids
  pids="$(find_pids)"
  if [[ -z "${pids}" ]]; then
    echo "Status: not running"
    return 0
  fi
  echo "Status: running"
  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    ps -p "${pid}" -o pid=,etime=,cmd=
  done <<< "${pids}"
}

stop_trader() {
  local pids
  pids="$(find_pids)"
  if [[ -z "${pids}" ]]; then
    echo "No running trader process found."
    return 0
  fi

  echo "Stopping trader process(es):"
  while IFS= read -r pid; do
    [[ -z "${pid}" ]] && continue
    echo "  TERM pid=${pid}"
    kill -TERM "${pid}" 2>/dev/null || true
  done <<< "${pids}"

  local end_ts
  end_ts=$(( $(date +%s) + WAIT_SEC ))
  while :; do
    local still
    still="$(find_pids)"
    if [[ -z "${still}" ]]; then
      echo "Stopped successfully."
      return 0
    fi
    if (( $(date +%s) >= end_ts )); then
      echo "TERM timeout, forcing KILL:"
      while IFS= read -r pid; do
        [[ -z "${pid}" ]] && continue
        echo "  KILL pid=${pid}"
        kill -KILL "${pid}" 2>/dev/null || true
      done <<< "${still}"
      sleep 1
      local after_kill
      after_kill="$(find_pids)"
      if [[ -n "${after_kill}" ]]; then
        echo "Warning: some trader process still exists:"
        while IFS= read -r pid; do
          [[ -z "${pid}" ]] && continue
          ps -p "${pid}" -o pid=,etime=,cmd=
        done <<< "${after_kill}"
        return 1
      fi
      echo "Stopped with KILL."
      return 0
    fi
    sleep 1
  done
}

start_trader() {
  if [[ ! -f "${ENV_FILE}" ]]; then
    echo "Env file not found: ${ENV_FILE}" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${LOG_FILE}")"
  echo "Starting trader..."

  local -a start_cmd
  if [[ -n "${TG_TRADE_EXEC_OVERRIDE}" ]]; then
    echo "Start override: ALERT_TG_TRADE_EXEC_ENABLED=${TG_TRADE_EXEC_OVERRIDE}"
    start_cmd=(env "ALERT_TG_TRADE_EXEC_ENABLED=${TG_TRADE_EXEC_OVERRIDE}" python3 -u "${TRADER}" --env "${ENV_FILE}")
  else
    start_cmd=(python3 -u "${TRADER}" --env "${ENV_FILE}")
  fi

  if command -v setsid >/dev/null 2>&1; then
    setsid nohup "${start_cmd[@]}" > "${LOG_FILE}" 2>&1 < /dev/null &
  else
    nohup "${start_cmd[@]}" > "${LOG_FILE}" 2>&1 < /dev/null &
  fi
  local new_pid=$!
  sleep 1

  if ps -p "${new_pid}" >/dev/null 2>&1; then
    echo "Started: pid=${new_pid}"
    echo "Log: ${LOG_FILE}"
  else
    echo "Start failed. Check log: ${LOG_FILE}" >&2
    return 1
  fi

  if [[ "${TAIL_LOG}" == "1" ]]; then
    echo "Tailing log (Ctrl+C to exit tail only):"
    tail -f "${LOG_FILE}"
  fi
}

setup_daily_recap_7am() {
  if [[ ! -x "${RECAP_CRON_SETUP}" ]]; then
    echo "Daily recap cron setup script not found or not executable: ${RECAP_CRON_SETUP}" >&2
    return 1
  fi
  echo "Installing daily recap cron (07:00, rolling 24h, telegram)..."
  "${RECAP_CRON_SETUP}" \
    --time 07:00 \
    --env "${ENV_FILE}" \
    --tz-offset "${DAILY_RECAP_TZ}" \
    --rolling-hours 24 \
    --with-bills \
    --with-exchange-history \
    --with-equity \
    --telegram
}

case "${ACTION}" in
  status)
    print_status
    ;;
  stop)
    stop_trader
    ;;
  start)
    start_trader
    ;;
  restart)
    stop_trader
    start_trader
    ;;
  *)
    echo "Invalid action: ${ACTION}" >&2
    exit 2
    ;;
esac

if [[ "${SETUP_DAILY_RECAP_7AM}" == "1" ]]; then
  setup_daily_recap_7am
fi
