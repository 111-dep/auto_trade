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

BARS=70080
HORIZON=24
INST_IDS=""
TITLE_PREFIX="2Y Multi-Inst"
EXCLUDE_XAU=1

usage() {
  cat <<'EOF'
Usage:
  run_backtest_batch_levels.sh [options]

Options:
  --env PATH            Env file path (default: ./okx_auto_trader.env)
  --bars N              Backtest bars on LTF (default: 70080, ~2 years on 15m)
  --horizon-bars N      Forward bars for outcome eval (default: 24)
  --inst-ids CSV        Instruments CSV. Empty => read from env (OKX_INST_IDS/OKX_INST_ID)
  --include-xau         Keep XAU-USDT-SWAP in auto-picked instruments
  --title-prefix TEXT   Prefix for Telegram summary title
  -h, --help            Show this help

Behavior:
  Runs 3 backtests sequentially and sends Telegram summary for each:
    1) L2 cumulative (L1+L2)
    2) L2 only
    3) L3 only
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --bars)
      BARS="${2:-}"
      shift 2
      ;;
    --horizon-bars)
      HORIZON="${2:-}"
      shift 2
      ;;
    --inst-ids)
      INST_IDS="${2:-}"
      shift 2
      ;;
    --include-xau)
      EXCLUDE_XAU=0
      shift
      ;;
    --title-prefix)
      TITLE_PREFIX="${2:-}"
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

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Env file not found: ${ENV_FILE}" >&2
  exit 1
fi

if [[ -z "${INST_IDS}" ]]; then
  INST_IDS="$(python3 - "${ENV_FILE}" "${EXCLUDE_XAU}" <<'PY'
import sys

env_file = sys.argv[1]
exclude_xau = int(sys.argv[2]) == 1
insts = ""
fallback = ""

with open(env_file, "r", encoding="utf-8") as f:
    for raw in f:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key == "OKX_INST_IDS":
            insts = val
        elif key == "OKX_INST_ID":
            fallback = val

picked = insts if insts else fallback
parts = [x.strip().upper() for x in picked.split(",") if x.strip()]
if exclude_xau:
    parts = [x for x in parts if x != "XAU-USDT-SWAP"]
print(",".join(parts))
PY
)"
fi

if [[ -z "${INST_IDS}" ]]; then
  echo "No instruments resolved. Use --inst-ids or configure OKX_INST_IDS." >&2
  exit 1
fi

if ! [[ "${BARS}" =~ ^[0-9]+$ ]]; then
  echo "--bars must be an integer, got: ${BARS}" >&2
  exit 1
fi
if ! [[ "${HORIZON}" =~ ^[0-9]+$ ]]; then
  echo "--horizon-bars must be an integer, got: ${HORIZON}" >&2
  exit 1
fi

LOG_DIR="${ROOT_DIR}/batch_logs"
mkdir -p "${LOG_DIR}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
MASTER_LOG="${LOG_DIR}/batch_levels_${RUN_TS}.log"

log() {
  local msg="$1"
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "${msg}" | tee -a "${MASTER_LOG}"
}

run_case() {
  local case_key="$1"
  shift
  local title="${TITLE_PREFIX} ${case_key}"
  local case_log="${LOG_DIR}/${case_key}_${RUN_TS}.log"

  log "START ${case_key} | bars=${BARS} horizon=${HORIZON} insts=${INST_IDS}"
  (
    set +e
    python3 "${TRADER}" \
      --env "${ENV_FILE}" \
      --backtest \
      --bt-bars "${BARS}" \
      --bt-horizon-bars "${HORIZON}" \
      --bt-inst-ids "${INST_IDS}" \
      --bt-send-telegram \
      --bt-title "${title}" \
      "$@" 2>&1 | tee "${case_log}" | tee -a "${MASTER_LOG}"
    exit "${PIPESTATUS[0]}"
  )
  local rc=$?
  if [[ ${rc} -ne 0 ]]; then
    log "FAIL ${case_key} rc=${rc} (check ${case_log})"
    return ${rc}
  fi
  log "DONE ${case_key} (log: ${case_log})"
}

log "Batch begin | env=${ENV_FILE}"
log "Resolved insts=${INST_IDS}"
log "Master log=${MASTER_LOG}"

failures=0
case_results=()

set +e
run_case "L2_CUM"  --bt-max-level 2
rc=$?
set -e
case_results+=("L2_CUM:${rc}")
if [[ ${rc} -ne 0 ]]; then failures=$((failures + 1)); fi

set +e
run_case "L2_ONLY" --bt-max-level 2 --bt-exact-level 2
rc=$?
set -e
case_results+=("L2_ONLY:${rc}")
if [[ ${rc} -ne 0 ]]; then failures=$((failures + 1)); fi

set +e
run_case "L3_ONLY" --bt-max-level 3 --bt-exact-level 3
rc=$?
set -e
case_results+=("L3_ONLY:${rc}")
if [[ ${rc} -ne 0 ]]; then failures=$((failures + 1)); fi

log "Batch finished | failures=${failures}"
for item in "${case_results[@]}"; do
  log "Result ${item}"
done
echo
echo "Master log:"
echo "  ${MASTER_LOG}"

if [[ ${failures} -ne 0 ]]; then
  exit 1
fi
