#!/usr/bin/env bash
set -euo pipefail

resolve_root_dir() {
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  if [[ -f "${script_dir}/../../okx_auto_trader.py" ]]; then
    (cd "${script_dir}/../.." && pwd)
    return
  fi
  if [[ -f "${script_dir}/../okx_auto_trader.py" ]]; then
    (cd "${script_dir}/.." && pwd)
    return
  fi
  printf '%s\n' "${script_dir}"
}

ROOT_DIR="$(resolve_root_dir)"
exec "${ROOT_DIR}/scripts/live/restart_live_trader.sh" \
  --env "${ROOT_DIR}/binance_auto_trader.env" \
  --log "${ROOT_DIR}/logs/binance_runtime.log" \
  "$@"
