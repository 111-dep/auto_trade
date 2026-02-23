#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ROOT_DIR}/okx_auto_trader.env"
TZ_OFFSET="+08:00"
DATE_ARG=""
ROLLING_HOURS=""
WITH_BILLS=0
WITH_EXCHANGE_HISTORY=0
WITH_EQUITY=0
TELEGRAM=0
TOP_N=5
OUT_DIR="${ROOT_DIR}/logs/daily_recap"
APPEND_SUMMARY="${OUT_DIR}/index.log"
PRINT_STDOUT=1

usage() {
  cat <<'EOF'
Usage:
  run_daily_recap.sh [options]

Options:
  --env PATH             Env file path (default: ./okx_auto_trader.env)
  --date YYYY-MM-DD      Local date to recap (default: today by --tz-offset)
  --rolling-hours N      Rolling window hours (e.g. 24). If set, overrides --date window
  --tz-offset +08:00     Local timezone offset (default: +08:00)
  --with-bills           Include bills reconcile (requires API connectivity)
  --with-exchange-history
                        Include exchange positions-history stats (requires API)
  --with-equity          Include current account equity (requires API connectivity)
  --telegram             Push short summary to telegram
  --top-n N              Top winners/losers count (default: 5)
  --out-dir PATH         Output folder (default: ./logs/daily_recap)
  --no-print             Do not print full markdown to stdout
  -h, --help             Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)
      ENV_FILE="${2:-}"
      shift 2
      ;;
    --date)
      DATE_ARG="${2:-}"
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
    --telegram)
      TELEGRAM=1
      shift
      ;;
    --top-n)
      TOP_N="${2:-5}"
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

mkdir -p "${OUT_DIR}"

if [[ -n "${DATE_ARG}" ]]; then
  REPORT_DATE="${DATE_ARG}"
else
  # Keep output filename stable with local machine date; recap logic itself still uses --tz-offset.
  REPORT_DATE="$(date +%F)"
fi

OUT_MD="${OUT_DIR}/${REPORT_DATE}.md"
OUT_JSON="${OUT_DIR}/${REPORT_DATE}.json"

CMD=(python3 -u "${ROOT_DIR}/daily_recap.py"
  --env "${ENV_FILE}"
  --tz-offset "${TZ_OFFSET}"
  --top-n "${TOP_N}"
  --out-md "${OUT_MD}"
  --out-json "${OUT_JSON}"
  --append-summary "${APPEND_SUMMARY}"
)

if [[ -n "${DATE_ARG}" ]]; then
  CMD+=(--date "${DATE_ARG}")
fi
if [[ -n "${ROLLING_HOURS}" ]]; then
  CMD+=(--rolling-hours "${ROLLING_HOURS}")
fi
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
if [[ "${PRINT_STDOUT}" == "1" ]]; then
  CMD+=(--print)
fi

"${CMD[@]}"

echo "Recap files:"
echo "  ${OUT_MD}"
echo "  ${OUT_JSON}"
echo "Rollup:"
echo "  ${APPEND_SUMMARY}"
