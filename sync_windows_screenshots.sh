#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec env OKX_SUITE_ROOT="${ROOT_DIR}" "${ROOT_DIR}/scripts/utils/sync_windows_screenshots.sh" "$@"
