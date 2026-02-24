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
SRC_DIR="${SRC_DIR:-/mnt/e/jk/screenshots}"
DST_DIR="${DST_DIR:-${ROOT_DIR}/screenshots}"
PATTERN="${PATTERN:-screenshot_*.png}"
SYNC_POLICY="${SYNC_POLICY:-mirror}" # mirror | copy

MODE="${1:-once}"
INTERVAL="${2:-2}"
POLICY="${3:-$SYNC_POLICY}"

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [once|watch] [interval_seconds] [mirror|copy]

Examples:
  $(basename "$0") once
  $(basename "$0") once 2 mirror
  $(basename "$0") once 2 copy
  $(basename "$0") watch 2

Optional env vars:
  SRC_DIR   Source folder (default: /mnt/e/jk/screenshots)
  DST_DIR   Target folder (default: ./screenshots in project root)
  PATTERN   Filename pattern (default: screenshot_*.png)
  SYNC_POLICY  mirror or copy (default: mirror)
EOF
}

sync_copy_once() {
  if [[ ! -d "$SRC_DIR" ]]; then
    echo "Source folder not found: $SRC_DIR" >&2
    return 1
  fi

  mkdir -p "$DST_DIR"

  shopt -s nullglob
  local files=("$SRC_DIR"/$PATTERN)
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    echo "No files matching '$PATTERN' in $SRC_DIR"
    return 0
  fi

  local copied=0
  local updated=0
  local unchanged=0
  local src
  local base
  local dst

  for src in "${files[@]}"; do
    [[ -f "$src" ]] || continue
    base="$(basename "$src")"
    dst="$DST_DIR/$base"

    if [[ -e "$dst" ]]; then
      if cmp -s "$src" "$dst"; then
        unchanged=$((unchanged + 1))
        continue
      fi
      cp "$src" "$dst"
      updated=$((updated + 1))
      echo "Updated: $base"
      continue
    fi

    cp "$src" "$dst"
    copied=$((copied + 1))
    echo "Copied: $base"
  done

  echo "Sync complete (copy) | copied=$copied updated=$updated unchanged=$unchanged"
}

sync_mirror_once() {
  if [[ ! -d "$SRC_DIR" ]]; then
    echo "Source folder not found: $SRC_DIR" >&2
    return 1
  fi

  mkdir -p "$DST_DIR"

  shopt -s nullglob
  local src_files=("$SRC_DIR"/$PATTERN)
  local dst_files=("$DST_DIR"/$PATTERN)
  shopt -u nullglob

  declare -A src_map=()
  local src
  local dst
  local base
  local copied=0
  local updated=0
  local deleted=0
  local unchanged=0

  for src in "${src_files[@]}"; do
    [[ -f "$src" ]] || continue
    base="$(basename "$src")"
    src_map["$base"]=1
  done

  for dst in "${dst_files[@]}"; do
    [[ -f "$dst" ]] || continue
    base="$(basename "$dst")"
    if [[ -z "${src_map[$base]:-}" ]]; then
      rm -f "$dst"
      deleted=$((deleted + 1))
      echo "Deleted: $base"
    fi
  done

  for src in "${src_files[@]}"; do
    [[ -f "$src" ]] || continue
    base="$(basename "$src")"
    dst="$DST_DIR/$base"

    if [[ -e "$dst" ]]; then
      if cmp -s "$src" "$dst"; then
        unchanged=$((unchanged + 1))
        continue
      fi
      cp "$src" "$dst"
      updated=$((updated + 1))
      echo "Updated: $base"
      continue
    fi

    cp "$src" "$dst"
    copied=$((copied + 1))
    echo "Copied: $base"
  done

  echo "Sync complete (mirror) | copied=$copied updated=$updated deleted=$deleted unchanged=$unchanged"
}

sync_once() {
  case "$POLICY" in
    mirror)
      sync_mirror_once
      ;;
    copy)
      sync_copy_once
      ;;
    *)
      echo "Invalid sync policy: $POLICY (use mirror or copy)" >&2
      return 1
      ;;
  esac
}

case "$MODE" in
  once)
    sync_once
    ;;
  watch)
    if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [[ "$INTERVAL" -lt 1 ]]; then
      echo "Interval must be a positive integer. Got: $INTERVAL" >&2
      exit 1
    fi

    echo "Watching '$SRC_DIR' -> '$DST_DIR' every ${INTERVAL}s (policy=$POLICY) ..."
    while true; do
      sync_once || true
      sleep "$INTERVAL"
    done
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Invalid mode: $MODE" >&2
    usage
    exit 1
    ;;
esac
