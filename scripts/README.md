# Scripts Layout

This directory organizes operational shell scripts by purpose while keeping
root-level command compatibility.

## Structure

- `scripts/live/`
  - `restart_live_trader.sh`
- `scripts/ops/`
  - `run_daily_recap.sh`
  - `setup_daily_recap_cron.sh`
- `scripts/backtest/`
  - `run_backtest_2y_cached.sh`
  - `run_backtest_batch_levels.sh`
- `scripts/utils/`
  - `sync_windows_screenshots.sh`

## Entry Point

Use scripts in this folder directly. Root-level compatibility wrappers are not
kept to avoid dual command paths.
