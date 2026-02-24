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

## Compatibility

Root-level script names are still valid and are now thin wrappers to the files
in this folder. Existing commands, cron jobs, and habits do not need to change.
