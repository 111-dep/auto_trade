# OKX Trade Suite

Automated trading and research toolkit for **OKX / Binance USDC perpetuals**.

It supports:

- multi-symbol live polling
- strategy profiles and same-symbol voting
- `market / limit / auto` execution modes
- TP1 / TP2 / breakeven / fallback risk management
- 2-year portfolio backtests
- trade journals, order-link journals, daily / weekly recap, and bill reconciliation

> Language:
> - Chinese: `README.md`
> - English: `README_EN.md`

> Related docs:
> - Package guide: `okx_trader/README.md`
> - Scripts guide: `scripts/README.md`
> - Change history: `CHANGELOG.md`

## Contents

- [1. Overview](#1-overview)
- [2. Repository Layout](#2-repository-layout)
- [3. Quick Start](#3-quick-start)
- [4. Trading and Risk Model](#4-trading-and-risk-model)
- [5. Strategy Profiles and Voting](#5-strategy-profiles-and-voting)
- [6. Backtesting and Research](#6-backtesting-and-research)
- [7. Journals, Reconciliation, and Recaps](#7-journals-reconciliation-and-recaps)
- [8. Development and Checks](#8-development-and-checks)
- [9. Documentation Map](#9-documentation-map)

## 1. Overview

| Area | Current capability |
|---|---|
| Exchanges | `EXCHANGE_PROVIDER=okx` / `binance` |
| Instruments | `OKX_INST_IDS` / `BINANCE_INST_IDS` multi-symbol polling |
| Signals | default `HTF / LOC / LTF` stack |
| Execution | `market` / `limit` / `auto`, override by `L1/L2/L3` |
| Position management | TP1, TP2, breakeven, trailing stop, exchange-native TP/SL with script fallback |
| Risk | daily loss guard, stop cooldown, losing-streak freeze, risk cap, single-instance lock |
| Reliability | idempotent `clOrdId`, persisted state, heartbeat logs, journals, order linking |
| Research | single-path backtest, 2Y interleaved backtest, snapshots, Monte Carlo |

**Exchange focus**

- **OKX**
  - private WS fast TP1 / breakeven management
  - prefers native split TP when possible, falls back to script-managed exits
- **Binance**
  - auto-detects `fapi / papi`
  - public REST fallback domains are supported
  - current live path is one-way / `net`

## 2. Repository Layout

**Main entry points**

- `okx_auto_trader.py`: unified CLI entry
- `okx_auto_trader.env.example`: configuration template
- `run_interleaved_backtest_2y.py`: 2-year portfolio backtest runner
- `run_monte_carlo_from_trades.py`: Monte Carlo simulation from trade CSV

**Core package**

- `okx_trader/runtime.py`: live runtime loop
- `okx_trader/runtime_run_once_for_inst.py`: per-instrument polling flow
- `okx_trader/runtime_execute_decision.py`: execution and order / stop / TP management
- `okx_trader/okx_client.py`: OKX client
- `okx_trader/binance_um_client.py`: Binance USDⓈ-M / USDC client
- `okx_trader/backtest.py`: backtest engine
- `okx_trader/strategy_variant.py`: strategy plugin dispatch layer
- `okx_trader/strategies/`: strategy plugins

**Scripts**

- `scripts/live/`: live start / restart helpers
- `scripts/backtest/`: backtest helpers
- `scripts/ops/`: recap and cron helpers
- `scripts/devhooks/`: local hooks and dev helpers

**Common outputs**

- `runtime.log`: OKX runtime log
- `logs/binance_runtime.log`: Binance runtime log
- `trade_journal.csv` / `logs/binance_trade_journal.csv`: trade journals
- `trade_journal_order_links.csv` / `logs/binance_trade_journal_order_links.csv`: order-link journals
- `logs/backtest_snapshots/`: backtest snapshots
- `logs/daily_recap/`, `logs/weekly_recap/`: recap outputs

## 3. Quick Start

### 3.1 Define the project root

```bash
ROOT="/home/dandan/Workspace/test/okx_trade_suite"
```

### 3.2 Prepare env files

**OKX**

```bash
cp "$ROOT/okx_auto_trader.env.example" "$ROOT/okx_auto_trader.env"
```

Fill at least:

- `OKX_API_KEY`
- `OKX_SECRET_KEY`
- `OKX_PASSPHRASE`

**Binance**

```bash
cp "$ROOT/okx_auto_trader.env.example" "$ROOT/binance_auto_trader.env"
```

Then update at minimum:

```env
EXCHANGE_PROVIDER=binance
BINANCE_API_KEY=...
BINANCE_SECRET_KEY=...
BINANCE_INST_IDS=BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP
BINANCE_PRIVATE_API_MODE=auto
BINANCE_QUOTE_ASSET=USDC
```

### 3.3 Safe dry-run check

```bash
OKX_DRY_RUN=1 ALERT_ONLY_MODE=1 python3 -u "$ROOT/okx_auto_trader.py" \
  --env "$ROOT/okx_auto_trader.env" \
  --once
```

Swap `--env` to `"$ROOT/binance_auto_trader.env"` for Binance.

### 3.4 Start or restart live processes

**OKX**

```bash
"$ROOT/scripts/live/restart_live_trader.sh"
"$ROOT/scripts/live/restart_live_trader.sh" --status
"$ROOT/scripts/live/restart_live_trader.sh" --no-tail
"$ROOT/scripts/live/restart_live_trader.sh" --start --no-open-tg
```

**Binance**

```bash
"$ROOT/scripts/live/restart_binance_trader.sh" --start
"$ROOT/scripts/live/restart_binance_trader.sh" --no-tail
"$ROOT/scripts/live/restart_binance_trader.sh" --status
```

### 3.5 Tail common logs

```bash
tail -f "$ROOT/runtime.log"
tail -f "$ROOT/logs/binance_runtime.log"
tail -f "$ROOT/trade_journal.csv"
tail -f "$ROOT/trade_journal_order_links.csv"
```

## 4. Trading and Risk Model

### 4.1 Exchange differences

| Item | OKX | Binance |
|---|---|---|
| `EXCHANGE_PROVIDER` | `okx` | `binance` |
| Instrument list | `OKX_INST_IDS` | `BINANCE_INST_IDS` |
| Credentials | `OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE` | `BINANCE_API_KEY / BINANCE_SECRET_KEY` |
| Private API mode | fixed OKX flow | `BINANCE_PRIVATE_API_MODE=auto|fapi|papi` |
| Private WS TP1/BE | supported | not used in current live path |
| Live position mode | based on OKX config | current live path is one-way / `net` |

### 4.2 Sizing priority

- `OKX_SIZING_MODE=margin` and `OKX_MARGIN_USDT > 0`
  - fixed margin sizing
  - `STRAT_RISK_FRAC` is ignored for position sizing
- `OKX_SIZING_MODE=margin` and `OKX_MARGIN_USDT = 0`
  - risk-based sizing
  - constrained by both `STRAT_RISK_FRAC` and `STRAT_RISK_MAX_MARGIN_FRAC`

Use percentage-like values such as `0.58%` and `30%`.

### 4.3 Key parameter groups

**Risk**

- `STRAT_DAILY_LOSS_LIMIT_PCT`
- `STRAT_STOP_REENTRY_COOLDOWN_MINUTES`
- `STRAT_TP2_REENTRY_COOLDOWN_HOURS`
- `STRAT_TP2_REENTRY_PARTIAL_UNTIL_HOURS` + `STRAT_TP2_REENTRY_PARTIAL_MAX_LEVEL`
- `STRAT_STOP_STREAK_FREEZE_COUNT` + `STRAT_STOP_STREAK_FREEZE_HOURS`
- `STRAT_STOP_STREAK_L2_ONLY`

**Entry execution**

- `STRAT_ENTRY_EXEC_MODE`
- `STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN`
- `STRAT_ENTRY_AUTO_MARKET_LEVEL_MAX`
- `STRAT_ENTRY_LIMIT_OFFSET_BPS`
- `STRAT_ENTRY_LIMIT_TTL_SEC`
- `STRAT_ENTRY_LIMIT_REPRICE_MAX`
- `STRAT_ENTRY_LIMIT_FALLBACK_MODE`
- `STRAT_ENTRY_L1/L2/L3_*`

**Position management**

- `STRAT_ENABLE_CLOSE`
- `STRAT_SIGNAL_EXIT_ENABLED`
- `STRAT_SPLIT_TP_ON_ENTRY`
- `STRAT_MGMT_AUTO_TIGHTEN_STOP`
- `STRAT_MGMT_TRAIL_AFTER_TP1`
- `OKX_WS_TP1_BE_ENABLED`

**Runtime safety**

- `OKX_SINGLE_INSTANCE_LOCK=1`
- `OKX_INSTANCE_LOCK_FILE`
- `LOG_HEARTBEAT_SECONDS`

## 5. Strategy Profiles and Voting

### 5.1 Group by strategy

```env
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;ALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
STRAT_PROFILE_ALT_VARIANT=classic
```

### 5.2 Compatibility mapping by instrument

```env
STRAT_PROFILE_MAP=BTC-USDT-SWAP:BTCETH
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
```

### 5.3 Same-symbol multi-profile voting

```env
STRAT_PROFILE_VOTE_INST_GROUPS=BTCETH+ELDER:BTC-USDT-SWAP;DEFAULT+ELDERALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_VOTE_MODE=any
STRAT_PROFILE_VOTE_MIN_AGREE=1
STRAT_PROFILE_VOTE_SCORE_MAP=BTCETH=0.154,ELDER=0.057
STRAT_PROFILE_VOTE_LEVEL_WEIGHT=0.01
```

### 5.4 Per-profile leverage override

```env
OKX_LEVERAGE=10
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;XAU:XAU-USDT-SWAP
STRAT_PROFILE_XAU_LEVERAGE=25
```

## 6. Backtesting and Research

### 6.1 Scenario presets

| Name | Meaning | fee_rate | slippage_bps | stop_extra_r | tp_haircut_r | miss_prob |
|---|---|---:|---:|---:|---:|---:|
| `S1-OPTIMISTIC` | optimistic bound | 0.0006 | 1.0 | 0.02 | 0.01 | 0.01 |
| `S2-MID_PESS` | moderately pessimistic | 0.0008 | 1.5 | 0.03 | 0.02 | 0.03 |
| `S3-LIVE_FIT` | closer to live behavior | 0.0010 | 3.0 | 0.05 | 0.04 | 0.06 |
| `S4-STRICT_PESS` | stricter pessimistic | 0.0012 | 5.0 | 0.08 | 0.06 | 0.10 |
| `S5-EXTREME_STRESS` | extreme stress test | 0.0016 | 8.0 | 0.12 | 0.10 | 0.15 |

### 6.2 Common commands

```bash
ROOT="/home/dandan/Workspace/test/okx_trade_suite"
ENV="$ROOT/okx_auto_trader.env"
BASE="$ROOT/scripts/backtest/run_backtest_2y_cached.sh --env $ENV --bars 70080 --risk-frac 0.005"
```

If you want backtests to prefer long-lived local cache without changing live cache TTL:

```env
OKX_HISTORY_CACHE_TTL_SECONDS=21600
OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS=315360000
```

`OKX_BACKTEST_HISTORY_CACHE_TTL_SECONDS` only affects backtest entry points.

**Standard 2Y backtest**

```bash
$BASE --scenario s3
```

**Execution mode comparison**

```bash
$BASE --scenario s3 --entry-exec-mode market

$BASE --scenario s3 \
  --entry-exec-mode auto \
  --entry-auto-market-level-min 3 \
  --entry-limit-fallback-mode market

$BASE --scenario s3 \
  --entry-exec-mode auto \
  --entry-auto-market-level-max 2 \
  --entry-limit-fallback-mode market
```

**Save a snapshot**

```bash
$BASE --scenario s3 --save-tag classic_livefit
```

**Single-path backtest**

```bash
python3 -u "$ROOT/okx_auto_trader.py" --env "$ENV" --backtest
```

**Strict recent-signal check**

```bash
python3 -u "$ROOT/check_recent_signals_strict.py" \
  --env "$ENV" \
  --hours 20 \
  --no-cache \
  --show-events 50
```

## 7. Journals, Reconciliation, and Recaps

### 7.1 Trade journals

Typical config:

```env
TRADE_JOURNAL_ENABLED=1
TRADE_JOURNAL_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv
TRADE_ORDER_LINK_ENABLED=1
TRADE_ORDER_LINK_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv
```

Typical fields:

- journal: `event_type`, `trade_id`, `inst_id`, `side`, `entry_price`, `exit_price`, `reason`, `pnl_usdt`
- extended: `profile_id`, `strategy_variant`, `vote_*`
- order links: `entry_ord_id`, `entry_cl_ord_id`, `event_ord_id`, `event_cl_ord_id`

### 7.2 Exchange bill reconciliation

```bash
cd "$ROOT"
NOW_UTC="$(date -u '+%Y-%m-%d %H:%M:%S')"

python3 reconcile_okx_bills.py \
  --env "$ROOT/okx_auto_trader.env" \
  --start "2026-02-22 00:00:00" \
  --end "$NOW_UTC" \
  --trade-filter-mode merge \
  --trade-clord-prefix AT \
  --order-link-path "$ROOT/trade_journal_order_links.csv" \
  --show-trade-ids 20 \
  --dump-trade-id-csv "$ROOT/logs/trade_id_reconcile.csv" \
  --funding-scope matched-trade-inst
```

Core formulas:

- `trade_net = sum(pnl + fee)`
- `funding_net = sum(balChg)`
- `recommended_net = trade_net + funding_net`

### 7.3 Daily and weekly recap

**Daily**

```bash
"$ROOT/scripts/ops/run_daily_recap.sh"

"$ROOT/scripts/ops/run_daily_recap.sh" \
  --rolling-hours 24 \
  --primary-source exchange_first \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

**Weekly**

```bash
"$ROOT/scripts/ops/run_weekly_recap.sh" \
  --primary-source exchange_first \
  --with-bills \
  --with-exchange-history \
  --with-equity
```

## 8. Development and Checks

### 8.1 Unit tests

```bash
cd "$ROOT"
python3 -m unittest discover -s tests -p "test_*.py"
```

### 8.2 pre-commit

```bash
cd "$ROOT"
python3 -m pip install --user pre-commit
pre-commit install
pre-commit run --all-files
```

Current local hooks mainly check:

- `secret-scan (local)`
- `py-compile-staged (local)`

## 9. Documentation Map

- `README.md`: Chinese project guide
- `okx_trader/README.md`: package responsibilities
- `scripts/README.md`: scripts layout
- `CHANGELOG.md`: change history
