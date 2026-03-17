# OKX Auto Trader (`okx_trade_suite`)

一个面向 **OKX / Binance USDC 永续合约** 的自动交易与回测系统，支持多币种轮询、多策略档案、多策略投票、分级执行、Managed Exit、日报/周报、Telegram 提醒，以及 2 年组合回测。

> 快速入口：
> - 项目使用说明：`README.md`
> - 模块说明：`okx_trader/README.md`
> - 历史更新记录：`CHANGELOG.md`

## 目录

- [1. 能力概览](#1-能力概览)
- [2. 仓库结构](#2-仓库结构)
- [3. 快速开始](#3-快速开始)
- [4. 交易与风控模型](#4-交易与风控模型)
- [5. 多策略档案与投票](#5-多策略档案与投票)
- [6. 回测与研究](#6-回测与研究)
- [7. 台账、对账与复盘](#7-台账对账与复盘)
- [8. 开发与自检](#8-开发与自检)
- [9. 相关文档](#9-相关文档)

## 1. 能力概览

| 模块 | 当前能力 |
|---|---|
| 交易所 | `EXCHANGE_PROVIDER=okx` / `binance` |
| 标的 | 多币种轮询：`OKX_INST_IDS` / `BINANCE_INST_IDS` |
| 信号 | 4H / 1H / 15m 三层信号 + 多策略档案 + 同币投票 |
| 执行 | `market` / `limit` / `auto`，支持 `L1/L2/L3` 分级覆盖 |
| 持仓管理 | TP1 分批、TP2、保本止损、移动止损、交易所原生 TP/SL 与脚本 fallback |
| 风控 | 日亏熔断、止损冷却、连亏冻结、风险开仓上限、单实例锁 |
| 运行可靠性 | `clOrdId` 幂等、状态持久化、心跳日志、台账、订单关联台账 |
| 复盘研究 | 单路径回测、2Y 组合回测、回测快照、日报/周报、账单对账 |

**交易所侧重点**

- **OKX**：支持私有 WS 快速 TP1/BE 管理；支持原生多 TP 优先、脚本 fallback。
- **Binance**：支持 `fapi / papi` 自动识别；公共 REST 有多域 fallback 与最近 K 线缓存兜底；条件止损支持“接管已有单 / 修复数量 / 清理重复单”。

## 2. 仓库结构

**核心入口**

- `okx_auto_trader.py`：统一 CLI 入口。
- `okx_auto_trader.env.example`：配置模板。
- `run_interleaved_backtest_2y.py`：2Y 组合回测主脚本。

**核心包**

- `okx_trader/runtime.py`：主轮询循环、心跳与状态汇总。
- `okx_trader/runtime_run_once_for_inst.py`：单币种一次轮询流程。
- `okx_trader/runtime_execute_decision.py`：执行层，负责开平仓、止损、TP 管理。
- `okx_trader/okx_client.py`：OKX 客户端。
- `okx_trader/binance_um_client.py`：Binance USDⓈ-M / USDC 永续客户端。
- `okx_trader/backtest.py`：回测引擎。
- `okx_trader/README.md`：模块级说明。

**脚本目录**

- `scripts/live/`：实盘启停脚本。
- `scripts/backtest/`：回测脚本。
- `scripts/ops/`：日报、周报、cron 安装等运维脚本。

**运行产物**

- `runtime.log`：OKX 运行日志。
- `logs/binance_runtime.log`：Binance 运行日志。
- `trade_journal.csv` / `logs/binance_trade_journal.csv`：交易台账。
- `trade_journal_order_links.csv` / `logs/binance_trade_journal_order_links.csv`：订单关联台账。
- `logs/backtest_snapshots/`：回测快照与索引。
- `logs/daily_recap/`、`logs/weekly_recap/`：日报与周报产物。

## 3. 快速开始

### 3.1 准备配置

建议先定义根目录变量：

```bash
ROOT="/home/dandan/Workspace/test/okx_trade_suite"
```

**OKX**

```bash
cp "$ROOT/okx_auto_trader.env.example" "$ROOT/okx_auto_trader.env"
# 然后填写 OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE
```

**Binance**

仓库里目前没有单独的 Binance 模板，通常做法是复制一份 env 再改 provider：

```bash
cp "$ROOT/okx_auto_trader.env.example" "$ROOT/binance_auto_trader.env"
```

最少需要把下面几项改成 Binance 版本：

```env
EXCHANGE_PROVIDER=binance
BINANCE_API_KEY=...
BINANCE_SECRET_KEY=...
BINANCE_INST_IDS=BTC-USDT-SWAP,ETH-USDT-SWAP,SOL-USDT-SWAP
BINANCE_PRIVATE_API_MODE=auto
BINANCE_QUOTE_ASSET=USDC
```

### 3.2 安全自检

先跑一次 dry-run / alert-only，确认配置、取数和轮询都正常：

```bash
OKX_DRY_RUN=1 ALERT_ONLY_MODE=1 python3 -u "$ROOT/okx_auto_trader.py" --env "$ROOT/okx_auto_trader.env" --once
```

如果要测 Binance，把 `--env` 换成 `"$ROOT/binance_auto_trader.env"` 即可。

### 3.3 实盘启停

**OKX**

```bash
# 重启并 tail
"$ROOT/scripts/live/restart_live_trader.sh"

# 查看状态
"$ROOT/scripts/live/restart_live_trader.sh" --status

# 只重启不 tail
"$ROOT/scripts/live/restart_live_trader.sh" --no-tail

# 关闭“开仓执行 TG”后启动（仅本次进程）
"$ROOT/scripts/live/restart_live_trader.sh" --start --no-open-tg
```

**Binance**

```bash
# 启动 / 重启 / 查看状态
"$ROOT/scripts/live/restart_binance_trader.sh" --start
"$ROOT/scripts/live/restart_binance_trader.sh" --no-tail
"$ROOT/scripts/live/restart_binance_trader.sh" --status
```

### 3.4 日志查看

```bash
# OKX
tail -f "$ROOT/runtime.log"

# Binance
tail -f "$ROOT/logs/binance_runtime.log"

# 台账
tail -f "$ROOT/trade_journal.csv"
tail -f "$ROOT/trade_journal_order_links.csv"
```

## 4. 交易与风控模型

### 4.1 交易所差异

| 项目 | OKX | Binance |
|---|---|---|
| `EXCHANGE_PROVIDER` | `okx` | `binance` |
| 标的列表 | `OKX_INST_IDS` | `BINANCE_INST_IDS` |
| 凭证 | `OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE` | `BINANCE_API_KEY / BINANCE_SECRET_KEY` |
| 私有接口模式 | 固定 OKX | `BINANCE_PRIVATE_API_MODE=auto|fapi|papi` |
| 私有 WS 快速 TP1/BE | 支持 | 当前不用，走轮询管理 |
| 持仓模式 | 按 OKX 配置 | 当前 live 按 `OKX_POS_MODE=net`（one-way） |

### 4.2 仓位计算优先级

- `OKX_SIZING_MODE=margin` 且 `OKX_MARGIN_USDT > 0`
  - 按固定保证金开仓。
  - `STRAT_RISK_FRAC` 不参与仓位反推。
- `OKX_SIZING_MODE=margin` 且 `OKX_MARGIN_USDT = 0`
  - 按风险百分比开仓。
  - 受 `STRAT_RISK_FRAC` 和 `STRAT_RISK_MAX_MARGIN_FRAC` 双重约束。

推荐把这两个值写成百分号格式，例如：`0.58%`、`30%`。

### 4.3 关键参数分组

**风险控制**

- `STRAT_DAILY_LOSS_LIMIT_PCT`：日亏熔断阈值。
- `STRAT_STOP_REENTRY_COOLDOWN_MINUTES`：止损后同方向冷却。
- `STRAT_STOP_STREAK_FREEZE_COUNT` + `STRAT_STOP_STREAK_FREEZE_HOURS`：连续止损冻结。
- `STRAT_STOP_STREAK_L2_ONLY`：冻结期间只禁 L3，还是全禁。
- `STRAT_SKIP_ON_FOREIGN_MGNMODE_POS`：若存在异保证金模式仓位，是否跳过该币种。

**入场执行**

- `STRAT_ENTRY_EXEC_MODE`：`market` / `limit` / `auto`。
- `STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN`：`auto` 下，等级大于等于该值走市价。
- `STRAT_ENTRY_AUTO_MARKET_LEVEL_MAX`：`auto` 下，等级小于等于该值走市价；优先于 `..._MIN`。
- `STRAT_ENTRY_LIMIT_OFFSET_BPS`：限价偏移基点。
- `STRAT_ENTRY_LIMIT_TTL_SEC`：限价等待秒数。
- `STRAT_ENTRY_LIMIT_REPRICE_MAX`：限价超时后的重挂次数。
- `STRAT_ENTRY_LIMIT_FALLBACK_MODE`：超时后 `market` 或 `skip`。
- `STRAT_ENTRY_L1/L2/L3_*`：按等级分别覆盖 `exec_mode / ttl / fallback_mode`。

**持仓管理**

- `STRAT_ENABLE_CLOSE`：是否允许脚本主动平仓。
- `STRAT_SIGNAL_EXIT_ENABLED`：是否允许信号失效提前平仓。
- `STRAT_SPLIT_TP_ON_ENTRY`：优先尝试交易所原生 TP1/TP2；不支持时自动 fallback 到脚本管理 TP1/TP2。
- `STRAT_MGMT_AUTO_TIGHTEN_STOP`：是否启用自动收紧止损；关闭后仅保留 `TP1 -> 保本`。
- `OKX_WS_TP1_BE_ENABLED`：OKX 私有 WS 快速管理开关。
- `OKX_WS_PRIVATE_URL`、`OKX_WS_RECONNECT_SECONDS`：私有 WS 连接参数。

**运行安全**

- `OKX_SINGLE_INSTANCE_LOCK=1`：开启单实例锁，防止重复启动实盘进程。
- `OKX_INSTANCE_LOCK_FILE`：锁文件路径，默认 `${OKX_STATE_FILE}.lock`。
- `LOG_HEARTBEAT_SECONDS`：心跳汇总日志周期。

## 5. 多策略档案与投票

### 5.1 推荐写法：按“策略 -> 币种”分组

```env
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;ALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
STRAT_PROFILE_ALT_VARIANT=classic
```

### 5.2 兼容旧写法：按“币种 -> 策略”绑定

```env
STRAT_PROFILE_MAP=BTC-USDT-SWAP:BTCETH
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
```

### 5.3 同币多策略投票

```env
STRAT_PROFILE_VOTE_INST_GROUPS=BTCETH+ELDER:BTC-USDT-SWAP;DEFAULT+ELDERALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_VOTE_MODE=any
STRAT_PROFILE_VOTE_MIN_AGREE=1
STRAT_PROFILE_VOTE_SCORE_MAP=BTCETH=0.154,ELDER=0.057
STRAT_PROFILE_VOTE_LEVEL_WEIGHT=0.01
```

### 5.4 按档案覆盖杠杆

```env
OKX_LEVERAGE=10
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;XAU:XAU-USDT-SWAP
STRAT_PROFILE_XAU_LEVERAGE=25
```

## 6. 回测与研究

### 6.1 场景等级

| 标准名 | 含义 | fee_rate | slippage_bps | stop_extra_r | tp_haircut_r | miss_prob |
|---|---|---:|---:|---:|---:|---:|
| `S1-OPTIMISTIC` | 乐观边界 | 0.0006 | 1.0 | 0.02 | 0.01 | 0.01 |
| `S2-MID_PESS` | 中等悲观 | 0.0008 | 1.5 | 0.03 | 0.02 | 0.03 |
| `S3-LIVE_FIT` | 贴近实盘 | 0.0010 | 3.0 | 0.05 | 0.04 | 0.06 |
| `S4-STRICT_PESS` | 严格悲观 | 0.0012 | 5.0 | 0.08 | 0.06 | 0.10 |
| `S5-EXTREME_STRESS` | 极端压力 | 0.0016 | 8.0 | 0.12 | 0.10 | 0.15 |

说明：`S2 / S3 / S4` 是更常用的实战口径，`S1 / S5` 更适合拿来做边界参考。

### 6.2 常用命令

先定义通用变量：

```bash
ROOT="/home/dandan/Workspace/test/okx_trade_suite"
ENV="$ROOT/okx_auto_trader.env"
BASE="$ROOT/scripts/backtest/run_backtest_2y_cached.sh --env $ENV --bars 70080 --risk-frac 0.005"
```

**标准 2Y 组合回测**

```bash
$BASE --scenario s2
```

**执行模式对比**

```bash
# 基线：全 market
$BASE --scenario s3 \
  --entry-exec-mode market \
  --title "2Y ManagedExit S3-LIVE_FIT MKT"

# auto：高等级市价，其余优先限价
$BASE --scenario s3 \
  --entry-exec-mode auto \
  --entry-auto-market-level-min 3 \
  --entry-limit-fallback-mode market \
  --title "2Y ManagedExit S3-LIVE_FIT AUTO"

# AUTO_REV：L1/L2 市价，L3 优先限价
$BASE --scenario s3 \
  --entry-exec-mode auto \
  --entry-auto-market-level-max 2 \
  --entry-limit-fallback-mode market \
  --title "2Y ManagedExit S3-LIVE_FIT AUTO_REV"
```

**保存经典回测快照**

```bash
$BASE --scenario s3 --save-tag classic_livefit
```

快照产物在：

- `logs/backtest_snapshots/<timestamp>_<tag>.log`
- `logs/backtest_snapshots/<timestamp>_<tag>_trades.csv`
- `logs/backtest_snapshots/index.csv`

**单路径回测**

```bash
python3 -u "$ROOT/okx_auto_trader.py" --env "$ENV" --backtest
```

**严格检查最近 N 小时到底有没有信号**

```bash
python3 -u "$ROOT/check_recent_signals_strict.py" \
  --env "$ENV" \
  --hours 20 \
  --no-cache \
  --show-events 50
```

**允许在线补拉历史数据**

```bash
$BASE --allow-fetch --scenario s2
```

## 7. 台账、对账与复盘

### 7.1 交易台账

典型配置：

```env
TRADE_JOURNAL_ENABLED=1
TRADE_JOURNAL_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv
TRADE_ORDER_LINK_ENABLED=1
TRADE_ORDER_LINK_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv
```

主要字段包括：`event_type`、`trade_id`、`inst_id`、`side`、`size`、`entry_price`、`exit_price`、`reason`、`pnl_usdt`、`profile_id`、`strategy_variant`、`vote_*`。

订单关联台账额外会记录：`entry_ord_id`、`entry_cl_ord_id`、`event_ord_id`、`event_cl_ord_id`。

### 7.2 交易所账单对账

用于核算“真实净收益（含手续费 / 资金费）”：

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

核心口径：

- `trade_net = sum(pnl + fee)`
- `funding_net = sum(balChg)`
- `recommended_net = trade_net + funding_net`

### 7.3 每日复盘

**手动日报**

```bash
"$ROOT/scripts/ops/run_daily_recap.sh"
```

**过去 24h 滚动窗口（推荐给 07:00 定时报）**

```bash
"$ROOT/scripts/ops/run_daily_recap.sh" \
  --rolling-hours 24 \
  --primary-source exchange_first \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

**常见附加项**

```bash
# 指定日期
"$ROOT/scripts/ops/run_daily_recap.sh" --date 2026-02-23 --tz-offset +08:00

# 附带账单对账
"$ROOT/scripts/ops/run_daily_recap.sh" --with-bills

# 附带交易所已平仓历史
"$ROOT/scripts/ops/run_daily_recap.sh" --with-exchange-history

# 附带当前权益
"$ROOT/scripts/ops/run_daily_recap.sh" --with-equity
```

**日报输出位置**

- `logs/daily_recap/YYYY-MM-DD.md`
- `logs/daily_recap/YYYY-MM-DD.json`
- `logs/daily_recap/index.log`

**安装 cron**

```bash
# 每天 00:10
"$ROOT/scripts/ops/setup_daily_recap_cron.sh" --time 00:10

# 每天 07:00 推送过去 24h 到 Telegram
"$ROOT/scripts/ops/setup_daily_recap_cron.sh" \
  --time 07:00 \
  --rolling-hours 24 \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

### 7.4 每周复盘

```bash
# 手动生成过去 7 天周报
"$ROOT/scripts/ops/run_weekly_recap.sh" \
  --primary-source exchange_first \
  --with-bills \
  --with-exchange-history \
  --with-equity

# 推送到 Telegram
"$ROOT/scripts/ops/run_weekly_recap.sh" \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram

# 安装每周一 07:05 cron
"$ROOT/scripts/ops/setup_weekly_recap_cron.sh" \
  --time 07:05 \
  --dow 1 \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

周报输出位置：

- `logs/weekly_recap/YYYY-MM-DD.md`
- `logs/weekly_recap/YYYY-MM-DD.json`
- `logs/weekly_recap/index.log`

## 8. 开发与自检

### 8.1 单元测试

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

当前本地钩子主要检查：

- `secret-scan (local)`：扫描疑似密钥。
- `py-compile-staged (local)`：编译检查已变更的 Python 文件。

## 9. 相关文档

- `okx_trader/README.md`：模块职责与实现说明。
- `CHANGELOG.md`：历史更新记录与模板。
- `scripts/README.md`：脚本层目录说明。
