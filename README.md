# OKX Auto Trader (okx_trade_suite)

本项目是一个面向 OKX 永续合约的自动交易与回测系统，支持多币种、多策略档案、同币多策略投票、实盘风控、2Y 组合回测与 Telegram 提醒。

## 1. 当前能力总览

- 多币种轮询执行（`OKX_INST_IDS`）。
- 多策略变体（`classic` / `btceth_smc_a2` / `elder_tss_v1` / `r_breaker_v1` / `range_reversion_v1` 等）。
- 档案化参数（`STRAT_PROFILE_MAP` + `STRAT_PROFILE_<PROFILE>_*`）。
- 同币多策略投票后只下一个单（`STRAT_PROFILE_VOTE_*`）。
- 分级执行与白名单（L1/L2/L3，`STRAT_EXEC_MAX_LEVEL` + `STRAT_EXEC_L3_INST_IDS`）。
- Managed Exit：TP1 分批、TP2、保本/费用缓冲、移动止损。
- 开仓可选拆分双腿（TP1/TP2）并在 TP1 后自动推进剩余仓位保本止损（支持 WS 快速通道）。
- 风控：日亏熔断、开仓频率限制、连续止损冷却/冻结、风险开仓硬保护。
- 下单幂等：支持 `clOrdId`（客户端订单号）以降低重复下单风险。
- 进程安全：单实例锁（默认开启，防止重复启动多个实盘进程）。
- 可选按风险百分比自动算仓（`OKX_MARGIN_USDT=0` + `STRAT_RISK_FRAC`）。
- 非同保证金模式仓位处理（`STRAT_SKIP_ON_FOREIGN_MGNMODE_POS`）。
- 逐笔交易台账（CSV，开仓/部分止盈/平仓/外部平仓）。
- 订单关联台账（CSV，按 `trade_id` 记录 `entry/event ordId/clOrdId`，便于精确对账）。
- 回测：
  - `okx_auto_trader.py --backtest`（单路径回测）
  - `run_interleaved_backtest_2y.py`（2Y 多币组合、真实顺序）

## 2. 项目结构

- `okx_auto_trader.py`: 统一入口（调用 `okx_trader.main`）。
- `okx_auto_trader.env`: 实盘配置（含密钥，勿外传）。
- `okx_auto_trader.env.example`: 配置模板。
- `run_interleaved_backtest_2y.py`: 2Y 组合回测主脚本。
- `scripts/`: 脚本分层目录（统一入口）。
  - `scripts/live/`: 实盘启停类脚本。
  - `scripts/ops/`: 日报与 cron 安装脚本。
  - `scripts/backtest/`: 回测批处理脚本。
  - `scripts/utils/`: 工具脚本。
- `scripts/backtest/run_backtest_batch_levels.sh`: L2/L3 批量回测脚本。
- `scripts/backtest/run_backtest_2y_cached.sh`: 2Y 缓存回测脚本。
- `scripts/ops/run_daily_recap.sh` / `scripts/ops/setup_daily_recap_cron.sh` / `scripts/live/restart_live_trader.sh`: 实盘运维脚本。
- `okx_trader/`: 核心包（信号、执行、风控、回测、状态、告警）。
  - `runtime.py`: 运行循环与心跳/状态汇总。
  - `runtime_run_once_for_inst.py`: 单币种轮询主流程（取数/投票/持仓检查）。
  - `runtime_execute_decision.py`: 下单与持仓管理执行层。
- `runtime.log`: 实盘运行日志。
- `alerts.log`: 本地提醒日志。
- `trade_journal.csv`: 逐笔交易台账。
- `trade_journal_order_links.csv`: 订单关联台账（复盘/对账用）。

## 3. 快速启动

1. 准备配置

```bash
cp okx_auto_trader.env.example okx_auto_trader.env
# 然后填写 OKX_API_KEY / OKX_SECRET_KEY / OKX_PASSPHRASE
```

2. 安全自检（仅跑一轮，不下单）

```bash
OKX_DRY_RUN=1 ALERT_ONLY_MODE=1 \
python3 -u okx_auto_trader.py --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env --once
```

3. nohup 实盘启动

```bash
nohup python3 -u /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.py \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  > /home/dandan/Workspace/test/okx_trade_suite/runtime.log 2>&1 &
```

4. 查看日志

```bash
tail -f /home/dandan/Workspace/test/okx_trade_suite/runtime.log
```

5. 停止实盘

```bash
pkill -TERM -f "python3 -u /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.py"
```

6. 一键管理实盘（推荐）

```bash
# 一键重启（默认会 tail 日志）
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh

# 仅查看状态
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh --status

# 只重启不 tail
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh --no-tail

# 仅停止 / 仅启动
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh --stop
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh --start

# 启动时关闭“开仓执行TG消息”（仅本次进程）
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh --start --no-open-tg

# 启动 + 一键安装每天07:00的24h日报TG（含净收益/权益）
/home/dandan/Workspace/test/okx_trade_suite/scripts/live/restart_live_trader.sh \
  --start --no-open-tg --setup-daily-recap-7am
```

7. 最小回归测试（本地无交易所依赖）

```bash
cd /home/dandan/Workspace/test/okx_trade_suite
python3 -m unittest discover -s tests -p "test_*.py"
```

8. 提交前安全检查（推荐）

```bash
cd /home/dandan/Workspace/test/okx_trade_suite
python3 -m pip install --user pre-commit
pre-commit install

# 首次可全量跑一遍
pre-commit run --all-files
```

当前配置的 pre-commit 检查：
- `secret-scan (local)`：扫描疑似密钥（含 `OKX_API_KEY/OKX_SECRET_KEY/OKX_PASSPHRASE` 等）。
- `py-compile-staged (local)`：对变更的 `*.py` 做语法编译检查。

## 4. 仓位计算优先级（非常重要）

- `OKX_SIZING_MODE=margin` 且 `OKX_MARGIN_USDT>0`：
  - 按固定保证金开仓（每单保证金= `OKX_MARGIN_USDT`）。
  - 此时 `STRAT_RISK_FRAC` 不参与计算。
- `OKX_SIZING_MODE=margin` 且 `OKX_MARGIN_USDT=0`：
  - 启用风险百分比开仓（按 `STRAT_RISK_FRAC` 结合止损距离反推仓位）。
  - 同时受 `STRAT_RISK_MAX_MARGIN_FRAC` 硬上限保护。

推荐：`STRAT_RISK_FRAC` 和 `STRAT_RISK_MAX_MARGIN_FRAC` 使用百分号格式（如 `0.5%`、`30%`）。

## 5. 关键风控开关

- `STRAT_DAILY_LOSS_LIMIT_PCT`: 24h 风控阈值（当前按“已实现亏损 + 持仓潜在亏损 + 新单潜在亏损”预算）。
- `STRAT_STOP_REENTRY_COOLDOWN_MINUTES`: 止损后同方向冷却。
- `STRAT_STOP_STREAK_FREEZE_COUNT` + `STRAT_STOP_STREAK_FREEZE_HOURS`: 连续止损冻结。
- `STRAT_STOP_STREAK_L2_ONLY`: 冻结期间仅禁 L3 或全禁。
- `STRAT_ENABLE_CLOSE`: 是否允许脚本主动平仓（止盈/止损/反手）。
- `STRAT_SIGNAL_EXIT_ENABLED`: 是否启用信号失效提前平仓（默认建议 `0`，更贴近当前回测口径）。
- `STRAT_SPLIT_TP_ON_ENTRY`: 是否拆分 TP1/TP2 双腿下单（建议 `1`）。
- `STRAT_ENTRY_EXEC_MODE`: 入场执行模式（`market` / `limit` / `auto`）。
- `STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN`: `auto` 下等级阈值（>= 阈值走市价）。
- `STRAT_ENTRY_LIMIT_OFFSET_BPS`: 限价偏移基点（越大越容易成交，价格通常更差）。
- `STRAT_ENTRY_LIMIT_TTL_SEC`: 限价等待秒数（超时后按 fallback 处理）。
- `STRAT_ENTRY_LIMIT_REPRICE_MAX`: 限价超时后重挂次数（建议先 `0`）。
- `STRAT_ENTRY_LIMIT_FALLBACK_MODE`: 限价未成交回退（`market` / `skip`）。
- `STRAT_SKIP_ON_FOREIGN_MGNMODE_POS`: 是否因异保证金模式仓位而跳过该币种。
- `OKX_SINGLE_INSTANCE_LOCK`: 是否启用单实例锁（默认 `1`）。
- `OKX_INSTANCE_LOCK_FILE`: 单实例锁文件路径（默认 `${OKX_STATE_FILE}.lock`）。
- `OKX_WS_TP1_BE_ENABLED`: 是否启用私有 WS 快速管理（TP1 成交后尽快推保本止损）。
- `OKX_WS_PRIVATE_URL`: 私有 WS 地址（实盘/模拟盘请按账户环境填写）。
- `OKX_WS_RECONNECT_SECONDS`: 私有 WS 断线重连间隔。

## 6. 多策略档案与投票

推荐：按“策略 -> 币种”分组绑定档案：

```env
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;ALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
STRAT_PROFILE_ALT_VARIANT=classic
```

兼容旧写法（按“币种 -> 策略”）：

```env
STRAT_PROFILE_MAP=BTC-USDT-SWAP:BTCETH
STRAT_PROFILE_BTCETH_VARIANT=btceth_smc_a2
```

同币多策略投票（最终只下 1 个单）推荐分组写法：

```env
STRAT_PROFILE_VOTE_INST_GROUPS=BTCETH+ELDER:BTC-USDT-SWAP;DEFAULT+ELDERALT:SOL-USDT-SWAP,DOGE-USDT-SWAP
STRAT_PROFILE_VOTE_MODE=any
STRAT_PROFILE_VOTE_MIN_AGREE=1
STRAT_PROFILE_VOTE_SCORE_MAP=BTCETH=0.154,ELDER=0.057
STRAT_PROFILE_VOTE_LEVEL_WEIGHT=0.01
```

可选：按档案覆盖杠杆（用于如 XAU 单独杠杆）：

```env
OKX_LEVERAGE=10
STRAT_PROFILE_INST_GROUPS=BTCETH:BTC-USDT-SWAP;XAU:XAU-USDT-SWAP
STRAT_PROFILE_XAU_LEVERAGE=25
```

兼容旧写法：

```env
STRAT_PROFILE_VOTE_MAP=BTC-USDT-SWAP:BTCETH+ELDER
STRAT_PROFILE_VOTE_MODE=any
STRAT_PROFILE_VOTE_MIN_AGREE=1
STRAT_PROFILE_VOTE_SCORE_MAP=BTCETH=0.154,ELDER=0.057
STRAT_PROFILE_VOTE_LEVEL_WEIGHT=0.01
```

## 7. 回测常用命令

统一场景命名（建议以后都用这个）：

| 标准名 | 旧习惯叫法 | fee_rate | slippage_bps | stop_extra_r | tp_haircut_r | miss_prob |
|---|---|---:|---:|---:|---:|---:|
| `S1-OPTIMISTIC` | 乐观 | 0.0006 | 1.0 | 0.02 | 0.01 | 0.01 |
| `S2-MID_PESS` | 中等悲观 | 0.0008 | 1.5 | 0.03 | 0.02 | 0.03 |
| `S3-LIVE_FIT` | 贴近实盘/实盘拟合 | 0.0010 | 3.0 | 0.05 | 0.04 | 0.06 |
| `S4-STRICT_PESS` | 严格悲观 | 0.0012 | 5.0 | 0.08 | 0.06 | 0.10 |
| `S5-EXTREME_STRESS` | 极端压力 | 0.0016 | 8.0 | 0.12 | 0.10 | 0.15 |

说明：
- `S2/S3/S4` 参数来自你实际常用/已验证口径。
- `S1/S5` 是边界参考，不建议直接拿来做实盘收益预期。

2Y 组合回测（以 `S2-MID_PESS` 为例）：

```bash
ENV="/home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env"

python3 -u /home/dandan/Workspace/test/okx_trade_suite/run_interleaved_backtest_2y.py \
  --env "$ENV" \
  --bars 70080 \
  --risk-frac 0.005 \
  --managed-exit \
  --fee-rate 0.0008 \
  --slippage-bps 1.5 \
  --stop-extra-r 0.03 \
  --tp-haircut-r 0.02 \
  --miss-prob 0.03 \
  --title "2Y ManagedExit S2-MID_PESS"
```

推荐脚本（默认只用本地缓存，缓存不足直接退出）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/backtest/run_backtest_2y_cached.sh \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --inst-ids BTC-USDT-SWAP,SOL-USDT-SWAP,DOGE-USDT-SWAP,SUI-USDT-SWAP,BCH-USDT-SWAP,LTC-USDT-SWAP,NEAR-USDT-SWAP,FIL-USDT-SWAP,UNI-USDT-SWAP \
  --bars 70080 \
  --risk-frac 0.005 \
  --scenario s2
```

入场执行模式对比（market vs auto）：

```bash
# 基线：全 market
/home/dandan/Workspace/test/okx_trade_suite/scripts/backtest/run_backtest_2y_cached.sh \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --bars 70080 \
  --risk-frac 0.005 \
  --scenario s3 \
  --entry-exec-mode market \
  --title "2Y ManagedExit S3-LIVE_FIT MKT"

# 自动：L3 走 market，L1/L2 优先 limit，未成交回退 market
/home/dandan/Workspace/test/okx_trade_suite/scripts/backtest/run_backtest_2y_cached.sh \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --bars 70080 \
  --risk-frac 0.005 \
  --scenario s3 \
  --entry-exec-mode auto \
  --entry-auto-market-level-min 3 \
  --entry-limit-fallback-mode market \
  --title "2Y ManagedExit S3-LIVE_FIT AUTO"
```

保存“经典回测快照”（避免每次重跑后找不到历史结果）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/backtest/run_backtest_2y_cached.sh \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --bars 70080 \
  --risk-frac 0.005 \
  --scenario s3 \
  --save-tag classic_livefit
```

快照产物默认落在：
- `logs/backtest_snapshots/<timestamp>_<tag>.log`（完整结果日志）
- `logs/backtest_snapshots/<timestamp>_<tag>_trades.csv`（交易明细）
- `logs/backtest_snapshots/index.csv`（汇总索引，含 `avg_r`/`payoff_r`/`profit_factor_r`）

查看最近快照：

```bash
tail -n 5 /home/dandan/Workspace/test/okx_trade_suite/logs/backtest_snapshots/index.csv
```

如果你明确允许脚本在线补拉缺失历史：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/backtest/run_backtest_2y_cached.sh --allow-fetch ...
```

严格检查“最近20小时到底有没有信号”（同一共同终点窗口，口径对齐当前策略+投票）：

```bash
python3 -u /home/dandan/Workspace/test/okx_trade_suite/check_recent_signals_strict.py \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --hours 20 \
  --no-cache \
  --show-events 50
```

说明：
- `--no-cache` 表示强制实时拉取；去掉该参数则优先用本地缓存。
- 输出分为 `raw`（策略原始信号）和 `decision`（投票+等级后可执行信号）。
- 采用“所有币共同可用的最新K线终点”回看 `N` 小时，避免各币数据不同步导致误判。

## 8. 交易台账（两个月后统计就看它）

配置：

```env
TRADE_JOURNAL_ENABLED=1
TRADE_JOURNAL_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv
TRADE_ORDER_LINK_ENABLED=1
TRADE_ORDER_LINK_PATH=/home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv
```

查看：

```bash
tail -f /home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv
tail -f /home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv
```

字段包含：`event_type`、`trade_id`、`inst_id`、`side`、`size`、`entry_price`、`exit_price`、`reason`、`pnl_usdt`、`profile_id`、`strategy_variant`、`vote_*` 等。
订单关联台账额外包含：`entry_ord_id`、`entry_cl_ord_id`、`event_ord_id`、`event_cl_ord_id`。

实盘“净收益”对账（含手续费/资金费）：

```bash
cd /home/dandan/Workspace/test/okx_trade_suite
NOW_UTC="$(date -u '+%Y-%m-%d %H:%M:%S')"

python3 reconcile_okx_bills.py \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --start "2026-02-22 00:00:00" \
  --end "$NOW_UTC" \
  --trade-filter-mode merge \
  --trade-clord-prefix AT \
  --order-link-path /home/dandan/Workspace/test/okx_trade_suite/trade_journal_order_links.csv \
  --show-trade-ids 20 \
  --dump-trade-id-csv /home/dandan/Workspace/test/okx_trade_suite/logs/trade_id_reconcile.csv \
  --funding-scope matched-trade-inst
```

说明：
- `trade_net = sum(pnl + fee)`（type=2，支持 `clOrdId` 前缀 + 订单关联台账双重过滤）
- `funding_net = sum(balChg)`（type=8，默认只统计“脚本实际交易过的币种”）
- `recommended_net = trade_net + funding_net`
- `raw_balChg_all` 仅作账户流水参考，不能直接当策略净收益（会混入逐仓保证金进出）
- `--show-trade-ids` 会打印按 `trade_id` 聚合后的 top/bottom（`bill_net`、`journal_pnl`、delta）。
- `--dump-trade-id-csv` 会导出逐 `trade_id` 对账明细，便于月度复盘。

## 9. 每日复盘（自动化）

手动生成今日复盘（默认 `+08:00`）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh
```

指定日期复盘：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh \
  --date 2026-02-23 \
  --tz-offset +08:00
```

按“过去 24 小时滚动窗口”复盘（推荐用于 07:00 定时报）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh \
  --rolling-hours 24 \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

附带账单对账（含手续费/资金费）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh --with-bills
```

对账质量硬指标（默认已启用）：
- `unmapped_ratio > 35%`：净收益口径自动回退到 `journal`（避免账单映射不完整时误判）。
- `selected_trade_rows >= 20` 且 `unmapped_ratio >= 50%`：标记 `ALERT`，在日报/TG/rollup 明确提示。
- 可通过参数覆盖：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh \
  --with-bills \
  --bills-unmapped-max-ratio 0.35 \
  --bills-alert-unmapped-ratio 0.50 \
  --bills-alert-min-selected 20
```

附带交易所已平仓历史口径（用于核对“连亏笔数”）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh --with-exchange-history
```

附带当前账户权益（用于“本金还有多少”）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_daily_recap.sh --with-equity
```

默认输出位置：
- `logs/daily_recap/YYYY-MM-DD.md`（日报）
- `logs/daily_recap/YYYY-MM-DD.json`（结构化结果）
- `logs/daily_recap/index.log`（每日一行滚动摘要）

日报新增“批次/并发风险”统计：
- 批次按 `signal_ts + side` 聚合（用于观察“同一批开仓”的连胜/连败），并输出批次级当前/最大连亏。
- 同向并发输出 `max_long/max_short/max_same_side`（用于观察相关币同时同向持仓拥挤度）。
- `batch_pnl` 仅统计“窗口内 OPEN 的批次”最终平仓结果；与 `journal close` 口径不同属正常。

安装每天定时任务（默认每天 `00:10`）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/setup_daily_recap_cron.sh --time 00:10
```

安装“每天 07:00 推送过去 24h 汇总到 Telegram”（含账单净值、交易所口径和权益）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/setup_daily_recap_cron.sh \
  --time 07:00 \
  --rolling-hours 24 \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

如你只想保留“日报 TG”，关闭“开仓执行 TG”，在 `okx_auto_trader.env` 设置：

```env
ALERT_TG_ENABLED=1
ALERT_TG_TRADE_EXEC_ENABLED=0
```

安装后可检查：

```bash
crontab -l | grep OKX_DAILY_RECAP
```

如只想先看要写入的 cron 行，不立即安装：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/setup_daily_recap_cron.sh --print-only
```

## 10. 每周复盘（自动化）

手动生成过去 7 天周报（滚动 168h）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_weekly_recap.sh \
  --with-bills \
  --with-exchange-history \
  --with-equity
```

周报推送到 Telegram：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/run_weekly_recap.sh \
  --with-bills \
  --with-exchange-history \
  --with-equity \
  --telegram
```

安装每周定时任务（默认每周一 07:05）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/scripts/ops/setup_weekly_recap_cron.sh \
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

检查安装结果：

```bash
crontab -l | grep OKX_WEEKLY_RECAP
```

## 11. 重大更新记录（持续维护）

> 约定：每次“影响实盘行为、风控、仓位或回测口径”的更新，都要在这里追加一条。

### 2026-02-21

- 新增 `range_reversion_v1` 策略插件（区间支撑阻力均值回归，含 L1/L2/L3 分级）。
- 新增同币多策略投票执行（含 `any/majority/unanimous`）。
- 新增投票加权机制（`STRAT_PROFILE_VOTE_SCORE_MAP` + `STRAT_PROFILE_VOTE_LEVEL_WEIGHT`）。
- 新增交易台账 `trade_journal.csv`（开仓/部分平仓/平仓/外部平仓事件）。
- 新增“连续无开仓超时提醒”（`ALERT_NO_OPEN_HOURS` / `ALERT_NO_OPEN_COOLDOWN_HOURS`，支持 Telegram）。
- 启动日志新增投票配置与台账配置展示，便于排查运行参数。
- 重构策略变体层：`okx_trader/strategy_variant.py` 改为“注册/分发层”，原实现迁移至 `okx_trader/strategy_variant_legacy.py`，实盘逻辑不变（已做回归验证）。
- 策略调用入口统一为 `VariantSignalInputs` 上下文对象（`strategy_contract.py`），`signals/backtest` 已接入，便于后续扩展多策略且不改行为。
- 新增下单 `clOrdId` 生成与幂等恢复（遇到重复 `clOrdId` 自动查询并复用已有订单）。
- 新增主进程单实例锁（防止重复启动多个实盘脚本）。
- 新增最小回归测试集（策略分发、投票逻辑、单实例锁、订单号规则）。
- 扩展最小回归测试集（Managed Exit 行为、OKX API 重试与 `clOrdId` 幂等恢复）。
- 新增策略插件目录 `okx_trader/strategies/`（自动发现并注册，便于后续批量扩展策略）。
- 运行时拆层：`runtime.py` 保留调度，`run_once_for_inst` 已迁移到独立模块，后续新增策略与风控逻辑更易维护。
- 配置层新增“策略分组写法”：`STRAT_PROFILE_INST_GROUPS` 与 `STRAT_PROFILE_VOTE_INST_GROUPS`（并保持旧 `..._MAP` 兼容，且旧写法优先）。
- 新增 `check_recent_signals_strict.py`：严格最近 N 小时信号检查（共同终点窗口、`raw` vs `decision` 分层统计、可选实时拉取）。

### 2026-02-22

- 实盘管理新增“私有 WS 快速通道”：订阅 `positions` 后，TP1 成交可不等 15m 收线，尽快推进剩余仓位保本止损。
- 新增 WS 相关配置：`OKX_WS_TP1_BE_ENABLED`、`OKX_WS_PRIVATE_URL`、`OKX_WS_RECONNECT_SECONDS`。
- 新增 `amend-order` 失败回退 `amend-algos`，提高“修改交易所止损”成功率（典型处理母单已完成/取消场景）。
- 修复拆分下单场景的 TP1->BE 止损推进稳定性：开仓时为附带 TP/SL 预写 `attachAlgoClOrdId` 并持久化到运行态，避免后续误改“已成交主单”触发 `51503`。
- 日亏风控升级为“预计亏损预算”：
  - 开仓前校验 `已实现亏损 + 当前持仓潜在亏损 + 新单潜在亏损 <= 日亏上限`，
  - 不再仅依赖“亏损发生后”才熔断，避免同一时刻过多风险暴露。
- 2Y 组合回测同步同口径风控（projected loss guard），减少实盘/回测偏差。
- 新增最小回归测试：`tests/test_ws_tp1_be.py`、`tests/test_projected_open_risk.py`。

### 2026-02-23

- `scripts/backtest/run_backtest_2y_cached.sh` 新增回测快照功能：
  - `--save-tag`：自动保存本次回测日志、交易CSV并写入 `logs/backtest_snapshots/index.csv`。
  - `--save-dir`：可自定义快照目录。

### 2026-02-24

- 新增周报自动化脚本：
  - `scripts/ops/run_weekly_recap.sh`（默认 168h 滚动窗口）
  - `scripts/ops/setup_weekly_recap_cron.sh`（默认周一 07:05）
- 周报输出与日报分离到 `logs/weekly_recap/`。
- `daily_recap.py` 新增“账单映射质量硬指标”：
  - 输出 `bills_quality`（`ok/warn/alert`）到日报、rollup、TG 摘要；
  - `unmapped_ratio > 35%` 时自动回退 `journal` 口径；
  - 样本量足够且 `unmapped_ratio >= 50%` 时触发 `ALERT` 提示。
- `scripts/ops/run_daily_recap.sh` 新增参数：
  - `--bills-unmapped-max-ratio`
  - `--bills-alert-unmapped-ratio`
  - `--bills-alert-min-selected`
- `logs/backtest_snapshots/index.csv` 自动记录关键摘要和 `payoff_r` / `profit_factor_r`，便于横向比较经典结果。
- 复盘链路增强：
  - 新增 `trade_journal_order_links.csv`，按 `trade_id` 记录开平仓事件关联的 `ordId/clOrdId`。
  - 执行层平仓事件补写订单回执，便于后续按订单ID做净值核算。
  - `reconcile_okx_bills.py` 新增 `--trade-filter-mode`（`prefix/order-link/merge/none`）与 `--order-link-path`，对账可直接结合订单关联台账。
- 新增每日复盘工具链：
  - `daily_recap.py`：按日期/滚动窗口汇总 `trade_journal.csv` + `runtime.log`，输出胜负统计、原因分布、连亏/连赢与运行健康指标。
  - `scripts/ops/run_daily_recap.sh`：一键生成日报（支持 `--rolling-hours` / `--with-bills` / `--with-exchange-history` / `--with-equity` / `--telegram`）。
  - `scripts/ops/setup_daily_recap_cron.sh`：一键安装 cron 定时任务。
- Telegram 通道拆分：
  - 新增 `ALERT_TG_TRADE_EXEC_ENABLED`，可只关闭“开仓执行”消息，不影响日报 Telegram 推送。
- 日报增强：
  - 支持 `--rolling-hours`（如 24h 滚动窗口）和 `--with-equity`（带当前账户权益）。
  - `--telegram` 内容升级为 24h 汇总（开仓次数、胜负、净收益、当前权益/基准本金）。
- 修复拆分双腿场景的外部平仓台账：
  - 以“未结清仓位(open_size-realized_size)”计算最终 EXTERNAL_CLOSE 尺寸，避免“只记录半仓/漏记”。
  - 平仓时引入 `positions-history` 对齐（同币种/方向/时间窗），优先使用交易所实际 `closeAvgPx/realizedPnl` 兜底。

### 2026-02-26

- `daily_recap.py` 新增“批次级风控画像”：
  - 按 `signal_ts + side` 聚合开仓批次，输出批次胜负、批次连亏/连赢、最大批次亏损/盈利。
  - 追加同向并发统计（`max_long/max_short/max_same_side/max_total`）及峰值时刻币种清单。
- 日报 Markdown / Telegram / rollup 一并输出批次连亏与同向并发峰值，便于快速判断“连败是否批次相关”。
- 单测新增覆盖：批次统计与“窗口前已开仓”并发计数，避免回归。

### 2026-02-27

- 新增“入场执行模式”：
  - 实盘支持 `market / limit / auto`（`STRAT_ENTRY_EXEC_MODE`）。
  - `auto` 可按等级阈值切分：低等级优先限价，高等级直接市价（`STRAT_ENTRY_AUTO_MARKET_LEVEL_MIN`）。
  - 新增限价行为参数：`STRAT_ENTRY_LIMIT_OFFSET_BPS`、`STRAT_ENTRY_LIMIT_TTL_SEC`、`STRAT_ENTRY_LIMIT_POLL_MS`、`STRAT_ENTRY_LIMIT_REPRICE_MAX`、`STRAT_ENTRY_LIMIT_FALLBACK_MODE`。
- 新增“限价取消后状态复核”安全保护：
  - 若取消未确认且订单仍 `live/partially_filled`，阻断 fallback 市价，避免重复补单导致过量开仓。
- 2Y 组合回测同步新增入场执行口径：
  - 支持 `--entry-exec-mode` 及相关参数；
  - 输出 `entry_modes`（market/limit/fallback）分布，便于对比与参数调优。
- 日报增强：
  - 从 runtime 日志解析 `entry_exec=`，新增入场执行统计（market/limit/fallback 比例）。
- 近期实盘推荐默认（当前 env）：
  - `auto + L3市价`，`limit_ttl=5s`，`reprice=0`，`fallback=market`。

## 12. 后续更新模板（复制追加）

```md
### YYYY-MM-DD

- [新增] ...
- [变更] ...
- [修复] ...
- [注意] 对实盘/回测口径的影响：...
```
