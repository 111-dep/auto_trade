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
- 回测：
  - `okx_auto_trader.py --backtest`（单路径回测）
  - `run_interleaved_backtest_2y.py`（2Y 多币组合、真实顺序）

## 2. 项目结构

- `okx_auto_trader.py`: 统一入口（调用 `okx_trader.main`）。
- `okx_auto_trader.env`: 实盘配置（含密钥，勿外传）。
- `okx_auto_trader.env.example`: 配置模板。
- `run_interleaved_backtest_2y.py`: 2Y 组合回测主脚本。
- `run_backtest_batch_levels.sh`: L2/L3 批量回测脚本。
- `okx_trader/`: 核心包（信号、执行、风控、回测、状态、告警）。
  - `runtime.py`: 运行循环与心跳/状态汇总。
  - `runtime_run_once_for_inst.py`: 单币种轮询主流程（取数/投票/持仓检查）。
  - `runtime_execute_decision.py`: 下单与持仓管理执行层。
- `runtime.log`: 实盘运行日志。
- `alerts.log`: 本地提醒日志。
- `trade_journal.csv`: 逐笔交易台账。

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
/home/dandan/Workspace/test/okx_trade_suite/restart_live_trader.sh

# 仅查看状态
/home/dandan/Workspace/test/okx_trade_suite/restart_live_trader.sh --status

# 只重启不 tail
/home/dandan/Workspace/test/okx_trade_suite/restart_live_trader.sh --no-tail

# 仅停止 / 仅启动
/home/dandan/Workspace/test/okx_trade_suite/restart_live_trader.sh --stop
/home/dandan/Workspace/test/okx_trade_suite/restart_live_trader.sh --start
```

7. 最小回归测试（本地无交易所依赖）

```bash
cd /home/dandan/Workspace/test/okx_trade_suite
python3 -m unittest discover -s tests -p "test_*.py"
```

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

2Y 组合回测（中等悲观口径示例）：

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
  --title "2Y ManagedExit 中等悲观"
```

推荐脚本（默认只用本地缓存，缓存不足直接退出）：

```bash
/home/dandan/Workspace/test/okx_trade_suite/run_backtest_2y_cached.sh \
  --env /home/dandan/Workspace/test/okx_trade_suite/okx_auto_trader.env \
  --inst-ids BTC-USDT-SWAP,SOL-USDT-SWAP,DOGE-USDT-SWAP,SUI-USDT-SWAP,BCH-USDT-SWAP,LTC-USDT-SWAP,NEAR-USDT-SWAP,FIL-USDT-SWAP,UNI-USDT-SWAP \
  --bars 70080 \
  --risk-frac 0.005 \
  --title "2Y ManagedExit 中等悲观"
```

如果你明确允许脚本在线补拉缺失历史：

```bash
/home/dandan/Workspace/test/okx_trade_suite/run_backtest_2y_cached.sh --allow-fetch ...
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
```

查看：

```bash
tail -f /home/dandan/Workspace/test/okx_trade_suite/trade_journal.csv
```

字段包含：`event_type`、`trade_id`、`inst_id`、`side`、`size`、`entry_price`、`exit_price`、`reason`、`pnl_usdt`、`profile_id`、`strategy_variant`、`vote_*` 等。

## 9. 重大更新记录（持续维护）

> 约定：每次“影响实盘行为、风控、仓位或回测口径”的更新，都要在这里追加一条。

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

## 10. 后续更新模板（复制追加）

```md
### YYYY-MM-DD

- [新增] ...
- [变更] ...
- [修复] ...
- [注意] 对实盘/回测口径的影响：...
```
