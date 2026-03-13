# Changelog

这里记录项目级的重要历史更新。根目录 `README.md` 只保留当前使用说明，避免入口文档越来越臃肿。

## 重大更新记录（持续维护）
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
- 新增“进阶版”分级执行覆盖：
  - 可按 `L1/L2/L3` 单独指定 `exec_mode / limit_ttl / fallback_mode`；
  - 典型用法：`L1=market`、`L2=limit(2s后market)`、`L3=limit(4~5s后skip)`。
- 新增实验策略 `xau_sibi_v1`（SIBI/FVG 回踩做空，含 strict/soft 失衡区近似与 one-touch 约束）：
  - 仅用于回测实验，未加入当前实盘配置。
  - 关键快照（S3, risk=0.4%）：
    - `xau_only_s3_r004_classic`: `final=1032.96`, `avgR=0.036`
    - `xau_only_s3_r004_sibi_relax2`: `final=889.39`, `avgR=-0.364`
    - `full_s3_r004_xau_sibi_relax2`: `final=31406.56`（基线 `baseline_s3_r004_rightrev_cmp` 为 `35962.14`）
  - 结论：当前参数下收益劣于 `XAU classic`，暂不启用到实盘，仅保留代码与快照作为历史研究记录。

### 2026-02-28

- 2Y 组合回测交易明细 CSV 增强（`run_interleaved_backtest_2y.py`）：
  - 新增导出字段：`entry_px`、`stop_px`、`tp1_px`、`tp2_px`、`risk_px`，便于后续做“止损后路径/触发质量”复盘分析。
- 新增实时执行优化开关（默认关闭，保持旧行为）：
  - `OKX_FAST_LTF_GATE=1` 时，先只检查 `LTF` 是否有新收线；仅在有新收线时才拉取 `HTF/LOC` 并计算信号/执行。
  - 目标：降低多币种串行轮询时的 REST 压力与空转延迟，不改变策略/风控/下单逻辑。
- 入场执行 `auto` 模式新增反向阈值：
  - 新增 `STRAT_ENTRY_AUTO_MARKET_LEVEL_MAX` / `--entry-auto-market-level-max`（`1~3`）；
  - 当该值>0时，`auto` 按“`level <= max` 走市价，否则走限价”决策（优先于 `..._MIN`），支持 `L1/L2` 市价、`L3` 限价。
- 日报/周报摘要增强（`daily_recap.py`，周报复用 rolling_168h）：
  - 顶部与 Telegram 摘要新增入场执行统计：`limit_fill` / `fallback_market` 及 `limit_fill_rate` / `fb_rate`。
  - `index.log` 一行汇总新增 `entry_legs`、`entry_fb`、`entry_limfill`，便于长期跟踪限价回退率。
- 实盘风险系数更新（当前 env）：
  - `STRAT_RISK_FRAC` 调整为 `0.58%`（即 `risk_frac=0.0058`），并已重启实盘进程生效。

## 后续更新模板（复制追加）

```md
### YYYY-MM-DD

- [新增] ...
- [变更] ...
- [修复] ...
- [注意] 对实盘/回测口径的影响：...
```
