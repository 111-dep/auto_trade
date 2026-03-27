# OKX Trader Package

> 项目级使用说明与实盘命令请看仓库根目录 `README.md`，历史更新记录请看 `CHANGELOG.md`。

模块职责：

- `common.py`: 通用工具函数（日志、解析、时间/进度、基础格式化）
- `models.py`: 数据模型（`Candle` / `Config` / `StrategyParams` / `PositionState`）
- `indicators.py`: 技术指标计算
- `okx_client.py`: OKX API 客户端、仓位解析、下单数量计算
- `binance_um_client.py`: Binance USDⓈ-M / USDC 永续客户端（当前为 one-way + 轮询管理版）
- `client_factory.py`: 按 `EXCHANGE_PROVIDER` 选择交易所客户端
- `signals.py`: 当前只保留 `pa_oral_baseline_v1` 的信号组装入口
- `state_store.py`: 本地状态持久化与日统计
- `alerts.py`: Telegram/邮件/本地提醒与信号提醒组装
- `runtime.py`: 实时轮询执行与策略管理
- `backtest.py`: 回测引擎、进度显示、结果汇总
- `main.py`: CLI 入口与流程编排
- `strategy_variant.py`: 只保留 oral PA 的策略名规范化与防误用保护

兼容性：

- 顶层入口仍使用 `okx_auto_trader.py`
- 旧版本完整备份在 `okx_auto_trader_legacy.py`


Binance 当前支持：

- `EXCHANGE_PROVIDER=binance` 时走 Binance USDC 永续客户端。
- 建议使用 `BINANCE_INST_IDS=BTC-USDT-SWAP,ETH-USDT-SWAP,...`，系统会自动映射为 `BTCUSDC` / `ETHUSDC`。
- 当前 live 路径按 `OKX_POS_MODE=net`（one-way）工作，不启用 OKX 私有 WS 快速 TP1/BE。
- 入场支持现有 `market / limit / limit+skip` 逻辑；Binance 侧默认是脚本管理 TP1/TP2，且会在开仓后补挂保护止损。
- 私有接口支持 `BINANCE_PRIVATE_API_MODE=auto|fapi|papi`，默认 `auto`：普通 USDⓈ-M key 会走 `fapi`，统一账户 / Portfolio Margin key 会自动切到 `papi`。
- `papi` 模式下，普通下单走 `/papi/v1/um/order`，保护止损走 `/papi/v1/um/conditional/order`，用于兼容 Binance Unified Account。
