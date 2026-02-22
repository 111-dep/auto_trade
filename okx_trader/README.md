# OKX Trader Package

> 项目级使用说明、实盘命令、回测口径与重大更新记录请看仓库根目录 `README.md`。

模块职责：

- `common.py`: 通用工具函数（日志、解析、时间/进度、基础格式化）
- `models.py`: 数据模型（`Candle` / `Config` / `StrategyParams` / `PositionState`）
- `indicators.py`: 技术指标计算
- `okx_client.py`: OKX API 客户端、仓位解析、下单数量计算
- `signals.py`: 三层信号引擎（4H/1H/15m）与止损/目标计算
- `state_store.py`: 本地状态持久化与日统计
- `alerts.py`: Telegram/邮件/本地提醒与信号提醒组装
- `runtime.py`: 实时轮询执行与策略管理
- `backtest.py`: 回测引擎、进度显示、结果汇总
- `main.py`: CLI 入口与流程编排
- `strategy_variant.py`: 策略变体分发/注册层（可扩展入口）
- `strategy_variant_legacy.py`: 历史策略实现（当前默认逻辑）

兼容性：

- 顶层入口仍使用 `okx_auto_trader.py`
- 旧版本完整备份在 `okx_auto_trader_legacy.py`
