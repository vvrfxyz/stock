# Architecture

本项目是一个以 PostgreSQL 为中心的股票数据落库流水线，`main.py` 负责调度，`scripts/` 负责具体任务，`db_manager.py` 统一 DB 写入逻辑，`data_sources/` 负责外部数据源适配。

## 数据源与职责边界

- Massive
  - `securities` Universe / 基本信息（名称、交易所、描述等）
  - 公司行动：分红、拆股
  - 日线价格（OHLCV、VWAP、turnover）
  - Grouped Daily：用于对指定日期范围进行“已存在记录”的回填/刷新（用于修正最新交易日）
  - `historical_shares`：通过 Ticker Overview 的 outstanding shares 与 Float 接口补齐 `total_shares` / `float_shares`
- 东方财富（`akshare`）
  - 日线价格应急兜底
  - 不作为默认主链路
- YFinance（补全/回填）
  - 公司行动历史补全（分红/拆股）：用于 Massive 免费层 2 年窗口之外的缺口补齐（`python main.py backfill_actions ...`）

关键原则：**只写自己真正“有来源保证”的字段**。例如：
- 东方财富不提供 VWAP，所以不应把 `daily_prices.vwap` 写成 `NULL` 覆盖既有值。
- Massive 不直接提供换手率，所以价格更新任务不应把 `daily_prices.turnover_rate` 写成 `NULL` 覆盖既有值。

## 核心表语义（约束/不变量）

- `securities.symbol`
  - 约定：标准化小写（例如 `aapl`）。
  - 用作脚本输入标识符。
- `securities.info_last_updated_at`
  - 语义：证券“非价格详情”最后一次成功刷新时间。
  - 仅由详情脚本更新。
- `securities.price_data_latest_date`
  - 语义：该证券在 `daily_prices` 中**实际覆盖到的最新日期**（应与 `MAX(daily_prices.date)` 对齐）。
  - 仅由价格写入脚本在成功写入后更新；必要时可用 `scripts/calibrate_price_latest_date.py` 纠偏。
- `daily_prices.vwap`
  - 可能为 `NULL`（例如仅由东方财富落库时）；由 Massive 系列任务补齐/修正。
- `daily_prices.turnover_rate`
  - 由 `historical_shares` 与 `daily_prices.volume` 重建得出。
  - 口径：优先 `float_shares`，否则 `total_shares`，否则保持 `NULL`。
- `daily_prices.adj_factor`
  - 定义：前复权（最新交易日=1）+ total return（拆股+现金分红）。
  - 用法：查询侧 `adj_close = close * adj_factor`（OHLC 同理）。
  - 维护：由 `scripts/recalc_adj_factor.py` 统一回填；`update_actions` 默认在 actions 有新增落库时自动触发重算。
- `historical_shares`
  - 历史股本（来源：Massive Overview / Float）。
  - Massive 免费层只覆盖最近约 2 年；更老历史保留只读，不主动回刷。

## Trading-date（交易日）策略

所有“日线价格更新”任务都应更新到最近一个**已收盘**的交易日（close-aware），避免：
- 在美股交易中途把当日未收盘数据写入数据库；
- 因为时区（例如在 Asia/Shanghai 运行）导致日期偏移。

本项目使用 `exchange_calendars`（`utils/trading_calendar.py`）计算最近已完成交易日，并在增量更新时以该日期作为 `end_date`。

## Daily 工作流（`python main.py daily_run`）

默认顺序：
1. `sync_massive_universe`：同步 Massive 活跃 US universe，只保留 `CS / ETF / ADRC`
2. `update_details`：更新证券详情（Massive）
3. `update_actions`：更新分红/拆股（Massive）
4. `update_massive_prices`：按 symbol 增量刷新最近 2 年窗口内日线
5. `update_grouped_daily`：回刷最近 5 个已收盘交易日的已存在记录（Massive Grouped Daily）
6. `update_massive_shares`：刷新 current-quarter shares / float
7. `rebuild_turnover_rate`：重建最近 5 个交易日换手率

第 5 步的目的：用 Massive 的市场全量聚合对“最新交易日”数据做一次精确修正，同时不破坏 `turnover_rate` 等派生字段。

复权因子建议维护方式：
- 常规：`update_actions` 默认在成功更新公司行动后自动触发对应证券的 `adj_factor` 重算（如需跳过：`--skip-recalc-adj-factor`）；
- 需要全量校验或口径调整时：手动跑 `python main.py recalc_adj_factor ...`。

## 数据一致性与故障恢复

- `scripts/check_data_integrity.py`
  - 只读一致性检查（例如 `price_data_latest_date` 与 `daily_prices` 的 MAX(date) 对齐性、symbol 规范等）。
- `scripts/calibrate_price_latest_date.py`
  - 以 `daily_prices` 实际数据回算 `securities.price_data_latest_date`，用于纠偏。
- 对于“无数据返回”的情况：
  - 不应把 `price_data_latest_date` 人为推进（否则会掩盖缺口并导致永久漏数）。
  - 应保留现状并在日志中显式提示，方便排查数据源延迟/缺失。
