# Stock Data Pipeline

一个 Greenfield 美股日线数据管道。当前主存储是 PostgreSQL，ClickHouse 作为未来矩阵计算与回测读取层；所有身份流转以 `security_id` 为锚点，`symbol` 只作为当前属性或历史属性。

核心原则：数据库的事实层只保存 raw truth。日线行情只存供应商/交易所给出的原始事实字段；复权价格、换手率、技术指标、成交额等派生值不进入事实表。复权因子允许单独分层保存：供应商因子是 reference snapshot，内部因子是带版本和事件哈希的 reproducible cache。

数据源：
- Massive：US 股票 universe、证券详情、公司行动、日线聚合、Grouped Daily、Daily Ticker Summary、shares/float。
- 官方/参考源：交易所 MIC/日历与 SEC filing metadata 使用独立底层表承载；Massive 对应 endpoint 只作为补充和对账来源。

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
docker compose up -d clickhouse
alembic upgrade head
python main.py init_clickhouse
python main.py update --market US
```

`.env` 至少需要：
- `DATABASE_URL`：PostgreSQL 连接串。
- `CLICKHOUSE_URL`：ClickHouse HTTP 地址，Docker 默认是 `http://localhost:8123`。
- `CLICKHOUSE_DATABASE`：ClickHouse 数据库名，默认 `stock`。

Massive API key 从项目根目录的 `activation_value.txt` 读取：
- 默认路径：`activation_value.txt`。
- 支持一行一个 key。
- 空行和以 `#` 开头的注释行会被忽略。

## 常用命令

```bash
# 推荐的每日定时调度入口
python main.py scheduled_update --market US

# 轻量调试入口：只跑详情、公司行动、日线
python main.py update --market US
python main.py update AAPL

# 初始化 ClickHouse schema
docker compose up -d clickhouse
python main.py init_clickhouse

# 将 PostgreSQL daily_prices 既有历史回填到 ClickHouse
python main.py backfill_clickhouse_daily_bars --limit 10000

# Massive 免费层能力范围内的全量重建（最近 2 年窗口）
python main.py rebuild_massive_dataset --market US

# 同步 Massive universe
python main.py sync_massive_universe --market US

# 单项调试入口（一般不需要日常手动跑）
python main.py update_massive_details AAPL NVDA --workers 4
python main.py update_massive_actions --market US --workers 4

# 单项调试 raw 日线事实
python main.py update_massive_prices AAPL --full-refresh
python main.py update_grouped_daily --market US --start-date 2026-03-01 --end-date 2026-03-10
python main.py update_open_close_summary AAPL --start-date 2026-03-13 --end-date 2026-03-13

# 历史股本 / float 事实
python main.py update_massive_shares AAPL --full-refresh

# 辅助 raw facts
python main.py update_massive_events META --force
python main.py update_massive_short_data TSLA --force
python main.py update_massive_news TSLA --force --lookback-days 7

# 复权因子 reference/cache，对账用；不会写入 daily_prices
python main.py update_adjustment_factors AAPL
python main.py update_adjustment_factors AAPL --refresh-vendor-daily-bars --daily-start-date 2026-02-01 --daily-end-date 2026-05-13

# 清理非普通股 / ETF 证券（默认 dry-run）
python main.py cleanup_us_universe
python main.py cleanup_us_universe --apply

# 迁移数据库（需要 OLD_DATABASE_URL / NEW_DATABASE_URL）
python scripts/migrate_database.py
```

`update` 默认顺序：
1. `update_massive_details`：自动检测是否需要更新股票基本信息（默认 30 天间隔）。
2. `update_massive_actions`：自动判断是否需要拉取最新分红/拆股（默认 90 天间隔或缺失）。
3. `update_massive_prices`：自动补齐当前缺失的日线 raw bar。

`scheduled_update` 是推荐的 cron 入口，顺序执行并复用同一进程内的 Massive key 限流状态：
- 每天：`update_massive_prices`、`update_massive_short_data` 增量、最近已完成交易日的 `update_open_close_summary --all`。
- 每周六：`update_massive_shares --all`。
- 每周日：`update_massive_actions --all --force`。
- 每月第一个周二：`update_massive_events --all --force`。
- 每月第一个周三：`update_massive_details --all --force`。

Debian 部署使用 systemd timer，每天 UTC+8 `10:00` 运行
`scripts/run_daily_cron.sh`，实际执行同一个 `scheduled_update` 入口。安装和排障命令见
`README.debian.md`。

需要一次性重拉 Massive 可覆盖窗口时，仍然使用各单项命令的 `--force` 或
`--full-refresh` 参数；默认日更路径不会走全量刷新。

## 目录结构

- `main.py`：CLI 中央控制器。
- `scripts/`：各类更新/维护脚本。
- `data_models/models.py`：SQLAlchemy ORM schema。
- `db_manager/`：`DatabaseManager` session/upsert/批量写入（按领域拆分为多个 mixin 模块）。
- `data_sources/`：外部数据源适配器，当前主要为 Massive。
- `utils/`：小型复用工具。
- `alembic/`：数据库迁移。
- `sql/clickhouse/`：ClickHouse DDL。
- `logs/`：运行日志。

## 数据一致性约定

- 当前已实现数据都是日线级别，主存储统一使用 PostgreSQL；字段类型保持 ClickHouse 兼容，方便未来迁移到列式存储和分钟级数据。
- 新增表或字段优先选择能直接映射到 ClickHouse 的口径，例如 `BIGINT/Int64`、`DATE/Date`、`TIMESTAMPTZ/DateTime64`、`NUMERIC(P,S)/Decimal(P,S)`。
- 未来上分钟线时，沿用 `security_id + timestamp + OHLCV + VWAP + trade_count + source + ingested_at` 的口径，避免重新解释日线字段。
- `daily_prices` 只保存 raw bar：`open/high/low/close/volume/vwap/trade_count/pre_market/after_hours`。
- `daily_prices` 不保存 `adj_factor`、`split_adj_factor`、`turnover_rate`、`turnover` 或技术指标。
- `vendor_adjustment_factors` 保存供应商返回的复权因子 reference，例如 Massive 的 `historical_adjustment_factor` 或 adjusted/raw close 比值。
- `computed_adjustment_factors` 保存内部按 `corporate_actions + daily_prices` 重算的 cache，使用 `methodology_version` 和 `event_hash` 标记口径。
- `corporate_actions` 是复权引擎唯一事件来源，保留 `DIVIDEND` / `SPLIT` 的供应商事件身份，允许同一 `ex_date` 存在多笔不同事件。
- `historical_shares` 是换手率计算的点时股本事实来源，使用 `security_id + filing_date + source` 去重。
- `exchanges` 以 MIC 作为交易所身份锚点；`trading_calendars` 保留逐交易所逐日期 session，不被 `exchanges` 取代。
- `sec_filings` 是 SEC filing index foundation；Form 4 明细进入 `insider_transactions`，13-F 明细进入 `institutional_holdings`。
- 价格更新脚本默认只更新到最近一个已收盘交易日，避免时区差异和盘中数据污染。

## 更多文档

- 文档目录：`docs/README.md`
- 架构与约定：`docs/architecture.md`
- 双库混合持久化目标架构：`docs/polyglot_persistence_architecture.md`
- Massive 免费层日线能力：`docs/massive_free_tier_daily_data.md`
- Massive-only 重建与每日运行：`docs/massive_rebuild_and_daily_run.md`
- 变更记录：`CHANGELOG.md`
