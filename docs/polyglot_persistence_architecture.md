# PostgreSQL + ClickHouse 混合持久化架构

本文记录当前目标架构：PostgreSQL 做元数据、事件、低频事实和日线 raw truth 的事务中心；ClickHouse 做未来高吞吐矩阵读取、回测扫描和分钟级数据的计算层。

## 核心原则

- `security_id` 是全局身份锚点。
- `symbol` 是当前属性或历史属性，不是 durable key。
- PostgreSQL 保存可审计事实和 cache/reference 的元信息。
- ClickHouse 保存大规模 bars 和未来宽矩阵。
- `daily_prices` 永远是 raw bar，不保存复权价格、复权因子、换手率、成交额或技术指标。
- 复权因子可以保存，但必须分层：vendor reference 和 computed cache 都不是 truth。

## PostgreSQL 职责

PostgreSQL 存储“需要关系约束、点时解释、人工可审计”的数据。

当前核心表：

- `securities`
  Durable identity、当前 symbol、交易所、类型、行业、CIK、FIGI、shares outstanding、维护时间戳。

- `security_symbol_history`
  symbol/event 历史，防止 symbol 变更带来的幸存者偏差。

- `daily_prices`
  Raw 日线 OHLCV、VWAP、trade_count、OTC、pre_market、after_hours。

- `corporate_actions`
  分红/拆股事件 truth。Massive `id` 进入 `source_event_id`。

- `vendor_adjustment_factors`
  Massive `historical_adjustment_factor` 或 adjusted/raw close 比值等供应商 reference。

- `computed_adjustment_factors`
  内部由 `corporate_actions + daily_prices` 重建的 cache，带 `methodology_version` 和 `event_hash`。

- `historical_shares`
  点时 total/float shares，含 `filing_date` 和 `period_end_date`。

- `historical_floats`
  Massive float by effective date。

- `short_interests`
  结算日 short interest。

- `short_volumes`
  日度 short volume 分解。

- `news_articles` / `news_article_insights`
  新闻、publisher、ticker insights 和 sentiment。

- `exchanges`
  交易所参考数据，以 ISO 10383 MIC 作为主键。

- `trading_calendars`
  交易所级交易日历，主键为 `(exchange_mic, trade_date)`，保存是否开市、半日市、开收盘时间和来源。

- `security_identifiers`
  CUSIP/CIK/FIGI/ISIN 等证券标识符与 `security_id` 的点时映射。

- `sec_filings`
  SEC EDGAR filing index metadata，以 `accession_number` 为稳定锚点。

- `insider_transactions`
  Form 3/4/5 ownership transaction 明细，用于内部人交易研究。

- `institutional_holdings`
  13-F holdings 明细，用于机构持仓、拥挤度和季度调仓研究。映射到 `security_id` 依赖 CUSIP/issuer/class 的点时 identifier layer。

## ClickHouse 职责

ClickHouse 负责机器读取，不负责人工编辑。

当前 DDL 位于：

```bash
sql/clickhouse/polyglot_persistence.sql
```

当前核心表：

- `raw_daily_bars`
  原始日线 bars，`ReplacingMergeTree(ingested_at)` 支持供应商修正后覆盖读取。

- `canonical_daily_bars`
  未来回测默认读取表，由清洗/择源规则生成。

当前初始化命令：

```bash
python main.py init_clickhouse
```

PostgreSQL 回填到 ClickHouse：

```bash
python main.py backfill_clickhouse_daily_bars --limit 10000
```

## ClickHouse Dictionary

目标是让 ClickHouse 查询时可以读取 PostgreSQL 维度字段，例如行业、当前 symbol、active 状态。

示意：

```sql
CREATE DICTIONARY pg_securities_dict
(
    id Int64,
    current_symbol String,
    market String,
    sector String,
    industry String,
    is_active UInt8
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
    port 5432
    host 'postgres-host'
    user 'postgres'
    password 'password'
    db 'stock'
    table 'securities'
))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(HASHED());
```

## 复权和派生值

Truth 层：

- `daily_prices` raw OHLCV。
- `corporate_actions` 分红/拆股事件。

Reference 层：

- `vendor_adjustment_factors` 保存供应商复权参考值。

Cache 层：

- `computed_adjustment_factors` 保存内部可重建结果。
- 当前 `raw_actions_v1`：
  - split factor = `split_from / split_to`
  - dividend factor = `(previous_raw_close - cash_amount) / previous_raw_close`
  - cumulative factor = 当前事件及之后所有事件的累计乘积

计算层：

- adjusted price = raw price * computed factor。
- turnover = volume / point-in-time shares。
- indicators = raw/canonical bars 的计算结果。

Massive 提供 SMA/EMA/MACD/RSI endpoint，但这些值只作为 vendor reference 对账输入。后续可以增加校验工具，用本地 raw/canonical bars 计算同口径指标，并和 Massive 返回值比较；差异用于定位复权、窗口边界或数据缺口问题，不把 Massive 指标写入事实表。

这些计算结果可以缓存，但不能混入 facts。

## 分钟级迁移预留

未来分钟线进入 ClickHouse 时，建议沿用：

- `security_id`
- `timestamp`
- `source`
- `vendor_symbol`
- `open/high/low/close`
- `volume`
- `vwap`
- `trade_count`
- `ingested_at`

PostgreSQL 不适合作为分钟级主扫描表；它只保留必要元数据、事件和审计控制。

## 工程落地点

- ORM schema：`data_models/models.py`
- DB upsert：`db_manager.py`
- Massive adapter：`data_sources/massive_source.py`
- Main CLI：`main.py`
- ClickHouse DDL：`sql/clickhouse/polyglot_persistence.sql`
- 当前架构文档：`docs/architecture.md`
