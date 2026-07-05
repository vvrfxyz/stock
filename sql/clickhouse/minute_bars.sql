-- Minute bars in ClickHouse (2026-07 intraday_1m_vw backfill).
-- security_id 是 PostgreSQL securities.id 的锚点值；symbol->security_id 的任期
-- 映射在 ClickHouse 内完成（symbol_tenures 由 scripts/import_minute_bars_clickhouse.py
-- --refresh-tenures 从 PostgreSQL 导出，段间已在 Python 侧消解重叠——重叠区间双方
-- 出局，与 import_day_aggs 的 ambiguous 守卫同语义）。

CREATE DATABASE IF NOT EXISTS stock;

-- 任期映射表：[start_date, end_date) 半开区间，ET 交易日口径。
CREATE TABLE IF NOT EXISTS stock.symbol_tenures (
    symbol       String,
    security_id  Int64,
    start_date   Date,
    end_date     Date
) ENGINE = MergeTree
ORDER BY (symbol, start_date);

-- 装载中转：一次一个 UTC 月的 parquet 原样进来，转换后 TRUNCATE。
CREATE TABLE IF NOT EXISTS stock.minute_bars_staging (
    datetime     DateTime64(6, 'UTC'),
    symbol       String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    vw           Float64,
    transactions Int64
) ENGINE = MergeTree
ORDER BY tuple();

-- 分钟线主表：UTC 时间戳（含盘前盘后 4:00-20:00 ET），未复权原始价。
-- ReplacingMergeTree(ingested_at) + 装载前 DROP PARTITION 双保险幂等。
CREATE TABLE IF NOT EXISTS stock.minute_bars (
    security_id   Int64,
    ts            DateTime('UTC'),
    vendor_symbol LowCardinality(String),
    open          Float64 CODEC(ZSTD(3)),
    high          Float64 CODEC(ZSTD(3)),
    low           Float64 CODEC(ZSTD(3)),
    close         Float64 CODEC(ZSTD(3)),
    volume        UInt64  CODEC(T64, ZSTD(3)),
    vwap          Float64 CODEC(ZSTD(3)),
    trade_count   UInt32  CODEC(T64, ZSTD(3)),
    source        LowCardinality(String) DEFAULT 'flatfiles_1m',
    ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(ts)
ORDER BY (security_id, ts);
