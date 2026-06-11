-- ClickHouse DDL for the polyglot persistence target architecture.
-- PostgreSQL owns securities.id; ClickHouse stores that value as Int64 security_id.

CREATE DATABASE IF NOT EXISTS stock;

USE stock;

CREATE TABLE IF NOT EXISTS raw_daily_bars (
    security_id       Int64,
    date              Date,
    source            LowCardinality(String),
    vendor_symbol     String,

    open              Decimal(19,6),
    high              Decimal(19,6),
    low               Decimal(19,6),
    close             Decimal(19,6),
    volume            Nullable(Int64),
    vwap              Nullable(Decimal(19,6)),
    trade_count       Nullable(Int64),
    otc               UInt8 DEFAULT 0,

    ingested_at       DateTime64(3, 'UTC'),
    is_suspect        UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(date)
ORDER BY (security_id, date, source);

CREATE TABLE IF NOT EXISTS canonical_daily_bars (
    security_id       Int64,
    date              Date,
    selected_source   LowCardinality(String),

    open              Decimal(19,6),
    high              Decimal(19,6),
    low               Decimal(19,6),
    close             Decimal(19,6),
    volume            Nullable(Int64),
    vwap              Nullable(Decimal(19,6)),

    built_at          DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(built_at)
PARTITION BY toYYYYMM(date)
ORDER BY (security_id, date);

CREATE DICTIONARY IF NOT EXISTS pg_securities_dict
(
    id Int64,
    current_symbol String,
    market String,
    sector String,
    industry String
)
PRIMARY KEY id
SOURCE(POSTGRESQL(
    port 5432
    host '{PG_HOST}'
    user '{PG_USER}'
    password '{PG_PASSWORD}'
    db '{PG_DATABASE}'
    table 'securities'
))
LIFETIME(MIN 300 MAX 3600)
LAYOUT(HASHED());
