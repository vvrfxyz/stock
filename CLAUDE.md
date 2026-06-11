# CLAUDE.md

This file gives Claude Code a compact, current map of the repository.

## Project Overview

This is a Greenfield US stock data pipeline. PostgreSQL is the current system of record for metadata, events, and daily raw facts. ClickHouse is prepared as the future matrix-read layer for backtests and large scans.

Primary data source:

- Massive: US ticker universe, ticker details, ticker events, dividends, splits, daily bars, grouped daily bars, open/close summary, shares/float, short data, and news.

Core architecture rules:

- Use `security_id` as the durable identity everywhere.
- Treat `symbol` as a mutable attribute/history item, never as the durable key.
- Keep `daily_prices` as raw OHLCV facts only.
- Do not store adjusted prices, turnover, local amount calculations, or technical indicators in fact tables.
- Adjustment factors may be stored only in separated layers: `vendor_adjustment_factors` as vendor reference snapshots and `computed_adjustment_factors` as reproducible internal caches.
- Only US `CS` and `ETF` securities are kept in the current universe.

## Main Files

- `main.py`: Central CLI controller.
- `data_models/models.py`: SQLAlchemy schema source of truth.
- `db_manager.py`: Shared session, upsert, cleanup, and batch-write utilities.
- `data_sources/massive_source.py`: Massive REST adapter.
- `utils/massive_config.py`: Massive keys, supported types, history-window helpers.
- `utils/key_rate_limiter.py`: Per-key, process-shared Massive rate limiter.
- `sql/clickhouse/polyglot_persistence.sql`: ClickHouse DDL.
- `alembic/versions/`: PostgreSQL migrations.

## Current Tables

- `securities`: Durable security identity and current metadata.
- `security_symbol_history`: Symbol-change history.
- `daily_prices`: Raw daily OHLCV/VWAP/trade-count facts.
- `corporate_actions`: Dividend and split event truth.
- `vendor_adjustment_factors`: Vendor-provided adjustment reference data.
- `computed_adjustment_factors`: Internal reproducible adjustment-factor cache.
- `historical_shares`: Point-in-time total/float share facts.
- `historical_floats`: Massive float facts by effective date.
- `short_interests`: Settlement-date short interest.
- `short_volumes`: Daily short-volume facts.
- `news_articles` and `news_article_insights`: Massive news and sentiment metadata.
- `exchanges`: Exchange/MIC reference data.
- `trading_calendars`: Exchange-level trading sessions keyed by `exchange_mic + trade_date`.
- `security_identifiers`: Point-in-time identifier mapping such as CUSIP/CIK/FIGI/ISIN.
- `sec_filings`: SEC EDGAR filing index metadata.
- `insider_transactions`: Form 3/4/5 ownership transaction rows.
- `institutional_holdings`: 13-F holdings rows.

Financial statements and ratios are still out of current ingestion scope. Use `sec_filings` as the SEC filing index foundation; do not revive `financial_reports` as a vague catch-all table.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
```

Required configuration:

- `DATABASE_URL` in `.env`.
- Massive API keys in `activation_value.txt`, one key per line.

Optional ClickHouse configuration:

- `CLICKHOUSE_URL`
- `CLICKHOUSE_DATABASE`

## Common Commands

```bash
python main.py update --market US
python main.py update AAPL

python main.py sync_massive_universe --market US
python main.py update_massive_details AAPL --force
python main.py update_massive_actions AAPL --force
python main.py update_massive_prices AAPL --full-refresh
python main.py update_massive_shares AAPL --full-refresh
python main.py update_massive_events META --force
python main.py update_massive_short_data TSLA --force
python main.py update_massive_news TSLA --force --lookback-days 7
python main.py update_adjustment_factors AAPL

python main.py init_clickhouse
python main.py backfill_clickhouse_daily_bars --limit 10000

python scripts/check_data_integrity.py --limit 5
python scripts/audit_recent_data.py --sample-size 32   # vendor 对账抽样审计（耗 API 配额）
```

Read adjusted prices via `utils/adjusted_prices.get_adjusted_daily_bars(session, symbol_or_id, start=, end=, as_of=)` — never store adjusted values back into fact tables.

`daily_run` is only a compatibility alias for `update`.

## Deployment (192.168.1.253)

- Production host: `home-debian` in `~/.ssh/config` (192.168.1.253, root login); code at `/home/wenruifeng/projects/stock`, runs as user `wenruifeng`.
- The host cannot reach github.com. Sync by SSH push, not remote pull:
  1. `git push origin main` (GitHub), then
  2. `git push ssh://home-debian/home/wenruifeng/projects/stock <sha>:refs/heads/_sync_main`
  3. on remote: `git checkout main && git reset --hard <sha>`, then `chown -R wenruifeng:wenruifeng` anything root created, drop `_sync_main`.
- Remote-only secrets that must never be overwritten or committed: `.env`, `activation_value.txt`.
- After schema changes run `.venv/bin/alembic upgrade head` on the remote.
- systemd timer `stock-daily-run.timer` runs `scheduled_update` daily at 10:00 Asia/Shanghai; check `journalctl -u stock-daily-run.service` and `logs/cron_daily_run.log`. See `README.debian.md` and `docs/deployment.md`.

## Data Integrity Notes

- Price scripts should update only through the most recent completed trading session.
- `securities.price_data_latest_date` should match `daily_prices.max(date)`.
- Company actions are keyed by Massive `source_event_id`; synthetic historical IDs should be cleaned up when a real vendor event ID becomes available.
- `computed_adjustment_factors.methodology_version` currently uses `raw_actions_v1`.
- Turnover and adjusted prices are calculation outputs, not facts.
- `historical_shares.filing_date` is the point-in-time boundary: never attach floats whose `effective_date` is after it.
- Corporate-action incremental batches must fall back to the full Massive history floor when any security in the batch has no `actions_last_updated_at`.

## Code Conventions

- loguru silently ignores `exc_info=True`; log tracebacks with `logger.opt(exception=e).error(...)`.
- Script logging goes through `utils/script_logging.setup_logging(name)`; do not redefine sinks per script.
- Script `main()` returns an int exit code (0/1) and `__main__` uses `raise SystemExit(main())`; `scheduled_update` isolates step failures and exits non-zero if any step failed.
- `ClickHouseClient` degrades (warn + stop writing) on write failure; only `backfill_clickhouse_daily_bars` uses `strict=True`. PostgreSQL is the system of record.
- Multi-row `pg_insert().values()` requires homogeneous dict keys; group heterogeneous rows with `_group_rows_by_key_set` (see `upsert_securities_by_symbol`).
