# Changelog

## Unreleased

### Changed

- Rebuilt the project around the Greenfield Massive-only path:
  - PostgreSQL is the current metadata/event/raw-fact store.
  - ClickHouse remains the future matrix-read and backtest-scan layer.
  - `security_id` is the canonical identity; `symbol` is mutable metadata/history.
- Limited the active US universe to Massive `CS` and `ETF` securities.
- Kept `daily_prices` as raw daily market facts only: OHLCV, VWAP, trade count, OTC flag, pre-market, and after-hours.
- Split adjustment-factor storage into explicit non-truth layers:
  - `vendor_adjustment_factors` for Massive reference snapshots.
  - `computed_adjustment_factors` for reproducible internal caches with `methodology_version` and `event_hash`.
- Updated the default `update` flow to run details, corporate actions, and missing daily prices.
- Preserved `daily_run` only as a compatibility alias for `update`.
- Added `scheduled_update` as the production scheduler entry: daily raw facts run every day, while shares/actions/events/details are staggered weekly or monthly.
- Changed Massive short data daily updates to use actual table max dates incrementally; `--force` remains available for full-window refreshes.

### Added

- Massive ingestion coverage for verified non-financial facts:
  - ticker universe and details;
  - ticker events and symbol history;
  - dividends and splits;
  - raw daily bars, grouped daily refresh, and open/close summaries;
  - shares and float;
  - short interest and short volume;
  - news articles and ticker sentiment insights.
- `update_adjustment_factors` command for rebuilding internal adjustment-factor caches and comparing them with Massive reference factors.
- `cleanup_us_universe` support for removing adjustment-factor rows associated with excluded securities.
- ClickHouse initialization and PostgreSQL-to-ClickHouse daily-bar backfill commands.
- Debian systemd service/timer files and cron wrapper for running `scheduled_update` every day at UTC+8 09:00.

### Removed

- Removed the current supported path for Polygon, YFinance, AkShare/Eastmoney, local turnover backfills, old `daily_prices.adj_factor`, and technical-indicator fact tables.
- Removed legacy S&P 500 symbol-keyed constituent storage in favor of future generic `security_id`-anchored index membership design.
- Excluded financial statements and ratios from the current Massive ingestion scope.

### Fixed

- Prevented `DatabaseManager.upsert_security_info()` from overwriting unrelated `securities` maintenance fields when updating details.
- Prevented Massive step-to-step 429 bursts by sharing per-key rate-limit history across tasks, using round-robin key selection, and blocking keys that just hit 429.
- Redacted Massive `apiKey` values from request and exception logs.
- Updated price updaters to target the most recent completed trading session instead of local `date.today()`.
- Cleaned duplicate AAPL-style corporate-action rows where old synthetic IDs and newer Massive event IDs represented the same economic event.
- Fixed corporate-action incremental batches losing history for never-synced securities: any batch containing a security without `actions_last_updated_at` now refetches from the full Massive history floor.
- Stopped backfilling future-effective floats into earlier `historical_shares` snapshots (`filing_date` is the point-in-time boundary; `historical_floats` keeps the full series).
- Fixed `upsert_securities_by_symbol` crashing with a CompileError when batch rows had different key sets (detail payloads drop `None` fields); rows are now grouped by key set.
- Stopped news insight re-upserts from overwriting an already-resolved `security_id` with NULL.
- Replaced 16 ineffective `exc_info=True` arguments (silently ignored by loguru) with `logger.opt(exception=...)` so error logs include tracebacks again.
- Made ClickHouse write failures non-fatal for the PostgreSQL ingestion path: clients degrade with a warning and `backfill_clickhouse_daily_bars` (now strict) can repair later.
- Made pipeline scripts return exit code 1 on critical failure and `scheduled_update` isolate step failures, continue remaining steps, and exit non-zero with a failure summary.
- Guarded previously unprotected `as_completed` result loops in short-data/news/events updates so one failed batch no longer aborts result accounting.
- Aligned vendor adjustment-factor fallback `factor_key` decimal formatting with the computed-side normalization so reconciliation joins match.

### Maintenance

- Consolidated 16 duplicated `setup_logging` implementations into `utils/script_logging.py`; the main controller now restores its own log sinks after each orchestrated step.
- Dropped the redundant `_security_id_date_uc` unique constraint on `daily_prices` (duplicate of the composite primary key) via migration `a1b2c3d4e5f6`.
- Replaced per-security timestamp UPDATE loops with a batched `update_security_timestamps`.
- Removed dead code: unused `ActionType` enum, `bulk_update_records`, `get_latest_floats_batch`, the yfinance-era `Dividends`/`Stock Splits` placeholder columns, and a no-op post-query filter in the price updater.
- Untracked committed log files, `__pycache__` artifacts, and the stray AAPL CSV; added `activation_value.txt` (API keys) and `*.csv` to `.gitignore`; documented `POSTGRES_*` variables required by docker-compose in `.env.example`.
