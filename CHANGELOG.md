# Changelog

## Unreleased

### Added (2026-06-15)

- Risk-free reference layer for research metrics: `risk_free_rates` table (migration `a6b7c8d9e0f1`) + `update_risk_free_rates` FRED CSV sync + `utils/risk_free_rates.load_risk_free_daily_returns`. Sunday `scheduled_update` refreshes DTB3 for the last 30 days.

### Changed (2026-06-15)

- `research.evaluate` now reports quantile Sharpe/IR on risk-free excess returns by default (`DTB3`, actual/360, held-exposure aligned). Use `--no-risk-free` to reproduce the old rf=0 evaluation basis.
- Cleaned #7 evaluation follow-up nits: coverage no longer exposes non-PIT listing snapshot columns, strict PIT failures persist trials before raising, repeated trial appends set `trial_id` on the skipped result, `latest_only` logs an aggregate warning, and CLI `--start` validation uses argparse exit code 2.

### Added (2026-06-12)

- T3 insider ingestion: `update_insider_transactions` parses Form 3/4/5 ownership XML from the `sec_filings` index into `insider_transactions` (one row per entry × reporting owner, `source+accession+row_hash` idempotency, footnote resolution); weekly incremental added to Sunday `scheduled_update`.
- T4 13-F ingestion: `update_institutional_holdings` discovers 13F-HR filings via EDGAR daily/quarterly form indexes (filer-CIK anchored, independent of issuer-side `sec_filings`), parses full-submission information tables into `institutional_holdings`, and backfills `security_id` from `security_identifiers` CUSIP mappings when available (COALESCE-protected on re-upsert). Migration `c3d4e5f6a7b8` adds `filer_name`. Weekly incremental added to Sunday `scheduled_update`.
- FX reference layer for non-USD dividends: `fx_rates` table (migration `d4e5f6a7b8c9`) + `update_fx_rates` ECB sync + `utils/fx_rates.UsdFxConverter`. `update_adjustment_factors` now converts CAD/NOK/ILS-style dividend cash to USD at the ex-date ECB rate (as-of fallback ≤7 days) instead of skipping; vendor reconciliation excludes these events from the chain (`non_usd_dividends` counter) because vendors emit no factor rows for them. USD event hashes are unchanged; run `update_fx_rates` then `update_adjustment_factors --all` to materialize the previously skipped ~322 events.
- CUSIP identity mapping: `sync_cusip_identifiers` extracts CUSIP|SYMBOL pairs from SEC fails-to-deliver files into `security_identifiers` (source `SEC_FTD`, exact + dotless share-class symbol matching, ambiguity-guarded) and backfills `institutional_holdings.security_id` for NULL rows (`map_unlinked_holdings_to_securities`, one-CUSIP-one-security HAVING guard). Sunday schedule runs it before the 13F incremental so new holdings map at write time.

### Removed (2026-06-12)

- Removed the ClickHouse layer entirely (client, `init_clickhouse` / `backfill_clickhouse_daily_bars` commands, DDL, docker-compose service, dual-writes in price scripts). PostgreSQL is the only store. The polyglot design doc is archived at `docs/archive/polyglot_persistence_architecture.md` for a future rebuild once minute-level data (Massive paid tier) arrives.

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
