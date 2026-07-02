# Changelog

## Unreleased

### Fixed (2026-07-02)

- Deep-review fix batch (16 code packages + docs; full report at `docs/audits/2026-07-02-deep-review.md`):
  - PIT correctness in the factor loading layer: `days_to_cover` visible delay unified with `short_interest_ratio` via a shared `SHORT_INTEREST_VISIBLE_DELAY_DAYS = 14` constant (was 1 — look-ahead leak); 13F aggregation rewritten to accession-level dedup with originals-only visibility (13F-HR/A excluded) and a period-monotonic guard; `insider_net_buy` now enforces `NON_DERIVATIVE`/`TRANSACTION` filters and accession-level multi-owner dedup in SQL; market-cap panel rolls the shares snapshot forward across SPLIT events (fixes the split-window ~10x median distortion feeding `size`/`earnings_yield`), and `update_massive_shares` force-refreshes snapshots stale relative to a recent split. Historical trials for the affected factors need to be re-run.
  - Identity: CIK-branch resolver now checks FIGI conflicts before declaring a rename (no more identity hijack); history-symbol resolution picks candidates by interval semantics with an ambiguous fallback; `rename_security` closes/open symbol-history intervals; FTD CUSIP bridge gains PIT semantics (is_active filter, unstable-symbol skip, `start_date` lower bound) plus a reverse audit and `scripts/repair_cusip_links.py`; rename chains in `sync_massive_universe` are topologically ordered with per-row failure isolation.
  - Grouped daily: symbol→id map restricted to active securities with ambiguity rejection; historical dates outside a 10-session window revert to update-only (no inserts under current-symbol attribution); no watermark stamping for NULL `price_data_latest_date` securities; `--end-date` clamped to the last completed session.
  - SEC EDGAR: rate-limited 403s on the daily form index are retried with backoff instead of being swallowed as "no filings"; 13F submissions with a broken primary doc backfill period/form/CIK from the SGML header and are rejected (retried later) rather than written with NULL period.
  - Evaluation/backtest: removed the dead terminal-sensitivity `run_backtest` call that crashed under pandas 3; `_pit_regression` now counts presence mismatches (value appears/disappears) as violations; IC uses the eligibility-filtered cross-section; trial IDs include an engine-code fingerprint; trials store writes are pid-suffixed and file-locked.
  - Observability/security: health_report only thresholds 13F quarters past the filing deadline, skips malformed periods, and flags >12h RUNNING zombies; loguru sinks set `diagnose=False`; API-key masking shared via `utils/secret_masking.py` and applied across the retry/exception chain (`raise ... from None`); `backup_postgres.sh` no longer exposes the DSN via argv or exports the whole `.env`.
  - CLI: unsupported markets exit 2; `update_adjustment_factors` subcommand forwards `--changed-since`/`--fail-on-vendor-mismatch`/`--max-mismatch-rate`; per-step stats now reach `pipeline_task_runs`; `--changed-since` also picks up dividends whose `ex_date` just became effective.
  - Docs brought back in line with the code (this batch): AGENTS.md build/test commands and pytest guidance, scheduled_update step tables deduplicated to the root README authority, factor counts/loader descriptions, identity event-type listing, test paths/counts.

### Fixed (2026-06-30)

- 13F historical coverage postmortem follow-ups: quarterly backfill via `update_institutional_holdings --quarter` completed for the historical window, and `research/factors/asof.py` resolves same-day visibility ties deterministically by the newest staleness anchor (the old "largest factor value wins" dedup was a correctness bug). See `docs/audits/2026-06-30-13f-factor-coverage-postmortem.md`.

### Fixed (2026-06-29)

- Demoted trading-day-gap and split-jump integrity checks from blocking to warning: suspended/illiquid tickers and normal split-day volatility were forcing `check_data_integrity` to exit 2 every day, masking real problems.
- Prevented case-insensitive ticker collisions in `update_grouped_daily` from attributing bars to the wrong security.

### Added (2026-06-24)

- Second factor batch — 5 new PIT builtin factors: `days_to_cover` (short interest / 20-day avg volume), `institutional_breadth` / `delta_institutional_ownership` / `ownership_concentration` (13F aggregates via the shared `research/institutional.py` loader), and `insider_net_buy` (90-day rolling net purchase from Form 3/4/5). `docs/factors.md` documents the catalog.

### Added (2026-06-23, post short_volume_ratio)

- `SecurityIdentityResolver` (`utils/security_identity.py`): FIGI→CIK→symbol→history priority chain for identity resolution; `sync_massive_universe` uses it for rename/recycle detection, with `security_identity_events` as the audit trail (migration `b7c8d9e0f1a2`).
- Identity repair tooling: `scripts/audit_security_identity.py` (read-only reconciliation, exit 2 = migration-blocking) and `scripts/repair_identity.py --dry-run/--apply` (merges split identities, writes MERGE events). See `docs/identity_lifecycle.md`.
- `pipeline_task_runs` table: per-step execution records (start/end/status/exit_code) for every `scheduled_update` run.
- `health_report` command: per-domain data health summary with unified exit codes (0/1/2). See `docs/data_quality_runbook.md`.
- `check_data_integrity` wired into the daily `scheduled_update` chain.

### Added (2026-06-23)

- `short_volume_ratio` factor: daily FINRA short volume ratio (`short_volume / total_volume`) as a PIT builtin factor, complementing the semi-monthly `short_interest_ratio`. Uses T+1 visible delay and 10-day staleness. `research/short_volume.py` + `research/factors/builtins/short_volume.py` + 11 unit tests.

### Added (2026-06-17)

- `short_interest_ratio` factor: semi-monthly PIT short interest ratio (`short_interest / total_shares`) as the first non-fundamentals builtin factor. Uses `event_table_to_asof_panel` with 14-day visible delay and 30-day staleness. `research/short_interest.py` + `research/factors/builtins/short_interest.py` + 18 unit tests.

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
