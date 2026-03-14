# Changelog

## Unreleased

### Added
- Add forward-adjustment support (`daily_prices.adj_factor`) and a maintenance command:
  - `python main.py recalc_adj_factor ...`
  - Algorithm: front-adjust to latest date (=1) with total return (splits + cash dividends).
  - `python main.py update_actions ...` now triggers an automatic per-symbol `adj_factor` recalculation by default (opt-out via `--skip-recalc-adj-factor`).
- Add corporate-actions history backfill via YFinance (useful when Polygon actions history is incomplete due to plan limits):
  - `python main.py backfill_actions ...` (fills missing dividends/splits and can recalc `adj_factor`)
- Add historical shares + turnover backfill tools:
  - `python main.py update_historical_shares ...` (YFinance `get_shares_full`)
  - `python main.py backfill_turnover_rate ...` (fills `turnover_rate` from `volume / total_shares`, only NULL by default)

### Fixed
- Prevent `DatabaseManager.upsert_security_info()` from overwriting unrelated `securities` maintenance fields (e.g. `price_data_latest_date`) when updating details.
- Prevent price updaters from overwriting columns they do not provide:
  - Eastmoney updater no longer writes `vwap=NULL`.
  - Polygon daily updater no longer writes `turnover_rate=NULL`.
- Update price updaters to target the most recent **completed** trading session (close-aware) instead of using local `date.today()`.
- Speed up Grouped Daily refresh by switching to `bulk_update_mappings` (no ORM `merge()` per row).

### Chore
- Stop tracking local artifacts (`.idea/`, `logs/*.log`, `__pycache__/*.pyc`) and ignore `.claude/` and `.DS_Store`.
- Add `utils/trading_calendar.py` for market-session-aware date helpers.
- Add read-only integrity checker: `scripts/check_data_integrity.py`.
- Update dependencies: add `tqdm`, remove unused `finnhub-python`, and pin `yfinance==1.2.0` for shares backfill.
