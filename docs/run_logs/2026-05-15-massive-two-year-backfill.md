# 2026-05-15 Massive Two-Year Backfill Run Log

Workspace: `/Users/wenruifeng/PycharmProjects/stock`
Local time: `2026-05-15 12:45:20 CST`
UTC time: `2026-05-15 04:45:20 UTC`

## Cron Check

- User crontab still contains `/Users/wenruifeng/PycharmProjects/stock/scripts/run_daily_cron.sh` at `0 10 * * *`, guarded by `/tmp/stock_daily_run.lock`.
- User crontab also contains `/Users/wenruifeng/.local/share/x-data-cron/cron/run-x-ingest-24h-finance.sh` every 4 hours.
- Process checks at run start did not find an active `main.py`, `update_massive*`, `scheduled_update`, `run_daily_cron.sh`, or stock-related Python cron process.
- Long manual backfill should run under `/tmp/stock_daily_run.lock` so the daily stock cron does not overlap.

## Daily Prices Baseline

- `daily_prices` rows at start: `21,284,990`
- earliest `daily_prices.date`: `1972-08-25`
- latest `daily_prices.date`: `2026-05-14`
- distinct `daily_prices.security_id`: `10,848`
- Note: `2026-05-14` had only `10` rows at start, from the prior sample verification run.

Recent date counts at start:

| date | rows |
| --- | ---: |
| 2026-05-14 | 10 |
| 2026-05-13 | 10,124 |
| 2026-05-12 | 10,131 |
| 2026-05-11 | 10,044 |
| 2026-05-08 | 10,017 |
| 2026-05-07 | 10,031 |
| 2026-05-06 | 10,033 |
| 2026-05-05 | 9,965 |
| 2026-05-04 | 10,010 |
| 2026-05-01 | 9,939 |

## Source Relabel Scope

Approved interpretation: relabel existing vendor market/event data from `MASSIVE` to `POLYGON`, then ingest fresh Massive data for the recent two-year window.

Tables to relabel:

- `corporate_actions`
- `vendor_adjustment_factors`
- `historical_shares`
- `historical_floats`
- `short_interests`
- `short_volumes`
- `security_symbol_history`
- `news_articles`

Tables intentionally not relabeled:

- `daily_prices`: no `source` column; Massive price refresh overwrites or fills by `(security_id, date)`.
- `trading_calendars`: `manual` infrastructure/reference data.
- `exchanges`: `migration` infrastructure/reference data.
- SEC filing/holding tables: not Polygon market data and currently empty in source-count scan.

## Resume Notes

If interrupted after relabeling but before completion, rerun the Massive commands. The source-aware tables can hold `POLYGON` and `MASSIVE` side by side because their conflict keys include `source`.

## Source Relabel Result

Completed at local time `2026-05-15 12:46 CST`.

| table | rows relabeled from MASSIVE to POLYGON |
| --- | ---: |
| corporate_actions | 229,248 |
| vendor_adjustment_factors | 35,360 |
| historical_shares | 10,148 |
| historical_floats | 9 |
| short_interests | 789 |
| short_volumes | 8,084 |
| security_symbol_history | 11 |
| news_articles | 36 |

## Massive Backfill Commands

### 1. Daily prices

Started at local time `2026-05-15 12:46 CST`.

Command:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python main.py update_massive_prices --full-refresh --market US --workers 18
```

Observed startup:

- target securities: `10,260`
- end trading date: `2026-05-14`
- Massive history floor used by code: `2024-05-14`

Checkpoint at local time `2026-05-15 13:33 CST`:

- active process: manual `update_massive_prices --full-refresh --market US --workers 18`, not cron.
- lock: `/tmp/stock_daily_run.lock` is held by this manual run, so the stock daily cron will not overlap while it remains active.
- terminal progress observed around `4,394 / 10,260` (`~43%`), alphabetically in `H*` symbols.
- `daily_prices` rows: `21,293,771`
- `daily_prices.date` range: `1972-08-25` to `2026-05-14`
- distinct `daily_prices.security_id`: `10,848`
- rows in Massive two-year window `2024-05-14` through `2026-05-14`: `4,388,137`
- `2026-05-14` rows: `4,506`
- active US CS/ETF securities with `price_data_latest_date >= 2026-05-14`: `4,506 / 10,260`
- active US CS/ETF securities still at `2026-05-13`: `5,670 / 10,260`

Resume warning:

- The running command is a `--full-refresh`, so every processed security refreshes the Massive-covered window starting at `2024-05-14`.
- If the run is interrupted, rerunning without `--full-refresh` may only top up from `price_data_latest_date + 1` and may not fully refresh the unprocessed symbols' two-year window.
- Safest resume is to continue with a `--full-refresh` command for the remaining symbols, or rerun the same command if exact remaining-symbol slicing is not needed.

Completed at local time `2026-05-15 14:29 CST`.

Terminal summary:

- success: `10,259`
- no new data: `1`
- up to date: `0`
- errors: `0`
- rows written: `4,223,306`
- elapsed: `1:42:24.716807`

Database verification at local time `2026-05-15 14:30 CST`:

- `daily_prices` rows: `21,305,808`
- `daily_prices.date` range: `1972-08-25` to `2026-05-14`
- distinct `daily_prices.security_id`: `10,848`
- rows in Massive two-year window `2024-05-14` through `2026-05-14`: `4,400,174`
- `2026-05-14` rows: `10,137`
- active US CS/ETF securities with `price_data_latest_date >= 2026-05-14`: `10,137 / 10,260`
- active US CS/ETF securities still at `2026-05-13`: `39 / 10,260`
- active US CS/ETF securities with null `price_data_latest_date`: `1 / 10,260`
- note: the remaining active securities have actual max price dates before `2026-05-14`; the run reported no processing errors.

### 2. Corporate actions

Started and completed at local time `2026-05-15 14:33 CST`.

Command:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python main.py update_massive_actions --all --force --market US
```

Terminal summary:

- success with new rows: `6,557`
- no actions: `3,703`
- errors: `0`
- elapsed: `0:02:37`

### 3. Short interest / short volume

Started at local time `2026-05-15 14:37 CST`.
Completed at local time `2026-05-15 14:59 CST`.

Command:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python main.py update_massive_short_data --all --force --market US
```

Terminal summary:

- success: `10,260`
- no data: `0`
- errors: `0`
- short_interest rows written: `405,843`
- short_volume rows written: `4,091,766`
- elapsed: `0:22:09`

## Checkpoint: 2026-05-15 14:58 CST

Cron/process state:

- `crontab -l` still contains the stock daily cron at `0 10 * * *`, guarded by `/tmp/stock_daily_run.lock`.
- `crontab -l` still contains the x-data finance cron every 4 hours.
- `/tmp/stock_daily_run.lock` was available at `2026-05-15 14:58 CST`.
- No active `main.py`, `run_daily_cron.sh`, `update_massive*`, `stock_daily_run.lock`, or `lockf` process remained after excluding the process-check command itself.

Source-count checkpoint:

| table | MASSIVE rows | POLYGON rows |
| --- | ---: | ---: |
| corporate_actions | 58,192 | 229,248 |
| vendor_adjustment_factors | 56,985 | 35,360 |
| historical_shares | 0 | 10,148 |
| historical_floats | 0 | 9 |
| short_interests | 405,843 | 789 |
| short_volumes | 4,091,766 | 8,084 |
| security_symbol_history | 0 | 11 |
| news_articles | 0 | 36 |

### 4. Ticker events / symbol history

Started at local time `2026-05-15 15:00 CST`.

Command:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python main.py update_massive_events --all --force --market US
```

Completed at local time `2026-05-15 16:44 CST`.

Terminal summary from the full run:

- success: `10,259`
- errors: `1`
- symbol history rows written: `11,087`
- elapsed: `1:43:48`

Observed warnings:

- Several transient Massive read timeout / connection reset warnings were retried by the script.
- Several transient Massive `429` warnings were retried by the script.
- The single terminal error was `rvph`, caused by duplicate ticker-change events in one Massive payload producing the same symbol-history upsert key twice.

Fix applied:

- Added a minimal regression test in `tests/test_update_massive_events.py`.
- Updated `scripts/update_massive_events.py` to deduplicate ticker-change rows by `(security_id, symbol, source, start_date)` before upsert.
- Verification command passed: `rtk python -m pytest tests/test_update_massive_events.py -q`.

Retry for failed symbol:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python main.py update_massive_events rvph --force --market US
```

Retry summary:

- success: `1`
- errors: `0`
- symbol history rows written: `1`
- elapsed: `0:00:01`

## Checkpoint: 2026-05-15 16:45 CST

Cron/process state:

- `/tmp/stock_daily_run.lock` was available after the events retry.
- No active `main.py`, `run_daily_cron.sh`, `update_massive*`, `stock_daily_run.lock`, or `lockf` process remained after excluding completed check commands.

Daily prices verification:

- `daily_prices` rows: `21,305,808`
- `daily_prices.date` range: `1972-08-25` to `2026-05-14`
- distinct `daily_prices.security_id`: `10,848`
- rows in Massive two-year window `2024-05-14` through `2026-05-14`: `4,400,174`
- `2026-05-14` rows: `10,137`

Source-count checkpoint:

| table | MASSIVE rows | POLYGON rows |
| --- | ---: | ---: |
| corporate_actions | 58,192 | 229,248 |
| vendor_adjustment_factors | 56,985 | 35,360 |
| historical_shares | 0 | 10,148 |
| historical_floats | 0 | 9 |
| short_interests | 405,843 | 789 |
| short_volumes | 4,091,766 | 8,084 |
| security_symbol_history | 11,088 | 11 |

Checkpoint at local `2026-05-15 17:27 CST`:

- current live job: `historical_shares` / `historical_floats` Massive chunked full refresh
- tmux session: `stock_massive_shares_20260515`
- progress file: `logs/massive_shares_chunks_2026-05-15.jsonl`
- completed chunks:
  - `0001:a:aciw`
  - `0002:acky:afbi`
  - `0003:afcg:airg`
  - `0004:airi:amba`
- running chunk: `0005:ambc:aosl`
- chunks total: `109`; symbols total: `10,849`; chunk size: `100`
- current source counts:
  - `historical_shares`: `MASSIVE=2,883`, `POLYGON=10,148`
  - `historical_floats`: `MASSIVE=250`, `POLYGON=9`

`daily_prices` checkpoint:

- total rows: `21,305,808`
- date range: `1972-08-25` to `2026-05-14`
- two-year window `2024-05-14` to `2026-05-14`: `4,400,174`
- latest date `2026-05-14`: `10,137`
- `pre_market` rows: `10`
- `after_hours` rows: `10`
- `pre_market` / `after_hours` date range: `2026-05-14` only

Trading-calendar note:

- The runtime Python had `pandas` installed but not `exchange_calendars`, so older child runs logged the weekday-only fallback warning.
- The calendar table is `trading_calendars`; current DB counts include `12,087` rows with `source=manual`.
- `utils/trading_calendar.py` now uses `trading_calendars` as a fallback when `exchange_calendars` is unavailable, before falling back to weekday-only rules.
- Verification: `python -m pytest tests/test_trading_calendar.py tests/test_update_massive_events.py tests/test_massive_shares.py -q` passed with `5 passed`.
| news_articles | 0 | 36 |

### 5. Historical shares / floats

Started at local time `2026-05-15 16:50 CST` as a resumable `tmux` job.

Reason for chunking:

- Massive shares full-refresh covers 9 snapshot dates from `2024-06-30` through `2026-05-14`.
- The US CS/ETF universe selected by `update_massive_shares --full-refresh --market US` has `10,849` symbols.
- That is roughly `97,641` ticker overview requests plus float batch requests, so it is not a short foreground job.
- The chunk runner calls the existing `update_massive_shares` command for 100 symbols at a time, so completed chunks are committed before moving on.

Command running inside tmux session `stock_massive_shares_20260515`:

```bash
rtk /usr/bin/lockf -t 0 /tmp/stock_daily_run.lock rtk python scripts/run_massive_shares_chunks.py --market US --chunk-size 100 > logs/massive_shares_chunks_2026-05-15.out 2>&1
```

Monitor commands:

```bash
rtk tmux ls
rtk tail -n 40 logs/massive_shares_chunks_2026-05-15.jsonl
rtk tail -n 80 logs/massive_shares_chunks_2026-05-15.out
```

Initial progress:

- `symbol_count`: `10,849`
- `chunk_count`: `109`
- first chunk started: `0001:a:aciw`

Checkpoint after first chunk:

- first chunk completed at UTC `2026-05-15T09:00:03+00:00` / local `2026-05-15 17:00 CST`
- first chunk elapsed: `543.651` seconds
- first chunk summary:
  - success: `99`
  - no data: `1`
  - errors: `0`
  - total_shares rows: `693`
  - historical_floats rows: `54`
  - float_shares matched rows: `431`
- second chunk started: `0002:acky:afbi`

Source counts after first chunk:

| table | MASSIVE rows | POLYGON rows |
| --- | ---: | ---: |
| historical_shares | 693 | 10,148 |
| historical_floats | 54 | 9 |
| security_symbol_history | 11,088 | 11 |

Checkpoint at local `2026-05-15 19:30 CST`:

- User resumed after commute; the long job was still present but network instability had caused symbol-level Massive request failures.
- Found a resumability bug: `scripts/update_massive_shares.py` counted `ERROR` / `FATAL_ERROR` but still returned exit code `0`, and `main.py` swallowed non-zero `SystemExit` from called scripts.
- Stopped the running chunk `0006:aotg:arkz`; the wrapper wrote `returncode=143`, so it is not considered complete.
- The previous chunk `0005:ambc:aosl` had been marked `returncode=0` by the old logic even though its log showed a fatal float-batch network error. Added a `chunk_invalidated` progress event for `0005:ambc:aosl`.
- Updated `scripts/run_massive_shares_chunks.py` so completed chunks are determined by each chunk's latest status and `chunk_invalidated` makes a chunk eligible for rerun.
- Updated `scripts/update_massive_shares.py` so any symbol/task error returns non-zero.
- Updated `main.py` so a non-zero child script exit is propagated to the outer process.
- Updated `data_sources/massive_source.py` to redact embedded `apiKey=` values from network exception text before logging/raising.
- Restarted tmux session `stock_massive_shares_20260515`; it skipped chunks `0001` through `0004` and restarted `0005:ambc:aosl`.
- Verification:
  - `python -m pytest tests/test_massive_source.py tests/test_massive_shares.py tests/test_main_execute_script.py tests/test_run_massive_shares_chunks.py tests/test_trading_calendar.py tests/test_update_massive_events.py -q` -> `23 passed`
  - `python -m compileall main.py scripts/update_massive_shares.py scripts/run_massive_shares_chunks.py data_sources/massive_source.py utils/trading_calendar.py tests/test_massive_shares.py tests/test_main_execute_script.py tests/test_run_massive_shares_chunks.py tests/test_massive_source.py tests/test_trading_calendar.py` -> exit `0`

Checkpoint at local `2026-05-15 19:36 CST`:

- Rerun of invalidated chunk `0005:ambc:aosl` completed successfully.
- Chunk `0005:ambc:aosl` summary:
  - success: `100`
  - no data: `0`
  - errors: `0`
  - total_shares rows: `770`
  - historical_floats rows: `68`
  - float_shares matched rows: `561`
  - returncode: `0`
- Current running chunk: `0006:aotg:arkz`
- Completed chunks by latest progress status: `5 / 109`
- Current source counts:
  - `historical_shares`: `MASSIVE=3,653`, `POLYGON=10,148`
  - `historical_floats`: `MASSIVE=318`, `POLYGON=9`
