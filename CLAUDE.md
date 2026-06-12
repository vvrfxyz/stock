# CLAUDE.md

This file gives Claude Code a compact, current map of the repository.

## Project Overview

This is a Greenfield US stock data pipeline. PostgreSQL is the system of record for metadata, events, and daily raw facts. (ClickHouse as a matrix-read layer was removed in 2026-06; it may return once minute-level data arrives — see `docs/archive/polyglot_persistence_architecture.md`.)

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
- `db_manager/`: Shared session, upsert, cleanup, and batch-write utilities, split by domain (`core/securities/corporate_actions/market_data/reference_data`); import stays `from db_manager import DatabaseManager`.
- `data_sources/massive_source.py`: Massive REST adapter.
- `utils/massive_task.py`: Shared skeleton for `update_massive_*` scripts (standard parser, runtime setup/teardown, security selection, thread-pool runner).
- `utils/massive_config.py`: Massive keys, supported types, history-window helpers.
- `utils/key_rate_limiter.py`: Per-key, process-shared Massive rate limiter.
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
- `sec_fundamental_facts`: Curated XBRL facts (`utils/sec_concepts.py` whitelist); `filed_date` is the point-in-time visibility boundary, all restatements kept.
- `insider_transactions`: Form 3/4/5 ownership transaction rows (one row per entry × reporting owner; layer by `transaction_code` before building features).
- `institutional_holdings`: 13-F holdings rows (filer-CIK anchored, discovered via EDGAR form index — not `sec_filings`; `security_id` stays NULL until a CUSIP mapping exists; `value` stored as reported — thousands of USD before 2023-01, USD after).
- `fx_rates`: ECB daily EUR-based reference rates; USD cross rates are computed at read time via `utils/fx_rates.UsdFxConverter`.

Financial ratios remain read-time computations — never store derived ratios back into fact tables. `sec_fundamental_facts` stores raw reported XBRL values only; do not revive `financial_reports` as a vague catch-all table.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic upgrade head
```

Required configuration:

- `DATABASE_URL` in `.env`.
- Massive API keys in `activation_value.txt`, one key per line.
- `SEC_USER_AGENT` in `.env` for SEC EDGAR scripts (self-identifying UA required by SEC).

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

python main.py sync_sec_identifiers                 # SEC ticker->CIK 映射
python main.py update_sec_filings aapl              # SEC filing 索引；--all 全市场约 18 分钟
python main.py update_sec_fundamentals aapl         # XBRL 基本面；--all --since 增量 / --bulk-zip 全量回填
python main.py update_insider_transactions aapl     # Form 3/4/5 明细；--all 处理全部待解析 filing
python main.py update_institutional_holdings --since 2026-06-01   # 13F 持仓；--quarter 2026Q1 季度回填
python main.py update_fx_rates                      # ECB 参考汇率；非 USD 分红折算依赖

python scripts/check_data_integrity.py --limit 5
python scripts/audit_recent_data.py --sample-size 32   # vendor 对账抽样审计（耗 API 配额）
```

Read adjusted prices via `utils/adjusted_prices.get_adjusted_daily_bars(session, symbol_or_id, start=, end=, as_of=)` — never store adjusted values back into fact tables.

## Testing

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q                       # 全量（含 PG 集成测试）
python -m pytest tests/ -q -m "not integration"  # 仅纯单元测试
```

- `-m integration` 标记的用例需要 PostgreSQL：优先使用 `TEST_DATABASE_URL`（指向**可丢弃**的库，会 create_all + TRUNCATE）；未设置时 conftest 用本地 postgres 二进制在 /tmp 起一次性集群（仅 unix socket），结束即销毁；两者都没有则自动 skip。conftest 有保险丝：`TEST_DATABASE_URL` 的库名必须含 `test`，否则拒绝运行。
- 专用测试库 `stock_test`（owner `stock_test`，已 `REVOKE CONNECT ON DATABASE stock FROM PUBLIC`，该账号物理上连不进生产库）建在 253 的 stock-postgres 容器里；两端 `.env` 都有 `TEST_DATABASE_URL`（本机默认注释掉，用 /tmp 一次性集群更快）。
- `tests/test_db_manager_pg.py` 锁定各 upsert 的冲突键/protected 字段/合成事件去重语义；改 `db_manager/` 任何写入路径都必须过这组测试。
- 脚本编排逻辑测试在 `tests/test_script_runs.py`（Mock source/db 直接调 `run(args, source, db)`），证券选择分支在 `tests/test_select_us_securities.py`（sqlite，只建 securities 表——全 metadata 含 sqlite 不支持的 ARRAY 列）。

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
- `computed_adjustment_factors.methodology_version` currently uses `raw_actions_v1`. Non-USD dividends (CAD/NOK/ILS cross-listings) are converted to USD via ECB rates from `fx_rates` when available — run `update_fx_rates` first, then a factor rebuild picks them up; without FX data the events are skipped as before. Vendor reconciliation excludes these events from the chain (vendor never emits factor rows for them).
- Turnover and adjusted prices are calculation outputs, not facts.
- `historical_shares.filing_date` is the point-in-time boundary: never attach floats whose `effective_date` is after it.
- Corporate-action incremental batches must fall back to the full Massive history floor when any security in the batch has no `actions_last_updated_at`.

## Code Conventions

- loguru silently ignores `exc_info=True`; log tracebacks with `logger.opt(exception=e).error(...)`.
- Script logging goes through `utils/script_logging.setup_logging(name)`; the first caller in a process becomes the primary log, later names get swappable per-script file sinks — do not redefine sinks per script.
- Script entrypoints are `main(argv: list[str] | None = None)` parsing via `parse_args(argv)` and returning an int exit code (0/1); `__main__` uses `raise SystemExit(main())`. `main.py` invokes scripts in-process by passing the argv list — never mutate `sys.argv`. `scheduled_update` isolates step failures and exits non-zero if any step failed.
- `update_massive_*` scripts build on `utils/massive_task.py` (`run_massive_task` + `select_us_securities` + `run_concurrently`); keep per-script code to parser extras, selection filters, and `process_*` logic.
- Multi-row `pg_insert().values()` requires homogeneous dict keys; group heterogeneous rows with `_group_rows_by_key_set` (see `upsert_securities_by_symbol`).
