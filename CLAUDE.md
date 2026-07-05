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
- `utils/security_identity.py`: Centralized `SecurityIdentityResolver` — FIGI→CIK→symbol→history priority chain for identity resolution; `sync_massive_universe` uses it for rename/recycle detection.
- `utils/key_rate_limiter.py`: Per-key, process-shared Massive rate limiter.
- `alembic/versions/`: PostgreSQL migrations.
- `research/`: 离线研究层（只读，绝不回写事实表）——`data.py` 批量复权面板加载（与 `utils/adjusted_prices` 同口径，有一致性测试锁定），`fundamentals.py` 基本面 PIT 归一化（`sec_fundamental_facts` -> TTM/时点指标 as-of 面板：重述感知 vintage 口径——as-of t 取 filed_date <= t 的最新已申报值，TTM 三分量锁同一 concept、任一分量重述即发新事件，营收同义概念按标准营收族优先级在事件流层 coalesce（含银行/保险的 RevenuesNetOfInterestExpense 等行业概念），270 天新鲜度门槛置 NaN），`backtest.py` 向量化回测引擎（t 日权重赚 t+1 收益，换手计成本），`strategies.py` 技术基线，`run_baselines.py` 入口。连库优先 `RESEARCH_DATABASE_URL`（指向 253 生产库）。
  - 回测数据边界：`computed_adjustment_factors` 只覆盖 ex_date >= 2024-05-14（Massive 免费档 730 天窗口决定的因子可信下限），更早的"复权价"未真正复权，面板不得早于该日；窗口内有 SPLIT 但无因子行的证券（因子构建只跑 is_active=True，退市股有缺口）须用 `securities_with_uncovered_events` 整体剔除。
  - 因子库（`research/factors/`）：PIT 因子框架，详见 `docs/factors.md`。当前 9 个 builtin 因子——`size`（log 市值）、`earnings_yield`（盈利收益率）、`short_interest_ratio`（空头仓位占比）、`short_volume_ratio`（日做空成交占比）、`days_to_cover`（空头天数覆盖）、`institutional_breadth`（13F 持仓机构数）、`delta_institutional_ownership`（季度 IO 变化）、`ownership_concentration`（持仓 HHI）、`insider_net_buy`（内部人净买入）。新增因子只需在 `builtins/` 下写一个 `@dataclass(frozen=True)` 类 + `register()` 即可。评估用 `python -m research.evaluate --factors size --start 2024-05-14`。

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
- `institutional_holdings`: 13-F holdings rows (filer-CIK anchored, discovered via EDGAR form index — not `sec_filings`; `security_id` mapped via SEC fails-to-deliver CUSIP identifiers (`sync_cusip_identifiers`), NULL where unmapped; `value` stored as reported — thousands of USD before 2023-01, USD after).
- `openfigi_cusip_lookups`: OpenFIGI CUSIP→FIGI lookup cache backing the 13F unmapped-CUSIP fallback (`sync_openfigi_identifiers`); MATCHED rows are never re-queried, NOT_FOUND/AMBIGUOUS negative-cache rows refresh after `--refresh-days`.
- `fx_rates`: ECB daily EUR-based reference rates; USD cross rates are computed at read time via `utils/fx_rates.UsdFxConverter`.
- `risk_free_rates`: FRED risk-free reference rates (DTB3 stored as annual discount-basis percent); research metrics read them via `utils/risk_free_rates.load_risk_free_daily_returns`.

Financial ratios remain read-time computations — never store derived ratios back into fact tables. `sec_fundamental_facts` stores raw reported XBRL values only; do not revive `financial_reports` as a vague catch-all table.

- `security_identity_events`: 身份变更审计事件（RENAME / RECYCLE / MERGE / SPLIT_IDENTITY / QUARANTINE / NEW_LISTING / MANUAL，与 `data_models/models.py` 的 event_type 注释一致）。
- `pipeline_task_runs`: scheduled_update 每步执行记录（start/end/status/exit_code/stats）。

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
python main.py sync_delisted_universe --dry-run     # Massive 退市 CS/ETF 名单补齐（20 年 universe）；富化阶段每只 1 请求回填 list_date，--skip-enrich 可跳过
python main.py update_massive_details AAPL --force
python main.py update_massive_actions AAPL --force
python main.py update_massive_prices AAPL --full-refresh
python main.py update_massive_shares AAPL --full-refresh
python main.py update_massive_events META --force
python main.py update_massive_short_data TSLA --force
python main.py update_massive_news TSLA --force --lookback-days 7
python main.py update_adjustment_factors AAPL

python main.py sync_sec_identifiers                 # SEC ticker->CIK 映射
python main.py sync_cusip_identifiers --months 12   # FTD CUSIP 映射 + 回填 13F security_id
python main.py sync_openfigi_identifiers --limit 500  # OpenFIGI 兜底补链 13F 未映射 CUSIP；可选 OPENFIGI_API_KEY 环境变量提速
python main.py update_sec_filings aapl              # SEC filing 索引；--all 全市场约 18 分钟
python main.py update_sec_fundamentals aapl         # XBRL 基本面；--all --since 增量 / --bulk-zip 全量回填
python main.py update_insider_transactions aapl     # Form 3/4/5 明细；--all 处理全部待解析 filing
python main.py update_institutional_holdings --since 2026-06-01   # 13F 持仓；--quarter 2026Q1 季度回填
python scripts/backfill_13f_quarters.py --oldest 2013Q2           # 13F 历史回填编排器（新→旧逐季、幂等续传、磁盘/daily-run 护栏；台账 logs/manual_backfill/13f_backfill_ledger.tsv）；2013Q2 前无 XML 信息表
python main.py update_fx_rates                      # ECB 参考汇率；非 USD 分红折算依赖
python main.py update_risk_free_rates               # FRED DTB3 无风险利率；评估层超额收益依赖

python scripts/check_data_integrity.py --limit 5
python scripts/audit_recent_data.py --sample-size 32   # vendor 对账抽样审计（耗 API 配额）
python main.py health_report                           # 数据域健康报告

python scripts/audit_security_identity.py              # 身份对账（只读）
python scripts/repair_identity.py --dry-run            # 存量身份修复 plan
python scripts/repair_identity.py --apply              # 执行修复（先确认 dry-run）
```

Read adjusted prices via `utils/adjusted_prices.get_adjusted_daily_bars(session, symbol_or_id, start=, end=, as_of=)` — never store adjusted values back into fact tables.

Key documentation: `docs/identity_lifecycle.md`（身份解析器原理 / rename-recycle 流程 / 存量修复）, `docs/data_quality_runbook.md`（health_report 解读 / 退出码 / 故障排查）, `docs/day_aggs_backfill_2026-07.md`（20 年日线回填工程记录：数据来源边界口径 / 退市 universe 补齐 / vendor 时点查询差一天陷阱 / 无主数据定案）。

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

- `daily_prices` 有三个来源，靠字段指纹区分（详见 `docs/day_aggs_backfill_2026-07.md`）：flat files (SIP) = vwap NULL + trade_count 有（2003-09-10 ~ 2023 年底主体）；Massive = vwap 有（2024 起）；yfinance 遗留 = 双 NULL（2003 前深历史 + OTC 填缝，读取层可按指纹过滤）。退市证券 universe 已补齐（含 2003 年以来 7,500 只退市 CS/ETF）。
- Price scripts should update only through the most recent completed trading session.
- Massive is symbol-keyed: history fetched for a recycled ticker belongs to whichever entity held the symbol then. Price/actions/short-data backfills are therefore clamped to `securities.list_date`, `sync_massive_universe` writes a `DEAD_TICKER_RECYCLE` RECYCLE event when a NEW listing reuses an inactive security's symbol, and `check_data_integrity` blocks on same-symbol securities with overlapping daily-price spans (2026-07 gogl/lazr/pinc/spcx/opi/fusd incident).
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

## Sub-repo / Explore / Team safety rule

当当前 cwd 不在 git 仓库内，或准备使用 Explore、Agent、team /task 工作流时，必须先校验运行前提，禁止在已知前提不满足时重复重试。

规则：

- 如果当前目录不是 git 仓库，不要直接重复调用需要 worktree isolation 的 Explore / Agent。
- 先定位目标项目目录，并检查该目录是否为 git 仓库。
- 如果目标子目录是 git 仓库：
  - 不要假设可以在父目录直接稳定使用 worktree isolation。
  - 如果必须继续使用子代理，直接切到那个目标子仓库目录后再继续。
  - 在无法切换到目标目录前，只允许使用 Glob / Grep / Read 做只读调研。
- 同一个前提错误如果已经出现一次，不得继续用同样方式重试。
- 包括但不限于：
  - `Cannot create agent worktree: not in a git repository and no WorktreeCreate hooks are configured`
  - `Team "team" does not exist. Call spawnTeam first to create the team.`
- 在使用 team /task/ Explore 的 team 模式前，必须先确认目标 team 当前存在。
- 不要假设名为 team 的默认 team 一定存在。
- 如果 team 不存在：
  - 不要继续用同一个 team 名重试。
  - 如果任务只是普通探索，直接退回非 team 模式。
  - 只有在确实需要协作拆分任务时，才重新创建 team。
- 上下文压缩后，如果无法确认 team 是否仍存在，默认按 “不存在” 处理，先不要使用 team 模式。
- 对于普通代码探索，优先级如下：
  1. 明确搜索：Grep / Glob / Read
  2. 开放式探索：非 team 的 Explore
  3. 只有在确实需要多人协作或任务拆分时，才使用 team
