# CLAUDE.md

This file gives Claude Code a compact, current map of the repository.

## Project Overview

This is a Greenfield US stock data pipeline. PostgreSQL is the system of record for metadata, events, and daily raw facts. ClickHouse（253 容器 `stock-clickhouse`）自 2026-07 起承载分钟线：`stock.minute_bars` 50.6 亿行（2003-09 起，归档 `flatfiles_1m` + 周度增量 `massive_1m`），读取走 `research/minute_bars.py`（HTTP 8123）；设计见 `docs/archive/polyglot_persistence_architecture.md` 与 `docs/minute_vw_backfill_2026-07.md`。

**Owner 资金现实（2026-07-07）**：实盘本金 **2 万美元**，散户不是机构。一切研究的变现/部署判定按散户口径：容量与市场冲击不是约束（每仓数百美元）；真实成本 = 价差一半（零佣金，小盘往返 20-80bps 压测）；约束是集中度（只能持 20-40 只，分位组合转化打折）与信号厚度本身。"容量太小"不构成否决理由，小盘栖息地是优势；机构口径结论须标注并按散户参数复审（模板 `research/retail_reality_study.py`）。

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
- `db_manager/`: Shared session, upsert, cleanup, and batch-write utilities, split by domain (`core/securities/corporate_actions/market_data/reference_data`); import stays `from db_manager import DatabaseManager`. 拆表阶段 1a（2026-07）：脚本对 `securities` 的直写已收口进 API——摘牌走 `deactivate_missing_securities`、身份补空走 `enrich_security_identity`（NULL-only COALESCE，8 列白名单）、水位重算走 `recalculate_price_latest_dates`；豁免清单：`repair_identity`（阶段 2）、`build_companies` 挂接腿、`cleanup_unknown_figi`（修复工具）。新脚本不得再写裸 `UPDATE securities`。
- `data_sources/massive_source.py`: Massive REST adapter.
- `utils/massive_task.py`: Shared skeleton for `update_massive_*` scripts (standard parser, runtime setup/teardown, security selection, thread-pool runner).
- `utils/massive_config.py`: Massive keys, supported types, history-window helpers.
- `utils/security_identity.py`: Centralized `SecurityIdentityResolver` — FIGI→CIK→symbol→history priority chain for identity resolution; `sync_massive_universe` uses it for rename/recycle detection.
- `utils/key_rate_limiter.py`: Per-key, process-shared Massive rate limiter.
- `alembic/versions/`: PostgreSQL migrations.
- `research/`: 离线研究层（只读，绝不回写事实表）——`data.py` 批量复权面板加载（与 `utils/adjusted_prices` 同口径，有一致性测试锁定），`fundamentals.py` 基本面 PIT 归一化（`sec_fundamental_facts` -> TTM/时点指标 as-of 面板：重述感知 vintage 口径——as-of t 取 filed_date <= t 的最新已申报值，TTM 三分量锁同一 concept、任一分量重述即发新事件，营收同义概念按标准营收族优先级在事件流层 coalesce（含银行/保险的 RevenuesNetOfInterestExpense 等行业概念），270 天新鲜度门槛置 NaN），`backtest.py` 向量化回测引擎（t 日权重赚 t+1 收益，换手计成本），`strategies.py` 技术基线，`run_baselines.py` 入口。连库优先 `RESEARCH_DATABASE_URL`（指向 253 生产库）。
  - 回测数据边界：`computed_adjustment_factors` 覆盖 ex_date >= 2003-01-01（2026-07 corporate-actions 归档回填，见 `docs/corp_actions_archive_2026-07.md`；pre-2024-05-14 段无 vendor reference 可对账，靠价格跳变抽验兜底），面板下限 `FACTOR_TRUST_FLOOR = 2003-01-01`；存在无因子覆盖事件的证券（值冲突挂起、POLYGON legacy 孤行、归档缺漏、退市缺口）须用 `securities_with_uncovered_events` 整体剔除——该函数对非 MASSIVE 孤行（同日无 MASSIVE 对应行）也判为洞；默认 straddle_v2 口径只计"跨立"价格序列（min_date < ex_date <= max_date）的事件（事件前/后无价格则无假跳空），`require_straddle=False` 复现旧口径；2026-07-07 孤行裁决（398 删 / 8,440 allowlist 导入 / FX 补链）后剔除数 325。20 年长面板内存大，`run_baselines`/`evaluate` 默认窗口仍为 2024-05-14 起，长窗口显式传 `--start`。
  - 因子库（`research/factors/`）：PIT 因子框架，详见 `docs/factors.md`。当前 9 个 builtin 因子——`size`（log 市值）、`earnings_yield`（盈利收益率）、`short_interest_ratio`（空头仓位占比）、`short_volume_ratio`（日做空成交占比）、`days_to_cover`（空头天数覆盖）、`institutional_breadth`（13F 持仓机构数）、`delta_institutional_ownership`（季度 IO 变化）、`ownership_concentration`（持仓 HHI）、`insider_net_buy`（内部人净买入）。新增因子只需在 `builtins/` 下写一个 `@dataclass(frozen=True)` 类 + `register()` 即可。评估用 `python -m research.evaluate --factors size --start 2024-05-14`。
  - PIT 股本/市值（2026-07 起）：`research/shares.py` 从 `sec_fundamental_facts` 载入 XBRL 股本事件流（dei→us-gaap coalesce、value>0、270 天新鲜度锚 period_end、拆股前滚锚 period_end——XBRL 股本是申报口径不随拆股回溯），经 `stitch_shares_events` 缝在 vendor `historical_shares` 段之下（vendor 优先、MASSIVE>POLYGON）；`load_market_cap_panel(include_xbrl=True 默认)` 输出缝合后市值面板，size/earnings_yield 经此获得 2009+ 历史。`include_xbrl=False` 复现纯 vendor 旧口径；AAPL 2020 拆股金样本测试锁定接缝（`tests/test_shares_pit.py`）。多类股分摊：`earnings_yield` 自二期起在读取层做公司 join（分子取锚证券广播回成员、分母 = `research/company_market_cap.py` 公司级合并市值；无 company_id 或公司无 common-equity 成员时回退证券级旧口径）；common-equity 判别 `is_common_equity`（share_class_figi 结构化正证据 > 名称启发式）。
  - 退市收益口径：`run_baselines` 与 `evaluate` 都接 `delisting_events.delisting_return` 逐证券实测 Series（`--no-delisting-returns` 复现旧口径；`--terminal-return` 标量降级为未覆盖 fallback；ETF 清盘 par=0 只活在读取层 `load_delisting_returns(fund_closure_par=True)`）。evaluate 的口径进 params_hash，新旧口径 trial 不互相顶替。长窗口两个入口都必须显式传 `--eval-start`。
  - ADR opt-in（2026-07-07 §E.6）：宇宙默认 CS-only（`research/data.py` 的 `DEFAULT_RESEARCH_TYPES`），`evaluate`/`run_baselines` 传 `--include-adr` 并入 ADRC/ADRP/ADRR（`RESEARCH_TYPES_WITH_ADR`）。股本口径敏感因子（size/earnings_yield/short_interest_ratio 类上标 `adr_unsafe=True`）的 ADR 列在 evaluate 层自动置 NaN（ADS/公司股本混杂，禁入直至归一化；`diagnostics.adr_gated_columns` 记数）。`price_cache.load_price_long_fast` 在显式给 security_ids 时不再叠类型门（双重过滤修复）；`fundamentals.METRICS` 已含 ifrs-full 概念（coalesce 排 us-gaap 后，金额仍限 unit=USD——TWD 申报的 TSM 拿不到值属有意保守）。语义锁定测试 `tests/test_research_adr_optin.py`。
  - 长窗口性能：`load_adjusted_panel` 有进程内缓存（容量 2 窗口，命中返回同 DataFrame 对象——消费方只准 rebind、不准原地改写；测试换数据须 `clear_panel_cache()`）；评估路径只拉 close/volume 两列。

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
- `delisting_events`: 退市结局（reason_code/confidence、并购对价、final_price、实测 delisting_return；唯一键 security_id+delist_date；`upsert_delisting_events` 为全量重建语义——冲突时未提供字段会被置 NULL，绝不可用于局部更新）。分类器 `scripts/build_delisting_events.py`：`--apply` 带降级重建保险丝——存量表有对价/回报数据时，缺任一 `--fetch-*-docs` 旗标或网络阶段终端降级即拒绝写库（`--allow-degraded-rebuild` 显式豁免）；重建必须带全 `--fetch-form25-docs --fetch-8k-docs`。
- `companies`: 公司实体（PERMCO 等价物）；cik UNIQUE 可空、id 永不回收；`securities.company_id` FK 归组（第一期只做 CS，ETF 发行人 CIK ≠ 基金实体不强归）。NULL-cik 行会被 `upsert_companies` 拒绝。

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
python main.py update_sec_filings aapl              # SEC filing 索引；--all 全市场约 18 分钟；--include-inactive 含退市（Form 25/8-K 回拉）；items 列存 8-K item codes
python main.py update_sec_fundamentals aapl         # XBRL 基本面；--all --since 增量 / --bulk-zip 全量回填；--include-inactive 含退市 CIK
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

Key documentation: `docs/identity_lifecycle.md`（身份解析器原理 / rename-recycle 流程 / 存量修复）, `docs/data_quality_runbook.md`（health_report 解读 / 退出码 / 故障排查）, `docs/day_aggs_backfill_2026-07.md`（20 年日线回填工程记录：数据来源边界口径 / 退市 universe 补齐 / vendor 时点查询差一天陷阱 / 无主数据定案）, `docs/research_ledger.md`（研究总账：已裁决因子/假设、方法论教训、开放问题——**开新研究前必查，收尾必回写**）。

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

- `daily_prices` 有三个来源（详见 `docs/day_aggs_backfill_2026-07.md` 与 `docs/minute_vw_backfill_2026-07.md`）：flat files (SIP) = trade_count 有 + date < 2024（其 vwap 2026-07 起由 daily_vw 归档回填补齐，回填带同实体守卫 close 位级相等；另有 13.7 万行 pre-2023H2 的早期 Massive vwap 在 day-aggs 重导时被保留）；Massive = vwap 有（2023H2 起为主）；yfinance 遗留 = vwap/trade_count 双 NULL（2003 前深历史 + OTC 填缝，读取层可按双 NULL 指纹过滤，该指纹不受回填影响）。退市证券 universe 已补齐（含 2003 年以来 7,500 只退市 CS/ETF）。分钟线（2003+，含盘前盘后，未复权）在 ClickHouse `stock.minute_bars`，读取走 `research/minute_bars.py`；绝不用分钟加总回填日线 close/volume（收盘竞价与合并成交量在分钟条之外）。
- Price scripts should update only through the most recent completed trading session.
- Massive is symbol-keyed: history fetched for a recycled ticker belongs to whichever entity held the symbol then. Price/actions/short-data backfills are therefore clamped to `securities.list_date`, `sync_massive_universe` writes a `DEAD_TICKER_RECYCLE` RECYCLE event when a NEW listing reuses an inactive security's symbol, and `check_data_integrity` blocks on same-symbol securities with overlapping daily-price spans (2026-07 gogl/lazr/pinc/spcx/opi/fusd incident).
- `securities.price_data_latest_date` should match `daily_prices.max(date)`.
- Company actions are keyed by Massive `source_event_id`; synthetic historical IDs should be cleaned up when a real vendor event ID becomes available. 2026-07 起 MASSIVE 源事件覆盖 2003+（归档回填 `docs/corp_actions_archive_2026-07.md`）；剩余 POLYGON legacy 孤行是待裁决的复权洞（`securities_with_uncovered_events` 自动剔除其证券；2026-07-07 证据分桶裁决后残余 ~1,067 跨立行/323 只，人工队列见 `scripts/adjudicate_polygon_orphans.py` 产出）；vendor 的 `historical_adjustment_factor`/`split_adjusted_cash_amount` 在归档中已证实损坏（重复拆分行双重折算），归档数据永不读这两列。
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

### 研究/回测代码：只写高性能、安全的算法（owner 指令，2026-07-07）

- **禁止逐日/逐证券 Python 循环做面板计算**。横截面统计用向量化矩阵运算（rolling/
  一次 rank + numpy 视图/掩码代数），复用现成机器：`evaluate._masked_rowwise_corr`、
  `_quantile_weight_matrices`、`backtest._derived_from_prices`。逐行循环只允许出现在
  稀疏事件（如 insider 事件条）或 ~百量级再平衡行上。
- **面板装载只走 COPY + 进程内共享缓存**，绝不重复拉同一面板：因子层用
  `research/factors/price_cache.py`（v2：长表一次 COPY、逐列一次 pivot、buffer 量化
  {200,420} 档跨因子共享——v1 曾因缓存键含精确起始日被 8 因子各拉 8 遍，引以为戒）；
  评估层用 `data.load_adjusted_panel`（容量 2 记忆化）。read_sql 在 30M 行量级慢 5-10 倍，禁用。
- **性能改动必须带等价性金测试**（新旧实现在含 NaN/并列/停牌/退市尾巴的合成面板上
  1e-12 一致，参照 `tests/test_evaluate_fast_equivalence.py`），速度绝不以数字漂移换。
- **数值安全**：病理值置 NaN 剔除排名，绝不 clip 成极值信号（residual_vol 曾把负残差
  方差 clip(0) 排进最强分位——教训）；零方差/零量/零区间一律 NaN；除法全部走
  `np.errstate` + 显式掩码，不吞 warning。
- **数据安全**：research/ 只读，绝不回写事实表；PIT 边界（filing_date/lag_days）先于
  一切优化；预注册判据写死在脚本 docstring 再跑数（改动留痕），试验全部进 trials.parquet。
- 慢作业先 profile 定位（`/usr/bin/sample <pid>`）再改，禁止盲目重跑。
- **研究/评估作业一律在 253 上跑**（owner 指令 2026-07-07）：跨网冷拉 GB 级面板在 I/O 争抢下 10-15 分钟，本地 socket 几十秒。Mac 只写码/提交，跑数走 `ssh home-debian`（253 的 `.env` DATABASE_URL 即本地库）。
- **评测/研究脚本必须带进度**（owner 指令 2026-07-07）：统一走 `research/progress.Progress`（逐步耗时 + RSS 双读数 now/peak + ⚠MEM 水位 + FAILED 带异常 + done() top3 耗时，stderr 逐行 flush，tail -f 可读）——evaluate/price_cache/load_adjusted_panel/run_baselines/retail_reality 已内置，新脚本抄 progress.py 顶部模板；扩展前先读 docstring 的"非目标"清单。OOM 风险要在日志里看得见（253 峰值 6G 即危险区）；研究长任务一律 `scripts/run_research.sh <tag> -- <cmd>` 拉起（systemd-run 固定 unit 名 + MemoryMax=7G + OnFailure 通知，见 docs/deployment.md；nohup/setsid 会被会话清理击杀）。evaluate 断点续跑：`--skip-existing`（trial 全指纹命中即跳过 compute）。开研前查账：`python -m research.trials report --factor <name>`（Bonferroni 分母机器化）。批处理侧：`run_concurrently` 非 TTY 下自带 30s 节流进度行（吞吐/ETA/rss/rate-wait 配额占比）。

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
