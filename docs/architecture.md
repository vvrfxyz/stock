# Architecture

本项目按 Greenfield raw truth 原则设计：PostgreSQL 负责证券身份、元数据、事件事实和当前日线事实。全系统以 `security_id` 作为 durable identity，`symbol` 只是当前代码或历史代码属性。（ClickHouse 已于 2026-07 随分钟级数据回归：`stock.minute_bars` 50.6 亿行分钟线，2003-09 起，归档+周度增量双源；设计见 `archive/polyglot_persistence_architecture.md`、工程记录见 `minute_vw_backfill_2026-07.md`。）

## 数据源与职责边界

- Massive
  - `securities`：Universe / 证券详情。
  - `security_symbol_history`：ticker events / symbol history。
  - `corporate_actions`：分红、拆股事件事实。
  - `daily_prices`：日线 OHLCV、VWAP、trade_count、盘前/盘后价格。
  - `historical_shares`：total shares / float shares 的点时事实。
  - `historical_floats`：free float effective-date 事实。
  - `short_interests` / `short_volumes`：做空相关日线/结算日事实。
  - `news_articles` / `news_article_insights`：新闻和 ticker sentiment metadata。

- Official/reference sources
  - `exchanges`：以 ISO 10383 MIC 为交易所身份锚点，Massive exchange reference 可作为补充字段。
  - `trading_calendars`：以交易所官方交易日历为 truth；Massive market status/upcoming holidays 只作为运行时检查或补充监控。
  - `sec_filings`：SEC EDGAR filing index metadata，Massive filings index 可作为结构化补充和对账来源。
  - `insider_transactions`：Form 3/4/5 ownership transaction 明细，优先保留 accession 可追溯性。
  - `institutional_holdings`：13-F holdings 明细，使用 CUSIP/issuer/title 映射到 `security_id`，映射不确定时允许 `security_id` 为空。
  - `security_identifiers`：CUSIP/CIK/FIGI/ISIN 等证券标识符的点时映射层。

关键原则：事实表只写数据源或交易事实直接提供的字段。派生值不进入事实表，包括复权价格、换手率、成交额和技术指标。复权因子可以保存，但必须单独分层：供应商因子是 reference，内部因子是可重建 cache，不是 source-of-truth。

## 存储边界

- PostgreSQL 是元数据、事件和日线 raw fact 的事务中心，也是当前唯一存储。
- 字段类型保持列式存储友好（`BIGINT`、`DATE`、`TIMESTAMPTZ`、`NUMERIC(P,S)`），未来重建列式读取层时迁移成本低。
- 分钟级数据尚未落地；未来新增分钟线时，沿用 `security_id + timestamp + OHLCV + VWAP + trade_count + source + ingested_at`。

## 核心表语义

- `securities`
  - `id` 是全局证券身份，所有事实表都引用它。
  - `current_symbol` 是当前最新代码，不作为历史回测的 durable key。
  - `price_data_latest_date` 与 `daily_prices.max(date)` 对齐，用于增量调度。

- `security_symbol_history`
  - 记录代码变更历史，用于避免 symbol 变更带来的幸存者偏差和未来函数。

- `daily_prices`
  - 保存 PostgreSQL 侧 raw 日线事实。
  - 字段只包含 `open/high/low/close/volume/vwap/trade_count/pre_market/after_hours`。
  - 不保存 `adj_factor`、`split_adj_factor`、`turnover_rate`、`turnover` 或技术指标。

- `vendor_adjustment_factors`
  - 保存供应商提供或可直接从供应商 adjusted/raw 响应得到的复权 reference。
  - Massive 分红/拆股的 `historical_adjustment_factor` 按 `factor_key = dividend:<source_event_id>` 或 `split:<source_event_id>` 落库，便于和内部计算精确对账。
  - 该表不参与 truth 判定；供应商修正时可以覆盖为新的 snapshot。

- `computed_adjustment_factors`
  - 保存内部由 `corporate_actions + daily_prices` 计算出的复权因子 cache。
  - 当前 `raw_actions_v1` 口径：拆股因子为 `split_from / split_to`；现金分红因子为 `(前一交易日 raw close - cash_amount) / 前一交易日 raw close`。
  - `cumulative_factor` 表示“该事件及之后所有事件”的累计因子，用于对齐 Massive `historical_adjustment_factor`。
  - `methodology_version` 和 `event_hash` 用来保证可复现；事件修正后按证券重建即可。

- `corporate_actions`
  - 保存分红和拆股的事件真相。
  - `ex_date` 是回测和复权计算的核心日期。
  - `source_event_id` 是供应商事件身份，允许同一 `ex_date` 有多笔不同分红事件。

- `historical_shares`
  - 保存股本点时事实。
  - `filing_date` 表示这条股本事实何时可被使用，是防未来函数边界。
  - `period_end_date` 表示这条事实归属的报告/快照周期。
  - `total_shares` 必填，`float_shares` 可选。

- `historical_floats`
  - 保存 Massive float endpoint 返回的 effective-date free float。
  - 与 `historical_shares` 一起作为未来换手率/流通盘分析输入。

- `short_interests`
  - 保存 settlement-date short interest、days to cover 和平均成交量。
  - 适合做拥挤度、空头压力和 squeeze 风险特征。

- `short_volumes`
  - 保存每日 short volume、total volume 和交易场所分解。
  - `short_volume_ratio` 来自供应商字段或可审计映射，不作为本地 turnover 替代品。

- `news_articles` / `news_article_insights`
  - 保存 Massive 新闻 metadata 和 ticker sentiment insights。
  - 用于未来事件驱动、情绪因子和风控过滤。

- `exchanges`
  - 保存交易所/MIC 参考数据。
  - `mic` 是 ISO 10383 市场代码，例如 `XNAS`、`XNYS`。
  - Massive `primary_exchange` 和 SEC/交易所官方资料都应对齐到这个字段。

- `trading_calendars`
  - 主键为 `(exchange_mic, trade_date)`。
  - 区域/资产市场属性从 `exchanges` 读取，避免在 calendar facts 上重复存储。
  - `is_open`、`is_half_day`、`open_at`、`close_at` 和 `timezone` 描述当日 session。
  - 官方交易所日历是 truth；Massive `marketstatus/upcoming` 不适合做完整历史日历。

- `security_identifiers`
  - 保存 `security_id` 与 CUSIP/CIK/FIGI/ISIN 等标识符的点时映射。
  - 这是 13-F、SEC filing 和多供应商数据对齐的基础。

- `sec_filings`
  - 保存 SEC filing index metadata，不保存全文。
  - `accession_number` 是 filing 的稳定锚点。
  - 回测特征必须使用 `accepted_at` 或 `available_at` 作为点时可用边界，不能用报告期结束日偷看未来。

- `insider_transactions`
  - 保存 Form 3/4/5 结构化交易明细。
  - 对策略最有价值的是公开市场买卖；RSU 授予、期权行权、赠与等必须按 `transaction_code` 分层处理。

- `institutional_holdings`
  - 保存 13-F 机构持仓明细。
  - 13-F 的用途是研究机构持仓、拥挤度、季度调仓、资金偏好和大持有人变化。
  - 披露有延迟，并且以 CUSIP/issuer/title 为主，不保证能直接映射到当前 ticker。

## SEC 接入策略

SEC 底层 schema 一次打好，包括 filing index、identifier mapping、Form 3/4/5 内部人交易和 13-F 持仓明细。采集执行仍然分步骤接入：先 filings index，再 Form 4，再 13-F。原因不是 schema 做不到一次性，而是三类数据的质量风险不同：

- filing index 以 CIK/accession 为锚点，最稳定，适合先作为总目录。
- Form 4 是逐交易行数据，同一个 filing 里可能有授予、行权、卖出、赠与等多种语义，必须按 `transaction_code` 分层后才能做策略特征。
- 13-F 以 CUSIP/issuer/class 为主，不直接给 `security_id`；映射层没校验前，允许先落原始 holdings，后续再补 `security_id`。

因此“底层结构一次完成”和“生产采集分阶段验证”并不冲突：前者保证未来不用反复改主 schema，后者避免把未验证的 vendor 结构化结果直接变成策略 truth。

## 自动更新工作流

推荐 cron 入口：

```bash
python main.py scheduled_update --market US
```

`scheduled_update` 顺序执行每日增量（universe 同步、日线、short data、近期 corporate actions、复权因子增量、盘前/盘后、完整性检查），并把 shares/grouped daily、SEC 全家桶/汇率/利率/身份审计、events/details 按周六/周日/每月错峰。**权威 step list 以根 `README.md` 的「常用命令」小节和 `main.py` 的 `build_scheduled_update_steps()` 为准**，此处不再重复维护。

日更路径只跑增量；各单项采集脚本的 `--force` / `--full-refresh` 保留为手动全量回补入口。

轻量调试入口：

```bash
python main.py update --market US
python main.py update AAPL
```

常用单项维护命令：

```bash
python main.py update_massive_shares AAPL --full-refresh
python main.py update_massive_events META --force
python main.py update_massive_short_data TSLA --force
python main.py update_massive_news TSLA --force --lookback-days 7
python main.py update_adjustment_factors AAPL
```

## 派生值策略

- 复权价格：回测/分析启动时读取 `corporate_actions` 或 `computed_adjustment_factors` cache，在内存中动态生成。
- 复权因子：`corporate_actions` 是 truth；`vendor_adjustment_factors` 是 reference；`computed_adjustment_factors` 是 reproducible cache。
- 换手率：回测/分析读取 `historical_shares` 和 `daily_prices.volume` 后动态计算。
- 技术指标：从 raw/canonical bar 在计算层生成；如需缓存，必须明确标注为 cache，而不是事实表。
  Massive 提供 SMA/EMA/MACD/RSI endpoint，但项目不把这些数值写入事实表。后续可以写一个校验小工具：同一 ticker/date/window/series_type 下，用本地 raw/canonical bars 手动计算指标，并和 Massive 返回值对账，用于发现复权口径、窗口边界或数据缺口问题。
- 成交额：如果供应商没有原始返回，不通过 `volume * vwap` 写回事实表。
- 财报/ratios：`sec_fundamental_facts` 保存 SEC XBRL 原始申报值（`utils/sec_concepts.py` 白名单，`filed_date` 是点时可见边界）；财务比率仍是读取层计算，不写回事实表，也不恢复模糊的 `financial_reports` 表。

## 数据一致性与故障恢复

- 日线价格更新到最近一个已收盘交易日，避免盘中数据污染。
- `scripts/check_data_integrity.py` 做只读一致性检查。
- `scripts/calibrate_price_latest_date.py` 可用 `daily_prices` 实际数据回算 `securities.price_data_latest_date`。
- 对于数据源无返回的情况，不推进 `price_data_latest_date`，避免掩盖缺口。
