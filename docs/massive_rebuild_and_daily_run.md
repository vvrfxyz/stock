# Massive-only 重建与每日运行

本文档描述当前推荐的 Massive-only 运行方式。当前系统按 Greenfield raw truth 原则落地：只把 Massive 直接提供或可审计映射的事实写入 PostgreSQL，不把复权价格、换手率、成交额或技术指标写成事实表。复权因子单独分层保存：供应商因子是 reference，内部计算因子是 cache。

- 核验日期：2026-05-14
- 适用前提：使用当前 `activation_value.txt` 中 Massive keys
- 重要限制：全量重建只能覆盖 Massive 免费层可访问的最近 `2 年` 窗口
- 当前保留证券类型：`CS` 和 `ETF`
- 当前排除范围：financial statements、ratios、实时 trades/quotes、snapshots、非 CS/ETF

## 当前默认数据模型

### `daily_prices`

核心字段：

- `security_id`
- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `vwap`
- `trade_count`
- `pre_market`
- `after_hours`

说明：

- `open/high/low/close/volume/vwap/trade_count` 来自 Massive aggregates 或 grouped daily。
- `pre_market` / `after_hours` 来自 Massive `Daily Ticker Summary`。
- 不保存 `turnover`、`turnover_rate`、`split_adj_factor`、`adj_factor` 或技术指标。

### `vendor_adjustment_factors` / `computed_adjustment_factors`

说明：

- `vendor_adjustment_factors` 保存 Massive `historical_adjustment_factor` 和可选 adjusted/raw close 比值，用于对账，不作为 truth。
- `computed_adjustment_factors` 保存内部由 `corporate_actions + daily_prices` 重建的累计复权因子 cache。
- `methodology_version` 和 `event_hash` 用来标记计算口径；公司行动被修正后按证券重建即可。

### `corporate_actions`

核心字段：

- `security_id`
- `action_type`
- `ex_date`
- `pay_date`
- `cash_amount`
- `currency`
- `split_to`
- `split_from`
- `source`
- `source_event_id`

说明：

- `DIVIDEND` 使用 `ex_date`、`cash_amount`、`currency`。
- `SPLIT` 使用 `ex_date`、`split_from`、`split_to`，Massive 的 `execution_date` 在入库时映射为 `ex_date`。
- `source_event_id` 是供应商事件身份；缺失时生成稳定 synthetic id。

### `historical_shares`

核心字段：

- `security_id`
- `filing_date`
- `period_end_date`
- `total_shares`
- `float_shares`
- `source`

说明：

- `filing_date` 是点时可用性边界，防止回测看到未来披露数据。
- `period_end_date` 是该股本事实归属的报告期或快照期。
- `total_shares` 必填，`float_shares` 可选。

### 其他低频 raw facts

- `security_symbol_history`：Massive ticker events / symbol history。
- `historical_floats`：Massive free float by effective date。
- `short_interests`：settlement-date short interest。
- `short_volumes`：daily short volume。
- `news_articles` / `news_article_insights`：news metadata 和 ticker sentiment insights。
- `exchanges` / `trading_calendars`：交易所 MIC 参考数据和交易所级交易日历。`trading_calendars` 保存逐交易所逐日期 session，不能被 `exchanges` 取代。官方交易所日历是 truth；Massive market status/upcoming holidays 只用于补充检查。
- `sec_filings` / `insider_transactions` / `institutional_holdings` / `security_identifiers`：SEC filing 底层结构。schema 一次打底，采集按数据质量和映射风险逐项接入。

SEC 的 schema 会一次打底，但采集不强行一次进入 daily run。filing index 最稳定；Form 4 需要按交易代码区分公开买卖、授予、行权、赠与；13-F 还依赖 CUSIP/issuer/class 到 `security_id` 的映射质量。分步接入是为了质量审计，不是底层架构留缺口。

## 初始化 / 升级 schema

```bash
alembic upgrade head
```

## Massive 免费层窗口内的全量重建

```bash
python main.py rebuild_massive_dataset --market US
```

该命令会顺序执行：

1. `sync_massive_universe`
2. `update_massive_details --all --force`
3. `update_massive_actions --all --force`
4. `update_massive_prices --full-refresh`
5. `update_grouped_daily`（最近 5 个交易日）
6. `update_massive_shares --all --full-refresh`
7. `update_adjustment_factors --all`（重建后刷新复权因子 cache，避免读取层过期）
8. `check_data_integrity --window-days 730`

vendor 复权因子 reference 的刷新仍可按需单独运行：

```bash
python main.py update_adjustment_factors AAPL
python main.py update_adjustment_factors AAPL --refresh-vendor-daily-bars
```

如果你还想补最近交易日的盘前/盘后价格：

```bash
python main.py rebuild_massive_dataset --market US --with-open-close-summary
```

这一步会显著增加耗时，因为 `Daily Ticker Summary` 需要逐 symbol / 逐 date 请求。

## 每日运行方案

推荐 cron 默认命令：

```bash
python main.py scheduled_update --market US
```

默认步骤按每天 / 每周六 / 每周日 / 每月错峰执行。**权威 step list 以根 `README.md` 的「常用命令」小节和 `main.py` 的 `build_scheduled_update_steps()` 为准**，此处只概括口径：

- 每天跑增量：universe 同步、缺失日线、short data、近 14 天 corporate actions、复权因子增量、当日盘前/盘后、完整性检查。
- 每周六：shares / float 与最近 5 个交易日 grouped daily、盘前/盘后 5 日补漏。
- 每周日：分红拆股全量 + 复权因子全量重建、ECB 汇率、FRED 利率、SEC identifiers/filings/fundamentals/insider/CUSIP/13F 增量、身份审计。
- 每月第一个周二 / 周三：ticker events / 证券详情全量刷新。

全量脚本仍保留：`update_massive_short_data --force`、`update_massive_prices --full-refresh`
和其他单项命令的 `--force` 都可以手动执行。默认日更不使用这些全量参数，避免在已有数据后再次拉完整 2 年窗口。

这些任务在一个 Python 进程中顺序执行，`KeyRateLimiter(scope="massive")` 会让 20 个 key 的使用历史跨步骤共享。

轻量手动调试入口：

```bash
python main.py update --market US
```

`update` 只跑详情、公司行动、日线三步，主要用于手动调试或快速补缺。

可选强制刷新最近 2 年价格窗口：

```bash
python main.py update --market US --full-refresh-prices
```

## 单项维护命令

这些命令可以单独调试；cron 日常入口已经由 `scheduled_update` 统一编排。

```bash
python main.py update_massive_shares AAPL --full-refresh
python main.py update_massive_events META --force
python main.py update_massive_short_data TSLA --force
python main.py update_massive_news TSLA --force --lookback-days 7
python main.py update_adjustment_factors AAPL
```

`news` 暂不进入默认 cron，属于策略特征层，建议在具体策略或回测批次需要时单独运行。`adjustment_factors` 已进入默认调度（每天 `--changed-since 3` 增量 + 每周日 `--all` 全量重建），这里的单项命令只用于按证券调试或对账。

## 计算口径

- 前复权/后复权/total return：由回测或分析层读取 `corporate_actions` 或 `computed_adjustment_factors` cache 后动态计算。
- 供应商复权因子：保存在 `vendor_adjustment_factors`，只用于 reference / 对账。
- 换手率：由回测或分析层读取 `historical_shares` 与 `daily_prices.volume` 后动态计算。
- 技术指标：由计算层基于 raw/canonical bar 生成，不作为事实表入库。
  Massive 提供 SMA/EMA/MACD/RSI，可作为后续校验工具的 vendor reference；不写入事实表。
- 成交额：如果供应商没有原始提供，不通过 `volume * vwap` 写入事实表。

## 速率限制

Massive 免费层的 `5 API Calls / Minute` 是按 per API key 计数的，并且服务端会严格按真实时间窗口限流。

本项目通过 `KeyRateLimiter(scope="massive")` 在单进程内跨步骤共享 request history，并在 429 时临时拉黑触发 429 的 key，从而避免在 `main.py` 里写死 `sleep()`。

当前 Debian 部署使用 systemd timer 执行每日任务。配置文件保存在 `deploy/systemd/`，
默认每天 UTC+8 `10:00` 执行：

```bash
sudo install -m 0644 deploy/systemd/stock-daily-run.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/stock-daily-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now stock-daily-run.timer
```

## 现实约束

- 全市场每天回填 `pre_market` / `after_hours` 约需要 9,000-10,000 次请求；20 个 key 在当前限流下理论耗时约 100 分钟起。
- Massive 免费层无法从零重建 2 年窗口之外的历史价格 / 公司行动 / shares。
- 如果坚持 Massive-only，数据边界就应该以 Massive 免费层窗口为准。
