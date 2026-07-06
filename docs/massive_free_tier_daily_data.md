# Massive 免费层日线能力

本文档记录当前项目已经用现有 Massive API keys 验证过、并纳入设计考虑的数据范围。

- 核验日期：2026-05-14
- 适用套餐：当前 `activation_value.txt` 中的 Massive keys
- 当前市场范围：US
- 当前保留证券类型：`CS` 和 `ETF`
- 当前排除范围：financial statements、ratios、实时 trades/quotes、snapshots、非 CS/ETF 证券

## 免费层约束

- 数据时效：End of Day。
- 历史窗口：当前按 Massive 免费层最近约 2 年窗口设计。
- 限流：每个 API key `5 calls / minute`。
- 多 key：项目使用 `KeyRateLimiter(scope="massive")` 做轮询、共享窗口和 429 临时 block。

## 已验证可用能力

### Ticker Universe / Types

主要用途：

- 构建 `securities` universe。
- 只保留 `CS` 和 `ETF`。
- 标记 active/inactive。

典型字段：

- `ticker`
- `name`
- `market`
- `locale`
- `primary_exchange`
- `type`
- `active`
- `currency_name`
- `cik`
- `composite_figi`
- `share_class_figi`

入库位置：

- `securities`

### Ticker Overview

主要用途：

- 更新证券详情、行业、描述、地址、员工数、市值、shares outstanding 等低频 metadata。

典型字段：

- `name`
- `description`
- `homepage_url`
- `market_cap`
- `sic_code`
- `sic_description`
- `total_employees`
- `list_date`
- `branding`
- `share_class_shares_outstanding`
- `weighted_shares_outstanding`

入库位置：

- `securities`
- `historical_shares` 的部分 total shares 参考输入

### Ticker Events

主要用途：

- 记录 symbol/name 等变更历史，减少幸存者偏差。

入库位置：

- `security_symbol_history`

当前命令：

```bash
python main.py update_massive_events META --force
```

### Custom Daily Bars

接口形态：

- `GET /v2/aggs/ticker/{ticker}/range/1/day/{from}/{to}`

典型字段：

- `o`: open
- `h`: high
- `l`: low
- `c`: close
- `v`: volume
- `vw`: VWAP
- `n`: trade count
- `t`: bar timestamp
- `otc`: OTC flag

入库位置：

- `daily_prices`

当前口径：

- 默认 `adjusted=false`。
- `daily_prices` 只保存 raw bar。
- `adjusted=true` 只可作为 vendor reference 对账，不写回 `daily_prices`。

### Grouped Daily Bars

主要用途：

- 按交易日批量刷新最近几天全市场日线事实。
- 修正最新交易日可能延迟修订的 OHLCV/VWAP/trade_count。

入库位置：

- `daily_prices`

当前命令：

```bash
python main.py update_grouped_daily --market US --start-date 2026-03-01 --end-date 2026-03-10
```

### Open/Close Summary

主要用途：

- 补充单日 `pre_market` 和 `after_hours`。

入库位置：

- `daily_prices.pre_market`
- `daily_prices.after_hours`

说明：

- 这两个字段是供应商提供的单日摘要价格，不是完整盘前/盘后分钟数据。

### Dividends

典型字段：

- `id`
- `ticker`
- `ex_dividend_date`
- `declaration_date`
- `record_date`
- `pay_date`
- `cash_amount`
- `currency`
- `frequency`
- `distribution_type`
- `historical_adjustment_factor`
- `split_adjusted_cash_amount`

入库位置：

- `corporate_actions`：分红事件 truth。
- `vendor_adjustment_factors`：Massive `historical_adjustment_factor` reference。

当前口径：

- `ex_dividend_date` 映射为 `corporate_actions.ex_date`。
- `historical_adjustment_factor` 不写入 `corporate_actions`，只写入 reference 表。

### Splits

典型字段：

- `id`
- `ticker`
- `execution_date`
- `split_from`
- `split_to`
- `adjustment_type`
- `historical_adjustment_factor`

入库位置：

- `corporate_actions`：拆股事件 truth。
- `vendor_adjustment_factors`：Massive `historical_adjustment_factor` reference。

当前口径：

- `execution_date` 映射为 `corporate_actions.ex_date`。
- 内部 `raw_actions_v1` 拆股因子为 `split_from / split_to`。

### Float / Shares

主要用途：

- 保存 total shares、float shares、free float percent 等点时事实。
- 作为未来换手率计算输入。

入库位置：

- `historical_shares`
- `historical_floats`
- `securities.share_class_shares_outstanding`
- `securities.weighted_shares_outstanding`

当前口径：

- 不直接落库 turnover 或 turnover_rate。
- 回测/分析时用 `daily_prices.volume` 结合点时 shares 动态计算。

### Short Interest / Short Volume

主要用途：

- 做拥挤度、做空压力、流动性风险等策略特征。

入库位置：

- `short_interests`
- `short_volumes`

当前命令：

```bash
python main.py update_massive_short_data TSLA --force
```

### News / Insights

主要用途：

- 保存新闻、publisher、ticker insights 和 sentiment。
- 作为未来事件驱动、情绪特征或风控输入。

入库位置：

- `news_articles`
- `news_article_insights`

当前命令：

```bash
python main.py update_massive_news TSLA --force --lookback-days 7
```

## 可拿但当前不作为事实入库

- 技术指标：SMA、EMA、MACD、RSI 等是派生值，不写事实表。
  Massive 提供这些 endpoint，可以作为后续校验工具的 vendor reference：用本地 raw/canonical bars 按同一参数手动计算，再和 Massive 返回值对账，检查复权参数、窗口边界和缺失 bar。
- adjusted bars：只作为 `vendor_adjustment_factors` reference，不覆盖 raw bars。
- `split_adjusted_cash_amount`：是供应商调整口径字段，当前保留在 API adapter 返回中，但不作为 `corporate_actions` truth。
## 当前不抓取

- Financial statements。
- Ratios。
- 实时 trades/quotes。
- Snapshots。
- 非 `CS` / `ETF` 类型。

（更正 2026-07-06：**1 分钟聚合实测可用**——`/v2/aggs/ticker/{T}/range/1/minute/...`
HTTP 200，含盘前盘后，730 天窗口内有效；已由 `update_minute_bars.py` 周度增量
写入 ClickHouse。本文档早前"分钟级不可用"的结论作废。）

## 结论

当前 Massive 免费层足够支持本项目的日线 Greenfield 基础链路：

- Universe 和证券详情。
- Raw daily OHLCV/VWAP/trade_count。
- 盘前/盘后单日摘要。
- 分红/拆股事件 truth。
- Vendor adjustment reference。
- Shares/float 点时输入。
- Short data。
- News/sentiment metadata。

不足部分也很明确：实时、财报/ratios 和 730 天外的历史窗口不在当前可用范围内（分钟级已于 2026-07 实测可用，见上方更正）。
