# `daily_prices.adj_factor` 设计与计算规则

## 目标与定义

本项目采用 **前复权（最新交易日=1）** 的 **Total Return** 口径（含拆股 + 现金分红）。

- 查询侧用法（任意 OHLC 同理）：
  - `adj_close = close * adj_factor`

## 计算规则（核心公式）

按日期从新到旧遍历交易日序列，维护累计因子 `F`（初始 `F=1`）。

对每个交易日 `t`：
1. 写入 `adj_factor(t) = F`
2. 在“跨过事件日后”（进入更早日期）更新 `F`：

### 拆股 / 合股

- 事件日：`execution_date = d`
- 比例：`r = split_to / split_from`
- 处理完日期 `d` 后，往更早日期：
  - `F = F / r`

例：AAPL 2020-08-31 4-for-1（`r=4`），则 `d` 之前的 `adj_factor` 将额外乘上 `0.25`。

### 现金分红

- 事件日：`ex_dividend_date = d`
- 每股现金：`D`（同日多条分红按 `D` 求和）
- 取除权日前一交易日收盘价 `C_prev`（raw close）
- 处理完日期 `d` 后，往更早日期：
  - `F = F * (C_prev - D) / C_prev`
  - 若 `C_prev` 缺失或 `C_prev <= D`：跳过该事件（避免异常比值）

## 非交易日事件日期的处理

数据源可能偶发提供非交易日作为事件日。本项目会将事件日期映射到 **下一个可用交易日（>= event_date）** 后再应用（仅允许在一个很小的窗口内平移；若跨度过大则跳过该事件并记录日志）。

## 运行与维护建议

- 公司行动更新完成后（`update_actions`），默认会自动触发该证券的 `adj_factor` 重算（如需跳过：`--skip-recalc-adj-factor`）。
- 若发现公司行动历史不全（例如超出 Massive 免费层 2 年窗口），可用 YFinance 补齐后再重算：
  - `python main.py backfill_actions AAPL --recalc-adj-factor`
- 若需要强制全量校验/重算：
  - `python main.py recalc_adj_factor AAPL`

## 验收示例（AAPL 2020-08-31 拆股）

应满足：`adj_factor(2020-08-28) ≈ adj_factor(2020-08-31) / 4`（允许存在 1e-6 级别的四舍五入误差）。

```sql
select date, close, adj_factor, close*adj_factor as adj_close
from daily_prices
where security_id=(select id from securities where symbol='aapl' limit 1)
  and date in ('2020-08-28','2020-08-31','2020-09-01')
order by date;
```
