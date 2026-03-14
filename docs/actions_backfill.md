# 公司行动补全（YFinance Backfill）

## 背景

本项目默认使用 Massive 落库公司行动（`stock_dividends` / `stock_splits`）。但 Massive 免费层历史窗口约为最近 2 年，因此更早的分红/拆股事件可能不会覆盖到，导致：

- `stock_dividends` / `stock_splits` 缺失早期事件；
- `daily_prices.adj_factor`（前复权 + total return）计算缺少事件，出现明显偏差。

## 解决方案

提供维护命令 `backfill_actions`，用 YFinance 补齐缺失的分红/拆股日期（只插入缺失，不覆盖已有记录），并可选择重算 `adj_factor`：

```bash
python main.py backfill_actions AAPL --recalc-adj-factor
```

## 关键口径：YFinance 分红数值的拆股调整

YFinance 常见口径是：历史分红 `dividends` 已按后续拆股进行缩放（即“按当前股本口径”）。为了与本项目 `daily_prices.close`（raw/未复权）保持一致，需要将其反向还原为事件发生时的 raw 口径。

直观例子（AAPL）：
- YFinance 在 2014-05-08 给出 `0.1175`（已按后续 `7-for-1` 和 `4-for-1` 缩放）
- raw 口径应为 `0.1175 * 7 * 4 = 3.29`

### 自动校验与自适应（避免“拍脑袋”）

`scripts/backfill_actions_from_yfinance.py` 会在同一 symbol 上做交叉验证：

- 取 **YFinance dividends** 与数据库中已有分红日期的重叠样本（>=5 天）；
- 计算两种候选口径并与数据库现有金额对比：
  1) 假设 YFinance 已拆股调整：`raw = dividend * Π(后续 splits)`
  2) 假设 YFinance 已是 raw：`raw = dividend`
- 选择 **mismatch 更少 / 总误差更小** 的方式作为该 symbol 的口径，并据此插入缺失日期。

## 数据写入策略（不变量）

- 只插入缺失日期：
  - 分红：若该 `ex_dividend_date` 数据库已存在任意记录，则跳过该日期（不覆盖）。
  - 拆股：若该 `execution_date` 已存在，则跳过该日期（不覆盖）。
- `StockDividend.currency` 为 NOT NULL：
  - 优先用 `securities.currency`，若缺失且为 US 市场，则默认 `USD`；否则跳过无法确定货币的记录。
- 可选重算：
  - `--recalc-adj-factor`：无论是否新增 actions，都重算；
  - 默认：仅当新增 actions 时重算；
  - `--skip-recalc-adj-factor`：跳过重算。

## 适用场景

- 发现某个 symbol 的 `stock_dividends` / `stock_splits` 明显缺失早期历史；
- 复权因子验证失败（例如 AAPL 2020-08-31 拆股前 `adj_factor` 比例不符合预期）。
