# Task #8b — PIT 市值面板

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 8 项(中性化原料的后半)

## 背景

横截面因子研究的标配中性化变量是 **log(市值)**。市值 = `当日收盘价 × 当日 outstanding shares`,听起来简单,**但 PIT 化是陷阱**:

- ❌ `securities.share_class_shares_outstanding` 是**当前快照**,所有历史日期都用它就是 look-ahead bias(用未来的股本算过去的市值)
- ✅ **`historical_shares.total_shares`** 才是 PIT 正确的,而且 CLAUDE.md 明确说: `filing_date` 是 PIT 可见性边界——某条 `historical_shares` 行只有在 `filing_date <= 当前日` 时才可被使用

报告原话: "PIT 市值面板(close × shares_outstanding,原料全在库内)"。原料确实全在库内,但**必须用 PIT-correct 的那个原料**。这是任务的核心难点。

## 作用域

### 新增文件

```
research/market_cap.py                # 主模块
tests/test_research_market_cap.py     # 单元 + 集成测试
```

### 修改文件

无。

## 契约

### `research.market_cap.load_shares_events(engine, *, security_ids: list[int] | None = None) -> pd.DataFrame`

把 `historical_shares` 表加载成 PIT 事件流。每条 = "某证券在某 `filing_date` 公开了 `period_end_date` 的 total_shares 数值"。

**返回 schema**(必须这些列、这个顺序):

| 列名 | 类型 | 语义 |
|---|---|---|
| `security_id` | `int64` | |
| `visible_date` | `datetime64[ns]` | = `filing_date`,信息可用边界 |
| `period_end_date` | `datetime64[ns]` | 报告期末 |
| `total_shares` | `int64` | 总股本 |

**SQL**:

```sql
SELECT security_id, filing_date AS visible_date, period_end_date, total_shares
FROM historical_shares
WHERE source = 'MASSIVE'
  AND total_shares IS NOT NULL
  AND (:security_ids IS NULL OR security_id = ANY(:security_ids))
ORDER BY security_id, filing_date, period_end_date
```

**重述/重复处理**: 同一 `(security_id, filing_date)` 可能有多个 `period_end_date`(罕见,vendor 双填),保留全部行;消费者(asof_panel)做 last-wins。

### `research.market_cap.load_market_cap_panel(engine, *, dates: pd.DatetimeIndex, security_ids: list[int] | None = None, max_staleness_days: int = 400, visible_delay_days: int = 0) -> pd.DataFrame`

PIT 市值面板。每个 `(date, security_id)` 取**当时可见**的最新 total_shares × **当日复权前 close**(原始价,因为 outstanding shares 与原始价匹配)。

- 输入:
  - `dates`: 通常是调仓日序列(`pd.DatetimeIndex`,纳秒)
  - `security_ids`: None = 全宇宙
  - `max_staleness_days`: shares 事件距 `date` 超过这么多天则视为停止披露,置 NaN(默认 400 天 = ~13 个月,容忍 10-K 年度披露周期)
  - `visible_delay_days`: 默认 0(shares 通常在 filing 当天就公开,与 fundamentals 的 filed_date 不同)
- 返回:**宽表** `index=dates, columns=security_id, values=market_cap`(float64,美元)

**实现核心**:用 `fundamentals.asof_panel` 的同款 `pd.merge_asof` 模式。**不要**重写一遍,任务允许导入 `research.fundamentals.asof_panel` **如果它已经做成了通用工具**;否则在本模块写一个清晰的小版本(只处理 shares 这一个 metric)。

**关键不变量** — 必须有测试锁:

1. **PIT 不漏 future shares**:如果 `events` 里有一条 `filing_date=2026-06-10`,而 `dates` 包含 `2026-06-09`,那 `2026-06-09` 的市值必须用 **2026-06-10 之前**最新的 shares,而不是 2026-06-10 的
2. **价格 = 原始 close**:`daily_prices.close` 直接读,**不要**复权(outstanding shares 没复权,要保持 unit 一致)
3. **stale 置 NaN**:某 security 最后一条 shares 事件是 2024-01-01,`max_staleness_days=400`,那 2025-03-01 之后的市值面板该 security 必须是 NaN
4. **缺数据 NaN 不抛错**:某 security 完全没有 shares 事件 / 完全没有 daily_prices,对应列全 NaN,函数正常返回

### `research.market_cap.load_log_market_cap_panel(...)`

签名同上,返回 log 市值 = `np.log(market_cap)`。市值 = 0 或 NaN → NaN。这是中性化中最常用的形式。

## 测试

`tests/test_research_market_cap.py` 必须包含:

### 单元(纯合成,无 DB)

注:这层基本都是 PIT 边界,合成数据最值得测。用本地构造的 `events` + `prices_wide` 直接喂给小工具函数(把 `load_market_cap_panel` 拆出一个 pure 函数 `compute_market_cap_panel(events, prices_wide, dates, max_staleness_days, visible_delay_days)`,无 engine 参数,方便测试)。

1. `test_pit_does_not_leak_future_shares`:
   - 1 个 security,2 条 shares 事件 (filing_date=2025-01-15 total=1e6, filing_date=2026-06-10 total=2e6)
   - dates 包含 2025-12-31 和 2026-06-09 和 2026-06-10 和 2026-06-15
   - 断言 2025-12-31 / 2026-06-09 用 1e6,2026-06-10 / 2026-06-15 用 2e6
2. `test_stale_shares_become_nan`:
   - 1 个 security,只有 1 条 shares 事件 (filing_date=2024-01-01, total=1e6)
   - max_staleness_days=400, dates 包含 2024-06-01, 2025-01-15, 2025-03-15
   - 2024-06-01 / 2025-01-15 是 1e6 × close,2025-03-15 (>400 天) 是 NaN
3. `test_missing_security_returns_all_nan`:
   - dates 里有 security_id=999,但 events 和 prices 都没有它 → 列全 NaN,不抛
4. `test_market_cap_unit_uses_raw_close_not_adjusted`:
   - 同一 security 跨拆股,close 在拆股日跳变,断言市值用 raw close × shares 算出来(在拆股前后约等,因为 shares 也按比例改了)。
   - 这是一个微妙不变量:**对 PIT 市值,raw close × PIT shares ≈ 复权 close × 当前 shares**;但选哪种都行,**只要内部一致**。任务要求:**用 raw close**(因为这正是 outstanding shares 配对的口径)。

### 集成 (`pg_db`)

5. `test_load_shares_events_against_real_schema`:
   - 插 1 个 security + 3 条 historical_shares 行
   - 调 `load_shares_events`,断言返回 3 行,列正确,顺序按 (security_id, filing_date)
6. `test_load_market_cap_panel_against_real_schema`:
   - 插 1 个 security,2 条 shares + 5 条 daily_prices,dates 跨越 shares 切换日,断言 panel 值正确

## 验收

```bash
# 1. 单元绿
python -m pytest tests/test_research_market_cap.py -q -m "not integration"

# 2. 集成绿
python -m pytest tests/test_research_market_cap.py -q

# 3. 全套无回归
python -m pytest tests/ -q
```

`tests/test_research_market_cap.py` ≥ 6 个测试 passed。

## 反需求

1. **不要**用 `securities.share_class_shares_outstanding` 或 `securities.weighted_shares_outstanding` 做市值 — 那是当前快照,**会产生 look-ahead bias**,违反报告 H2 同款 PIT 原则
2. **不要**把 PIT 市值面板存 DB — read-time,与因子库其他工具一致
3. **不要**做"流通市值" (`float_shares` × close) — 任务只要总市值。流通市值有 PIT 边界(float effective_date),复杂度翻倍,另开任务
4. **不要**在面板里同时返回原始 close 和市值 — 只返回市值。需要 close 的话调用者直接读 `research.data`
5. **不要**用 `daily_prices.adjusted_close` 之类的列 — 不存在,事实表只有 raw,**注意 review 已经把"研究层只读 raw + read-time 复权"这条铁律强化过**
6. **不要**做做空利息 / shares borrowed 等其他股本系信号 — 那是另一组任务

## 实现建议

如果发现 `research.fundamentals.asof_panel` 可以**直接复用**(把 events 改名成 `metric="total_shares"` 喂进去),那是首选。但 `asof_panel` 当前签名是为 fundamentals 设计的,字段名不完全对(它有 `value`/`metric`/`period_end`/`visible_date`),Codex 自己判断:

1. **复用** `asof_panel` — 把 shares events 改成它要的 schema,然后 join 价格做乘法。代码最少
2. **写新的** `_shares_asof_array` — 用 `merge_asof` 直接做,不依赖 fundamentals 模块。隔离最好

任意一条路都可接受,在 PR 描述里说明选了哪条 + 为什么。

## 工作时长估算

5-8 小时(PIT 边界 + raw vs adjusted 的选择 + 测试用例多)。

## 后续依赖(给将来的参考,本任务不做)

这个面板是后续多个任务的输入:

- `research/evaluate.py` 横截面回归的中性化变量
- `research/factors/` 里的 size 因子 = `-log_market_cap`(经典 Fama-French SMB 来源)
- 流通调整后的"做空压力"因子:`short_interest / market_cap`

不需要现在做。
