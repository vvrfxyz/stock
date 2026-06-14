# Task #6a — `research/factors/asof.py` 通用 as-of 面板工具

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 6 项

## 背景

`research/fundamentals.py:asof_panel` 和 `research/market_cap.py:compute_market_cap_panel` 99% 是同一个 PIT 逻辑(`pd.merge_asof` + staleness 过滤 + pivot + reindex),只是事件列名(metric vs total_shares)和 staleness 列名(period_end vs effective_visible_date)不同。**未来 insider / 13F / short / 股本** 的因子全部要复用这套防未来管线 — 不能每个因子各自抄一遍。

任务: 提取出通用 `event_table_to_asof_panel(...)`, 然后**重构 fundamentals.asof_panel 和 market_cap.compute_market_cap_panel 用它**, 现有所有测试**必须仍然全过**。

报告原话: "把 fundamentals.asof_panel 抽成通用「事件表→as-of 面板」工具(insider/13F/short/股本全部复用同一条防未来管线)"

## 作用域

### 新增文件

```
research/factors/__init__.py      # 仅 expose 包,空 __all__ 即可
research/factors/asof.py          # 通用 as-of 工具
tests/test_factors_asof.py        # 通用工具单元测试 (纯合成)
```

### 修改文件

```
research/fundamentals.py          # asof_panel 改成调用 event_table_to_asof_panel,语义 100% 保持
research/market_cap.py            # compute_market_cap_panel 改成调用 event_table_to_asof_panel,语义 100% 保持
```

### 必须保留的现有测试不变

- `tests/test_research_fundamentals.py` 所有用例
- `tests/test_research_market_cap.py` 所有用例

如果任何现有测试断言失败,**第一反应是修自己的实现**,而不是动测试。允许动测试的唯一场景: 测试在做实现细节的 white-box 检查(目前看下来都是 black-box 行为断言,不会有这种场景)。

## 契约

### `research.factors.asof.event_table_to_asof_panel`

```python
def event_table_to_asof_panel(
    events: pd.DataFrame,
    *,
    dates: pd.DatetimeIndex,
    value_column: str,
    visible_date_column: str = "visible_date",
    staleness_anchor_column: str = "period_end",
    visible_delay_days: int = 0,
    max_staleness_days: int | None = None,
    security_universe: list[int] | pd.Index | None = None,
) -> pd.DataFrame:
    """事件表 → 宽表 as-of 面板,PIT 防未来。"""
```

**输入 events** 必须有至少这些列:
- `security_id`: int64
- `visible_date_column`(默认 `"visible_date"`): datetime64[ns]
- `staleness_anchor_column`(默认 `"period_end"`): datetime64[ns]
- `value_column`(参数指定): 任意 numeric 类型,会强转 float64

**输出**: wide DataFrame
- `index = dates`(完全等于输入,**不**排序,**不**去重)
- `columns = security_universe`(如果给了)或 `events["security_id"].unique()`(如果没给)
- `dtype = float64`
- 没事件的 security_id 列全 NaN
- 没事件的 date 行(date 早于所有 visible_date)全 NaN
- staleness 过滤后超期格子置 NaN

**算法**:

1. dates 标准化到 datetime64[ns]
2. events 过滤掉缺 `security_id` / `visible_date_column` / `value_column` 的行
3. `effective_visible_date = visible_date_column + Timedelta(visible_delay_days)`
4. `security_universe`:
   - 如果用户给了 → 用用户的(int64 + sorted),不在 events 里的 security 那一列全 NaN
   - 没给 → `events["security_id"].unique()`(int64 + sorted)
5. 构造 `grid = (date × security_id)` cross-join
6. `pd.merge_asof(grid, events, left_on="date", right_on="effective_visible_date", by="security_id", direction="backward")`
7. 如果 `max_staleness_days` 给了:对 `staleness_anchor_column`(从 joined 里取)对照 `date - staleness`,超期的 `value_column` 置 NaN
8. `pivot_table(index="date", columns="security_id", values=value_column, aggfunc="last").reindex(index=dates, columns=security_universe)`
9. 返回 cast 成 float64 的结果

**关键不变量**(必须有测试锁):

1. **PIT 不漏 future**: events 里 visible_date=2026-06-10 的行不能影响 date=2026-06-09 的格子
2. **空 events** 不抛错,返回 dates × security_universe 形状的全 NaN panel
3. **空 dates** 不抛错,返回 0 × security_universe
4. **security_universe 给了但事件全没有这些 security** → 全 NaN panel,不抛
5. **staleness=None** → 不做过滤(即"不会因为太老就消失")
6. **staleness=0** → 任何事件都立刻 stale → 任何不在 visible 当日的格子是 NaN
7. **同一个 (security_id, visible_date) 有多个事件**(罕见但发生): pivot 用 `aggfunc="last"`,以**输入 events 里出现的顺序**为准。如果调用方要 last-wins 按某个其他列排序,**应该在调用前** sort_values 好
8. **dates 重复值**: 输出 index 必须等于输入 dates(包括重复),pivot+reindex 自然处理

### `research.factors.asof.attach_event_to_returns` (不做)

`research/events.py` 里已经有这个,**不要**搬到 factors/asof.py。它是事件研究的工具,跟 as-of latest value panel 是不同范畴。

## 重构现有模块

### `research.fundamentals.asof_panel`

旧函数签名保留 100% 不变:

```python
def asof_panel(
    events: pd.DataFrame,
    *,
    dates: pd.DatetimeIndex,
    max_staleness_days: int = 270,
    visible_delay_days: int = 1,
) -> dict[str, pd.DataFrame]:
```

实现里:**按 metric 分组,对每个 group 调用 `event_table_to_asof_panel`**。组装回 dict。

旧的 staleness 行为是**用 `period_end < date - staleness`** (源码 `joined["period_end"] < joined["date"] - staleness`),所以 `staleness_anchor_column="period_end"`。
visible_delay_days 默认仍是 1(fundamentals 特定)。

事件 schema 没变,所有调用方零改动,所有测试零改动,**只是内部 refactor**。

### `research.market_cap.compute_market_cap_panel`

旧函数签名保留:

```python
def compute_market_cap_panel(
    events: pd.DataFrame,
    prices_wide: pd.DataFrame,
    dates: pd.DatetimeIndex,
    max_staleness_days: int,
    visible_delay_days: int,
) -> pd.DataFrame:
```

实现里:
1. prices_wide 按现有方式准备(coerce index/columns)
2. `security_universe = event_ids.union(prices.columns)` (跟现在一样,保留 "missing security 列全 NaN" 行为)
3. 调用 `event_table_to_asof_panel(events, dates=dates, value_column="total_shares", visible_date_column="visible_date", staleness_anchor_column="visible_date", max_staleness_days=max_staleness_days, visible_delay_days=visible_delay_days, security_universe=security_universe)` 得到 `shares`
4. 返回 `prices * shares` 强转 float64

**注意 staleness_anchor_column 是 `"visible_date"`,不是 `"period_end_date"`** — 这跟 fundamentals 不同,因为 market_cap 的现有实现里 staleness 用的是 `effective_visible_date < date - staleness`,不是用 period_end_date。**这是有意的**(shares 的"过期"是基于披露日,不是报告期末日),保留原语义。

测试 `tests/test_research_market_cap.py::test_stale_shares_become_nan` 是这个语义的 lock,refactor 后必须仍然过。

## 测试

### `tests/test_factors_asof.py` 必须包含

(纯合成 pandas DataFrame,无 DB):

1. `test_basic_asof_lookup` — 1 security,2 events,4 dates 跨越事件 → assert 期望值
2. `test_pit_no_future_leak` — events 全部在 dates 之后,所有格子 NaN
3. `test_max_staleness_caps_old_events` — staleness=10,事件 100 天前 → NaN
4. `test_no_staleness_keeps_old_events` — staleness=None,事件 1000 天前还是有效
5. `test_visible_delay_shifts_visibility` — events 在 date d,visible_delay=2 → date d 仍是 NaN,date d+2 才有值
6. `test_security_universe_includes_missing_security` — events 只有 security 1,universe=[1,2,3] → columns 含 1,2,3,column 2/3 全 NaN
7. `test_security_universe_default_uses_event_ids` — universe=None → columns 严格等于 events 里出现过的 security_ids,sorted
8. `test_empty_events_returns_all_nan` — events 空 → 全 NaN panel,不抛
9. `test_empty_dates_returns_empty` — dates 空 → 0 × universe shape
10. `test_dates_order_and_duplicates_preserved` — dates 含重复 + 非升序 → 输出 index **不被排序、不被去重**
11. `test_multiple_events_same_security_same_visible_date_last_wins` — 同 (sec, visible_date) 两行不同 value → aggfunc=last 取最后
12. `test_nan_values_in_events_filtered` — events 有 value=NaN 的行 → 被过滤,不影响其他行
13. `test_explicit_staleness_anchor_column` — value_column=total_shares, staleness_anchor=visible_date(market_cap 模式) → 验证行为对

### 现有测试零改动

- `tests/test_research_fundamentals.py` 全部 unit + integration
- `tests/test_research_market_cap.py` 全部 unit + integration

如果任何已有测试 fail,**先怀疑自己 refactor 漏了细节**,**不要** relax 测试。

## 验收

```bash
# 1. 新工具单元测试
python -m pytest tests/test_factors_asof.py -q -m "not integration"

# 2. 新工具完整
python -m pytest tests/test_factors_asof.py -q

# 3. fundamentals 仍然全过(refactor 不破坏旧行为)
python -m pytest tests/test_research_fundamentals.py -q

# 4. market_cap 仍然全过
python -m pytest tests/test_research_market_cap.py -q

# 5. 全套无回归 — 至少 278 passed(当前基线)+ 13 新 = 291 passed
python -m pytest tests/ -q
```

`tests/test_factors_asof.py` ≥ 13 个测试 passed。`tests/ -q` ≥ 291 passed。**fundamentals 和 market_cap 的现有测试全部 unchanged**。

## 反需求 (绝不能做)

1. **不要**改 fundamentals.asof_panel 的签名(callers 依赖,会破坏 run_baselines)
2. **不要**改 market_cap.compute_market_cap_panel 的签名
3. **不要**改任何现有测试代码 — 你的实现必须满足现有测试,而不是反过来
4. **不要**重写 attach_event_to_returns(events.py 的事件研究工具,不在本任务范围)
5. **不要**改任何数据库 schema、alembic、db_manager
6. **不要**改 `research/__init__.py` 注册 factors 子包(让它自然 import,不用 re-export)
7. **不要**引入新依赖(pandas/numpy/sqlalchemy 之外)
8. **不要**加任何"通用 timezone 处理"或"通用 dtype coerce" — 输入 events 已经在调用方 normalize 好了
9. **不要**改 events.py(那是事件研究,不是 as-of)

## 实现建议

- 在 `event_table_to_asof_panel` 内部:先用 `events.copy()` 防污染调用方
- `pd.merge_asof` 要求两边都 sorted,sort 在函数内部做(`events.sort_values(["effective_visible_date", staleness_anchor_column], kind="mergesort")`)
- 输出 index 保留输入 dates 的原始顺序:用 `.reindex(dates)` 最后一步
- security_universe 强转 int64 时,小心 NaN security_id 抛错 → 调用方/此函数应在传入前清掉

## 工作时长估算

8-12 小时(refactor 既要谨慎保留语义,又要做新工具完整,加测试。**对中等复杂度的抽象任务,这是合理估计**)
