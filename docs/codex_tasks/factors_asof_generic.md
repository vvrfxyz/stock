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
2. events 过滤掉缺 `security_id` / `visible_date_column` / `value_column` 的行。**此外**:若 `staleness_anchor_column != visible_date_column` 且 `max_staleness_days is not None`,**同时过滤 `staleness_anchor_column` 为 NaT 的行** — 因为 `pandas NaT < timestamp` 返回 False,陈旧值会静默绕过步骤 7 的 staleness 过滤。当前两个 caller (fundamentals SQL 硬过滤 period_end / market_cap anchor=visible_date 退化为同列)均不受影响,此条是 hygiene 性契约澄清。
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

1. **PIT 不漏 future**: events 里 visible_date=2026-06-10 的行不能影响 date=2026-06-09 的格子(visible_delay_days=0 时;>0 时类推)
2. **空 events** 不抛错,返回 dates × security_universe 形状的全 NaN panel
3. **空 dates** 不抛错,返回 0 × security_universe
4. **security_universe 给了但事件全没有这些 security** → 全 NaN panel,不抛
5. **staleness=None** → 不做过滤(即"不会因为太老就消失")
6. **staleness 精确公式**(anchor-relative,不变量必须按此口径写测试):

   - `staleness=k` (k ≥ 0,整数天) → 对 joined 后每个格子,当 `joined[staleness_anchor_column] < date - Timedelta(days=k)` 时该格 `value=NaN`。
   - `staleness=0` 退化为 `anchor < date`,即 anchor 等于 date 当日仍保留,严格更早的格子置 NaN。
   - `staleness=None` 跳过过滤。
   - **anchor 选型对结果有强烈影响**(测 staleness=0 时务必显式选 anchor):
     - `anchor=visible_date_column`(market_cap 默认) → 只在"披露当日及之后 staleness 天窗口内"保留;staleness=0 时只在披露当日保留。
     - `anchor=period_end_column`(fundamentals 默认,period_end 通常远早于 visible_date) → 几乎所有 fundamentals 面板在 staleness=0 时被擦成 NaN,这是预期行为不是 bug。

7. **同 (security_id, effective_visible_date) 多事件的胜出规则**(精确公式):

   - 工具内部按 `events.sort_values(["effective_visible_date", staleness_anchor_column], kind="mergesort")` 排序后调 `pd.merge_asof(direction="backward")`。
   - 真实的胜出规则是 `merge_asof` 取**右表 `staleness_anchor_column` 最大**的那一条;anchor 相同时退化为输入物理顺序(mergesort 稳定)。
   - `pivot_table(aggfunc="last")` **不是**同日多事件的去重机制 — 它仅在输入 `dates` 含重复值时参与决策。
   - 调用方若需要其他 tie-breaker,**应该在调用前** sort_values 好。
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

**dates 行为差异**: 旧实现入口 `pd.DatetimeIndex(sorted(pd.to_datetime(dates)))` 把 dates 强制排序;新实现遵循 `event_table_to_asof_panel` 的输出契约 "index 完全等于输入,不排序不去重"。现有所有 caller(`load_fundamental_panel` + 全部测试用例)均已传升序 dates,**实测不受影响**;此条仅为消除 Codex 实施时的犹豫:**不要在 wrapper 里再加一次 sorted()** —— 把排序责任交还给调用方。

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

**`visible_delay_days` 与 anchor 的 staleness 行为差异说明**:

- 老实现 staleness 对比的是 `effective_visible_date`(= visible_date + delay)。
- 新工具按 spec 用 `staleness_anchor_column="visible_date"`,即 staleness 窗口从**原始**披露日起算。
- `visible_delay_days=0` 时 `visible_date == effective_visible_date`,**两套实现等价**——当前所有生产 caller (`load_market_cap_panel` / `load_log_market_cap_panel`) 都用 delay=0,所以测试和生产都不会变。
- `visible_delay_days>0` 时新实现会比老实现**早 `visible_delay_days` 天置 NaN**,这是有意的语义收紧(shares 的"过期"应基于实际披露发生时刻,与 effective delay 是两个独立维度;延后可见≠延后过期)。"行为完全不变" 严格只适用于 `visible_delay_days=0` 的现有 caller。

测试 `tests/test_research_market_cap.py::test_stale_shares_become_nan` 用 `visible_delay_days=0`,是这个语义的 lock,refactor 后必须仍然过。

**`(security_id, visible_date)` 多事件的"取最新报告期"语义**:

老实现按 `sort_values(["effective_visible_date", "period_end_date"], kind="mergesort")` 双键排序,同 visible_date 下多个重述事件被强制按 `period_end_date` 升序物理排列,`merge_asof(backward)` 因此取到 `period_end_date` 最大的一条。新工具按 spec 用 `sort_values(["effective_visible_date", staleness_anchor_column], kind="mergesort")`,而 market_cap 模式下 `staleness_anchor_column == visible_date_column`,两列退化为同序副键,选择**完全依赖输入行的物理顺序**。

生产中 `load_shares_events` 的 SQL `ORDER BY security_id, filing_date, period_end_date` 恰好保证 `period_end_date` 升序在物理后位,与新工具的稳定 mergesort 结合,效果等价于老实现。**重构必须保持 `load_shares_events` 的 ORDER BY 不变**,任何修改会无声破坏此隐式契约。调用方若传入自定义 events,需自行 pre-sort,工具内部不再追加 `period_end_date` 副排序键。

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
11. `test_multiple_events_same_security_same_effective_visible_date_anchor_max_wins` — 同 (sec, effective_visible_date) 两行不同 `staleness_anchor_column` 值 → `merge_asof(backward)` 取 anchor 最大那一条;anchor 相同时取输入物理顺序最后一条(mergesort 稳定)
12. `test_nan_values_in_events_filtered` — events 有 value=NaN 的行 → 被过滤,不影响其他行
13. `test_explicit_staleness_anchor_column` — value_column=total_shares, staleness_anchor=visible_date(market_cap 模式) → 验证行为对
14. `test_anchor_choice_changes_staleness_behavior` — 同一组 events,staleness=30,anchor 分别选 `visible_date` 和 `period_end`(差距 100 天) → 两种 anchor 给出可区分的 NaN 模式;锁定不变量 6 的 anchor-relative 公式
15. `test_nat_in_staleness_anchor_filtered_when_distinct_from_visible_date` — events 含一行 anchor=NaT(anchor ≠ visible_date 时),staleness 给定 → 该行在步骤 2 被过滤,**不**因 `NaT < timestamp == False` 静默逃过 staleness 检查

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

# 5. 全套无回归 — 至少 278 passed(当前基线)+ 15 新 = 293 passed
python -m pytest tests/ -q
```

`tests/test_factors_asof.py` ≥ 15 个测试 passed。`tests/ -q` ≥ 293 passed(基线 278 + 新 15)。**fundamentals 和 market_cap 的现有测试全部 unchanged**。

## 反需求 (绝不能做)

1. **不要**改 fundamentals.asof_panel 的签名(callers 依赖,会破坏 run_baselines)
2. **不要**改 market_cap.compute_market_cap_panel 的签名
3. **不要**改任何现有测试代码 — 你的实现必须满足现有测试,而不是反过来
4. **不要**重写 attach_event_to_returns(events.py 的事件研究工具,不在本任务范围)
5. **不要**改任何数据库 schema、alembic、db_manager
6. **不要**改 `research/__init__.py` 注册 factors 子包(让它自然 import,不用 re-export)
7. **不要**引入新依赖(pandas/numpy/sqlalchemy 之外)
8. **不要**加任何"通用 timezone 处理"或"通用 dtype coerce" — 输入 events 已经在**包装层** normalize 好了(见下方"wrapper 层 dtype 归一化责任")
9. **不要**改 events.py(那是事件研究,不是 as-of)

### wrapper 层 dtype 归一化责任(澄清反需求 8)

`event_table_to_asof_panel` 不做 dtype 校验也不 coerce `visible_date_column` / `staleness_anchor_column` 列(遵守反需求 8)。**但**包装层 `fundamentals.asof_panel` 和 `market_cap.compute_market_cap_panel` **必须保留各自的 `_to_ns(...)` 调用**,把 events 的 `visible_date` / `period_end` / `period_end_date` 列强转为 `datetime64[ns]` 再交给工具。

理由:这两个包装层就是工具的直接调用方,上游 `load_*` 函数从 PostgreSQL 读出的时间列常为 object / date / `datetime64[us]` dtype,直接 `Timedelta` 加法或 `merge_asof` 会静默坏(`merge_asof` 要求两侧 dtype 一致,pandas 3.x 默认推断为 ns 之外的精度会触发 cast)。**反需求 8 的"已在调用方 normalize"指的是包装层这一层,不是 `load_*` 函数。** Codex 不得以 "DRY" 为由删除 `_to_ns` 调用。

## 实现建议

- 在 `event_table_to_asof_panel` 内部:先用 `events.copy()` 防污染调用方
- `pd.merge_asof` 要求两边都 sorted,sort 在函数内部做(`events.sort_values(["effective_visible_date", staleness_anchor_column], kind="mergesort")`)
- 输出 index 保留输入 dates 的原始顺序:用 `.reindex(dates)` 最后一步
- security_universe 强转 int64 时,小心 NaN security_id 抛错 → 调用方/此函数应在传入前清掉

## 工作时长估算

8-12 小时(refactor 既要谨慎保留语义,又要做新工具完整,加测试。**对中等复杂度的抽象任务,这是合理估计**)
