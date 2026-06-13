# Task #9 — 财报事件日历

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 9 项

## 背景

报告原话: "从 sec_filings(含 accepted_at 精确时间戳)派生,**零新数据**解锁 PEAD/公告日研究;事件研究靠横截面事件数堆样本,是短窗口下少数统计可行的方向。"

PEAD = Post-Earnings Announcement Drift,经典事件研究。需要的输入是"每只股票每个财报公告事件的精确发生时刻 + 当时已公开的最新信息"。库里 `sec_filings` 表已经把 10-K / 10-Q 索引齐(配合 `accepted_at` 精确到秒的时间戳),**所有原料都在,不需要新数据**。

## 作用域

### 新增文件

```
research/events.py                # 主模块
tests/test_research_events.py     # 单元 + 集成测试
```

### 修改文件

无。

## 契约

### `research.events.load_earnings_events(engine, *, since: date | None = None, until: date | None = None, security_ids: list[int] | None = None, form_types: tuple[str, ...] = ("10-K", "10-Q", "10-K/A", "10-Q/A")) -> pd.DataFrame`

从 `sec_filings` 派生事件流,**严格 PIT**:每条事件代表"某证券在某时刻披露了财报"。

**返回 schema**(必须正好这些列,这个顺序):

| 列名 | 类型 | 语义 |
|---|---|---|
| `security_id` | `int64` | 关联 securities.id,**只保留非空**(`sec_filings.security_id IS NOT NULL`) |
| `accession_number` | `string` | SEC accession,可作事件唯一键 |
| `form_type` | `string` | 例如 `"10-K"` / `"10-Q"` |
| `filing_date` | `datetime64[ns]` | 申报日期,日期粒度 |
| `accepted_at` | `datetime64[ns, UTC]` | EDGAR 受理时刻,**精确到秒**(关键 PIT 边界) |
| `period_of_report` | `datetime64[ns]` | 报告期末日(可能 NULL,稀少) |
| `event_visible_at` | `datetime64[ns, UTC]` | 信息真正公开可用的时刻(见下方"可见性规则") |

**可见性规则** (`event_visible_at`):

- 如果 `accepted_at` 在交易时段 (Mon-Fri 9:30-16:00 ET) 内 → 当日交易日下一根 1-min bar(简化:`accepted_at` 本身)
- 如果 `accepted_at` 在盘后 (16:00 ET 之后) 或周末 / 节假日 → **下一个交易日的 09:30 ET**
- 如果 `accepted_at` 为 NULL(老 filing 缺这个字段)→ `filing_date + 1 day 09:30 ET`(保守 fallback)

时区处理: `accepted_at` 在库里是 timezone-aware UTC,**所有比较/算术先转成 ET(`America/New_York`)做日历判断,再转回 UTC 存回 `event_visible_at`**。

#### 简化交易日历

任务**不**要求精确节假日处理。简版:

- "交易日" = 周一到周五,**不**特殊处理美股节假日
- 这是已知简化,在 docstring 里写明"节假日不处理 = 极少数事件可能比真实早 1-3 天可见"
- 如果项目里已有 `utils/trading_calendar.py` 暴露了节假日 API,**优先**复用(Codex 自己看一眼);否则用简化版

### `research.events.attach_event_to_returns(events: pd.DataFrame, returns: pd.DataFrame, *, window: tuple[int, int] = (-5, 20)) -> pd.DataFrame`

事件研究的核心工具:把每个事件按 `event_visible_at` 对齐到日频 returns 面板,产出"事件窗口收益矩阵"。

- 输入:
  - `events`: `load_earnings_events` 的返回
  - `returns`: 日频宽表 (index=`DatetimeIndex` UTC, columns=`security_id`),典型来源 `research.data.load_adjusted_panel(...)["adj_close"].pct_change()`
  - `window`: `(pre, post)` 日数,默认 t-5 到 t+20
- 输出:长表

| 列名 | 类型 | 语义 |
|---|---|---|
| `accession_number` | `string` | 事件唯一键 |
| `security_id` | `int64` | |
| `event_date` | `datetime64[ns]` | 事件可见日(取 `event_visible_at.date()`)|
| `relative_day` | `int` | 距事件的交易日偏移,-5 到 +20 |
| `return` | `float` | 当日 return(若 returns 缺失 → NaN) |

**关键 PIT 不变量**: `relative_day = 0` 对应 **事件可见的当天**(即 `event_visible_at` 那天的下一根交易日 close 的 return)。事件研究中 `relative_day = 0` 的 return 是"公告日效应",不应包含公告前的信息。在 docstring 写明这条不变量,加测试锁。

## 测试

`tests/test_research_events.py` 必须包含:

### 单元(纯合成,无 DB)

1. `test_visibility_after_hours_pushes_to_next_open` — `accepted_at = 2026-06-10 21:00 UTC`(美东 17:00 EDT 盘后)→ `event_visible_at = 2026-06-11 13:30 UTC`(美东 09:30 EDT 次日开盘)
2. `test_visibility_during_market_hours_uses_accepted_at` — `accepted_at = 2026-06-10 17:30 UTC`(美东 13:30 EDT 盘中)→ `event_visible_at = 2026-06-10 17:30 UTC`(同时刻)
3. `test_visibility_weekend_pushes_to_monday` — 周六 / 周日 accept → 下周一 09:30 ET
4. `test_visibility_null_accepted_at_uses_filing_date_next_open` — `accepted_at = None` → `filing_date + 1 day 09:30 ET`
5. `test_attach_event_to_returns_relative_day_zero_is_visible_day` — 合成 returns + 1 个事件,断言 `relative_day=0` 的 return 对应 `event_visible_at` 那天
6. `test_attach_event_to_returns_window_bounds` — 窗口 `(-2, 3)` 产出 6 行/事件
7. `test_attach_event_to_returns_missing_returns_become_nan` — returns 面板缺该 security_id 列 → 全部 NaN,不抛异常

### 集成 (`pytest.mark.integration`, 用 `pg_db`)

8. `test_load_earnings_events_basic` — 插 3 个 securities + 5 个 sec_filings(其中 2 个 10-K, 2 个 10-Q, 1 个 8-K),断言:
   - `load_earnings_events()` 默认只返回 4 个(10-K + 10-Q)
   - `form_types=("8-K",)` 只返回 1 个
   - 输出列正好是规格里的 7 列,顺序一致
9. `test_load_earnings_events_skips_null_security_id` — 插 1 个 `security_id IS NULL` 的 filing,断言**不在**输出里
10. `test_since_until_window` — 插 3 个 filing,日期分别在窗口前/内/后,断言只返回窗口内的

## 验收

```bash
# 1. 单元绿
python -m pytest tests/test_research_events.py -q -m "not integration"

# 2. 集成绿
python -m pytest tests/test_research_events.py -q

# 3. 全套无回归
python -m pytest tests/ -q
```

`tests/test_research_events.py` ≥ 10 个测试 passed。

## 反需求

1. **不要**在 `sec_filings` 加索引或改 schema(读现有列就够)
2. **不要**做 8-K / Form 4 / Form 13F 的事件特化逻辑 — 任务只覆盖 10-K/10-Q 的 PEAD,其他 form 通过 `form_types` 参数支持但不写专门的可见性逻辑
3. **不要**把事件存进 DB 表 — 这是 read-time 派生工具
4. **不要**用 `securities.symbol` 做 join — 任何场景下都用 `security_id`
5. **不要**做 earnings consensus / surprise 计算 — 那需要分析师预期数据,我们没有;PEAD 用 returns + 事件窗口已能跑
6. **不要**实现完整 NYSE 节假日历 — 简版可接受,文档说清楚就行

## 实现建议

- 时区: `pytz` 已被 pandas 依赖,直接用 `pd.Timestamp.tz_convert("America/New_York")`
- SQL:

```sql
SELECT id, security_id, accession_number, form_type, filing_date, accepted_at, period_of_report
FROM sec_filings
WHERE source = 'SEC_EDGAR'
  AND security_id IS NOT NULL
  AND form_type = ANY(:form_types)
  AND (:since IS NULL OR filing_date >= :since)
  AND (:until IS NULL OR filing_date <= :until)
  AND (:security_ids IS NULL OR security_id = ANY(:security_ids))
ORDER BY security_id, filing_date, accepted_at
```

- 事件窗口对齐: returns.index 上做 `searchsorted` 找 `event_visible_at.date()` 的位置 + window 偏移,比逐事件 reindex 快很多

## 工作时长估算

4-6 小时(可见性规则的边界 case + 事件窗口的 PIT 不变量需要仔细)。
