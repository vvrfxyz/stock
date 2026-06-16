# Task #10 — short_interest_ratio 因子 (做空数据子单)

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Next 第 2 项 ("第一梯队另类因子: 做空数据"); 因子库骨架 (#6a/#6b) 第一次真压力测试。

**依赖**: #6a (`research/factors/asof.py`) 与 #6b (`research/factors/protocol.py` + `register`) 已 merged。本任务**只新增一个 builtin 因子**,基础骨架完全不动。

**Spec 版本**: v2 (2026-06-16, 经 Codex 对抗式 review 修订 — 修复 B1/B2/B3 blocker, H1/H2/H3/H4/H5/H6 high, M1/M2/M3/M4/M5/M6)

## 背景

`size` 和 `earnings_yield` 两个 builtin 都从 fundamentals/market_cap 取数,两者 PIT 形态相似。因子骨架是否真的通用,得用一张**完全不同形态的事件表**来验证。`short_interests` 是好选择:

- **半月频率** (FINRA 每月 15 号和月底两次结算),与季度财报、每日 market_cap 都不同
- **披露滞后** (FINRA 在 settlement_date 之后的第 8 个工作日 16:00 ET 发布,折日历日约 10-12 天,跨假日更长),首次系统验证 `event_table_to_asof_panel` 的 `visible_delay_days` 机制
- **新鲜度截断** (覆盖偏稀股票可能整月无新报告),首次系统验证 `max_staleness_days` 机制

经济含义 (写给 reviewer): 做空者信息含量高 (Boehmer/Jones/Zhang 2008), short_interest_ratio 越高横截面预期下个月收益越**负**; 极端值反向可能触发逼空。本任务只产因子, 不做信号方向预设, 由 evaluate.py 的 rank-IC 自行决定。

## 作用域

### 新增文件

| 文件 | 内容 |
|---|---|
| `research/short_interest.py` | SI 事件流 loader + asof 比率面板 compute + 一站式 load |
| `research/factors/builtins/short_interest.py` | `ShortInterestFactor` builtin + 顶层 `register(...)` |
| `tests/test_short_interest.py` | `research/short_interest.py` 的纯单元测试 (合成 DF, 不连库) |
| `tests/test_short_interest_factor_builtin.py` | `ShortInterestFactor` 的 monkeypatch 单元测试 (独立文件,避开 `tests/test_factors_builtins.py` 的 module-level integration mark) |
| `tests/test_short_interest_integration.py` | 1 个 `@pytest.mark.integration` 测试,用 conftest `pg_db` fixture 跑真实 schema(参考 `tests/test_research_market_cap.py::test_load_shares_events_against_real_schema`) |

### 修改文件

| 文件 | 改动 |
|---|---|
| `research/evaluate.py` | 顶部追加一行 `from research.factors.builtins import short_interest as _short_interest  # noqa: F401`(触发 builtin 注册,与 size / earnings_yield 同模式)。其他逻辑一行不动 |

### 不动文件 (硬约束)

`research/factors/protocol.py`、`research/factors/asof.py`、`research/market_cap.py`、`research/factors/builtins/{size,earnings_yield}.py`、`tests/test_factors_builtins.py`、`tests/test_factors_protocol.py` —— **一字不动**。

## 契约

### `research/short_interest.py`

**镜像 `research/market_cap.py` 模式**: 暴露 `load_short_interest_events` + `compute_short_interest_ratio_panel` + `load_short_interest_ratio_panel`。

**允许**: `from research.market_cap import load_shares_events`(复用 shares 事件 loader,这是因子层依赖数据层、且 share 事件 PIT 语义已锁定的合理依赖)。

**禁止**: import `research.market_cap` 的其他函数(`compute_market_cap_panel` / `load_market_cap_panel` / 任何 `_load_raw_close_wide` 私有 helper)。

```python
def load_short_interest_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 short_interests 的 PIT 可见事件流。

    返回列(必须 typed-empty 也保留): security_id (int64),
        visible_date (datetime64[ns]),
        settlement_date (datetime64[ns]),
        short_interest (int64)

    空结果约定: security_ids=[] 立即返回 typed-empty DataFrame(列 dtype 同上),
                不发 SQL; 与 `research.market_cap._empty_shares_events()` 同模式。
    """
```

SQL 形态(`DISTINCT ON` 保证同 (security_id, settlement_date) 多 source / 多次重导只胜出最新插入行):
```sql
select distinct on (security_id, settlement_date)
       security_id,
       settlement_date as visible_date,
       settlement_date,
       short_interest
from short_interests
where short_interest is not null
  and (:security_ids::bigint[] is null or security_id = any(:security_ids))
order by security_id, settlement_date, created_at desc, id desc
```

**注意 `visible_date` 与 `settlement_date` 同列双绑**: `settlement_date` 同时承担"事件可见基线"和"staleness anchor"两个角色(因为 FINRA SI 报告无中间 `period_end` 概念,settlement 即是发布事实日)。`visible_delay_days` 由调用者注入,**不在 SQL 里加减**。

```python
def compute_short_interest_ratio_panel(
    events: pd.DataFrame,
    shares_events: pd.DataFrame,
    dates: pd.DatetimeIndex,
    *,
    visible_delay_days: int,
    si_max_staleness_days: int,
    shares_max_staleness_days: int,
) -> pd.DataFrame:
    """合成 SI 事件与 shares 事件, 计算 short_interest / total_shares 比率宽表。

    分子 / 分母独立 asof 化:
    - 分子: events 直接传 event_table_to_asof_panel,
            visible_date_column='visible_date',
            staleness_anchor_column='visible_date'(即 settlement_date),
            visible_delay_days=visible_delay_days,
            max_staleness_days=si_max_staleness_days。
    - 分母: shares_events 传 event_table_to_asof_panel,
            visible_date_column='visible_date'(filing_date),
            staleness_anchor_column='visible_date',
            visible_delay_days=0(filing_date 即可见日,无额外滞后),
            max_staleness_days=shares_max_staleness_days。

    universe = 分子 events.security_id ∪ 分母 shares_events.security_id (sorted int64)。
    任一面板某 (date, security_id) 缺值 -> 输出 NaN, 不要 0 / sentinel。
    分母 ≤ 0 -> 输出 NaN (不要 +∞)。
    输出 dtype 锁定 float64。

    contract: shares_events 由调用方负责传入 typed-clean 数据
              (`load_shares_events` 已保证 non-null + dtype)。compute 不对 shares 做 NaN/NaT 过滤,
              只对自身分子 events 做 `pd.notna(security_id, visible_date, short_interest)` 三键过滤。
    """
```

```python
def load_short_interest_ratio_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    visible_delay_days: int = 14,
    si_max_staleness_days: int = 30,
    shares_max_staleness_days: int = 400,
) -> pd.DataFrame:
    """一站式加载, 返回 short_interest_ratio 宽表 (index=dates, columns)。

    列契约:
    - security_ids is None: columns = sorted(SI events.security_id ∪ shares events.security_id)
    - security_ids 非空 list: columns = pd.Index(security_ids, dtype=int64) 原序保留,
      请求中无任何数据的 ID 该列全 NaN(不要 drop)。
    - security_ids = []: columns 为 empty Int64Index, 不发任何 SQL。

    实现内部: `from research.market_cap import load_shares_events` 复用 shares loader。
    """
```

**关键默认值** (写在 spec 里,Codex 不要自创):

| 参数 | 默认 | 依据 |
|---|---|---|
| `visible_delay_days` | **14** (日历日) | FINRA 在 settlement_date 后的 `BD+8` 16:00 ET 发布 SI;BD+8 折日历日典型 10-12 天,跨美国假日(感恩节/圣诞节) 可达 13-14 天;再加 1 天 buffer 覆盖"16:00 ET 发布晚于美股 close,当日 PIT 不可用"。`visible_delay_days=14` 是保守 BD+8 + same-day publish 缓冲。**未来若需要精确口径**: caller 可在 loader 外用 `utils.trading_calendar` 计算 BD+8 实际 publication date 作为 events.visible_date,并将本参数置为 0。本任务**不**实现这条路径(scope 控制)。|
| `si_max_staleness_days` | **30** (日历日) | 半月发布,正常 14-16 天一更; 30 天没新数据 = vendor 真的漏了或股票退市边缘,置 NaN 安全 |
| `shares_max_staleness_days` | **400** | 与 `load_market_cap_panel` 一致 (年报频率 + 安全冗余),已 grep 确认 |

依赖关系: `compute_short_interest_ratio_panel` **必须** import `research.factors.asof.event_table_to_asof_panel` 直接用,**不要**在内部重复实现 merge_asof 逻辑。

### `research/factors/builtins/short_interest.py`

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.short_interest import load_short_interest_ratio_panel


@dataclass(frozen=True)
class ShortInterestFactor:
    name: ClassVar[str] = "short_interest_ratio"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_short_interest_ratio_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(ShortInterestFactor())
```

**禁止**在 `compute` 里给 loader 传 `as_of` (协议 #6b 反需求)。
**禁止**在 builtin 里写参数 (visible_delay_days 等), 用 loader 默认值; 想覆盖默认值时由调用方先调 loader、再包成自定义因子。

### `research/evaluate.py` 修改

仅追加一行(位置在与 `size` / `earnings_yield` import 同区):

```python
from research.factors.builtins import short_interest as _short_interest  # noqa: F401
```

**不**改 `evaluate.py` 任何其他行;**不**在 `_REGISTRY` 上再做任何手动注册操作 (builtin 顶层 `register(...)` 已搞定)。

## 测试

### `tests/test_short_interest.py` (纯单元, 不连库)

合成 DataFrame 喂 compute,锁定语义。所有 monkeypatch 目标用 `research.short_interest.*`(绑定在本模块里的别名,不是 `research.market_cap.*`)。

1. `test_compute_basic`: 构造 2 只 security × 3 settlement_date 的 SI 事件 + 对应 shares 事件, dates 覆盖前后, 断言比率值 = SI / shares 且面板 shape 正确 + 输出 dtype 锁定 `float64`。
2. `test_visible_delay_pushes_visibility`: SI settlement=2026-01-15, visible_delay=14, dates 含 2026-01-22 (delay 期内) 与 2026-01-29 (delay 期满后)。断言 2026-01-22 处该证券值为 NaN, 2026-01-29 处有值。
3. `test_si_staleness_truncates`: SI 事件 settlement=2026-01-15(visible 后即 2026-01-29 起可见), si_max_staleness_days=30, dates 含 2026-01-29 / 2026-02-14 / 2026-02-15。断言 2026-01-29 有值(0 天 stale)、2026-02-14 有值(30 天 stale 内)、2026-02-15 为 NaN(31 天)。**(此用例必须基于 visible_date 后的 dates,且 staleness anchor = visible_date,使数学闭合)**。
4. `test_shares_staleness_independent`: 分母 shares 过期但分子 SI 未过期 → 分子值有但比率 NaN (因为分母 NaN)。
5. `test_zero_or_negative_shares_returns_nan`: 分母 = 0 → 比率 NaN, 不报 ZeroDivisionError 也不返 +∞。验证 dtype 仍 float64。
6. `test_empty_events`: SI 事件全空, shares 事件有 → 比率宽表 universe = shares ID, 全 NaN。SI 与 shares 都空 → universe 为空 Int64Index, columns dtype 为 int64。
7. `test_universe_is_union`: SI 出现 [1,2], shares 出现 [2,3] → universe = [1,2,3] sorted。dtype 锁定。
8. **NaN 陷阱用例 (CLAUDE.md 硬规则)**: SI events 中 `short_interest=np.nan` 的行 → 不报 InvalidOperation, 不进面板; shares_events 不做这种过滤(契约前置保证)。
9. `test_loader_security_ids_filter`: monkeypatch `research.short_interest.load_short_interest_events` 和 `research.short_interest.load_shares_events`(已被 `from research.market_cap import load_shares_events` 绑入本模块,故 monkeypatch 路径在 short_interest 命名空间), 喂 SI=[1,2], shares=[2,3]:
   - `security_ids=None` → universe=[1,2,3]
   - `security_ids=[2, 999]` → universe=[2,999], 999 列全 NaN
   - `security_ids=[]` → universe 为空, 不调任何 loader
10. `test_compute_empty_dates`: dates=empty → 返回 empty DataFrame, index dtype datetime64[ns], columns 仍为 universe int64。
11. `test_compute_multiple_si_same_security_orders_by_visible_date`: 同 security 2 个 settlement_date, dates 在两者中间。断言用较早 settlement 的值(asof backward + within staleness)。

### `tests/test_short_interest_factor_builtin.py` (新文件,纯单元)

完全独立于 `tests/test_factors_builtins.py`(后者顶层挂 `pytestmark = pytest.mark.integration`,会被验收命令过滤)。

1. `test_short_interest_factor_returns_panel_shape`: monkeypatch `research.factors.builtins.short_interest.load_short_interest_ratio_panel` 返回构造面板。`ctx.security_universe = pd.Index([10, 20, 30], dtype="int64")`, loader 返回的 panel columns=[10, 20]。断言 compute 返回 shape == (len(dates), 3), columns=[10, 20, 30](30 列全 NaN), dtype=float64。
2. `test_short_interest_factor_registered`: `from research.factors.protocol import get; from research.factors.builtins import short_interest as _trigger`(触发 register), `get("short_interest_ratio")` 返回 `ShortInterestFactor` 实例。
3. `test_short_interest_factor_does_not_pass_as_of_to_loader`: monkeypatch loader, ctx.as_of 设为非 None, 断言 loader 被调用时 kwargs 不含 `as_of`(锁 #6b 反需求)。

### `tests/test_short_interest_integration.py` (新文件, `@pytest.mark.integration`)

仿 `tests/test_research_market_cap.py::test_load_shares_events_against_real_schema` 模式,只 1 个测试,目标是锁定 SQL 在真实 schema 上能跑、列名/dtype/parse_dates 正确:

```python
@pytest.mark.integration
def test_load_short_interest_events_against_real_schema(pg_db):
    # insert: 1 security, 2 short_interests 行 (不同 settlement_date),
    #         + 1 行同 settlement 不同 source (验证 DISTINCT ON 胜出规则)
    # 调 load_short_interest_events, 断言:
    #   - 列名 / dtype 与 typed-empty 一致
    #   - DISTINCT ON 在重复 (sid, sdate) 上胜出 created_at desc
    #   - security_ids=[id] 过滤行为正确
    #   - security_ids=[] 直接返回 typed-empty, 不发 SQL
```

**测试**禁止**真连生产库** (用 conftest `pg_db` fixture 的临时集群)。

## 验收

下列命令在主管机器上全绿,且无 warning 新增:

```bash
# 1. 本任务新增的纯单元测试
.venv/bin/python -m pytest tests/test_short_interest.py tests/test_short_interest_factor_builtin.py -q

# 2. 本任务新增的集成测试
.venv/bin/python -m pytest tests/test_short_interest_integration.py -q -m integration

# 3. 全量纯单测 (基线 + 本任务) 不退化
.venv/bin/python -m pytest tests/ -q -m "not integration"

# 4. 全量含 integration (基线 + 本任务) 不退化
.venv/bin/python -m pytest tests/ -q
```

主管验收时还会跑(Codex 不必跑):

```bash
# 真连库 spot-check (主管手动, 不在自动验收里; 仅参考)
.venv/bin/python -c "
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from research.data import research_engine
from research.short_interest import load_short_interest_ratio_panel

engine = research_engine()
dates = pd.date_range('2026-05-01', '2026-06-10', freq='B')
panel = load_short_interest_ratio_panel(engine, dates=dates, security_ids=None)
print(f'shape={panel.shape}, nonnull pct={panel.notna().mean().mean():.1%}')
print(panel.iloc[:, :3].dropna(how=\"all\").tail())
"

# CLI 注册验证 (确认 evaluate.py import 生效)
.venv/bin/python -c "
import research.evaluate  # noqa: F401
from research.factors.protocol import list_factors
print(list_factors())  # 应含 'short_interest_ratio'
"
```

## 反需求 (明令禁止)

1. **不**改 `research/factors/protocol.py`、`research/factors/asof.py`、`research/market_cap.py`、`research/factors/builtins/{size,earnings_yield}.py` 任何字符。
2. **不**改 `tests/test_factors_builtins.py`、`tests/test_factors_protocol.py`(避免触动现有 integration mark 语义,新测试一律放新文件)。
3. **不**写 alembic 迁移 (本任务零 schema 变化)。
4. **不**改 `scheduled_update` / `main.py` 调度 (short_interests 已有 `update_massive_short_data` 在调度里)。
5. **不**新增依赖。坚持 pandas/numpy/sqlalchemy + 已 import 的 stdlib。
6. **不**写多行 docstring; 单行中文说明 why 即可。
7. **不**加 `# 已废弃` / 类似的注释; 用不上的代码直接不写。
8. **不**写"signal direction"或多空假设到代码或测试里; 因子返回比率,符号交给 evaluate.py。
9. **不**用 `is None` 检查 pd.read_sql_query 出来的 object 列 (CLAUDE.md NaN 陷阱)。
10. **不**给 builtin compute 传 `as_of` 给 loader (#6b 反需求, 测试 `test_short_interest_factor_does_not_pass_as_of_to_loader` 锁定)。
11. **不**在因子里写 0 / sentinel 替换 NaN; 缺数据就 NaN。
12. **不**做 winsorize / clip / log / zscore / rank 任何统计变换; 返回原始 ratio,变换交给 evaluate.py。
13. **不**用 `ticker` / `symbol` 做 join 键; 全程 `security_id`(CLAUDE.md 核心规则)。
14. **不**消费 `avg_daily_volume` / `days_to_cover` / `short_volume` 等列;本任务**只**实现 `short_interest / total_shares`,其他做空因子是 sibling 子单。
15. **不**在 loader 内引入 `utils.trading_calendar` 计算 BD+8(scope 控制;默认参数 14 日历日已说明保守口径,精确口径留给后续 PR)。
16. **不**在 loader 的 SQL 里加减 `visible_delay_days`(让 asof helper 统一处理时间口径,避免数据库与 Python 两边算时间)。

## 实施提示

- 起 PR 前本机跑完所有四条验收命令,全绿
- commit 顺序建议:
  1. `feat(research): add short_interest_ratio loader with PIT semantics`
  2. `feat(factors): add short_interest_ratio builtin factor + wire into evaluate`
  3. `test(short-interest): cover loader staleness / visible-delay / edge cases + integration schema check`
- sandbox 限制 (`.git/refs/heads/feat/` mkdir 被拒,`.git/index.lock` 创建被拒,新依赖网络被拒) 已知; 不要硬扛, 保留 working tree 留给主管代 commit
- 规格矛盾立即停手 + `git restore` (硬规则, 参 codex_tasks/README)
- 集成测试用 `pg_db` fixture 时,注意 `created_at` 是 `server_default=func.now()`,要触发 DISTINCT ON 顺序差异需要 `time.sleep(0.01)` 或显式 `created_at=` 注入
