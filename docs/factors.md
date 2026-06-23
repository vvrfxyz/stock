# 因子库 (`research/factors/`)

本文档描述研究层因子库的架构、现有因子目录、以及如何新增因子。

## 架构概览

```
research/
├── factors/
│   ├── protocol.py          # Factor 协议 + 全局注册表
│   ├── asof.py              # event_table_to_asof_panel — 通用 PIT 事件→宽表工具
│   └── builtins/            # 内置因子（每个文件一个因子，import 即注册）
│       ├── size.py
│       ├── earnings_yield.py
│       ├── short_interest.py
│       └── short_volume.py
├── evaluate.py              # 因子评估引擎（rank-IC / Newey-West t / 分位回报 / Sharpe）
├── _trials_store.py         # 评估结果持久化（trials.parquet）
├── short_interest.py        # PIT short_interest_ratio 数据加载
├── short_volume.py          # PIT short_volume_ratio 数据加载
├── fundamentals.py          # PIT 基本面面板（TTM / 时点指标）
├── market_cap.py            # PIT 市值面板（close × total_shares）
├── data.py                  # 复权价格面板 + 因子事件加载
├── backtest.py              # 向量化回测引擎
├── strategies.py            # 技术基线策略（动量 / 反转 / 趋势）
└── run_baselines.py         # 技术基线入口脚本
```

### 核心抽象

**Factor 协议** (`protocol.py`)：

```python
@runtime_checkable
class Factor(Protocol):
    name: ClassVar[str]
    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        """返回 index=dates, columns=security_ids 的 float64 宽表。"""
        ...
```

**FactorContext** 包含：
- `engine` — SQLAlchemy Engine（连生产库）
- `dates` — 目标日期序列
- `security_universe` — 目标证券 ID 集合
- `as_of` — PIT 截止日（可选，用于模拟历史时点）

**event_table_to_asof_panel** (`asof.py`)：

将 `(security_id, visible_date, value)` 形式的事件流转换为 PIT 防未来宽表。
核心参数：
- `visible_delay_days` — 从事件日到可见日的延迟天数（模拟披露滞后）
- `max_staleness_days` — 超过此天数未更新的值置 NaN（防止用过时数据）
- `staleness_anchor_column` — staleness 计时的起点列（通常是 `visible_date` 或 `effective_visible_date`）

## 因子目录

| 因子名 | 类 | 数据源 | 频率 | `visible_delay_days` | `max_staleness_days` | 含义 |
|---|---|---|---|---:|---:|---|
| `size` | `SizeFactor` | `historical_shares` x `daily_prices.close` | 日频 | 0 | 400 | log(市值)。经典 Fama-French 规模因子。值越大 = 大盘股 |
| `earnings_yield` | `EarningsYieldFactor` | `sec_fundamental_facts` (NetIncomeLoss TTM) / 市值 | 季频 PIT | 1 | 270 | 盈利收益率（E/P）。值越大 = 更"便宜" |
| `short_interest_ratio` | `ShortInterestFactor` | `short_interests` / `historical_shares.total_shares` | 半月频 | 14 | 30 | 空头仓位占总股本比率。FINRA 半月报，BD+8 后公布 |
| `short_volume_ratio` | `ShortVolumeFactor` | `short_volumes.short_volume` / `total_volume` | 日频 | 1 | 10 | 每日做空成交量占总成交量比率。FINRA T+1 公布 |

注：`earnings_yield` 的 delay 由 `fundamentals.py` 内部 `filed_date` 决定（SEC 申报日即可见日），这里 1 天是默认保守缓冲。

### 因子逻辑白话解释

- **size**：公司多大？收盘价 x 总股本 = 市值，取 log 让分布更对称。小盘股历史上有超额收益（Fama-French SMB）。
- **earnings_yield**：公司赚钱能力值不值当前股价？过去四个季度净利润总和（TTM）/ 市值。高 = 便宜，低 = 贵。严格按 SEC 申报日做 PIT 防偷看。
- **short_interest_ratio**：空头仓位有多拥挤？FINRA 每半月公布一次各股的融券余额，除以总股本得到比率。高 = 空头共识看跌（但也可能引发轧空）。
- **short_volume_ratio**：今天成交里多少是做空？FINRA 每日汇总各交易所的做空成交量和总成交量。跟 `short_interest_ratio` 互补：一个看"存量仓位"，一个看"增量流量"。

## 评估因子

```bash
# 评估单个因子
python -m research.evaluate --factors size --start 2024-05-14

# 评估所有注册因子
python -m research.evaluate --all --start 2024-05-14

# 自定义参数
python -m research.evaluate --factors earnings_yield \
    --start 2024-05-14 --end 2026-06-20 \
    --horizons 1,5,21 \
    --n-quantiles 5 \
    --cost-bps 10 \
    --note "baseline run"

# 不扣无风险利率（复现旧口径）
python -m research.evaluate --all --no-risk-free
```

评估输出：
- **Rank IC**：因子值排名与未来收益排名的 Spearman 相关系数（逐日横截面）
- **Newey-West t**：对 IC 序列做自相关修正后的 t 统计量（|t| > 2 为显著）
- **IC Decay**：不同持有期（horizon）下 IC 的衰减速度
- **分位回报**：按因子值分 5 组，看多空两端的年化收益、Sharpe、IR
- **Trials 持久化**：结果自动存入 `research/output/trials.parquet`，避免重复跑

## 新增因子指南

### 1. 写数据加载模块（如果需要新数据源）

在 `research/` 下新建 `your_data.py`，提供 `load_xxx_events()` 和 `load_xxx_panel()` 两个函数。
遵循已有模式（`short_interest.py` / `short_volume.py`）：

```python
def load_xxx_events(engine, *, security_ids=None) -> pd.DataFrame:
    """返回 (security_id, visible_date, ..., value) 的事件流。"""
    ...

def load_xxx_panel(engine, *, dates, security_ids=None, ...) -> pd.DataFrame:
    """返回 index=dates, columns=security_ids 的 PIT 宽表。"""
    events = load_xxx_events(engine, security_ids=security_ids)
    return event_table_to_asof_panel(events, dates=dates, ...)
```

**注意**：事件流里的列名不要叫 `date`——`asof.py` 内部 `merge_asof` 会跟 grid 的 `date` 列冲突，改用 `trade_date` / `settlement_date` / `period_end_date` 等具体含义的名字。

### 2. 写 builtin 因子

在 `research/factors/builtins/` 下新建文件：

```python
from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.your_data import load_xxx_panel


@dataclass(frozen=True)
class YourFactor:
    name: ClassVar[str] = "your_factor_name"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_xxx_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(YourFactor())
```

### 3. 写测试

在 `tests/test_your_factor.py` 中覆盖：

- **compute 基本功能**：给定事件流，输出形状和值是否正确
- **visible_delay 推迟可见性**：delay 天之前应该是 NaN
- **staleness 截断**：超 `max_staleness_days` 天应该变 NaN
- **NaN 事件忽略**：值为 NaN 的事件行应被跳过，不覆盖之前的有效值
- **空事件 / 空日期**：边界条件
- **loader security_ids 过滤**：monkeypatch 加载函数，验证过滤和 reindex
- **builtin 注册**：`get("your_factor_name")` 能取到

参考 `tests/test_short_volume.py`（11 个用例覆盖上述全部场景）。

### 4. 评估

```bash
python -m research.evaluate --factors your_factor_name --start 2024-05-14
```

## 数据约束

- **回测起始日不早于 2024-05-14**：`computed_adjustment_factors` 只覆盖 ex_date >= 该日的事件（Massive 免费档 730 天窗口）。更早的"复权价"未真正复权。
- **退市股因子缺口**：因子构建只跑 `is_active=True`，退市后有 SPLIT 事件但无因子行的证券须用 `securities_with_uncovered_events` 整体剔除。
- **连库优先 `RESEARCH_DATABASE_URL`**：研究层默认连 253 生产库（只读），避免本地无数据时报错。

## 路线图

近期候选（按数据就绪度排序）：

| 因子 | 数据源 | 形态 | 状态 |
|---|---|---|---|
| `days_to_cover` | short_interests / avg daily volume | 半月频 | 待做 |
| `institutional_breadth` | institutional_holdings (13F) | 季频 | 数据就绪 |
| `delta_institutional_ownership` | institutional_holdings delta IO | 季频 | 数据就绪 |
| `ownership_concentration` | institutional_holdings HHI | 季频 | 数据就绪 |

远期方向：
- PIT 指数成分（iShares ETF 持仓 / EDGAR N-PORT）
- 退市处置事件（delisting_events 表）
- 回测引擎升级（分位多空 -> beta 中性 -> 波动率目标）
- 研究读路径切 Parquet + DuckDB
