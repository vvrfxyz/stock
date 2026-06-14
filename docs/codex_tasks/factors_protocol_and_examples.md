# Task #6b — 因子协议 + 注册表 + 示例因子

**来源**: `docs/audits/2026-06-13_deep_review_and_roadmap.md` 路线 Now 第 6 项的后半

**依赖**: #6a (factors_asof_generic) 必须**先 merge** — 本任务直接 import `research.factors.asof.event_table_to_asof_panel`。

## 背景

报告原话: "**因子统一 compute(ctx)→宽表协议+注册表**"。

任务: 定义 1 个**最小可用的因子协议**,任何因子都遵守同样的契约,可以**统一对接** evaluate.py (Now 第 7 项)。再写 2 个示例因子证明协议工作 + 通过测试锁定行为。

不在这里做的:
- evaluate.py (那是任务 #7,基于因子协议算 rank-IC / 分位组合)
- 真实的因子计算逻辑深度(13F holdings breadth、insider buy/sell 比 — 都是后续 Next 任务)
- 因子组合 / 中性化的 wiring(那是 Next 期 backtest 升级要做的)

## 作用域

### 新增文件

```
research/factors/protocol.py      # 协议: Context dataclass + Factor protocol + 注册表
research/factors/builtins/__init__.py    # 空,空 __all__
research/factors/builtins/size.py        # 示例因子 #1: log market cap (size)
research/factors/builtins/earnings_yield.py    # 示例因子 #2: TTM earnings / market_cap
tests/test_factors_protocol.py    # 协议本身的单元测试
tests/test_factors_builtins.py    # 示例因子的单元测试
```

### 修改文件

无。`research/factors/__init__.py` 在 #6a 已建,**不要再动**(尤其不要 re-export builtins)。

## 契约

### Context dataclass

```python
# research/factors/protocol.py
from dataclasses import dataclass
import pandas as pd
from sqlalchemy.engine import Engine

@dataclass(frozen=True)
class FactorContext:
    """因子计算所需的"防未来"运行时上下文。所有因子从这里取数,统一 PIT 边界。"""
    engine: Engine                       # 只读 PG 连接
    dates: pd.DatetimeIndex              # 调仓日序列(纳秒)
    security_universe: pd.Index          # int64 column index — 因子输出必须用这个 columns
    as_of: pd.Timestamp | None = None    # 可选: 全局可见性边界,默认 None = dates 最后一天
```

**为什么 frozen**: 因子不能改 context(防止串扰 + 副作用)。
**为什么 security_universe 在 context 里**: 因子输出宽表必须等于 universe(让 evaluate.py 统一对齐); 同时 universe 是 evaluate.py 决定的(可能是 SPX, R3K, 或自定义),不是因子自己挑。

### Factor protocol

```python
# research/factors/protocol.py
from typing import Protocol

class Factor(Protocol):
    """因子契约: name + compute。compute 是纯函数(同 ctx 同 output)。"""
    name: str                            # 注册表 key, e.g. "size", "earnings_yield"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        """返回 wide panel: index=ctx.dates, columns=ctx.security_universe, dtype=float64。
        缺数据用 NaN(不要 0,不要 sentinel value)。
        必须 PIT-correct: as-of date 之后的信息不能泄漏。
        """
        ...
```

### 注册表

```python
# research/factors/protocol.py
_REGISTRY: dict[str, Factor] = {}

def register(factor: Factor) -> Factor:
    """登记因子。如果同名已存在 → 抛 ValueError(防止 typo 静默覆盖)。"""
    if factor.name in _REGISTRY:
        raise ValueError(f"factor {factor.name!r} already registered")
    _REGISTRY[factor.name] = factor
    return factor

def get(name: str) -> Factor:
    """按名取因子。不存在 → KeyError。"""
    return _REGISTRY[name]

def list_factors() -> list[str]:
    """所有已注册因子名(sorted)。"""
    return sorted(_REGISTRY)
```

**注意**: `_REGISTRY` 是模块级状态,**单进程内全局唯一**。新增 builtin 时,builtin 模块自己调 `register(...)` 注册。

### 示例因子 #1: size

```python
# research/factors/builtins/size.py
import numpy as np
import pandas as pd
from dataclasses import dataclass
from research.factors.protocol import Factor, FactorContext, register
from research.market_cap import load_market_cap_panel

@dataclass(frozen=True)
class SizeFactor:
    name: str = "size"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        mcap = load_market_cap_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist() if ctx.security_universe is not None else None,
        )
        log_mcap = np.log(mcap.where(mcap > 0))
        return log_mcap.reindex(index=ctx.dates, columns=ctx.security_universe).astype(np.float64)

register(SizeFactor())
```

**Why log mcap = "size"**: Fama-French convention. 高 size = 大盘股, 低 size = 小盘股。负号是消费端的事(SMB 想要 small,翻号在 evaluate)。

### 示例因子 #2: earnings_yield

```python
# research/factors/builtins/earnings_yield.py
import pandas as pd
from dataclasses import dataclass
from research.factors.protocol import Factor, FactorContext, register
from research.fundamentals import load_fundamental_panel
from research.market_cap import load_market_cap_panel

@dataclass(frozen=True)
class EarningsYieldFactor:
    name: str = "earnings_yield"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        ids = ctx.security_universe.tolist() if ctx.security_universe is not None else None
        fundamentals = load_fundamental_panel(
            ctx.engine, dates=ctx.dates, metrics=("net_income_ttm",), security_ids=ids,
        )
        ni = fundamentals["net_income_ttm"]
        mcap = load_market_cap_panel(ctx.engine, dates=ctx.dates, security_ids=ids)
        ratio = ni / mcap.where(mcap > 0)
        return ratio.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")

register(EarningsYieldFactor())
```

**Why this pairing**: size 是市场原料(无基本面依赖);earnings_yield 是基本面 × 市值组合 — 这两个因子触及到 #6 完整管线(asof_panel + market_cap),证明协议 + builtin 工作。

## 测试

### `tests/test_factors_protocol.py` (纯单元,无 DB)

1. `test_register_and_get` — 注册 mock factor,`get(name)` 取回
2. `test_register_duplicate_raises` — 同名 register 2 次 → ValueError
3. `test_list_factors_sorted` — 注册多个 factor → list 返回 sorted 名字
4. `test_factor_context_is_frozen` — 试图改 ctx 字段 → FrozenInstanceError
5. `test_factor_protocol_compliance` — 用 typing.runtime_checkable 检查 SizeFactor 和 EarningsYieldFactor 是否实现协议(运行时检查 `isinstance(SizeFactor(), Factor)` 真的成立)

**注**: `_REGISTRY` 是全局状态 — 测试间会污染。tests 必须用 monkeypatch 或 fixture **重置 _REGISTRY**(eg. autouse fixture: `_REGISTRY.clear()` setup + 重新 import builtins)。

### `tests/test_factors_builtins.py` (集成,用 pg_db)

1. `test_size_factor_against_synthetic_panel`(integration) — 插 3 个 securities + 各自 shares + prices, 调 `size.compute(ctx)`,assert log_mcap 计算正确,shape = (len(dates), 3),负市值/零市值 → NaN
2. `test_earnings_yield_factor_against_synthetic_panel`(integration) — 类似,assert ni/mcap 正确,某行 mcap=0 → ratio=NaN
3. `test_factor_outputs_match_universe_columns` — universe=[1,2,3,999] 但 999 无数据 → column 999 全 NaN(不 drop 不报错)

## 验收

```bash
# 1. 协议单元
python -m pytest tests/test_factors_protocol.py -q -m "not integration"

# 2. builtin 集成
python -m pytest tests/test_factors_builtins.py -q

# 3. 全套无回归 — 至少 291 + 8 = 299 passed
python -m pytest tests/ -q
```

测试至少 8 个 passed(5 protocol + 3 builtins)。

## 反需求

1. **不要**写 evaluate.py — 那是任务 #7
2. **不要**写中性化逻辑 — 那是 Next 期
3. **不要**做行业 / 市值之外的因子 — 2 个示例就够,证明协议工作
4. **不要**改 fundamentals / market_cap 的实现 — 它们的 API 在 #6a 已经稳定
5. **不要**改 research/factors/asof.py(#6a 的产物)— 你的 builtin 只**消费**它,不动它
6. **不要**注册到全局 `__init__.py`,不要做 magic auto-discovery — 显式 import 每个 builtin 子模块即可
7. **不要**用 `cls()` 或 metaclass 玩花 — `@dataclass(frozen=True)` 就够了
8. **不要**引入 abstract base class —`typing.Protocol` 足够,不强制继承
9. **不要**在因子内部捕异常然后 swallow — 让上层处理

## 实现建议

- `register()` 可以做成装饰器或函数调用。文档建议用函数调用(`register(SizeFactor())`)— 简单显式
- `FactorContext` 用 `dataclass(frozen=True)` 后是 hashable,可以做 LRU cache key (未来 evaluate.py 用)
- builtin 模块在 import 时调 `register(...)` 注册 —— 这意味着用户必须先 import 才能 get,**这是 deliberate**(防止 dead code 偷偷拖慢启动)

## 后续依赖

#6b 之后:
- **任务 #7 evaluate.py** 用 `protocol.get(name).compute(ctx)` 算因子值,然后做 rank-IC / 分位组合
- **任务 Next: 13F / short / insider 因子** 都加 `research/factors/builtins/<name>.py`,共享同一协议
- **任务 #8 中性化** 写 `research/factors/neutralize.py`,接收 panel + industry/size,产出残差 — 协议外 utility

## 工作时长估算

6-10 小时(协议轻,builtin 都简单,但测试要锁全局状态污染)。
