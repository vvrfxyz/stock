"""composite_v1 复合信号注册因子（骨架定案：low_vol + high_52w 残差 + size）。

定案出处：`docs/research_ledger.md` 因子裁决表 composite_v1 行——2026-07-07 size
关卡重审 PASS（594569c），breadth 弃用（是 size 马甲），骨架固化为
**low_vol + high_52w + size**。此前信号只内联在 `research/composite_study.py` 的秩
聚合里，`retail_reality_study.get(args.factor)` 与 `evaluate --factors` 都无法消费；
本模块把定案骨架抽为注册 builtin，成分**写死**（不做可配置参数——防 composite_study
的实验性 --components 旧默认污染唯一可用候选）。composite_study 保留为实验场，其
默认 COMPONENTS 已同步订正、复合构造复用本模块的共享函数（`build_composite`）以保
位级一致。

信号构造（与 composite_study 位级一致，共享 `residualize_high_52w`/`combine_ranks`）：
1. eligible 内排名——成分各自 `.where(eligible)`（掩到 eligible ∧ 有值）后
   `.rank(axis=1, pct=True)`（横截面百分位秩），保证 [0,1] 刻度不被不可交易名字拉伸；
2. high_52w 逐日横截面 OLS 残差化——对 low_vol 秩逐日回归、残差当日重排回 [0,1]
   （无前视，每日独立），剥掉 high_52w 与 low_vol 的共线部分；
3. 0.5 中性填补——composite = (Σ可得秩 + 0.5×缺失数) / k_total（k_total=3），
   缺失当中性 0.5 而非当因子（诚实的方差缩减，不让"缺测量"系统性挤占极端分位）；
4. low_vol 在场门——主干 low_vol 秩缺失的行整格置 NaN。
   注意残差化会把 low_vol 的 NaN 传导进 high_52w 的可得性（残差在 low_vol 或
   high_52w 任一缺失处为 NaN），k 计数因此以残差化后的可得性为准——与 composite_study
   一致。

元属性推导：
- lookback_days = 成分最大回看 = high_52w 的 252（low_vol=63、size=0）；
- lag_days = 1（成分皆 t 收盘即得）；
- adr_unsafe = True——成分含 size（市值/股本口径），ADR 的 ADS/公司股本混杂，
  evaluate 层会对 ADR 列置 NaN（§E.3）；只要有一个 adr_unsafe 成分，复合分即不安全。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Mapping, Sequence

import numpy as np
import pandas as pd

from research.backtest import eligibility_mask
from research.data import securities_with_uncovered_events
from research.factors.price_cache import raw_bar_panels
from research.factors.protocol import FactorContext, get, register

# 定案骨架成分（写死，防实验性默认污染生产口径）。低波动是主干（在场门）。
COMPONENTS: tuple[str, ...] = ("low_vol", "high_52w", "size")
_BASE_COMPONENT = "low_vol"
_RESIDUALIZED_COMPONENT = "high_52w"
# eligible 复刻与 composite_study 同：raw close/volume 面板前推 200 日暖机
# （eligibility_mask 的 63 日滚动中位成交额需暖机段）。
_ELIGIBILITY_BUFFER_DAYS = 200


def rowwise_ols_residual_rank(y_rank: pd.DataFrame, x_rank: pd.DataFrame) -> pd.DataFrame:
    """逐日横截面 OLS：y = a + b·x + e，残差当日重排回 [0,1]。无前视。

    从 composite_study 抽出的共享实现——两处口径的单一事实源。
    """
    y, x = y_rank.to_numpy(), x_rank.to_numpy()
    valid = ~np.isnan(y) & ~np.isnan(x)
    ym, xm = np.where(valid, y, np.nan), np.where(valid, x, np.nan)
    n = valid.sum(axis=1).astype("float64")
    with np.errstate(invalid="ignore", divide="ignore"):
        mx = np.nansum(xm, axis=1) / n
        my = np.nansum(ym, axis=1) / n
        dx, dy = xm - mx[:, None], ym - my[:, None]
        beta = np.nansum(dx * dy, axis=1) / np.nansum(dx * dx, axis=1)
        resid = dy - beta[:, None] * dx
    out = pd.DataFrame(resid, index=y_rank.index, columns=y_rank.columns)
    return out.rank(axis=1, pct=True)


def eligible_component_ranks(
    ctx: FactorContext, eligible: pd.DataFrame, names: Sequence[str]
) -> dict[str, pd.DataFrame]:
    """各成分 compute -> eligible 掩码 -> 横截面百分位秩（先掩后排名）。"""
    ranks: dict[str, pd.DataFrame] = {}
    for name in names:
        panel = get(name).compute(ctx).where(eligible)
        ranks[name] = panel.rank(axis=1, pct=True)
    return ranks


def residualize_high_52w(
    ranks: Mapping[str, pd.DataFrame], names: Sequence[str]
) -> dict[str, pd.DataFrame]:
    """把 high_52w 秩替换为对 low_vol 秩的逐日残差秩（若 high_52w 在成分集）。

    返回新 dict，不原地改调用方的 ranks。
    """
    out = dict(ranks)
    if _RESIDUALIZED_COMPONENT in names:
        out[_RESIDUALIZED_COMPONENT] = rowwise_ols_residual_rank(
            out[_RESIDUALIZED_COMPONENT], out[_BASE_COMPONENT]
        )
    return out


def combine_ranks(ranks: Mapping[str, pd.DataFrame], names: Sequence[str]) -> pd.DataFrame:
    """0.5 中性填补聚合 + low_vol 在场门。ranks 须为**已残差化**的成分秩。

    composite = (Σ可得秩 + 0.5×缺失数) / k_total，随后主干 low_vol 缺失的行置 NaN。
    """
    names = list(names)
    assert _BASE_COMPONENT in names, f"主干 {_BASE_COMPONENT} 必须在打分集"
    base = ranks[_BASE_COMPONENT]
    stack = np.stack([ranks[n].to_numpy() for n in names])          # (k, T, N)
    available = ~np.isnan(stack)
    k_total = float(len(names))
    composite_vals = (
        np.nansum(np.where(available, stack, 0.0), axis=0)
        + 0.5 * (k_total - available.sum(axis=0))
    ) / k_total
    composite = pd.DataFrame(composite_vals, index=base.index, columns=base.columns)
    return composite.where(base.notna())


def build_composite(ranks: Mapping[str, pd.DataFrame], names: Sequence[str]) -> pd.DataFrame:
    """完整复合构造：high_52w 残差化 -> 0.5 填补聚合 -> low_vol 在场门。

    输入为 eligible 掩码 + 排名后的成分秩（`eligible_component_ranks` 的产物）。
    composite_study 与注册因子共用本函数，位级一致由此保证。
    """
    return combine_ranks(residualize_high_52w(ranks, names), names)


def composite_eligibility(ctx: FactorContext) -> pd.DataFrame:
    """复刻 composite_study 的 eligible：raw close/成交额双门 + 未覆盖事件 gate。

    输出对齐 ctx.dates × ctx.security_universe（缺列填 False），供成分秩掩码。

    口径声明（审核 #5，2026-07-08）：本 gate 的门槛**写死** eligibility_mask 默认
    （min_price=3 / 2M / 63 日窗），不随 evaluate 的 --min-price/--min-median-dollar-volume
    旗标变化——evaluate 层掩码与因子内 NaN 取交集，收紧生效、放宽无效（放宽门槛
    对本因子是 no-op，但仍进 params_hash）。uncovered gate 窗口为 ctx.dates 首尾
    （evaluate 面板层的同名 gate 用 buffered end，窗口边缘 (end, end+buffer] 的
    未覆盖事件两层判定可能不同）。保持与 composite_study 位级同源优先于与 evaluate
    门槛联动——改动此处必须两边同改。
    """
    security_ids = ctx.security_universe.tolist()
    bars = raw_bar_panels(
        ctx.engine, dates=ctx.dates, security_ids=security_ids,
        columns=("close", "volume"), buffer_days=_ELIGIBILITY_BUFFER_DAYS,
    )
    close = bars["close"]
    eligible = eligibility_mask(close, close * bars["volume"]).loc[ctx.dates]
    bad = securities_with_uncovered_events(
        ctx.engine, start=ctx.dates[0].date(), end=ctx.dates[-1].date()
    )
    if bad:
        eligible = eligible & ~pd.Series(ctx.security_universe.isin(bad), index=ctx.security_universe)
    # reindex 补列引入 NaN 会把 bool 列升成 object，fillna 后仍是 object——显式收回
    # bool，防下游布尔代数静默异型（测试 test_universe_extra_column_filled_false 锁定）。
    return (
        eligible.reindex(index=ctx.dates, columns=ctx.security_universe)
        .fillna(False)
        .astype(bool)
    )


@dataclass(frozen=True)
class CompositeV1Factor:
    name: ClassVar[str] = "composite_v1"
    lookback_days: ClassVar[int] = 252   # = max(high_52w 252, low_vol 63, size 0)
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    # 成分含 size（市值/股本口径）→ ADR 的 ADS/公司股本混杂，禁入直至归一化（§E.3）。
    adr_unsafe: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        eligible = composite_eligibility(ctx)
        ranks = eligible_component_ranks(ctx, eligible, COMPONENTS)
        composite = build_composite(ranks, COMPONENTS)
        return composite.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(CompositeV1Factor())
