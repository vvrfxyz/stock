"""TA 条件化/集成因子（wave-11；family=ta_combo；假设预注册 docs/wave11_hypotheses.md）。

动物园验尸线索：四个反转味指标符号一致（bollinger_b +2.55 / mfi +1.9 / rsi +1.4 /
-macd_hist +2.9~），像同一真反转潜因子的四个高噪声测量——单个不过 Bonferroni，
集成/条件化是把它们拧成一股的两条正路（wave-3 已证量能条件化机制在日内尺度成立）。

组件全部复用 ta_zoo 的构造（同参数、同复权口径），此处只做组合层。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.builtins.ta_zoo import (
    BollingerBFactor,
    MacdHistFactor,
    MfiFactor,
    RsiFactor,
    _panels,
)
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class ReversalEnsembleFactor:
    """H1：四反转测量的横截面秩平均（方差缩减）。全成员有值才计（潜因子口径，
    缺测量的名字不硬凑——与复合打分的 0.5 填补不同，这里测的是潜因子存在性）。"""
    name: ClassVar[str] = "reversal_ensemble"
    lookback_days: ClassVar[int] = 60
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        members = [
            BollingerBFactor().compute(ctx),
            MfiFactor().compute(ctx),
            RsiFactor().compute(ctx),
            -MacdHistFactor().compute(ctx),   # 反号：趋势加速度的反转读法
        ]
        ranks = [m.rank(axis=1, pct=True) for m in members]
        stacked = np.stack([r.to_numpy() for r in ranks])
        out = pd.DataFrame(np.nanmean(stacked, axis=0), index=ctx.dates,
                           columns=ctx.security_universe)
        all_valid = ~np.isnan(stacked)
        return out.where(pd.DataFrame(all_valid.all(axis=0), index=ctx.dates,
                                      columns=ctx.security_universe))


@dataclass(frozen=True)
class VolumeConfirmedReversalFactor:
    """H2：bollinger_b × 异常量（21 日均量倍数 cap 3）。机制：反转收益=流动性
    提供报酬，无量位移无人被迫接盘。wave-3 同构机制的日频迁移。"""
    name: ClassVar[str] = "volume_confirmed_reversal"
    lookback_days: ClassVar[int] = 42
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    vol_window: int = 21
    cap: float = 3.0

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        # 乘法条件化要求零居中：bollinger_b 返回 -pct_b（中性点 -0.5），+0.5 移到
        # 中轨=0——超卖为正、超买为负，放量对称放大偏离（不居中会把超卖乘成更负，
        # 单测逮住的构造 bug）
        centered = BollingerBFactor().compute(ctx) + 0.5
        p = _panels(ctx, buffer_days=90)
        avg_vol = p["volume"].rolling(self.vol_window, min_periods=15).mean()
        abn = (p["volume"] / avg_vol).clip(upper=self.cap).where(avg_vol > 0)
        abn = abn.reindex(index=ctx.dates, columns=ctx.security_universe)
        return centered * abn


@dataclass(frozen=True)
class AtrNormalizedTrendFactor:
    """H4：sma_gap / ATR(21)——超级趋势族的本质（ATR 缩放的趋势偏离）。
    排除 sma_gap 之死的波动混淆解释；低先验，死则整族结案。"""
    name: ClassVar[str] = "atr_trend"
    lookback_days: ClassVar[int] = 71
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    sma_window: int = 50
    atr_window: int = 21

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=160)
        c, h, l = p["adj_close"], p["adj_high"], p["adj_low"]
        prev_c = c.shift(1)
        # np.fmax 忽略 NaN：首日/缺口日 prev_c 缺失时 TR 退化为 h-l；全 NaN 日保持 NaN
        tr = np.fmax(np.fmax((h - l).to_numpy(), (h - prev_c).abs().to_numpy()),
                     (l - prev_c).abs().to_numpy())
        true_range = pd.DataFrame(tr, index=c.index, columns=c.columns)
        atr = true_range.rolling(self.atr_window, min_periods=15).mean()
        sma = c.rolling(self.sma_window, min_periods=38).mean()
        signal = (c - sma).div(atr).where(atr > 0)
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(ReversalEnsembleFactor())
register(VolumeConfirmedReversalFactor())
register(AtrNormalizedTrendFactor())
