"""技术分析三大支柱（wave-4；百年文献里最稳健的价格异象，此前竟未上架）。

- momentum_12_1（Jegadeesh-Titman 1993）：过去 12 个月收益跳过最近 1 个月
  （t-252 → t-21），正向。跳过近月避开短期反转的污染。
- high_52w（George-Hwang 2004, JF）：当前价 / 过去 252 日最高价，正向
  （越接近 52 周新高越好——锚定效应压制对好消息的反应，随后漂移）。
  动量的"位置版"，GH 原文声称解释力强于动量本身。
- low_vol（Ang-Hodrick-Xing-Zhang 2006）：过去 63 日日收益标准差取负，
  正向定义（分高=低波动=历史上风险调整后跑赢——著名的低波动异象）。

全部用复权收盘价（与 classic_price 同 adjusted_close_panel 共享缓存，
同 universe/窗口零额外装载）。PIT：t 收盘即得。

预登记假设（写在跑之前，防事后合理化）：
- momentum_12_1 应显著为正（美股最强横截面异象，但 2009+ 有大崩溃拖累）；
- high_52w 与 momentum 高相关（>0.6），增量存疑；
- low_vol 的 IC 可能弱（它的效应传统上在风险调整后收益，而非原始收益差），
  且与 max_lottery 高相关（波动大的名字才有大的单日收益）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.price_cache import adjusted_close_panel
from research.factors.protocol import FactorContext, register


def _adj_close_panel(ctx: FactorContext, buffer_days: int) -> pd.DataFrame:
    return adjusted_close_panel(
        ctx.engine, dates=ctx.dates,
        security_ids=ctx.security_universe.tolist(), buffer_days=buffer_days)


@dataclass(frozen=True)
class Momentum12to1Factor:
    name: ClassVar[str] = "momentum_12_1"
    lookback_days: ClassVar[int] = 252
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = _adj_close_panel(ctx, buffer_days=400)
        momentum = adj_close.shift(21) / adj_close.shift(252) - 1
        return momentum.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class High52WeekFactor:
    name: ClassVar[str] = "high_52w"
    lookback_days: ClassVar[int] = 252
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    min_days: int = 126

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = _adj_close_panel(ctx, buffer_days=400)
        rolling_high = adj_close.rolling(252, min_periods=self.min_days).max()
        nearness = adj_close / rolling_high
        return nearness.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class LowVolFactor:
    name: ClassVar[str] = "low_vol"
    lookback_days: ClassVar[int] = 63
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 63
    min_days: int = 42

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = _adj_close_panel(ctx, buffer_days=130)
        rets = adj_close.pct_change(fill_method=None)
        vol = rets.rolling(self.window, min_periods=self.min_days).std()
        return (-vol).reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(Momentum12to1Factor())
register(High52WeekFactor())
register(LowVolFactor())
