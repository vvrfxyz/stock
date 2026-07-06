"""经典价格异象对照组：MAX 彩票效应 + 短期反转（新技术因子的强基线）。

- max_lottery（Bali-Cakici-Whitelaw 2011, JFE"Maxing out"）：
  过去 21 交易日最大的 5 个日收益均值，负向（彩票型暴涨被散户追高 -> 未来跑输）。
  与分钟矩量族形成"极端收益偏好"的日线/分钟两个测量层级。
- short_term_reversal（Jegadeesh 1990）：过去 21 日收益取负（跳过最近 1 日
  避开买卖价差反弹），正向定义（分高=近月输家=预期反弹）。

两者都用复权收盘价（与 run_baselines 同一 apply_adjustment 口径）。PIT：t 收盘即得。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar

import pandas as pd

from research.data import apply_adjustment, load_factor_events, load_price_long, to_wide
from research.factors.protocol import FactorContext, register


def _adj_close_panel(ctx: FactorContext, buffer_days: int) -> pd.DataFrame:
    start = (ctx.dates[0] - timedelta(days=buffer_days)).date()
    end = ctx.dates[-1].date()
    prices = load_price_long(
        ctx.engine, start=start, end=end,
        types=("CS", "ETF"), security_ids=ctx.security_universe.tolist())
    events = load_factor_events(ctx.engine, as_of=end)
    prices = apply_adjustment(prices, events, as_of=end)
    return to_wide(prices, "adj_close")


@dataclass(frozen=True)
class MaxLotteryFactor:
    name: ClassVar[str] = "max_lottery"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21
    min_days: int = 15

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = _adj_close_panel(ctx, buffer_days=45)
        rets = adj_close.pct_change(fill_method=None)
        # MAX(1) 口径（Bali et al. 表 1 主结果之一）：过去 21 日最大单日收益，
        # 向量化 rolling max；MAX(5) 均值需逐窗排序，面板上不经济
        max1 = rets.rolling(self.window, min_periods=self.min_days).max()
        return (-max1).reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class ShortTermReversalFactor:
    name: ClassVar[str] = "short_term_reversal"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = _adj_close_panel(ctx, buffer_days=45)
        # -(P_{t-1}/P_{t-21} - 1)：跳过最近 1 日
        reversal = -(adj_close.shift(1) / adj_close.shift(21) - 1)
        return reversal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(MaxLotteryFactor())
register(ShortTermReversalFactor())
