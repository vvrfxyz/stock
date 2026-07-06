"""日内资金流因子族（分钟数据独占）：尾盘持续性 + 聪明钱缺口。

- last30_persistence（Heston-Korajczyk-Sadka 2010, JF：日内周期性横截面动量）：
  因子 = 尾盘半小时收益的 21 日均值，正向（尾盘强者恒强——机构收盘前执行流的持续性）。
- smart_money_gap：因子 = (尾盘收益 − 开盘半小时收益) 的 21 日均值，正向
  （开盘散户噪声、尾盘机构定价的"聪明钱"叙事；Smart Money Flow 指数的横截面化）。

分钟 close 不含 16:00 收盘竞价 print——这是特性不是缺陷：信号只用连续竞价段，
被预测的次日收益从竞价开始，边界干净无重叠。
PIT：t 日收盘即得。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.minute_loader import load_minute_feature_panel
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class Last30PersistenceFactor:
    name: ClassVar[str] = "last30_persistence"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21
    min_days: int = 15

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(), ("ret_last30",))
        signal = panels["ret_last30"].rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class SmartMoneyGapFactor:
    name: ClassVar[str] = "smart_money_gap"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21
    min_days: int = 15

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(), ("ret_last30", "ret_first30"))
        gap = panels["ret_last30"] - panels["ret_first30"]
        signal = gap.rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(Last30PersistenceFactor())
register(SmartMoneyGapFactor())
