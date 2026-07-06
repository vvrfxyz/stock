"""日内矩量因子族（分钟数据独占）：已实现偏度 + 符号跳跃变差。

家族声明（多重检验口径）：rskew 与 rsj 高相关（文献 ~0.7），按同族登记，
只保留胜者。两者都是"彩票偏好 -> 高偏度/上行跳跃被高估"的负向因子。

- realized_skew（Amaya-Christoffersen-Jacobs-Vasquez 2015, JFE）：
  日度 RSkew = sqrt(n)·Σr³/RV^1.5（5 分钟子采样对数收益），因子 = 21 日均值，负向。
- signed_jump（Bollerslev-Li-Zhao 2020, JFQA）：
  日度 RSJ = (RV⁺ − RV⁻)/RV，因子 = 5 日均值（原文周频口径），负向。

PIT：特征当日收盘即得（lag_days=0 语义，取 1 保守——t 日信号最早 t+1 开盘可交易，
评估层 t 权重赚 t+1 收益的口径本身已内含一天执行延迟）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.minute_loader import load_minute_feature_panel
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class RealizedSkewFactor:
    name: ClassVar[str] = "realized_skew"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21
    min_days: int = 15

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(), ("rskew",))
        rskew = panels["rskew"]
        # 特征端 rskew=0 表示"矩量无效日"（bar 不足），置 NaN 不进均值
        rskew = rskew.where(rskew != 0.0)
        # 负向：高偏度未来收益低 -> 取负让"分高=预期收益高"与评估层口径一致
        signal = -rskew.rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class SignedJumpFactor:
    name: ClassVar[str] = "signed_jump"
    lookback_days: ClassVar[int] = 5
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 5
    min_days: int = 4

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(), ("rv", "rv_up", "rv_down"))
        rv = panels["rv"].where(panels["rv"] > 0)
        rsj = (panels["rv_up"] - panels["rv_down"]) / rv
        signal = -rsj.rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(RealizedSkewFactor())
register(SignedJumpFactor())
