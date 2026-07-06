"""EOD 压力反转因子族（wave-3；family=eod_pressure，多重检验按族收敛）。

假设（源自 wave-2 发现：last30_persistence 在 2016+ 符号反转，NW t=-2.57）：
尾盘半小时的价格位移若由资金流推动（而非信息），次日被流动性提供者修正。
"是否流推"用尾盘成交量占比的时序异常度衡量——这是分钟数据独占的条件变量。

- eod_reversal：signal_t = -ret_last30_t（当日新鲜信号，无平滑——反转类信号
  的半衰期短，wave-2 的 21 日均值版本已显著，按日版本假设更强）。
- eod_reversal_flow：-ret_last30_t × min(abn_t, 3)，
  abn_t = vol_last30_share_t / 其 21 日滚动均值（时序异常，避免"有些名字
  天生尾盘集中"的横截面伪条件）；连续加权，不设阈值参数。

PIT：两者 t 日收盘可得。预期正向（分高 = 尾盘被砸/流推下跌 = 次日反弹）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.minute_loader import load_minute_feature_panel
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class EodReversalFactor:
    name: ClassVar[str] = "eod_reversal"
    lookback_days: ClassVar[int] = 1
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(), ("ret_last30",), buffer_days=10)
        signal = -panels["ret_last30"]
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class EodReversalFlowFactor:
    name: ClassVar[str] = "eod_reversal_flow"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    abn_window: int = 21
    abn_min_days: int = 10
    abn_cap: float = 3.0

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panels = load_minute_feature_panel(
            ctx.dates, ctx.security_universe.tolist(),
            ("ret_last30", "vol_last30_share"), buffer_days=45)
        share = panels["vol_last30_share"].where(panels["vol_last30_share"] > 0)
        baseline = share.rolling(self.abn_window, min_periods=self.abn_min_days).mean()
        abn = (share / baseline).clip(upper=self.abn_cap)
        signal = -panels["ret_last30"] * abn
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(EodReversalFactor())
register(EodReversalFlowFactor())
