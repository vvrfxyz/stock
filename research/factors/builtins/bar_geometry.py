"""K 线几何因子族（日线 OHLC+vwap 的连续统计量——离散形态识别的严肃替代）。

- shadow_asymmetry：上影线占比 − 下影线占比的 21 日均值，负向。
  这是 realized_skew 的"日线 K 线近似"——本族与分钟矩量族的头对头对比，
  回答"5B 行分钟数据相对日线 bar 形状多买到了多少 alpha"。
- close_vwap_pressure：ln(close/vwap) 的 5 日均值，负向（收盘显著高于当日
  均价 = 尾盘买压/迫仓，短期反转；vwap 为 2026-07 回填的全时段 SIP 口径）。

离散 K 线形态（锤子线/吞没等）学术证据薄弱（Marshall-Young-Rose 2006），
连续统计量保留同一信息且有横截面可比性。PIT：bar t 收盘即得。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import ClassVar

import pandas as pd
from sqlalchemy import text

from research.factors.protocol import FactorContext, register


def _load_bar_panels(ctx: FactorContext, buffer_days: int, columns: str) -> pd.DataFrame:
    start = (ctx.dates[0] - timedelta(days=buffer_days)).date()
    end = ctx.dates[-1].date()
    sql = text(f"""
        select p.security_id, p.date, {columns}
        from daily_prices p
        where p.date between :start and :end
          and p.security_id = any(:ids)
        order by p.security_id, p.date
    """)
    with ctx.engine.connect() as conn:
        frame = pd.read_sql_query(
            sql, conn,
            params={"start": start, "end": end, "ids": ctx.security_universe.tolist()})
    frame["date"] = pd.to_datetime(frame["date"])
    return frame


@dataclass(frozen=True)
class ShadowAsymmetryFactor:
    name: ClassVar[str] = "shadow_asymmetry"
    lookback_days: ClassVar[int] = 21
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21
    min_days: int = 15

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        frame = _load_bar_panels(
            ctx, buffer_days=45,
            columns="p.open::float8 as open, p.high::float8 as high, "
                    "p.low::float8 as low, p.close::float8 as close")
        rng = frame["high"] - frame["low"]
        upper = (frame["high"] - frame[["open", "close"]].max(axis=1)) / rng
        lower = (frame[["open", "close"]].min(axis=1) - frame["low"]) / rng
        frame["sasym"] = (upper - lower).where(rng > 0)
        wide = frame.pivot_table(index="date", columns="security_id", values="sasym", aggfunc="last")
        # 负向：持续上影线偏多（日内冲高回落，右尾被卖出兑现）-> 未来收益低
        signal = -wide.rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class CloseVwapPressureFactor:
    name: ClassVar[str] = "close_vwap_pressure"
    lookback_days: ClassVar[int] = 5
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 5
    min_days: int = 3

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        import numpy as np

        frame = _load_bar_panels(
            ctx, buffer_days=15,
            columns="p.close::float8 as close, p.vwap::float8 as vwap")
        valid = (frame["vwap"] > 0) & (frame["close"] > 0)
        frame["cvp"] = np.log(frame["close"].where(valid) / frame["vwap"].where(valid))
        wide = frame.pivot_table(index="date", columns="security_id", values="cvp", aggfunc="last")
        # 负向：收盘持续压在 vwap 之上 = 买压外溢 -> 短期反转
        signal = -wide.rolling(self.window, min_periods=self.min_days).mean()
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(ShadowAsymmetryFactor())
register(CloseVwapPressureFactor())
