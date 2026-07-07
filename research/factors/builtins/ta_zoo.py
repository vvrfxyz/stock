"""经典技术指标动物园（wave-10；family=ta_zoo，用户点名 OBV 等全量补测）。

预注册假设（跑之前写死，防事后合理化）：这些指标是价格/成交量的变换，
大概率是已裁决因子的马甲——RSI/布林 ≈ 短期反转（已死）+ 波动；MACD/SMA gap ≈
动量（已被 high_52w 吸收）；唐奇安 ≈ high_52w 短窗版。**例外候选是量价族
（OBV/AD/MFI）**：成交量方向性信息我们只测过 short_volume，量价背离未测过。
判定纪律：长窗 Bonferroni + 三关卡 partial IC（size/low_vol/high_52w）——
"显著但马甲"记死亡（被吸收），只有过三关的才算新发现。

口径统一：方向性价格序列用复权收盘；H/L 乘 (adj_close/close) 同比缩放
（同一复权因子作用于全部日内价位）；成交量不复权（拆股跨界的 21 日窗污染
罕见且对秩排序影响可忽略，文档化接受）。全部 t 收盘可得，lag_days=1。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, register


def _panels(ctx: FactorContext, buffer_days: int) -> dict[str, pd.DataFrame]:
    raw = raw_bar_panels(ctx.engine, dates=ctx.dates,
                         security_ids=ctx.security_universe.tolist(),
                         columns=("open", "high", "low", "close", "volume"),
                         buffer_days=buffer_days)
    adj = adjusted_close_panel(ctx.engine, dates=ctx.dates,
                               security_ids=ctx.security_universe.tolist(),
                               buffer_days=buffer_days)
    adj = adj.reindex(index=raw["close"].index, columns=raw["close"].columns)
    ratio = adj / raw["close"]
    return {"adj_close": adj, "volume": raw["volume"],
            "adj_high": raw["high"] * ratio, "adj_low": raw["low"] * ratio}


def _finalize(signal: pd.DataFrame, ctx: FactorContext) -> pd.DataFrame:
    return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class ObvSlopeFactor:
    """OBV 21 日增量 / 21 日均量（无量纲累积资金流斜率）。方向 +（吸筹领先价格）。"""
    name: ClassVar[str] = "obv_slope"
    lookback_days: ClassVar[int] = 42
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=100)
        direction = np.sign(p["adj_close"].diff())
        obv = (direction * p["volume"]).fillna(0.0).cumsum()
        avg_vol = p["volume"].rolling(self.window, min_periods=15).mean()
        signal = (obv - obv.shift(self.window)) / (avg_vol * self.window)
        return _finalize(signal.where(avg_vol > 0), ctx)


@dataclass(frozen=True)
class AdLineSlopeFactor:
    """Chaikin A/D 线 21 日斜率（CLV 加权量，衡量日内收位吸派）。方向 +。"""
    name: ClassVar[str] = "adline_slope"
    lookback_days: ClassVar[int] = 42
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 21

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=100)
        rng = p["adj_high"] - p["adj_low"]
        clv = ((p["adj_close"] - p["adj_low"]) - (p["adj_high"] - p["adj_close"])).div(rng)
        clv = clv.where(rng > 0)
        ad = (clv * p["volume"]).fillna(0.0).cumsum()
        avg_vol = p["volume"].rolling(self.window, min_periods=15).mean()
        signal = (ad - ad.shift(self.window)) / (avg_vol * self.window)
        return _finalize(signal.where(avg_vol > 0), ctx)


@dataclass(frozen=True)
class MfiFactor:
    """MFI(14)（量加权 RSI）。预注册方向 -：超买回落（反转族假设）。"""
    name: ClassVar[str] = "mfi_14"
    lookback_days: ClassVar[int] = 28
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 14

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=70)
        typical = (p["adj_high"] + p["adj_low"] + p["adj_close"]) / 3
        flow = typical * p["volume"]
        up = flow.where(typical.diff() > 0, 0.0)
        dn = flow.where(typical.diff() < 0, 0.0)
        up_sum = up.rolling(self.window, min_periods=10).sum()
        dn_sum = dn.rolling(self.window, min_periods=10).sum()
        mfi = 100 * up_sum / (up_sum + dn_sum)
        return _finalize(-mfi.where((up_sum + dn_sum) > 0), ctx)


@dataclass(frozen=True)
class RsiFactor:
    """RSI(14)（Wilder 平滑用简单均替代，横截面排序等价性足够）。预注册方向 -。"""
    name: ClassVar[str] = "rsi_14"
    lookback_days: ClassVar[int] = 28
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 14

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=70)
        delta = p["adj_close"].diff()
        gain = delta.clip(lower=0).rolling(self.window, min_periods=10).mean()
        loss = (-delta.clip(upper=0)).rolling(self.window, min_periods=10).mean()
        rsi = 100 * gain / (gain + loss)
        return _finalize(-rsi.where((gain + loss) > 0), ctx)


@dataclass(frozen=True)
class MacdHistFactor:
    """MACD(12,26,9) 柱 / 价格（无量纲趋势加速度）。预注册方向 +（趋势跟随）。"""
    name: ClassVar[str] = "macd_hist"
    lookback_days: ClassVar[int] = 60
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=150)
        c = p["adj_close"]
        macd = c.ewm(span=12, min_periods=10).mean() - c.ewm(span=26, min_periods=20).mean()
        hist = macd - macd.ewm(span=9, min_periods=7).mean()
        return _finalize(hist / c, ctx)


@dataclass(frozen=True)
class BollingerBFactor:
    """布林 %B（20 日 ±2σ 通道位置）。预注册方向 -（均值回归假设）。"""
    name: ClassVar[str] = "bollinger_b"
    lookback_days: ClassVar[int] = 40
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 20

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=90)
        c = p["adj_close"]
        mid = c.rolling(self.window, min_periods=15).mean()
        sd = c.rolling(self.window, min_periods=15).std()
        pct_b = (c - (mid - 2 * sd)) / (4 * sd)
        return _finalize(-pct_b.where(sd > 0), ctx)


@dataclass(frozen=True)
class DonchianPosFactor:
    """唐奇安通道位置（55 日高低点区间内位置）。预注册方向 +（突破跟随；
    预期被 high_52w 吸收——这就是三关卡要回答的问题）。"""
    name: ClassVar[str] = "donchian_pos"
    lookback_days: ClassVar[int] = 55
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 55

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=120)
        c = p["adj_close"]
        hi = c.rolling(self.window, min_periods=40).max()
        lo = c.rolling(self.window, min_periods=40).min()
        signal = (c - lo) / (hi - lo)
        return _finalize(signal.where(hi > lo), ctx)


@dataclass(frozen=True)
class SmaGapFactor:
    """价格对 50 日均线偏离。预注册方向 +（趋势）；预期动量马甲。"""
    name: ClassVar[str] = "sma_gap_50"
    lookback_days: ClassVar[int] = 50
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 50

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        p = _panels(ctx, buffer_days=120)
        c = p["adj_close"]
        sma = c.rolling(self.window, min_periods=38).mean()
        return _finalize(c / sma - 1, ctx)


for _factor in (ObvSlopeFactor(), AdLineSlopeFactor(), MfiFactor(), RsiFactor(),
                MacdHistFactor(), BollingerBFactor(), DonchianPosFactor(), SmaGapFactor()):
    register(_factor)
