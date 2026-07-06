"""残差波动率因子（wave-5；low_vol 的精修版，family=vol）。

Ang-Hodrick-Xing-Zhang 2006 用的是对 FF3 的残差特质波动；第一期先做对市场
单因子的残差版：63 日滚动窗内 r_i = a + b·r_m + e，signal = -std(e)。
r_m 用横截面等权均值（自含证券自身，1/N 稀释可忽略；含全 universe 而非
eligible 掩码后的子集——代理噪音方向中性，文档化接受）。

全程滚动矩恒等式向量化（无逐窗回归），且**市场矩按各证券自身可得日掩码计算**
（2026-07 设计审计修正：否则停牌/缺口名字的矩样本错位可产生负残差方差，
clip(0) 会把数据病理直接排进最强信号分位）：
    E[·|i] 均在证券 i 的非缺失日子集上取样 -> 对齐 OLS 恒等式保证 res_var >= 0，
    材料性负值只可能来自数值异常 -> 置 NaN 剔除，不参与排名；
    零总方差（63 日价格纹丝不动的挂牌工具/SPAC 信托价）同样置 NaN——
    那不是"低波动股票"，是不交易的东西。

与 low_vol（总波动）的区别：剥掉市场 beta 承载的系统性波动，留特质部分——
若 low_vol 的 IC 主要来自 beta（低 beta 效应），residual_vol 会明显弱于 low_vol；
若来自特质波动（真·彩票偏好定价），两者相当甚至更强。这本身就是一次机制检验。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.price_cache import adjusted_close_panel
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class ResidualVolFactor:
    name: ClassVar[str] = "residual_vol"
    lookback_days: ClassVar[int] = 63
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window: int = 63
    min_days: int = 42

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adj_close = adjusted_close_panel(
            ctx.engine, dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(), buffer_days=130)
        rets = adj_close.pct_change(fill_method=None)
        mkt = rets.mean(axis=1)
        w, mp = self.window, self.min_days

        mask = rets.notna()
        n_valid = mask.rolling(w, min_periods=1).sum()
        # 证券自身矩（rolling mean 天然跳过 NaN，样本 = 非缺失日）
        e_r = rets.rolling(w, min_periods=mp).mean()
        e_r2 = (rets * rets).rolling(w, min_periods=mp).mean()
        e_rm = rets.mul(mkt, axis=0).rolling(w, min_periods=mp).mean()
        # 市场矩掩码到同一子集：sum(mask·r_m)/sum(mask)
        e_m = mask.mul(mkt, axis=0).rolling(w, min_periods=1).sum() / n_valid
        e_m2 = mask.mul(mkt * mkt, axis=0).rolling(w, min_periods=1).sum() / n_valid

        var_i = e_r2 - e_r * e_r
        var_m = e_m2 - e_m * e_m
        cov = e_rm - e_r * e_m
        with np.errstate(invalid="ignore", divide="ignore"):
            res_var = var_i - (cov * cov) / var_m
        res_var = res_var.where(var_m > 0)
        res_var = res_var.where(res_var > -1e-12)      # 材料性负值 = 数值异常 -> NaN
        res_var = res_var.clip(lower=0.0)              # 浮点尘埃归零
        res_var = res_var.where(var_i > 0)             # 零总方差 = 非交易工具 -> NaN
        signal = -np.sqrt(res_var)
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(ResidualVolFactor())
