"""operating_profitability（H3, Fama-French 2015 变体，EBIT 口径）：
营业利润 TTM / 总资产 = operating_income_ttm / assets。

预注册见 docs/wave12_fundamental_hypotheses.md（4bba108，冻结）。方向为正。
**明确非 FF-2015 原口径**（原口径要扣 InterestExpense，不在白名单——本轮
不扩白名单、EBIT 口径绕开）。

分子单一 metric（无跨 metric 兜底，故不需要 period_end 对齐）。分母 assets>0
才除、否则 NaN。分子分母同源 sec_fundamental_facts、同挂锚证券，经
company_broadcast 广播回成员列（无 company_id 者用自身值）。不设 adr_unsafe
（分子分母同源、不含股本/市值口径）；宇宙口径仍 CS-only。绝不回写事实表。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.builtins._fundamental_ratio import (
    build_membership,
    company_broadcast,
    expanded_security_ids,
)
from research.factors.protocol import FactorContext, register
from research.fundamentals import load_fundamental_panel

_VALUE_METRICS = ("operating_income_ttm", "assets")


@dataclass(frozen=True)
class OperatingProfitabilityFactor:
    name: ClassVar[str] = "operating_profitability"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        universe = pd.Index([int(s) for s in ctx.security_universe], dtype="int64")
        sid_to_cid = build_membership(ctx.engine, universe)
        expanded = expanded_security_ids(universe, sid_to_cid)

        panels = load_fundamental_panel(
            ctx.engine,
            dates=ctx.dates,
            metrics=_VALUE_METRICS,
            security_ids=expanded,
        )
        empty = pd.DataFrame(
            index=pd.DatetimeIndex(pd.to_datetime(ctx.dates)),
            columns=pd.Index([], dtype="int64"),
            dtype="float64",
        )

        def bcast(key: str) -> pd.DataFrame:
            return company_broadcast(panels.get(key, empty), universe, sid_to_cid)

        oi = bcast("operating_income_ttm")
        assets = bcast("assets")

        ratio = oi / assets.where(assets > 0)
        ratio = ratio.replace([np.inf, -np.inf], np.nan)
        return ratio.reindex(
            index=ctx.dates, columns=ctx.security_universe
        ).astype("float64")


register(OperatingProfitabilityFactor())
