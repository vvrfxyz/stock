"""gross_profitability（H1, Novy-Marx 2013）：毛利 / 总资产 = GP/AT。

预注册见 docs/wave12_fundamental_hypotheses.md（4bba108，冻结）。方向为正
（高毛利 -> 高后续收益）。

分子 = 毛利 TTM：优先直报 gross_profit_ttm；缺失格用
revenue_ttm − cost_of_revenue_ttm 兜底，但**仅当两腿 as-of 事件的 period_end
一致**（H1 跨 metric 对齐规则——TTM 窗口错位的差不是毛利，置 NaN）。period_end
门槛靠 load_fundamental_panel(include_period_end=True) 逐格取所选事件的
period_end，与值面板逐格 NaN 一致（见 asof_panel）。

分母 = assets（as-of 时点值），assets>0 才除、否则 NaN。分子分母同源
sec_fundamental_facts、同挂 CIK 锚证券，经 company_broadcast 广播回成员列
（见 _fundamental_ratio；无 company_id 的证券用自身值）。

不设 adr_unsafe：分子分母同源、不含股本/市值口径（H1 明确）；宇宙口径仍
CS-only（ADR 不进宇宙）。一切在内存完成，绝不回写事实表。

生产库探针（2026-07-08，2020+2023 抽样）：直报 40,063 格；兜底可用（无直报
但 rev/cost 齐备）3,195 格，其中 period_end 一致 2,982、错位 213（6.7%）——
门槛确有作用，兜底净增约 7% 覆盖。
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

_VALUE_METRICS = ("gross_profit_ttm", "revenue_ttm", "cost_of_revenue_ttm", "assets")
_PERIOD_END_METRICS = ("revenue_ttm", "cost_of_revenue_ttm")


@dataclass(frozen=True)
class GrossProfitabilityFactor:
    name: ClassVar[str] = "gross_profitability"
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
            include_period_end=True,
        )
        empty = pd.DataFrame(
            index=pd.DatetimeIndex(pd.to_datetime(ctx.dates)),
            columns=pd.Index([], dtype="int64"),
            dtype="float64",
        )

        def bcast(key: str) -> pd.DataFrame:
            return company_broadcast(panels.get(key, empty), universe, sid_to_cid)

        gp = bcast("gross_profit_ttm")
        revenue = bcast("revenue_ttm")
        cost = bcast("cost_of_revenue_ttm")
        assets = bcast("assets")
        revenue_pe = bcast("revenue_ttm__period_end")
        cost_pe = bcast("cost_of_revenue_ttm__period_end")

        # 兜底减法仅在两腿 period_end 一致时启用（任一 NaN -> 不等 -> NaN）
        fallback = (revenue - cost).where(revenue_pe == cost_pe)
        numerator = gp.where(gp.notna(), fallback)

        ratio = numerator / assets.where(assets > 0)
        ratio = ratio.replace([np.inf, -np.inf], np.nan)
        return ratio.reindex(
            index=ctx.dates, columns=ctx.security_universe
        ).astype("float64")


register(GrossProfitabilityFactor())
