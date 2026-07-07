"""asset_growth（H4, Cooper-Gulen-Schill 2008）：资产同比增长 = Assets_t / Assets_{t-1y} − 1。

预注册见 docs/wave12_fundamental_hypotheses.md H4 节（4bba108，冻结）。预注册方向
**负**（高资产增长 -> 低后续收益：扩张的公司随后跑输）。

**符号约定**：compute 输出**经济方向原样**（高增长为**正**值），**绝不在 compute
里翻符号**——方向判定在评估层看 t 的符号（同 accruals / delta_institutional_ownership
先例）。

**事件层 YoY**（非 as-of 面板取 t−252 差分近似——那混入申报时点噪声）：由
research.fundamentals.load_yoy_ratio_panel -> build_yoy_ratio_events 产出，两腿锁同
concept、YoY 事件 visible_date = max(两腿 filed_date)、任一腿重述即重发、单调
period_end 护栏（详见该函数 docstring）。上一期 Assets>0 才除、否则 NaN。

分子分母同源 Assets、同挂 CIK 锚证券，经 company_broadcast 广播回成员列
（无 company_id 者用自身值）。不设 adr_unsafe（同源、不含股本/市值口径）；
宇宙口径仍 CS-only。绝不回写事实表。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.builtins._fundamental_ratio import (
    build_membership,
    company_broadcast,
    expanded_security_ids,
)
from research.factors.protocol import FactorContext, register
from research.fundamentals import load_yoy_ratio_panel


@dataclass(frozen=True)
class AssetGrowthFactor:
    name: ClassVar[str] = "asset_growth"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        universe = pd.Index([int(s) for s in ctx.security_universe], dtype="int64")
        sid_to_cid = build_membership(ctx.engine, universe)
        expanded = expanded_security_ids(universe, sid_to_cid)

        panel = load_yoy_ratio_panel(
            ctx.engine,
            dates=ctx.dates,
            source_metric="assets",
            out_metric="asset_growth",
            security_ids=expanded,
        )
        broadcast = company_broadcast(panel, universe, sid_to_cid)
        return broadcast.reindex(
            index=ctx.dates, columns=ctx.security_universe
        ).astype("float64")


register(AssetGrowthFactor())
