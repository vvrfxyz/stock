"""accruals（H2, Sloan 1996）：应计项 = (净利 TTM − 经营现金流 TTM) / 总资产。

预注册见 docs/wave12_fundamental_hypotheses.md（4bba108，冻结）。预注册方向
**负**（高应计 -> 低后续收益：盈余里"纸面"成分高的公司随后变脸）。

**符号约定**：compute 输出**经济方向原样**（NI>CFO 即高应计为**正**值），
**绝不在 compute 里翻符号**——方向判定在评估层看 t 的符号（与 ledger 记账口径
一致，镜像 delta_institutional_ownership：compute 输出原始经济量，负向因子
由评估层处理，不在因子内取负）。

两腿 net_income_ttm 与 operating_cash_flow_ttm 同为 TTM 指标、同锚 CIK 证券。
理论上同 as-of 天两腿应同 period_end，但生产库抽样（2026-07-08，2020+2023）
显示 NI/CFO 两腿 period_end 在 **1.35%**（1316/97456）的格上错位——申报节奏差
（如 10-Q 只带 income statement 的补充申报）。这些格上两腿是不同 TTM 窗口，
相减不是干净的应计，故照 H1 同规则对齐：**仅当两腿 period_end 一致才相减**，
否则置 NaN。

分母 assets>0 才除、否则 NaN。分子分母同源 sec_fundamental_facts、同挂锚证券，
经 company_broadcast 广播回成员列（无 company_id 者用自身值）。不设 adr_unsafe
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

_VALUE_METRICS = ("net_income_ttm", "operating_cash_flow_ttm", "assets")


@dataclass(frozen=True)
class AccrualsFactor:
    name: ClassVar[str] = "accruals"
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

        ni = bcast("net_income_ttm")
        cfo = bcast("operating_cash_flow_ttm")
        assets = bcast("assets")
        ni_pe = bcast("net_income_ttm__period_end")
        cfo_pe = bcast("operating_cash_flow_ttm__period_end")

        # 两腿 period_end 一致才相减（任一 NaN -> 不等 -> NaN）；经济方向原样（不翻符号）
        accrual = (ni - cfo).where(ni_pe == cfo_pe)
        ratio = accrual / assets.where(assets > 0)
        ratio = ratio.replace([np.inf, -np.inf], np.nan)
        return ratio.reindex(
            index=ctx.dates, columns=ctx.security_universe
        ).astype("float64")


register(AccrualsFactor())
