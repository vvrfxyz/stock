from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.fundamentals import load_fundamental_panel
from research.market_cap import load_market_cap_panel


@dataclass(frozen=True)
class EarningsYieldFactor:
    name: ClassVar[str] = "earnings_yield"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        ids = ctx.security_universe.tolist()
        fundamentals = load_fundamental_panel(
            ctx.engine,
            dates=ctx.dates,
            metrics=("net_income_ttm",),
            security_ids=ids,
        )
        ni = fundamentals.get(
            "net_income_ttm",
            pd.DataFrame(index=ctx.dates, columns=ctx.security_universe, dtype="float64"),
        )
        mcap = load_market_cap_panel(ctx.engine, dates=ctx.dates, security_ids=ids)
        ratio = ni / mcap.where(mcap > 0)
        return ratio.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(EarningsYieldFactor())
