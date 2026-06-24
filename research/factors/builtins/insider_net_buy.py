from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.insider import load_insider_net_buy_panel


@dataclass(frozen=True)
class InsiderNetBuyFactor:
    name: ClassVar[str] = "insider_net_buy"
    lookback_days: ClassVar[int] = 90
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_insider_net_buy_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(InsiderNetBuyFactor())
