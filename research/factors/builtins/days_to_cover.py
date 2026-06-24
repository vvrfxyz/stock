from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.days_to_cover import load_days_to_cover_panel
from research.factors.protocol import FactorContext, register


@dataclass(frozen=True)
class DaysToCoverFactor:
    name: ClassVar[str] = "days_to_cover"
    lookback_days: ClassVar[int] = 20
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_days_to_cover_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(DaysToCoverFactor())
