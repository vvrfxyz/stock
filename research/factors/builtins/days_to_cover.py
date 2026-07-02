from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.days_to_cover import load_days_to_cover_panel
from research.factors.protocol import FactorContext, register
from research.short_interest import SHORT_INTEREST_VISIBLE_DELAY_DAYS


@dataclass(frozen=True)
class DaysToCoverFactor:
    name: ClassVar[str] = "days_to_cover"
    lookback_days: ClassVar[int] = 20
    lag_days: ClassVar[int] = SHORT_INTEREST_VISIBLE_DELAY_DAYS
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_days_to_cover_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(DaysToCoverFactor())
