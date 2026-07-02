from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.short_interest import (
    SHORT_INTEREST_VISIBLE_DELAY_DAYS,
    load_short_interest_ratio_panel,
)


@dataclass(frozen=True)
class ShortInterestFactor:
    name: ClassVar[str] = "short_interest_ratio"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = SHORT_INTEREST_VISIBLE_DELAY_DAYS
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_short_interest_ratio_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(ShortInterestFactor())
