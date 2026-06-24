from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.institutional import load_delta_institutional_ownership_panel


@dataclass(frozen=True)
class DeltaInstitutionalOwnershipFactor:
    name: ClassVar[str] = "delta_institutional_ownership"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_delta_institutional_ownership_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(DeltaInstitutionalOwnershipFactor())
