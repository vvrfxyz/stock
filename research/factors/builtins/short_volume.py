from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.short_volume import load_short_volume_ratio_panel


@dataclass(frozen=True)
class ShortVolumeFactor:
    name: ClassVar[str] = "short_volume_ratio"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 0
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        panel = load_short_volume_ratio_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return panel.reindex(index=ctx.dates, columns=ctx.security_universe)


register(ShortVolumeFactor())
