from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.factors.protocol import FactorContext, register
from research.market_cap import load_log_market_cap_panel


@dataclass(frozen=True)
class SizeFactor:
    name: ClassVar[str] = "size"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        log_mcap = load_log_market_cap_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
        )
        return log_mcap.reindex(index=ctx.dates, columns=ctx.security_universe)


register(SizeFactor())
