from __future__ import annotations

import numpy as np
import pandas as pd


def event_table_to_asof_panel(
    events: pd.DataFrame,
    *,
    dates: pd.DatetimeIndex,
    value_column: str,
    visible_date_column: str = "visible_date",
    staleness_anchor_column: str = "period_end",
    visible_delay_days: int = 0,
    max_staleness_days: int | None = None,
    security_universe: list[int] | pd.Index | None = None,
) -> pd.DataFrame:
    """事件表 -> 宽表 as-of 面板，PIT 防未来。"""
    dates = pd.DatetimeIndex(pd.to_datetime(dates)).astype("datetime64[ns]")
    ev = events.copy()

    required = ["security_id", visible_date_column, value_column]
    ev = ev.dropna(subset=required)
    if staleness_anchor_column != visible_date_column and max_staleness_days is not None:
        ev = ev.dropna(subset=[staleness_anchor_column])

    if security_universe is None:
        if ev.empty:
            universe = pd.Index([], dtype=np.int64)
        else:
            universe = pd.Index(ev["security_id"].astype(np.int64).unique(), dtype=np.int64).sort_values()
    else:
        universe = pd.Index(security_universe, dtype=np.int64).sort_values()

    if len(dates) == 0 or len(universe) == 0:
        return pd.DataFrame(index=dates, columns=universe, dtype=np.float64)

    if ev.empty:
        return pd.DataFrame(index=dates, columns=universe, dtype=np.float64)

    ev["security_id"] = ev["security_id"].astype(np.int64)
    ev[value_column] = ev[value_column].astype(np.float64)
    ev["effective_visible_date"] = ev[visible_date_column] + pd.Timedelta(
        days=visible_delay_days
    )

    grid = pd.DataFrame(
        {
            "date": np.repeat(dates.to_numpy(), len(universe)),
            "security_id": np.tile(universe.to_numpy(), len(dates)),
        }
    )
    joined = pd.merge_asof(
        grid.sort_values("date"),
        ev.sort_values(
            ["effective_visible_date", staleness_anchor_column], kind="mergesort"
        ),
        left_on="date",
        right_on="effective_visible_date",
        by="security_id",
        direction="backward",
    )

    if max_staleness_days is not None:
        stale = joined[staleness_anchor_column] < joined["date"] - pd.Timedelta(
            days=max_staleness_days
        )
        joined.loc[stale, value_column] = np.nan

    panel = joined.pivot_table(
        index="date", columns="security_id", values=value_column, aggfunc="last"
    ).reindex(index=dates, columns=universe)
    return panel.astype(np.float64)
