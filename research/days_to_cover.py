"""研究层 PIT days_to_cover 面板。

days_to_cover = short_interest / avg_daily_volume(20 日)。
分子用 short_interests 的 PIT 仓位（结算日 T+1 可见），
分母用 daily_prices 的 20 日滚动平均成交量。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel
from research.short_interest import load_short_interest_events


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def _load_volume_wide(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None,
) -> pd.DataFrame:
    if len(dates) == 0:
        columns = pd.Index(security_ids or [], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)
    if security_ids is not None and not security_ids:
        return pd.DataFrame(index=dates, columns=pd.Index([], dtype=np.int64), dtype=np.float64)

    id_clause = "and security_id = any(:security_ids)" if security_ids is not None else ""
    sql = text(
        f"""
        select security_id, date, volume::float8 as volume
        from daily_prices
        where date between :start and :end
          and volume is not null
          {id_clause}
        order by security_id, date
        """
    )
    # 向前多拉 30 个自然日以填满 rolling(20) 的 warmup 窗口
    buffer_start = dates[0].date() - pd.Timedelta(days=30)
    params: dict[str, object] = {"start": buffer_start, "end": dates[-1].date()}
    if security_ids is not None:
        params["security_ids"] = security_ids
    vol = pd.read_sql_query(sql, engine, params=params, parse_dates=["date"])
    if vol.empty:
        columns = pd.Index(security_ids or [], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)
    vol = _to_ns(vol, ("date",))
    vol["security_id"] = vol["security_id"].astype(np.int64)
    wide = vol.pivot_table(index="date", columns="security_id", values="volume", aggfunc="last")
    if security_ids is not None:
        wide = wide.reindex(columns=pd.Index(security_ids, dtype=np.int64))
    return wide.astype(np.float64)


def load_days_to_cover_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    visible_delay_days: int = 1,
    si_max_staleness_days: int = 60,
) -> pd.DataFrame:
    """一站式加载，返回 days_to_cover 宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    if security_ids is not None and not security_ids:
        return pd.DataFrame(index=dates, columns=pd.Index([], dtype=np.int64), dtype=np.float64)
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    si_events = load_short_interest_events(engine, security_ids=security_ids)
    si_panel = event_table_to_asof_panel(
        si_events.rename(columns={"short_interest": "value"}),
        dates=dates,
        value_column="value",
        visible_date_column="visible_date",
        staleness_anchor_column="visible_date",
        visible_delay_days=visible_delay_days,
        max_staleness_days=si_max_staleness_days,
        security_universe=requested_security_ids,
    )

    vol_wide = _load_volume_wide(engine, dates=dates, security_ids=security_ids)
    avg_vol = vol_wide.rolling(20, min_periods=10).mean().reindex(index=dates)
    avg_vol = avg_vol.reindex(columns=si_panel.columns)

    dtc = si_panel / avg_vol.where(avg_vol > 0)
    panel = dtc.reindex(index=dates, columns=si_panel.columns).astype(np.float64)
    if requested_security_ids is not None:
        return panel.reindex(columns=requested_security_ids).astype(np.float64)
    return panel
