"""risk_free_rates 读取层：FRED DTB3 as-of 查询与区间现金收益。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text

DEFAULT_SERIES_ID = "DTB3"
DEFAULT_MAX_STALENESS_DAYS = 7


def load_risk_free_daily_returns(
    engine,
    index: pd.DatetimeIndex,
    *,
    series_id: str = DEFAULT_SERIES_ID,
    max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS,
) -> pd.Series:
    """从 risk_free_rates 读取与交易日 index 对齐的 rf simple return（actual/360）。"""
    normalized = pd.DatetimeIndex(pd.to_datetime(index)).astype("datetime64[ns]")
    series_upper = series_id.upper()
    if normalized.empty:
        return pd.Series(dtype="float64", index=normalized, name=series_upper)
    rows = pd.read_sql_query(
        text(
            """
            select date, rate_pct
            from risk_free_rates
            where series_id = :series_id
              and date <= :end
            order by date asc
            """
        ),
        engine,
        params={"series_id": series_upper, "end": normalized.max().date()},
        parse_dates=["date"],
    )
    if rows.empty:
        raise LookupError(
            f"risk_free_rates has no {series_upper} rows; run update_risk_free_rates or pass --no-risk-free"
        )
    src_dates = pd.DatetimeIndex(rows["date"]).astype("datetime64[ns]")
    rate_series = pd.Series(rows["rate_pct"].astype(float).to_numpy(), index=src_dates)
    last_obs_date = pd.Series(src_dates, index=src_dates).reindex(normalized, method="ffill")
    aligned_rate = rate_series.reindex(normalized, method="ffill")
    staleness = (normalized - last_obs_date.to_numpy()).days
    missing = aligned_rate.isna().to_numpy() | (staleness > max_staleness_days)
    if missing.any():
        bad = normalized[missing][0]
        raise LookupError(f"risk_free_rates has no fresh {series_upper} row for {bad.date()}")
    day_gaps = np.r_[1, np.diff(normalized.values).astype("timedelta64[D]").astype(int).clip(min=1)]
    values = aligned_rate.to_numpy() / 100.0 * day_gaps / 360.0
    return pd.Series(values, index=normalized, dtype="float64", name=series_upper)
