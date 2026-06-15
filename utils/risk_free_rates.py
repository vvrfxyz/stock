"""risk_free_rates 读取层：FRED DTB3 as-of 查询与区间现金收益。"""
from __future__ import annotations

from bisect import bisect_right
from datetime import date
from decimal import Decimal, localcontext

import pandas as pd
from sqlalchemy import text

DEFAULT_SERIES_ID = "DTB3"
DEFAULT_MAX_STALENESS_DAYS = 7


def rate_pct_to_simple_return(rate_pct: Decimal, *, days: int = 1) -> Decimal:
    """DTB3 discount-basis annual percent -> simple cash return over days (actual/360)."""
    with localcontext() as ctx:
        ctx.prec = 28
        return rate_pct / Decimal("100") * Decimal(days) / Decimal("360")


def load_risk_free_daily_returns(
    engine,
    index: pd.DatetimeIndex,
    *,
    series_id: str = DEFAULT_SERIES_ID,
    max_staleness_days: int = DEFAULT_MAX_STALENESS_DAYS,
) -> pd.Series:
    """从 risk_free_rates 读取与交易日 index 对齐的 rf simple return。"""
    normalized = pd.DatetimeIndex(pd.to_datetime(index)).astype("datetime64[ns]")
    if normalized.empty:
        return pd.Series(dtype="float64", index=normalized, name=series_id.upper())
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
        params={"series_id": series_id.upper(), "end": normalized.max().date()},
        parse_dates=["date"],
    )
    if rows.empty:
        raise LookupError(f"risk_free_rates has no {series_id.upper()} rows; run update_risk_free_rates or pass --no-risk-free")
    dates = [pd.Timestamp(value).date() for value in rows["date"]]
    rates = [Decimal(str(value)) for value in rows["rate_pct"]]
    values: list[float] = []
    prev_date: date | None = None
    for ts in normalized:
        current = ts.date()
        index_pos = bisect_right(dates, current) - 1
        if index_pos < 0 or (current - dates[index_pos]).days > max_staleness_days:
            raise LookupError(f"risk_free_rates has no fresh {series_id.upper()} row for {current}")
        days = 1 if prev_date is None else max((current - prev_date).days, 1)
        values.append(float(rate_pct_to_simple_return(rates[index_pos], days=days)))
        prev_date = current
    return pd.Series(values, index=normalized, dtype="float64", name=series_id.upper())
