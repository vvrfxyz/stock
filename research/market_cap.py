"""研究层 PIT 市值面板：raw close × historical_shares.total_shares。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_SHARES_COLUMNS = ["security_id", "visible_date", "period_end_date", "total_shares"]


def _empty_shares_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "period_end_date": pd.Series(dtype="datetime64[ns]"),
            "total_shares": pd.Series(dtype=np.int64),
        }
    )


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """统一到 ns 精度，避免 merge_asof 两侧 dtype 不一致。"""
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def load_shares_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 historical_shares 的 PIT 可见事件流。"""
    if security_ids is not None and not security_ids:
        return _empty_shares_events()
    sql = text(
        """
        select security_id, filing_date as visible_date, period_end_date, total_shares
        from historical_shares
        where total_shares is not null
          and (:security_ids is null or security_id = any(:security_ids))
        order by security_id, filing_date, period_end_date
        """
    )
    events = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "period_end_date"],
    )
    if events.empty:
        return _empty_shares_events()
    events = _to_ns(events, ("visible_date", "period_end_date"))
    events["security_id"] = events["security_id"].astype(np.int64)
    events["total_shares"] = events["total_shares"].astype(np.int64)
    return events[_SHARES_COLUMNS]


def _load_raw_close_wide(
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
        select security_id, date, close::float8 as close
        from daily_prices
        where date = any(:dates)
          and close is not null
          {id_clause}
        order by security_id, date
        """
    )
    params: dict[str, object] = {"dates": [ts.date() for ts in dates]}
    if security_ids is not None:
        params["security_ids"] = security_ids
    prices = pd.read_sql_query(sql, engine, params=params, parse_dates=["date"])
    if prices.empty:
        columns = pd.Index(security_ids or [], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)
    prices = _to_ns(prices, ("date",))
    prices["security_id"] = prices["security_id"].astype(np.int64)
    wide = prices.pivot_table(index="date", columns="security_id", values="close", aggfunc="last")
    if security_ids is not None:
        wide = wide.reindex(columns=pd.Index(security_ids, dtype=np.int64))
    return wide.reindex(dates).astype(np.float64)


def _coerce_security_columns(columns: pd.Index) -> pd.Index:
    return pd.Index([int(col) for col in columns], dtype=np.int64)


def compute_market_cap_panel(
    events: pd.DataFrame,
    prices_wide: pd.DataFrame,
    dates: pd.DatetimeIndex,
    max_staleness_days: int,
    visible_delay_days: int,
) -> pd.DataFrame:
    """合成事件与 raw close 宽表，计算 PIT 市值宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    prices = prices_wide.copy()
    prices.index = pd.DatetimeIndex(pd.to_datetime(prices.index)).astype("datetime64[ns]")
    prices.columns = _coerce_security_columns(prices.columns)

    ev = events.reindex(columns=_SHARES_COLUMNS).copy()
    if not ev.empty:
        ev = _to_ns(ev, ("visible_date", "period_end_date"))
        ev = ev[pd.notna(ev["security_id"]) & pd.notna(ev["visible_date"])]
        ev = ev[pd.notna(ev["total_shares"])]
        ev["security_id"] = ev["security_id"].astype(np.int64)
        ev["total_shares"] = ev["total_shares"].astype(np.float64)

    event_ids = pd.Index(ev["security_id"].unique(), dtype=np.int64) if not ev.empty else pd.Index([], dtype=np.int64)
    security_ids = event_ids.union(prices.columns).sort_values()
    prices = prices.reindex(index=dates, columns=security_ids).astype(np.float64)
    if len(dates) == 0 or len(security_ids) == 0:
        return pd.DataFrame(index=dates, columns=security_ids, dtype=np.float64)

    shares = pd.DataFrame(np.nan, index=dates, columns=security_ids, dtype=np.float64)
    if not ev.empty:
        ev["effective_visible_date"] = ev["visible_date"] + pd.Timedelta(days=visible_delay_days)
        grid = pd.DataFrame(
            {
                "date": np.repeat(dates.to_numpy(), len(security_ids)),
                "security_id": np.tile(security_ids.to_numpy(), len(dates)),
            }
        )
        joined = pd.merge_asof(
            grid.sort_values("date"),
            ev.sort_values(
                ["effective_visible_date", "period_end_date"], kind="mergesort"
            ),
            left_on="date",
            right_on="effective_visible_date",
            by="security_id",
            direction="backward",
        )
        stale = joined["effective_visible_date"] < joined["date"] - pd.Timedelta(
            days=max_staleness_days
        )
        joined.loc[stale, "total_shares"] = np.nan
        shares = joined.pivot_table(
            index="date", columns="security_id", values="total_shares", aggfunc="last"
        ).reindex(index=dates, columns=security_ids)

    return (prices * shares).astype(np.float64)


def load_market_cap_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 400,
    visible_delay_days: int = 0,
) -> pd.DataFrame:
    """一站式加载 raw close 与 PIT shares，返回市值宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    events = load_shares_events(engine, security_ids=security_ids)
    prices = _load_raw_close_wide(engine, dates=dates, security_ids=security_ids)
    return compute_market_cap_panel(
        events,
        prices,
        dates,
        max_staleness_days=max_staleness_days,
        visible_delay_days=visible_delay_days,
    )


def load_log_market_cap_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 400,
    visible_delay_days: int = 0,
) -> pd.DataFrame:
    """返回 log(PIT 市值)，非正数与缺失值保留为 NaN。"""
    market_cap = load_market_cap_panel(
        engine,
        dates=dates,
        security_ids=security_ids,
        max_staleness_days=max_staleness_days,
        visible_delay_days=visible_delay_days,
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(market_cap.where(market_cap > 0))
