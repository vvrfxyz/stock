"""研究层 PIT insider_net_buy 面板（insider_transactions）。

insider_net_buy = 过去 90 天内部人净买入股数。
只取开放市场买卖（transaction_code in ('P','S')），
按 transaction_acquired_disposed 定方向（'A'=买入为正，'D'=卖出为负），
以 filing_date 作可见日（Form 4 申报后才可见），交易日聚合后取 90 日滚动和。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_COLUMNS = ["security_id", "visible_date", "transaction_date", "signed_shares"]


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "transaction_date": pd.Series(dtype="datetime64[ns]"),
            "signed_shares": pd.Series(dtype=np.float64),
        }
    )


def load_insider_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 insider_transactions 的签名股数事件流。"""
    if security_ids is not None and not security_ids:
        return _empty_events()
    sql = text(
        """
        select security_id,
               filing_date as visible_date,
               transaction_date,
               case when transaction_acquired_disposed = 'A'
                    then transaction_shares::float8
                    else -transaction_shares::float8
               end as signed_shares
        from insider_transactions
        where security_id is not null
          and filing_date is not null
          and transaction_date is not null
          and transaction_code in ('P', 'S')
          and transaction_shares is not null
          and transaction_shares > 0
          and transaction_acquired_disposed in ('A', 'D')
          and transaction_date >= '1990-01-01'
          and (cast(:security_ids as bigint[]) is null
               or security_id = any(cast(:security_ids as bigint[])))
        order by security_id, filing_date, transaction_date
        """
    )
    events = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "transaction_date"],
    )
    if events.empty:
        return _empty_events()
    events = _to_ns(events, ("visible_date", "transaction_date"))
    events["security_id"] = events["security_id"].astype(np.int64)
    events["signed_shares"] = events["signed_shares"].astype(np.float64)
    return events[_COLUMNS]


def load_insider_net_buy_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    visible_delay_days: int = 1,
    window_days: int = 90,
) -> pd.DataFrame:
    """一站式加载，返回 90 日内部人净买入股数宽表。

    可见性按 filing_date + visible_delay_days：每个 date t 累计的是
    "可见日 <= t 且 t - 可见日 < window_days" 的签名股数之和。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    universe = (
        requested_security_ids
        if requested_security_ids is not None
        else pd.Index([], dtype=np.int64)
    )

    events = load_insider_events(engine, security_ids=security_ids)
    if len(dates) == 0 or events.empty:
        cols = universe
        if requested_security_ids is None and not events.empty:
            cols = pd.Index(events["security_id"].unique(), dtype=np.int64).sort_values()
        return pd.DataFrame(index=dates, columns=cols, dtype=np.float64)

    events = events.copy()
    events["effective_visible_date"] = events["visible_date"] + pd.Timedelta(days=visible_delay_days)

    if requested_security_ids is None:
        universe = pd.Index(events["security_id"].unique(), dtype=np.int64).sort_values()

    # 每个 date t 的窗口和 = (可见日 <= t 的累计) - (可见日 <= t - window 的累计)。
    # 用两次 merge_asof（backward）在每证券上取截至 t 与截至 t-window 的前缀和差。
    window = pd.Timedelta(days=window_days)
    events = events.sort_values(["security_id", "effective_visible_date"], kind="mergesort")
    events["cum_shares"] = events.groupby("security_id")["signed_shares"].cumsum()
    ev_sorted = events[["security_id", "effective_visible_date", "cum_shares"]].sort_values(
        "effective_visible_date", kind="mergesort"
    )

    grid = pd.DataFrame(
        {
            "pos": np.arange(len(dates) * len(universe)),
            "date": np.repeat(dates.to_numpy(), len(universe)),
            "security_id": np.tile(universe.to_numpy(), len(dates)),
            "window_start": np.repeat((dates - window).to_numpy(), len(universe)),
        }
    )

    upper_df = pd.merge_asof(
        grid.sort_values("date", kind="mergesort"),
        ev_sorted,
        left_on="date",
        right_on="effective_visible_date",
        by="security_id",
        direction="backward",
    )
    lower_df = pd.merge_asof(
        grid.sort_values("window_start", kind="mergesort"),
        ev_sorted,
        left_on="window_start",
        right_on="effective_visible_date",
        by="security_id",
        direction="backward",
    )
    upper = upper_df.set_index("pos")["cum_shares"].reindex(grid["pos"]).to_numpy()
    lower = lower_df.set_index("pos")["cum_shares"].reindex(grid["pos"]).to_numpy()

    # upper/lower 都是 NaN 表示该证券无任何 insider event——应保持 NaN 而非填 0
    has_data = ~(np.isnan(upper) & np.isnan(lower))
    net = np.where(has_data, np.nan_to_num(upper, nan=0.0) - np.nan_to_num(lower, nan=0.0), np.nan)
    out = grid.assign(net=net)
    panel = out.pivot_table(index="date", columns="security_id", values="net", aggfunc="last")
    return panel.reindex(index=dates, columns=universe).astype(np.float64)
