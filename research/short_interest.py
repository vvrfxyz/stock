"""研究层 PIT short_interest_ratio 面板。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel
from research.market_cap import load_shares_events

# FINRA 半月度空头仓位报告在结算日后约 8 个工作日（BD+8）才公布，
# 统一取 14 个自然日作为 PIT 可见延迟兜底；所有消费 short_interests 的因子共用此常量。
SHORT_INTEREST_VISIBLE_DELAY_DAYS = 14

_SHORT_INTEREST_COLUMNS = ["security_id", "visible_date", "settlement_date", "short_interest"]
_SHARES_COLUMNS = ["security_id", "visible_date", "period_end_date", "total_shares"]


def _empty_short_interest_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "settlement_date": pd.Series(dtype="datetime64[ns]"),
            "short_interest": pd.Series(dtype=np.int64),
        }
    )


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def load_short_interest_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 short_interests 的 PIT 可见事件流。"""
    if security_ids is not None and not security_ids:
        return _empty_short_interest_events()
    sql = text(
        """
        select distinct on (security_id, settlement_date)
               security_id,
               settlement_date as visible_date,
               settlement_date,
               short_interest
        from short_interests
        where short_interest is not null
          and (cast(:security_ids as bigint[]) is null or security_id = any(cast(:security_ids as bigint[])))
        order by security_id, settlement_date, created_at desc, id desc
        """
    )
    events = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "settlement_date"],
    )
    if events.empty:
        return _empty_short_interest_events()
    events = _to_ns(events, ("visible_date", "settlement_date"))
    events["security_id"] = events["security_id"].astype(np.int64)
    events["short_interest"] = events["short_interest"].astype(np.int64)
    return events[_SHORT_INTEREST_COLUMNS]


def compute_short_interest_ratio_panel(
    events: pd.DataFrame,
    shares_events: pd.DataFrame,
    dates: pd.DatetimeIndex,
    *,
    visible_delay_days: int,
    si_max_staleness_days: int,
    shares_max_staleness_days: int,
) -> pd.DataFrame:
    """合成 SI 事件与 shares 事件，计算 short_interest / total_shares 比率宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")

    ev = events.reindex(columns=_SHORT_INTEREST_COLUMNS).copy()
    if not ev.empty:
        ev = _to_ns(ev, ("visible_date", "settlement_date"))
        ev = ev[pd.notna(ev["security_id"]) & pd.notna(ev["visible_date"])]
        ev = ev[pd.notna(ev["short_interest"])]
        ev["security_id"] = ev["security_id"].astype(np.int64)
        ev["short_interest"] = ev["short_interest"].astype(np.float64)
    ev["effective_visible_date"] = ev["visible_date"] + pd.Timedelta(days=visible_delay_days)

    shares = shares_events.reindex(columns=_SHARES_COLUMNS).copy()
    if not shares.empty:
        shares = _to_ns(shares, ("visible_date", "period_end_date"))
        shares["security_id"] = shares["security_id"].astype(np.int64)
        shares["total_shares"] = shares["total_shares"].astype(np.float64)

    si_ids = pd.Index(ev["security_id"].unique(), dtype=np.int64) if not ev.empty else pd.Index([], dtype=np.int64)
    share_ids = (
        pd.Index(shares["security_id"].unique(), dtype=np.int64)
        if not shares.empty
        else pd.Index([], dtype=np.int64)
    )
    security_ids = si_ids.union(share_ids).sort_values()

    if len(dates) == 0 or len(security_ids) == 0:
        return pd.DataFrame(index=dates, columns=security_ids, dtype=np.float64)

    short_interest = event_table_to_asof_panel(
        ev,
        dates=dates,
        value_column="short_interest",
        visible_date_column="visible_date",
        staleness_anchor_column="effective_visible_date",
        visible_delay_days=visible_delay_days,
        max_staleness_days=si_max_staleness_days,
        security_universe=security_ids,
    )
    total_shares = event_table_to_asof_panel(
        shares,
        dates=dates,
        value_column="total_shares",
        visible_date_column="visible_date",
        staleness_anchor_column="visible_date",
        visible_delay_days=0,
        max_staleness_days=shares_max_staleness_days,
        security_universe=security_ids,
    )
    return (short_interest / total_shares.where(total_shares > 0)).astype(np.float64)


def load_short_interest_ratio_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    visible_delay_days: int = SHORT_INTEREST_VISIBLE_DELAY_DAYS,
    si_max_staleness_days: int = 30,
    shares_max_staleness_days: int = 400,
) -> pd.DataFrame:
    """一站式加载，返回 short_interest_ratio 宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    if security_ids is not None and not security_ids:
        return pd.DataFrame(index=dates, columns=pd.Index([], dtype=np.int64), dtype=np.float64)
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    events = load_short_interest_events(engine, security_ids=security_ids)
    shares_events = load_shares_events(engine, security_ids=security_ids)
    panel = compute_short_interest_ratio_panel(
        events,
        shares_events,
        dates,
        visible_delay_days=visible_delay_days,
        si_max_staleness_days=si_max_staleness_days,
        shares_max_staleness_days=shares_max_staleness_days,
    )
    if requested_security_ids is not None:
        return panel.reindex(columns=requested_security_ids).astype(np.float64)
    return panel
