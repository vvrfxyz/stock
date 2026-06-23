"""研究层 PIT short_volume_ratio 面板。

short_volume_ratio = short_volume / total_volume（日频 FINRA 数据）。
与 short_interest_ratio（半月频仓位占比）互补：一个看"流量中做空比例"，
一个看"仓位中做空比例"。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel

_COLUMNS = ["security_id", "visible_date", "trade_date", "short_volume_ratio"]


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "trade_date": pd.Series(dtype="datetime64[ns]"),
            "short_volume_ratio": pd.Series(dtype=np.float64),
        }
    )


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def load_short_volume_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 short_volumes 的 PIT 可见事件流。

    visible_date = date（交易当日），因子调用方再叠加 visible_delay_days
    把可见性推到 T+1。
    用 short_volume / total_volume 自算比率，不依赖 vendor 预算字段。
    """
    if security_ids is not None and not security_ids:
        return _empty_events()
    sql = text(
        """
        select distinct on (security_id, date)
               security_id,
               date as visible_date,
               date as trade_date,
               case when total_volume > 0
                    then short_volume::double precision / total_volume
                    else null
               end as short_volume_ratio
        from short_volumes
        where short_volume is not null
          and total_volume is not null
          and (cast(:security_ids as bigint[]) is null
               or security_id = any(cast(:security_ids as bigint[])))
        order by security_id, date, created_at desc, id desc
        """
    )
    events = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "trade_date"],
    )
    if events.empty:
        return _empty_events()
    events = _to_ns(events, ("visible_date", "trade_date"))
    events["security_id"] = events["security_id"].astype(np.int64)
    events["short_volume_ratio"] = events["short_volume_ratio"].astype(np.float64)
    return events[_COLUMNS]


def compute_short_volume_ratio_panel(
    events: pd.DataFrame,
    dates: pd.DatetimeIndex,
    *,
    visible_delay_days: int,
    max_staleness_days: int,
) -> pd.DataFrame:
    """事件表 -> PIT 宽表。"""
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")

    ev = events.reindex(columns=_COLUMNS).copy()
    if not ev.empty:
        ev = _to_ns(ev, ("visible_date", "trade_date"))
        ev = ev[pd.notna(ev["security_id"]) & pd.notna(ev["visible_date"])]
        ev = ev[pd.notna(ev["short_volume_ratio"])]
        ev["security_id"] = ev["security_id"].astype(np.int64)
        ev["short_volume_ratio"] = ev["short_volume_ratio"].astype(np.float64)

    return event_table_to_asof_panel(
        ev,
        dates=dates,
        value_column="short_volume_ratio",
        visible_date_column="visible_date",
        staleness_anchor_column="visible_date",
        visible_delay_days=visible_delay_days,
        max_staleness_days=max_staleness_days,
    )


def load_short_volume_ratio_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    visible_delay_days: int = 1,
    max_staleness_days: int = 10,
) -> pd.DataFrame:
    """一站式加载，返回 short_volume_ratio 宽表。

    默认 visible_delay_days=1（FINRA T+1 公布），
    max_staleness_days=10（日频，超 10 个交易日未更新视为 stale）。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    if security_ids is not None and not security_ids:
        return pd.DataFrame(
            index=dates, columns=pd.Index([], dtype=np.int64), dtype=np.float64
        )
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    events = load_short_volume_events(engine, security_ids=security_ids)
    panel = compute_short_volume_ratio_panel(
        events,
        dates,
        visible_delay_days=visible_delay_days,
        max_staleness_days=max_staleness_days,
    )
    if requested_security_ids is not None:
        return panel.reindex(columns=requested_security_ids).astype(np.float64)
    return panel
