"""研究层财报事件日历。节假日暂按工作日简化，极少数事件可能早 1-3 天可见。"""
from __future__ import annotations

from datetime import date, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


_EVENT_COLUMNS = [
    "security_id",
    "accession_number",
    "form_type",
    "filing_date",
    "accepted_at",
    "period_of_report",
    "event_visible_at",
]
_ATTACHED_COLUMNS = ["accession_number", "security_id", "event_date", "relative_day", "return"]
_ET = ZoneInfo("America/New_York")
_UTC = ZoneInfo("UTC")
_OPEN_TIME = time(hour=9, minute=30)
_CLOSE_TIME = time(hour=16, minute=0)


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "accession_number": pd.Series(dtype="string"),
            "form_type": pd.Series(dtype="string"),
            "filing_date": pd.Series(dtype="datetime64[ns]"),
            "accepted_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "period_of_report": pd.Series(dtype="datetime64[ns]"),
            "event_visible_at": pd.Series(dtype="datetime64[ns, UTC]"),
        },
        columns=_EVENT_COLUMNS,
    )


def _empty_attached() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "accession_number": pd.Series(dtype="string"),
            "security_id": pd.Series(dtype=np.int64),
            "event_date": pd.Series(dtype="datetime64[ns]"),
            "relative_day": pd.Series(dtype=np.int64),
            "return": pd.Series(dtype=np.float64),
        },
        columns=_ATTACHED_COLUMNS,
    )


def _is_weekday(value: date) -> bool:
    return value.weekday() < 5


def _next_weekday(value: date) -> date:
    current = value
    while not _is_weekday(current):
        current += timedelta(days=1)
    return current


def _next_open_after(value: pd.Timestamp) -> pd.Timestamp:
    current_date = value.date()
    if not _is_weekday(current_date):
        current_date = _next_weekday(current_date)
    elif value.time() > _CLOSE_TIME:
        current_date = _next_weekday(current_date + timedelta(days=1))

    return pd.Timestamp.combine(current_date, _OPEN_TIME).tz_localize(_ET).tz_convert("UTC")


def _event_visible_at(accepted_at, filing_date) -> pd.Timestamp:
    """EDGAR 受理时刻 -> 可用于事件研究的 PIT 可见时刻。"""
    if pd.isna(accepted_at):
        # 无 accepted_at 时按 filing_date+1 起算,仍要经 _next_open_after 走 weekday + 收盘后跳天逻辑,
        # 否则周五 filing 会落到周六导致 relative_day=0 含义跟非 NULL 分支不一致。
        fallback = pd.Timestamp.combine(
            pd.Timestamp(filing_date).date() + timedelta(days=1), _OPEN_TIME
        ).tz_localize(_ET)
        return _next_open_after(fallback)

    accepted = pd.Timestamp(accepted_at)
    if accepted.tzinfo is None:
        accepted = accepted.tz_localize(_UTC)
    accepted_et = accepted.tz_convert(_ET)
    if _is_weekday(accepted_et.date()) and _OPEN_TIME <= accepted_et.time() <= _CLOSE_TIME:
        return accepted.tz_convert("UTC")
    return _next_open_after(accepted_et)


def _coerce_event_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_events()
    out = df[_EVENT_COLUMNS].copy()
    out["security_id"] = out["security_id"].astype(np.int64)
    out["accession_number"] = out["accession_number"].astype("string")
    out["form_type"] = out["form_type"].astype("string")
    out["filing_date"] = pd.to_datetime(out["filing_date"]).astype("datetime64[ns]")
    out["accepted_at"] = pd.to_datetime(out["accepted_at"], utc=True)
    out["period_of_report"] = pd.to_datetime(out["period_of_report"]).astype("datetime64[ns]")
    out["event_visible_at"] = pd.to_datetime(out["event_visible_at"], utc=True)
    return out


def load_earnings_events(
    engine: Engine,
    *,
    since: date | None = None,
    until: date | None = None,
    security_ids: list[int] | None = None,
    form_types: tuple[str, ...] = ("10-K", "10-Q", "10-K/A", "10-Q/A"),
) -> pd.DataFrame:
    """从 sec_filings 派生 PIT 财报事件流。"""
    if not form_types or security_ids == []:
        return _empty_events()

    clauses = [
        "source = 'SEC_EDGAR'",
        "security_id is not null",
        "form_type = any(:form_types)",
    ]
    params: dict = {"form_types": list(form_types)}
    if since is not None:
        clauses.append("filing_date >= :since")
        params["since"] = since
    if until is not None:
        clauses.append("filing_date <= :until")
        params["until"] = until
    if security_ids is not None:
        clauses.append("security_id = any(:security_ids)")
        params["security_ids"] = security_ids

    sql = text(
        f"""
        select security_id, accession_number, form_type, filing_date, accepted_at, period_of_report
        from sec_filings
        where {' and '.join(clauses)}
        order by security_id, filing_date, accepted_at
        """
    )
    df = pd.read_sql_query(
        sql,
        engine,
        params=params,
        parse_dates=["filing_date", "accepted_at", "period_of_report"],
    )
    if df.empty:
        return _empty_events()

    df["event_visible_at"] = [
        _event_visible_at(accepted_at, filing_date)
        for accepted_at, filing_date in zip(df["accepted_at"], df["filing_date"], strict=True)
    ]
    return _coerce_event_frame(df)


def _return_dates(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    dates = pd.DatetimeIndex(pd.to_datetime(index))
    if dates.tz is not None:
        dates = dates.tz_convert("UTC").tz_localize(None)
    return dates.normalize()


def attach_event_to_returns(
    events: pd.DataFrame,
    returns: pd.DataFrame,
    *,
    window: tuple[int, int] = (-5, 20),
) -> pd.DataFrame:
    """事件对齐日频收益。relative_day=0 是事件可见日 close 的收益，不能含公告前信息。"""
    if events.empty:
        return _empty_attached()

    returns = returns.sort_index()
    return_dates = _return_dates(returns.index)
    pre, post = window
    rows = []
    for event in events.itertuples(index=False):
        security_id = int(event.security_id)
        event_date = pd.Timestamp(event.event_visible_at).date()
        base_index = int(return_dates.searchsorted(pd.Timestamp(event_date), side="left"))
        has_column = security_id in returns.columns
        for relative_day in range(pre, post + 1):
            pos = base_index + relative_day
            value = np.nan
            if has_column and 0 <= pos < len(returns):
                value = returns.iloc[pos][security_id]
            rows.append(
                {
                    "accession_number": event.accession_number,
                    "security_id": security_id,
                    "event_date": pd.Timestamp(event_date),
                    "relative_day": relative_day,
                    "return": value,
                }
            )

    out = pd.DataFrame(rows, columns=_ATTACHED_COLUMNS)
    out["accession_number"] = out["accession_number"].astype("string")
    out["security_id"] = out["security_id"].astype(np.int64)
    out["event_date"] = pd.to_datetime(out["event_date"]).astype("datetime64[ns]")
    out["relative_day"] = out["relative_day"].astype(np.int64)
    out["return"] = out["return"].astype(np.float64)
    return out
