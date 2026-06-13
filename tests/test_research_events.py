"""research.events 的财报事件日历语义测试。"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.events import _event_visible_at, attach_event_to_returns, load_earnings_events


def _event(accession, security_id, visible_at):
    return {
        "accession_number": accession,
        "security_id": security_id,
        "event_visible_at": pd.Timestamp(visible_at),
    }


def _returns() -> pd.DataFrame:
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-08", "2026-06-09", "2026-06-10", "2026-06-11"]))
    return pd.DataFrame({1: [0.01, 0.02, 0.03, 0.04]}, index=dates.tz_localize("UTC"))


def _insert_security(pg_db, security_id, symbol):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, market, type, is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, 'US', 'CS', true, 30)
                """
            ),
            {"id": security_id, "symbol": symbol},
        )
        conn.commit()


def _insert_filing(
    pg_db,
    accession_number,
    form_type,
    filing_date,
    *,
    security_id=1,
    accepted_at=None,
    source="SEC_EDGAR",
):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into sec_filings
                    (security_id, source, accession_number, form_type, filing_date, accepted_at)
                values
                    (:security_id, :source, :accession_number, :form_type, :filing_date, :accepted_at)
                """
            ),
            {
                "security_id": security_id,
                "source": source,
                "accession_number": accession_number,
                "form_type": form_type,
                "filing_date": filing_date,
                "accepted_at": accepted_at,
            },
        )
        conn.commit()


def test_visibility_after_hours_pushes_to_next_open():
    visible = _event_visible_at(pd.Timestamp("2026-06-10 21:00:00Z"), date(2026, 6, 10))
    assert visible == pd.Timestamp("2026-06-11 13:30:00Z")


def test_visibility_during_market_hours_uses_accepted_at():
    visible = _event_visible_at(pd.Timestamp("2026-06-10 17:30:00Z"), date(2026, 6, 10))
    assert visible == pd.Timestamp("2026-06-10 17:30:00Z")


def test_visibility_weekend_pushes_to_monday():
    saturday = _event_visible_at(pd.Timestamp("2026-06-13 17:30:00Z"), date(2026, 6, 13))
    sunday = _event_visible_at(pd.Timestamp("2026-06-14 17:30:00Z"), date(2026, 6, 14))
    assert saturday == pd.Timestamp("2026-06-15 13:30:00Z")
    assert sunday == pd.Timestamp("2026-06-15 13:30:00Z")


def test_visibility_null_accepted_at_uses_filing_date_next_open():
    visible = _event_visible_at(None, date(2026, 6, 10))
    assert visible == pd.Timestamp("2026-06-11 13:30:00Z")


def test_visibility_nan_accepted_at_uses_filing_date_next_open():
    visible = _event_visible_at(np.nan, date(2026, 6, 10))
    assert visible == pd.Timestamp("2026-06-11 13:30:00Z")


def test_attach_event_to_returns_relative_day_zero_is_visible_day():
    events = pd.DataFrame([_event("a1", 1, "2026-06-10 21:00:00Z")])
    attached = attach_event_to_returns(events, _returns(), window=(-1, 1))
    row = attached[attached["relative_day"] == 0].iloc[0]
    assert row["event_date"] == pd.Timestamp("2026-06-10")
    assert row["return"] == 0.03


def test_attach_event_to_returns_window_bounds():
    events = pd.DataFrame([_event("a1", 1, "2026-06-10 21:00:00Z")])
    attached = attach_event_to_returns(events, _returns(), window=(-2, 3))
    assert len(attached) == 6
    assert attached["relative_day"].tolist() == [-2, -1, 0, 1, 2, 3]


def test_attach_event_to_returns_missing_returns_become_nan():
    events = pd.DataFrame([_event("a1", 2, "2026-06-10 21:00:00Z")])
    attached = attach_event_to_returns(events, _returns(), window=(-1, 1))
    assert attached["return"].isna().all()


@pytest.mark.integration
def test_load_earnings_events_basic(pg_db):
    for security_id, symbol in [(1, "aapl"), (2, "msft"), (3, "nvda")]:
        _insert_security(pg_db, security_id, symbol)
    _insert_filing(pg_db, "a1", "10-K", date(2026, 2, 1), security_id=1)
    _insert_filing(pg_db, "a2", "10-Q", date(2026, 5, 1), security_id=1)
    _insert_filing(pg_db, "a3", "10-K", date(2026, 2, 2), security_id=2)
    _insert_filing(pg_db, "a4", "10-Q", date(2026, 5, 2), security_id=3)
    _insert_filing(pg_db, "a5", "8-K", date(2026, 6, 1), security_id=3)

    events = load_earnings_events(pg_db.engine)
    eight_k = load_earnings_events(pg_db.engine, form_types=("8-K",))

    assert len(events) == 4
    assert len(eight_k) == 1
    assert list(events.columns) == [
        "security_id",
        "accession_number",
        "form_type",
        "filing_date",
        "accepted_at",
        "period_of_report",
        "event_visible_at",
    ]
    assert eight_k.iloc[0]["accession_number"] == "a5"


@pytest.mark.integration
def test_load_earnings_events_skips_null_security_id(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_filing(pg_db, "a1", "10-Q", date(2026, 5, 1), security_id=1)
    _insert_filing(pg_db, "a2", "10-Q", date(2026, 5, 2), security_id=None)

    events = load_earnings_events(pg_db.engine)

    assert events["accession_number"].tolist() == ["a1"]


@pytest.mark.integration
def test_since_until_window(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_filing(pg_db, "before", "10-Q", date(2026, 4, 30), security_id=1)
    _insert_filing(pg_db, "inside", "10-Q", date(2026, 5, 1), security_id=1)
    _insert_filing(pg_db, "after", "10-Q", date(2026, 5, 2), security_id=1)

    events = load_earnings_events(pg_db.engine, since=date(2026, 5, 1), until=date(2026, 5, 1))

    assert events["accession_number"].tolist() == ["inside"]
