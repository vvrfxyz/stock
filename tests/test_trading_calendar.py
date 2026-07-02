from datetime import datetime, timezone, date

import pytest

from utils import trading_calendar


def test_missing_exchange_calendar_uses_db_sessions_for_last_completed_day(monkeypatch):
    monkeypatch.setattr(trading_calendar, "xc", None)
    monkeypatch.setattr(trading_calendar, "pd", None)
    monkeypatch.setattr(
        trading_calendar,
        "_get_db_session_dates",
        lambda market: (date(2026, 5, 22), date(2026, 5, 26)),
        raising=False,
    )

    now = datetime(2026, 5, 26, 12, 0, tzinfo=timezone.utc)

    assert trading_calendar.get_last_completed_trading_date("US", now) == date(2026, 5, 22)


def test_missing_exchange_calendar_uses_db_sessions_for_shift(monkeypatch):
    monkeypatch.setattr(trading_calendar, "xc", None)
    monkeypatch.setattr(trading_calendar, "pd", None)
    monkeypatch.setattr(
        trading_calendar,
        "_get_db_session_dates",
        lambda market: (date(2026, 5, 22), date(2026, 5, 26)),
        raising=False,
    )

    assert trading_calendar.shift_trading_date("US", date(2026, 5, 22), 1) == date(2026, 5, 26)


def test_us_last_completed_uses_market_local_date_before_close():
    if trading_calendar.xc is None or trading_calendar.pd is None:
        pytest.skip("exchange_calendars/pandas unavailable")

    now = datetime.fromisoformat("2026-07-01T01:46:00+08:00")

    assert trading_calendar.get_last_completed_trading_date("US", now) == date(2026, 6, 29)
