"""research.market_cap 的 PIT 市值面板语义测试。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.market_cap import (
    compute_market_cap_panel,
    load_market_cap_panel,
    load_shares_events,
)


def _events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "period_end_date", "total_shares"])
    for col in ("visible_date", "period_end_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _prices(dates, data) -> pd.DataFrame:
    return pd.DataFrame(data, index=pd.DatetimeIndex(pd.to_datetime(dates)))


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


def _insert_share(pg_db, security_id, filing_date, period_end_date, total_shares, source="MASSIVE"):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into historical_shares
                    (security_id, filing_date, period_end_date, total_shares, source)
                values
                    (:security_id, :filing_date, :period_end_date, :total_shares, :source)
                """
            ),
            {
                "security_id": security_id,
                "filing_date": filing_date,
                "period_end_date": period_end_date,
                "total_shares": total_shares,
                "source": source,
            },
        )
        conn.commit()


def _insert_price(pg_db, security_id, date, close):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into daily_prices
                    (security_id, date, open, high, low, close, volume)
                values
                    (:security_id, :date, :close, :close, :close, :close, 100)
                """
            ),
            {"security_id": security_id, "date": date, "close": close},
        )
        conn.commit()


def test_pit_does_not_leak_future_shares():
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-31", "2026-06-09", "2026-06-10", "2026-06-15"]))
    events = _events(
        (1, "2025-01-15", "2024-12-31", 1_000_000),
        (1, "2026-06-10", "2026-03-31", 2_000_000),
    )
    prices = _prices(dates, {1: [10.0, 11.0, 12.0, 13.0]})

    panel = compute_market_cap_panel(events, prices, dates, 600, 0)

    assert panel.loc["2025-12-31", 1] == 10_000_000.0
    assert panel.loc["2026-06-09", 1] == 11_000_000.0
    assert panel.loc["2026-06-10", 1] == 24_000_000.0
    assert panel.loc["2026-06-15", 1] == 26_000_000.0


def test_stale_shares_become_nan():
    dates = pd.DatetimeIndex(pd.to_datetime(["2024-06-01", "2025-01-15", "2025-03-15"]))
    events = _events((1, "2024-01-01", "2023-12-31", 1_000_000))
    prices = _prices(dates, {1: [2.0, 3.0, 4.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0)

    assert panel.loc["2024-06-01", 1] == 2_000_000.0
    assert panel.loc["2025-01-15", 1] == 3_000_000.0
    assert np.isnan(panel.loc["2025-03-15", 1])


def test_missing_security_returns_all_nan():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02", "2026-01-05"]))
    prices = pd.DataFrame(index=dates, columns=pd.Index([999], dtype=np.int64), dtype=np.float64)

    panel = compute_market_cap_panel(_events(), prices, dates, 400, 0)

    assert panel.columns.tolist() == [999]
    assert panel[999].isna().all()


def test_security_without_prices_returns_all_nan():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02", "2026-01-05"]))
    events = _events((1, "2026-01-01", "2025-12-31", 1_000_000))
    prices = pd.DataFrame(index=dates)

    panel = compute_market_cap_panel(events, prices, dates, 400, 0)

    assert panel.columns.tolist() == [1]
    assert panel[1].isna().all()


def test_market_cap_unit_uses_raw_close_not_adjusted():
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-05-30", "2025-06-02"]))
    events = _events(
        (1, "2025-01-01", "2024-12-31", 1_000_000),
        (1, "2025-06-01", "2025-05-31", 2_000_000),
    )
    prices = _prices(dates, {1: [100.0, 50.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0)

    assert panel.loc["2025-05-30", 1] == 100_000_000.0
    assert panel.loc["2025-06-02", 1] == 100_000_000.0


def test_nan_total_shares_event_is_missing():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-15"]))
    events = _events(
        (1, "2026-01-01", "2025-12-31", 1_000_000),
        (1, "2026-06-10", "2026-03-31", np.nan),
    )
    prices = _prices(dates, {1: [10.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0)

    assert panel.loc["2026-06-15", 1] == 10_000_000.0


@pytest.mark.integration
def test_load_shares_events_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_share(pg_db, 1, "2025-01-15", "2024-12-31", 1_000_000, "MASSIVE")
    _insert_share(pg_db, 1, "2025-04-15", "2025-03-31", 1_100_000, "POLYGON")
    _insert_share(pg_db, 1, "2025-07-15", "2025-06-30", 1_200_000, "MASSIVE")

    events = load_shares_events(pg_db.engine)

    assert list(events.columns) == ["security_id", "visible_date", "period_end_date", "total_shares"]
    assert len(events) == 3
    assert events["security_id"].dtype == np.int64
    assert str(events["visible_date"].dtype) == "datetime64[ns]"
    assert str(events["period_end_date"].dtype) == "datetime64[ns]"
    assert events["total_shares"].dtype == np.int64
    assert events["visible_date"].tolist() == [
        pd.Timestamp("2025-01-15"),
        pd.Timestamp("2025-04-15"),
        pd.Timestamp("2025-07-15"),
    ]


@pytest.mark.integration
def test_load_market_cap_panel_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_share(pg_db, 1, "2025-01-02", "2024-12-31", 1_000_000)
    _insert_share(pg_db, 1, "2025-01-05", "2025-01-04", 2_000_000)
    for date, close in (
        ("2025-01-01", 9.0),
        ("2025-01-02", 10.0),
        ("2025-01-04", 11.0),
        ("2025-01-05", 12.0),
        ("2025-01-06", 13.0),
    ):
        _insert_price(pg_db, 1, date, close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-04", "2025-01-05", "2025-01-06"]))

    panel = load_market_cap_panel(pg_db.engine, dates=dates)

    assert np.isnan(panel.loc["2025-01-01", 1])
    assert panel.loc["2025-01-02", 1] == 10_000_000.0
    assert panel.loc["2025-01-04", 1] == 11_000_000.0
    assert panel.loc["2025-01-05", 1] == 24_000_000.0
    assert panel.loc["2025-01-06", 1] == 26_000_000.0
