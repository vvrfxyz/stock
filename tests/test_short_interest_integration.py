from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.short_interest import load_short_interest_events


def _insert_security(pg_db, security_id: int, symbol: str) -> None:
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


def _insert_short_interest(
    pg_db,
    security_id: int,
    settlement_date: str,
    short_interest: int,
    source: str,
    created_at: str,
) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into short_interests
                    (security_id, ticker, settlement_date, short_interest, source, created_at)
                values
                    (:security_id, 'aapl', :settlement_date, :short_interest, :source, :created_at)
                """
            ),
            {
                "security_id": security_id,
                "settlement_date": settlement_date,
                "short_interest": short_interest,
                "source": source,
                "created_at": created_at,
            },
        )
        conn.commit()


@pytest.mark.integration
def test_load_short_interest_events_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_short_interest(pg_db, 1, "2026-01-15", 100, "MASSIVE", "2026-01-20 00:00:00+00")
    _insert_short_interest(pg_db, 1, "2026-01-15", 150, "TEST", "2026-01-21 00:00:00+00")
    _insert_short_interest(pg_db, 1, "2026-01-31", 200, "MASSIVE", "2026-02-05 00:00:00+00")

    events = load_short_interest_events(pg_db.engine)

    assert list(events.columns) == ["security_id", "visible_date", "settlement_date", "short_interest"]
    assert len(events) == 2
    assert events["security_id"].dtype == np.int64
    assert str(events["visible_date"].dtype) == "datetime64[ns]"
    assert str(events["settlement_date"].dtype) == "datetime64[ns]"
    assert events["short_interest"].dtype == np.int64
    assert events["visible_date"].tolist() == [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-01-31")]
    assert events["settlement_date"].tolist() == [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-01-31")]
    assert events["short_interest"].tolist() == [150, 200]

    filtered = load_short_interest_events(pg_db.engine, security_ids=[1])
    assert filtered["short_interest"].tolist() == [150, 200]

    missing = load_short_interest_events(pg_db.engine, security_ids=[999])
    assert list(missing.columns) == ["security_id", "visible_date", "settlement_date", "short_interest"]
    assert missing.empty

    empty = load_short_interest_events(object(), security_ids=[])
    assert list(empty.columns) == ["security_id", "visible_date", "settlement_date", "short_interest"]
    assert empty.empty
