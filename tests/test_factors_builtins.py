from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.factors.builtins.earnings_yield import EarningsYieldFactor
from research.factors.builtins.size import SizeFactor
from research.factors.protocol import FactorContext


pytestmark = pytest.mark.integration


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


def _insert_share(pg_db, security_id: int, total_shares: int) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into historical_shares
                    (security_id, filing_date, period_end_date, total_shares, source)
                values
                    (:security_id, '2025-12-31', '2025-12-31', :total_shares, 'TEST')
                """
            ),
            {"security_id": security_id, "total_shares": total_shares},
        )
        conn.commit()


def _insert_price(pg_db, security_id: int, date: str, close: float) -> None:
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


def _insert_net_income(pg_db, security_id: int, value: float) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into sec_fundamental_facts
                    (security_id, cik, taxonomy, concept, unit, period_start, period_end,
                     is_instant, value, fiscal_year, fiscal_period, form_type,
                     accession_number, filed_date)
                values
                    (:security_id, :cik, 'us-gaap', 'NetIncomeLoss', 'USD',
                     '2025-01-01', '2025-12-31', false, :value, 2025, 'FY', '10-K',
                     :accession_number, '2026-01-01')
                """
            ),
            {
                "security_id": security_id,
                "cik": f"{security_id:010d}",
                "value": value,
                "accession_number": f"0000-26-{security_id:06d}",
            },
        )
        conn.commit()


def _seed_market_data(pg_db) -> None:
    for security_id, symbol in ((1, "aaa"), (2, "bbb"), (3, "ccc")):
        _insert_security(pg_db, security_id, symbol)
        _insert_share(pg_db, security_id, 10)


def test_size_factor_against_synthetic_panel(pg_db):
    _seed_market_data(pg_db)
    for security_id, date, close in (
        (1, "2026-01-02", 5.0),
        (2, "2026-01-02", 0.0),
        (3, "2026-01-02", -2.0),
        (1, "2026-01-03", 6.0),
        (2, "2026-01-03", 4.0),
        (3, "2026-01-03", 1.0),
    ):
        _insert_price(pg_db, security_id, date, close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02", "2026-01-03"]))
    universe = pd.Index([1, 2, 3], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = SizeFactor().compute(ctx)

    assert panel.shape == (2, 3)
    pd.testing.assert_index_equal(panel.index, dates)
    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel.dtypes.tolist() == [np.float64, np.float64, np.float64]
    assert panel.loc["2026-01-02", 1] == np.log(50.0)
    assert np.isnan(panel.loc["2026-01-02", 2])
    assert np.isnan(panel.loc["2026-01-02", 3])
    assert panel.loc["2026-01-03", 2] == np.log(40.0)


def test_earnings_yield_factor_against_synthetic_panel(pg_db):
    _seed_market_data(pg_db)
    for security_id, value in ((1, 100.0), (2, 40.0), (3, -20.0)):
        _insert_net_income(pg_db, security_id, value)
    for security_id, close in ((1, 5.0), (2, 0.0), (3, 2.0)):
        _insert_price(pg_db, security_id, "2026-01-02", close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([1, 2, 3], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = EarningsYieldFactor().compute(ctx)

    assert panel.shape == (1, 3)
    pd.testing.assert_index_equal(panel.index, dates)
    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel.dtypes.tolist() == [np.float64, np.float64, np.float64]
    assert panel.loc["2026-01-02", 1] == 100.0 / 50.0
    assert np.isnan(panel.loc["2026-01-02", 2])
    assert panel.loc["2026-01-02", 3] == -20.0 / 20.0


def test_factor_outputs_match_universe_columns(pg_db):
    _seed_market_data(pg_db)
    for security_id, value in ((1, 100.0), (2, 80.0), (3, 60.0)):
        _insert_net_income(pg_db, security_id, value)
        _insert_price(pg_db, security_id, "2026-01-02", 5.0)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([1, 2, 3, 999], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    size = SizeFactor().compute(ctx)
    earnings_yield = EarningsYieldFactor().compute(ctx)

    pd.testing.assert_index_equal(size.columns, universe)
    pd.testing.assert_index_equal(earnings_yield.columns, universe)
    assert size[999].isna().all()
    assert earnings_yield[999].isna().all()
