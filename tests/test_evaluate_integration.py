from __future__ import annotations

import importlib.util
from datetime import date

import pandas as pd
import pytest
from sqlalchemy import text

from research.evaluate import run_evaluation


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(importlib.util.find_spec("pyarrow") is None, reason="pyarrow is not installed"),
]


@pytest.fixture(autouse=True)
def _fresh_panel_cache():
    """load_adjusted_panel 进程内缓存假设同 URL 数据不变；集成测试同库换数据，必须清。"""
    from research.data import clear_panel_cache

    clear_panel_cache()
    yield
    clear_panel_cache()


def _insert_security(pg_db, security_id: int, symbol: str, *, is_active: bool = True, sec_type: str = "CS") -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, market, type, is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, 'US', :type, :is_active, 30)
                """
            ),
            {"id": security_id, "symbol": symbol, "type": sec_type, "is_active": is_active},
        )
        conn.commit()


def _insert_price(pg_db, security_id: int, date_str: str, close: float, volume: float = 1000.0) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into daily_prices
                    (security_id, date, open, high, low, close, volume)
                values
                    (:security_id, :date, :close, :close, :close, :close, :volume)
                """
            ),
            {"security_id": security_id, "date": date_str, "close": close, "volume": volume},
        )
        conn.commit()


def _insert_share(pg_db, security_id: int, filing_date: str, shares: int) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into historical_shares
                    (security_id, filing_date, period_end_date, total_shares, source)
                values
                    (:security_id, :filing_date, :filing_date, :shares, 'TEST')
                """
            ),
            {"security_id": security_id, "filing_date": filing_date, "shares": shares},
        )
        conn.commit()


def _insert_net_income(pg_db, security_id: int, value: float, filed_date: str) -> None:
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
                     :accession_number, :filed_date)
                """
            ),
            {
                "security_id": security_id,
                "cik": f"{security_id:010d}",
                "value": value,
                "accession_number": f"0000-26-{security_id:06d}",
                "filed_date": filed_date,
            },
        )
        conn.commit()


def test_earnings_yield_full_pipeline(pg_db, tmp_path):
    for sid, symbol in ((1, "aaa"), (2, "bbb"), (3, "ccc")):
        _insert_security(pg_db, sid, symbol)
        _insert_share(pg_db, sid, "2025-12-31", 10)
        _insert_net_income(pg_db, sid, float(sid * 100), "2026-01-01")
    for day in pd.bdate_range("2026-01-02", periods=20):
        for sid in (1, 2, 3):
            _insert_price(pg_db, sid, day.date().isoformat(), 10.0 + sid)

    result = run_evaluation(
        "earnings_yield",
        engine=pg_db.engine,
        start=date(2026, 1, 2),
        end=date(2026, 1, 29),
        as_of=date(2026, 1, 29),
        eval_start=date(2026, 1, 2),
        horizons=(1,),
        n_quantiles=5,
        trials_path=tmp_path / "trials.parquet",
        min_median_dollar_volume=1,
        eligibility_window=1,
        risk_free_series=None,
    )

    assert result.ic_table.shape[0] == 1
    assert result.coverage["pit_violations"].max() == 0
    assert (tmp_path / "trials.parquet").exists()


def test_size_full_pipeline_with_output_dir(pg_db, tmp_path):
    for sid, symbol in ((1, "aaa"), (2, "bbb"), (3, "ccc")):
        _insert_security(pg_db, sid, symbol)
        _insert_share(pg_db, sid, "2025-12-31", 10)
    for day in pd.bdate_range("2026-01-02", periods=20):
        for sid in (1, 2, 3):
            _insert_price(pg_db, sid, day.date().isoformat(), 10.0 + sid)

    result = run_evaluation(
        "size",
        engine=pg_db.engine,
        start=date(2026, 1, 2),
        end=date(2026, 1, 29),
        eval_start=date(2026, 1, 2),
        horizons=(1,),
        trials_path=tmp_path / "nested" / "trials.parquet",
        min_median_dollar_volume=1,
        eligibility_window=1,
        risk_free_series=None,
    )

    assert result.status == "ok"
    assert (tmp_path / "nested" / "trials.parquet").exists()
