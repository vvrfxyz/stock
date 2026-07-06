from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.factors.builtins.earnings_yield import EarningsYieldFactor
from research.factors.builtins.size import SizeFactor
from research.factors.protocol import FactorContext


pytestmark = pytest.mark.integration


def _insert_security(pg_db, security_id: int, symbol: str, *, name: str | None = None,
                     company_id: int | None = None, cik: str | None = None) -> None:
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, name, market, type, cik, company_id,
                     is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, :name, 'US', 'CS', :cik, :company_id, true, 30)
                """
            ),
            {"id": security_id, "symbol": symbol, "name": name,
             "cik": cik, "company_id": company_id},
        )
        conn.commit()


def _insert_company(pg_db, cik: str, name: str) -> int:
    pg_db.upsert_companies([{"cik": cik, "name": name}])
    return pg_db.get_company_id_by_cik(cik)


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


def test_earnings_yield_company_level_join(pg_db):
    """B1 接线：公司成员拿公司级 ni/公司级合并市值，无 company 证券保持旧口径。

    公司 77 三名成员：锚 sid=1（事实全挂它）、非锚 sid=2（零事实——goog 型）、
    工具行误标 sid=4（notes 名称，市值绝不进公司分母）。sid=3 无 company_id。
    """
    company_id = _insert_company(pg_db, "0000000077", "Dual Class Corp")
    _insert_security(pg_db, 1, "dcla", name="Dual Class Corp Class A",
                     company_id=company_id, cik="0000000077")
    _insert_security(pg_db, 2, "dclc", name="Dual Class Corp Class C",
                     company_id=company_id, cik="0000000077")
    _insert_security(pg_db, 3, "solo", name="Solo Inc.")
    _insert_security(pg_db, 4, "dcln", name="Dual Class Corp 5.00% Senior Notes due 2026",
                     company_id=company_id, cik="0000000077")
    for security_id in (1, 2, 3, 4):
        _insert_share(pg_db, security_id, 10)
    _insert_net_income(pg_db, 1, 100.0)  # 事实只挂锚证券（resolve_cik_map 入库语义）
    _insert_net_income(pg_db, 3, 40.0)
    for security_id, close in ((1, 5.0), (2, 3.0), (3, 2.0), (4, 100.0)):
        _insert_price(pg_db, security_id, "2026-01-02", close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([1, 2, 3], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = EarningsYieldFactor().compute(ctx)

    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel.dtypes.tolist() == [np.float64, np.float64, np.float64]
    # 公司分母 = 50 + 30 = 80：notes 行的 10*100=1000 被 common-equity 过滤
    # 挡在门外；锚与非锚同拿 公司 ni / 公司市值
    assert panel.loc["2026-01-02", 1] == 100.0 / 80.0
    assert panel.loc["2026-01-02", 2] == 100.0 / 80.0
    # 无 company_id：自身 ni / 自身市值（旧口径逐位不变）
    assert panel.loc["2026-01-02", 3] == 40.0 / 20.0


def test_earnings_yield_anchor_outside_universe_still_broadcasts(pg_db):
    """锚证券不在 universe（goog 入选、googl 未入选的窗口）时非锚仍可计算。"""
    company_id = _insert_company(pg_db, "0000000077", "Dual Class Corp")
    _insert_security(pg_db, 1, "dcla", name="Dual Class Corp Class A",
                     company_id=company_id, cik="0000000077")
    _insert_security(pg_db, 2, "dclc", name="Dual Class Corp Class C",
                     company_id=company_id, cik="0000000077")
    for security_id in (1, 2):
        _insert_share(pg_db, security_id, 10)
    _insert_net_income(pg_db, 1, 100.0)
    for security_id, close in ((1, 5.0), (2, 3.0)):
        _insert_price(pg_db, security_id, "2026-01-02", close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([2], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = EarningsYieldFactor().compute(ctx)

    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel.loc["2026-01-02", 2] == 100.0 / 80.0

def test_earnings_yield_dual_anchor_facts_take_first_not_sum(pg_db):
    """锚翻转过渡窗（gliba/gncma 型）：公司内两只成员同日各有新鲜 TTM 事实，
    分子须取 security_id 最小者（镜像 resolve_cik_map 决胜），绝不加总双计。"""
    company_id = _insert_company(pg_db, "0000000077", "Dual Anchor Corp")
    _insert_security(pg_db, 1, "olda", name="Dual Anchor Corp Class A",
                     company_id=company_id, cik="0000000077")
    _insert_security(pg_db, 2, "newa", name="Dual Anchor Corp Class B",
                     company_id=company_id, cik="0000000077")
    for security_id in (1, 2):
        _insert_share(pg_db, security_id, 10)
        _insert_price(pg_db, security_id, "2026-01-02", 5.0)
    _insert_net_income(pg_db, 1, 100.0)
    _insert_net_income(pg_db, 2, 90.0)  # 两代锚同时新鲜（存量数据实况）
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([1, 2], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = EarningsYieldFactor().compute(ctx)

    # 分母 = 50+50=100；分子取 sid=1 的 100.0（首个非 NaN），绝不是 190.0
    assert panel.loc["2026-01-02", 1] == 100.0 / 100.0
    assert panel.loc["2026-01-02", 2] == 100.0 / 100.0


def test_earnings_yield_company_without_common_members_falls_back(pg_db):
    """公司全员被判非 common（无 FIGI 的 'Units' 名称误伤）时，成员整体回退
    证券级旧口径，避免从可算值退化成 NaN。"""
    company_id = _insert_company(pg_db, "0000000088", "Pipeline Partners")
    _insert_security(pg_db, 1, "plp", name="Pipeline Partners Common Units",
                     company_id=company_id, cik="0000000088")
    _insert_share(pg_db, 1, 10)
    _insert_price(pg_db, 1, "2026-01-02", 5.0)
    _insert_net_income(pg_db, 1, 25.0)
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-02"]))
    universe = pd.Index([1], dtype="int64")
    ctx = FactorContext(pg_db.engine, dates=dates, security_universe=universe)

    panel = EarningsYieldFactor().compute(ctx)

    # 公司级分母恒 NaN（无 common 成员）——回退旧口径：自身 ni / 自身市值
    assert panel.loc["2026-01-02", 1] == 25.0 / 50.0
