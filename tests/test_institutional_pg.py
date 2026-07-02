"""research.institutional 聚合 SQL 的 PostgreSQL 集成测试。

test_institutional.py 的单元测试 mock 掉 SQL 层，锁不住聚合语义本身；
这里在真实 PG 上覆盖：
- accession 级去重：同 filer 同期同证券多行必须求和（H2）；
- 仅原件可见性：13F-HR/A 不参与聚合、不推迟 visible_date、不遮蔽新季度（H3）；
- 迟到旧季度事件被 period 单调守卫丢弃（M2）；
- delta 因子的上季基数取自守卫后的原件事件流。
"""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from data_models.models import InstitutionalHolding, Security
from research.institutional import (
    load_delta_institutional_ownership_panel,
    load_institutional_aggregates,
    load_institutional_holdings_panel,
)

pytestmark = pytest.mark.integration


def _insert_security(pg_db, security_id=1, symbol="aapl") -> int:
    with pg_db.get_session() as session:
        session.add(Security(
            id=security_id,
            symbol=symbol,
            current_symbol=symbol,
            market="US",
            type="CS",
            is_active=True,
            full_refresh_interval=30,
        ))
        session.commit()
    return security_id


def _add_holdings(pg_db, rows: list[dict]) -> None:
    """rows: dict(security_id, filer_cik, form_type, filing_date, period,
    accession, market_value, shares)。source_row_hash 按调用内序号生成。"""
    with pg_db.get_session() as session:
        for i, r in enumerate(rows):
            session.add(InstitutionalHolding(
                source="SEC_EDGAR",
                accession_number=r["accession"],
                source_row_hash=f"{r['accession']}:{i}",
                security_id=r["security_id"],
                filer_cik=r["filer_cik"],
                form_type=r["form_type"],
                filing_date=r["filing_date"],
                period=r["period"],
                market_value=r["market_value"],
                shares_or_principal_amount=r["shares"],
                shares_or_principal_type="SH",
                put_call=None,
            ))
        session.commit()


class TestAccessionLevelSum:
    """H2：distinct on 不得截断同 accession 内同证券的多行拆仓。"""

    def test_multi_row_same_security_summed(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            # filer 0001：同一 accession 内同证券按 discretion 拆成两行
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=200_000, shares=2_000),
            # filer 0002：单行
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2025, 11, 12), period=q3, accession="B1",
                 market_value=300_000, shares=3_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["n_holders"] == 2.0
        assert row["total_value"] == 600_000.0
        assert row["total_shares"] == 6_000.0
        assert row["visible_date"] == pd.Timestamp("2025-11-12")
        # filer 0001 合计 300k，filer 0002 300k：hhi = 2 * 300k^2 / 600k^2 = 0.5
        assert abs(row["hhi"] - 0.5) < 1e-12

    def test_same_filer_two_originals_latest_accession_wins(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 同 filer 同期第二份原件：整体取代第一份
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 20), period=q3, accession="A2",
                 market_value=120_000, shares=1_200),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["n_holders"] == 1.0
        assert row["total_value"] == 120_000.0
        assert row["total_shares"] == 1_200.0
        assert row["visible_date"] == pd.Timestamp("2025-11-20")


class TestAmendmentExcluded:
    """H3：修正件不参与聚合——不改值、不推迟 visible_date、不遮蔽新季度。"""

    def test_amendment_does_not_shift_visible_date_or_mask_new_quarter(self, pg_db):
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 迟到数月的 Q3 修正件：旧口径会把 Q3 的 visible_date 推到 2026-06-01，
            # 遮蔽已可见的 Q4
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR/A",
                 filing_date=date(2026, 6, 1), period=q3, accession="A2",
                 market_value=999_000, shares=9_990),
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="A3",
                 market_value=150_000, shares=1_500),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert agg["period"].tolist() == [pd.Timestamp(q3), pd.Timestamp(q4)]
        q3_row = agg.iloc[0]
        assert q3_row["visible_date"] == pd.Timestamp("2025-11-10")
        assert q3_row["total_value"] == 100_000.0  # 修正值未混入

        # 修正件到达后，面板仍显示 Q4，不回退到 Q3
        dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-15"]))
        panels = load_institutional_holdings_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )
        assert panels["total_value"].loc["2026-06-15", sid] == 150_000.0
        assert panels["total_shares"].loc["2026-06-15", sid] == 1_500.0


class TestPeriodMonotonicGuard:
    """M2：可见序内 period 单调不减，迟到的旧季度事件整体丢弃。"""

    def test_late_old_quarter_dropped(self, pg_db):
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            # Q4 按时申报
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="B1",
                 market_value=200_000, shares=2_000),
            # Q3 唯一原件在 Q4 可见之后才到：保留会让 as-of 面板回退
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 3, 1), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert agg["period"].tolist() == [pd.Timestamp(q4)]

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-05"]))
        panels = load_institutional_holdings_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )
        assert panels["total_value"].loc["2026-03-05", sid] == 200_000.0


class TestDeltaPriorBase:
    """delta 上季基数取自同一原件口径事件流，且基数可见日不晚于本季事件。"""

    def test_prior_quarter_base_correct(self, pg_db):
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # Q3 修正件（在 Q4 申报之前到达）：不得污染上季基数
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR/A",
                 filing_date=date(2026, 1, 15), period=q3, accession="A2",
                 market_value=500_000, shares=5_000),
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="A3",
                 market_value=120_000, shares=1_200),
        ])

        dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-01", "2026-02-20"]))
        panel = load_delta_institutional_ownership_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )

        # 首季无上季基数
        assert np.isnan(panel.loc["2025-12-01", sid])
        # 基数是 Q3 原件的 1000 股（可见 2025-11-10 <= Q4 可见 2026-02-10），
        # 不是修正件的 5000：(1200 - 1000) / 1000 = 0.2
        assert abs(panel.loc["2026-02-20", sid] - 0.2) < 1e-10
