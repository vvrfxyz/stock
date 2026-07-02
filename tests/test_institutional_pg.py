"""research.institutional 聚合 SQL 的 PostgreSQL 集成测试。

test_institutional.py 的单元测试 mock 掉 SQL 层，锁不住聚合语义本身；
这里在真实 PG 上覆盖：
- accession 级去重：同 filer 同期同证券多行必须求和（H2）；
- 仅原件可见性：13F-HR/A 不参与聚合、不推迟 visible_date、不遮蔽新季度（H3）；
- 迟到旧季度事件被 period 单调守卫丢弃（M2）；
- 两段式可见性：迟交 filer 触发准时批+终版两条事件、无迟交只发一条、
  全迟交只发终版；截止日边界 = period + 46 天（含当日）；
- 核心修复：迟交 ~250 天的 straggler 不再让该券该季在面板上全程 NaN；
- delta 因子的上季基数取"本事件可见时点上、上季已公开的最新事件值"，
  上季终版可见晚于本季准时批时不得被用作基数（前视锁定）。
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
        # 两份原件都在截止日（2025-11-15 = period + 46 天）之前：不触发两段式
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 同 filer 同期第二份原件：整体取代第一份
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 14), period=q3, accession="A2",
                 market_value=120_000, shares=1_200),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["n_holders"] == 1.0
        assert row["total_value"] == 120_000.0
        assert row["total_shares"] == 1_200.0
        assert row["visible_date"] == pd.Timestamp("2025-11-14")

    def test_second_original_after_deadline_emits_two_stage(self, pg_db):
        """同 filer 第二份原件迟于截止日：准时批用截止日前最新的 A1 快照
        （当时公开的就是它），终版切到 A2。"""
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 截止日（2025-11-15）之后的第二份原件
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 20), period=q3, accession="A2",
                 market_value=120_000, shares=1_200),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 2
        ontime, final = agg.iloc[0], agg.iloc[1]
        assert ontime["visible_date"] == pd.Timestamp("2025-11-10")
        assert ontime["total_value"] == 100_000.0
        assert ontime["total_shares"] == 1_000.0
        assert final["visible_date"] == pd.Timestamp("2025-11-20")
        assert final["total_value"] == 120_000.0
        assert final["total_shares"] == 1_200.0
        assert (agg["n_holders"] == 1.0).all()


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


class TestTwoStageVisibility:
    """两段式事件发射：迟交触发准时批+终版，无迟交/全迟交各只发一条。"""

    def test_late_filer_emits_ontime_and_final(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)  # 截止日 = 2025-11-15（period + 46 天）
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 迟交机构：截止日后 2 个多月才申报
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 1, 20), period=q3, accession="B1",
                 market_value=200_000, shares=2_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 2
        ontime, final = agg.iloc[0], agg.iloc[1]
        # 准时批：只含截止日前的申报，计数 = 准时机构数
        assert ontime["visible_date"] == pd.Timestamp("2025-11-10")
        assert ontime["n_holders"] == 1.0
        assert ontime["total_value"] == 100_000.0
        assert ontime["total_shares"] == 1_000.0
        assert abs(ontime["hhi"] - 1.0) < 1e-12
        # 终版：全量计数
        assert final["visible_date"] == pd.Timestamp("2026-01-20")
        assert final["n_holders"] == 2.0
        assert final["total_value"] == 300_000.0
        assert final["total_shares"] == 3_000.0
        # (100k^2 + 200k^2) / 300k^2 = 5/9
        assert abs(final["hhi"] - 5.0 / 9.0) < 1e-12

        # 面板：截止日起先见准时批，终版到达后切全量
        dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-01", "2026-02-01"]))
        panels = load_institutional_holdings_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )
        assert panels["n_holders"].loc["2025-12-01", sid] == 1.0
        assert panels["n_holders"].loc["2026-02-01", sid] == 2.0

    def test_no_late_filer_single_event(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2025, 11, 12), period=q3, accession="B1",
                 market_value=200_000, shares=2_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        # 无迟交者：准时批与终版 visible_date 相同，不重复发事件
        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["visible_date"] == pd.Timestamp("2025-11-12")
        assert row["n_holders"] == 2.0
        assert row["total_value"] == 300_000.0

    def test_all_late_final_only(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            # 唯一持有人在截止日后才申报：准时批为空，只发终版
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 1, 20), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 1
        row = agg.iloc[0]
        assert row["visible_date"] == pd.Timestamp("2026-01-20")
        assert row["n_holders"] == 1.0

    def test_deadline_boundary_inclusive(self, pg_db):
        sid = _insert_security(pg_db)
        q3 = date(2025, 9, 30)
        _add_holdings(pg_db, [
            # 恰在截止日（period + 46 = 2025-11-15）当天：算准时
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 15), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # 截止日次日：算迟交
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2025, 11, 16), period=q3, accession="B1",
                 market_value=200_000, shares=2_000),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        assert len(agg) == 2
        assert agg.iloc[0]["visible_date"] == pd.Timestamp("2025-11-15")
        assert agg.iloc[0]["n_holders"] == 1.0
        assert agg.iloc[1]["visible_date"] == pd.Timestamp("2025-11-16")
        assert agg.iloc[1]["n_holders"] == 2.0


class TestTwoStageStragglerNoNanGap:
    """核心修复：一家迟交 ~250 天的机构不再让该券该季在面板上全程 NaN。"""

    def test_ontime_batch_covers_quarter_until_next(self, pg_db):
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            # Q3 准时申报
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # Q3 straggler：迟交 250 天。旧语义下整季 visible_date 被拖到
            # 2026-06-07，早已超过 period + 200 天 staleness -> 该季"生而过期"
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 6, 7), period=q3, accession="B1",
                 market_value=50_000, shares=500),
            # Q4 准时申报
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="A2",
                 market_value=120_000, shares=1_200),
        ])

        agg = load_institutional_aggregates(pg_db.engine, security_ids=[sid])

        # Q3 迟到终版被单调守卫丢弃（可见于 Q4 事件之后），留下 Q3 准时批 + Q4
        assert list(zip(agg["period"], agg["visible_date"])) == [
            (pd.Timestamp(q3), pd.Timestamp("2025-11-10")),
            (pd.Timestamp(q4), pd.Timestamp("2026-02-10")),
        ]

        dates = pd.DatetimeIndex(pd.to_datetime([
            "2025-11-10", "2025-12-15", "2026-01-15", "2026-02-09",
            "2026-02-10", "2026-06-15",
        ]))
        panels = load_institutional_holdings_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )

        n = panels["n_holders"][sid]
        # 准时批从截止日前的申报日起可见，直至下季接棒——无 NaN 长隙
        assert n.loc["2025-11-10"] == 1.0
        assert n.loc["2025-12-15"] == 1.0
        assert n.loc["2026-01-15"] == 1.0
        assert n.loc["2026-02-09"] == 1.0
        assert n.loc["2026-02-10"] == 1.0   # Q4 接棒
        assert n.loc["2026-06-15"] == 1.0   # straggler 到达也不回退/不置 NaN
        assert n.notna().all()


class TestDeltaTwoStagePairing:
    """delta 两段式配对：基数 = 本事件可见时点上、上季已公开的最新事件值。"""

    def test_ontime_and_final_pair_with_then_visible_prior(self, pg_db):
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            # Q3：A 准时（1000 股），B 迟交但在 Q4 事件前到达（Q3 终版存活）
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2025, 12, 20), period=q3, accession="B1",
                 market_value=50_000, shares=500),
            # Q4（截止 2026-02-15）：A 准时，B 迟交
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="A2",
                 market_value=120_000, shares=1_200),
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 3, 5), period=q4, accession="B2",
                 market_value=60_000, shares=600),
        ])

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-20", "2026-03-10"]))
        panel = load_delta_institutional_ownership_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )

        # Q4 准时批（1200 股）时点上，Q3 终版（1500 股，可见 2025-12-20）已公开
        # -> 基数 1500：(1200 - 1500) / 1500 = -0.2
        assert abs(panel.loc["2026-02-20", sid] - (-0.2)) < 1e-10
        # Q4 终版（1800 股）：基数仍是当时可见的 Q3 最新值 1500 -> 0.2
        assert abs(panel.loc["2026-03-10", sid] - 0.2) < 1e-10

    def test_prior_final_later_than_current_ontime_locked_out(self, pg_db):
        """前视锁定：上季终版可见晚于本季准时批时，不得被用作基数。"""
        sid = _insert_security(pg_db)
        q3, q4 = date(2025, 9, 30), date(2025, 12, 31)
        _add_holdings(pg_db, [
            # Q3：A 准时（1000 股）
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2025, 11, 10), period=q3, accession="A1",
                 market_value=100_000, shares=1_000),
            # Q3：B 拖到 Q4 准时批（2026-02-10）之后才交 -> Q3 终版被守卫丢弃
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 2, 20), period=q3, accession="B1",
                 market_value=50_000, shares=500),
            # Q4：A 准时（1200 股），B 迟交（合计 1800 股）
            dict(security_id=sid, filer_cik="0001", form_type="13F-HR",
                 filing_date=date(2026, 2, 10), period=q4, accession="A2",
                 market_value=120_000, shares=1_200),
            dict(security_id=sid, filer_cik="0002", form_type="13F-HR",
                 filing_date=date(2026, 3, 5), period=q4, accession="B2",
                 market_value=60_000, shares=600),
        ])

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-12", "2026-03-10"]))
        panel = load_delta_institutional_ownership_panel(
            pg_db.engine, dates=dates, security_ids=[sid],
        )

        # Q4 准时批基数只能是 Q3 准时批的 1000（Q3 终版 1500 当时不可见）：
        # (1200 - 1000) / 1000 = 0.2，绝不是前视的 (1200-1500)/1500 = -0.2
        assert abs(panel.loc["2026-02-12", sid] - 0.2) < 1e-10
        # Q4 终版：Q3 终版已被守卫丢弃，基数仍是 1000 -> (1800-1000)/1000 = 0.8
        assert abs(panel.loc["2026-03-10", sid] - 0.8) < 1e-10
