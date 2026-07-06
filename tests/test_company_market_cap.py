"""research.company_market_cap 的公司级合并市值语义测试。

单元层：is_common_equity_name 名称分类器 + aggregate_company_market_cap 纯聚合；
集成层：load_security_company_map / load_company_market_cap_panel 走真实 schema。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.company_market_cap import (
    aggregate_company_market_cap,
    is_common_equity_name,
    load_company_market_cap_panel,
    load_security_company_map,
)


# ---------------------------------------------------------------------------
# is_common_equity_name（第一期名称启发式——归组与读取层共用的唯一判别）
# ---------------------------------------------------------------------------

class TestIsCommonEquityName:
    @pytest.mark.parametrize("name", [
        # rilyg/oxlcg/tmusi 型 baby bond：票息 + Notes + due 年份三特征齐中
        "5.00% Senior Notes due 2026",
        "B. Riley Financial, Inc. 5.00% Senior Notes due 2026",
        "6.75% Notes due 2031",
        # bhfan 型存托凭证
        "Depositary Shares, each representing a 1/1000th interest",
        "Depositary shares each representing a 1/1,000th interest in a share of preferred stock",
        # 优先股 / 权证 / 单位 / 债券
        "7.25% Series A Cumulative Redeemable Preferred Stock",
        "Warrants to purchase Common Stock",
        "Units, each consisting of one share of common stock and one warrant",
        "Bonds due 2028",
        "Subordinated Debentures due 2055",
        "Rights to purchase Series B shares",
    ])
    def test_instrument_names_excluded(self, name):
        assert is_common_equity_name(name) is False

    @pytest.mark.parametrize("name", [
        "Alphabet Inc. Class C",
        "Alphabet Inc. Class A Common Stock",
        "Berkshire Hathaway Inc.",
        "Apple Inc.",
        "Common Stock",  # 裸普通股描述不含工具行特征
    ])
    def test_common_equity_names_kept(self, name):
        assert is_common_equity_name(name) is True

    def test_word_boundary_does_not_match_inside_words(self):
        # "rights"/"units" 的词边界锚定：Bright/Wright/United 不误伤
        assert is_common_equity_name("Bright Horizons Family Solutions Inc.") is True
        assert is_common_equity_name("Wright Medical Group N.V.") is True
        assert is_common_equity_name("United Airlines Holdings, Inc.") is True

    def test_known_phase1_false_positives_are_locked(self):
        # 已知误伤（真名撞词表），第一期文档化接受：变更此行为须同步更新
        # research/company_market_cap.py 的 docstring 与归组报告口径。
        assert is_common_equity_name("Preferred Bank") is False
        assert is_common_equity_name("Unit Corporation") is False

    def test_null_and_blank_names_default_to_common(self):
        assert is_common_equity_name(None) is True
        assert is_common_equity_name("") is True
        assert is_common_equity_name("   ") is True


# ---------------------------------------------------------------------------
# aggregate_company_market_cap（纯函数聚合）
# ---------------------------------------------------------------------------

def _panel(dates, data) -> pd.DataFrame:
    df = pd.DataFrame(data, index=pd.DatetimeIndex(pd.to_datetime(dates)))
    df.columns = pd.Index(df.columns, dtype=np.int64)
    return df


def _membership(*rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["security_id", "company_id"])


class TestAggregateCompanyMarketCap:
    def test_two_class_company_sums(self):
        dates = ["2026-01-02", "2026-01-05"]
        panel = _panel(dates, {1: [10.0, 11.0], 2: [5.0, 6.0], 3: [100.0, 100.0]})
        membership = _membership((1, 77), (2, 77), (3, 88))

        out = aggregate_company_market_cap(panel, membership)

        assert out.columns.tolist() == [77, 88]
        assert out.loc["2026-01-02", 77] == 15.0
        assert out.loc["2026-01-05", 77] == 17.0
        assert out.loc["2026-01-02", 88] == 100.0

    def test_security_not_in_membership_is_ignored(self):
        # 调用方已过滤成员（如工具行误标）——不在 membership 的列绝不进合并市值
        dates = ["2026-01-02"]
        panel = _panel(dates, {1: [10.0], 2: [5.0], 3: [999.0]})
        membership = _membership((1, 77), (2, 77))

        out = aggregate_company_market_cap(panel, membership)

        assert out.columns.tolist() == [77]
        assert out.loc["2026-01-02", 77] == 15.0

    def test_nan_semantics_min_count(self):
        # 部分成员缺市值 -> 可得成员之和；全员 NaN -> NaN（不是 0）
        dates = ["2026-01-02", "2026-01-05"]
        panel = _panel(dates, {1: [10.0, np.nan], 2: [np.nan, np.nan]})
        membership = _membership((1, 77), (2, 77))

        out = aggregate_company_market_cap(panel, membership)

        assert out.loc["2026-01-02", 77] == 10.0
        assert np.isnan(out.loc["2026-01-05", 77])

    def test_empty_membership_returns_empty_columns(self):
        dates = ["2026-01-02"]
        panel = _panel(dates, {1: [10.0]})

        out = aggregate_company_market_cap(panel, _membership())

        assert out.columns.tolist() == []
        assert out.index.equals(panel.index)


# ---------------------------------------------------------------------------
# 集成：真实 schema 上的映射加载与一站式面板
# ---------------------------------------------------------------------------

def _insert_company(pg_db, cik, name) -> int:
    pg_db.upsert_companies([{"cik": cik, "name": name}])
    return pg_db.get_company_id_by_cik(cik)


def _insert_security(pg_db, security_id, symbol, *, name=None, sec_type="CS",
                     company_id=None, cik=None, is_active=True):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, name, market, type, cik, company_id,
                     is_active, full_refresh_interval)
                values
                    (:id, :symbol, :symbol, :name, 'US', :type, :cik, :company_id,
                     :is_active, 30)
                """
            ),
            {"id": security_id, "symbol": symbol, "name": name, "type": sec_type,
             "cik": cik, "company_id": company_id, "is_active": is_active},
        )
        conn.commit()


def _insert_share(pg_db, security_id, filing_date, period_end_date, total_shares):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into historical_shares
                    (security_id, filing_date, period_end_date, total_shares, source)
                values (:security_id, :filing_date, :period_end_date, :total_shares, 'MASSIVE')
                """
            ),
            {"security_id": security_id, "filing_date": filing_date,
             "period_end_date": period_end_date, "total_shares": total_shares},
        )
        conn.commit()


def _insert_price(pg_db, security_id, date, close):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into daily_prices
                    (security_id, date, open, high, low, close, volume)
                values (:security_id, :date, :close, :close, :close, :close, 100)
                """
            ),
            {"security_id": security_id, "date": date, "close": close},
        )
        conn.commit()


@pytest.mark.integration
class TestLoadSecurityCompanyMap:
    def test_map_scopes_to_cs_with_company_and_flags_instruments(self, pg_db):
        company_id = _insert_company(pg_db, "0000000077", "Dual Class Corp")
        _insert_security(pg_db, 1, "dca", name="Dual Class Corp Class A",
                         company_id=company_id, cik="0000000077")
        _insert_security(pg_db, 2, "dcag", name="Dual Class Corp 5.00% Senior Notes due 2026",
                         company_id=company_id, cik="0000000077")
        # company_id 为 NULL 的 CS 与 ETF 都不在映射里
        _insert_security(pg_db, 3, "solo", name="Solo Inc.", cik="0000000088")
        _insert_security(pg_db, 4, "etf1", name="Some ETF", sec_type="ETF",
                         company_id=company_id)

        members = load_security_company_map(pg_db.engine)

        assert list(members.columns) == ["security_id", "company_id", "security_name", "is_common_equity"]
        assert members["security_id"].tolist() == [1, 2]
        assert members["is_common_equity"].tolist() == [True, False]
        assert (members["company_id"] == company_id).all()

    def test_company_ids_filter_and_empty(self, pg_db):
        company_id = _insert_company(pg_db, "0000000077", "Dual Class Corp")
        _insert_security(pg_db, 1, "dca", name="Class A", company_id=company_id)

        assert load_security_company_map(pg_db.engine, company_ids=[company_id])["security_id"].tolist() == [1]
        assert load_security_company_map(pg_db.engine, company_ids=[company_id + 999]).empty
        assert load_security_company_map(pg_db.engine, company_ids=[]).empty


@pytest.mark.integration
class TestLoadCompanyMarketCapPanel:
    def test_two_class_sum_excludes_instrument_mislabel(self, pg_db):
        company_id = _insert_company(pg_db, "0001652044", "Alphabet Inc.")
        _insert_security(pg_db, 1, "googl", name="Alphabet Inc. Class A",
                         company_id=company_id, cik="0001652044")
        _insert_security(pg_db, 2, "goog", name="Alphabet Inc. Class C",
                         company_id=company_id, cik="0001652044")
        # 工具行误标：type='CS' 且已挂 company_id，但绝不进合并市值
        _insert_security(pg_db, 3, "googn", name="Alphabet 5.00% Senior Notes due 2026",
                         company_id=company_id, cik="0001652044")
        for sid, shares in ((1, 1_000_000), (2, 2_000_000), (3, 500_000)):
            _insert_share(pg_db, sid, "2025-01-02", "2024-12-31", shares)
        for sid, close in ((1, 10.0), (2, 5.0), (3, 100.0)):
            _insert_price(pg_db, sid, "2025-01-06", close)
        dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-06"]))

        panel = load_company_market_cap_panel(pg_db.engine, dates=dates)

        assert panel.columns.tolist() == [company_id]
        # 1M*10 + 2M*5 = 20M；notes 行的 0.5M*100 被 common-equity 过滤挡在门外
        assert panel.loc["2025-01-06", company_id] == 20_000_000.0

    def test_requested_company_without_members_returns_nan_column(self, pg_db):
        company_id = _insert_company(pg_db, "0000000099", "Empty Corp")
        dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-06"]))

        panel = load_company_market_cap_panel(pg_db.engine, dates=dates, company_ids=[company_id])

        assert panel.columns.tolist() == [company_id]
        assert panel[company_id].isna().all()

    def test_company_ids_filter_scopes_members(self, pg_db):
        cid_a = _insert_company(pg_db, "0000000011", "Corp A")
        cid_b = _insert_company(pg_db, "0000000022", "Corp B")
        _insert_security(pg_db, 1, "corpa", name="Corp A Common Stock", company_id=cid_a)
        _insert_security(pg_db, 2, "corpb", name="Corp B Common Stock", company_id=cid_b)
        for sid in (1, 2):
            _insert_share(pg_db, sid, "2025-01-02", "2024-12-31", 1_000_000)
            _insert_price(pg_db, sid, "2025-01-06", 3.0)
        dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-06"]))

        panel = load_company_market_cap_panel(pg_db.engine, dates=dates, company_ids=[cid_b])

        assert panel.columns.tolist() == [cid_b]
        assert panel.loc["2025-01-06", cid_b] == 3_000_000.0
