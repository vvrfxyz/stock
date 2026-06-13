"""research.fundamentals 的 point-in-time TTM/时点指标构造语义测试（纯合成数据）。

v2 口径：重述感知——同一 period 的多次申报构成 vintage 序列，
as-of t 取 filed_date <= t 的最新 vintage。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from research.fundamentals import asof_panel, build_metric_events


def _f(sec, concept, ps, pe, filed, value):
    return {
        "security_id": sec,
        "concept": concept,
        "period_start": ps,
        "period_end": pe,
        "filed_date": filed,
        "value": value,
    }


def _facts(*rows) -> pd.DataFrame:
    df = pd.DataFrame(list(rows))
    for col in ("period_start", "period_end", "filed_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def test_annual_fact_is_direct_ttm():
    facts = _facts(_f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0))
    events = build_metric_events(facts)
    assert len(events) == 1
    row = events.iloc[0]
    assert row["metric"] == "net_income_ttm"
    assert row["value"] == 100.0
    assert row["period_end"] == pd.Timestamp("2023-12-31")
    assert row["visible_date"] == pd.Timestamp("2024-02-20")


def test_ttm_from_ytd_plus_prior_fy_minus_prior_ytd():
    facts = _facts(
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-09-30", "2023-11-05", 70.0),
    )
    events = build_metric_events(facts)
    ttm = events[events["period_end"] == pd.Timestamp("2024-09-30")]
    assert len(ttm) == 1
    assert ttm.iloc[0]["value"] == 80.0 + 100.0 - 70.0
    # 三分量齐备的时点 = 最后一个分量的 filed_date
    assert ttm.iloc[0]["visible_date"] == pd.Timestamp("2024-11-01")


def test_missing_prior_ytd_produces_no_derived_event():
    facts = _facts(
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
    )
    events = build_metric_events(facts)
    # 只剩年度直报事件，不外推 TTM
    assert events["period_end"].tolist() == [pd.Timestamp("2023-12-31")]


def test_revenue_coalesce_prefers_higher_priority_concept():
    facts = _facts(
        _f(1, "SalesRevenueNet", "2023-01-01", "2023-12-31", "2024-02-20", 480.0),
        _f(1, "Revenues", "2023-01-01", "2023-12-31", "2024-02-20", 500.0),
    )
    events = build_metric_events(facts)
    assert len(events) == 1
    assert events.iloc[0]["value"] == 500.0


def test_financial_revenue_concept_covers_banks():
    # 银行只报 RevenuesNetOfInterestExpense，也应产出 revenue_ttm
    facts = _facts(
        _f(2, "RevenuesNetOfInterestExpense", "2023-01-01", "2023-12-31", "2024-02-25", 320.0)
    )
    events = build_metric_events(facts)
    assert len(events) == 1
    assert events.iloc[0]["metric"] == "revenue_ttm"
    assert events.iloc[0]["value"] == 320.0


def test_ttm_components_must_share_concept():
    # YTD 与去年同期在新概念下、全年只有旧概念：不得跨概念拼 TTM
    facts = _facts(
        _f(
            1,
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "2024-01-01",
            "2024-03-31",
            "2024-05-01",
            30.0,
        ),
        _f(
            1,
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            "2023-01-01",
            "2023-03-31",
            "2023-05-01",
            25.0,
        ),
        _f(1, "SalesRevenueNet", "2023-01-01", "2023-12-31", "2024-02-20", 200.0),
    )
    events = build_metric_events(facts)
    assert events["period_end"].tolist() == [pd.Timestamp("2023-12-31")]
    assert events.iloc[0]["value"] == 200.0


def test_53_week_fiscal_year_prior_ytd_within_tolerance():
    # 52/53 周财年：去年同期 period_end 距今 371 天，仍应匹配
    facts = _facts(
        _f(1, "NetIncomeLoss", "2023-12-31", "2024-09-28", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-30", "2024-02-20", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-09-23", "2023-11-05", 70.0),
    )
    events = build_metric_events(facts)
    ttm = events[events["period_end"] == pd.Timestamp("2024-09-28")]
    assert len(ttm) == 1
    assert ttm.iloc[0]["value"] == 110.0


def test_asof_panel_visibility_delayed_one_day_and_staleness():
    facts = _facts(
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-09-30", "2023-11-05", 70.0),
    )
    events = build_metric_events(facts)
    dates = pd.to_datetime(["2024-02-20", "2024-03-01", "2024-11-01", "2024-11-02", "2025-11-05"])
    panel = asof_panel(events, dates=pd.DatetimeIndex(dates), max_staleness_days=400)
    col = panel["net_income_ttm"][1]
    assert np.isnan(col.loc["2024-02-20"])  # filed 当日默认不可见，避免盘后一日前视
    assert col.loc["2024-03-01"] == 100.0  # FY 直报次日后可见
    assert col.loc["2024-11-01"] == 100.0  # Q3 filed 当日仍只能看到旧 FY
    assert col.loc["2024-11-02"] == 110.0  # Q3 filed 次日可见
    assert np.isnan(col.loc["2025-11-05"])  # period_end 落后 400 天以上，置 NaN


def test_instant_metric_asof_latest_visible():
    facts = _facts(
        _f(1, "Assets", "2024-03-31", "2024-03-31", "2024-05-01", 10.0),
        _f(1, "Assets", "2024-06-30", "2024-06-30", "2024-08-01", 12.0),
    )
    events = build_metric_events(facts)
    dates = pd.DatetimeIndex(pd.to_datetime(["2024-06-01", "2024-08-01", "2024-08-02"]))
    panel = asof_panel(events, dates=dates)
    col = panel["assets"][1]
    assert col.loc["2024-06-01"] == 10.0
    assert col.loc["2024-08-01"] == 10.0  # filed 当日默认不可见
    assert col.loc["2024-08-02"] == 12.0


def test_late_filed_older_period_dropped_for_monotonic_asof():
    facts = _facts(
        _f(1, "Assets", "2024-06-30", "2024-06-30", "2024-08-01", 12.0),
        # 更老报告期反而更晚才首次可见：保留会让 as-of 序列倒退
        _f(1, "Assets", "2024-03-31", "2024-03-31", "2024-09-15", 10.0),
    )
    events = build_metric_events(facts)
    assert events["period_end"].tolist() == [pd.Timestamp("2024-06-30")]


# --- v2：重述感知 ---


def test_restated_annual_emits_second_vintage_event():
    facts = _facts(
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
        # 一年后重述（下一年 10-K 的比较期重列）
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2025-02-18", 92.0),
    )
    events = build_metric_events(facts)
    assert len(events) == 2
    dates = pd.DatetimeIndex(pd.to_datetime(["2024-06-30", "2025-06-30"]))
    panel = asof_panel(events, dates=dates, max_staleness_days=600)
    col = panel["net_income_ttm"][1]
    assert col.loc["2024-06-30"] == 100.0  # 重述前看到原始值
    assert col.loc["2025-06-30"] == 92.0  # 重述后看到新值


def test_restated_ytd_component_reemits_ttm():
    facts = _facts(
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-09-30", "2023-11-05", 70.0),
        # Q3 YTD 在 12 月被修正
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-12-10", 75.0),
    )
    events = build_metric_events(facts)
    ttm = events[
        (events["period_end"] == pd.Timestamp("2024-09-30"))
        & (events["metric"] == "net_income_ttm")
    ].sort_values("visible_date")
    assert len(ttm) == 2
    assert ttm["value"].tolist() == [110.0, 105.0]
    assert ttm["visible_date"].tolist() == [
        pd.Timestamp("2024-11-01"),
        pd.Timestamp("2024-12-10"),
    ]


def test_unchanged_revision_does_not_duplicate_event():
    # build 层兜底：同值的重复 vintage 不应产出第二条事件
    facts = _facts(
        _f(1, "NetIncomeLoss", "2024-01-01", "2024-09-30", "2024-11-01", 80.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2024-02-20", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-12-31", "2025-02-18", 100.0),
        _f(1, "NetIncomeLoss", "2023-01-01", "2023-09-30", "2023-11-05", 70.0),
    )
    events = build_metric_events(facts)
    ttm = events[events["period_end"] == pd.Timestamp("2024-09-30")]
    assert len(ttm) == 1  # FY 同值"重述"不触发新 TTM 事件


def test_same_day_multiple_filings_keep_last():
    facts = _facts(
        _f(1, "Assets", "2024-06-30", "2024-06-30", "2024-08-01", 11.0),
        _f(1, "Assets", "2024-06-30", "2024-06-30", "2024-08-01", 12.0),
    )
    events = build_metric_events(facts)
    assert len(events) == 1
    assert events.iloc[0]["value"] == 12.0
