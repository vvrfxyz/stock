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
    load_split_events,
)


def _events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "period_end_date", "total_shares"])
    for col in ("visible_date", "period_end_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _splits(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "ex_date", "split_from", "split_to"])
    df["ex_date"] = pd.to_datetime(df["ex_date"])
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


def _insert_split(pg_db, security_id, ex_date, split_from, split_to, source="MASSIVE", source_event_id=None):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into corporate_actions
                    (security_id, action_type, ex_date, split_from, split_to, source, source_event_id)
                values
                    (:security_id, 'SPLIT', :ex_date, :split_from, :split_to, :source, :source_event_id)
                """
            ),
            {
                "security_id": security_id,
                "ex_date": ex_date,
                "split_from": split_from,
                "split_to": split_to,
                "source": source,
                "source_event_id": source_event_id or f"split:{security_id}:{ex_date}:{source}",
            },
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


def test_forward_split_between_snapshots_keeps_market_cap_continuous():
    # 10:1 正拆股：ex 日 raw close /10、快照股本要等下一次 filing 才 ×10。
    # 无校正时 (ex, 下一快照) 窗口市值恰错 10 倍；校正后应连续。
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-09", "2025-06-10", "2025-06-30", "2025-07-01"]))
    events = _events(
        (1, "2025-03-31", "2025-03-31", 1_000_000),
        (1, "2025-07-01", "2025-06-30", 10_000_000),
    )
    prices = _prices(dates, {1: [100.0, 10.0, 10.0, 10.0]})
    splits = _splits((1, "2025-06-10", 1.0, 10.0))

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2025-06-09", 1] == 100_000_000.0
    assert panel.loc["2025-06-10", 1] == pytest.approx(100_000_000.0)
    assert panel.loc["2025-06-30", 1] == pytest.approx(100_000_000.0)
    # 新快照生效后乘数回到 1，不得重复放大
    assert panel.loc["2025-07-01", 1] == pytest.approx(100_000_000.0)
    # ex 日前后相对跳变在阈值内（原 bug 是 10x 跳变）
    jump = panel[1].pct_change().abs().max()
    assert jump < 0.01


def test_reverse_split_between_snapshots_keeps_market_cap_continuous():
    # 1:50 反拆股（DBGI 场景）：ex 日 raw close ×50，无校正时市值虚增 50 倍。
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-09", "2025-06-10", "2025-06-30"]))
    events = _events((1, "2025-03-31", "2025-03-31", 50_000_000))
    prices = _prices(dates, {1: [1.0, 50.0, 50.0]})
    splits = _splits((1, "2025-06-10", 50.0, 1.0))

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2025-06-09", 1] == 50_000_000.0
    assert panel.loc["2025-06-10", 1] == pytest.approx(50_000_000.0)
    assert panel.loc["2025-06-30", 1] == pytest.approx(50_000_000.0)


def test_split_on_or_before_anchor_date_is_not_double_counted():
    # ex_date <= 快照 visible 锚点日的拆股已含在快照股本里，乘数区间是半开 (anchor, t]。
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-11", "2025-06-30"]))
    events = _events((1, "2025-06-10", "2025-06-10", 10_000_000))
    prices = _prices(dates, {1: [10.0, 10.0]})
    splits = _splits((1, "2025-06-10", 1.0, 10.0))

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2025-06-11", 1] == 100_000_000.0
    assert panel.loc["2025-06-30", 1] == 100_000_000.0


def test_multiple_splits_in_window_compound():
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-09", "2025-06-10", "2025-06-20"]))
    events = _events((1, "2025-03-31", "2025-03-31", 1_000_000))
    prices = _prices(dates, {1: [100.0, 50.0, 25.0]})
    splits = _splits(
        (1, "2025-06-10", 1.0, 2.0),
        (1, "2025-06-20", 1.0, 2.0),
    )

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2025-06-09", 1] == 100_000_000.0
    assert panel.loc["2025-06-10", 1] == pytest.approx(100_000_000.0)
    assert panel.loc["2025-06-20", 1] == pytest.approx(100_000_000.0)


def test_split_correction_only_touches_affected_security():
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-09", "2025-06-10"]))
    events = _events(
        (1, "2025-03-31", "2025-03-31", 1_000_000),
        (2, "2025-03-31", "2025-03-31", 3_000_000),
    )
    prices = _prices(dates, {1: [100.0, 10.0], 2: [20.0, 21.0]})
    splits = _splits((1, "2025-06-10", 1.0, 10.0))

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2025-06-10", 1] == pytest.approx(100_000_000.0)
    assert panel.loc["2025-06-09", 2] == 60_000_000.0
    assert panel.loc["2025-06-10", 2] == 63_000_000.0


def test_no_splits_behaves_identically_to_legacy():
    # 回归锁：无 SPLIT / 不传 splits / 空 splits 三者结果逐格一致。
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-31", "2026-06-09", "2026-06-10", "2026-06-15"]))
    events = _events(
        (1, "2025-01-15", "2024-12-31", 1_000_000),
        (1, "2026-06-10", "2026-03-31", 2_000_000),
        (2, "2025-01-15", "2024-12-31", 5_000_000),
    )
    prices = _prices(dates, {1: [10.0, 11.0, 12.0, 13.0], 2: [1.0, np.nan, 2.0, 3.0]})

    legacy = compute_market_cap_panel(events, prices, dates, 600, 0)
    with_none = compute_market_cap_panel(events, prices, dates, 600, 0, splits=None)
    with_empty = compute_market_cap_panel(
        events, prices, dates, 600, 0,
        splits=pd.DataFrame(columns=["security_id", "ex_date", "split_from", "split_to"]),
    )
    unrelated = compute_market_cap_panel(
        events, prices, dates, 600, 0,
        splits=_splits((999, "2026-06-10", 1.0, 10.0)),
    )

    pd.testing.assert_frame_equal(with_none, legacy)
    pd.testing.assert_frame_equal(with_empty, legacy)
    pd.testing.assert_frame_equal(unrelated, legacy)


def test_split_correction_respects_staleness_nan():
    # 快照过期转 NaN 的格子，拆股校正不得凭空造出数值。
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-03-15"]))
    events = _events((1, "2024-01-01", "2023-12-31", 1_000_000))
    prices = _prices(dates, {1: [4.0]})
    splits = _splits((1, "2025-01-10", 1.0, 10.0))

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert np.isnan(panel.loc["2025-03-15", 1])


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


@pytest.mark.integration
def test_load_split_events_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl")
    _insert_split(pg_db, 1, "2025-06-10", 1, 10)
    # 同一拆股的合成 ID 替身：经济键相同，distinct 后只剩一行
    _insert_split(pg_db, 1, "2025-06-10", 1, 10, source_event_id="massive-split:dupe")
    _insert_split(pg_db, 1, "2025-08-01", 50, 1)

    splits = load_split_events(pg_db.engine)

    assert list(splits.columns) == ["security_id", "ex_date", "split_from", "split_to"]
    assert len(splits) == 2
    assert splits["security_id"].dtype == np.int64
    assert str(splits["ex_date"].dtype) == "datetime64[ns]"
    assert splits["ex_date"].tolist() == [pd.Timestamp("2025-06-10"), pd.Timestamp("2025-08-01")]
    assert splits["split_from"].tolist() == [1.0, 50.0]
    assert splits["split_to"].tolist() == [10.0, 1.0]


@pytest.mark.integration
def test_load_market_cap_panel_applies_split_correction(pg_db):
    _insert_security(pg_db, 1, "dbgi")
    _insert_share(pg_db, 1, "2025-03-31", "2025-03-31", 50_000_000)
    _insert_split(pg_db, 1, "2025-06-10", 50, 1)
    for date, close in (
        ("2025-06-09", 1.0),
        ("2025-06-10", 50.0),
        ("2025-06-30", 50.0),
    ):
        _insert_price(pg_db, 1, date, close)
    dates = pd.DatetimeIndex(pd.to_datetime(["2025-06-09", "2025-06-10", "2025-06-30"]))

    panel = load_market_cap_panel(pg_db.engine, dates=dates)

    assert panel.loc["2025-06-09", 1] == 50_000_000.0
    assert panel.loc["2025-06-10", 1] == pytest.approx(50_000_000.0)
    assert panel.loc["2025-06-30", 1] == pytest.approx(50_000_000.0)
