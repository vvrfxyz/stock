"""research.shares 的 PIT 股本事件流拼接与 research.market_cap 段间 seam 测试。

金样本：AAPL 2020-08-31 拆股 4:1（任务文档钦定的坑）——XBRL 股本是申报时口径、
不随拆股回溯调整，拆股 ex 日到下一次申报之间必须由 _split_rollforward_shares
把 as-of 股本 ×4，市值才连续（raw close 已跳变）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from research.market_cap import compute_market_cap_panel, load_market_cap_panel
from research.shares import (
    STITCHED_COLUMNS,
    XBRL_MAX_STALENESS_DAYS,
    XBRL_VISIBLE_DELAY_DAYS,
    load_xbrl_shares_events,
    stitch_shares_events,
)

# ---------------------------------------------------------------------------
# fixtures（沿 tests/test_research_market_cap.py 的纯 pandas 风格）
# ---------------------------------------------------------------------------


def _vendor_events(*rows) -> pd.DataFrame:
    """historical_shares 段事件帧（load_shares_events(include_source=True) 形状）。"""
    df = pd.DataFrame(
        rows,
        columns=["security_id", "visible_date", "period_end_date", "total_shares", "source"],
    )
    for col in ("visible_date", "period_end_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _empty_vendor_events() -> pd.DataFrame:
    return _vendor_events()


def _xbrl_events(*rows) -> pd.DataFrame:
    """XBRL 段事件帧（load_xbrl_shares_events 形状）。

    行 = (security_id, filed_date, period_end, total_shares)；visible_date 按
    loader 口径 = filed_date + 1 天，stale_after = period_end + 270 天，
    split_anchor = period_end。
    """
    df = pd.DataFrame(
        rows, columns=["security_id", "filed_date", "period_end_date", "total_shares"]
    )
    for col in ("filed_date", "period_end_date"):
        df[col] = pd.to_datetime(df[col])
    df["visible_date"] = df["filed_date"] + pd.Timedelta(days=XBRL_VISIBLE_DELAY_DAYS)
    df["source"] = "XBRL"
    df["stale_after"] = df["period_end_date"] + pd.Timedelta(days=XBRL_MAX_STALENESS_DAYS)
    df["split_anchor"] = df["period_end_date"]
    return df[STITCHED_COLUMNS]


def _splits(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "ex_date", "split_from", "split_to"])
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    return df


def _prices(dates, data) -> pd.DataFrame:
    return pd.DataFrame(data, index=pd.DatetimeIndex(pd.to_datetime(dates)))


# ---------------------------------------------------------------------------
# AAPL 2020 拆股金样本（生产库核实的申报值）
# ---------------------------------------------------------------------------


def test_aapl_2020_split_golden_no_market_cap_cliff():
    # 申报事实：filed 2020-07-31 (pe 2020-07-17) 4,275,634,000 股；
    # SPLIT ex 2020-08-31 1→4；filed 2020-10-30 (pe 2020-10-16) 17,001,802,000；
    # filed 2021-01-28 → 16,788,096,000。
    events = stitch_shares_events(
        _empty_vendor_events(),
        _xbrl_events(
            (1, "2020-07-31", "2020-07-17", 4_275_634_000),
            (1, "2020-10-30", "2020-10-16", 17_001_802_000),
            (1, "2021-01-28", "2021-01-15", 16_788_096_000),
        ),
    )
    splits = _splits((1, "2020-08-31", 1.0, 4.0))
    dates = pd.DatetimeIndex(
        pd.to_datetime(
            ["2020-08-28", "2020-08-31", "2020-10-29", "2020-10-30", "2020-11-02", "2021-02-01"]
        )
    )
    # 构造"真市值不变"的 raw close：拆股日价格恰好 /4
    prices = _prices(dates, {1: [400.0, 100.0, 100.0, 100.0, 100.0, 100.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    # 拆股前：4,275,634,000 × 400
    assert panel.loc["2020-08-28", 1] == 4_275_634_000 * 400.0
    # ex 日到下一次申报可见前：as-of 股本必须被滚动 ×4（17,102,536,000）
    assert panel.loc["2020-08-31", 1] == pytest.approx(17_102_536_000 * 100.0)
    assert panel.loc["2020-10-29", 1] == pytest.approx(17_102_536_000 * 100.0)
    # filed 2020-10-30 的申报 +1 天延迟，10-30 当日仍用滚动值
    assert panel.loc["2020-10-30", 1] == pytest.approx(17_102_536_000 * 100.0)
    # 新申报可见后用申报值（乘数回到 1，不得重复放大）
    assert panel.loc["2020-11-02", 1] == pytest.approx(17_001_802_000 * 100.0)
    assert panel.loc["2021-02-01", 1] == pytest.approx(16_788_096_000 * 100.0)
    # 无 4x 断崖：ex 日相对跳变为 0；全序列最大跳变仅是申报间真实回购差异
    # （17.0B→16.8B 约 -1.3%），远小于口径错位会造成的 300% 断崖
    series = panel[1]
    assert abs(series.loc["2020-08-31"] / series.loc["2020-08-28"] - 1) < 1e-9
    assert series.pct_change().abs().max() < 0.02


def test_xbrl_split_between_period_end_and_filing_rolls_from_measurement_date():
    # 拆股 ex 落在 period_end（测量日）与 filed_date 之间：申报值是拆前口径，
    # 滚动锚必须取 period_end——锚取 visible_date 会漏乘拆股比（市值错 4 倍）。
    events = stitch_shares_events(
        _empty_vendor_events(),
        _xbrl_events((1, "2020-09-01", "2020-08-25", 4_000_000_000)),
    )
    splits = _splits((1, "2020-08-31", 1.0, 4.0))
    dates = pd.DatetimeIndex(pd.to_datetime(["2020-09-02", "2020-09-03"]))
    prices = _prices(dates, {1: [100.0, 100.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0, splits=splits)

    assert panel.loc["2020-09-02", 1] == pytest.approx(16_000_000_000 * 100.0)
    assert panel.loc["2020-09-03", 1] == pytest.approx(16_000_000_000 * 100.0)


# ---------------------------------------------------------------------------
# stitch_shares_events：vendor 段优先 + 双源去重
# ---------------------------------------------------------------------------


def test_stitch_vendor_supersedes_xbrl_from_first_vendor_visible():
    vendor = _vendor_events(
        (1, "2024-06-30", "2024-06-28", 1_000, "MASSIVE"),
        (1, "2024-09-30", "2024-09-27", 1_100, "MASSIVE"),
    )
    xbrl = _xbrl_events(
        (1, "2024-01-14", "2023-12-30", 900),   # vendor 段前：保留
        (1, "2024-06-29", "2024-06-15", 990),   # visible = 06-30 = vendor 起点：丢弃
        (1, "2024-07-15", "2024-06-30", 995),   # vendor 段内：丢弃
        (2, "2024-07-15", "2024-06-30", 500),   # 无 vendor 覆盖的证券：保留
    )

    stitched = stitch_shares_events(vendor, xbrl)

    sid1 = stitched[stitched["security_id"] == 1]
    assert sid1["source"].tolist() == ["XBRL", "MASSIVE", "MASSIVE"]
    assert sid1["total_shares"].tolist() == [900, 1_000, 1_100]
    sid2 = stitched[stitched["security_id"] == 2]
    assert sid2["source"].tolist() == ["XBRL"]
    assert sid2["total_shares"].tolist() == [500]


def test_stitch_massive_beats_polygon_on_same_visible_date():
    vendor = _vendor_events(
        (1, "2024-06-30", "2024-06-28", 200, "POLYGON"),
        (1, "2024-06-30", "2024-06-28", 100, "MASSIVE"),
        (1, "2024-07-31", "2024-07-30", 300, "POLYGON"),  # 无 MASSIVE 对应日：保留
    )

    stitched = stitch_shares_events(vendor, _xbrl_events())

    assert len(stitched) == 2
    first = stitched.iloc[0]
    assert first["visible_date"] == pd.Timestamp("2024-06-30")
    assert first["source"] == "MASSIVE"
    assert first["total_shares"] == 100
    assert stitched.iloc[1]["source"] == "POLYGON"
    assert stitched.iloc[1]["total_shares"] == 300


def test_stitch_vendor_frame_without_source_column_passes_through():
    vendor = _vendor_events(
        (1, "2024-06-30", "2024-06-28", 100, "MASSIVE"),
    ).drop(columns=["source"])

    stitched = stitch_shares_events(vendor, _xbrl_events())

    assert stitched["source"].tolist() == ["VENDOR"]
    assert stitched["total_shares"].tolist() == [100]
    assert stitched["stale_after"].tolist() == [
        pd.Timestamp("2024-06-30") + pd.Timedelta(days=400)
    ]


def test_stitch_bakes_per_segment_expiry_and_split_anchor():
    vendor = _vendor_events((1, "2024-06-30", "2024-06-28", 100, "MASSIVE"))
    xbrl = _xbrl_events((2, "2020-02-10", "2020-01-31", 900))

    stitched = stitch_shares_events(vendor, xbrl, vendor_max_staleness_days=400)

    v_row = stitched[stitched["security_id"] == 1].iloc[0]
    assert v_row["stale_after"] == pd.Timestamp("2024-06-30") + pd.Timedelta(days=400)
    assert v_row["split_anchor"] == pd.Timestamp("2024-06-30")  # vendor 锚 visible
    x_row = stitched[stitched["security_id"] == 2].iloc[0]
    assert x_row["stale_after"] == pd.Timestamp("2020-01-31") + pd.Timedelta(days=270)
    assert x_row["split_anchor"] == pd.Timestamp("2020-01-31")  # XBRL 锚 period_end


def test_stitch_both_empty_returns_typed_empty_frame():
    stitched = stitch_shares_events(_empty_vendor_events(), _xbrl_events())
    assert list(stitched.columns) == STITCHED_COLUMNS
    assert stitched.empty


# ---------------------------------------------------------------------------
# 过期 seam：XBRL 270 天锚 period_end，vendor 400 天锚 visible_date
# ---------------------------------------------------------------------------


def test_staleness_seam_per_segment():
    # sid 1 = XBRL：period_end 2020-01-31 → 最后有效日 2020-10-27（+270 天）
    # sid 2 = vendor：visible 2020-01-01 → 最后有效日 2021-02-04（+400 天）
    events = stitch_shares_events(
        _vendor_events((2, "2020-01-01", "2019-12-31", 2_000_000, "MASSIVE")),
        _xbrl_events((1, "2020-02-10", "2020-01-31", 1_000_000)),
        vendor_max_staleness_days=400,
    )
    dates = pd.DatetimeIndex(
        pd.to_datetime(["2020-10-27", "2020-10-28", "2021-02-04", "2021-02-05"])
    )
    prices = _prices(dates, {1: [10.0, 10.0, 10.0, 10.0], 2: [5.0, 5.0, 5.0, 5.0]})

    panel = compute_market_cap_panel(events, prices, dates, 400, 0)

    assert panel.loc["2020-10-27", 1] == 10_000_000.0
    assert np.isnan(panel.loc["2020-10-28", 1])  # period_end + 270 天后过期
    assert np.isnan(panel.loc["2021-02-04", 1])
    assert panel.loc["2021-02-04", 2] == 10_000_000.0
    assert np.isnan(panel.loc["2021-02-05", 2])  # visible + 400 天后过期


def test_stitched_vendor_only_matches_legacy_plain_frame():
    # 回归锁：纯 vendor 事件经 stitch（带 stale_after/split_anchor 列）后的面板
    # 与旧口径 plain 帧逐格一致（含拆股滚动与过期）。
    dates = pd.DatetimeIndex(
        pd.to_datetime(["2025-06-09", "2025-06-10", "2025-06-30", "2026-08-01"])
    )
    plain = pd.DataFrame(
        [
            (1, pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31"), 1_000_000),
            (2, pd.Timestamp("2025-03-31"), pd.Timestamp("2025-03-31"), 3_000_000),
        ],
        columns=["security_id", "visible_date", "period_end_date", "total_shares"],
    )
    vendor = _vendor_events(
        (1, "2025-03-31", "2025-03-31", 1_000_000, "MASSIVE"),
        (2, "2025-03-31", "2025-03-31", 3_000_000, "MASSIVE"),
    )
    prices = _prices(dates, {1: [100.0, 10.0, 10.0, 10.0], 2: [20.0, 21.0, 22.0, 23.0]})
    splits = _splits((1, "2025-06-10", 1.0, 10.0))

    legacy = compute_market_cap_panel(plain, prices, dates, 400, 0, splits=splits)
    stitched = compute_market_cap_panel(
        stitch_shares_events(vendor, _xbrl_events()), prices, dates, 400, 0, splits=splits
    )

    pd.testing.assert_frame_equal(stitched, legacy)


# ---------------------------------------------------------------------------
# 集成：load_xbrl_shares_events / load_market_cap_panel(include_xbrl=True)
# ---------------------------------------------------------------------------


def _insert_security(pg_db, security_id, symbol, cik=None):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into securities
                    (id, symbol, current_symbol, market, type, is_active, full_refresh_interval, cik)
                values
                    (:id, :symbol, :symbol, 'US', 'CS', true, 30, :cik)
                """
            ),
            {"id": security_id, "symbol": symbol, "cik": cik},
        )
        conn.commit()


def _insert_fact(
    pg_db,
    security_id,
    concept,
    period_end,
    filed_date,
    value,
    *,
    taxonomy="dei",
    unit="shares",
    accession="acc-default",
    cik="0000000001",
):
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into sec_fundamental_facts
                    (security_id, cik, taxonomy, concept, unit, period_start, period_end,
                     is_instant, value, accession_number, filed_date)
                values
                    (:security_id, :cik, :taxonomy, :concept, :unit, :period_end, :period_end,
                     true, :value, :accession, :filed_date)
                """
            ),
            {
                "security_id": security_id,
                "cik": cik,
                "taxonomy": taxonomy,
                "concept": concept,
                "unit": unit,
                "period_end": period_end,
                "filed_date": filed_date,
                "value": value,
                "accession": accession,
            },
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


@pytest.mark.integration
def test_load_xbrl_shares_events_against_real_schema(pg_db):
    _insert_security(pg_db, 1, "aapl", cik="320193")
    _insert_security(pg_db, 2, "googl", cik="1652044")
    # dei 与 us-gaap 同日申报：dei 优先，us-gaap 行被 coalesce 掉
    _insert_fact(pg_db, 1, "EntityCommonStockSharesOutstanding", "2020-07-17", "2020-07-31",
                 4_275_634_000, taxonomy="dei", accession="a1", cik="0000320193")
    _insert_fact(pg_db, 1, "CommonStockSharesOutstanding", "2020-06-27", "2020-07-31",
                 999, taxonomy="us-gaap", accession="a1", cik="0000320193")
    # value<=0 的脏 dei 行被过滤（brk.a 2010-05-07 场景）
    _insert_fact(pg_db, 1, "EntityCommonStockSharesOutstanding", "2020-10-16", "2020-10-30",
                 0, taxonomy="dei", accession="a2", cik="0000320193")
    # 只报 us-gaap 的公司（Alphabet 无 dei 行）：us-gaap 兜底
    _insert_fact(pg_db, 2, "CommonStockSharesOutstanding", "2020-06-30", "2020-08-05",
                 600_000_000, taxonomy="us-gaap", accession="a3", cik="0001652044")

    events = load_xbrl_shares_events(pg_db.engine)

    assert list(events.columns) == STITCHED_COLUMNS
    assert len(events) == 2
    aapl = events[events["security_id"] == 1].iloc[0]
    # 无 sec_filings.accepted_at 时 visible = filed_date + 1 天
    assert aapl["visible_date"] == pd.Timestamp("2020-08-01")
    assert aapl["total_shares"] == 4_275_634_000
    assert aapl["stale_after"] == pd.Timestamp("2020-07-17") + pd.Timedelta(days=270)
    assert aapl["split_anchor"] == pd.Timestamp("2020-07-17")
    googl = events[events["security_id"] == 2].iloc[0]
    assert googl["total_shares"] == 600_000_000


@pytest.mark.integration
def test_load_xbrl_visible_date_respects_accepted_at(pg_db):
    _insert_security(pg_db, 1, "aapl", cik="320193")
    _insert_fact(pg_db, 1, "EntityCommonStockSharesOutstanding", "2020-07-17", "2020-07-31",
                 4_275_634_000, taxonomy="dei", accession="late-acc", cik="0000320193")
    with pg_db.engine.connect() as conn:
        conn.execute(
            text(
                """
                insert into sec_filings (source, cik, form_type, accession_number, filing_date, accepted_at)
                values ('SEC', '0000320193', '10-Q', 'late-acc', '2020-07-31',
                        '2020-08-03 09:00:00-04')
                """
            )
        )
        conn.commit()

    events = load_xbrl_shares_events(pg_db.engine)

    # accepted_at 的美东自然日（08-03）晚于 filed_date（07-31）→ visible = 08-03 + 1
    assert events.iloc[0]["visible_date"] == pd.Timestamp("2020-08-04")


@pytest.mark.integration
def test_load_market_cap_panel_stitches_xbrl_before_vendor(pg_db):
    _insert_security(pg_db, 1, "aapl", cik="320193")
    # XBRL 段（vendor 段起点之前）
    _insert_fact(pg_db, 1, "EntityCommonStockSharesOutstanding", "2024-03-28", "2024-04-01",
                 1_000_000, taxonomy="dei", accession="b1", cik="0000320193")
    # vendor 段自 2024-06-30 起
    _insert_share(pg_db, 1, "2024-06-30", "2024-06-28", 2_000_000)
    for date, close in (
        ("2024-04-01", 10.0),
        ("2024-04-02", 10.0),
        ("2024-06-28", 11.0),
        ("2024-07-01", 12.0),
    ):
        _insert_price(pg_db, 1, date, close)
    dates = pd.DatetimeIndex(
        pd.to_datetime(["2024-04-01", "2024-04-02", "2024-06-28", "2024-07-01"])
    )

    panel = load_market_cap_panel(pg_db.engine, dates=dates)
    legacy = load_market_cap_panel(pg_db.engine, dates=dates, include_xbrl=False)

    # filed 04-01 → visible 04-02：当日不可见（+1 天烘焙）
    assert np.isnan(panel.loc["2024-04-01", 1])
    assert panel.loc["2024-04-02", 1] == 10_000_000.0
    assert panel.loc["2024-06-28", 1] == 11_000_000.0   # 仍是 XBRL 段
    assert panel.loc["2024-07-01", 1] == 24_000_000.0   # vendor 段接管
    # include_xbrl=False 退回旧行为：vendor 段之前无股本
    assert np.isnan(legacy.loc["2024-04-02", 1])
    assert legacy.loc["2024-07-01", 1] == 24_000_000.0
