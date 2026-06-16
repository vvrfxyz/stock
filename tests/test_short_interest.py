from __future__ import annotations

import numpy as np
import pandas as pd

import research.short_interest as short_interest
from research.short_interest import compute_short_interest_ratio_panel, load_short_interest_ratio_panel


def _si_events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "settlement_date", "short_interest"])
    for col in ("visible_date", "settlement_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _shares_events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "period_end_date", "total_shares"])
    for col in ("visible_date", "period_end_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _compute(events, shares_events, dates, **kwargs) -> pd.DataFrame:
    params = {
        "visible_delay_days": 0,
        "si_max_staleness_days": 400,
        "shares_max_staleness_days": 400,
    }
    params.update(kwargs)
    return compute_short_interest_ratio_panel(
        events,
        shares_events,
        pd.DatetimeIndex(pd.to_datetime(dates)),
        **params,
    )


def test_compute_basic():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-02-01", "2026-02-16"]))
    events = _si_events(
        (1, "2026-01-15", "2026-01-15", 100),
        (1, "2026-01-31", "2026-01-31", 150),
        (1, "2026-02-15", "2026-02-15", 120),
        (2, "2026-01-15", "2026-01-15", 200),
        (2, "2026-01-31", "2026-01-31", 180),
        (2, "2026-02-15", "2026-02-15", 220),
    )
    shares = _shares_events(
        (1, "2026-01-01", "2025-12-31", 1_000),
        (2, "2026-01-01", "2025-12-31", 2_000),
    )

    panel = _compute(events, shares, dates)

    assert panel.shape == (3, 2)
    assert panel.dtypes.tolist() == [np.float64, np.float64]
    assert panel.loc["2026-01-15", 1] == 0.10
    assert panel.loc["2026-02-01", 1] == 0.15
    assert panel.loc["2026-02-16", 1] == 0.12
    assert panel.loc["2026-02-16", 2] == 0.11


def test_visible_delay_pushes_visibility():
    events = _si_events((1, "2026-01-15", "2026-01-15", 100))
    shares = _shares_events((1, "2026-01-01", "2025-12-31", 1_000))

    panel = _compute(events, shares, ["2026-01-22", "2026-01-29"], visible_delay_days=14)

    assert np.isnan(panel.loc["2026-01-22", 1])
    assert panel.loc["2026-01-29", 1] == 0.10


def test_si_staleness_truncates():
    events = _si_events((1, "2026-01-15", "2026-01-15", 100))
    shares = _shares_events((1, "2026-01-01", "2025-12-31", 1_000))

    panel = _compute(
        events,
        shares,
        ["2026-01-29", "2026-02-14", "2026-02-15"],
        visible_delay_days=14,
        si_max_staleness_days=30,
    )

    assert panel.loc["2026-01-29", 1] == 0.10
    assert panel.loc["2026-02-14", 1] == 0.10
    assert np.isnan(panel.loc["2026-02-15", 1])


def test_shares_staleness_independent():
    events = _si_events((1, "2026-01-15", "2026-01-15", 100))
    shares = _shares_events((1, "2026-01-01", "2025-12-31", 1_000))

    panel = _compute(
        events,
        shares,
        ["2026-01-29"],
        visible_delay_days=14,
        si_max_staleness_days=30,
        shares_max_staleness_days=10,
    )

    assert np.isnan(panel.loc["2026-01-29", 1])


def test_zero_or_negative_shares_returns_nan():
    events = _si_events(
        (1, "2026-01-15", "2026-01-15", 100),
        (2, "2026-01-15", "2026-01-15", 100),
    )
    shares = _shares_events(
        (1, "2026-01-01", "2025-12-31", 0),
        (2, "2026-01-01", "2025-12-31", -10),
    )

    panel = _compute(events, shares, ["2026-01-15"])

    assert panel.dtypes.tolist() == [np.float64, np.float64]
    assert np.isnan(panel.loc["2026-01-15", 1])
    assert np.isnan(panel.loc["2026-01-15", 2])


def test_empty_events():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))
    shares = _shares_events((2, "2026-01-01", "2025-12-31", 2_000))

    panel = _compute(_si_events(), shares, dates)

    assert panel.columns.tolist() == [2]
    assert panel[2].isna().all()

    empty_panel = _compute(_si_events(), _shares_events(), dates)
    assert empty_panel.columns.tolist() == []
    assert empty_panel.columns.dtype == np.dtype("int64")


def test_universe_is_union():
    events = _si_events(
        (1, "2026-01-15", "2026-01-15", 100),
        (2, "2026-01-15", "2026-01-15", 200),
    )
    shares = _shares_events(
        (2, "2026-01-01", "2025-12-31", 2_000),
        (3, "2026-01-01", "2025-12-31", 3_000),
    )

    panel = _compute(events, shares, ["2026-01-15"])

    assert panel.columns.tolist() == [1, 2, 3]
    assert panel.columns.dtype == np.dtype("int64")
    assert np.isnan(panel.loc["2026-01-15", 1])
    assert panel.loc["2026-01-15", 2] == 0.10
    assert np.isnan(panel.loc["2026-01-15", 3])


def test_nan_short_interest_event_is_ignored():
    events = _si_events(
        (1, "2026-01-01", "2026-01-01", 100),
        (1, "2026-01-15", "2026-01-15", np.nan),
    )
    shares = _shares_events((1, "2026-01-01", "2025-12-31", 1_000))

    panel = _compute(events, shares, ["2026-01-20"])

    assert panel.loc["2026-01-20", 1] == 0.10


def test_loader_security_ids_filter(monkeypatch):
    calls = []
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))

    def fake_load_short_interest_events(engine, *, security_ids=None):
        calls.append(("si", security_ids))
        return _si_events(
            (1, "2026-01-15", "2026-01-15", 100),
            (2, "2026-01-15", "2026-01-15", 200),
        )

    def fake_load_shares_events(engine, *, security_ids=None):
        calls.append(("shares", security_ids))
        return _shares_events(
            (2, "2026-01-01", "2025-12-31", 2_000),
            (3, "2026-01-01", "2025-12-31", 3_000),
        )

    monkeypatch.setattr(short_interest, "load_short_interest_events", fake_load_short_interest_events)
    monkeypatch.setattr(short_interest, "load_shares_events", fake_load_shares_events)

    panel = load_short_interest_ratio_panel(object(), dates=dates, security_ids=None, visible_delay_days=0)
    assert panel.columns.tolist() == [1, 2, 3]

    filtered = load_short_interest_ratio_panel(object(), dates=dates, security_ids=[2, 999], visible_delay_days=0)
    assert filtered.columns.tolist() == [2, 999]
    assert filtered.loc["2026-01-15", 2] == 0.10
    assert filtered[999].isna().all()

    empty = load_short_interest_ratio_panel(object(), dates=dates, security_ids=[])
    assert empty.columns.tolist() == []
    assert calls == [("si", None), ("shares", None), ("si", [2, 999]), ("shares", [2, 999])]


def test_compute_empty_dates():
    events = _si_events((1, "2026-01-15", "2026-01-15", 100))
    shares = _shares_events((2, "2026-01-01", "2025-12-31", 2_000))

    panel = _compute(events, shares, [])

    assert len(panel.index) == 0
    assert str(panel.index.dtype) == "datetime64[ns]"
    assert panel.columns.tolist() == [1, 2]
    assert panel.columns.dtype == np.dtype("int64")


def test_compute_multiple_si_same_security_orders_by_visible_date():
    events = _si_events(
        (1, "2026-01-31", "2026-01-31", 200),
        (1, "2026-01-15", "2026-01-15", 100),
    )
    shares = _shares_events((1, "2026-01-01", "2025-12-31", 1_000))

    panel = _compute(events, shares, ["2026-01-30"])

    assert panel.loc["2026-01-30", 1] == 0.10
