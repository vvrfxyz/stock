"""Tests for research.days_to_cover and the DaysToCoverFactor builtin.

All tests run without a database connection by monkeypatching
load_short_interest_events and _load_volume_wide.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.days_to_cover as dtc_mod
from research.days_to_cover import load_days_to_cover_panel
from research.factors import protocol as _proto
from research.factors.protocol import FactorContext

# Import builtin at module level so register() fires before any fixture snapshot.
import research.factors.builtins.days_to_cover as _dtc_builtin  # noqa: F401
from research.factors.builtins.days_to_cover import DaysToCoverFactor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_registry():
    saved = dict(_proto._REGISTRY)
    yield
    _proto._REGISTRY.clear()
    _proto._REGISTRY.update(saved)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _si_events(*rows) -> pd.DataFrame:
    """Build short_interest events: (security_id, visible_date, settlement_date, short_interest)."""
    if not rows:
        return pd.DataFrame(
            {
                "security_id": pd.Series(dtype=np.int64),
                "visible_date": pd.Series(dtype="datetime64[ns]"),
                "settlement_date": pd.Series(dtype="datetime64[ns]"),
                "short_interest": pd.Series(dtype=np.int64),
            }
        )
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "settlement_date", "short_interest"])
    for col in ("visible_date", "settlement_date"):
        df[col] = pd.to_datetime(df[col])
    return df


def _vol_wide(dates, data_dict) -> pd.DataFrame:
    """Build a volume wide panel: index=dates, columns=security_ids."""
    idx = pd.DatetimeIndex(pd.to_datetime(dates))
    df = pd.DataFrame(data_dict, index=idx, dtype=np.float64)
    df.columns = pd.Index(df.columns, dtype=np.int64)
    return df


def _patch(monkeypatch, si_rows, vol_dates, vol_data):
    """Monkeypatch both loaders on dtc_mod."""
    si = _si_events(*si_rows)
    vol = _vol_wide(vol_dates, vol_data)

    monkeypatch.setattr(dtc_mod, "load_short_interest_events", lambda engine, *, security_ids=None: si)
    monkeypatch.setattr(dtc_mod, "_load_volume_wide", lambda engine, *, dates, security_ids: vol)


_DUMMY = object()


# ---------------------------------------------------------------------------
# compute 基本功能
# ---------------------------------------------------------------------------

def test_compute_basic(monkeypatch):
    """SI=100, avg_vol=50 -> DTC=2.0. Two securities, forward-fill SI."""
    si_rows = [
        (1, "2026-01-15", "2026-01-15", 100),
        (2, "2026-01-15", "2026-01-15", 200),
    ]
    vol_dates = [
        "2025-12-20", "2025-12-21", "2025-12-22", "2025-12-23", "2025-12-24",
        "2025-12-25", "2025-12-26", "2025-12-27", "2025-12-28", "2025-12-29",
        "2025-12-30", "2025-12-31",
        "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05",
        "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09", "2026-01-10",
        "2026-01-11", "2026-01-12", "2026-01-13", "2026-01-14", "2026-01-15",
        "2026-01-16", "2026-01-17",
    ]
    vol_data = {
        1: [50.0] * len(vol_dates),
        2: [100.0] * len(vol_dates),
    }
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16", "2026-01-17"]))
    panel = load_days_to_cover_panel(
        _DUMMY, dates=dates, security_ids=[1, 2],
        visible_delay_days=0, si_max_staleness_days=400,
    )

    assert panel.shape == (3, 2)
    assert panel.dtypes.tolist() == [np.float64, np.float64]
    # SI=100 / avg_vol=50 = 2.0
    assert abs(panel.loc["2026-01-15", 1] - 2.0) < 1e-9
    # SI=200 / avg_vol=100 = 2.0
    assert abs(panel.loc["2026-01-15", 2] - 2.0) < 1e-9
    # Forward-fill: SI stays at 100, vol stays at 50
    assert abs(panel.loc["2026-01-17", 1] - 2.0) < 1e-9


def test_visible_delay_pushes_visibility(monkeypatch):
    """SI event on day T, with visible_delay_days=1, NaN on T and visible on T+1."""
    si_rows = [(1, "2026-01-15", "2026-01-15", 100)]
    vol_dates = pd.date_range("2025-12-15", "2026-01-17", freq="D").strftime("%Y-%m-%d").tolist()
    vol_data = {1: [50.0] * len(vol_dates)}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-15", "2026-01-16"]),
        security_ids=[1],
        visible_delay_days=1, si_max_staleness_days=400,
    )

    assert np.isnan(panel.loc["2026-01-15", 1])
    assert abs(panel.loc["2026-01-16", 1] - 2.0) < 1e-9


def test_staleness_truncates(monkeypatch):
    """SI event, si_max_staleness_days=10, should go NaN after 10 days from visible_date."""
    si_rows = [(1, "2026-01-15", "2026-01-15", 100)]
    vol_dates = pd.date_range("2025-12-15", "2026-01-30", freq="D").strftime("%Y-%m-%d").tolist()
    vol_data = {1: [50.0] * len(vol_dates)}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-15", "2026-01-25", "2026-01-26"]),
        security_ids=[1],
        visible_delay_days=0, si_max_staleness_days=10,
    )

    assert abs(panel.loc["2026-01-15", 1] - 2.0) < 1e-9
    assert abs(panel.loc["2026-01-25", 1] - 2.0) < 1e-9
    assert np.isnan(panel.loc["2026-01-26", 1])


def test_zero_volume_returns_nan(monkeypatch):
    """avg_vol=0 should produce NaN, not inf."""
    si_rows = [(1, "2026-01-15", "2026-01-15", 100)]
    vol_dates = pd.date_range("2025-12-15", "2026-01-17", freq="D").strftime("%Y-%m-%d").tolist()
    vol_data = {1: [0.0] * len(vol_dates)}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-15"]),
        security_ids=[1],
        visible_delay_days=0, si_max_staleness_days=400,
    )

    assert np.isnan(panel.loc["2026-01-15", 1])


def test_nan_short_interest_ignored(monkeypatch):
    """NaN SI event should not overwrite prior valid SI."""
    si_rows = [
        (1, "2026-01-01", "2026-01-01", 100),
        (1, "2026-01-15", "2026-01-15", np.nan),
    ]
    vol_dates = pd.date_range("2025-12-01", "2026-01-22", freq="D").strftime("%Y-%m-%d").tolist()
    vol_data = {1: [50.0] * len(vol_dates)}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-20"]),
        security_ids=[1],
        visible_delay_days=0, si_max_staleness_days=400,
    )

    assert abs(panel.loc["2026-01-20", 1] - 2.0) < 1e-9


def test_empty_events(monkeypatch):
    """No SI events -> empty columns panel."""
    si_rows = []
    vol_dates = ["2026-01-15"]
    vol_data = {1: [50.0]}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))
    panel = load_days_to_cover_panel(
        _DUMMY, dates=dates, security_ids=[1],
        visible_delay_days=0, si_max_staleness_days=400,
    )

    assert panel.columns.tolist() == [1]
    assert panel[1].isna().all()


def test_empty_dates(monkeypatch):
    """dates=[] -> empty index panel."""
    si_rows = [(1, "2026-01-15", "2026-01-15", 100)]
    vol_dates = ["2026-01-15"]
    vol_data = {1: [50.0]}
    _patch(monkeypatch, si_rows, vol_dates, vol_data)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime([]),
        security_ids=[1],
        visible_delay_days=0, si_max_staleness_days=400,
    )

    assert len(panel.index) == 0


def test_empty_security_ids_list(monkeypatch):
    """security_ids=[] -> empty columns panel."""
    monkeypatch.setattr(dtc_mod, "load_short_interest_events", lambda engine, *, security_ids=None: _si_events())
    monkeypatch.setattr(dtc_mod, "_load_volume_wide", lambda engine, *, dates, security_ids: _vol_wide([], {}))

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-15"]),
        security_ids=[],
    )

    assert panel.columns.tolist() == []
    assert panel.columns.dtype == np.dtype("int64")


# ---------------------------------------------------------------------------
# loader（monkeypatch）
# ---------------------------------------------------------------------------

def test_loader_security_ids_filter(monkeypatch):
    """Test None vs [2,999] vs []."""
    calls = []

    def fake_si(engine, *, security_ids=None):
        calls.append(("si", security_ids))
        return _si_events(
            (1, "2026-01-15", "2026-01-15", 100),
            (2, "2026-01-15", "2026-01-15", 200),
        )

    def fake_vol(engine, *, dates, security_ids):
        calls.append(("vol", security_ids))
        vol_dates = pd.date_range("2025-12-15", "2026-01-17", freq="D").strftime("%Y-%m-%d").tolist()
        return _vol_wide(vol_dates, {1: [50.0] * len(vol_dates), 2: [100.0] * len(vol_dates)})

    monkeypatch.setattr(dtc_mod, "load_short_interest_events", fake_si)
    monkeypatch.setattr(dtc_mod, "_load_volume_wide", fake_vol)

    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))

    # None -> all securities
    panel = load_days_to_cover_panel(_DUMMY, dates=dates, security_ids=None, visible_delay_days=0)
    assert 1 in panel.columns and 2 in panel.columns

    # [2, 999]
    calls.clear()
    filtered = load_days_to_cover_panel(_DUMMY, dates=dates, security_ids=[2, 999], visible_delay_days=0)
    assert filtered.columns.tolist() == [2, 999]
    assert filtered[999].isna().all()

    # []
    empty = load_days_to_cover_panel(_DUMMY, dates=dates, security_ids=[])
    assert empty.columns.tolist() == []


def test_loader_dedupes_security_ids(monkeypatch):
    """[2,2,999] -> [2,999]."""
    passed_ids = []

    def fake_si(engine, *, security_ids=None):
        passed_ids.append(security_ids)
        return _si_events((2, "2026-01-15", "2026-01-15", 200))

    def fake_vol(engine, *, dates, security_ids):
        vol_dates = pd.date_range("2025-12-15", "2026-01-17", freq="D").strftime("%Y-%m-%d").tolist()
        return _vol_wide(vol_dates, {2: [100.0] * len(vol_dates)})

    monkeypatch.setattr(dtc_mod, "load_short_interest_events", fake_si)
    monkeypatch.setattr(dtc_mod, "_load_volume_wide", fake_vol)

    panel = load_days_to_cover_panel(
        _DUMMY,
        dates=pd.to_datetime(["2026-01-15"]),
        security_ids=[2, 2, 999],
        visible_delay_days=0,
    )

    assert panel.columns.tolist() == [2, 999]
    assert panel.columns.dtype == np.dtype("int64")
    assert abs(panel.loc["2026-01-15", 2] - 2.0) < 1e-9
    assert panel[999].isna().all()
    # load_short_interest_events should receive deduplicated list
    assert sorted(passed_ids[-1]) == [2, 999]


# ---------------------------------------------------------------------------
# builtin 因子
# ---------------------------------------------------------------------------

def test_builtin_factor_shape(monkeypatch):
    """DaysToCoverFactor.compute returns correct shape via FactorContext."""
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
    universe = pd.Index([10, 20, 30], dtype="int64")
    loaded = pd.DataFrame({10: [2.0, 2.5], 20: [3.0, 3.5]}, index=dates, dtype="float64")

    def fake_loader(engine, *, dates, security_ids):
        return loaded

    monkeypatch.setattr(_dtc_builtin, "load_days_to_cover_panel", fake_loader)
    ctx = FactorContext(object(), dates=dates, security_universe=universe)

    panel = DaysToCoverFactor().compute(ctx)

    assert panel.shape == (2, 3)
    pd.testing.assert_index_equal(panel.index, dates)
    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel[30].isna().all()
    assert abs(panel.loc["2026-01-15", 10] - 2.0) < 1e-9


def test_builtin_factor_registered():
    """get('days_to_cover') returns DaysToCoverFactor."""
    from research.factors.protocol import get

    factor = get("days_to_cover")
    assert isinstance(factor, DaysToCoverFactor)
    assert factor.name == "days_to_cover"
    assert factor.lookback_days == 20
    assert factor.lag_days == 1
    assert factor.pit_guarantee is True
