from __future__ import annotations

import numpy as np
import pandas as pd

import research.short_volume as short_volume_mod
from research.short_volume import compute_short_volume_ratio_panel, load_short_volume_ratio_panel


def _sv_events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "trade_date", "short_volume_ratio"])
    for col in ("visible_date", "trade_date"):
        df[col] = pd.to_datetime(df[col])
    df["short_volume_ratio"] = df["short_volume_ratio"].astype(np.float64)
    return df


def _compute(events, dates, **kwargs) -> pd.DataFrame:
    params = {
        "visible_delay_days": 0,
        "max_staleness_days": 400,
    }
    params.update(kwargs)
    return compute_short_volume_ratio_panel(
        events,
        pd.DatetimeIndex(pd.to_datetime(dates)),
        **params,
    )


# ---------- compute 基本功能 ----------


def test_compute_basic():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16", "2026-01-17"]))
    events = _sv_events(
        (1, "2026-01-15", "2026-01-15", 0.30),
        (1, "2026-01-16", "2026-01-16", 0.25),
        (2, "2026-01-15", "2026-01-15", 0.50),
        (2, "2026-01-16", "2026-01-16", 0.45),
    )

    panel = _compute(events, dates)

    assert panel.shape == (3, 2)
    assert panel.dtypes.tolist() == [np.float64, np.float64]
    assert abs(panel.loc["2026-01-15", 1] - 0.30) < 1e-9
    assert abs(panel.loc["2026-01-16", 1] - 0.25) < 1e-9
    # 01-17 尚无新数据，forward-fill 最后一条
    assert abs(panel.loc["2026-01-17", 1] - 0.25) < 1e-9
    assert abs(panel.loc["2026-01-15", 2] - 0.50) < 1e-9


def test_visible_delay_pushes_visibility():
    events = _sv_events((1, "2026-01-15", "2026-01-15", 0.40))

    panel = _compute(events, ["2026-01-15", "2026-01-16"], visible_delay_days=1)

    assert np.isnan(panel.loc["2026-01-15", 1])
    assert abs(panel.loc["2026-01-16", 1] - 0.40) < 1e-9


def test_staleness_truncates():
    events = _sv_events((1, "2026-01-15", "2026-01-15", 0.30))

    panel = _compute(
        events,
        ["2026-01-15", "2026-01-25", "2026-01-26"],
        visible_delay_days=0,
        max_staleness_days=10,
    )

    assert abs(panel.loc["2026-01-15", 1] - 0.30) < 1e-9
    assert abs(panel.loc["2026-01-25", 1] - 0.30) < 1e-9
    assert np.isnan(panel.loc["2026-01-26", 1])


def test_nan_ratio_event_is_ignored():
    events = _sv_events(
        (1, "2026-01-01", "2026-01-01", 0.30),
        (1, "2026-01-15", "2026-01-15", np.nan),
    )

    panel = _compute(events, ["2026-01-20"])

    assert abs(panel.loc["2026-01-20", 1] - 0.30) < 1e-9


def test_empty_events():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))

    panel = _compute(_sv_events(), dates)

    assert panel.columns.tolist() == []
    assert panel.columns.dtype == np.dtype("int64")


def test_empty_dates():
    events = _sv_events((1, "2026-01-15", "2026-01-15", 0.30))

    panel = _compute(events, [])

    assert len(panel.index) == 0
    assert str(panel.index.dtype) == "datetime64[ns]"
    assert panel.columns.tolist() == [1]
    assert panel.columns.dtype == np.dtype("int64")


def test_multiple_events_same_security_orders_by_date():
    events = _sv_events(
        (1, "2026-01-16", "2026-01-16", 0.50),
        (1, "2026-01-15", "2026-01-15", 0.30),
    )

    panel = _compute(events, ["2026-01-15", "2026-01-16"])

    assert abs(panel.loc["2026-01-15", 1] - 0.30) < 1e-9
    assert abs(panel.loc["2026-01-16", 1] - 0.50) < 1e-9


# ---------- loader（monkeypatch） ----------


def test_loader_security_ids_filter(monkeypatch):
    calls = []
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))

    def fake_load(engine, *, security_ids=None):
        calls.append(security_ids)
        return _sv_events(
            (1, "2026-01-15", "2026-01-15", 0.30),
            (2, "2026-01-15", "2026-01-15", 0.50),
        )

    monkeypatch.setattr(short_volume_mod, "load_short_volume_events", fake_load)

    panel = load_short_volume_ratio_panel(object(), dates=dates, security_ids=None, visible_delay_days=0)
    assert panel.columns.tolist() == [1, 2]

    filtered = load_short_volume_ratio_panel(object(), dates=dates, security_ids=[2, 999], visible_delay_days=0)
    assert filtered.columns.tolist() == [2, 999]
    assert abs(filtered.loc["2026-01-15", 2] - 0.50) < 1e-9
    assert filtered[999].isna().all()

    empty = load_short_volume_ratio_panel(object(), dates=dates, security_ids=[])
    assert empty.columns.tolist() == []
    assert calls == [None, [2, 999]]


def test_loader_dedupes_duplicate_security_ids(monkeypatch):
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))

    def fake_load(engine, *, security_ids=None):
        return _sv_events((2, "2026-01-15", "2026-01-15", 0.50))

    monkeypatch.setattr(short_volume_mod, "load_short_volume_events", fake_load)

    panel = load_short_volume_ratio_panel(object(), dates=dates, security_ids=[2, 2, 999], visible_delay_days=0)

    assert panel.columns.tolist() == [2, 999]
    assert panel.columns.dtype == np.dtype("int64")
    assert abs(panel.loc["2026-01-15", 2] - 0.50) < 1e-9
    assert panel[999].isna().all()


# ---------- builtin 因子注册 ----------


def test_short_volume_factor_returns_panel_shape(monkeypatch):
    import research.factors.builtins.short_volume as sv_builtin
    from research.factors.builtins.short_volume import ShortVolumeFactor
    from research.factors.protocol import FactorContext

    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
    universe = pd.Index([10, 20, 30], dtype="int64")
    loaded = pd.DataFrame({10: [0.3, 0.4], 20: [0.5, 0.6]}, index=dates, dtype="float64")

    def fake_loader(engine, *, dates, security_ids):
        return loaded

    monkeypatch.setattr(sv_builtin, "load_short_volume_ratio_panel", fake_loader)
    ctx = FactorContext(object(), dates=dates, security_universe=universe)

    panel = ShortVolumeFactor().compute(ctx)

    assert panel.shape == (2, 3)
    pd.testing.assert_index_equal(panel.index, dates)
    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel[30].isna().all()


def test_short_volume_factor_registered():
    from research.factors.builtins import short_volume as _trigger  # noqa: F401
    from research.factors.protocol import get

    assert get("short_volume_ratio").name == "short_volume_ratio"
