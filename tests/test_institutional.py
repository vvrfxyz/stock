"""Tests for research.institutional (13F holdings panel loaders) and the three
institutional builtin factors: institutional_breadth, ownership_concentration,
delta_institutional_ownership.

All tests run without a database connection by monkeypatching
``load_institutional_aggregates``.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.institutional as institutional
from research.institutional import (
    load_delta_institutional_ownership_panel,
    load_institutional_holdings_panel,
)
from research.factors import protocol as _proto
from research.factors.protocol import FactorContext

# Import builtins at module level so register() fires before any fixture snapshot.
import research.factors.builtins.institutional_breadth as _ib_mod  # noqa: F401
import research.factors.builtins.ownership_concentration as _oc_mod  # noqa: F401
import research.factors.builtins.delta_institutional_ownership as _dio_mod  # noqa: F401
from research.factors.builtins.institutional_breadth import InstitutionalBreadthFactor
from research.factors.builtins.ownership_concentration import OwnershipConcentrationFactor
from research.factors.builtins.delta_institutional_ownership import (
    DeltaInstitutionalOwnershipFactor,
)


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

def _agg_events(*rows) -> pd.DataFrame:
    """Build an aggregates DataFrame identical to what load_institutional_aggregates returns.

    Each row: (security_id, visible_date, period, n_holders, total_value, total_shares, hhi)
    """
    df = pd.DataFrame(
        rows,
        columns=["security_id", "visible_date", "period",
                 "n_holders", "total_value", "total_shares", "hhi"],
    )
    for col in ("visible_date", "period"):
        df[col] = pd.to_datetime(df[col])
    df["security_id"] = df["security_id"].astype(np.int64)
    for col in ("n_holders", "total_value", "total_shares", "hhi"):
        df[col] = df[col].astype(np.float64)
    return df


def _patch_agg(monkeypatch, *rows):
    """Monkeypatch load_institutional_aggregates to return a fixed DataFrame."""
    agg = _agg_events(*rows)

    def fake_load(engine, *, security_ids=None):
        if security_ids is not None and len(security_ids) == 0:
            # Replicate the real early-return for empty list
            from research.institutional import _empty_agg
            empty = _empty_agg()
            empty["hhi"] = pd.Series(dtype=np.float64)
            return empty
        return agg.copy()

    monkeypatch.setattr(institutional, "load_institutional_aggregates", fake_load)


_DUMMY_ENGINE = object()


# ---------------------------------------------------------------------------
# Panel loader tests
# ---------------------------------------------------------------------------

class TestHoldingsPanelBasic:
    """test_holdings_panel_basic: 2 securities, 2 quarters."""

    def test_shape_and_values(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            # sec 1: Q3-2025
            (1, "2025-11-15", "2025-09-30", 50, 1_000_000, 5_000, 0.04),
            # sec 1: Q4-2025
            (1, "2026-02-15", "2025-12-31", 60, 1_200_000, 6_000, 0.05),
            # sec 2: Q3-2025
            (2, "2025-11-15", "2025-09-30", 30, 500_000, 3_000, 0.10),
            # sec 2: Q4-2025
            (2, "2026-02-15", "2025-12-31", 35, 600_000, 3_500, 0.08),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-01", "2026-03-01"]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1, 2],
        )

        assert set(panels.keys()) == {"n_holders", "total_value", "total_shares", "hhi"}
        for key in panels:
            assert panels[key].shape == (2, 2), f"panel {key} shape mismatch"
            assert panels[key].columns.tolist() == [1, 2]
            pd.testing.assert_index_equal(panels[key].index, dates)

        # At 2025-12-01: Q3 visible (visible_date 2025-11-15 <= 2025-12-01)
        assert panels["n_holders"].loc["2025-12-01", 1] == 50.0
        assert panels["n_holders"].loc["2025-12-01", 2] == 30.0
        assert panels["hhi"].loc["2025-12-01", 1] == 0.04
        assert panels["total_value"].loc["2025-12-01", 2] == 500_000.0
        assert panels["total_shares"].loc["2025-12-01", 2] == 3_000.0

        # At 2026-03-01: Q4 visible
        assert panels["n_holders"].loc["2026-03-01", 1] == 60.0
        assert panels["hhi"].loc["2026-03-01", 2] == 0.08


class TestHoldingsPanelStaleness:
    """test_holdings_panel_staleness: max_staleness_days anchored on period column."""

    def test_stale_value_becomes_nan(self, monkeypatch):
        # Period 2025-12-31, visible 2026-02-15
        # With max_staleness_days=100, stale after period + 100 = 2026-04-10
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime([
            "2026-03-01",   # within staleness
            "2026-04-10",   # exactly at boundary (period + 100 days)
            "2026-04-11",   # past staleness
        ]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
            max_staleness_days=100,
        )

        assert panels["n_holders"].loc["2026-03-01", 1] == 50.0
        assert panels["n_holders"].loc["2026-04-10", 1] == 50.0
        assert np.isnan(panels["n_holders"].loc["2026-04-11", 1])


class TestHoldingsPanelForwardFills:
    """test_holdings_panel_forward_fills: Q4 value carries forward before Q1 filing."""

    def test_forward_fill_before_next_filing(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
            (1, "2026-05-15", "2026-03-31", 60, 1_200_000, 6_000, 0.05),
        )

        dates = pd.DatetimeIndex(pd.to_datetime([
            "2026-03-01",   # Q4 visible, before Q1 filing
            "2026-04-01",   # Q4 still carried forward (Q1 not yet visible)
            "2026-05-20",   # Q1 now visible
        ]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # Q4 value forward-fills across March and April
        assert panels["n_holders"].loc["2026-03-01", 1] == 50.0
        assert panels["n_holders"].loc["2026-04-01", 1] == 50.0
        # After Q1 filing becomes visible
        assert panels["n_holders"].loc["2026-05-20", 1] == 60.0


class TestHoldingsPanelEmptyAgg:
    """test_holdings_panel_empty_agg: no data returns empty panels with correct dtypes."""

    def test_empty_returns_correct_structure(self, monkeypatch):
        _patch_agg(monkeypatch)  # no rows

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1, 2],
        )

        for key in ("n_holders", "total_value", "total_shares", "hhi"):
            panel = panels[key]
            pd.testing.assert_index_equal(panel.index, dates)
            assert panel.columns.tolist() == [1, 2]
            assert panel.dtypes.tolist() == [np.float64, np.float64]
            assert panel.isna().all().all()


class TestHoldingsPanelEmptyDates:
    """test_holdings_panel_empty_dates: dates=[] returns empty panels."""

    def test_empty_dates(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
        )

        dates = pd.DatetimeIndex([], dtype="datetime64[ns]")
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        for key in ("n_holders", "total_value", "total_shares", "hhi"):
            assert len(panels[key].index) == 0
            assert str(panels[key].index.dtype) == "datetime64[ns]"


class TestHoldingsPanelEmptySecurityIds:
    """test_holdings_panel_empty_security_ids: security_ids=[] returns empty panels."""

    def test_empty_security_ids(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-01"]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[],
        )

        for key in ("n_holders", "total_value", "total_shares", "hhi"):
            assert panels[key].columns.tolist() == []
            assert panels[key].columns.dtype == np.dtype("int64")


class TestHoldingsPanelSecurityIdsFilter:
    """test_holdings_panel_security_ids_filter: requesting [1, 999] includes ghost 999."""

    def test_ghost_security_is_all_nan(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-01"]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1, 999],
        )

        for key in ("n_holders", "total_value", "total_shares", "hhi"):
            assert panels[key].columns.tolist() == [1, 999]
            assert panels[key][999].isna().all()

        assert panels["n_holders"].loc["2026-03-01", 1] == 50.0


# ---------------------------------------------------------------------------
# Delta ownership tests
# ---------------------------------------------------------------------------

class TestDeltaBasic:
    """test_delta_basic: Q4 shares=1000, Q1 shares=1200 -> delta=0.2 after Q1 visible."""

    def test_delta_value(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 1_000, 0.04),
            (1, "2026-05-15", "2026-03-31", 55, 1_200_000, 1_200, 0.05),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-01"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        assert panel.shape == (1, 1)
        # (1200 - 1000) / 1000 = 0.2
        assert abs(panel.loc["2026-06-01", 1] - 0.2) < 1e-10


class TestDeltaFirstQuarterIsNan:
    """test_delta_first_quarter_is_nan: first quarter has no prior -> NaN."""

    def test_nan_for_first_quarter(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 5_000, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-01"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        assert np.isnan(panel.loc["2026-03-01", 1])


class TestDeltaZeroPriorReturnsNan:
    """test_delta_zero_prior_returns_nan: prior=0 -> NaN (division by zero guarded)."""

    def test_zero_prior_shares(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 0, 0.04),   # zero shares
            (1, "2026-05-15", "2026-03-31", 55, 1_200_000, 1_200, 0.05),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-01"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # Division by zero should produce NaN, not inf
        assert np.isnan(panel.loc["2026-06-01", 1])


class TestDeltaEmpty:
    """test_delta_empty: no data -> empty panel."""

    def test_empty_agg(self, monkeypatch):
        _patch_agg(monkeypatch)

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-01"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1, 2],
        )

        pd.testing.assert_index_equal(panel.index, dates)
        assert panel.columns.tolist() == [1, 2]
        assert panel.dtypes.tolist() == [np.float64, np.float64]
        assert panel.isna().all().all()


class TestDeltaSecurityIdsFilter:
    """test_delta_security_ids_filter: filter to subset + ghost security."""

    def test_filter_and_ghost(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2026-02-15", "2025-12-31", 50, 1_000_000, 1_000, 0.04),
            (1, "2026-05-15", "2026-03-31", 55, 1_200_000, 1_200, 0.05),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-01"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1, 999],
        )

        assert panel.columns.tolist() == [1, 999]
        assert abs(panel.loc["2026-06-01", 1] - 0.2) < 1e-10
        assert panel[999].isna().all()


# ---------------------------------------------------------------------------
# Builtin factor tests
# ---------------------------------------------------------------------------

class TestInstitutionalBreadthFactorShape:
    """test_institutional_breadth_factor_shape: verify compute returns correct shape."""

    def test_shape(self, monkeypatch):
        dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
        universe = pd.Index([10, 20, 30], dtype="int64")
        loaded = {
            "n_holders": pd.DataFrame(
                {10: [50.0, 55.0], 20: [30.0, 35.0]}, index=dates, dtype="float64",
            ),
            "total_value": pd.DataFrame(
                {10: [1e6, 1.1e6], 20: [5e5, 6e5]}, index=dates, dtype="float64",
            ),
            "total_shares": pd.DataFrame(
                {10: [5000.0, 5500.0], 20: [3000.0, 3500.0]}, index=dates, dtype="float64",
            ),
            "hhi": pd.DataFrame(
                {10: [0.04, 0.05], 20: [0.10, 0.08]}, index=dates, dtype="float64",
            ),
        }

        def fake_loader(engine, *, dates, security_ids):
            return loaded

        monkeypatch.setattr(_ib_mod, "load_institutional_holdings_panel", fake_loader)
        ctx = FactorContext(object(), dates=dates, security_universe=universe)

        panel = InstitutionalBreadthFactor().compute(ctx)

        assert panel.shape == (2, 3)
        pd.testing.assert_index_equal(panel.index, dates)
        pd.testing.assert_index_equal(panel.columns, universe)
        assert panel.dtypes.tolist() == [np.float64, np.float64, np.float64]
        # sec 30 not in loaded data -> NaN after reindex
        assert panel[30].isna().all()
        assert panel.loc["2026-01-15", 10] == 50.0


class TestOwnershipConcentrationFactorShape:
    """test_ownership_concentration_factor_shape: verify compute returns correct shape."""

    def test_shape(self, monkeypatch):
        dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
        universe = pd.Index([10, 20, 30], dtype="int64")
        loaded = {
            "n_holders": pd.DataFrame(
                {10: [50.0, 55.0], 20: [30.0, 35.0]}, index=dates, dtype="float64",
            ),
            "total_value": pd.DataFrame(
                {10: [1e6, 1.1e6], 20: [5e5, 6e5]}, index=dates, dtype="float64",
            ),
            "total_shares": pd.DataFrame(
                {10: [5000.0, 5500.0], 20: [3000.0, 3500.0]}, index=dates, dtype="float64",
            ),
            "hhi": pd.DataFrame(
                {10: [0.04, 0.05], 20: [0.10, 0.08]}, index=dates, dtype="float64",
            ),
        }

        def fake_loader(engine, *, dates, security_ids):
            return loaded

        monkeypatch.setattr(_oc_mod, "load_institutional_holdings_panel", fake_loader)
        ctx = FactorContext(object(), dates=dates, security_universe=universe)

        panel = OwnershipConcentrationFactor().compute(ctx)

        assert panel.shape == (2, 3)
        pd.testing.assert_index_equal(panel.index, dates)
        pd.testing.assert_index_equal(panel.columns, universe)
        assert panel[30].isna().all()
        assert panel.loc["2026-01-15", 10] == 0.04
        assert panel.loc["2026-01-16", 20] == 0.08


class TestDeltaInstitutionalOwnershipFactorShape:
    """test_delta_institutional_ownership_factor_shape: verify compute returns correct shape."""

    def test_shape(self, monkeypatch):
        dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
        universe = pd.Index([10, 20, 30], dtype="int64")
        loaded = pd.DataFrame(
            {10: [0.1, 0.15], 20: [0.2, 0.25]}, index=dates, dtype="float64",
        )

        def fake_loader(engine, *, dates, security_ids):
            return loaded

        monkeypatch.setattr(_dio_mod, "load_delta_institutional_ownership_panel", fake_loader)
        ctx = FactorContext(object(), dates=dates, security_universe=universe)

        panel = DeltaInstitutionalOwnershipFactor().compute(ctx)

        assert panel.shape == (2, 3)
        pd.testing.assert_index_equal(panel.index, dates)
        pd.testing.assert_index_equal(panel.columns, universe)
        assert panel[30].isna().all()
        assert panel.loc["2026-01-15", 10] == 0.1


class TestAllThreeRegistered:
    """test_all_three_registered: import triggers registration, get() returns correct types."""

    def test_registry(self):
        from research.factors.protocol import get

        ib = get("institutional_breadth")
        oc = get("ownership_concentration")
        dio = get("delta_institutional_ownership")

        assert isinstance(ib, InstitutionalBreadthFactor)
        assert isinstance(oc, OwnershipConcentrationFactor)
        assert isinstance(dio, DeltaInstitutionalOwnershipFactor)

        # Verify metadata
        assert ib.name == "institutional_breadth"
        assert ib.pit_guarantee is True
        assert oc.name == "ownership_concentration"
        assert dio.name == "delta_institutional_ownership"
