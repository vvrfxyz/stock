"""Tests for research.institutional (13F holdings panel loaders) and the three
institutional builtin factors: institutional_breadth, ownership_concentration,
delta_institutional_ownership.

All tests run without a database connection: panel tests monkeypatch
``load_institutional_aggregates``; aggregate-level tests (period monotonic
guard, HHI, two-stage straggler/panel semantics) monkeypatch
``pd.read_sql_query``. The aggregation SQL itself (accession-level dedup,
original-only visibility, two-stage on-time/final event emission) is locked
by the PostgreSQL integration tests in tests/test_institutional_pg.py.

Two-stage visibility (2026-07): each (security_id, period) may emit up to two
events — an on-time batch (only filings by period + 46 days) and a final one
(all filings). Tests here lock the pandas-side behavior: the monotonic guard
letting both same-period events through while dropping late old-period finals,
the panel showing on-time values from the deadline (no more "born stale"
quarters), and the delta prior-base pairing rule (latest prior-period event
with visible_date <= current event's — no lookahead).
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
            return _empty_agg()
        return agg.copy()

    monkeypatch.setattr(institutional, "load_institutional_aggregates", fake_load)


def _sql_result(*rows) -> pd.DataFrame:
    """Build a DataFrame shaped like the raw SQL result of load_institutional_aggregates.

    Each row: (security_id, period, visible_date, n_holders, total_value,
    total_shares, sum_sq_value) — SQL emits rows ordered by (security_id, period).
    """
    df = pd.DataFrame(
        rows,
        columns=["security_id", "period", "visible_date",
                 "n_holders", "total_value", "total_shares", "sum_sq_value"],
    )
    for col in ("visible_date", "period"):
        df[col] = pd.to_datetime(df[col])
    return df


def _patch_sql(monkeypatch, *rows):
    """Monkeypatch pd.read_sql_query so load_institutional_aggregates runs for real."""
    result = _sql_result(*rows)

    def fake_read_sql_query(sql, engine, *, params=None, parse_dates=None):
        return result.copy()

    monkeypatch.setattr(institutional.pd, "read_sql_query", fake_read_sql_query)


_DUMMY_ENGINE = object()


def _ns_index(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(index).astype("datetime64[ns]")


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
            pd.testing.assert_index_equal(panels[key].index, _ns_index(dates))

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
            pd.testing.assert_index_equal(panel.index, _ns_index(dates))
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

        pd.testing.assert_index_equal(panel.index, _ns_index(dates))
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
# Aggregate loader tests (real load_institutional_aggregates, mocked SQL result)
# ---------------------------------------------------------------------------

class TestAggregatesPeriodMonotonicGuard:
    """A late filing for an old period must not roll the as-of series backwards."""

    def test_late_old_period_event_dropped(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 filed late (2026-03-01) — visible AFTER Q4's visible_date
            (1, "2025-09-30", "2026-03-01", 10, 100_000.0, 1_000.0, 1_000_000.0),
            (1, "2025-12-31", "2026-02-15", 12, 120_000.0, 1_200.0, 1_440_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        # Only the Q4 event survives: the late Q3 event would make merge_asof
        # fall back to stale Q3 values after 2026-03-01
        assert agg["period"].tolist() == [pd.Timestamp("2025-12-31")]
        assert agg["n_holders"].tolist() == [12.0]

    def test_in_order_events_untouched(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            (1, "2025-09-30", "2025-11-15", 10, 100_000.0, 1_000.0, 1_000_000.0),
            (1, "2025-12-31", "2026-02-15", 12, 120_000.0, 1_200.0, 1_440_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert agg["period"].tolist() == [
            pd.Timestamp("2025-09-30"), pd.Timestamp("2025-12-31"),
        ]

    def test_guard_is_per_security(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # sec 1: normal ordering
            (1, "2025-12-31", "2026-02-15", 12, 120_000.0, 1_200.0, 1_440_000.0),
            # sec 2: only an old-period event — must NOT be dropped by sec 1's newer period
            (2, "2025-09-30", "2026-03-01", 5, 50_000.0, 500.0, 250_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert set(agg["security_id"]) == {1, 2}


class TestAggregatesHhi:
    """HHI = sum(filer_value^2) / total_value^2, NaN when total_value <= 0."""

    def test_hhi_from_sum_sq(self, monkeypatch):
        # two equal filers of 500k each: hhi = 2*500k^2 / 1M^2 = 0.5
        _patch_sql(
            monkeypatch,
            (1, "2025-12-31", "2026-02-15", 2, 1_000_000.0, 1_000.0, 2 * 500_000.0 ** 2),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert abs(agg["hhi"].iloc[0] - 0.5) < 1e-12

    def test_hhi_nan_on_zero_total_value(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            (1, "2025-12-31", "2026-02-15", 1, 0.0, 1_000.0, 0.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert np.isnan(agg["hhi"].iloc[0])


class TestDeltaPriorBaseAfterGuard:
    """Delta prior-quarter base must come from an event visible no later than the
    current quarter's event — guaranteed by the monotonic guard."""

    def test_late_old_quarter_does_not_corrupt_delta(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q2 base
            (1, "2025-06-30", "2025-08-15", 10, 100_000.0, 1_000.0, 1_000_000.0),
            # Q3 filed extremely late — dropped by the guard
            (1, "2025-09-30", "2026-03-01", 10, 100_000.0, 9_999.0, 1_000_000.0),
            # Q4
            (1, "2025-12-31", "2026-02-15", 12, 120_000.0, 1_200.0, 1_440_000.0),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-03-15"]))
        panel = load_delta_institutional_ownership_panel(
            object(), dates=dates, security_ids=[1],
        )

        # Prior base is Q2's 1000 shares (visible 2025-08-15 <= Q4's 2026-02-15),
        # not the dropped late-Q3 9999: (1200 - 1000) / 1000 = 0.2
        assert abs(panel.loc["2026-03-15", 1] - 0.2) < 1e-10

    def test_prior_visible_no_later_than_current(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            (1, "2025-09-30", "2025-11-15", 10, 100_000.0, 1_000.0, 1_000_000.0),
            (1, "2025-12-31", "2026-02-15", 12, 120_000.0, 1_500.0, 1_440_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        # After the guard, visible_date is non-decreasing within each security,
        # so every prior-quarter base was already visible at the current event
        by_sec = agg.sort_values(["security_id", "period"])
        assert by_sec.groupby("security_id")["visible_date"].is_monotonic_increasing.all()


# ---------------------------------------------------------------------------
# Two-stage visibility tests (on-time batch + final event per period)
# ---------------------------------------------------------------------------

class TestTwoStageGuardCoexistence:
    """Monotonic guard + two-stage events: same-period pairs pass, late
    old-period finals drop while their on-time batch survives."""

    def test_same_period_pair_passes_guard(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 on-time batch then Q3 final: same period, ascending visible_date
            (1, "2025-09-30", "2025-11-14", 40, 400_000.0, 4_000.0, 4_000_000.0),
            (1, "2025-09-30", "2025-12-20", 41, 410_000.0, 4_100.0, 4_100_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert len(agg) == 2
        assert agg["period"].tolist() == [pd.Timestamp("2025-09-30")] * 2
        assert agg["visible_date"].tolist() == [
            pd.Timestamp("2025-11-14"), pd.Timestamp("2025-12-20"),
        ]
        # hhi computed per event row
        assert agg["hhi"].notna().all()

    def test_late_final_dropped_ontime_survives(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 on-time batch
            (1, "2025-09-30", "2025-11-14", 40, 400_000.0, 4_000.0, 4_000_000.0),
            # Q3 final arrives AFTER Q4's on-time batch — must be dropped
            (1, "2025-09-30", "2026-06-07", 41, 410_000.0, 4_100.0, 4_100_000.0),
            # Q4 (no straggler for Q4: single final-only event)
            (1, "2025-12-31", "2026-02-14", 42, 420_000.0, 4_200.0, 4_200_000.0),
        )

        agg = institutional.load_institutional_aggregates(object())

        assert list(zip(agg["period"], agg["visible_date"])) == [
            (pd.Timestamp("2025-09-30"), pd.Timestamp("2025-11-14")),
            (pd.Timestamp("2025-12-31"), pd.Timestamp("2026-02-14")),
        ]
        assert agg["n_holders"].tolist() == [40.0, 42.0]


class TestTwoStageStragglerPanel:
    """Core fix: a straggler filing ~250 days late no longer blanks the whole
    quarter — the on-time batch is visible from the deadline until next quarter."""

    def test_no_nan_gap_despite_250_day_straggler(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 on-time batch: visible at the deadline
            (1, "2025-09-30", "2025-11-14", 40, 400_000.0, 4_000.0, 4_000_000.0),
            # Q3 final: straggler filed ~250 days after period end. Old single-event
            # semantics made this the ONLY Q3 event with visible_date 2026-06-07,
            # already past period + 200d staleness -> the quarter was born stale (NaN).
            (1, "2025-09-30", "2026-06-07", 41, 410_000.0, 4_100.0, 4_100_000.0),
            # Q4 on time
            (1, "2025-12-31", "2026-02-14", 42, 420_000.0, 4_200.0, 4_200_000.0),
        )

        dates = pd.DatetimeIndex(pd.to_datetime([
            "2025-11-13",   # before the on-time batch is visible
            "2025-11-14",   # on-time batch visible at the deadline
            "2026-01-15",   # mid-quarter: old semantics gave NaN here
            "2026-02-13",   # last day before Q4
            "2026-02-14",   # Q4 takes over
            "2026-06-15",   # after the straggler: Q4 still shown, no regression
        ]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        n = panels["n_holders"][1]
        assert np.isnan(n.loc["2025-11-13"])
        assert n.loc["2025-11-14"] == 40.0
        assert n.loc["2026-01-15"] == 40.0       # the fix: no more "born stale" NaN
        assert n.loc["2026-02-13"] == 40.0
        assert n.loc["2026-02-14"] == 42.0
        assert n.loc["2026-06-15"] == 42.0       # late Q3 final dropped, no rollback
        # No NaN gap anywhere from deadline through Q4 staleness window
        assert n.loc["2025-11-14":].notna().all()

    def test_final_updates_values_when_not_late_beyond_next_quarter(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 on-time batch then Q3 final one month later (before Q4 events)
            (1, "2025-09-30", "2025-11-14", 40, 400_000.0, 4_000.0, 4_000_000.0),
            (1, "2025-09-30", "2025-12-20", 41, 410_000.0, 4_100.0, 4_100_000.0),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2025-12-01", "2026-01-05"]))
        panels = load_institutional_holdings_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # On-time value first, switched to the full aggregate once final arrives
        assert panels["n_holders"].loc["2025-12-01", 1] == 40.0
        assert panels["n_holders"].loc["2026-01-05", 1] == 41.0


class TestDeltaTwoStagePairing:
    """Delta prior base = latest prior-period event with visible_date <= the
    current event's visible_date. _patch_agg bypasses the guard, so these lock
    the pairing rule itself (including the lookahead lock)."""

    def test_ontime_current_ignores_later_prior_final(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            # Q3 on-time (1000 sh) and Q3 final (1100 sh, visible AFTER Q4 on-time)
            (1, "2025-11-14", "2025-09-30", 40, 400_000.0, 1_000.0, 0.04),
            (1, "2026-03-01", "2025-09-30", 41, 410_000.0, 1_100.0, 0.04),
            # Q4 on-time (1200 sh) and Q4 final (1300 sh)
            (1, "2026-02-14", "2025-12-31", 42, 420_000.0, 1_200.0, 0.04),
            (1, "2026-03-20", "2025-12-31", 43, 430_000.0, 1_300.0, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-20", "2026-03-25"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # Q4 on-time at 2026-02-20: prior Q3 final (visible 2026-03-01) is NOT
        # yet visible -> base is Q3 on-time's 1000, never 1100 (lookahead lock)
        assert abs(panel.loc["2026-02-20", 1] - 0.2) < 1e-10
        # Q4 final at 2026-03-25: prior Q3 final now visible -> base 1100
        assert abs(panel.loc["2026-03-25", 1] - (1_300.0 - 1_100.0) / 1_100.0) < 1e-10

    def test_prior_final_visible_same_day_is_valid_base(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            (1, "2025-11-14", "2025-09-30", 40, 400_000.0, 1_000.0, 0.04),
            # Q3 final visible the SAME day as Q4 on-time: <= rule, not lookahead
            (1, "2026-02-14", "2025-09-30", 41, 410_000.0, 1_100.0, 0.04),
            (1, "2026-02-14", "2025-12-31", 42, 420_000.0, 1_200.0, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-20"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # base = Q3 final 1100 (visible_date equal is allowed): (1200-1100)/1100
        assert abs(panel.loc["2026-02-20", 1] - (1_200.0 - 1_100.0) / 1_100.0) < 1e-10

    def test_no_visible_prior_candidate_is_nan(self, monkeypatch):
        _patch_agg(
            monkeypatch,
            # Q3's ONLY event (all-late final) visible after Q4 on-time
            (1, "2026-03-01", "2025-09-30", 41, 410_000.0, 1_000.0, 0.04),
            (1, "2026-02-14", "2025-12-31", 42, 420_000.0, 1_200.0, 0.04),
            (1, "2026-03-20", "2025-12-31", 43, 430_000.0, 1_300.0, 0.04),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-20", "2026-03-25"]))
        panel = load_delta_institutional_ownership_panel(
            _DUMMY_ENGINE, dates=dates, security_ids=[1],
        )

        # Q4 on-time: no prior-period candidate visible yet -> NaN
        assert np.isnan(panel.loc["2026-02-20", 1])
        # Q4 final: Q3's event visible by 2026-03-20 -> base 1000
        assert abs(panel.loc["2026-03-25", 1] - 0.3) < 1e-10

    def test_end_to_end_with_guard_straggler_final_never_used(self, monkeypatch):
        _patch_sql(
            monkeypatch,
            # Q3 on-time (1000 sh); Q3 final is a straggler after Q4 events (guard drops it)
            (1, "2025-09-30", "2025-11-14", 40, 400_000.0, 1_000.0, 4_000_000.0),
            (1, "2025-09-30", "2026-06-07", 41, 410_000.0, 1_100.0, 4_100_000.0),
            # Q4 on-time (1200 sh) and Q4 final (1250 sh)
            (1, "2025-12-31", "2026-02-14", 42, 420_000.0, 1_200.0, 4_200_000.0),
            (1, "2025-12-31", "2026-03-05", 43, 430_000.0, 1_250.0, 4_300_000.0),
        )

        dates = pd.DatetimeIndex(pd.to_datetime(["2026-02-20", "2026-03-10"]))
        panel = load_delta_institutional_ownership_panel(
            object(), dates=dates, security_ids=[1],
        )

        # Both Q4 events pair with Q3's on-time 1000 (the dropped straggler
        # final never becomes a base)
        assert abs(panel.loc["2026-02-20", 1] - 0.2) < 1e-10
        assert abs(panel.loc["2026-03-10", 1] - 0.25) < 1e-10


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
