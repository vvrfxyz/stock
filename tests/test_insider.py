"""Tests for insider net buy panel and builtin factor."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock

import research.factors.protocol as _proto
from research.factors.protocol import FactorContext, get
from research.insider import load_insider_net_buy_panel


# ---------------------------------------------------------------------------
# helpers / fixtures
# ---------------------------------------------------------------------------

def _insider_events(*rows) -> pd.DataFrame:
    """Build a tiny insider events DataFrame matching load_insider_events schema."""
    if not rows:
        return pd.DataFrame(
            {
                "security_id": pd.Series(dtype=np.int64),
                "visible_date": pd.Series(dtype="datetime64[ns]"),
                "transaction_date": pd.Series(dtype="datetime64[ns]"),
                "signed_shares": pd.Series(dtype=np.float64),
            }
        )
    df = pd.DataFrame(rows, columns=["security_id", "visible_date", "transaction_date", "signed_shares"])
    for col in ("visible_date", "transaction_date"):
        df[col] = pd.to_datetime(df[col])
    df["security_id"] = df["security_id"].astype(np.int64)
    df["signed_shares"] = df["signed_shares"].astype(np.float64)
    return df


@pytest.fixture()
def _isolate_registry():
    """Save and restore the factor registry around each test."""
    saved = dict(_proto._REGISTRY)
    yield
    _proto._REGISTRY.clear()
    _proto._REGISTRY.update(saved)


def _patch_events(monkeypatch, events: pd.DataFrame):
    """Monkeypatch load_insider_events to return *events* regardless of args."""
    monkeypatch.setattr(
        "research.insider.load_insider_events",
        lambda engine, *, security_ids=None: events,
    )


# ---------------------------------------------------------------------------
# panel tests
# ---------------------------------------------------------------------------

def test_net_buy_basic(monkeypatch):
    """Buy 100 on day 1, sell 50 on day 10. At day 15: net = +50. Two securities."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", -50.0),
        (2, "2025-01-05", "2025-01-05", 200.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 2],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(50.0)
    assert panel.loc[pd.Timestamp("2025-01-15"), 2] == pytest.approx(200.0)


def test_visible_delay(monkeypatch):
    """Event visible_date=Jan 15, delay=1. On Jan 15: NaN. On Jan 16: see the event."""
    events = _insider_events(
        (1, "2025-01-15", "2025-01-15", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15", "2025-01-16"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    # effective_visible_date = Jan 16; on Jan 15 the event is not yet visible
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 1])
    assert panel.loc[pd.Timestamp("2025-01-16"), 1] == pytest.approx(100.0)


def test_window_expiry(monkeypatch):
    """Buy 100 on day 1, window=30 days. At day 31: still 100. At day 32: drops to 0.

    effective_visible_date = 2025-01-02 (delay=1).
    date=2025-01-31: upper <= Jan31 => 100; lower <= (Jan31-30d=Jan01) => NaN(0) => net=100.
    date=2025-02-01: upper <= Feb01 => 100; lower <= (Feb01-30d=Jan02) => 100 => net=0.
    """
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-31", "2025-02-01"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=30,
    )
    assert panel.loc[pd.Timestamp("2025-01-31"), 1] == pytest.approx(100.0)
    assert panel.loc[pd.Timestamp("2025-02-01"), 1] == pytest.approx(0.0)


def test_accumulation(monkeypatch):
    """Multiple buys within window sum up correctly."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", 50.0),
        (1, "2025-01-20", "2025-01-20", 75.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-25"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-25"), 1] == pytest.approx(225.0)


def test_buy_and_sell_cancel(monkeypatch):
    """Buy 100 + sell 100 within window -> net = 0."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (1, "2025-01-10", "2025-01-10", -100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(0.0)


def test_no_events_for_security_is_nan(monkeypatch):
    """Security in universe but no insider events -> NaN, not 0."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 999],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(100.0)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 999])


def test_empty_events(monkeypatch):
    """No events at all -> all NaN."""
    _patch_events(monkeypatch, _insider_events())

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1, 2],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 2)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 1])
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 2])


def test_empty_dates(monkeypatch):
    """dates=[] -> empty index."""
    events = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime([]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert len(panel) == 0
    assert 1 in panel.columns


def test_empty_security_ids(monkeypatch):
    """security_ids=[] -> empty columns."""
    _patch_events(monkeypatch, _insider_events())

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.shape == (1, 0)


def test_security_ids_filter(monkeypatch):
    """Test None vs explicit list vs empty list for security_ids."""
    events_all = _insider_events(
        (1, "2025-01-01", "2025-01-01", 100.0),
        (2, "2025-01-01", "2025-01-01", 200.0),
        (3, "2025-01-01", "2025-01-01", 300.0),
    )

    calls: list = []

    def mock_load(engine, *, security_ids=None):
        calls.append(security_ids)
        if security_ids is None:
            return events_all
        return events_all[events_all["security_id"].isin(security_ids)].copy()

    monkeypatch.setattr("research.insider.load_insider_events", mock_load)

    dates = pd.to_datetime(["2025-01-15"])

    # None -> discover universe from events
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=None)
    assert panel.shape[1] == 3
    assert calls[-1] is None

    # [2, 999]
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=[2, 999])
    assert panel.shape[1] == 2
    assert set(calls[-1]) == {2, 999}
    assert panel.loc[pd.Timestamp("2025-01-15"), 2] == pytest.approx(200.0)
    assert pd.isna(panel.loc[pd.Timestamp("2025-01-15"), 999])

    # []
    panel = load_insider_net_buy_panel(MagicMock(), dates=dates, security_ids=[])
    assert panel.shape[1] == 0


def test_dedupes_security_ids(monkeypatch):
    """[2,2,999] -> deduplicated to [2,999]."""
    events = _insider_events(
        (2, "2025-01-01", "2025-01-01", 200.0),
    )

    calls: list = []

    def mock_load(engine, *, security_ids=None):
        calls.append(security_ids)
        return events

    monkeypatch.setattr("research.insider.load_insider_events", mock_load)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[2, 2, 999],
    )
    assert panel.shape[1] == 2
    # load_insider_events should receive deduplicated list
    passed_ids = calls[-1]
    assert sorted(passed_ids) == [2, 999]


def test_multiple_events_same_filing_date(monkeypatch):
    """CEO and CFO both file on same day; their shares should accumulate."""
    events = _insider_events(
        (1, "2025-01-10", "2025-01-10", 100.0),  # CEO
        (1, "2025-01-10", "2025-01-10", 50.0),   # CFO
    )
    _patch_events(monkeypatch, events)

    panel = load_insider_net_buy_panel(
        MagicMock(),
        dates=pd.to_datetime(["2025-01-15"]),
        security_ids=[1],
        visible_delay_days=1,
        window_days=90,
    )
    assert panel.loc[pd.Timestamp("2025-01-15"), 1] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# builtin factor tests
# ---------------------------------------------------------------------------

def test_builtin_factor_shape(monkeypatch, _isolate_registry):
    """InsiderNetBuyFactor.compute returns the correct shape via FactorContext."""
    import research.factors.builtins.insider_net_buy as _mod

    dates = pd.DatetimeIndex(pd.to_datetime(["2025-01-15", "2025-01-16"]))
    universe = pd.Index([1, 2], dtype=np.int64)

    mock_panel = pd.DataFrame(
        [[10.0, 20.0], [30.0, 40.0]],
        index=dates,
        columns=universe,
        dtype=np.float64,
    )

    def fake_loader(engine, *, dates, security_ids):
        return mock_panel

    monkeypatch.setattr(_mod, "load_insider_net_buy_panel", fake_loader)

    from research.factors.builtins.insider_net_buy import InsiderNetBuyFactor

    # register in the isolated registry
    _proto._REGISTRY.pop("insider_net_buy", None)
    _proto.register(InsiderNetBuyFactor())

    ctx = FactorContext(engine=MagicMock(), dates=dates, security_universe=universe)
    factor = get("insider_net_buy")
    result = factor.compute(ctx)

    assert result.shape == (2, 2)
    pd.testing.assert_frame_equal(result, mock_panel)


def test_builtin_factor_registered(_isolate_registry):
    """get('insider_net_buy') returns InsiderNetBuyFactor."""
    from research.factors.builtins.insider_net_buy import InsiderNetBuyFactor

    _proto._REGISTRY.pop("insider_net_buy", None)
    _proto.register(InsiderNetBuyFactor())

    factor = get("insider_net_buy")
    assert isinstance(factor, InsiderNetBuyFactor)
    assert factor.name == "insider_net_buy"
    assert factor.lookback_days == 90
    assert factor.lag_days == 1
    assert factor.pit_guarantee is True
