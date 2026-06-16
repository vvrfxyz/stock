from __future__ import annotations

import numpy as np
import pandas as pd

import research.factors.builtins.short_interest as short_interest_builtin
from research.factors.builtins.short_interest import ShortInterestFactor
from research.factors.protocol import FactorContext


def test_short_interest_factor_returns_panel_shape(monkeypatch):
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15", "2026-01-16"]))
    universe = pd.Index([10, 20, 30], dtype="int64")
    loaded = pd.DataFrame({10: [0.1, 0.2], 20: [0.3, 0.4]}, index=dates, dtype="float64")

    def fake_loader(engine, *, dates, security_ids):
        return loaded

    monkeypatch.setattr(short_interest_builtin, "load_short_interest_ratio_panel", fake_loader)
    ctx = FactorContext(object(), dates=dates, security_universe=universe)

    panel = ShortInterestFactor().compute(ctx)

    assert panel.shape == (2, 3)
    pd.testing.assert_index_equal(panel.index, dates)
    pd.testing.assert_index_equal(panel.columns, universe)
    assert panel.dtypes.tolist() == [np.float64, np.float64, np.float64]
    assert panel[30].isna().all()


def test_short_interest_factor_registered():
    from research.factors.builtins import short_interest as _trigger  # noqa: F401
    from research.factors.protocol import get

    assert isinstance(get("short_interest_ratio"), ShortInterestFactor)


def test_short_interest_factor_does_not_pass_as_of_to_loader(monkeypatch):
    calls = []
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-15"]))
    universe = pd.Index([10], dtype="int64")

    def fake_loader(engine, **kwargs):
        calls.append(kwargs)
        return pd.DataFrame({10: [0.1]}, index=dates, dtype="float64")

    monkeypatch.setattr(short_interest_builtin, "load_short_interest_ratio_panel", fake_loader)
    ctx = FactorContext(object(), dates=dates, security_universe=universe, as_of=pd.Timestamp("2026-02-01"))

    ShortInterestFactor().compute(ctx)

    assert calls == [{"dates": dates, "security_ids": [10]}]
    assert "as_of" not in calls[0]
