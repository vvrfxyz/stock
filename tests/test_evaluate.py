from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import pytest

from research.backtest import hold_between_rebalances, run_backtest
from research.evaluate import (
    FactorEvaluationError,
    _forward_return,
    _params_hash,
    _pit_regression,
    _quantile_weights_for_day,
    _rank_ic_series,
    default_nw_lag,
    evaluate_factor,
)
from research.factors.protocol import FactorContext


def _panel(n_dates: int = 120, n_names: int = 120) -> tuple[pd.DatetimeIndex, pd.Index, pd.DataFrame]:
    dates = pd.bdate_range("2025-01-02", periods=n_dates)
    universe = pd.Index(range(1, n_names + 1), dtype="int64")
    base = np.linspace(-1.0, 1.0, n_names)
    drift = np.linspace(0, 0.2, n_dates)[:, None]
    factor = pd.DataFrame(base + drift, index=dates, columns=universe)
    return dates, universe, factor


def _prices_from_returns(forward_returns: pd.DataFrame, horizon: int = 1) -> pd.DataFrame:
    if horizon != 1:
        raise NotImplementedError
    out = pd.DataFrame(100.0, index=forward_returns.index, columns=forward_returns.columns)
    for i in range(1, len(out.index)):
        prev_return = forward_returns.iloc[i - 1].fillna(0.0)
        out.iloc[i] = out.iloc[i - 1] * (1 + prev_return)
    return out


def test_perfect_ic_yields_high_nw_t():
    _, _, factor = _panel(n_dates=80, n_names=120)
    forward = {1: factor.copy()}
    eligible = pd.DataFrame(True, index=factor.index, columns=factor.columns)

    result = evaluate_factor(factor, forward, eligibility=eligible, horizons=(1,), min_coverage=50)

    assert result.ic_table.loc[1, "mean_ic"] > 0.99
    assert result.ic_table.loc[1, "nw_t"] > 10


def test_pure_noise_factor_is_noisy():
    rng = np.random.default_rng(42)
    dates, universe, _ = _panel(n_dates=180, n_names=500)
    factor = pd.DataFrame(rng.normal(size=(len(dates), len(universe))), index=dates, columns=universe)
    fwd = pd.DataFrame(rng.normal(size=(len(dates), len(universe))), index=dates, columns=universe)
    eligible = pd.DataFrame(True, index=dates, columns=universe)

    result = evaluate_factor(factor, {1: fwd}, eligibility=eligible, horizons=(1,), min_coverage=50)

    assert abs(result.ic_table.loc[1, "mean_ic"]) < 0.02
    assert abs(result.ic_table.loc[1, "nw_t"]) < 1.5
    assert result.ic_table.loc[1, "is_noisy"] is True


def test_lookahead_detection():
    rng = np.random.default_rng(7)
    dates, universe, _ = _panel(n_dates=120, n_names=120)
    fwd = pd.DataFrame(rng.normal(size=(len(dates), len(universe))), index=dates, columns=universe)
    factor = fwd.copy()
    eligible = pd.DataFrame(True, index=dates, columns=universe)

    result = evaluate_factor(factor, {1: fwd}, eligibility=eligible, horizons=(1,), min_coverage=50)

    assert result.ic_table.loc[1, "mean_ic"] > 0.5
    assert result.diagnostics["ic_decay_halflife"] < 2
    assert result.diagnostics["lookahead_suspect"] is True


def test_ic_decay_monotonic_on_ar1():
    dates, universe, factor = _panel(n_dates=160, n_names=120)
    forward = {h: factor * (0.8 ** h) + h * np.arange(len(universe))[None, :] * 1e-8 for h in (1, 5, 10)}
    eligible = pd.DataFrame(True, index=dates, columns=universe)

    result = evaluate_factor(factor, forward, eligibility=eligible, horizons=(1, 5, 10), min_coverage=50)
    decay = result.ic_decay[result.ic_decay["lag"] == 0].set_index("horizon")["ic"]

    assert decay.loc[1] >= decay.loc[5] >= decay.loc[10]


@pytest.mark.parametrize(
    "x,y,expected",
    [
        ([1, 2, 3], [1, 2, 3], 1.0),
        ([1, 2, 3], [3, 2, 1], -1.0),
        ([1, 1, 2], [1, 2, 3], 0.8660254037844387),
    ],
)
def test_rank_ic_matches_spearmanr_hardcoded(x, y, expected):
    dates = pd.to_datetime(["2025-01-02"])
    factor = pd.DataFrame([x], index=dates, columns=[1, 2, 3])
    fwd = pd.DataFrame([y], index=dates, columns=[1, 2, 3])

    ic = _rank_ic_series(factor, fwd, min_coverage=1)

    assert ic.iloc[0] == pytest.approx(expected, abs=1e-10)


def test_nw_lag_default_formula():
    expected = {
        (100, 1): 4,
        (100, 5): 5,
        (100, 21): 21,
        (252, 1): 4,
        (252, 5): 5,
        (252, 21): 21,
        (520, 1): 5,
        (520, 5): 5,
        (520, 21): 21,
        (1000, 1): 6,
        (1000, 5): 6,
        (1000, 21): 21,
    }
    for (t, h), value in expected.items():
        assert default_nw_lag(h, t) == value


def test_quantile_ir_matches_run_backtest_directly():
    dates, universe, factor = _panel(n_dates=90, n_names=120)
    fwd = factor.rank(axis=1, pct=True) / 1000
    adj_close = _prices_from_returns(fwd)
    eligible = pd.DataFrame(True, index=dates, columns=universe)

    result = evaluate_factor(
        factor,
        {1: _forward_return(adj_close, 1)},
        eligibility=eligible,
        horizons=(1,),
        adj_close=adj_close,
        cost_bps=0,
        min_coverage=50,
    )
    rebalance = factor.index[::1]
    weights_at_rebalance = pd.DataFrame(0.0, index=rebalance, columns=universe)
    for dt in rebalance:
        weights_at_rebalance.loc[dt] = _quantile_weights_for_day(factor.loc[dt], eligible.loc[dt], 5)["q5"]
    weights = hold_between_rebalances(weights_at_rebalance, adj_close.index)
    direct = run_backtest("direct", weights, adj_close, cost_bps=0).metrics()["sharpe"]

    assert result.quantile_metrics.loc[(1, "q5"), "sharpe_net"] == pytest.approx(direct)


def test_coverage_diagnostic():
    _, _, factor = _panel(n_dates=80, n_names=120)
    factor.iloc[:, ::2] = np.nan
    eligible = pd.DataFrame(True, index=factor.index, columns=factor.columns)

    result = evaluate_factor(factor, {1: factor.copy()}, eligibility=eligible, horizons=(1,), min_coverage=10)

    assert result.coverage["factor_coverage"].mean() == pytest.approx(0.5, abs=0.05)


def test_empty_factor_raises_and_single_horizon_all_nan_skips():
    dates, universe, factor = _panel(n_dates=80, n_names=120)
    eligible = pd.DataFrame(True, index=dates, columns=universe)
    empty = pd.DataFrame(np.nan, index=dates, columns=universe)

    with pytest.raises(FactorEvaluationError):
        evaluate_factor(empty, {1: factor}, eligibility=eligible, horizons=(1,))

    result = evaluate_factor(factor, {1: empty}, eligibility=eligible, horizons=(1,), min_coverage=50)
    rows = result.to_trial_rows()

    assert result.ic_table.loc[1, "is_noisy"] is True
    assert any(row["metric"] == "flag_horizon_skipped" and row["horizon"] == 1 for row in rows)


@dataclass(frozen=True)
class RecordingFactor:
    name = "recording"
    seen_max_date: pd.Timestamp | None = None

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        object.__setattr__(self, "seen_max_date", ctx.dates.max())
        return pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)


def test_factor_context_as_of_truncates_dates(monkeypatch):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    factor = RecordingFactor()
    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "_load_active_status", lambda engine, columns: pd.Series(True, index=columns))
    monkeypatch.setattr(ev, "_git_meta", lambda: (None, False), raising=False)

    ev.run_evaluation(
        factor,
        engine=object(),
        start=dates.min().date(),
        end=dates.max().date(),
        as_of=dates[10].date(),
        horizons=(1,),
        eval_start=dates[2].date(),
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
    )

    assert factor.seen_max_date <= dates[10]


def test_run_evaluation_requires_eval_start_for_short_default_warmup(monkeypatch):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])

    with pytest.raises(FactorEvaluationError, match="252-day warmup"):
        ev.run_evaluation(
            RecordingFactor(),
            engine=object(),
            start=dates.min().date(),
            end=dates.max().date(),
            horizons=(1,),
            min_median_dollar_volume=1,
            eligibility_window=1,
            trials_path=None,
        )


@dataclass(frozen=True)
class TimeVaryingFactor:
    name = "time_varying"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)


def test_pit_regression_triggers_lookahead_suspect():
    dates = pd.bdate_range("2025-01-02", periods=220)
    universe = pd.Index(range(1, 121), dtype="int64")
    live = pd.DataFrame(0.0, index=dates, columns=universe)

    diff = _pit_regression(TimeVaryingFactor(), live, object(), dates, universe)

    assert diff > 1e-6


def test_params_hash_excludes_note():
    config = {"factor_name": "x", "cost_bps": 10.0, "note": "a"}
    other = config | {"note": "b"}

    assert _params_hash(config) == _params_hash(other)
