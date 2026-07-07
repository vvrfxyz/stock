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
    _markdown_table,
    _params_hash,
    _pit_regression,
    _quantile_weights_for_day,
    _rank_ic_series,
    _result_summary,
    default_nw_lag,
    evaluate_factor,
    parse_args,
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


def test_ic_uses_eligible_cross_section_only():
    rng = np.random.default_rng(3)
    dates, universe, _ = _panel(n_dates=120, n_names=240)
    fwd = pd.DataFrame(rng.normal(size=(len(dates), len(universe))), index=dates, columns=universe)
    # eligible 名字上是纯噪声，ineligible 名字上是完美前视信号：
    # IC 若不做 eligibility 过滤，会被不可交易的一半横截面拉到显著。
    factor = pd.DataFrame(rng.normal(size=(len(dates), len(universe))), index=dates, columns=universe)
    factor.iloc[:, 120:] = fwd.iloc[:, 120:]
    eligible = pd.DataFrame(True, index=dates, columns=universe)
    eligible.iloc[:, 120:] = False

    result = evaluate_factor(factor, {1: fwd}, eligibility=eligible, horizons=(1,), min_coverage=50)

    assert abs(result.ic_table.loc[1, "mean_ic"]) < 0.1
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


def test_quantile_sharpe_subtracts_risk_free_for_net_exposure_only():
    dates, universe, factor = _panel(n_dates=90, n_names=120)
    fwd = factor.rank(axis=1, pct=True) / 1000
    adj_close = _prices_from_returns(fwd)
    eligible = pd.DataFrame(True, index=dates, columns=universe)
    rf = pd.Series(0.0001, index=dates, name="DTB3")

    no_rf = evaluate_factor(
        factor,
        {1: _forward_return(adj_close, 1)},
        eligibility=eligible,
        horizons=(1,),
        adj_close=adj_close,
        cost_bps=0,
        min_coverage=50,
    )
    with_rf = evaluate_factor(
        factor,
        {1: _forward_return(adj_close, 1)},
        eligibility=eligible,
        horizons=(1,),
        adj_close=adj_close,
        cost_bps=0,
        min_coverage=50,
        risk_free_returns=rf,
    )

    assert with_rf.quantile_metrics.loc[(1, "q5"), "sharpe_net"] < no_rf.quantile_metrics.loc[(1, "q5"), "sharpe_net"]
    assert with_rf.quantile_metrics.loc[(1, "ls_q5_q1"), "sharpe_net"] == pytest.approx(
        no_rf.quantile_metrics.loc[(1, "ls_q5_q1"), "sharpe_net"]
    )


def test_quantile_sharpe_rejects_missing_risk_free_dates():
    dates, universe, factor = _panel(n_dates=90, n_names=120)
    fwd = factor.rank(axis=1, pct=True) / 1000
    adj_close = _prices_from_returns(fwd)
    eligible = pd.DataFrame(True, index=dates, columns=universe)
    rf = pd.Series(0.0001, index=dates[:-1], name="DTB3")

    with pytest.raises(FactorEvaluationError, match="risk_free_returns missing"):
        evaluate_factor(
            factor,
            {1: _forward_return(adj_close, 1)},
            eligibility=eligible,
            horizons=(1,),
            adj_close=adj_close,
            cost_bps=0,
            min_coverage=50,
            risk_free_returns=rf,
        )


def test_coverage_diagnostic():
    _, _, factor = _panel(n_dates=80, n_names=120)
    factor.iloc[:, ::2] = np.nan
    eligible = pd.DataFrame(True, index=factor.index, columns=factor.columns)

    result = evaluate_factor(factor, {1: factor.copy()}, eligibility=eligible, horizons=(1,), min_coverage=10)

    assert result.coverage["factor_coverage"].mean() == pytest.approx(0.5, abs=0.05)
    assert result.coverage["factor_count"].median() == 60


def test_result_summary_includes_count_coverage_metrics():
    _, _, factor = _panel(n_dates=80, n_names=120)
    factor.iloc[:10, :] = np.nan
    factor.iloc[:, 60:] = np.nan
    eligible = pd.DataFrame(True, index=factor.index, columns=factor.columns)

    result = evaluate_factor(factor, {1: factor.copy()}, eligibility=eligible, horizons=(1,), min_coverage=50)
    summary = _result_summary(result)

    assert summary.loc[1, "factor_count_median"] == 60
    assert summary.loc[1, "factor_count_max"] == 60
    assert summary.loc[1, "days_below_min_coverage"] == 10
    assert "factor_count_median" in _markdown_table(summary)


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
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
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
        risk_free_series=None,
    )

    assert factor.seen_max_date <= dates[10]


def test_run_evaluation_loads_risk_free_only_for_quantile_backtest_dates(monkeypatch):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=30, n_names=120)
    end = dates[19].date()
    eval_start = dates[5].date()
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    seen_indexes = []

    def fake_load_risk_free(engine, index, *, series_id):
        loaded_index = pd.DatetimeIndex(index)
        seen_indexes.append(loaded_index)
        if loaded_index.max().date() > end:
            raise AssertionError("risk-free loader saw unused forward-return buffer dates")
        return pd.Series(0.0, index=loaded_index, name=series_id)

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
    monkeypatch.setattr(ev, "load_risk_free_daily_returns", fake_load_risk_free)

    ev.run_evaluation(
        RecordingFactor(),
        engine=object(),
        start=dates.min().date(),
        end=end,
        horizons=(5,),
        eval_start=eval_start,
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
    )

    assert len(seen_indexes) == 1
    assert list(seen_indexes[0]) == list(dates[5:20])


def test_uncovered_events_window_covers_forward_return_buffer(monkeypatch):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=30, n_names=120)
    end = dates[19].date()
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    seen = {}

    def fake_uncovered(engine, *, start, end, require_straddle=True):
        seen["end"] = end
        seen["require_straddle"] = require_straddle
        return []

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", fake_uncovered)
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))

    ev.run_evaluation(
        RecordingFactor(),
        engine=object(),
        start=dates.min().date(),
        end=end,
        horizons=(5,),
        eval_start=dates[5].date(),
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
        risk_free_series=None,
    )

    # 剔除窗口须覆盖前向收益实际用到的缓冲日期，而非止步于 end
    assert seen["end"] == ev._buffered_end(end, 5)
    assert seen["end"] > end
    # gate 行为与 params_hash 标签同源：run_evaluation 显式传 require_straddle（straddle_v2）
    assert seen["require_straddle"] is True


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
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))

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
            risk_free_series=None,
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

    diff, presence = _pit_regression(TimeVaryingFactor(), live, object(), dates, universe)

    assert diff > 1e-6
    assert presence == 0


@dataclass(frozen=True)
class PresenceLeakFactor:
    name = "presence_leak"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        # as-of 重放时最后一天不可见，而 live 有值 → presence 型前视
        values = pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)
        if ctx.as_of is not None:
            values.loc[values.index >= pd.Timestamp(ctx.as_of)] = np.nan
        return values


def test_pit_regression_counts_presence_violations():
    dates = pd.bdate_range("2025-01-02", periods=220)
    universe = pd.Index(range(1, 121), dtype="int64")
    live = pd.DataFrame(1.0, index=dates, columns=universe)

    diff, presence = _pit_regression(PresenceLeakFactor(), live, object(), dates, universe)

    assert pd.isna(diff)
    assert presence > 0


def test_params_hash_excludes_note():
    config = {"factor_name": "x", "cost_bps": 10.0, "note": "a"}
    other = config | {"note": "b"}

    assert _params_hash(config) == _params_hash(other)


def test_params_hash_distinguishes_uncovered_gate_versions():
    # gate 口径（straddle_v2 / legacy_v1）进 params_hash：新旧口径 trial 不互相顶替
    from research.data import uncovered_gate_version

    base = {"factor_name": "x", "cost_bps": 10.0}
    new = base | {"uncovered_gate": uncovered_gate_version()}
    legacy = base | {"uncovered_gate": uncovered_gate_version(require_straddle=False)}

    assert new["uncovered_gate"] == "straddle_v2"
    assert legacy["uncovered_gate"] == "legacy_v1"
    assert len({_params_hash(base), _params_hash(new), _params_hash(legacy)}) == 3


def test_parse_args_rejects_start_before_trust_floor():
    # 2026-07 归档回填后 trust floor = 2003-01-01（价格与事件的共同硬地板）
    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--factors", "size", "--start", "2002-12-31"])

    assert exc_info.value.code == 2


def test_run_evaluation_strict_persists_before_raise(monkeypatch, tmp_path):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    appended = []

    def fake_append(result, path):
        appended.append((result.factor_name, path, result.diagnostics["lookahead_suspect"]))
        return "trial"

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
    monkeypatch.setattr(ev, "_pit_regression", lambda *args, **kwargs: (1.0, 0))
    monkeypatch.setattr("research._trials_store.append_trial", fake_append)

    with pytest.raises(FactorEvaluationError, match="failed PIT regression"):
        ev.run_evaluation(
            RecordingFactor(),
            engine=object(),
            start=dates.min().date(),
            end=dates.max().date(),
            horizons=(1,),
            eval_start=dates[2].date(),
            min_median_dollar_volume=1,
            eligibility_window=1,
            trials_path=tmp_path / "trials.parquet",
            strict=True,
            risk_free_series=None,
        )

    assert appended == [("recording", tmp_path / "trials.parquet", True)]


def test_run_evaluation_strict_fails_on_presence_violations(monkeypatch, tmp_path):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
    # 值差为 NaN、仅 presence 违规：strict 仍须失败
    monkeypatch.setattr(ev, "_pit_regression", lambda *args, **kwargs: (np.nan, 3))

    with pytest.raises(FactorEvaluationError, match="failed PIT regression"):
        ev.run_evaluation(
            RecordingFactor(),
            engine=object(),
            start=dates.min().date(),
            end=dates.max().date(),
            horizons=(1,),
            eval_start=dates[2].date(),
            min_median_dollar_volume=1,
            eligibility_window=1,
            trials_path=None,
            strict=True,
            risk_free_series=None,
        )


def test_run_evaluation_reports_presence_violations_in_diagnostics(monkeypatch):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
    monkeypatch.setattr(ev, "_pit_regression", lambda *args, **kwargs: (np.nan, 7))

    result = ev.run_evaluation(
        RecordingFactor(),
        engine=object(),
        start=dates.min().date(),
        end=dates.max().date(),
        horizons=(1,),
        eval_start=dates[2].date(),
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
        risk_free_series=None,
    )

    assert result.diagnostics["pit_presence_violations"] == 7
    assert result.diagnostics["lookahead_suspect"] is True
    rows = result.to_trial_rows()
    assert any(row["metric"] == "pit_presence_violations" and row["value"] == 7.0 for row in rows)


def test_run_evaluation_raises_when_trial_append_fails(monkeypatch, tmp_path):
    import research.evaluate as ev

    dates, universe, _ = _panel(n_dates=20, n_names=120)
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }

    def fake_append(result, path):
        raise OSError("disk full")

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *args, **kwargs: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *args, **kwargs: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *args, **kwargs: pd.Series(dtype="float64"))
    monkeypatch.setattr(ev, "_pit_regression", lambda *args, **kwargs: (1.0, 0))
    monkeypatch.setattr("research._trials_store.append_trial", fake_append)

    with pytest.raises(OSError, match="disk full"):
        ev.run_evaluation(
            RecordingFactor(),
            engine=object(),
            start=dates.min().date(),
            end=dates.max().date(),
            horizons=(1,),
            eval_start=dates[2].date(),
            min_median_dollar_volume=1,
            eligibility_window=1,
            trials_path=tmp_path / "trials.parquet",
            risk_free_series=None,
        )
