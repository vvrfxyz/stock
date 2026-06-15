from __future__ import annotations

import importlib.util

import pandas as pd
import pytest

HAS_PYARROW = importlib.util.find_spec("pyarrow") is not None
pytestmark = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow is not installed")

if HAS_PYARROW:
    import pyarrow as pa
    import pyarrow.parquet as pq
else:
    pa = None
    pq = None

from research._trials_store import _git_meta, append_trial, load_trials
from research.evaluate import EvaluationResult, _params_hash


def _result(note: str | None = None, factor_name: str = "demo") -> EvaluationResult:
    dates = pd.bdate_range("2025-01-02", periods=3)
    ic_table = pd.DataFrame(
        {"mean_ic": [0.1], "std_ic": [0.2], "nw_t": [1.5], "nw_lag": [1], "n_obs": [3], "is_noisy": [True]},
        index=pd.Index([1], name="horizon"),
    )
    ic_decay = pd.DataFrame({"horizon": [1, 1], "lag": [0, 1], "ic": [0.1, 0.05]})
    q_index = pd.MultiIndex.from_tuples([(1, "q1"), (1, "q5"), (1, "ls_q5_q1")], names=["horizon", "quantile_label"])
    quantile_metrics = pd.DataFrame(
        {
            "ann_return": [0.01, 0.02, 0.03],
            "ann_vol": [0.1, 0.1, 0.2],
            "sharpe_gross": [0.1, 0.2, 0.3],
            "sharpe_net": [0.0, 0.1, 0.2],
            "ann_turnover": [1.0, 1.0, 2.0],
            "max_drawdown": [-0.1, -0.1, -0.2],
        },
        index=q_index,
    )
    coverage = pd.DataFrame(
        {
            "n_universe": [100, 100, 100],
            "factor_coverage": [0.9, 0.95, 1.0],
            "fwd_ret_coverage_given_factor": [0.98, 0.99, 1.0],
            "pit_violations": [0, 0, 0],
            "n_active": [100, 100, 100],
            "n_delisted": [0, 0, 0],
        },
        index=dates,
    )
    config = {"start": dates.min().date(), "end": dates.max().date(), "cost_bps": 10.0, "note": note}
    return EvaluationResult(
        factor_name=factor_name,
        factor_version="v1",
        code_git_sha="abc",
        code_git_dirty=False,
        horizons=(1,),
        eval_dates=dates,
        as_of=dates.max(),
        cost_bps=10.0,
        n_quantiles=5,
        universe_hash="universe",
        universe_size_mean=100.0,
        universe_size_min=100,
        params_hash=_params_hash(config),
        config=config,
        ic_table=ic_table,
        ic_decay=ic_decay,
        quantile_metrics=quantile_metrics,
        coverage=coverage,
        diagnostics={
            "pit_regression_max_abs_diff": 0.0,
            "factor_freshness_gap_days": 0,
            "unexpected_coverage_jump_days": 0,
            "skipped_horizons": (),
        },
    )


def test_append_creates_parquet_when_path_does_not_exist(tmp_path):
    path = tmp_path / "trials.parquet"

    trial_id = append_trial(_result(), path)

    df = load_trials(path)
    assert path.exists()
    assert set(df["trial_id"]) == {trial_id}
    assert "ic_mean" in set(df["metric"].astype(str))


def test_append_twice_accumulates_rows_idempotently(tmp_path):
    path = tmp_path / "trials.parquet"
    result = _result()

    first = append_trial(result, path)
    n_rows = len(load_trials(path))
    second = append_trial(_result(), path)

    assert first == second
    assert len(load_trials(path)) == n_rows


def test_append_atomic_on_error(tmp_path, monkeypatch):
    path = tmp_path / "trials.parquet"
    append_trial(_result(), path)
    before = path.read_bytes()

    def boom(*args, **kwargs):
        raise RuntimeError("write failed")

    monkeypatch.setattr(pq, "write_table", boom)
    with pytest.raises(RuntimeError):
        append_trial(_result(factor_name="other"), path)

    assert path.read_bytes() == before


def test_schema_version_reindex_old_rows_nan(tmp_path):
    path = tmp_path / "old.parquet"
    table = pa.Table.from_pylist(
        [{"trial_id": "old", "schema_version": 1, "metric": "ic_mean", "value": 0.1}],
        schema=pa.schema(
            [
                pa.field("trial_id", pa.string()),
                pa.field("schema_version", pa.int16()),
                pa.field("metric", pa.dictionary(pa.int32(), pa.string())),
                pa.field("value", pa.float64()),
            ]
        ),
    )
    pq.write_table(table, path)

    df = load_trials(path)

    assert "note" in df.columns
    assert pd.isna(df.loc[0, "note"])


def test_git_meta_returns_none_outside_git_repo(tmp_path, monkeypatch):
    import research._trials_store as store

    store._git_meta.cache_clear()
    monkeypatch.setattr(store, "_repo_root", lambda: tmp_path)

    assert _git_meta() == (None, False)
    store._git_meta.cache_clear()


def test_trial_id_is_content_addressed(tmp_path):
    path = tmp_path / "trials.parquet"

    first = append_trial(_result(), path)
    second = _result().to_trial_rows()[0]["trial_id"]

    assert first == second


def test_params_hash_excludes_note(tmp_path):
    first = _result(note="a").to_trial_rows()[0]
    second = _result(note="b").to_trial_rows()[0]

    assert first["params_hash"] == second["params_hash"]
    assert first["trial_id"] == second["trial_id"]


def test_load_trials_latest_only_collapses_older_rows(tmp_path):
    path = tmp_path / "trials.parquet"
    append_trial(_result(factor_name="a"), path)
    rows = load_trials(path)
    updated = rows.copy()
    updated["trial_id"] = "newer"
    updated["created_at"] = pd.Timestamp("2099-01-01", tz="UTC")  # 2099 strictly > append_trial's utcnow()
    pq.write_table(pa.Table.from_pandas(pd.concat([rows, updated], ignore_index=True), preserve_index=False), path)

    latest = load_trials(path, latest_only=True)

    assert set(latest["trial_id"]) == {"newer"}
