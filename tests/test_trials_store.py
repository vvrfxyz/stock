from __future__ import annotations

import importlib.util
import os
from pathlib import Path

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

from research._trials_store import _git_meta, append_study, append_trial, load_trials
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
    duplicate = _result()
    second = append_trial(duplicate, path)

    assert first == second
    assert duplicate.trial_id == first
    assert duplicate.created_at is None
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


def test_trial_id_includes_engine_code_fingerprint(monkeypatch):
    import research.evaluate as ev

    result = _result()
    monkeypatch.setattr(ev, "_engine_code_fingerprint", lambda: "engine-a")
    first = result._trial_id_value()
    monkeypatch.setattr(ev, "_engine_code_fingerprint", lambda: "engine-b")
    second = result._trial_id_value()

    assert first != second


def test_write_tmp_name_is_process_unique_and_lock_created(tmp_path, monkeypatch):
    path = tmp_path / "trials.parquet"
    seen: list[Path] = []
    real_write = pq.write_table

    def record(table, dest, *args, **kwargs):
        seen.append(Path(dest))
        return real_write(table, dest, *args, **kwargs)

    monkeypatch.setattr(pq, "write_table", record)
    append_trial(_result(), path)

    assert seen == [tmp_path / f".trials.parquet.{os.getpid()}.tmp"]
    assert (tmp_path / "trials.parquet.lock").exists()


def test_append_trial_serializes_via_flock(tmp_path, monkeypatch):
    import fcntl

    import research._trials_store as store

    path = tmp_path / "trials.parquet"
    ops: list[int] = []
    real_flock = fcntl.flock

    def record(fd, op):
        ops.append(op)
        return real_flock(fd, op)

    monkeypatch.setattr(store.fcntl, "flock", record)
    append_trial(_result(), path)

    assert ops == [fcntl.LOCK_EX, fcntl.LOCK_UN]


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


def test_load_trials_latest_only_logs_summary_not_full_id_list(tmp_path, monkeypatch):
    import research._trials_store as store

    path = tmp_path / "trials.parquet"
    append_trial(_result(factor_name="a"), path)
    rows = load_trials(path)
    updated = rows.copy()
    updated["trial_id"] = "newer"
    updated["created_at"] = pd.Timestamp("2099-01-01", tz="UTC")
    pq.write_table(pa.Table.from_pandas(pd.concat([rows, updated], ignore_index=True), preserve_index=False), path)
    messages = []

    class _Logger:
        def warning(self, message, *args):
            messages.append(("warning", message.format(*args)))

        def debug(self, message, *args):
            messages.append(("debug", message.format(*args)))

    monkeypatch.setattr(store, "logger", _Logger())

    load_trials(path, latest_only=True)

    warnings = [message for level, message in messages if level == "warning"]
    assert any("latest_only collapsed" in message for message in warnings)
    assert not any("latest_only dropped trial_ids=[" in message for message in warnings)


# ---------------------------------------------------------------------------
# W0-P3：study 行（trial_kind='study'，部署判定入台账、不入 Bonferroni 分母）
# ---------------------------------------------------------------------------

def _study_kwargs(**overrides):
    from datetime import date

    base = dict(
        study="retail_reality",
        factor_name="composite_v1",
        verdict=True,
        criteria="40bps alpha t>=2 且子组合中位超额>0",
        params={"holdings": 30, "n_sims": 1000, "caliber": "delist_realized_v2"},
        eval_start=date(2016, 1, 4),
        eval_end=date(2026, 7, 2),
        report_path="research/output/retail_reality_x.md",
        criterion_values={"alpha_nw_t_40bps": 2.31, "sub_median_ann": 0.086},
    )
    base.update(overrides)
    return base


def test_append_study_writes_kind_and_verdict(tmp_path):
    path = tmp_path / "trials.parquet"
    trial_id = append_study(path=path, **_study_kwargs())

    df = load_trials(path)
    assert set(df["trial_id"].astype(str)) == {trial_id}
    assert set(df["trial_kind"].astype(str)) == {"study"}
    verdict_rows = df[df["metric"] == "study_verdict"]
    assert len(verdict_rows) == 1
    assert verdict_rows["value"].iloc[0] == 1.0
    crits = df[df["metric"] == "study_criterion"]
    assert set(crits["note"]) == {"alpha_nw_t_40bps", "sub_median_ann"}


def test_append_study_idempotent_same_window_same_params(tmp_path):
    path = tmp_path / "trials.parquet"
    first = append_study(path=path, **_study_kwargs())
    n = len(load_trials(path))
    second = append_study(path=path, **_study_kwargs())  # 同口径同 verdict 重跑 → 静默跳过
    assert first == second
    assert len(load_trials(path)) == n


def test_append_study_verdict_drift_appends_new_row(tmp_path):
    # 同代码同口径同窗口 verdict 漂移（= 数据变了）：不静默吞，追加新行、旧行保留（审核 #8/#10）
    path = tmp_path / "trials.parquet"
    first = append_study(path=path, **_study_kwargs(verdict=True))
    drifted = append_study(path=path, **_study_kwargs(verdict=False))
    assert first != drifted
    df = load_trials(path)
    verdicts = df[df["metric"] == "study_verdict"].set_index("trial_id")["value"]
    assert verdicts[first] == 1.0 and verdicts[drifted] == 0.0  # 新旧结局都留账


def test_append_study_window_enters_params_hash(tmp_path):
    # 窗口进 params_hash：同 params 不同窗口是两条账，latest_only 不互相折叠（审核 #3）
    from datetime import date

    path = tmp_path / "trials.parquet"
    first = append_study(path=path, **_study_kwargs())
    second = append_study(path=path, **_study_kwargs(eval_end=date(2024, 7, 2)))
    df = load_trials(path)
    assert df.set_index("trial_id").loc[[first, second], "params_hash"].nunique() == 2
    latest = load_trials(path, latest_only=True)
    assert {first, second} <= set(latest["trial_id"].astype(str))


def test_append_study_new_params_new_trial(tmp_path):
    path = tmp_path / "trials.parquet"
    first = append_study(path=path, **_study_kwargs())
    second = append_study(path=path, **_study_kwargs(params={"holdings": 20, "n_sims": 1000}))
    assert first != second
    assert load_trials(path)["trial_id"].nunique() == 2


def test_append_study_rejects_unknown_kind(tmp_path):
    with pytest.raises(ValueError, match="unknown study kind"):
        append_study(path=tmp_path / "t.parquet", **_study_kwargs(study="ad_hoc"))


def test_study_rows_coexist_with_evaluate_trials(tmp_path):
    path = tmp_path / "trials.parquet"
    eval_id = append_trial(_result(), path)
    study_id = append_study(path=path, **_study_kwargs())

    df = load_trials(path)
    kind = df["trial_kind"].fillna("evaluate").astype(str)
    assert set(df.loc[kind == "study", "trial_id"]) == {study_id}
    assert set(df.loc[kind != "study", "trial_id"]) == {eval_id}
    # 历史行（trial_kind NULL）按 evaluate 解释
    assert df.loc[df["trial_id"] == eval_id, "trial_kind"].isna().all() or (
        df.loc[df["trial_id"] == eval_id, "trial_kind"].astype(str) == "evaluate"
    ).all()


def test_study_rows_excluded_from_bonferroni_denominator(tmp_path):
    from research.trials import factor_report, overview, study_report

    path = tmp_path / "trials.parquet"
    append_trial(_result(factor_name="composite_v1"), path)
    append_study(path=path, **_study_kwargs())

    df = load_trials(path)
    ov = overview(df)
    assert int(ov.loc["composite_v1", "trials"]) == 1  # study 行不计入分母

    per_trial, _ = factor_report(df, "composite_v1")
    assert len(per_trial) == 1

    st = study_report(df, "composite_v1")
    assert len(st) == 1
    assert st["verdict"].iloc[0] == "PASS"


def test_load_trials_requalifies_legacy_low_coverage_pass(tmp_path):
    path = tmp_path / "trials.parquet"
    append_study(
        path=path,
        **_study_kwargs(
            params={"cost_mode": "measured", "q5_insufficient": True},
            verdict=True,
        ),
    )

    df = load_trials(path)
    verdict = df[df["metric"] == "study_verdict"].iloc[0]
    assert verdict["value"] == 0.0
    assert "q5 coverage <70%" in verdict["note"]


def test_report_survives_v1_null_kind_plus_study_rows(tmp_path):
    # 审核 #0 金测试：v1 老账（trial_kind 全 NULL）+ 只追加过 study 行 → parquet 读回
    # trial_kind 是 categories={'study'} 的 Categorical，naive fillna('evaluate') 会抛
    # TypeError。overview/factor_report/study_report 必须不抛且分母正确。
    from research._trials_store import _arrow_schema
    from research.trials import factor_report, overview, study_report

    path = tmp_path / "trials.parquet"
    old_row = {
        "trial_id": "legacy01", "schema_version": 1, "trial_kind": None,
        "created_at": pd.Timestamp("2026-07-01", tz="UTC"),
        "factor_name": "composite_v1", "factor_version": "v1",
        "eval_start": pd.Timestamp("2016-01-04").date(),
        "eval_end": pd.Timestamp("2026-07-02").date(),
        "horizon": 5, "metric": "ic_nw_t", "value": 3.1, "params_hash": "abc",
    }
    pq.write_table(pa.Table.from_pylist([old_row], schema=_arrow_schema()), path)
    append_study(path=path, **_study_kwargs())

    df = load_trials(path)
    ov = overview(df)                                     # 不得抛 TypeError
    assert int(ov.loc["composite_v1", "trials"]) == 1     # 老行计入分母，study 行不计
    per_trial, _ = factor_report(df, "composite_v1")
    assert set(per_trial.index) == {"legacy01"[:12]}
    st = study_report(df, "composite_v1")
    assert len(st) == 1
