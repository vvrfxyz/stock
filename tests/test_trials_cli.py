"""research/trials.py 查账 CLI 的单元测试（2026-07-07）。

不碰 parquet/pyarrow：monkeypatch load_trials 喂合成长表，测纯聚合与 CLI 出口。
"""
from __future__ import annotations

import pandas as pd
import pytest

import research.trials as trials_cli


def _synthetic_trials() -> pd.DataFrame:
    rows = []
    for trial, (factor, ph, created, t_by_h) in enumerate({
        "t1": ("size", "aaa", "2026-07-01", {5: 1.2, 21: 2.8}),
        "t2": ("size", "bbb", "2026-07-05", {5: -3.4, 21: 0.9}),
        "t3": ("low_vol", "ccc", "2026-07-06", {21: 1.1}),
    }.values()):
        tid = f"trial{trial}" * 4
        for h, t in t_by_h.items():
            rows.append({
                "trial_id": tid, "factor_name": factor, "factor_version": f"v{trial}",
                "params_hash": ph * 8, "created_at": pd.Timestamp(created, tz="UTC"),
                "eval_start": pd.Timestamp("2025-01-02").date(),
                "eval_end": pd.Timestamp("2026-06-30").date(),
                "horizon": h, "metric": "ic_nw_t", "value": t, "is_noisy": abs(t) < 2,
                "note": None,
            })
        rows.append({**rows[-1], "metric": "ic_mean", "value": 0.01})
    return pd.DataFrame(rows)


def test_overview_counts_trials_and_params():
    out = trials_cli.overview(_synthetic_trials())
    assert out.loc["size", "trials"] == 2
    assert out.loc["size", "params"] == 2
    assert out.loc["low_vol", "trials"] == 1
    assert list(out.index) == ["size", "low_vol"]  # 按 trial 数降序


def test_factor_report_best_t_and_denominator():
    per_trial, by_horizon = trials_cli.factor_report(_synthetic_trials(), "size")
    assert len(per_trial) == 2                      # Bonferroni 分母
    assert per_trial["best_abs_nw_t"].max() == pytest.approx(3.4)
    assert by_horizon.loc[5, "value"] == pytest.approx(-3.4)   # 绝对值最大保留符号
    assert by_horizon.loc[21, "value"] == pytest.approx(2.8)


def test_factor_report_survives_all_nan_horizon():
    # 被跳过的 horizon 落 ic_nw_t=NaN 行；全 NaN 组曾让 groupby.idxmax 抛
    # ValueError（pandas 3.0.3，审查确认）——须整组静默消失而非崩溃。
    df = _synthetic_trials()
    df.loc[df["horizon"] == 5, "value"] = float("nan")
    per_trial, by_horizon = trials_cli.factor_report(df, "size")
    assert len(per_trial) == 2
    assert 5 not in by_horizon.index
    assert by_horizon.loc[21, "value"] == pytest.approx(2.8)


def test_main_report_factor(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: _synthetic_trials())
    assert trials_cli.main(["report", "--factor", "size"]) == 0
    out = capsys.readouterr().out
    assert "Bonferroni 分母）: 2" in out
    assert "逐 horizon 最佳" in out


def test_main_unknown_factor_lists_known(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: _synthetic_trials())
    assert trials_cli.main(["report", "--factor", "nope"]) == 1
    assert "low_vol, size" in capsys.readouterr().out


def test_main_empty_store(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: pd.DataFrame())
    assert trials_cli.main(["report"]) == 1
    assert "无 trial 记录" in capsys.readouterr().out
