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
    # bonf_z 随分母动态：m=1 -> 1.96，m=2 -> 2.24
    assert out.loc["low_vol", "bonf_z"] == pytest.approx(1.96)
    assert out.loc["size", "bonf_z"] == pytest.approx(2.24)


def test_bonferroni_z_dynamic_threshold():
    # 双侧：m=1 就是普通 1.96；分母越大阈值越高
    assert trials_cli.bonferroni_z(1) == pytest.approx(1.959964, abs=1e-5)
    assert trials_cli.bonferroni_z(2) == pytest.approx(2.241403, abs=1e-5)
    assert trials_cli.bonferroni_z(20) > trials_cli.bonferroni_z(2)
    with pytest.raises(ValueError):
        trials_cli.bonferroni_z(0)


def test_unreachable_git_shas_real_repo():
    import subprocess

    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=trials_cli.DEFAULT_TRIALS_PATH.parents[2],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    df = pd.DataFrame({"code_git_sha": [head, "f" * 40, None]})
    missing = trials_cli.unreachable_git_shas(df)
    assert head not in missing          # 本机可达的 sha 不报
    assert "f" * 40 in missing          # 伪 sha 报为不可达
    # 无 code_git_sha 列（如老账/合成数据）静默返回空
    assert trials_cli.unreachable_git_shas(pd.DataFrame({"x": [1]})) == []


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
    assert "Bonferroni 分母，study 行不计入）: 2" in out
    assert "双侧 Bonferroni z 阈值（alpha=0.05, m=2）: 2.24" in out
    assert "逐 horizon 最佳" in out


def test_main_warns_on_unreachable_shas(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: _synthetic_trials())
    monkeypatch.setattr(trials_cli, "unreachable_git_shas", lambda trials: ["e" * 40])
    assert trials_cli.main(["report"]) == 0
    out = capsys.readouterr().out
    assert "可能存在异地台账" in out
    assert "e" * 12 in out


def test_main_unknown_factor_lists_known(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: _synthetic_trials())
    assert trials_cli.main(["report", "--factor", "nope"]) == 1
    assert "low_vol, size" in capsys.readouterr().out


def test_main_empty_store(monkeypatch, capsys):
    monkeypatch.setattr(trials_cli, "load_trials", lambda path: pd.DataFrame())
    assert trials_cli.main(["report"]) == 1
    assert "无 trial 记录" in capsys.readouterr().out
