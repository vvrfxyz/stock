"""trials.parquet 查账 CLI——load_trials 的第一个仓内正式消费者（2026-07-07）。

开新研究前查账（CLAUDE.md：研究总账必查）的机器侧配套：Bonferroni 分母
从"人肉数 markdown"变一条命令。只读，绝不写任何东西。

    python -m research.trials report                 # 全因子概览（trial 数/口径数/窗口）
    python -m research.trials report --factor size   # 单因子详单（多重检验分母在此）

与 docs/research_ledger.md 的关系：总账记人写的裁决叙事，这里给裁决要引用的
机器数字（评估次数、最佳 t、口径分布）。不自动写总账——机器起草、人裁决。
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

import pandas as pd

from research._trials_store import load_trials

# 与 evaluate.py 的写侧同锚（__file__ 绝对路径）：cwd 相对路径会在错目录下
# 把有账报成"无 trial 记录"——假空账比报错更危险（审查确认，2026-07-07）。
DEFAULT_TRIALS_PATH = Path(__file__).resolve().parent / "output" / "trials.parquet"


def bonferroni_z(m: int, alpha: float = 0.05) -> float:
    """双侧 Bonferroni z 阈值：m 个 trial 分摊 alpha 后的单尾分位。

    以前"除以分母再看 2.9"是钉在人脑里的经验数；这里改成由当前分母动态
    推导——z = Phi^{-1}(1 - alpha/2/m)。scipy 缺席时退回标准库 NormalDist
    （同一分布，无精度损失）。
    """
    if m < 1:
        raise ValueError(f"Bonferroni 分母必须 >= 1，得到 {m}")
    p = 1 - alpha / 2 / m
    try:
        from scipy.stats import norm

        return float(norm.ppf(p))
    except ImportError:
        from statistics import NormalDist

        return float(NormalDist().inv_cdf(p))


def unreachable_git_shas(trials: pd.DataFrame) -> list[str]:
    """台账里本机 git 不可达的 code_git_sha（异地台账线索）。

    trials.parquet 可能在多台机器（本地 Mac / 253）各自积账；如果某 trial 的
    code_git_sha 在当前仓库 `git cat-file -e` 查不到，多半是别处跑的评估——
    Bonferroni 分母可能被低估。git 本身不可用/不在仓库时静默返回空（不阻塞、
    不误报——此时"查不到"不构成异地证据）。
    """
    if "code_git_sha" not in trials.columns:
        return []
    shas = sorted({s for s in trials["code_git_sha"].dropna().astype(str) if s})
    if not shas:
        return []
    root = Path(__file__).resolve().parents[1]
    try:
        probe = subprocess.run(
            ["git", "rev-parse", "--git-dir"], cwd=root, timeout=2, check=False, capture_output=True
        )
        if probe.returncode != 0:
            return []
        missing = []
        for sha in shas:
            check = subprocess.run(
                ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
                cwd=root,
                timeout=2,
                check=False,
                capture_output=True,
            )
            if check.returncode != 0:
                missing.append(sha)
        return missing
    except Exception:
        return []


def _split_kinds(trials: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(假设检验型 trial, study 行)。历史行 trial_kind 为 NULL，按 'evaluate' 解释。

    分母口径（预注册，roadmap §1 P3）：study 行不计入 Bonferroni 分母——
    分母只数假设检验型 trial；study 判据用各自预注册阈值。
    """
    if "trial_kind" not in trials.columns:
        return trials, trials.iloc[0:0]
    # parquet 的 dictionary<string> 读回是 Categorical：老账 NULL 行 + 只追加过
    # study 行时 categories={'study'}，直接 fillna('evaluate') 会因新类别抛
    # TypeError（对抗审核实证复现，2026-07-08）。先脱 Categorical 再补默认。
    kind = trials["trial_kind"].astype(object).fillna("evaluate").astype(str)
    return trials[kind != "study"], trials[kind == "study"]


def overview(trials: pd.DataFrame) -> pd.DataFrame:
    """全因子概览：每因子一行——trial 数（多重检验分母）、口径/版本数、窗口、最近一次。

    只统计假设检验型 trial；study 行（部署判定）另行列示，不进此表分母。
    """
    trials, _ = _split_kinds(trials)
    created = pd.to_datetime(trials["created_at"], utc=True, errors="coerce")
    grouped = trials.assign(_created=created).groupby("factor_name")
    out = pd.DataFrame(
        {
            "trials": grouped["trial_id"].nunique(),
            "params": grouped["params_hash"].nunique(),
            "versions": grouped["factor_version"].nunique(),
            "eval_min": grouped["eval_start"].min(),
            "eval_max": grouped["eval_end"].max(),
            "last_run": grouped["_created"].max().dt.date,
        }
    )
    out["bonf_z"] = out["trials"].map(lambda m: round(bonferroni_z(int(m)), 2))
    return out.sort_values("trials", ascending=False)


def study_report(trials: pd.DataFrame, factor: str | None = None) -> pd.DataFrame:
    """study 行详单（部署判定痕迹）：每 study 一行——结局、判据摘要、口径、窗口。"""
    _, studies = _split_kinds(trials)
    if studies.empty:
        return pd.DataFrame()
    rows = studies[studies["metric"] == "study_verdict"].copy()
    if factor:
        rows = rows[rows["factor_name"] == factor]
    if rows.empty:
        return pd.DataFrame()
    rows["_created"] = pd.to_datetime(rows["created_at"], utc=True, errors="coerce")
    out = pd.DataFrame(
        {
            "created": rows["_created"].dt.date,
            "study": rows["factor_version"],
            "factor": rows["factor_name"],
            "verdict": rows["value"].map({1.0: "PASS", 0.0: "FAIL"}),
            "eval_start": rows["eval_start"],
            "eval_end": rows["eval_end"],
            "params_hash": rows["params_hash"].astype(str).str[:12],
            "criteria": rows["note"].astype(str).str[:60],
        }
    ).sort_values("created")
    return out.reset_index(drop=True)


def factor_report(trials: pd.DataFrame, factor: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """单因子详单：(逐 trial 表, 逐 horizon 最佳 |ic_nw_t| 表)。

    trials 行是 (trial_id, horizon, metric) 长表；逐 trial 表按 trial_id 折叠，
    ic_nw_t 取该 trial 各 horizon 的最大绝对值。study 行不进此表（见 study_report）。
    """
    trials, _ = _split_kinds(trials)
    rows = trials[trials["factor_name"] == factor].copy()
    if rows.empty:
        return pd.DataFrame(), pd.DataFrame()
    rows["_created"] = pd.to_datetime(rows["created_at"], utc=True, errors="coerce")
    nw = rows[rows["metric"] == "ic_nw_t"].copy()
    nw["_abs_t"] = nw["value"].abs()

    per_trial = rows.groupby("trial_id").agg(
        created=("_created", "max"),
        eval_start=("eval_start", "first"),
        eval_end=("eval_end", "first"),
        params_hash=("params_hash", "first"),
        note=("note", "first"),
    )
    best_t = nw.groupby("trial_id")["_abs_t"].max()
    per_trial["best_abs_nw_t"] = best_t
    per_trial = per_trial.sort_values("created")
    per_trial.index = [str(t)[:12] for t in per_trial.index]
    per_trial["created"] = per_trial["created"].dt.date
    per_trial["params_hash"] = per_trial["params_hash"].astype(str).str[:12]

    by_horizon = pd.DataFrame()
    # 被跳过的 horizon 会落 ic_nw_t=NaN 的 trial 行；全 NaN 组会让 groupby.idxmax
    # 抛 ValueError（pandas 3.0.3 实测），先剔除再取最佳。
    valid = nw.dropna(subset=["_abs_t"])
    if not valid.empty:
        idx = valid.groupby("horizon")["_abs_t"].idxmax()
        by_horizon = valid.loc[idx, ["horizon", "value", "is_noisy", "eval_start", "eval_end", "trial_id"]].copy()
        by_horizon["trial_id"] = by_horizon["trial_id"].astype(str).str[:12]
        by_horizon = by_horizon.set_index("horizon").sort_index()
    return per_trial, by_horizon


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="trials.parquet 查账（只读）")
    sub = parser.add_subparsers(dest="command", required=True)
    report = sub.add_parser("report", help="查账报告")
    report.add_argument("--factor", help="单因子详单；缺省为全因子概览")
    report.add_argument("--trials-path", type=Path, default=DEFAULT_TRIALS_PATH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    trials = load_trials(args.trials_path)
    if trials.empty:
        print(f"无 trial 记录: {args.trials_path}")
        return 1
    if args.factor:
        per_trial, by_horizon = factor_report(trials, args.factor)
        studies = study_report(trials, args.factor)
        if per_trial.empty and studies.empty:
            known = ", ".join(sorted(trials["factor_name"].dropna().unique()))
            print(f"因子 {args.factor!r} 无 trial；已有: {known}")
            return 1
        if not per_trial.empty:
            n_trials, n_params = len(per_trial), per_trial["params_hash"].nunique()
            z_thresh = bonferroni_z(n_trials)
            print(f"== {args.factor} 查账 ==")
            print(f"trial 数（多重检验/Bonferroni 分母，study 行不计入）: {n_trials}；参数口径 {n_params} 种")
            print(f"双侧 Bonferroni z 阈值（alpha=0.05, m={n_trials}）: {z_thresh:.2f}\n")
            print("-- 逐 trial（时间序）--")
            print(per_trial.to_string())
            if not by_horizon.empty:
                print(f"\n-- 逐 horizon 最佳 |ic_nw_t|（* |t| 须过上面的阈值 {z_thresh:.2f} 才算显著）--")
                print(by_horizon.to_string())
        if not studies.empty:
            print("\n-- study 行（部署判定痕迹，不入 Bonferroni 分母；判据用各自预注册阈值）--")
            print(studies.to_string())
    else:
        print(f"== trials 概览（{args.trials_path}）==")
        print(overview(trials).to_string())
        print("\n* bonf_z = 该因子分母下的双侧 Bonferroni z 阈值（alpha=0.05）")
        studies = study_report(trials)
        if not studies.empty:
            print(f"\n-- study 行（{len(studies)} 条部署判定，不入分母）--")
            print(studies.to_string())
    missing_shas = unreachable_git_shas(trials)
    if missing_shas:
        preview = ", ".join(s[:12] for s in missing_shas[:5])
        print(
            f"\n⚠ 可能存在异地台账：{len(missing_shas)} 个 code_git_sha 在本机 git 不可达"
            f"（{preview}{'…' if len(missing_shas) > 5 else ''}）——Bonferroni 分母可能被低估"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
