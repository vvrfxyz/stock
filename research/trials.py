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
from pathlib import Path

import pandas as pd

from research._trials_store import load_trials

# 与 evaluate.py 的写侧同锚（__file__ 绝对路径）：cwd 相对路径会在错目录下
# 把有账报成"无 trial 记录"——假空账比报错更危险（审查确认，2026-07-07）。
DEFAULT_TRIALS_PATH = Path(__file__).resolve().parent / "output" / "trials.parquet"


def overview(trials: pd.DataFrame) -> pd.DataFrame:
    """全因子概览：每因子一行——trial 数（多重检验分母）、口径/版本数、窗口、最近一次。"""
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
    return out.sort_values("trials", ascending=False)


def factor_report(trials: pd.DataFrame, factor: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """单因子详单：(逐 trial 表, 逐 horizon 最佳 |ic_nw_t| 表)。

    trials 行是 (trial_id, horizon, metric) 长表；逐 trial 表按 trial_id 折叠，
    ic_nw_t 取该 trial 各 horizon 的最大绝对值。
    """
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
        if per_trial.empty:
            known = ", ".join(sorted(trials["factor_name"].dropna().unique()))
            print(f"因子 {args.factor!r} 无 trial；已有: {known}")
            return 1
        n_trials, n_params = len(per_trial), per_trial["params_hash"].nunique()
        print(f"== {args.factor} 查账 ==")
        print(f"trial 数（多重检验/Bonferroni 分母）: {n_trials}；参数口径 {n_params} 种\n")
        print("-- 逐 trial（时间序）--")
        print(per_trial.to_string())
        if not by_horizon.empty:
            print("\n-- 逐 horizon 最佳 |ic_nw_t|（* 记得除以上面的分母再看显著性）--")
            print(by_horizon.to_string())
    else:
        print(f"== trials 概览（{args.trials_path}）==")
        print(overview(trials).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
