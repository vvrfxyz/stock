"""复合打分实验场（wave-6 起源；设计经对抗审计修订，family=composite_v1）。

【生产口径 vs 本脚本（2026-07-08，路线图 W0-P0）】
定案骨架 = low_vol + high_52w 残差 + size，已抽为**注册 builtin 因子
`research/factors/builtins/composite_v1.py`**——那是生产口径（成分写死），
`evaluate --factors composite_v1` 与 `retail_reality --factor composite_v1` 均消费它。
定案出处：`docs/research_ledger.md` 因子裁决表 composite_v1 行（2026-07-07 size 关卡
重审 PASS，594569c）。**本脚本保留为实验场**：可用 --components 试不同成分集、
--include-eod 做诊断对照；复合构造复用注册因子的共享函数（`residualize_high_52w`/
`combine_ranks`）以保与生产口径位级一致，默认 COMPONENTS 已订正为定案骨架。

【预注册成功判据——先于任何数字写死】
主判据（相对主干，不是绝对显著性）：复合分须在同窗/同 universe/同口径下
**同时**优于单独 low_vol 的 (a) IC IR（ic_mean/ic_std）与 (b) h=21 q5 纯多头
净 Sharpe。预期正交合成 IC ≈ Σic/√k ~ 与 low_vol 打平——诚实的赢法是
方差缩减，不是原始 IC。不得事后在 4 horizon × 8 指标网格里挑赢的格子。

【审计修正落地】
- eod_reversal_flow 剔出打分集（其总账裁决=执行叠加，不是成分；它是唯一
  日频翻新的成分，会主导复合分的逐日变化——付它的换手、赚不到它的隔夜 alpha）。
- "可得均值"会把缺失变成因子（k=3 名字的复合分方差高 √(5/3)，系统性挤占
  极端分位，而 13F 缺失与小盘/未映射相关）——改用 **0.5 中性填补**：
  composite = (Σ可得秩 + 0.5×缺失数) / k_total，且要求主干 low_vol 必须在场。
- high_52w 残差化**逐日横截面**做（rank 对 rank 的当日 OLS，残差当日重排回
  [0,1]）——全样本 beta 或复用 wave-4 全窗相关系数都是前视，禁止。
- 排名在 (eligible ∧ 有值) 内进行，先掩码后排名（成分各自的 [0,1] 刻度
  不能被不可交易名字拉伸）。
- 伪样本外：2004-2015（13F 空缺按 0.5 填补=诚实的前 13F 时代复合）另跑一腿。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from dotenv import load_dotenv

from research.backtest import eligibility_mask
from research._trials_store import append_study
from research.data import (
    load_delisting_returns,
    research_engine,
    resolve_terminal_returns,
    securities_with_uncovered_events,
)
from research.evaluate import _markdown_table, evaluate_factor, _forward_return
from research.factors.builtins.composite_v1 import (
    combine_ranks,
    eligible_component_ranks,
    residualize_high_52w,
)
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
# 定案骨架（2026-07-07 ledger）：low_vol + high_52w 残差 + size。此前默认是含已裁决
# 死亡的 breadth/delta_IO 的 wave-6 旧 4 信号版——订正为骨架，防实验场误用旧默认。
COMPONENTS = ("low_vol", "high_52w", "size")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--components", default=",".join(COMPONENTS),
                        help="打分成分（低于 Bonferroni 的成分须剔除；事后翻符号=样本内挖掘，禁止）")
    parser.add_argument("--include-eod", action="store_true",
                        help="诊断口径：把 eod_reversal_flow 加回打分集（5 信号对照）")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # systemd-run 洗净环境（run_research.sh 发射）下 .env 是唯一的连库配置来源
    args = parse_args(argv)
    engine = research_engine()
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]

    probe_dates = pd.bdate_range(args.start, args.end)
    bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                          columns=("close", "volume"), buffer_days=200)
    close = bars["close"]
    dates = close.index[(close.index >= pd.Timestamp(args.start)) & (close.index <= pd.Timestamp(args.end))]
    adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids, buffer_days=200)
    adj_close = adj_close.reindex(index=close.index, columns=close.columns)
    universe = close.columns
    eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
    bad = securities_with_uncovered_events(engine, start=args.start, end=args.end)
    if bad:
        eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)

    ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                        as_of=pd.Timestamp(args.end))
    names = [x.strip() for x in args.components.split(",") if x.strip()]
    if args.include_eod:
        names.append("eod_reversal_flow")
    assert "low_vol" in names, "主干 low_vol 必须在打分集"
    # 复合构造复用注册因子 composite_v1 的共享函数——位级一致的单一事实源。
    ranks = eligible_component_ranks(ctx, eligible, names)
    for name in names:
        print(f"  {name}: coverage={ranks[name].notna().sum(axis=1).median():.0f}/day", flush=True)
    ranks = residualize_high_52w(ranks, names)
    composite = combine_ranks(ranks, names)

    available = ~np.isnan(np.stack([ranks[n].to_numpy() for n in names]))
    k_dist = pd.Series(available.sum(axis=0)[ranks["low_vol"].notna().to_numpy()]).value_counts(
        normalize=True).sort_index()
    print(f"k 分布（主干在场行）：\n{k_dist.round(3).to_string()}", flush=True)

    adj_for_eval = adj_close.loc[dates]
    # 前向收益/分位回测退市终局与 evaluate 同口径（W0 审核发现 #2：本脚本曾是
    # 第四个未收口的 ffill 消费方，composite_v1 的判据恰在这条路径上算出）。
    realized = load_delisting_returns(engine)
    terminal, term_fallback = resolve_terminal_returns(realized, None)
    forward = {h: _forward_return(adj_for_eval, h, terminal_return=terminal,
                                  terminal_return_fallback=term_fallback) for h in (1, 5, 10, 21)}
    tag = "composite_v1_5sig" if args.include_eod else "composite_v1"
    results = {}
    for label, factor in (("composite", composite), ("low_vol_solo", ranks["low_vol"])):
        res = evaluate_factor(factor, forward, eligibility=eligible, horizons=(1, 5, 10, 21),
                              adj_close=adj_for_eval, cost_bps=10.0, min_coverage=100,
                              factor_name=f"{tag}_{label}",
                              terminal_return=terminal, terminal_return_fallback=term_fallback)
        ic = res.ic_table
        q = res.quantile_metrics
        results[label] = {
            "ic_mean_h5": float(ic.loc[5, "mean_ic"]), "nw_t_h5": float(ic.loc[5, "nw_t"]),
            "ic_ir_h5": float(ic.loc[5, "mean_ic"] / ic.loc[5, "std_ic"]) if "std_ic" in ic.columns else np.nan,
            "q5_net_sharpe_h21": float(q.loc[(21, "q5"), "sharpe_net"]),
            "q5_ann_ret_h21": float(q.loc[(21, "q5"), "ann_return"]),
            "q5_turnover_h21": float(q.loc[(21, "q5"), "ann_turnover"]),
            "ls_net_sharpe_h21": float(q.loc[(21, "ls_q5_q1"), "sharpe_net"]),
        }
        print(f"\n== {label} ==\n{pd.Series(results[label]).round(4).to_string()}", flush=True)

    table = pd.DataFrame(results).T
    verdict = (table.loc["composite", "ic_ir_h5"] > table.loc["low_vol_solo", "ic_ir_h5"]) and (
        table.loc["composite", "q5_net_sharpe_h21"] > table.loc["low_vol_solo", "q5_net_sharpe_h21"])
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"{tag}_{dates[0].date()}_{dates[-1].date()}.md")
    with open(out, "w") as fh:
        fh.write(f"# 复合打分 {tag} {dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"成分：{', '.join(names)}（high_52w 为逐日残差版）；0.5 中性填补；"
                 f"主干 low_vol 必须在场。\n\n{_markdown_table(table.round(4))}\n\n"
                 f"预注册判据（IC IR 与 q5 净 Sharpe 双优于 low_vol 单干）："
                 f"{'PASS' if verdict else 'FAIL'}\n")
    print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
    # 部署判定入机器台账（W0-P3；不入 Bonferroni 分母，见 _trials_store.append_study）
    append_study(
        study="composite_study",
        factor_name=tag,
        verdict=bool(verdict),
        criteria="IC IR(h5) 与 q5 净 Sharpe(h21) 双优于 low_vol 单干",
        params={"components": list(names), "include_eod": bool(args.include_eod), "cost_bps": 10.0,
                "ic_delisting_caliber": "realized_v1"},
        eval_start=dates[0].date(),
        eval_end=dates[-1].date(),
        report_path=os.path.relpath(out, os.path.dirname(os.path.dirname(__file__))),
        criterion_values={
            "ic_ir_h5_composite": float(table.loc["composite", "ic_ir_h5"]),
            "ic_ir_h5_low_vol_solo": float(table.loc["low_vol_solo", "ic_ir_h5"]),
            "q5_net_sharpe_h21_composite": float(table.loc["composite", "q5_net_sharpe_h21"]),
            "q5_net_sharpe_h21_low_vol_solo": float(table.loc["low_vol_solo", "q5_net_sharpe_h21"]),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
