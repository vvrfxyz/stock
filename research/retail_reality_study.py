"""散户口径重审（wave-10b；本金 2 万美元的现实约束下重审 wave-9 小盘效应）。

【口径改变什么（2026-07-07 用户告知本金 $20k）】
- 机构口径判死的"容量/冲击"约束消失：每仓 $500-1000 vs 小盘 $2M+ ADV。
- 真实成本 = 价差一半（零佣金）；小盘往返按 20/40/80 bps 三档压力测试。
- 新约束 = 集中度：$20k 只能持 20-40 只，不能复制 ~200 只的整分位——
  用**随机子组合模拟**量化转化损耗：从小盘桶 q5 成员中随机抽 N 只等权，
  1000 次重抽的收益分布 vs 整分位组合。这是"纸面 IC → 你的账户"的真实翻译。

【预注册判据】小盘桶 q5 整分位（月频 5 相位、40bps 成本档）对小盘桶等权基准
alpha t>=2 且 30 只子组合的中位年化超额 > 0——两者都过才判"散户可部署"。

用法：
    python -m research.retail_reality_study --start 2016-01-04 --end 2026-07-02
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from research.backtest import TRADING_DAYS, eligibility_mask, run_backtest
from research.data import research_engine, securities_with_uncovered_events
from research.evaluate import _markdown_table, _newey_west_t, default_nw_lag
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, get
from research.lowvol_monetization import _capm_stats, _expand_daily, _security_flags
from research.market_cap import load_market_cap_panel
from research.progress import Progress
from research.size_neutral_study import _bucket_labels
from utils.risk_free_rates import load_risk_free_daily_returns

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--factor", default="residual_vol")
    parser.add_argument("--holdings", type=int, default=30, help="散户可持只数（$20k/仓位）")
    parser.add_argument("--n-sims", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=7)
    return parser.parse_args(argv)


def _pick_with_continuity(members: np.ndarray, holdings: int, rng: np.random.Generator) -> np.ndarray:
    """持仓延续抽样：保留仍在成员集（q5）的旧持仓，只随机补充离场者。

    复刻真实散户行为、保留 q5 的天然黏性——每期独立重抽会虚增年换手到 24 倍、
    40bps 下多付 ~10%/年（2026-07-07 首版 bug，实测子组合中位被打到 3.7% vs
    整分位 10.7%；修复后中位 8.6%，已回写 research_ledger）。
    """
    picks = np.zeros_like(members)
    held_idx = np.array([], dtype=int)
    for r in range(len(members)):
        idx = np.flatnonzero(members[r])
        if len(idx) == 0:
            continue
        take = min(holdings, len(idx))
        keep = held_idx[np.isin(held_idx, idx)][:take]
        pool = np.setdiff1d(idx, keep, assume_unique=False)
        add = rng.choice(pool, size=min(take - len(keep), len(pool)), replace=False) \
            if take > len(keep) and len(pool) else np.array([], dtype=int)
        held_idx = np.concatenate([keep, add])
        picks[r, held_idx] = True
    return picks


def main(argv: list[str] | None = None) -> int:
    load_dotenv()  # systemd-run 洗净环境（run_research.sh 发射）下 .env 是唯一的连库配置来源
    args = parse_args(argv)
    engine = research_engine()
    prog = Progress("retail_reality")
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]
    flags = _security_flags(engine, ids)

    probe_dates = pd.bdate_range(args.start, args.end)
    with prog.stage("装载 bar/复权面板"):
        bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                              columns=("close", "volume"), buffer_days=200)
        close = bars["close"]
        dates = close.index[(close.index >= pd.Timestamp(args.start)) & (close.index <= pd.Timestamp(args.end))]
        adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids, buffer_days=200)
        adj_close = adj_close.reindex(index=close.index, columns=close.columns)
    universe = close.columns
    eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
    eligible = eligible & pd.Series(universe.isin(flags.index[flags["is_common"]]), index=universe)
    bad = securities_with_uncovered_events(engine, start=args.start, end=args.end)
    if bad:
        eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)

    ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                        as_of=pd.Timestamp(args.end))
    with prog.stage(f"因子 {args.factor} + 市值面板"):
        signal = get(args.factor).compute(ctx)
        mcap = load_market_cap_panel(engine, dates=dates, security_ids=ids).reindex(
            index=dates, columns=universe)
    covered = eligible & signal.notna() & mcap.notna()
    sig_np, mcap_np, cov_np = signal.to_numpy(), mcap.to_numpy(), covered.to_numpy()
    adj_for_bt = adj_close.loc[dates]
    rf = load_risk_free_daily_returns(engine, dates)

    # 小盘桶 q5 成员矩阵（h=21 五相位；成员=小盘桶内信号前 1/5）
    horizon, offsets = 21, (0, 4, 8, 12, 16)
    date_pos = {d: i for i, d in enumerate(dates)}
    member_frames, bench_frames = [], []
    rng = np.random.default_rng(args.seed)
    for off in offsets:
        reb = dates[off::horizon]
        members_mat = np.zeros((len(reb), len(universe)), dtype=bool)
        bench_mat = np.zeros_like(members_mat)
        for r, d in enumerate(reb):
            i = date_pos[d]
            buckets = _bucket_labels(mcap_np[i], cov_np[i], 3)
            small = np.flatnonzero(buckets == 1)
            if len(small) < 100:
                continue
            order = sig_np[i, small].argsort(kind="stable")
            ranks = np.empty(len(small), dtype="int64")
            ranks[order] = np.arange(len(small))
            q = np.minimum(ranks * 5 // len(small), 4) + 1
            members_mat[r, small[q == 5]] = True
            bench_mat[r, small] = True
        member_frames.append((reb, members_mat, bench_mat))

    def _weights_from(mat_bool: np.ndarray) -> np.ndarray:
        n = mat_bool.sum(axis=1, keepdims=True).astype("float64")
        return np.divide(mat_bool.astype("float64"), n, out=np.zeros_like(mat_bool, dtype="float64"),
                         where=n > 0)

    # 整分位组合与基准（各成本档）
    port_w = sum(_expand_daily(_weights_from(m), reb, dates, universe)
                 for reb, m, _ in member_frames) / len(member_frames)
    bench_w = sum(_expand_daily(_weights_from(b), reb, dates, universe)
                  for reb, _, b in member_frames) / len(member_frames)
    rows = []
    for cost in (20.0, 40.0, 80.0):
        port = run_backtest(f"retail_q5_c{cost:g}", port_w, adj_for_bt,
                            cost_bps=cost, hold_through_gaps=True).daily_returns
        bench = run_backtest(f"retail_bench_c{cost:g}", bench_w, adj_for_bt,
                             cost_bps=cost, hold_through_gaps=True).daily_returns
        stats = _capm_stats(port, bench, rf)
        rows.append({"cost_bps": cost, **{k: stats[k] for k in
                                          ("ann_ret", "alpha_ann", "alpha_nw_t", "ir")}})
    full_table = pd.DataFrame(rows).set_index("cost_bps")

    # 集中度模拟：offset 0 的成员矩阵上抽 N 只（同权重、同 40bps）
    reb0, members0, _ = member_frames[0]
    sub_ann = []
    daily_rets = adj_for_bt.pct_change(fill_method=None).to_numpy()
    pos0 = np.searchsorted(reb0.values, dates.values, side="right") - 1
    sim_step = max(1, args.n_sims // 20)  # 抽样打行：journald 有 burst rate-limit
    for sim_i in range(args.n_sims):
        if sim_i % sim_step == 0:
            prog.log(f"模拟 {sim_i}/{args.n_sims}")
        # 持仓延续：保留仍在 q5 的旧持仓，只补充离场者（语义与首版 bug 见 _pick_with_continuity）
        picks = _pick_with_continuity(members0, args.holdings, rng)
        w = _weights_from(picks)[np.clip(pos0, 0, None)]
        w[pos0 < 0] = 0.0
        held = np.vstack([np.zeros((1, w.shape[1])), w[:-1]])
        gross = np.nansum(held * np.nan_to_num(daily_rets, nan=0.0), axis=1)
        turnover = np.abs(w - np.vstack([np.zeros((1, w.shape[1])), w[:-1]])).sum(axis=1)
        net = gross - turnover * 40.0 / 1e4
        sub_ann.append((1 + pd.Series(net, index=dates)).prod() ** (TRADING_DAYS / len(dates)) - 1)
    sub_ann = pd.Series(sub_ann)
    bench40 = run_backtest("retail_bench_c40", bench_w, adj_for_bt,
                           cost_bps=40.0, hold_through_gaps=True).daily_returns
    bench_ann = float((1 + bench40).prod() ** (TRADING_DAYS / len(bench40)) - 1)
    sim_table = pd.DataFrame({
        f"{args.holdings}只子组合年化": sub_ann.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]),
    })

    verdict = (full_table.loc[40.0, "alpha_nw_t"] >= 2
               and float(sub_ann.median()) - bench_ann > 0)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"retail_reality_{args.factor}_{dates[0].date()}_{dates[-1].date()}.md")
    with open(out, "w") as fh:
        fh.write(f"# 散户口径重审（{args.factor}，$20k/{args.holdings} 只） {dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"## 小盘桶 q5 整分位 vs 小盘桶等权（同成本）\n\n{_markdown_table(full_table.round(4))}\n\n"
                 f"## 集中度模拟（{args.n_sims} 次随机 {args.holdings} 只，40bps）\n\n"
                 f"{_markdown_table(sim_table.round(4))}\n\n小盘桶基准年化（40bps）：{bench_ann:.4f}\n\n"
                 f"预注册判据（40bps alpha t>=2 且子组合中位超额>0）：{'PASS' if verdict else 'FAIL'}\n")
    print(f"\n== 整分位 ==\n{full_table.round(4).to_string()}", flush=True)
    print(f"\n== {args.holdings} 只子组合 ==\n{sim_table.round(4).to_string()}\nbench40 ={bench_ann:.4f}", flush=True)
    print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
    prog.done()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
