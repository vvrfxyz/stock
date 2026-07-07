"""散户口径重审（wave-10b；本金 2 万美元的现实约束下重审 wave-9 小盘效应）。

【口径改变什么（2026-07-07 用户告知本金 $20k）】
- 机构口径判死的"容量/冲击"约束消失：每仓 $500-1000 vs 小盘 $2M+ ADV。
- 真实成本 = 价差一半（零佣金）；小盘往返按 20/40/80 bps 三档压力测试。
- 新约束 = 集中度：$20k 只能持 20-40 只，不能复制 ~200 只的整分位——
  用**随机子组合模拟**量化转化损耗：从小盘桶 q5 成员中随机抽 N 只等权，
  1000 次重抽的收益分布 vs 整分位组合。这是"纸面 IC → 你的账户"的真实翻译。

【口径 v2（2026-07-08，路线图 W0-P1；判据结构不变、数字与 wave-10b 不可直比）】
- 子组合手写 numpy 引擎删除，与整分位/基准同走 run_backtest：补齐停牌冻结、
  跨缺口收益、退市终局三语义（旧快循环三者皆缺，小盘 q5 收益被系统性高估）。
- 全部三条回测腿接 delisting_events 逐证券实测退市收益（load_delisting_returns），
  小盘 q5 的退市密集不再按 0% 处理。
- `--exchange-drop-fallback -0.30` 可选：EXCHANGE_DROP 无实测行读取层合成经验值
  （W1 双口径跑的载体；判定以保守口径为准，见 roadmap §2）。

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
from research._trials_store import append_study
from research.data import (
    load_delisting_returns,
    research_engine,
    resolve_terminal_returns,
    securities_with_uncovered_events,
)
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
    parser.add_argument(
        "--exchange-drop-fallback", type=float, default=None,
        help="EXCHANGE_DROP 无实测退市收益行的读取层经验值（如 -0.30）；默认不合成（保守口径见 roadmap §2）",
    )
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


def _weights_from(mat_bool: np.ndarray) -> np.ndarray:
    """布尔成员矩阵 → 等权权重矩阵（空行权重全 0）。"""
    n = mat_bool.sum(axis=1, keepdims=True).astype("float64")
    return np.divide(mat_bool.astype("float64"), n, out=np.zeros_like(mat_bool, dtype="float64"),
                     where=n > 0)


def _subportfolio_net_returns(
    picks: np.ndarray,
    reb: pd.DatetimeIndex,
    dates: pd.DatetimeIndex,
    universe: pd.Index,
    adj_sub: pd.DataFrame,
    *,
    cost_bps: float,
    terminal_return: float | pd.Series | None,
    terminal_return_fallback: float | None,
    name: str = "retail_sim",
) -> pd.Series:
    """单个子组合：再平衡布尔成员 → 日频等权权重 → run_backtest 净收益。

    口径 v2：与整分位/基准腿同引擎（停牌冻结 + 跨缺口收益 + 退市终局注入），
    删除旧手写 numpy 快循环（三语义皆缺，见 wave-10b 口径 v2 记录）。
    adj_sub 允许是全宇宙面板的列子集（性能：q5 成员并集 ~2-4k 列），但必须包含
    picks 全部权重所在列——裁列丢权重宁可炸不可静默；多次调用传**同一个**
    adj_sub 对象以命中 run_backtest 的 _DERIVED_CACHE（容量 2，按 id() 缓存）。
    """
    pos = np.searchsorted(reb.values, dates.values, side="right") - 1
    w_full = _weights_from(picks)
    col_idx = universe.get_indexer(adj_sub.columns)
    if (col_idx < 0).any():
        # get_indexer 对宇宙外列返回 -1，numpy fancy index 会静默取末列权重（审核 #1）
        alien = adj_sub.columns[col_idx < 0].tolist()
        raise ValueError(f"{name}: adj_sub 含 universe 外列 {alien[:5]}{'…' if len(alien) > 5 else ''}")
    outside = float(w_full.sum() - w_full[:, col_idx].sum())
    if not np.isclose(outside, 0.0):
        raise ValueError(f"{name}: 裁列后丢失权重质量 {outside:.6f}（adj_sub 列未覆盖 picks）")
    w = w_full[np.clip(pos, 0, None)][:, col_idx]
    w[pos < 0] = 0.0
    w_df = pd.DataFrame(w, index=dates, columns=adj_sub.columns)
    return run_backtest(
        name, w_df, adj_sub, cost_bps=cost_bps, hold_through_gaps=True,
        terminal_return=terminal_return, terminal_return_fallback=terminal_return_fallback,
    ).daily_returns


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
    # 退市终局收益（口径 v2）：三条回测腿统一接逐证券实测（口径同 evaluate）
    realized = load_delisting_returns(engine, exchange_drop_fallback=args.exchange_drop_fallback)
    terminal, term_fallback = resolve_terminal_returns(realized, None)

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

    # 整分位组合与基准（各成本档）
    port_w = sum(_expand_daily(_weights_from(m), reb, dates, universe)
                 for reb, m, _ in member_frames) / len(member_frames)
    bench_w = sum(_expand_daily(_weights_from(b), reb, dates, universe)
                  for reb, _, b in member_frames) / len(member_frames)
    rows = []
    for cost in (20.0, 40.0, 80.0):
        port = run_backtest(f"retail_q5_c{cost:g}", port_w, adj_for_bt,
                            cost_bps=cost, hold_through_gaps=True,
                            terminal_return=terminal,
                            terminal_return_fallback=term_fallback).daily_returns
        bench = run_backtest(f"retail_bench_c{cost:g}", bench_w, adj_for_bt,
                             cost_bps=cost, hold_through_gaps=True,
                             terminal_return=terminal,
                             terminal_return_fallback=term_fallback).daily_returns
        stats = _capm_stats(port, bench, rf)
        rows.append({"cost_bps": cost, **{k: stats[k] for k in
                                          ("ann_ret", "alpha_ann", "alpha_nw_t", "ir")}})
    full_table = pd.DataFrame(rows).set_index("cost_bps")

    # 集中度模拟：offset 0 的成员矩阵上抽 N 只（同权重、同 40bps）。
    # 口径 v2：走 _subportfolio_net_returns（与整分位同引擎）；裁列到 q5 成员并集，
    # adj_sub 单一对象贯穿全部 sim 吃 _DERIVED_CACHE（勿在循环内引入第三个面板对象）。
    reb0, members0, _ = member_frames[0]
    sub_cols = universe[members0.any(axis=0)]
    adj_sub = adj_for_bt[sub_cols]
    sub_ann = []
    sim_step = max(1, args.n_sims // 20)  # 抽样打行：journald 有 burst rate-limit
    for sim_i in range(args.n_sims):
        if sim_i % sim_step == 0:
            prog.log(f"模拟 {sim_i}/{args.n_sims}")
        # 持仓延续：保留仍在 q5 的旧持仓，只补充离场者（语义与首版 bug 见 _pick_with_continuity）
        picks = _pick_with_continuity(members0, args.holdings, rng)
        net = _subportfolio_net_returns(
            picks, reb0, dates, universe, adj_sub, cost_bps=40.0,
            terminal_return=terminal, terminal_return_fallback=term_fallback,
            name=f"retail_sim_{sim_i}",
        )
        sub_ann.append(float((1 + net).prod() ** (TRADING_DAYS / len(net)) - 1))
    sub_ann = pd.Series(sub_ann)
    bench40 = run_backtest("retail_bench_c40", bench_w, adj_for_bt,
                           cost_bps=40.0, hold_through_gaps=True,
                           terminal_return=terminal,
                           terminal_return_fallback=term_fallback).daily_returns
    bench_ann = float((1 + bench40).prod() ** (TRADING_DAYS / len(bench40)) - 1)
    sim_table = pd.DataFrame({
        f"{args.holdings}只子组合年化": sub_ann.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]),
    })

    verdict = (full_table.loc[40.0, "alpha_nw_t"] >= 2
               and float(sub_ann.median()) - bench_ann > 0)
    caliber = "delist_realized_v2" + (
        f"+edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # 文件名编入引擎口径版本（_v2）：v2 数字与 wave-10b 旧口径不可直比，
    # 同名覆盖会毁掉历史判定的纸面依据（审核 #4）；edf 分支再加后缀防双口径互覆。
    suffix = "_v2" + (
        f"_edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    out = os.path.join(
        OUTPUT_DIR,
        f"retail_reality_{args.factor}_{dates[0].date()}_{dates[-1].date()}{suffix}.md")
    with open(out, "w") as fh:
        fh.write(f"# 散户口径重审（{args.factor}，$20k/{args.holdings} 只） {dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"口径：{caliber}（引擎统一 run_backtest + 退市实测注入，2026-07-08 口径 v2；"
                 f"数字与 wave-10b 旧口径不可直比）\n\n"
                 f"## 小盘桶 q5 整分位 vs 小盘桶等权（同成本）\n\n{_markdown_table(full_table.round(4))}\n\n"
                 f"## 集中度模拟（{args.n_sims} 次随机 {args.holdings} 只，40bps）\n\n"
                 f"{_markdown_table(sim_table.round(4))}\n\n小盘桶基准年化（40bps）：{bench_ann:.4f}\n\n"
                 f"预注册判据（40bps alpha t>=2 且子组合中位超额>0）：{'PASS' if verdict else 'FAIL'}\n")
    print(f"\n== 整分位 ==\n{full_table.round(4).to_string()}", flush=True)
    print(f"\n== {args.holdings} 只子组合 ==\n{sim_table.round(4).to_string()}\nbench40 ={bench_ann:.4f}", flush=True)
    print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
    # 部署判定入机器台账（W0-P3；不入 Bonferroni 分母，见 _trials_store.append_study）
    append_study(
        study="retail_reality",
        factor_name=args.factor,
        verdict=bool(verdict),
        criteria="40bps alpha t>=2 且子组合中位超额>0",
        params={
            "caliber": caliber, "holdings": args.holdings, "n_sims": args.n_sims,
            "seed": args.seed, "exchange_drop_fallback": args.exchange_drop_fallback,
        },
        eval_start=dates[0].date(),
        eval_end=dates[-1].date(),
        report_path=os.path.relpath(out, os.path.dirname(os.path.dirname(__file__))),
        criterion_values={
            "alpha_nw_t_40bps": float(full_table.loc[40.0, "alpha_nw_t"]),
            "sub_median_ann": float(sub_ann.median()),
            "bench_ann_40bps": bench_ann,
        },
    )
    prog.done()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
