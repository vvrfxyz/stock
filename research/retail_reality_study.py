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

【成本口径澄清（2026-07-08，team-lead 裁决）】run_backtest 的 cost=turnover(Σ|Δw|)
×cost_bps/1e4——开满仓 turnover=1 即 cost=cost_bps，故 **cost_bps 语义 = 单边每单位
换手成本**。fixed 40 档实收单边 40bps（=往返 80bps）；本 docstring/历史文档把它叫
"往返"是**误称**，实际成本假设比名义严苛 1 倍（历史四次 FAIL 的单边 40bps vs 小盘实测
单边中位 6-10bps，高估 4-6 倍——翻案叙事因此更扎实）。fixed 档存量不动（trial 连续性
优先）；measured 模式按同一单边口径对齐：单边成本_i = median_63d(cs_spread_i)/2 ×
(1+stress_mult)（cs=CS 有效价差全宽 → 市价单≈半宽 /2；stress_mult=0.5 预注册，留 50%
垫涵盖冲击/滑点超半价差部分——无可引文献故理据链留痕，跑后精化另立项）。

【预注册判据】小盘桶 q5 整分位（月频 5 相位、40bps 成本档）对小盘桶等权基准
alpha t>=2 且 30 只子组合的中位年化超额 > 0——两者都过才判"散户可部署"。
measured 模式判据锚改用 measured 档 alpha t（口径自洽，见 --cost-mode）。

用法：
    python -m research.retail_reality_study --start 2016-01-04 --end 2026-07-02
    python -m research.retail_reality_study --cost-mode measured  # 逐股实测价差档
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
    parser.add_argument(
        "--cost-mode", choices=("fixed", "measured"), default="fixed",
        help="fixed=固定 20/40/80bps 三档（默认，对照）；measured=逐股实测 cs_spread 价差档",
    )
    parser.add_argument(
        "--stress-mult", type=float, default=0.5,
        help="measured 压力乘数（预注册 0.5）：单边成本=近 63 日 cs_spread 中位/2×(1+mult)",
    )
    parser.add_argument(
        "--measured-fallback-bps", type=float, default=40.0,
        help="measured 模式下无价差覆盖证券的固定回退单边档（默认 40bps）",
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


def _measured_cost_bps(
    dates: pd.DatetimeIndex,
    universe: pd.Index,
    *,
    stress_mult: float,
    fallback_bps: float,
    url: str | None = None,
) -> tuple[pd.Series, dict]:
    """逐证券单边成本（bps）Series，供 run_backtest 的 per-security cost 接口。

    ## 口径预注册（roadmap §6 接入节，2026-07-08 写死）
    - 单边成本_i = median_63d(cs_spread_i) / 2 × (1 + stress_mult)。cs_spread 是
      Corwin-Schultz **有效价差全宽**（相对价差）；散户市价单成本 ≈ 半宽（/2）。
    - **压力乘数 stress_mult=0.5（预注册）**：涵盖冲击/滑点超出半价差的部分——CS 估的
      是价差全宽、未含市场冲击与部分滑点，留 50% 保守垫。文献先验：小盘市价单实现
      成本常达有效价差的 1.3-1.6×（Hasbrouck 2009 有效价差 vs 实现价差楔子），0.5 居中
      偏保守。若跑后需精化另立项，本轮单一预注册值不调参。
    - 时点：cs_spread 是日频微观结构特征、d 日收盘即得（minute_loader PIT 口径），
      逐证券取全窗 63 交易日中位（月频再平衡持有期内成本近似恒定，避免逐日噪声）。
    - **无价差覆盖证券 → 固定 fallback_bps（默认 40bps 单边）并计数**：多为低流动/OTC
      填缝票（全库 cs 无效 42.3%）。覆盖率进报告；若 q5 成员覆盖 <70% 结论标"覆盖不足"。

    返回 (cost_bps Series[index=security_id], diagnostics dict)。cost 为**期内恒定**的
    逐证券标量（全窗中位）——run_backtest 的 per-security 成本接口按此逐列计换手成本。
    """
    from research.factors.minute_loader import load_minute_feature_panel

    ids = [int(c) for c in universe]
    feat = load_minute_feature_panel(
        pd.DatetimeIndex(dates), ids, ("cs_spread",), buffer_days=95, min_bars=100, url=url
    )
    cs = feat.get("cs_spread", pd.DataFrame(index=dates))
    cs = cs.reindex(index=dates)
    # 全窗 63 交易日滚动中位的期末值（近似期内恒定成本）；不足 63 日用现有窗口中位
    med = cs.rolling(63, min_periods=20).median().iloc[-1] if len(cs) else pd.Series(dtype="float64")
    med = med.reindex(universe)
    one_side_frac = med / 2.0 * (1.0 + stress_mult)  # 相对价差半宽 × 压力
    cost = (one_side_frac * 10_000).astype("float64")  # 转 bps
    covered = cost.notna()
    cost = cost.where(covered, fallback_bps)
    diag = {
        "n_universe": int(len(universe)),
        "n_covered": int(covered.sum()),
        "coverage": float(covered.mean()) if len(universe) else 0.0,
        "median_cost_bps_covered": float(cost[covered].median()) if covered.any() else float("nan"),
        "fallback_bps": float(fallback_bps),
        "stress_mult": float(stress_mult),
    }
    return cost, diag


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

    # measured 成本档（可选）：逐证券单边 bps Series；覆盖率诊断入报告。
    measured_cost = None
    measured_diag = None
    if args.cost_mode == "measured":
        with prog.stage("measured 成本档（cs_spread 63d 中位）"):
            measured_cost, measured_diag = _measured_cost_bps(
                dates, universe, stress_mult=args.stress_mult,
                fallback_bps=args.measured_fallback_bps)

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

    measured_table = None
    if measured_cost is not None:
        port_m = run_backtest("retail_q5_measured", port_w, adj_for_bt,
                              cost_bps=measured_cost, hold_through_gaps=True,
                              terminal_return=terminal,
                              terminal_return_fallback=term_fallback).daily_returns
        bench_m = run_backtest("retail_bench_measured", bench_w, adj_for_bt,
                               cost_bps=measured_cost, hold_through_gaps=True,
                               terminal_return=terminal,
                               terminal_return_fallback=term_fallback).daily_returns
        stats_m = _capm_stats(port_m, bench_m, rf)
        measured_table = pd.DataFrame([{"cost_mode": "measured",
                                        **{k: stats_m[k] for k in
                                           ("ann_ret", "alpha_ann", "alpha_nw_t", "ir")}}]
                                      ).set_index("cost_mode")

    # 集中度模拟：offset 0 的成员矩阵上抽 N 只（同权重、同 40bps）。
    # 口径 v2：走 _subportfolio_net_returns（与整分位同引擎）；裁列到 q5 成员并集，
    # adj_sub 单一对象贯穿全部 sim 吃 _DERIVED_CACHE（勿在循环内引入第三个面板对象）。
    reb0, members0, _ = member_frames[0]
    sub_cols = universe[members0.any(axis=0)]
    adj_sub = adj_for_bt[sub_cols]
    # 集中度 sim 的成本档：fixed 模式 40bps（对照口径不变）；measured 模式用逐证券价差档
    # 裁到 sub_cols。q5 成员价差覆盖率进诊断（B.2 资格线：<70% 结论标"覆盖不足"）。
    sim_cost = 40.0
    q5_coverage = None
    if measured_cost is not None:
        sim_cost = measured_cost.reindex(sub_cols).astype("float64")
        # 覆盖率 = sub_cols 中 cs 实测（非 fallback）占比
        is_fallback = np.isclose(sim_cost.to_numpy(), args.measured_fallback_bps)
        q5_coverage = float((~is_fallback).mean()) if len(sub_cols) else 0.0
        measured_diag["q5_member_coverage"] = q5_coverage
        measured_diag["q5_insufficient"] = bool(q5_coverage < 0.70)
    sub_ann = []
    sim_step = max(1, args.n_sims // 20)  # 抽样打行：journald 有 burst rate-limit
    for sim_i in range(args.n_sims):
        if sim_i % sim_step == 0:
            prog.log(f"模拟 {sim_i}/{args.n_sims}")
        # 持仓延续：保留仍在 q5 的旧持仓，只补充离场者（语义与首版 bug 见 _pick_with_continuity）
        picks = _pick_with_continuity(members0, args.holdings, rng)
        net = _subportfolio_net_returns(
            picks, reb0, dates, universe, adj_sub, cost_bps=sim_cost,
            terminal_return=terminal, terminal_return_fallback=term_fallback,
            name=f"retail_sim_{sim_i}",
        )
        sub_ann.append(float((1 + net).prod() ** (TRADING_DAYS / len(net)) - 1))
    sub_ann = pd.Series(sub_ann)
    # 基准腿成本：measured 模式用价差档，fixed 用 40bps（判据锚，见下）
    bench_cost = measured_cost if measured_cost is not None else 40.0
    bench40 = run_backtest("retail_bench_c40", bench_w, adj_for_bt,
                           cost_bps=bench_cost, hold_through_gaps=True,
                           terminal_return=terminal,
                           terminal_return_fallback=term_fallback).daily_returns
    bench_ann = float((1 + bench40).prod() ** (TRADING_DAYS / len(bench40)) - 1)
    sim_table = pd.DataFrame({
        f"{args.holdings}只子组合年化": sub_ann.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95]),
    })

    # 判据锚：fixed 模式用 40bps 行 alpha t（对照口径不变，逐位复现旧结论）；
    # measured 模式用 measured 行 alpha t（同 sim 亦 measured 成本），口径自洽不混。
    if measured_cost is not None:
        verdict_alpha_t = float(measured_table.loc["measured", "alpha_nw_t"])
        criteria = "measured alpha t>=2 且子组合中位超额>0"
    else:
        verdict_alpha_t = float(full_table.loc[40.0, "alpha_nw_t"])
        criteria = "40bps alpha t>=2 且子组合中位超额>0"
    verdict = (verdict_alpha_t >= 2 and float(sub_ann.median()) - bench_ann > 0)
    insufficient = bool(measured_diag and measured_diag.get("q5_insufficient"))
    caliber = ("measured_v2" if measured_cost is not None else "delist_realized_v2") + (
        f"+edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # 文件名编入引擎口径版本（_v2）：v2 数字与 wave-10b 旧口径不可直比，
    # 同名覆盖会毁掉历史判定的纸面依据（审核 #4）；edf 分支再加后缀防双口径互覆。
    # measured 模式再加 _measured 后缀防与 fixed 报告互覆。
    suffix = "_v2" + ("_measured" if measured_cost is not None else "") + (
        f"_edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    out = os.path.join(
        OUTPUT_DIR,
        f"retail_reality_{args.factor}_{dates[0].date()}_{dates[-1].date()}{suffix}.md")
    measured_section = ""
    if measured_table is not None:
        cov = measured_diag
        flag = "  ⚠**覆盖不足（q5<70%）**" if insufficient else ""
        measured_section = (
            f"## measured 成本档（cs_spread 逐股，压力乘数 {args.stress_mult}）\n\n"
            f"{_markdown_table(measured_table.round(4))}\n\n"
            f"宇宙价差覆盖 {cov['coverage']*100:.0f}%（{cov['n_covered']}/{cov['n_universe']}）、"
            f"covered 中位单边 {cov['median_cost_bps_covered']:.1f}bps、"
            f"q5 成员覆盖 {cov.get('q5_member_coverage', float('nan'))*100:.0f}%、"
            f"fallback {cov['fallback_bps']:.0f}bps{flag}\n\n")
    with open(out, "w") as fh:
        fh.write(f"# 散户口径重审（{args.factor}，$20k/{args.holdings} 只） {dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"口径：{caliber}（引擎统一 run_backtest + 退市实测注入，2026-07-08 口径 v2；"
                 f"数字与 wave-10b 旧口径不可直比）\n\n"
                 f"## 小盘桶 q5 整分位 vs 小盘桶等权（固定成本三档）\n\n{_markdown_table(full_table.round(4))}\n\n"
                 f"{measured_section}"
                 f"## 集中度模拟（{args.n_sims} 次随机 {args.holdings} 只，"
                 f"{'measured 价差档' if measured_cost is not None else '40bps'}）\n\n"
                 f"{_markdown_table(sim_table.round(4))}\n\n小盘桶基准年化：{bench_ann:.4f}\n\n"
                 f"预注册判据（{criteria}）：{'PASS' if verdict else 'FAIL'}"
                 f"{'（覆盖不足，结论仅供参考）' if insufficient else ''}\n")
    print(f"\n== 整分位 ==\n{full_table.round(4).to_string()}", flush=True)
    if measured_table is not None:
        print(f"\n== measured ==\n{measured_table.round(4).to_string()}", flush=True)
    print(f"\n== {args.holdings} 只子组合 ==\n{sim_table.round(4).to_string()}\nbench ={bench_ann:.4f}", flush=True)
    print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
    # 部署判定入机器台账（W0-P3；不入 Bonferroni 分母，见 _trials_store.append_study）
    # fixed 模式的 params/criterion_values 键与旧版逐字保持（台账 schema 不破、新旧口径
    # trial 可比）；measured 特有键仅在 measured 模式下追加。
    study_params = {
        "caliber": caliber, "holdings": args.holdings, "n_sims": args.n_sims,
        "seed": args.seed, "exchange_drop_fallback": args.exchange_drop_fallback,
        "cost_mode": args.cost_mode,
    }
    anchor_key = "alpha_nw_t" if measured_cost is not None else "alpha_nw_t_40bps"
    bench_key = "bench_ann" if measured_cost is not None else "bench_ann_40bps"
    study_values = {
        anchor_key: verdict_alpha_t,
        "sub_median_ann": float(sub_ann.median()),
        bench_key: bench_ann,
    }
    if measured_cost is not None:
        study_params.update({
            "stress_mult": args.stress_mult,
            "measured_coverage": measured_diag["coverage"],
            "q5_member_coverage": measured_diag.get("q5_member_coverage"),
            "q5_insufficient": insufficient,
        })
    append_study(
        study="retail_reality",
        factor_name=args.factor,
        verdict=bool(verdict),
        criteria=criteria,
        params=study_params,
        eval_start=dates[0].date(),
        eval_end=dates[-1].date(),
        report_path=os.path.relpath(out, os.path.dirname(os.path.dirname(__file__))),
        criterion_values=study_values,
    )
    prog.done()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
