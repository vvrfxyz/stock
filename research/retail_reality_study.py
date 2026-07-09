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
    parser.add_argument(
        "--measured-min-periods", default="20,10",
        help="measured rolling(63) 中位的有效天数下限，逗号分隔多档并排跑（默认 20,10）；"
             "20=预注册主档，10=覆盖资格线变体（2026-07-09 裁决，见 _measured_cost_bps docstring）",
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
    min_periods: int = 20,
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

    ## min_periods 口径变体（2026-07-09，team-lead 裁决——覆盖资格线而非结果偏好）
    rolling(63) 中位的 min_periods = 有效价差天数下限。**调整动机 = 提高覆盖资格线，
    非结果偏好**：2026-07-09 两份只读诊断显示 q5 缺覆盖的构成——(c) join bug=0、
    (a) 真无分钟史仅 6-7%、主体是 (b) n_bars<100 稀薄交易 + (d) 63 窗内有效天数不足。
    min_periods 20→10 把 (d) 段"真有 100+ bar 但窗口内天数不足"的**干净票**纳入覆盖
    （2020 q5 覆盖 61%→68%、2025 48%→56%），不触碰 thin-bar 段——那段 cs 向下偏 2.4×
    + 负估计剔除率高 6-8pp，n_bars 门 100 永久不降（ledger 方法论节）。**两档 20/10
    并排跑、verdict 翻转显著标注**；min_periods 进 study params_hash，新旧档 trial 不互顶。
    **诚实预期**：①后 2020 达标（~68-70%）而 2025 大概率仍 <70%；若终裁呈"早年 PASS、
    近年覆盖不足"分裂形态，结论按最弱一环表述（v2 翻案仅在覆盖达标窗口内成立），不做
    全窗断言——这是数据边界的真实形状。

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
    # 全窗 63 交易日滚动中位的期末值（近似期内恒定成本）；有效天数下限 = min_periods
    med = cs.rolling(63, min_periods=min_periods).median().iloc[-1] if len(cs) else pd.Series(dtype="float64")
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
        "min_periods": int(min_periods),
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


def _concentration_sim(
    members0, reb0, dates, universe, adj_sub, *, holdings, n_sims, rng, sim_cost,
    terminal, term_fallback, prog,
) -> pd.Series:
    """集中度模拟：n_sims 次随机 holdings 只子组合的年化收益分布（同引擎、同成本档）。"""
    sub_ann = []
    sim_step = max(1, n_sims // 20)  # 抽样打行：journald 有 burst rate-limit
    for sim_i in range(n_sims):
        if sim_i % sim_step == 0:
            prog.log(f"模拟 {sim_i}/{n_sims}")
        picks = _pick_with_continuity(members0, holdings, rng)
        net = _subportfolio_net_returns(
            picks, reb0, dates, universe, adj_sub, cost_bps=sim_cost,
            terminal_return=terminal, terminal_return_fallback=term_fallback,
            name=f"retail_sim_{sim_i}",
        )
        sub_ann.append(float((1 + net).prod() ** (TRADING_DAYS / len(net)) - 1))
    return pd.Series(sub_ann)


def _parse_min_periods(spec: str) -> list[int]:
    """'20,10' -> [20, 10]（保序去重，供 measured 双档并排）。"""
    seen, out = set(), []
    for tok in spec.split(","):
        tok = tok.strip()
        if not tok:
            continue
        v = int(tok)
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out or [20]


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
    # 固定成本三档（对照，两模式都算——measured 报告并排展示以佐证"固定档高估小盘"）
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

    reb0, members0, _ = member_frames[0]
    sub_cols = universe[members0.any(axis=0)]
    adj_sub = adj_for_bt[sub_cols]
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if args.cost_mode == "fixed":
        # fixed 模式：单 sim@40bps + 40bps 判据锚，逐位复现旧结论（对照口径不动）。
        sub_ann = _concentration_sim(
            members0, reb0, dates, universe, adj_sub, holdings=args.holdings,
            n_sims=args.n_sims, rng=rng, sim_cost=40.0,
            terminal=terminal, term_fallback=term_fallback, prog=prog)
        bench40 = run_backtest("retail_bench_c40", bench_w, adj_for_bt,
                               cost_bps=40.0, hold_through_gaps=True,
                               terminal_return=terminal,
                               terminal_return_fallback=term_fallback).daily_returns
        bench_ann = float((1 + bench40).prod() ** (TRADING_DAYS / len(bench40)) - 1)
        sim_table = pd.DataFrame({
            f"{args.holdings}只子组合年化": sub_ann.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])})
        verdict_alpha_t = float(full_table.loc[40.0, "alpha_nw_t"])
        criteria = "40bps alpha t>=2 且子组合中位超额>0"
        verdict = (verdict_alpha_t >= 2 and float(sub_ann.median()) - bench_ann > 0)
        caliber = "delist_realized_v2" + (
            f"+edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
        suffix = "_v2" + (
            f"_edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
        out = os.path.join(
            OUTPUT_DIR,
            f"retail_reality_{args.factor}_{dates[0].date()}_{dates[-1].date()}{suffix}.md")
        with open(out, "w") as fh:
            fh.write(f"# 散户口径重审（{args.factor}，$20k/{args.holdings} 只） {dates[0].date()} ~ {dates[-1].date()}\n\n"
                     f"口径：{caliber}（引擎统一 run_backtest + 退市实测注入，2026-07-08 口径 v2；"
                     f"数字与 wave-10b 旧口径不可直比）\n\n"
                     f"## 小盘桶 q5 整分位 vs 小盘桶等权（固定成本三档）\n\n{_markdown_table(full_table.round(4))}\n\n"
                     f"## 集中度模拟（{args.n_sims} 次随机 {args.holdings} 只，40bps）\n\n"
                     f"{_markdown_table(sim_table.round(4))}\n\n小盘桶基准年化：{bench_ann:.4f}\n\n"
                     f"预注册判据（{criteria}）：{'PASS' if verdict else 'FAIL'}\n")
        print(f"\n== 整分位 ==\n{full_table.round(4).to_string()}", flush=True)
        print(f"\n== {args.holdings} 只子组合 ==\n{sim_table.round(4).to_string()}\nbench40 ={bench_ann:.4f}", flush=True)
        print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
        append_study(
            study="retail_reality", factor_name=args.factor, verdict=bool(verdict),
            criteria=criteria,
            params={"caliber": caliber, "holdings": args.holdings, "n_sims": args.n_sims,
                    "seed": args.seed, "exchange_drop_fallback": args.exchange_drop_fallback,
                    "cost_mode": args.cost_mode},
            eval_start=dates[0].date(), eval_end=dates[-1].date(),
            report_path=os.path.relpath(out, os.path.dirname(os.path.dirname(__file__))),
            criterion_values={"alpha_nw_t_40bps": verdict_alpha_t,
                              "sub_median_ann": float(sub_ann.median()),
                              "bench_ann_40bps": bench_ann})
        prog.done()
        return 0

    # measured 模式：min_periods 双档并排（覆盖资格线变体，2026-07-09 裁决）。
    # 每档独立算成本/表/sim/verdict/覆盖；verdict 翻转显著标注；每档一条台账行
    # （min_periods 进 params_hash，新旧档不互顶）。
    tiers = _parse_min_periods(args.measured_min_periods)
    tier_results = []
    for mp in tiers:
        with prog.stage(f"measured 成本档 min_periods={mp}"):
            measured_cost, mdiag = _measured_cost_bps(
                dates, universe, stress_mult=args.stress_mult,
                fallback_bps=args.measured_fallback_bps, min_periods=mp)
        port_m = run_backtest(f"retail_q5_measured_mp{mp}", port_w, adj_for_bt,
                              cost_bps=measured_cost, hold_through_gaps=True,
                              terminal_return=terminal, terminal_return_fallback=term_fallback).daily_returns
        bench_m = run_backtest(f"retail_bench_measured_mp{mp}", bench_w, adj_for_bt,
                               cost_bps=measured_cost, hold_through_gaps=True,
                               terminal_return=terminal, terminal_return_fallback=term_fallback).daily_returns
        stats_m = _capm_stats(port_m, bench_m, rf)
        sim_cost = measured_cost.reindex(sub_cols).astype("float64")
        is_fb = np.isclose(sim_cost.to_numpy(), args.measured_fallback_bps)
        q5_cov = float((~is_fb).mean()) if len(sub_cols) else 0.0
        mdiag["q5_member_coverage"] = q5_cov
        mdiag["q5_insufficient"] = bool(q5_cov < 0.70)
        sub_ann = _concentration_sim(
            members0, reb0, dates, universe, adj_sub, holdings=args.holdings,
            n_sims=args.n_sims, rng=rng, sim_cost=sim_cost,
            terminal=terminal, term_fallback=term_fallback, prog=prog)
        bench_ann = float((1 + bench_m).prod() ** (TRADING_DAYS / len(bench_m)) - 1)
        alpha_t = float(stats_m["alpha_nw_t"])
        verdict = (alpha_t >= 2 and float(sub_ann.median()) - bench_ann > 0)
        tier_results.append({
            "mp": mp, "stats": stats_m, "diag": mdiag, "sub_ann": sub_ann,
            "bench_ann": bench_ann, "alpha_t": alpha_t, "verdict": verdict,
        })

    # verdict 翻转检测（不同 min_periods 档判据不一致 = 重要敏感性信息）
    verdicts = {t["mp"]: t["verdict"] for t in tier_results}
    verdict_flip = len(set(verdicts.values())) > 1
    caliber = "measured_v2" + (
        f"+edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    suffix = "_v2_measured" + (
        f"_edf{args.exchange_drop_fallback:g}" if args.exchange_drop_fallback is not None else "")
    out = os.path.join(
        OUTPUT_DIR, f"retail_reality_{args.factor}_{dates[0].date()}_{dates[-1].date()}{suffix}.md")
    # 并排 measured 表 + 每档覆盖/判据
    mrows = []
    for t in tier_results:
        d = t["diag"]
        mrows.append({"min_periods": t["mp"],
                      **{k: t["stats"][k] for k in ("ann_ret", "alpha_ann", "alpha_nw_t", "ir")},
                      "sub_median": float(t["sub_ann"].median()), "bench_ann": t["bench_ann"],
                      "q5_cov": round(d["q5_member_coverage"], 3),
                      "verdict": "PASS" if t["verdict"] else "FAIL",
                      "insufficient": d["q5_insufficient"]})
    measured_table = pd.DataFrame(mrows).set_index("min_periods")
    with open(out, "w") as fh:
        fh.write(f"# 散户口径重审（{args.factor}，$20k/{args.holdings} 只，measured 双档） "
                 f"{dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"口径：{caliber}（引擎统一 run_backtest + 退市实测注入；min_periods 双档=覆盖资格线变体，"
                 f"2026-07-09 裁决，n_bars 门 100 不降）\n\n"
                 f"## 固定成本三档（对照）\n\n{_markdown_table(full_table.round(4))}\n\n"
                 f"## measured 逐股价差档 × min_periods 双档（压力乘数 {args.stress_mult}）\n\n"
                 f"{_markdown_table(measured_table.round(4))}\n\n")
        if verdict_flip:
            fh.write(f"⚠**verdict 随 min_periods 翻转**：{verdicts}——覆盖达标档为准，"
                     f"但翻转本身是重要敏感性信息（v2 翻案对覆盖资格线敏感）。\n\n")
        for t in tier_results:
            d = t["diag"]
            flag = "  ⚠**覆盖不足（q5<70%）**" if d["q5_insufficient"] else ""
            fh.write(f"- min_periods={t['mp']}：宇宙覆盖 {d['coverage']*100:.0f}%"
                     f"（{d['n_covered']}/{d['n_universe']}）、covered 中位单边 "
                     f"{d['median_cost_bps_covered']:.1f}bps、q5 成员覆盖 "
                     f"{d['q5_member_coverage']*100:.0f}%、fallback {d['fallback_bps']:.0f}bps、"
                     f"判据 {'PASS' if t['verdict'] else 'FAIL'}{flag}\n")
        fh.write("\n**结论口径**：分裂形态（早年 PASS/近年覆盖不足）按最弱一环表述——"
                 "v2 翻案仅在覆盖达标窗口内成立，不做全窗断言（数据边界的真实形状）。\n")
    print(f"\n== 整分位（固定档）==\n{full_table.round(4).to_string()}", flush=True)
    print(f"\n== measured 双档 ==\n{measured_table.round(4).to_string()}", flush=True)
    if verdict_flip:
        print(f"\n⚠ verdict 随 min_periods 翻转: {verdicts}", flush=True)
    print(f"report: {out}", flush=True)
    # 每档一条台账行（min_periods 进 params_hash）
    for t in tier_results:
        d = t["diag"]
        append_study(
            study="retail_reality", factor_name=args.factor, verdict=bool(t["verdict"]),
            criteria=f"measured(min_periods={t['mp']}) alpha t>=2 且子组合中位超额>0",
            params={"caliber": caliber, "holdings": args.holdings, "n_sims": args.n_sims,
                    "seed": args.seed, "exchange_drop_fallback": args.exchange_drop_fallback,
                    "cost_mode": args.cost_mode, "stress_mult": args.stress_mult,
                    "min_periods": t["mp"], "measured_coverage": d["coverage"],
                    "q5_member_coverage": d["q5_member_coverage"],
                    "q5_insufficient": d["q5_insufficient"]},
            eval_start=dates[0].date(), eval_end=dates[-1].date(),
            report_path=os.path.relpath(out, os.path.dirname(os.path.dirname(__file__))),
            criterion_values={"alpha_nw_t": t["alpha_t"],
                              "sub_median_ann": float(t["sub_ann"].median()),
                              "bench_ann": t["bench_ann"]})
    prog.done()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
