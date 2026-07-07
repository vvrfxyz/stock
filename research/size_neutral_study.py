"""size 中性化检验（wave-9；夜间变现全景的直接推论，幸存 alpha 的最后藏身处）。

背景：wave-5~8 证明 (a) residual_vol 排序过全部关卡（对 size 正交后仍留 63% IC），
(b) 一切等权多头形态对基准零 alpha——疑被等权小盘暴露的结构性拖累（size 制度）吃掉。
本研究把 size 从**组合构造层**剥掉，回答终极问题：排序信息在 size 中性的
可实施组合里到底有没有 alpha。

【预注册判据——先于任何数字写死】
- 信号冻结 residual_vol（wave-5 已裁决的 vol 族首选表达；不得事后换 low_vol 挑好看）。
- 构造：每个再平衡日（h=21，5 相位错峰）在 (eligible ∧ 有信号 ∧ 有市值) 宇宙内
  按市值分三桶；桶内按信号分五分位；组合 = 每桶 1/3 资本 × 桶内 q5 等权；
  基准 = 每桶 1/3 资本 × 全桶等权——**桶匹配基准，size 暴露按构造相等**。
- "size 中性可部署"须全部满足：对桶匹配基准超额 CAPM alpha（rf=DTB3）NW t >= 2、
  alpha >= 1.5%/年、25bps 成本压力下仍 > 0（组合与基准同成本处理）。
- 诊断（不进判据）：三桶各自的桶内日频 rank IC——效应住在哪个桶决定成本现实性。

用法：
    python -m research.size_neutral_study --start 2016-01-04 --end 2026-07-02
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from research.backtest import TRADING_DAYS, eligibility_mask, run_backtest
from research.company_market_cap import is_common_equity
from research.data import research_engine, securities_with_uncovered_events
from research.evaluate import (
    _forward_return,
    _markdown_table,
    _masked_rowwise_corr,
    _newey_west_t,
    default_nw_lag,
)
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, get
from research.lowvol_monetization import _capm_stats, _expand_daily, _security_flags
from research.market_cap import load_market_cap_panel
from utils.risk_free_rates import load_risk_free_daily_returns

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--factor", default="residual_vol")
    parser.add_argument("--n-buckets", type=int, default=3)
    parser.add_argument("--n-quantiles", type=int, default=5)
    return parser.parse_args(argv)


def _bucket_labels(mcap_row: np.ndarray, valid: np.ndarray, n_buckets: int) -> np.ndarray:
    """单行：市值 pct 秩 -> 桶标签 1..n（无效 -> 0）。"""
    labels = np.zeros(len(mcap_row), dtype="int64")
    idx = np.flatnonzero(valid)
    if len(idx) < n_buckets * 20:
        return labels
    order = mcap_row[idx].argsort(kind="stable")
    ranks = np.empty(len(idx), dtype="int64")
    ranks[order] = np.arange(len(idx))
    labels[idx] = np.minimum(ranks * n_buckets // len(idx), n_buckets - 1) + 1
    return labels


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    engine = research_engine()
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]
    flags = _security_flags(engine, ids)

    probe_dates = pd.bdate_range(args.start, args.end)
    bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                          columns=("close", "volume"), buffer_days=200)
    close = bars["close"]
    dates = close.index[(close.index >= pd.Timestamp(args.start)) & (close.index <= pd.Timestamp(args.end))]
    adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids, buffer_days=200)
    adj_close = adj_close.reindex(index=close.index, columns=close.columns)
    universe = close.columns
    eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
    common_ids = flags.index[flags["is_common"]]
    eligible = eligible & pd.Series(universe.isin(common_ids), index=universe)
    bad = securities_with_uncovered_events(engine, start=args.start, end=args.end)
    if bad:
        eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)
    print(f"universe={len(universe)} CS, days={len(dates)}", flush=True)

    ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                        as_of=pd.Timestamp(args.end))
    signal = get(args.factor).compute(ctx)
    mcap = load_market_cap_panel(engine, dates=dates, security_ids=ids)
    mcap = mcap.reindex(index=dates, columns=universe)
    covered = eligible & signal.notna() & mcap.notna()
    print(f"covered/day median={int(covered.sum(axis=1).median())}", flush=True)

    adj_for_bt = adj_close.loc[dates]
    rf = load_risk_free_daily_returns(engine, dates)
    sig_np, mcap_np, cov_np = signal.to_numpy(), mcap.to_numpy(), covered.to_numpy()

    # ---- 组合构造（h=21 五相位；行级循环只在 ~125 再平衡行 × 5 相位上，成本可忽略）
    horizon, offsets = 21, (0, 4, 8, 12, 16)
    port_frames, bench_frames = [], []
    date_pos = {d: i for i, d in enumerate(dates)}
    for off in offsets:
        reb = dates[off::horizon]
        w_port = np.zeros((len(reb), len(universe)))
        w_bench = np.zeros_like(w_port)
        for r, d in enumerate(reb):
            i = date_pos[d]
            buckets = _bucket_labels(mcap_np[i], cov_np[i], args.n_buckets)
            for b in range(1, args.n_buckets + 1):
                members = np.flatnonzero(buckets == b)
                if len(members) < args.n_quantiles * 20:
                    continue
                order = sig_np[i, members].argsort(kind="stable")
                ranks = np.empty(len(members), dtype="int64")
                ranks[order] = np.arange(len(members))
                q = np.minimum(ranks * args.n_quantiles // len(members), args.n_quantiles - 1) + 1
                top = members[q == args.n_quantiles]
                w_port[r, top] = 1.0 / args.n_buckets / len(top)
                w_bench[r, members] = 1.0 / args.n_buckets / len(members)
        port_frames.append(_expand_daily(w_port, reb, dates, universe))
        bench_frames.append(_expand_daily(w_bench, reb, dates, universe))
    avg_port_w = sum(port_frames) / len(port_frames)
    avg_bench_w = sum(bench_frames) / len(bench_frames)

    rows = []
    for cost in (0.0, 10.0, 25.0):
        port = run_backtest(f"sn_q5_c{cost:g}", avg_port_w, adj_for_bt,
                            cost_bps=cost, hold_through_gaps=True).daily_returns
        bench = run_backtest(f"sn_bench_c{cost:g}", avg_bench_w, adj_for_bt,
                             cost_bps=cost, hold_through_gaps=True).daily_returns
        stats = _capm_stats(port, bench, rf)
        rows.append({"cost_bps": cost, **{k: stats[k] for k in
                                          ("ann_ret", "beta", "alpha_ann", "alpha_nw_t", "ir",
                                           "max_rel_dd", "underwater_days")}})
    main_table = pd.DataFrame(rows).set_index("cost_bps")

    # ---- 诊断：桶内日频 rank IC（h=5）
    fwd = _forward_return(adj_for_bt, 5)
    fwd_np = fwd.rank(axis=1, method="average", na_option="keep").to_numpy()
    daily_buckets = np.zeros_like(sig_np, dtype="int64")
    for i in range(len(dates)):
        daily_buckets[i] = _bucket_labels(mcap_np[i], cov_np[i], args.n_buckets)
    ic_rows = []
    for b in range(1, args.n_buckets + 1):
        in_bucket = daily_buckets == b
        masked = pd.DataFrame(np.where(in_bucket, sig_np, np.nan), index=dates, columns=universe)
        f_rank = masked.rank(axis=1, method="average", na_option="keep").to_numpy()
        fwd_masked = pd.DataFrame(np.where(in_bucket, fwd.to_numpy(), np.nan),
                                  index=dates, columns=universe)
        r_rank = fwd_masked.rank(axis=1, method="average", na_option="keep").to_numpy()
        valid = ~np.isnan(f_rank) & ~np.isnan(r_rank)
        ic = _masked_rowwise_corr(f_rank, r_rank, valid, min_coverage=50)
        ser = pd.Series(ic, index=dates).dropna()
        ic_rows.append({"bucket": f"T{b}({'小' if b == 1 else '大' if b == args.n_buckets else '中'}盘)",
                        "ic_mean_h5": float(ser.mean()),
                        "nw_t": _newey_west_t(ser, default_nw_lag(5, len(ser))),
                        "n_days": len(ser)})
    ic_table = pd.DataFrame(ic_rows).set_index("bucket")

    verdict = (main_table.loc[10.0, "alpha_nw_t"] >= 2 and main_table.loc[10.0, "alpha_ann"] >= 0.015
               and main_table.loc[25.0, "alpha_ann"] > 0)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"size_neutral_{args.factor}_{dates[0].date()}_{dates[-1].date()}.md")
    with open(out, "w") as fh:
        fh.write(f"# size 中性化检验（{args.factor}） {dates[0].date()} ~ {dates[-1].date()}\n\n"
                 f"构造：{args.n_buckets} 市值桶 × 桶内 q{args.n_quantiles}，桶匹配基准，同成本。\n\n"
                 f"## 对桶匹配基准（组合 vs 基准同成本）\n\n{_markdown_table(main_table.round(4))}\n\n"
                 f"## 桶内 IC 诊断（h=5）\n\n{_markdown_table(ic_table.round(4))}\n\n"
                 f"预注册判据（t>=2 & alpha>=1.5% & 25bps>0）：{'PASS' if verdict else 'FAIL'}\n")
    print(f"\n== 对桶匹配基准 ==\n{main_table.round(4).to_string()}", flush=True)
    print(f"\n== 桶内 IC ==\n{ic_table.round(4).to_string()}", flush=True)
    print(f"\n预注册判据：{'PASS' if verdict else 'FAIL'}\nreport: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
