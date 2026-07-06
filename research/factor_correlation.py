"""价格系因子横截面相关矩阵（wave-4 研究脚本，只读）。

问题（总账开放问题#3）：六个价格系因子是不是其实只有两三个独立信号换马甲？
嫌疑：high_52w ≈ momentum 的位置版；max_lottery ≈ -low_vol（波动大才有大单日收益）；
eod_reversal 应与全部低相关（频率不同）。

方法（全向量化）：
- 对每个交易日做因子值的横截面秩相关（Spearman，复用 evaluate 的
  _masked_rowwise_corr 于 rank 矩阵），输出两两"日均秩相关"矩阵；
- 增量信息比：对每对 (A, B)，A 对 B 逐日横截面秩回归取残差后残差与
  fwd 收益的 IC——这里用更便宜的等价诊断：partial 秩 IC
  ic(A|B) ≈ (ic_A - r_AB × ic_B) / sqrt(1 - r_AB²)（Spearman 近似），
  直接由日频 IC 序列与相关矩阵合成，零额外面板扫描。

用法：
    RESEARCH_DATABASE_URL=... RESEARCH_CLICKHOUSE_URL=... \
        python -m research.factor_correlation --start 2016-01-04 --end 2026-07-02 \
        --factors momentum_12_1,high_52w,low_vol,max_lottery,short_term_reversal,eod_reversal_flow
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from research.backtest import eligibility_mask
from research.data import research_engine
from research.evaluate import _markdown_table, _masked_rowwise_corr, _newey_west_t, default_nw_lag
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, get

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
DEFAULT_FACTORS = "momentum_12_1,high_52w,low_vol,max_lottery,short_term_reversal,eod_reversal_flow"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--factors", default=DEFAULT_FACTORS)
    parser.add_argument("--horizon", type=int, default=5, help="增量 IC 用的前瞻窗口")
    return parser.parse_args(argv)


def _pairwise_daily_rank_corr(ranks: dict[str, np.ndarray], valids: dict[str, np.ndarray],
                              names: list[str]) -> pd.DataFrame:
    """两两日均横截面秩相关矩阵（对每对因子在共同有效值上算逐日 Pearson-of-ranks 再取均值）。"""
    n = len(names)
    mat = np.full((n, n), np.nan)
    for i in range(n):
        mat[i, i] = 1.0
        for j in range(i + 1, n):
            corr = _masked_rowwise_corr(ranks[names[i]], ranks[names[j]],
                                        valids[names[i]] & valids[names[j]], min_coverage=100)
            finite = corr[~np.isnan(corr)]
            mat[i, j] = mat[j, i] = float(finite.mean()) if len(finite) else np.nan
    return pd.DataFrame(mat, index=names, columns=names)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    names = [x.strip() for x in args.factors.split(",") if x.strip()]
    engine = research_engine()
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]
    probe_dates = pd.bdate_range(args.start, args.end)
    bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                          columns=("close", "volume"), buffer_days=400)
    close = bars["close"]
    dates = close.index[(close.index >= pd.Timestamp(args.start)) & (close.index <= pd.Timestamp(args.end))]
    eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
    universe = close.columns
    print(f"universe={len(universe)} CS, days={len(dates)}", flush=True)

    ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                        as_of=pd.Timestamp(args.end))
    ranks: dict[str, np.ndarray] = {}
    valids: dict[str, np.ndarray] = {}
    ic_series: dict[str, pd.Series] = {}

    adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids, buffer_days=400)
    adj_close = adj_close.reindex(index=dates, columns=universe)
    filled = adj_close.ffill()
    shifted = filled.shift(-args.horizon)
    fwd = (shifted / filled - 1).where(adj_close.notna() & shifted.notna())
    fwd_rank = fwd.rank(axis=1, method="average", na_option="keep").to_numpy()
    fwd_valid = fwd.notna().to_numpy()

    for name in names:
        panel = get(name).compute(ctx).where(eligible)
        ranked = panel.rank(axis=1, method="average", na_option="keep")
        ranks[name] = ranked.to_numpy()
        valids[name] = panel.notna().to_numpy()
        corr = _masked_rowwise_corr(ranks[name], fwd_rank, valids[name] & fwd_valid, min_coverage=100)
        ic_series[name] = pd.Series(corr, index=dates)
        print(f"  {name}: coverage_mean={valids[name].sum(axis=1).mean():.0f}", flush=True)

    corr_matrix = _pairwise_daily_rank_corr(ranks, valids, names)

    # partial IC：ic(A|B)（A 对 B 正交化后的近似增量 IC，逐日序列级合成）
    rows = []
    for a in names:
        ic_a = ic_series[a]
        nw = default_nw_lag(args.horizon, len(ic_a.dropna()))
        rows.append({"factor": a, "ic": float(ic_a.mean()), "nw_t": _newey_west_t(ic_a.dropna(), nw),
                     **{f"|{b}": float(((ic_a - corr_matrix.loc[a, b] * ic_series[b])
                                        / np.sqrt(max(1 - corr_matrix.loc[a, b] ** 2, 1e-12))).mean())
                        for b in names if b != a}})
    partial = pd.DataFrame(rows).set_index("factor")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"factor_correlation_{dates[0].date()}_{dates[-1].date()}.md")
    lines = [f"# 价格系因子相关结构 {dates[0].date()} ~ {dates[-1].date()}",
             f"\nuniverse={len(universe)} CS, days={len(dates)}, horizon={args.horizon}\n",
             "## 日均横截面秩相关\n", _markdown_table(corr_matrix.round(3)),
             f"\n## 原始 IC 与 partial IC（对单个因子正交化后, h={args.horizon}）\n",
             _markdown_table(partial.round(4)),
             "\npartial 口径：ic(A|B) ≈ (ic_A - r×ic_B)/√(1-r²)，序列级近似，"
             "作筛选诊断用；正式增量结论需逐日截面回归残差 IC 确认。"]
    with open(out, "w") as fh:
        fh.write("\n".join(lines))
    print(f"\n== 秩相关矩阵 ==\n{corr_matrix.round(3)}", flush=True)
    print(f"\n== partial IC ==\n{partial.round(4)}", flush=True)
    print(f"\nreport: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
