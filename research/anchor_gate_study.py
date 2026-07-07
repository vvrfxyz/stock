"""H3 锚定门控反转诊断（wave-11；假设见 docs/wave11_hypotheses.md）。

问题：反转是否集中在远离 52 周高的名字（远离锚 = 位移是噪声/流动性 → 回归；
贴近锚 = 位移是信息 → 漂移不回归）？

方法（全向量化）：逐日按 high_52w 中位数把 (eligible ∧ 双有值) 截面劈两半，
半区内分别算 bollinger_b 对 fwd 的秩 IC（_masked_rowwise_corr），
差异序列 NW t 检验。附各半区独立 IC 供三关卡预筛。

用法：python -m research.anchor_gate_study --start 2016-01-04 --end 2026-07-02
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
from research.progress import Progress

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--horizon", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    prog = Progress("anchor_gate", total=4)
    engine = research_engine()
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]

    with prog.stage("面板与因子", item=1):
        probe_dates = pd.bdate_range(args.start, args.end)
        bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                              columns=("close", "volume"), buffer_days=200)
        close = bars["close"]
        dates = close.index[(close.index >= pd.Timestamp(args.start))
                            & (close.index <= pd.Timestamp(args.end))]
        adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids,
                                         buffer_days=200).reindex(index=close.index,
                                                                  columns=close.columns)
        universe = close.columns
        eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
        bad = securities_with_uncovered_events(engine, start=args.start, end=args.end)
        if bad:
            eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)
        ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                            as_of=pd.Timestamp(args.end))
        reversal = get("bollinger_b").compute(ctx).where(eligible)
        anchor = get("high_52w").compute(ctx).where(eligible)

    with prog.stage("半区划分", item=2):
        fwd = _forward_return(adj_close.loc[dates], args.horizon)
        valid = reversal.notna() & anchor.notna() & fwd.notna()
        anchor_masked = anchor.where(valid)
        median = anchor_masked.median(axis=1)
        far = anchor_masked.lt(median, axis=0) & valid       # 远离 52 周高
        near = anchor_masked.ge(median, axis=0) & valid      # 贴近 52 周高

    with prog.stage("半区 IC", item=3):
        f_rank_all = reversal.rank(axis=1, method="average", na_option="keep").to_numpy()
        r_rank_all = fwd.rank(axis=1, method="average", na_option="keep").to_numpy()
        rows, series = [], {}
        for label, mask in (("far_from_high", far), ("near_high", near)):
            m = mask.to_numpy()
            # 半区内重排名（半区截面独立排序，防跨区秩污染）
            f_half = reversal.where(mask).rank(axis=1, method="average", na_option="keep").to_numpy()
            r_half = fwd.where(mask).rank(axis=1, method="average", na_option="keep").to_numpy()
            ic = _masked_rowwise_corr(f_half, r_half, m & ~np.isnan(f_half) & ~np.isnan(r_half),
                                      min_coverage=50)
            ser = pd.Series(ic, index=dates).dropna()
            series[label] = pd.Series(ic, index=dates)
            rows.append({"half": label, "ic_mean": float(ser.mean()),
                         "nw_t": _newey_west_t(ser, default_nw_lag(args.horizon, len(ser))),
                         "n_days": len(ser)})
        diff = (series["far_from_high"] - series["near_high"]).dropna()
        rows.append({"half": "far_minus_near", "ic_mean": float(diff.mean()),
                     "nw_t": _newey_west_t(diff, default_nw_lag(args.horizon, len(diff))),
                     "n_days": len(diff)})
        table = pd.DataFrame(rows).set_index("half")

    with prog.stage("写报告", item=4):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out = os.path.join(OUTPUT_DIR,
                           f"anchor_gate_{dates[0].date()}_{dates[-1].date()}.md")
        with open(out, "w") as fh:
            fh.write(f"# H3 锚定门控反转 {dates[0].date()} ~ {dates[-1].date()}\n\n"
                     f"反转=bollinger_b，锚=high_52w 逐日中位劈半，h={args.horizon}。\n"
                     f"预注册预测：far > near；死刑：far_minus_near 不显著。\n\n"
                     f"{_markdown_table(table.round(4))}\n")
    print(table.round(4).to_string(), flush=True)
    print(f"report: {out}", flush=True)
    prog.done()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
