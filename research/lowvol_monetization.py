"""low_vol 变现研究：q5 纯多头对基准的超额（wave-5，只读；设计经对抗审计修订）。

【预注册裁决规则——先于任何数字写死，改动须留痕】
主检验 = h=21、5 相位错峰平均组合（offsets 0/4/8/12/16，等资本分片，可实施口径）。
"可部署"须**全部**满足：
  (i)   对 spy 与 EW-ex-q5 两基准的超额 CAPM alpha（rf=DTB3，全程超额空间）NW t >= 2；
  (ii)  净 alpha >= 2%/年（经济门槛），且 25bps 成本压力档下仍 > 0；
  (iii) 2003-07 ~ 2015-12 伪样本外段 alpha 同号（点估计即可，不要求显著）;
  (iv)  q5 成分干净：非普通股/SPAC 型权重占比逐年披露且不构成主要收益来源。
h=5 与单相位仅作敏感性；LS 仅作诊断（做空腿不进部署判定）。

【审计修正落地】
- 变现对象冻结为 low_vol（总账主干）；residual_vol 只做机制裁决（Study A），
  不得事后挑"哪个变现好看"——多重性防线。
- alpha/Sharpe 全在超额空间：alpha_raw = alpha_true + (1-β)·rf 的机械项在
  2022+ 高利率段可制造 1-2%/年假 alpha。
- spy 总收益完整性在脚本内断言（因子链行数），静默退化为纯价格收益会
  凭空送 ~1.3%/年 alpha。
- EW 基准双口径：含 q5（保守）与剔除 q5（干净对照）。
- 换手口径：双边名义额合计，cost = bps × 双边（报告注明，防 2 倍歧义）。

用法：
    python -m research.lowvol_monetization --start 2016-01-04 --end 2026-07-02
    python -m research.lowvol_monetization --start 2003-07-01 --end 2015-12-31   # OOS 腿
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text

from research.backtest import TRADING_DAYS, eligibility_mask, run_backtest
from research.company_market_cap import is_common_equity
from research.data import research_engine, securities_with_uncovered_events
from research.evaluate import (
    _markdown_table,
    _newey_west_t,
    _quantile_weight_matrices,
    default_nw_lag,
)
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels
from research.factors.protocol import FactorContext, get
from utils.risk_free_rates import load_risk_free_daily_returns

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
SPY_SECURITY_ID = 3379  # symbol='spy'，复权因子链 2007-03 起（78 分红事件）
SPAC_NAME_RE = re.compile(r"acquisition|blank check|spac", re.IGNORECASE)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--factor", default="low_vol", help="变现对象（冻结为 low_vol；改动即另一试验须登记）")
    return parser.parse_args(argv)


def _assert_spy_total_return(engine, start: date, end: date) -> None:
    """spy 因子链在窗口内必须有分红因子行，否则基准是纯价格收益——立刻失败。"""
    with engine.connect() as conn:
        n = conn.execute(text(
            "select count(*) from computed_adjustment_factors "
            "where security_id = :sid and factor_type = 'historical_adjustment' "
            "and date between :s and :e"), {"sid": SPY_SECURITY_ID, "s": start, "e": end}).scalar()
    expected = max(2, int((end - start).days / 365.25 * 4) - 3)  # 季度分红，容忍尾季
    if n < expected:
        raise RuntimeError(
            f"spy 复权因子行 {n} < 预期下限 {expected}（窗口 {start}~{end}）——"
            "基准将退化为纯价格收益，alpha 全部作废。先跑 update_adjustment_factors spy。")


def _security_flags(engine, ids: list[int]) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "select id, name, share_class_figi, list_date from securities where id = any(:ids)"),
            {"ids": ids}).fetchall()
    df = pd.DataFrame(rows, columns=["id", "name", "share_class_figi", "list_date"]).set_index("id")
    df["is_common"] = [
        is_common_equity(r.name, share_class_figi=r.share_class_figi) for r in df.itertuples()]
    df["spac_like"] = df["name"].fillna("").str.contains(SPAC_NAME_RE)
    return df


def _expand_daily(mat: np.ndarray, rebalance_index: pd.DatetimeIndex,
                  daily_index: pd.DatetimeIndex, columns: pd.Index) -> pd.DataFrame:
    pos = np.searchsorted(rebalance_index.values, daily_index.values, side="right") - 1
    out = mat[np.clip(pos, 0, None)]
    if (pos < 0).any():
        out = out.copy()
        out[pos < 0] = 0.0
    return pd.DataFrame(out, index=daily_index, columns=columns)


def _capm_stats(port: pd.Series, bench: pd.Series, rf: pd.Series) -> dict[str, float]:
    df = pd.concat({"p": port, "b": bench, "rf": rf}, axis=1).dropna()
    pe, be = df["p"] - df["rf"], df["b"] - df["rf"]
    beta = float(np.cov(be, pe, ddof=0)[0, 1] / np.var(be))
    alpha_series = pe - beta * be
    diff = df["p"] - df["b"]
    rel_wealth = (1 + df["p"]).cumprod() / (1 + df["b"]).cumprod()
    rel_dd = rel_wealth / rel_wealth.cummax() - 1
    underwater = (rel_dd < 0).astype(int)
    spell = underwater.groupby((underwater == 0).cumsum()).cumsum().max()
    up, dn = df["b"] > 0, df["b"] < 0
    return {
        "ann_ret": float((1 + df["p"]).prod() ** (TRADING_DAYS / len(df)) - 1),
        "beta": beta,
        "alpha_ann": float(alpha_series.mean() * TRADING_DAYS),
        "alpha_nw_t": _newey_west_t(alpha_series, default_nw_lag(1, len(alpha_series))),
        "excess_geo": float(((1 + df["p"]).prod() / (1 + df["b"]).prod()) ** (TRADING_DAYS / len(df)) - 1),
        "ir": float(diff.mean() / diff.std(ddof=1) * np.sqrt(TRADING_DAYS)) if diff.std(ddof=1) > 0 else np.nan,
        "sharpe_ex": float(pe.mean() / pe.std(ddof=1) * np.sqrt(TRADING_DAYS)) if pe.std(ddof=1) > 0 else np.nan,
        "max_rel_dd": float(rel_dd.min()),
        "underwater_days": int(spell),
        "up_capture": float(df.loc[up, "p"].mean() / df.loc[up, "b"].mean()) if up.any() else np.nan,
        "down_capture": float(df.loc[dn, "p"].mean() / df.loc[dn, "b"].mean()) if dn.any() else np.nan,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    engine = research_engine()
    _assert_spy_total_return(engine, args.start, args.end)
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
    # 审计修正：变现 universe 过滤到普通股（baby bond/preferred 误标 CS 的低波动
    # 工具行会挤占 q5 顶端，把票息测成"alpha"）
    common_ids = flags.index[flags["is_common"]]
    eligible = eligible & pd.Series(universe.isin(common_ids), index=universe)
    # 复权洞证券整体剔除（与 evaluate 同口径；2003-2015 OOS 腿尤其关键）
    bad = securities_with_uncovered_events(engine, start=args.start, end=args.end)
    if bad:
        eligible = eligible & ~pd.Series(universe.isin(bad), index=universe)
        print(f"excluded {len(bad)} securities with uncovered adjustment events", flush=True)

    spy_close = adjusted_close_panel(engine, dates=probe_dates,
                                     security_ids=[SPY_SECURITY_ID], buffer_days=10)
    spy = spy_close.iloc[:, 0].reindex(dates).pct_change(fill_method=None).rename("spy")
    panel_rets = adj_close.loc[dates].pct_change(fill_method=None)
    ew_incl = panel_rets.where(eligible).mean(axis=1).rename("ew_incl_q5")
    rf = load_risk_free_daily_returns(engine, dates)

    ctx = FactorContext(engine=engine, dates=dates, security_universe=universe,
                        as_of=pd.Timestamp(args.end))
    signal = get(args.factor).compute(ctx)
    adj_for_bt = adj_close.loc[dates]

    horizon, offsets = 21, (0, 4, 8, 12, 16)
    weight_frames, per_offset = [], []
    for off in offsets:
        reb = dates[off::horizon]
        mats = _quantile_weight_matrices(signal.loc[reb], eligible.loc[reb], 5)
        w = _expand_daily(mats["q5"], reb, dates, universe)
        weight_frames.append(w)
        port = run_backtest(f"q5_o{off}", w, adj_for_bt, cost_bps=10.0, hold_through_gaps=True).daily_returns
        per_offset.append({"offset": off, **{f"{k}_spy": v for k, v in _capm_stats(port, spy, rf).items()
                                             if k in ("alpha_ann", "alpha_nw_t")}})
    # 主组合：5 相位等资本平均（可实施的错峰调仓账本）
    avg_weights = sum(weight_frames) / len(weight_frames)
    ew_ex = panel_rets.where(eligible & (avg_weights <= 0)).mean(axis=1).rename("ew_ex_q5")

    rows, cost_rows = [], []
    for cost in (0.0, 10.0, 25.0):
        result = run_backtest(f"q5_staggered_c{cost:g}", avg_weights, adj_for_bt,
                              cost_bps=cost, hold_through_gaps=True)
        port = result.daily_returns
        if cost == 10.0:
            for bname, bench in (("spy", spy), ("ew_incl_q5", ew_incl), ("ew_ex_q5", ew_ex)):
                rows.append({"bench": bname, **_capm_stats(port, bench, rf)})
            ann_turn = float(result.turnover.mean() * TRADING_DAYS)
            d = (port - ew_ex).dropna()
            yearly = pd.DataFrame({
                "q5_minus_ew_ex": d.groupby(d.index.year).apply(lambda s: float((1 + s).prod() - 1)),
                "q5_minus_spy": (port - spy).dropna().groupby(port.index.year).apply(
                    lambda s: float((1 + s).prod() - 1)),
            })
        cost_rows.append({"cost_bps": cost,
                          "alpha_ann_vs_spy": _capm_stats(port, spy, rf)["alpha_ann"],
                          "alpha_ann_vs_ew_ex": _capm_stats(port, ew_ex, rf)["alpha_ann"]})

    # q5 成分披露：非普通股已剔除；SPAC 型权重占比逐年
    spac_ids = flags.index[flags["spac_like"]]
    spac_w = avg_weights[avg_weights.columns.intersection(spac_ids)].sum(axis=1)
    composition = pd.DataFrame({
        "spac_like_weight": spac_w.groupby(spac_w.index.year).mean(),
        "n_holdings": (avg_weights > 0).sum(axis=1).groupby(avg_weights.index.year).mean().round(0),
    })

    bench_table = pd.DataFrame(rows).set_index("bench")
    offset_table = pd.DataFrame(per_offset).set_index("offset")
    cost_table = pd.DataFrame(cost_rows).set_index("cost_bps")
    lines = [f"# low_vol 变现研究（{args.factor} q5 纯多头） {dates[0].date()} ~ {dates[-1].date()}",
             f"\nuniverse={len(universe)} CS→普通股过滤，days={len(dates)}；"
             f"主口径 h=21 五相位错峰；换手=双边名义额合计，cost=bps×双边；"
             f"年换手（双边，10bps 档）={ann_turn:.1f}x\n",
             "## 对基准（10bps，超额空间 CAPM）\n", _markdown_table(bench_table.round(4)),
             "\n## 相位敏感性（单相位 q5 vs spy）\n", _markdown_table(offset_table.round(4)),
             "\n## 成本压力\n", _markdown_table(cost_table.round(4)),
             "\n## q5 成分披露（SPAC 型名称权重）\n", _markdown_table(composition.round(4)),
             "\n## 逐年超额\n", _markdown_table(yearly.round(4))]
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"lowvol_monetization_{dates[0].date()}_{dates[-1].date()}.md")
    with open(out, "w") as fh:
        fh.write("\n".join(lines))
    print(f"\n== 对基准 ==\n{bench_table.round(4).to_string()}", flush=True)
    print(f"\n== 相位 ==\n{offset_table.round(4).to_string()}", flush=True)
    print(f"\n== 成本压力 ==\n{cost_table.round(4).to_string()}", flush=True)
    print(f"\n== 成分 ==\n{composition.round(4).to_string()}", flush=True)
    print(f"\nreport: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
