"""EOD 压力反转的隔夜/日内归因分解（wave-3 研究脚本，只读）。

问题：尾盘位移的次日回归（wave-2 发现 t=-2.57）发生在哪一段——
隔夜（次日开盘前，收盘时建仓可全额捕获）还是次日盘中（需要盘中执行）？
这决定信号的可执行性，是分钟数据独占的问题。

方法（全向量化，复用 evaluate 的分位权重矩阵机器）：
- 信号 t 日收盘可得；成分收益取 t+1 日：
    overnight_{t+1} = adjO_{t+1} / adjC_t - 1      （复权跨日，防除权跳变假信号）
    intraday_{t+1} = C_{t+1} / O_{t+1} - 1         （同日原始价，因子约分）
    close2close_{t+1} = adjC_{t+1} / adjC_t - 1    （对账：≈两者复合）
- 分位组合用 evaluate._quantile_weight_matrices（与正式评估同一语义：
  rank method='first'、tradable<100 全零、LS ±0.5）；
  成分 NaN 按持仓不结算处理（0 贡献，与 run_backtest 的 fillna(0) 口径一致）。
- 统计：日均 bps、Newey-West t（lag=default_nw_lag）、年化 Sharpe、逐年均值。

用法：
    RESEARCH_DATABASE_URL=... RESEARCH_CLICKHOUSE_URL=... \
        python -m research.eod_decomposition --start 2016-01-04 --end 2026-07-02
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
from research.evaluate import _newey_west_t, _quantile_weight_matrices, default_nw_lag
from research.factors.minute_loader import load_minute_feature_panel
from research.factors.price_cache import adjusted_close_panel, raw_bar_panels

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, default=date(2016, 1, 4))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 7, 2))
    parser.add_argument("--n-quantiles", type=int, default=5)
    return parser.parse_args(argv)


def _spread_series(weights: np.ndarray, component: np.ndarray, index: pd.DatetimeIndex) -> pd.Series:
    """t 日权重赚 t+1 日成分收益；成分 NaN = 不结算（0 贡献）。"""
    contrib = weights[:-1] * np.nan_to_num(component[1:], nan=0.0)
    return pd.Series(contrib.sum(axis=1), index=index[:-1], dtype="float64")


def _stat_row(series: pd.Series) -> dict[str, float]:
    mean = float(series.mean())
    std = float(series.std(ddof=1))
    return {
        "bps_per_day": mean * 1e4,
        "nw_t": _newey_west_t(series, default_nw_lag(1, len(series))),
        "ann_sharpe": mean / std * np.sqrt(252) if std > 0 else np.nan,
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    engine = research_engine()
    with engine.connect() as conn:
        ids = [int(r[0]) for r in conn.execute(
            text("select id from securities where upper(type) = 'CS' order by id"))]
    print(f"universe: {len(ids)} CS", flush=True)

    probe_dates = pd.bdate_range(args.start, args.end)
    bars = raw_bar_panels(engine, dates=probe_dates, security_ids=ids,
                          columns=("open", "close", "volume"), buffer_days=130)
    close = bars["close"]
    dates = close.index[(close.index >= pd.Timestamp(args.start)) & (close.index <= pd.Timestamp(args.end))]
    adj_close = adjusted_close_panel(engine, dates=probe_dates, security_ids=ids, buffer_days=130)
    adj_close = adj_close.reindex(index=close.index, columns=close.columns)

    eligible = eligibility_mask(close, close * bars["volume"]).loc[dates]
    universe = close.columns

    feats = load_minute_feature_panel(dates, [int(c) for c in universe],
                                      ("ret_last30", "vol_last30_share"), buffer_days=45)
    ret_last30 = feats["ret_last30"].reindex(index=dates, columns=universe)
    share = feats["vol_last30_share"].reindex(index=dates, columns=universe)
    share = share.where(share > 0)
    abn = (share / share.rolling(21, min_periods=10).mean()).clip(upper=3.0)
    signals = {
        "eod_reversal": -ret_last30,
        "eod_reversal_flow": -ret_last30 * abn,
    }

    # 成分收益矩阵（dates × universe）
    ratio = (adj_close / close).loc[dates]
    adj_open = (bars["open"].loc[dates] * ratio).to_numpy()
    adj_c = adj_close.loc[dates].to_numpy()
    raw_o = bars["open"].loc[dates].to_numpy()
    raw_c = close.loc[dates].to_numpy()
    prev_adj_c = np.vstack([np.full((1, adj_c.shape[1]), np.nan), adj_c[:-1]])
    with np.errstate(invalid="ignore", divide="ignore"):
        overnight = adj_open / prev_adj_c - 1
        intraday = raw_c / raw_o - 1
        close2close = adj_c / prev_adj_c - 1
    components = {"overnight": overnight, "intraday": intraday, "close2close": close2close}

    lines = [f"# EOD 反转归因分解 {dates[0].date()} ~ {dates[-1].date()}",
             f"\nuniverse={len(universe)} CS, days={len(dates)}, n_quantiles={args.n_quantiles}",
             "成分口径：overnight=复权跨日开盘/前收，intraday=同日原始收/开，close2close≈复合对账。\n"]
    yearly_frames: dict[str, pd.DataFrame] = {}
    for sig_name, signal in signals.items():
        mats = _quantile_weight_matrices(signal, eligible, args.n_quantiles)
        legs = {"LS(q5-q1)": mats[f"ls_q{args.n_quantiles}_q1"],
                f"q{args.n_quantiles}(long)": mats[f"q{args.n_quantiles}"],
                "q1(short-side)": mats["q1"]}
        rows = []
        for leg_name, w in legs.items():
            for comp_name, comp in components.items():
                s = _spread_series(w, comp, dates)
                rows.append({"leg": leg_name, "component": comp_name, **_stat_row(s)})
                if leg_name.startswith("LS"):
                    yearly_frames.setdefault(sig_name, pd.DataFrame())
                    yearly_frames[sig_name][comp_name] = s.groupby(s.index.year).mean() * 1e4
        table = pd.DataFrame(rows).set_index(["leg", "component"])
        lines.append(f"## {sig_name}\n\n{table.round(3).to_markdown()}\n")
        lines.append(f"### LS 逐年日均 bps\n\n{yearly_frames[sig_name].round(2).to_markdown()}\n")
        print(f"\n== {sig_name} ==\n{table.round(3)}", flush=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = os.path.join(OUTPUT_DIR, f"eod_decomposition_{dates[0].date()}_{dates[-1].date()}.md")
    with open(out, "w") as fh:
        fh.write("\n".join(lines))
    print(f"\nreport: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
