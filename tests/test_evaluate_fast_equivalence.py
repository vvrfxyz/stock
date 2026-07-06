"""evaluate 性能重构的等价性金测试：新向量化实现 vs 旧参照实现，数字逐位一致。

参照实现 = 重构前 evaluate.py 的原始代码原样拷贝（2026-07-06, commit dc236b9 时点）。
合成面板刻意覆盖：NaN 洞、并列值、tradable<100 的日子、零方差日、停牌段、
面板首日前无再平衡的日子。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import hold_between_rebalances, run_backtest
from research.evaluate import _ic_decay_table, _quantile_metrics, _rank_ic_series


# ---------------------------------------------------------------------------
# 参照实现（旧代码原样拷贝）
# ---------------------------------------------------------------------------

def _rank_ic_series_reference(factor, forward_return, min_coverage):
    aligned_factor, aligned_return = factor.align(forward_return, join="left", axis=None)
    valid = aligned_factor.notna() & aligned_return.notna()
    f_rank = aligned_factor.rank(axis=1, method="average", na_option="keep")
    r_rank = aligned_return.rank(axis=1, method="average", na_option="keep")
    rows = []
    for dt in f_rank.index:
        mask = valid.loc[dt]
        if int(mask.sum()) < min_coverage:
            rows.append(np.nan)
            continue
        x = f_rank.loc[dt, mask]
        y = r_rank.loc[dt, mask]
        x_std = x.std(ddof=1)
        y_std = y.std(ddof=1)
        if x_std == 0 or y_std == 0 or pd.isna(x_std) or pd.isna(y_std):
            rows.append(np.nan)
        else:
            rows.append(float(x.corr(y)))
    return pd.Series(rows, index=f_rank.index, dtype="float64")


def _quantile_weights_for_day_reference(signal, eligible, n_quantiles):
    base = pd.Series(0.0, index=signal.index, dtype="float64")
    tradable = signal[eligible.fillna(False) & signal.notna()]
    if len(tradable) < 100:
        return {f"q{i}": base.copy() for i in range(1, n_quantiles + 1)} | {f"ls_q{n_quantiles}_q1": base.copy()}
    ranks = tradable.rank(method="first")
    labels = np.minimum(((ranks - 1) * n_quantiles // len(tradable)).astype(int) + 1, n_quantiles)
    out = {}
    for q in range(1, n_quantiles + 1):
        weights = base.copy()
        members = labels.index[labels == q]
        if len(members) > 0:
            weights.loc[members] = 1.0 / len(members)
        out[f"q{q}"] = weights
    long_short = base.copy()
    low = labels.index[labels == 1]
    high = labels.index[labels == n_quantiles]
    if len(low) > 0 and len(high) > 0:
        long_short.loc[high] = 0.5 / len(high)
        long_short.loc[low] = -0.5 / len(low)
    out[f"ls_q{n_quantiles}_q1"] = long_short
    return out


def _quantile_metrics_reference(factor, eligibility, adj_close, horizons, n_quantiles, cost_bps):
    columns = ["ann_return", "ann_vol", "sharpe_gross", "sharpe_net", "ann_turnover", "max_drawdown"]
    rows = []
    labels = [f"q{i}" for i in range(1, n_quantiles + 1)] + [f"ls_q{n_quantiles}_q1"]
    for horizon in horizons:
        rebalance_index = factor.index[::horizon]
        weights_by_label = {
            label: pd.DataFrame(0.0, index=rebalance_index, columns=factor.columns, dtype="float64")
            for label in labels
        }
        for dt in rebalance_index:
            day_weights = _quantile_weights_for_day_reference(factor.loc[dt], eligibility.loc[dt], n_quantiles)
            for label, weights in day_weights.items():
                weights_by_label[label].loc[dt] = weights
        for label in labels:
            weights = hold_between_rebalances(weights_by_label[label], adj_close.index)
            gross = run_backtest(f"{label}_h{horizon}_gross", weights, adj_close, cost_bps=0, hold_through_gaps=True).metrics()
            net = run_backtest(f"{label}_h{horizon}_net", weights, adj_close, cost_bps=cost_bps, hold_through_gaps=True).metrics()
            rows.append({
                "horizon": horizon, "quantile_label": label,
                "ann_return": net.get("cagr", np.nan), "ann_vol": net.get("ann_vol", np.nan),
                "sharpe_gross": gross.get("sharpe", np.nan), "sharpe_net": net.get("sharpe", np.nan),
                "ann_turnover": net.get("ann_turnover", np.nan), "max_drawdown": net.get("max_drawdown", np.nan),
            })
    return pd.DataFrame(rows).set_index(["horizon", "quantile_label"]).sort_index()[columns]


# ---------------------------------------------------------------------------
# 合成面板：坑全覆盖
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def panels():
    rng = np.random.default_rng(7)
    dates = pd.bdate_range("2022-01-03", periods=160)
    n = 240
    cols = pd.Index(range(1, n + 1), dtype="int64")
    prices = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0, 0.02, (len(dates), n)), axis=0)),
        index=dates, columns=cols)
    # 停牌段与退市尾巴
    prices.iloc[40:47, 5] = np.nan
    prices.iloc[120:, 7] = np.nan
    # 因子：带 NaN、带并列
    factor = pd.DataFrame(rng.normal(0, 1, (len(dates), n)), index=dates, columns=cols)
    factor.iloc[:, 10:20] = factor.iloc[:, 10:20].round(1)     # 制造并列
    factor[factor.abs() > 2.2] = np.nan
    factor.iloc[30] = np.nan                                    # 整日无信号
    factor.iloc[31, : n - 60] = np.nan                          # tradable < 100 的日子
    eligible = pd.DataFrame(True, index=dates, columns=cols)
    eligible.iloc[:, -8:] = False
    eligible.iloc[50:60, :30] = False
    fwd = prices.ffill().shift(-5) / prices.ffill() - 1
    return {"dates": dates, "cols": cols, "prices": prices,
            "factor": factor, "eligible": eligible, "fwd": fwd}


class TestRankIcEquivalence:
    def test_ic_series_bitwise(self, panels):
        new = _rank_ic_series(panels["factor"], panels["fwd"], min_coverage=50)
        ref = _rank_ic_series_reference(panels["factor"], panels["fwd"], min_coverage=50)
        pd.testing.assert_series_equal(new, ref, rtol=1e-12, atol=1e-14)

    def test_ic_series_min_coverage_gate(self, panels):
        new = _rank_ic_series(panels["factor"], panels["fwd"], min_coverage=210)
        ref = _rank_ic_series_reference(panels["factor"], panels["fwd"], min_coverage=210)
        pd.testing.assert_series_equal(new, ref, rtol=1e-12, atol=1e-14)

    def test_ic_series_constant_rows_zero_variance(self, panels):
        """整行常数（零方差）→ 两实现都必须给 NaN，而非 0 或除零告警。"""
        factor = panels["factor"].copy()
        fwd = panels["fwd"].copy()
        factor.iloc[10] = 1.0            # 因子整行常数
        fwd.iloc[20] = 0.05              # 收益整行常数
        factor.iloc[21] = 1.0
        fwd.iloc[21] = 0.05              # 双侧同时常数
        new = _rank_ic_series(factor, fwd, min_coverage=50)
        ref = _rank_ic_series_reference(factor, fwd, min_coverage=50)
        pd.testing.assert_series_equal(new, ref, rtol=1e-12, atol=1e-14)
        assert np.isnan(new.iloc[10]) and np.isnan(new.iloc[20]) and np.isnan(new.iloc[21])


class TestDecayEquivalence:
    def test_decay_table(self, panels):
        fwd_map = {1: panels["prices"].ffill().shift(-1) / panels["prices"].ffill() - 1,
                   5: panels["fwd"]}
        new = _ic_decay_table(panels["factor"], fwd_map, (1, 5), min_coverage=50)
        # 参照：直接用参照 IC 逐 lag 算
        rows = []
        for horizon in (1, 5):
            returns = fwd_map[horizon].reindex(index=panels["factor"].index,
                                               columns=panels["factor"].columns)
            for lag in range(5 + 1):
                ic = _rank_ic_series_reference(panels["factor"], returns.shift(-lag), 50).mean()
                rows.append({"horizon": horizon, "lag": lag,
                             "ic": float(ic) if pd.notna(ic) else np.nan})
        ref = pd.DataFrame(rows, columns=["horizon", "lag", "ic"])
        pd.testing.assert_frame_equal(new, ref, rtol=1e-12, atol=1e-14)


class TestQuantileMetricsEquivalence:
    def test_quantile_metrics_bitwise(self, panels):
        new = _quantile_metrics(panels["factor"], panels["eligible"], panels["prices"],
                                horizons=(1, 5), n_quantiles=5, cost_bps=10.0)
        ref = _quantile_metrics_reference(panels["factor"], panels["eligible"], panels["prices"],
                                          horizons=(1, 5), n_quantiles=5, cost_bps=10.0)
        pd.testing.assert_frame_equal(new, ref, rtol=1e-10, atol=1e-12)
