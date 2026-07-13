"""wave16 market_intraday_momentum 语义锁定测试（纯合成数据，无 DB/ClickHouse）。

锁定：NW β/t 对已知自相关结构的正确性、sign 交易的成本双边语义与 sgn=0 不交易、
剔极端日稳健均值、同集合市值加权聚合、判据布尔逻辑（含复现 FAIL 一票否决）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.market_intraday_momentum_study import (
    cap_weighted_market,
    drop_extreme_mean_bps,
    evaluate_asset_verdict,
    nw_beta_t,
    series_stats,
    trading_net,
)


class TestNWBeta:
    def test_known_beta_recovered(self):
        rng = np.random.default_rng(0)
        x = rng.normal(0, 0.01, 2000)
        y = 0.5 * x + rng.normal(0, 0.001, 2000)
        beta, t, n = nw_beta_t(x, y, 10)
        assert n == 2000
        assert beta == pytest.approx(0.5, abs=0.02)
        assert t > 10

    def test_zero_relation_t_small(self):
        rng = np.random.default_rng(1)
        x = rng.normal(0, 0.01, 3000)
        y = rng.normal(0, 0.01, 3000)
        _, t, _ = nw_beta_t(x, y, 10)
        assert abs(t) < 3

    def test_nan_pairs_dropped_min_obs(self):
        x = np.full(50, 0.01)
        y = np.full(50, 0.02)
        beta, t, n = nw_beta_t(x, y, 10)   # n < MIN_OBS=100 → NaN
        assert n == 50 and np.isnan(beta) and np.isnan(t)

    def test_matches_statsmodels_convention(self):
        # OLS beta 精确等于 cov/var（数值恒等，不依赖外部库）
        rng = np.random.default_rng(2)
        x = rng.normal(0, 1, 500)
        y = 2.0 * x + rng.normal(0, 1, 500)
        beta, _, _ = nw_beta_t(x, y, 0)
        xd = x - x.mean(); yd = y - y.mean()
        assert beta == pytest.approx(float(xd @ yd) / float(xd @ xd), rel=1e-12)


class TestTrading:
    def test_cost_is_two_sided(self):
        r1 = np.array([0.01] * 300)
        y = np.array([0.001] * 300)
        net, traded = trading_net(r1, y, 2.0)
        assert traded.all()
        assert net[0] == pytest.approx(0.001 - 2 * 2.0 / 1e4, rel=1e-12)

    def test_sign_zero_no_trade_no_cost(self):
        r1 = np.array([0.0, 0.01, -0.01])
        y = np.array([0.001, 0.001, 0.001])
        net, traded = trading_net(r1, y, 2.0)
        assert not traded[0] and net[0] == 0.0
        assert traded[1] and net[1] == pytest.approx(0.001 - 4e-4)
        assert traded[2] and net[2] == pytest.approx(-0.001 - 4e-4)

    def test_nan_days_stay_nan(self):
        r1 = np.array([np.nan, 0.01])
        y = np.array([0.001, np.nan])
        net, traded = trading_net(r1, y, 2.0)
        assert np.isnan(net[0]) and np.isnan(net[1])
        assert not traded.any()

    def test_series_stats_and_sharpe(self):
        rng = np.random.default_rng(3)
        net = rng.normal(0.0005, 0.001, 1000)
        st = series_stats(net, 10)
        assert st["n"] == 1000
        assert st["mean_bps"] == pytest.approx(float(net.mean() * 1e4), rel=1e-12)
        assert st["sharpe"] == pytest.approx(
            float(net.mean() / net.std(ddof=1) * np.sqrt(252)), rel=1e-9)

    def test_drop_extreme_removes_largest_abs(self):
        net = np.array([0.0001] * 200 + [0.5, -0.5])  # 两个极端日（正负对冲）
        m_drop = drop_extreme_mean_bps(net, 2)
        assert m_drop == pytest.approx(1.0, rel=1e-9)  # 剔掉两端后恰 0.0001 → 1 bps

    def test_drop_extreme_kills_single_crisis_driver(self):
        # 全靠一根大阳线撑正的序列：剔除后转负 → c4 应拦截
        net = np.array([-0.0001] * 150 + [0.30])
        assert float(np.mean(net)) > 0
        assert drop_extreme_mean_bps(net, 10) < 0


class TestMarketAggregation:
    def test_cap_weighting_and_joint_mask(self):
        idx = pd.DatetimeIndex(["2020-01-02", "2020-01-03"])
        r1 = pd.DataFrame({"a": [0.01, 0.02], "b": [0.03, np.nan]}, index=idx)
        y = pd.DataFrame({"a": [0.001, 0.002], "b": [0.003, 0.004]}, index=idx)
        w = pd.DataFrame({"a": [1e9, 1e9], "b": [3e9, 3e9]}, index=idx)
        mkt = cap_weighted_market(r1, y, w)
        # day1：两股都有效 → r1 = (1*0.01+3*0.03)/4，y 同理
        assert mkt["r1"].iloc[0] == pytest.approx((0.01 + 3 * 0.03) / 4)
        assert mkt["y"].iloc[0] == pytest.approx((0.001 + 3 * 0.003) / 4)
        assert mkt["n_names"].iloc[0] == 2
        # day2：b 的 r1 缺 → b 整体剔除（y 侧也不用 b）
        assert mkt["r1"].iloc[1] == pytest.approx(0.02)
        assert mkt["y"].iloc[1] == pytest.approx(0.002)
        assert mkt["n_names"].iloc[1] == 1
        assert mkt["cap_coverage"].iloc[1] == pytest.approx(0.25)

    def test_zero_or_negative_weight_excluded(self):
        idx = pd.DatetimeIndex(["2020-01-02"])
        r1 = pd.DataFrame({"a": [0.01], "b": [0.02]}, index=idx)
        y = pd.DataFrame({"a": [0.001], "b": [0.002]}, index=idx)
        w = pd.DataFrame({"a": [1e9], "b": [0.0]}, index=idx)
        mkt = cap_weighted_market(r1, y, w)
        assert mkt["r1"].iloc[0] == pytest.approx(0.01)
        assert mkt["n_names"].iloc[0] == 1


class TestVerdict:
    def _regs(self, repl_t, post_t):
        return ({"beta": 0.05 if repl_t > 0 else -0.05, "nw_t": repl_t},
                {"beta": 0.05 if post_t > 0 else -0.05, "nw_t": post_t})

    def test_all_pass(self):
        repl, post = self._regs(4.0, 2.5)
        v = evaluate_asset_verdict(repl, post, {"mean_bps": 1.0, "nw_t": 2.5}, 0.5)
        assert v["pass"]

    def test_replication_fail_vetoes(self):
        repl, post = self._regs(2.0, 5.0)  # 复现 t<3 → 一票否决
        v = evaluate_asset_verdict(repl, post, {"mean_bps": 5.0, "nw_t": 5.0}, 5.0)
        assert not v["c1_repl_reg"] and not v["pass"]

    def test_negative_beta_fails_even_high_t(self):
        repl = {"beta": -0.05, "nw_t": 6.0}
        post = {"beta": 0.05, "nw_t": 3.0}
        v = evaluate_asset_verdict(repl, post, {"mean_bps": 1.0, "nw_t": 2.5}, 0.5)
        assert not v["c1_repl_reg"] and not v["pass"]

    def test_crisis_robust_gate(self):
        repl, post = self._regs(4.0, 2.5)
        v = evaluate_asset_verdict(repl, post, {"mean_bps": 1.0, "nw_t": 2.5}, -0.1)
        assert not v["c4_crisis_robust"] and not v["pass"]

    def test_study_kind_registered(self):
        from research._trials_store import STUDY_KINDS
        assert "market_intraday_momentum" in STUDY_KINDS
