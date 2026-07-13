"""wave17 same_month_seasonality 语义锁定测试（纯合成数据，无 DB）。

锁定：same_month lag 取行与 min 观测门、momentum_12_1 月网格窗口（跳过 t−1）、
秩/IC/残差化的数值语义、LS 权重与换手（建仓全额/漂移差分/断月重启）、
退市月 0% 贡献、判据布尔逻辑。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.same_month_seasonality_study import (
    MIN_CROSS_SECTION,
    build_signal_panels,
    evaluate_verdict,
    quantile_ls_weights,
    rank_pct,
    residual_rank,
    run_monthly_engine,
    spearman_ic,
)


def _months(n: int) -> pd.PeriodIndex:
    return pd.period_range("2003-01", periods=n, freq="M")


class TestSignalConstruction:
    def test_lag_rows_and_min_obs(self):
        # 132 个月（11 年）：行 t=126 的 lag 1..10 中只有 j=126-12k>=0 的 k=1..10 全有
        T, N = 132, 2
        idx = pd.date_range("2003-01-31", periods=T, freq="ME")
        arr = np.zeros((T, N))
        # 每年同月（12 的倍数行差）填 5%，其他填 1%——同月均值应是 5%
        ret = pd.DataFrame(np.full((T, N), 0.01), index=idx)
        for t in range(6, T, 12):
            ret.iloc[t] = 0.05
        sig, mom = build_signal_panels(ret)
        t_hold = 126  # 与 t=6 同余（126-6=120=12*10）
        assert sig[t_hold, 0] == pytest.approx(0.05, rel=1e-12)
        # 行 30（同余 6，只有 lag1=18、lag2=6 两个观测）→ min 3 不满足 → NaN
        assert np.isnan(sig[30, 0])

    def test_momentum_skips_most_recent_month(self):
        T, N = 20, 1
        idx = pd.date_range("2003-01-31", periods=T, freq="ME")
        ret = pd.DataFrame(np.zeros((T, N)), index=idx)
        ret.iloc[19] = 0.50   # t−1 月大涨：不得进入 t=20 的动量……T=20 无行 20，检查 t=19
        ret.iloc[7] = 0.10    # 行 7 ∈ [t−12, t−2]=[7,17] for t=19
        sig, mom = build_signal_panels(ret)
        # t=19：窗口=行 7..17（11 个月），含 0.10 不含 0.50
        assert mom[19, 0] == pytest.approx(0.10, rel=1e-9)

    def test_momentum_requires_full_11_months(self):
        T = 14
        idx = pd.date_range("2003-01-31", periods=T, freq="ME")
        ret = pd.DataFrame(np.zeros((T, 1)), index=idx)
        ret.iloc[3] = np.nan  # 窗口缺一个月
        sig, mom = build_signal_panels(ret)
        assert np.isnan(mom[13, 0])   # t=13 窗口=1..11 含 NaN → NaN
        assert np.isnan(mom[12, 0])   # 窗口=0..10 完整 → 有值？行 3 也在 → NaN
        # 无缺失参照
        ret2 = pd.DataFrame(np.zeros((T, 1)), index=idx)
        _, mom2 = build_signal_panels(ret2)
        assert mom2[13, 0] == pytest.approx(0.0)


class TestRankAndIC:
    def test_rank_pct_ties_average(self):
        r = rank_pct(np.array([1.0, 2.0, 2.0, 3.0]))
        assert r[1] == pytest.approx(r[2])
        assert r[0] < r[1] < r[3]

    def test_spearman_perfect_and_inverse(self):
        s = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        ic, n = spearman_ic(s, s * 2)
        assert ic == pytest.approx(1.0) and n == 5
        ic_inv, _ = spearman_ic(s, -s)
        assert ic_inv == pytest.approx(-1.0)

    def test_residual_rank_kills_control(self):
        # 信号 = 控制的单调函数 → 残差与收益不再相关
        ctrl = np.linspace(0, 1, 50)
        sig = ctrl ** 2
        resid = residual_rank(sig, ctrl)
        # 秩空间里 sig 的秩 == ctrl 的秩 → 残差全 ~0
        assert np.nanmax(np.abs(resid)) < 1e-10

    def test_residual_keeps_orthogonal_part(self):
        rng = np.random.default_rng(0)
        ctrl = rng.normal(0, 1, 500)
        indep = rng.normal(0, 1, 500)
        sig = ctrl + 3 * indep
        resid = residual_rank(sig, ctrl)
        ic, _ = spearman_ic(resid, indep)
        assert ic > 0.5


class TestLSAndTurnover:
    def test_weights_sum_and_sides(self):
        s = np.arange(100, dtype="float64")
        w = quantile_ls_weights(s, 5)
        assert w.sum() == pytest.approx(0.0, abs=1e-12)
        assert w[w > 0].sum() == pytest.approx(1.0)
        assert w[w < 0].sum() == pytest.approx(-1.0)
        assert (w[80:] > 0).all() and (w[:20] < 0).all()

    def _engine_inputs(self, T: int, N: int):
        sig = np.tile(np.arange(N, dtype="float64"), (T, 1))
        mom = np.tile(np.arange(N, dtype="float64")[::-1], (T, 1))
        ret = np.zeros((T, N))
        elig = np.ones((T, N), dtype=bool)
        return sig, mom, ret, elig

    def test_first_month_full_turnover_then_zero(self):
        T, N = 3, max(400, MIN_CROSS_SECTION)
        sig, mom, ret, elig = self._engine_inputs(T, N)
        res = run_monthly_engine(sig, mom, ret, elig, _months(T))
        assert res.turnover[0] == pytest.approx(2.0)   # 建仓：|+1|+|−1|
        assert res.turnover[1] == pytest.approx(0.0)   # 权重不变、零收益无漂移
        assert res.ls_gross[1] == pytest.approx(0.0)

    def test_delisted_month_contributes_zero(self):
        T, N = 2, max(400, MIN_CROSS_SECTION)
        sig, mom, ret, elig = self._engine_inputs(T, N)
        ret[0, N - 1] = np.nan          # 多腿一只月内退市 → 该股贡献 0
        ret[0, : N - 1] = 0.0
        res = run_monthly_engine(sig, mom, ret, elig, _months(T))
        assert res.ls_gross[0] == pytest.approx(0.0)

    def test_thin_month_skipped_and_counted(self):
        T, N = 2, max(400, MIN_CROSS_SECTION)
        sig, mom, ret, elig = self._engine_inputs(T, N)
        elig[0, :] = False
        elig[0, :10] = True             # <300 → 剔月
        res = run_monthly_engine(sig, mom, ret, elig, _months(T))
        assert res.skipped_thin == 1
        assert np.isnan(res.ic[0]) and np.isnan(res.ls_gross[0])
        assert res.turnover[1] == pytest.approx(2.0)  # 断月后重启全额


class TestVerdict:
    def _blocks(self, t1, t2, t3, h4):
        return ({"mean": 0.01 if t1 > 0 else -0.01, "nw_t": t1},
                {"mean": 0.01 if t2 > 0 else -0.01, "nw_t": t2},
                {"mean": 0.001 if t3 > 0 else -0.001, "nw_t": t3}, h4)

    def test_all_pass(self):
        h1, h2, h3, h4 = self._blocks(3.5, 2.5, 2.1, 0.005)
        assert evaluate_verdict(h1, h2, h3, h4)["pass"]

    def test_momentum_rename_gate(self):
        h1, h2, h3, h4 = self._blocks(5.0, 1.0, 3.0, 0.005)  # H2 不过 → 整体 FAIL
        v = evaluate_verdict(h1, h2, h3, h4)
        assert not v["h2_partial"] and not v["pass"]

    def test_stability_sign_gate(self):
        h1, h2, h3, h4 = self._blocks(3.5, 2.5, 2.1, -0.001)
        v = evaluate_verdict(h1, h2, h3, h4)
        assert not v["h4_stability"] and not v["pass"]

    def test_study_kind_registered(self):
        from research._trials_store import STUDY_KINDS
        assert "calendar_technical" in STUDY_KINDS
