"""wave-10b 散户集中度模拟的持仓延续抽样（_pick_with_continuity）单元测试。

锁定 2026-07-07 修复语义：保留仍在 q5 的旧持仓、只随机补充离场者——
每期独立重抽是首版 bug（虚增年换手到 24 倍）。

口径 v2（2026-07-08，W0-P1）追加：_subportfolio_net_returns 金测试——子组合与
整分位同引擎（run_backtest），锁定停牌冻结/跨缺口收益/退市实测注入三语义 +
裁列等价 + 裁列丢权重快速失败。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import run_backtest
from research.retail_reality_study import _pick_with_continuity, _subportfolio_net_returns, _weights_from


def _members(rows: list[list[int]], n_cols: int) -> np.ndarray:
    mat = np.zeros((len(rows), n_cols), dtype=bool)
    for r, cols in enumerate(rows):
        mat[r, cols] = True
    return mat


def test_holdings_persist_while_still_members():
    # 成员集不变时持仓一只不换（换手为零），且张数 = min(holdings, 成员数)
    members = _members([[0, 1, 2, 3, 4]] * 4, n_cols=6)
    picks = _pick_with_continuity(members, holdings=3, rng=np.random.default_rng(0))
    assert picks.sum(axis=1).tolist() == [3, 3, 3, 3]
    for r in range(1, len(members)):
        assert (picks[r] == picks[0]).all()


def test_only_leavers_are_replaced():
    # 第二期 0/1 离场：2 必须保留，补充的两只来自新成员集且无重复
    members = _members([[0, 1, 2], [2, 3, 4, 5]], n_cols=6)
    picks = _pick_with_continuity(members, holdings=3, rng=np.random.default_rng(0))
    assert set(np.flatnonzero(picks[0])) == {0, 1, 2}
    second = np.flatnonzero(picks[1])
    assert 2 in second
    assert set(second) <= {2, 3, 4, 5}
    assert len(second) == 3  # bool 矩阵天然无重复，张数补齐到 holdings


def test_pool_smaller_than_holdings_takes_all():
    members = _members([[0, 1]] * 2, n_cols=4)
    picks = _pick_with_continuity(members, holdings=30, rng=np.random.default_rng(0))
    assert picks.sum(axis=1).tolist() == [2, 2]


def test_empty_row_skipped_and_holdings_survive_gap():
    # 空成员期不建仓；此前持仓在下一有效期若仍在成员集则延续
    members = _members([[0, 1, 2], [], [0, 1, 2]], n_cols=4)
    picks = _pick_with_continuity(members, holdings=2, rng=np.random.default_rng(0))
    assert picks[1].sum() == 0
    assert (picks[2] == picks[0]).all()


def test_stale_holdings_never_picked_outside_members():
    # 每期 picks 严格是当期成员子集（陈旧持仓不会越界残留）
    rng = np.random.default_rng(7)
    members = rng.random((8, 20)) < 0.4
    picks = _pick_with_continuity(members, holdings=5, rng=rng)
    assert not (picks & ~members).any()
    take = np.minimum(members.sum(axis=1), 5)
    assert (picks.sum(axis=1) == take).all()


# ---------------------------------------------------------------------------
# 口径 v2：_subportfolio_net_returns 金测试（与 run_backtest 同引擎语义）
# ---------------------------------------------------------------------------

def _synthetic_panel() -> tuple[pd.DatetimeIndex, pd.Index, pd.DataFrame]:
    """3 证券 × 12 日合成面板：101 正常；102 中段停牌 3 日复牌跳空 +20%；
    103 第 7 日起永久缺失（退市尾巴）。"""
    dates = pd.bdate_range("2024-01-01", periods=12)
    universe = pd.Index([101, 102, 103], dtype="int64")
    px = pd.DataFrame(100.0, index=dates, columns=universe)
    px[101] = 100.0 * (1.01 ** np.arange(12))
    px.loc[dates[4]:dates[6], 102] = np.nan          # 停牌 3 日
    px.loc[dates[7]:, 102] = 120.0                   # 复牌跳空 +20%
    px.loc[dates[6]:, 103] = np.nan                  # 第 7 日起退市
    return dates, universe, px


def _full_holdings_picks(n_reb: int, n_cols: int) -> np.ndarray:
    return np.ones((n_reb, n_cols), dtype=bool)


def test_subportfolio_matches_direct_run_backtest_bitwise():
    # 金测试主断言：helper 输出与手工构造权重直接调 run_backtest 位级一致
    dates, universe, px = _synthetic_panel()
    reb = pd.DatetimeIndex([dates[0], dates[6]])
    picks = _full_holdings_picks(2, 3)
    terminal = pd.Series({103: -0.5})
    got = _subportfolio_net_returns(picks, reb, dates, universe, px, cost_bps=40.0,
                                    terminal_return=terminal, terminal_return_fallback=None)
    w = pd.DataFrame(1.0 / 3.0, index=dates, columns=universe)
    want = run_backtest("direct", w, px, cost_bps=40.0, hold_through_gaps=True,
                        terminal_return=terminal, terminal_return_fallback=None).daily_returns
    pd.testing.assert_series_equal(got, want, check_names=False, rtol=0, atol=0)


def test_subportfolio_column_subset_equals_full_panel():
    # 裁列等价：picks 只覆盖 101/103 时，裁列面板与全宇宙面板结果位级一致
    dates, universe, px = _synthetic_panel()
    reb = pd.DatetimeIndex([dates[0], dates[6]])
    picks = np.zeros((2, 3), dtype=bool)
    picks[:, [0, 2]] = True                          # 101 与 103
    terminal = pd.Series({103: -0.5})
    sub = px[universe[[0, 2]]]
    got_sub = _subportfolio_net_returns(picks, reb, dates, universe, sub, cost_bps=40.0,
                                        terminal_return=terminal, terminal_return_fallback=None)
    got_full = _subportfolio_net_returns(picks, reb, dates, universe, px, cost_bps=40.0,
                                         terminal_return=terminal, terminal_return_fallback=None)
    pd.testing.assert_series_equal(got_sub, got_full, check_names=False, rtol=0, atol=0)


def test_subportfolio_delisting_injection_and_zero_fallback():
    # 退市语义：103 的实测 -50% 注入首个永久缺失日；无实测且无 fallback = 旧口径 0%
    dates, universe, px = _synthetic_panel()
    reb = pd.DatetimeIndex([dates[0]])
    picks = np.zeros((1, 3), dtype=bool)
    picks[0, [0, 2]] = True                          # 等权持 101/103
    with_inj = _subportfolio_net_returns(picks, reb, dates, universe, px, cost_bps=0.0,
                                         terminal_return=pd.Series({103: -0.5}),
                                         terminal_return_fallback=None)
    without = _subportfolio_net_returns(picks, reb, dates, universe, px, cost_bps=0.0,
                                        terminal_return=None, terminal_return_fallback=None)
    d = dates[6]                                      # 103 首个永久缺失日
    assert with_inj[d] == pytest.approx(without[d] + 0.5 * (-0.5))
    # 注入只发生一次：其余日两口径一致
    rest = with_inj.drop(d)
    pd.testing.assert_series_equal(rest, without.drop(d), check_names=False, rtol=0, atol=0)


def test_subportfolio_gap_recovery_on_frozen_weights():
    # 停牌语义：102 停牌 3 日、复牌 +20% 跳空收益按冻结权重落在复牌日（旧快循环丢失该收益）
    dates, universe, px = _synthetic_panel()
    reb = pd.DatetimeIndex([dates[0]])
    picks = np.zeros((1, 3), dtype=bool)
    picks[0, 1] = True                               # 全仓 102
    net = _subportfolio_net_returns(picks, reb, dates, universe, px, cost_bps=0.0,
                                    terminal_return=None, terminal_return_fallback=None)
    resume = dates[7]
    assert net[resume] == pytest.approx(120.0 / 100.0 - 1)      # 跨缺口收益一次性补回
    assert net[dates[4]:dates[6]].fillna(0.0).abs().sum() == 0  # 停牌期间零收益


def test_subportfolio_raises_when_columns_dropped():
    # 裁列丢权重必须炸：picks 含 102 但 adj_sub 只有 101/103
    dates, universe, px = _synthetic_panel()
    reb = pd.DatetimeIndex([dates[0]])
    picks = np.ones((1, 3), dtype=bool)
    sub = px[universe[[0, 2]]]
    with pytest.raises(ValueError, match="丢失权重质量"):
        _subportfolio_net_returns(picks, reb, dates, universe, sub, cost_bps=0.0,
                                  terminal_return=None, terminal_return_fallback=None)


def test_weights_from_zero_rows():
    # 空成员行权重全 0（不产生 NaN/inf）
    w = _weights_from(np.array([[True, True, False], [False, False, False]]))
    assert w[0].tolist() == pytest.approx([0.5, 0.5, 0.0])
    assert w[1].tolist() == [0.0, 0.0, 0.0]
    assert np.isfinite(w).all()


# --------------------------------------------------------------------------- #
# measured 成本档口径（_measured_cost_bps）：cs/2×(1+mult)、fallback、覆盖率
# --------------------------------------------------------------------------- #
class TestMeasuredCost:
    def _mock_loader(self, monkeypatch, cs_panel):
        import research.factors.minute_loader as ml
        import research.retail_reality_study as rr
        # _measured_cost_bps 内部 from ... import load_minute_feature_panel，故 patch 源模块
        monkeypatch.setattr(ml, "load_minute_feature_panel",
                            lambda dates, ids, cols, **kw: {"cs_spread": cs_panel})

    def test_one_side_caliber_and_fallback(self, monkeypatch):
        from research.retail_reality_study import _measured_cost_bps
        dates = pd.date_range("2024-01-01", periods=70, freq="B")
        # sec1 cs 恒 0.0020（20bps 全宽）；sec2 恒 0.0040；sec3 全 NaN（无覆盖→fallback）
        cs = pd.DataFrame({1: [0.0020]*70, 2: [0.0040]*70, 3: [np.nan]*70}, index=dates)
        self._mock_loader(monkeypatch, cs)
        cost, diag = _measured_cost_bps(dates, pd.Index([1, 2, 3], dtype="int64"),
                                        stress_mult=0.5, fallback_bps=40.0)
        # 单边 = 全宽/2 × 1.5 -> sec1: 20/2*1.5=15bps; sec2: 40/2*1.5=30bps
        assert cost[1] == pytest.approx(15.0)
        assert cost[2] == pytest.approx(30.0)
        assert cost[3] == pytest.approx(40.0)  # fallback
        assert diag["n_covered"] == 2
        assert diag["coverage"] == pytest.approx(2/3)

    def test_stress_mult_zero_is_half_spread(self, monkeypatch):
        from research.retail_reality_study import _measured_cost_bps
        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        cs = pd.DataFrame({1: [0.0030]*30}, index=dates)  # 30bps 全宽
        self._mock_loader(monkeypatch, cs)
        cost, _ = _measured_cost_bps(dates, pd.Index([1], dtype="int64"),
                                     stress_mult=0.0, fallback_bps=40.0)
        assert cost[1] == pytest.approx(15.0)  # 30/2×1.0

    def test_uses_rolling_median_not_mean(self, monkeypatch):
        from research.retail_reality_study import _measured_cost_bps
        dates = pd.date_range("2024-01-01", periods=63, freq="B")
        vals = [0.0020]*62 + [0.5]  # 一个极端尾值：中位不受影响、均值会爆
        cs = pd.DataFrame({1: vals}, index=dates)
        self._mock_loader(monkeypatch, cs)
        cost, _ = _measured_cost_bps(dates, pd.Index([1], dtype="int64"),
                                     stress_mult=0.5, fallback_bps=40.0)
        assert cost[1] == pytest.approx(15.0)  # 中位=0.0020 -> 15bps，未被 0.5 尾值污染
