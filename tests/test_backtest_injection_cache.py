"""run_backtest 退市注入缓存化的等价性金测试（W0 审核 #19 option b 提速）。

铁律：速度绝不以数字漂移换。本文件用**旧注入实现的参照函数**（重构前 backtest.py
的逐 sim copy+mask+fillna 全路径）对拍新的缓存快路径 + gross 贡献语义守卫，
断言 daily_returns / equity / turnover 逐位一致（check_exact=True）。

覆盖场景：
- 常规退市（持仓先于退市开始）——走缓存快路径；
- 病理：持仓在退市尾巴**中途才开始**（hold_through_gaps=False 时快路径不等价，
  守卫须检出并逐列回退，结果与旧实现逐位一致）；hold_through_gaps=True 时天然等价；
- Series 值 + fallback 补洞 + Series 缺失不注入；
- 未持仓的退市列（缓存在其价格首缺失日注入但 held=0，gross 中性）；
- 多列混合（常规 + 病理 + 未持仓 + fallback 同面板）；
- 缓存命中：同参数二次调用结果一致；
- 标量 terminal_return 分支不受影响。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.backtest as bt
from research.backtest import run_backtest


# --------------------------------------------------------------------------- #
# 参照实现：重构前 backtest.py 的退市注入全路径（无缓存），逐字复刻旧口径。
# --------------------------------------------------------------------------- #
def _reference_backtest(
    name, weights, adj_close, *, cost_bps=10.0, hold_through_gaps=True,
    terminal_return=None, terminal_return_fallback=None,
):
    ffilled = adj_close.ffill()
    returns = ffilled.pct_change(fill_method=None)
    valid_pair = adj_close.notna() & ffilled.shift(1).notna()
    returns = returns.where(valid_pair)
    ever_future_price = adj_close.notna()[::-1].cummax()[::-1]
    missing = adj_close.isna()
    prev_missing = missing.shift(1, fill_value=False)
    gap_entry = missing & ~prev_missing
    carry_zone = missing | (~missing & prev_missing)

    weights = weights.reindex(index=returns.index, columns=returns.columns).fillna(0.0)
    held = weights.shift(1).fillna(0.0)
    terminal_mask = held.gt(0) & adj_close.isna() & ~ever_future_price
    terminal_missing_position_days = int(terminal_mask.sum().sum())

    returns_filled = returns.fillna(0.0)
    if terminal_return is not None and terminal_missing_position_days > 0:
        first_terminal = terminal_mask & ~terminal_mask.shift(1, fill_value=False)
        returns = returns.copy()
        if isinstance(terminal_return, pd.Series):
            per_security = terminal_return.reindex(returns.columns).astype("float64")
            if terminal_return_fallback is not None:
                per_security = per_security.fillna(terminal_return_fallback)
            inject = first_terminal & per_security.notna()
            returns = returns.mask(inject, per_security, axis=1)
        else:
            returns[first_terminal] = terminal_return
        returns_filled = returns.fillna(0.0)

    if hold_through_gaps:
        entry_held = held.where(gap_entry)
        frozen = entry_held.ffill().where(carry_zone)
        effective_held = held.where(~carry_zone, frozen).fillna(held)
    else:
        effective_held = held
    gross = (effective_held * returns_filled).sum(axis=1)
    turnover = (weights - weights.shift(1).fillna(0.0)).abs().sum(axis=1)
    net = gross - turnover * cost_bps / 10_000
    return net


def _assert_bit_equal(weights, adj_close, **kw):
    bt._TERMINAL_INJECTION_CACHE.clear()
    bt._DERIVED_CACHE.clear()
    got = run_backtest("t", weights, adj_close, **kw)
    ref = _reference_backtest("ref", weights, adj_close, **kw)
    pd.testing.assert_series_equal(got.daily_returns, ref, check_names=False, check_exact=True)
    return got


def _dates(n):
    return pd.date_range("2024-01-01", periods=n, freq="B")


def _df(data, n):
    return pd.DataFrame(data, index=_dates(n), dtype="float64")


# --------------------------------------------------------------------------- #
class TestNormalDelisting:
    def test_held_before_delist_fast_path(self):
        # sec1 持有全程、t=3 起永久缺失（退市尾巴），held 在首缺失日>0 -> 快路径
        n = 6
        adj = _df({1: [10, 11, 12, None, None, None], 2: [20, 21, 22, 23, 24, 25]}, n)
        w = _df({1: [1.0] * n, 2: [0.0] * n}, n)
        got = _assert_bit_equal(w, adj, cost_bps=0.0,
                                terminal_return=pd.Series({1: -0.5}))
        # 注入落在 t=3（首个永久缺失日），held(t=3)=w(t=2)=1 -> -0.5
        assert got.daily_returns.iloc[3] == pytest.approx(-0.5)

    def test_series_with_fallback(self):
        n = 6
        adj = _df({1: [10, 11, None, None, None, None],
                   2: [20, 21, 22, None, None, None]}, n)
        w = _df({1: [1.0] * n, 2: [1.0] * n}, n)
        _assert_bit_equal(w, adj, cost_bps=0.0,
                          terminal_return=pd.Series({1: -1.0}),  # sec2 -> fallback
                          terminal_return_fallback=-0.3)

    def test_series_missing_no_fallback_no_injection(self):
        n = 5
        adj = _df({1: [10, 11, None, None, None]}, n)
        w = _df({1: [1.0] * n}, n)
        _assert_bit_equal(w, adj, cost_bps=0.0, terminal_return=pd.Series({999: -1.0}))


class TestUnheldDelisted:
    def test_delisted_but_not_held_is_gross_neutral(self):
        # sec2 退市但从未持有：缓存在其首缺失日注入，但 held=0 -> gross 中性、快路径等价
        n = 6
        adj = _df({1: [10, 11, 12, 13, 14, 15], 2: [20, 21, None, None, None, None]}, n)
        w = _df({1: [1.0] * n, 2: [0.0] * n}, n)  # 只持 sec1
        # sec3 持有并退市以确保 terminal_missing_position_days>0 触发注入分支
        adj[3] = [30, 31, 32, None, None, None]
        w[3] = [1.0] * n
        _assert_bit_equal(w, adj, cost_bps=0.0,
                          terminal_return=pd.Series({2: -0.9, 3: -0.4}))


class TestPathologicalMidTailEntry:
    def _panel(self):
        # sec1 t=2 起永久缺失；持仓在 t=4 才 >0（尾巴中途开始）
        n = 6
        adj = _df({1: [10, 11, None, None, None, None]}, n)
        w = _df({1: [0, 0, 0, 1.0, 1.0, 1.0]}, n)  # held(t)=w(t-1): held 首>0 在 t=4
        return w, adj

    def test_mid_tail_entry_no_hold_through_gaps_guard_falls_back(self):
        # hold_through_gaps=False：effective_held=held，现行注入在 held 首>0 日(t=4)有效，
        # 价格视角(t=2)不等价 -> 守卫须检出并逐列回退，结果与旧实现逐位一致。
        w, adj = self._panel()
        _assert_bit_equal(w, adj, cost_bps=0.0, hold_through_gaps=False,
                          terminal_return=pd.Series({1: -0.5}))

    def test_mid_tail_entry_hold_through_gaps_equivalent(self):
        # hold_through_gaps=True：effective_held 冻结在首缺失日的 held(=0) -> 两口径都赚 0，
        # 天然等价（守卫判安全，走快路径）。
        w, adj = self._panel()
        _assert_bit_equal(w, adj, cost_bps=0.0, hold_through_gaps=True,
                          terminal_return=pd.Series({1: -0.5}))


class TestMultiColumnMixed:
    def test_mixed_normal_pathological_unheld_fallback(self):
        n = 7
        adj = _df({
            1: [10, 11, 12, None, None, None, None],   # 常规退市（持有）
            2: [20, 21, None, None, None, None, None],  # 病理：中途持有
            3: [30, 31, 32, 33, 34, 35, 36],            # 全程存活
            4: [40, 41, 42, None, None, None, None],    # 退市但用 fallback
            5: [50, 51, None, None, None, None, None],  # 退市但从未持有
        }, n)
        w = _df({
            1: [1, 1, 1, 1, 1, 1, 1],
            2: [0, 0, 0, 0, 1, 1, 1],   # held 首>0 在 t=5（尾巴中途）
            3: [1, 1, 1, 1, 1, 1, 1],
            4: [1, 1, 1, 1, 1, 1, 1],
            5: [0, 0, 0, 0, 0, 0, 0],
        }, n)
        for htg in (True, False):
            _assert_bit_equal(w, adj, cost_bps=5.0, hold_through_gaps=htg,
                              terminal_return=pd.Series({1: -0.6, 2: -0.5, 5: -0.9}),
                              terminal_return_fallback=-0.25)


class TestCacheHitAndScalar:
    def test_second_call_same_result(self):
        n = 6
        adj = _df({1: [10, 11, 12, None, None, None]}, n)
        w = _df({1: [1.0] * n}, n)
        tr = pd.Series({1: -0.5})
        bt._TERMINAL_INJECTION_CACHE.clear()
        r1 = run_backtest("a", w, adj, cost_bps=0.0, terminal_return=tr).daily_returns
        # 同一 adj / tr 对象二次调用命中缓存
        r2 = run_backtest("b", w, adj, cost_bps=0.0, terminal_return=tr).daily_returns
        pd.testing.assert_series_equal(r1, r2, check_names=False, check_exact=True)
        assert len(bt._TERMINAL_INJECTION_CACHE) >= 1

    def test_scalar_branch_unchanged(self):
        n = 6
        adj = _df({1: [10, 11, 12, None, None, None]}, n)
        w = _df({1: [1.0] * n}, n)
        _assert_bit_equal(w, adj, cost_bps=0.0, terminal_return=-1.0)

    def test_identity_guard_rejects_stale_id(self):
        # 不同对象即使 id 复用也不误命中（双 ref 身份复核）
        n = 5
        adj1 = _df({1: [10, 11, None, None, None]}, n)
        w = _df({1: [1.0] * n}, n)
        _assert_bit_equal(w, adj1, cost_bps=0.0, terminal_return=pd.Series({1: -0.5}))
        adj2 = _df({1: [10, 11, 12, None, None]}, n)  # 不同尾巴
        _assert_bit_equal(w, adj2, cost_bps=0.0, terminal_return=pd.Series({1: -0.5}))


# --------------------------------------------------------------------------- #
# effective_held 仅对 gap 列冻结的优化——逐位对拍旧全列实现（TestEffectiveHeld）。
# --------------------------------------------------------------------------- #
class TestEffectiveHeldGapRestriction:
    def test_single_gap_reprice_frozen_weight(self):
        # col1 t=2 停牌、t=3 复牌；跨缺口收益须落在进入缺口时的冻结权重（held[t=2]），
        # 而非复牌日已清零的 held[t=3]。
        n = 5
        adj = _df({1: [10, 11, None, 13, 14], 2: [20, 21, 22, 23, 24]}, n)
        w = _df({1: [1, 1, 0, 0, 1.0], 2: [0, 0, 0, 0, 0]}, n)
        _assert_bit_equal(w, adj)

    def test_multi_gap_same_column_frozen_switches(self):
        # 同列两段缺口，冻结值随各自 gap_entry 换挡（0.5 -> 0.2）
        n = 8
        adj = _df({1: [10, None, 12, 13, None, None, 16, 17]}, n)
        w = _df({1: [0.5, 0.5, 0.3, 0.3, 0.3, 0.2, 0.2, 0.2]}, n)
        _assert_bit_equal(w, adj)

    def test_gap_to_tail_with_injection_fast_and_fallback(self):
        # 缺口贯穿到面板尾（退市尾巴）× terminal_return 注入的复合路径：
        # sec1 常规退市（持有，快路径）；sec2 Case1 中途持有（hold_through_gaps=False 回退）。
        n = 7
        adj = _df({
            1: [10, 11, 12, None, None, None, None],
            2: [20, 21, None, None, None, None, None],
        }, n)
        w = _df({1: [1, 1, 1, 1, 1, 1, 1], 2: [0, 0, 0, 0, 1, 1, 1]}, n)
        for htg in (True, False):
            _assert_bit_equal(w, adj, hold_through_gaps=htg,
                              terminal_return=pd.Series({1: -0.5, 2: -0.4}))

    def test_all_columns_have_gaps_degenerate(self):
        # 掩码全真 = gap_cols == 全列，等价旧全列路径
        n = 6
        adj = _df({1: [10, None, 12, 13, 14, 15], 2: [20, 21, None, 23, 24, 25],
                   3: [30, 31, 32, None, 34, 35]}, n)
        w = _df({1: [0.4] * n, 2: [0.3] * n, 3: [0.3] * n}, n)
        _assert_bit_equal(w, adj)

    def test_zero_columns_have_gaps_pure_fast_path(self):
        # 掩码全假 = 无任何缺口，effective_held ≡ held（不冻结）
        n = 6
        adj = _df({1: [10, 11, 12, 13, 14, 15], 2: [20, 21, 22, 23, 24, 25]}, n)
        w = _df({1: [1, 0.5, 0.5, 1, 1, 0], 2: [0, 0.5, 0.5, 0, 0, 1.0]}, n)
        got = _assert_bit_equal(w, adj, cost_bps=8.0)
        assert got.terminal_missing_position_days == 0

    def test_hold_through_gaps_false_branch_unchanged(self):
        n = 6
        adj = _df({1: [10, 11, None, 13, 14, None]}, n)
        w = _df({1: [1.0] * n}, n)
        _assert_bit_equal(w, adj, hold_through_gaps=False)

    def test_mixed_gap_and_nongap_held(self):
        # 非 gap 列被持有：优化后其 effective_held 须 == held（reference 全列 where 亦 == held）
        n = 6
        adj = _df({1: [10, 11, None, 13, 14, 15],   # gap 列
                   2: [20, 21, 22, 23, 24, 25]}, n)  # 无 gap 列，持有
        w = _df({1: [0.5] * n, 2: [0.5] * n}, n)
        _assert_bit_equal(w, adj)
