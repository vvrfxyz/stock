"""回测引擎语义锁定测试。

锁定 H10 审查项指出的核心不变量：
- shift(1) 对齐：t 日权重赚 t+1 日收益，绝无当日前视
- 换手成本：turnover = sum(|w_t - w_{t-1}|)，net = gross - turnover * bps/10000
- NaN 收益 + ffill：停牌缺口复牌日一次性补回跨缺口收益
- hold_through_gaps：停牌期冻结持仓权重，复牌跳空不被清零权重吞掉
- terminal_missing_position_days：持仓后价格永久消失的 security-day 统计
- 分位权重 / rank-IC / forward-return 语义
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import (
    BacktestResult,
    _hold_through_price_gaps,
    _returns_with_gap_recovery,
    _terminal_missing_position_days,
    hold_between_rebalances,
    rebalance_dates,
    run_backtest,
)


# --------------- helpers ---------------

def _dates(specs: list[str]) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.to_datetime(specs))


def _panel(data: dict[int, list[float | None]], dates: list[str]) -> pd.DataFrame:
    idx = _dates(dates)
    return pd.DataFrame(data, index=idx, dtype="float64")


# ============================================================
# 1. shift(1) 对齐：t 日权重赚 t+1 日收益
# ============================================================

class TestShiftAlignment:
    """权重→收益对齐是回测最核心的 PIT 不变量。"""

    def test_weight_earns_next_day_return(self):
        """t=0 建仓，t=1 收益必须反映 t=0→t=1 的价格变化。"""
        prices = _panel({1: [100.0, 110.0, 121.0]}, ["2026-01-01", "2026-01-02", "2026-01-03"])
        # 全仓持有 security 1
        weights = _panel({1: [1.0, 1.0, 0.0]}, ["2026-01-01", "2026-01-02", "2026-01-03"])

        result = run_backtest("test", weights, prices, cost_bps=0.0)

        # t=0 的权重 shift(1) -> t=1 才生效
        # t=1: held=w[t=0]=1.0, return=(110-100)/100=0.10
        # t=2: held=w[t=1]=1.0, return=(121-110)/110=0.10
        # t=0: held=NaN->0 (first day, no return from shift)
        assert abs(result.daily_returns.iloc[1] - 0.10) < 1e-9
        assert abs(result.daily_returns.iloc[2] - 0.10) < 1e-9

    def test_day_zero_return_is_zero(self):
        """第一天不应有收益（没有前一天的权重）。"""
        prices = _panel({1: [100.0, 110.0]}, ["2026-01-01", "2026-01-02"])
        weights = _panel({1: [1.0, 1.0]}, ["2026-01-01", "2026-01-02"])

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # day 0: shift(1) gives NaN -> fillna(0) -> held=0 -> gross=0
        assert result.daily_returns.iloc[0] == 0.0

    def test_no_same_day_lookahead(self):
        """t 日才决定的权重不能赚 t 日的收益（那样就是看了当天收盘再"建仓"）。"""
        prices = _panel({1: [100.0, 200.0, 200.0]}, ["2026-01-01", "2026-01-02", "2026-01-03"])
        # 在 t=1（涨了 100%）时才建仓
        weights = _panel({1: [0.0, 1.0, 0.0]}, ["2026-01-01", "2026-01-02", "2026-01-03"])

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # t=1: held=w[0]=0, 不赚这 100%
        assert result.daily_returns.iloc[1] == 0.0
        # t=2: held=w[1]=1.0, return=(200-200)/200=0
        assert result.daily_returns.iloc[2] == 0.0


# ============================================================
# 2. 换手与成本
# ============================================================

class TestTurnoverAndCost:

    def test_turnover_calculation(self):
        """换手 = sum(|w_t - w_{t-1}|)。"""
        prices = _panel({1: [100.0, 100.0, 100.0, 100.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
        weights = _panel({1: [0.0, 1.0, 0.5, 0.0]},
                         ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # turnover[0]=|0-0|=0 (no prior weight)
        # turnover[1]=|1-0|=1
        # turnover[2]=|0.5-1|=0.5
        # turnover[3]=|0-0.5|=0.5
        assert abs(result.turnover.iloc[0]) < 1e-9
        assert abs(result.turnover.iloc[1] - 1.0) < 1e-9
        assert abs(result.turnover.iloc[2] - 0.5) < 1e-9
        assert abs(result.turnover.iloc[3] - 0.5) < 1e-9

    def test_cost_reduces_return(self):
        """net = gross - turnover * bps / 10000."""
        prices = _panel({1: [100.0, 110.0, 110.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03"])
        weights = _panel({1: [1.0, 0.0, 0.0]},
                         ["2026-01-01", "2026-01-02", "2026-01-03"])

        result_free = run_backtest("free", weights, prices, cost_bps=0.0)
        result_paid = run_backtest("paid", weights, prices, cost_bps=100.0)

        # t=1: gross return = 10%, turnover = |0-1|=1 (from w[0]=1 to w[1]=0? No.)
        # Actually: turnover is computed from weights (signal), not held.
        # turnover[1] = |w[1]-w[0]| = |0-1| = 1
        # cost = 1 * 100/10000 = 0.01
        # net = 0.10 - 0.01 = 0.09
        assert abs(result_free.daily_returns.iloc[1] - 0.10) < 1e-9
        assert abs(result_paid.daily_returns.iloc[1] - 0.09) < 1e-9


# ============================================================
# 3. 停牌/缺口收益恢复
# ============================================================

class TestGapRecovery:

    def test_gap_return_on_reprice_day(self):
        """停牌缺口在复牌日一次性计入跨缺口收益。"""
        returns = _returns_with_gap_recovery(
            _panel({1: [100.0, None, None, 80.0]},
                   ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
        )
        # t=1, t=2: NaN (停牌)
        assert pd.isna(returns.iloc[1, 0])
        assert pd.isna(returns.iloc[2, 0])
        # t=3 复牌: (80-100)/100 = -0.20
        assert abs(returns.iloc[3, 0] - (-0.20)) < 1e-9

    def test_normal_return_unaffected(self):
        """连续有价时正常 pct_change。"""
        returns = _returns_with_gap_recovery(
            _panel({1: [100.0, 105.0, 110.25]},
                   ["2026-01-01", "2026-01-02", "2026-01-03"])
        )
        assert abs(returns.iloc[1, 0] - 0.05) < 1e-9
        assert abs(returns.iloc[2, 0] - 0.05) < 1e-9


# ============================================================
# 4. hold_through_gaps 冻结停牌期权重
# ============================================================

class TestHoldThroughGaps:

    def test_frozen_weight_captures_gap_return(self):
        """策略在停牌期把权重清零，但冻结机制让复牌跳空仍作用于入缺口时的仓位。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, None, None, 60.0]}, dates)
        # 策略 t=0 全仓，t=1 起清零（停牌时策略决定不持有）
        weights = _panel({1: [1.0, 0.0, 0.0, 0.0]}, dates)

        result_hold = run_backtest("hold", weights, prices, cost_bps=0.0, hold_through_gaps=True)
        result_no = run_backtest("no_hold", weights, prices, cost_bps=0.0, hold_through_gaps=False)

        # 复牌日 t=3: (60-100)/100 = -0.40
        # hold_through: 冻结 held=1.0 -> gross = 1.0 * (-0.40) = -0.40
        assert abs(result_hold.daily_returns.iloc[3] - (-0.40)) < 1e-9
        # no hold: held=weights.shift(1)[t=3]=w[2]=0.0 -> gross = 0 * (-0.40) = 0
        assert result_no.daily_returns.iloc[3] == 0.0

    def test_non_gap_unaffected(self):
        """连续有价的格子不受 hold_through_gaps 影响。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03"]
        prices = _panel({1: [100.0, 110.0, 121.0]}, dates)
        weights = _panel({1: [1.0, 1.0, 0.0]}, dates)

        r_hold = run_backtest("h", weights, prices, cost_bps=0.0, hold_through_gaps=True)
        r_no = run_backtest("n", weights, prices, cost_bps=0.0, hold_through_gaps=False)

        pd.testing.assert_series_equal(r_hold.daily_returns, r_no.daily_returns, check_names=False)


# ============================================================
# 5. terminal_missing_position_days
# ============================================================

class TestTerminalMissing:

    def test_counts_permanent_missing(self):
        """持仓后价格永久消失的 security-day 计数。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, 110.0, None, None]}, dates)
        # held = weights.shift(1): t=1 held=1, t=2 held=1, t=3 held=0
        weights = _panel({1: [1.0, 1.0, 0.0, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # t=2: held=w[1]=1, price=NaN, never recovers -> 1 day
        # t=3: held=w[2]=0 -> not counted
        assert result.terminal_missing_position_days == 1

    def test_temporary_gap_not_counted(self):
        """价格暂时缺失但之后恢复的不计入 terminal。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, None, None, 80.0]}, dates)
        weights = _panel({1: [1.0, 1.0, 1.0, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        assert result.terminal_missing_position_days == 0


# ============================================================
# 5b. terminal_return 退市收益政策
# ============================================================

class TestTerminalReturn:

    def test_terminal_return_injects_loss_on_delist(self):
        """terminal_return=-1.0 时，退市当日为永久缺失持仓注入 -100% 收益。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, 110.0, None, None]}, dates)
        # held = weights.shift(1): t=2 held=w[1]=1（退市当日仍持有）
        weights = _panel({1: [1.0, 1.0, 0.0, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0, terminal_return=-1.0)

        # t=1: 正常收益 (110-100)/100 = 0.10
        assert abs(result.daily_returns.iloc[1] - 0.10) < 1e-9
        # t=2: 价格首次永久缺失，held=1 -> 注入 -1.0 * 1 = -1.0
        assert abs(result.daily_returns.iloc[2] - (-1.0)) < 1e-9
        # t=3: 仍缺失但非"第一天"，不重复注入
        assert result.daily_returns.iloc[3] == 0.0
        # equity 反映总损失：(1+0)(1.10)(1-1.0)(1+0) = 0
        assert abs(result.equity.iloc[-1]) < 1e-9

    def test_terminal_return_none_preserves_current_behavior(self):
        """terminal_return=None（默认）保持旧口径：永久缺失持仓静默赚 0%。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, 110.0, None, None]}, dates)
        weights = _panel({1: [1.0, 1.0, 0.0, 0.0]}, dates)

        default = run_backtest("default", weights, prices, cost_bps=0.0)
        explicit_none = run_backtest("none", weights, prices, cost_bps=0.0, terminal_return=None)

        # 退市日收益为 0（旧口径）
        assert default.daily_returns.iloc[2] == 0.0
        assert default.daily_returns.iloc[3] == 0.0
        pd.testing.assert_series_equal(
            default.daily_returns, explicit_none.daily_returns, check_names=False
        )

    def test_terminal_return_only_when_held(self):
        """退市时未持仓（held=0）则不注入收益。"""
        dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]
        prices = _panel({1: [100.0, 110.0, None, None]}, dates)
        # t=2 held=w[1]=0：退市前已清仓
        weights = _panel({1: [1.0, 0.0, 0.0, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0, terminal_return=-1.0)
        assert result.daily_returns.iloc[2] == 0.0
        assert result.daily_returns.iloc[3] == 0.0


# ============================================================
# 6. equity 曲线一致性
# ============================================================

class TestEquity:

    def test_equity_matches_cumprod(self):
        """equity = (1 + daily_returns).cumprod()。"""
        prices = _panel({1: [100.0, 110.0, 99.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03"])
        weights = _panel({1: [1.0, 1.0, 0.0]},
                         ["2026-01-01", "2026-01-02", "2026-01-03"])

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        expected = (1 + result.daily_returns).cumprod()
        pd.testing.assert_series_equal(result.equity, expected, check_names=False)


# ============================================================
# 7. BacktestResult.metrics 基本语义
# ============================================================

class TestMetrics:

    def test_metrics_keys(self):
        """metrics 返回预期的键集合。"""
        prices = _panel({1: [100.0, 110.0]}, ["2026-01-01", "2026-01-02"])
        weights = _panel({1: [1.0, 0.0]}, ["2026-01-01", "2026-01-02"])
        result = run_backtest("test", weights, prices, cost_bps=0.0)
        m = result.metrics()
        expected_keys = {"total_return", "cagr", "ann_vol", "sharpe", "max_drawdown",
                         "ann_turnover", "avg_positions", "terminal_missing_position_days"}
        assert set(m.keys()) == expected_keys

    def test_zero_return_gives_zero_total(self):
        """价格不动，收益为零。"""
        prices = _panel({1: [100.0, 100.0, 100.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03"])
        weights = _panel({1: [1.0, 1.0, 0.0]},
                         ["2026-01-01", "2026-01-02", "2026-01-03"])
        result = run_backtest("test", weights, prices, cost_bps=0.0)
        assert abs(result.metrics()["total_return"]) < 1e-9


# ============================================================
# 8. 多证券交叉持仓
# ============================================================

class TestMultiSecurity:

    def test_two_securities_weighted(self):
        """同时持有两只，收益是加权平均。"""
        dates = ["2026-01-01", "2026-01-02"]
        prices = _panel({1: [100.0, 110.0], 2: [200.0, 220.0]}, dates)
        # 50/50 权重
        weights = _panel({1: [0.5, 0.0], 2: [0.5, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # t=1: held=[0.5, 0.5], returns=[0.10, 0.10], gross=0.10
        assert abs(result.daily_returns.iloc[1] - 0.10) < 1e-9

    def test_asymmetric_weights(self):
        """不等权重时收益按权重加总。"""
        dates = ["2026-01-01", "2026-01-02"]
        prices = _panel({1: [100.0, 120.0], 2: [100.0, 90.0]}, dates)
        # 70% sec1, 30% sec2
        weights = _panel({1: [0.7, 0.0], 2: [0.3, 0.0]}, dates)

        result = run_backtest("test", weights, prices, cost_bps=0.0)
        # t=1: 0.7*0.20 + 0.3*(-0.10) = 0.14 - 0.03 = 0.11
        assert abs(result.daily_returns.iloc[1] - 0.11) < 1e-9


# ============================================================
# 9. rebalance_dates 和 hold_between_rebalances
# ============================================================

class TestRebalanceUtils:

    def test_rebalance_dates_monthly(self):
        """月频再平衡取每月最后一个交易日。"""
        idx = _dates(["2026-01-15", "2026-01-31", "2026-02-15", "2026-02-28"])
        result = rebalance_dates(idx, "M")
        assert list(result) == [pd.Timestamp("2026-01-31"), pd.Timestamp("2026-02-28")]

    def test_hold_between_fills_forward(self):
        """再平衡日之间的权重是 ffill。"""
        idx = _dates(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
        reb_weights = pd.DataFrame({1: [0.5, 1.0]},
                                   index=_dates(["2026-01-01", "2026-01-03"]))
        filled = hold_between_rebalances(reb_weights, idx)
        assert filled.loc["2026-01-01", 1] == 0.5
        assert filled.loc["2026-01-02", 1] == 0.5  # ffill
        assert filled.loc["2026-01-03", 1] == 1.0
        assert filled.loc["2026-01-04", 1] == 1.0  # ffill


# ============================================================
# 10. evaluate 层 forward_return 语义
# ============================================================

class TestForwardReturn:

    def test_forward_return_horizon_1(self):
        """horizon=1 前向收益 = 次日收盘 / 当日收盘 - 1。"""
        from research.evaluate import _forward_return
        prices = _panel({1: [100.0, 110.0, 99.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03"])
        fwd = _forward_return(prices, 1)
        assert abs(fwd.iloc[0, 0] - 0.10) < 1e-9
        assert abs(fwd.iloc[1, 0] - (-0.10)) < 1e-9
        assert pd.isna(fwd.iloc[2, 0])  # 最后一行没有 t+1

    def test_forward_return_horizon_2(self):
        """horizon=2 前向收益 = t+2 / t - 1。"""
        from research.evaluate import _forward_return
        prices = _panel({1: [100.0, 110.0, 121.0, 133.1]},
                        ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
        fwd = _forward_return(prices, 2)
        assert abs(fwd.iloc[0, 0] - 0.21) < 1e-9
        assert abs(fwd.iloc[1, 0] - 0.21) < 1e-9
        assert pd.isna(fwd.iloc[2, 0])
        assert pd.isna(fwd.iloc[3, 0])

    def test_forward_return_with_gap(self):
        """价格缺口 ffill 后再算前向收益；原始 NaN 位置返回 NaN。"""
        from research.evaluate import _forward_return
        prices = _panel({1: [100.0, None, 120.0]},
                        ["2026-01-01", "2026-01-02", "2026-01-03"])
        fwd = _forward_return(prices, 1)
        # t=0: adj_close=100, not NaN; shift(-1)=ffill[1]=100, but adj_close[1]=NaN so shifted notna check...
        # valid_pair = adj_close.notna() & shifted.notna()
        # t=0: adj_close[0]=100 notna, shifted=ffill.shift(-1)[0]=ffill[1]=100 -> shifted notna(100)=True -> valid
        # But shifted = filled.shift(-1) where filled = ffill -> filled=[100,100,120], shifted=[100,120,NaN]
        # t=0: price=100, shifted=100 -> fwd=0.0; valid_pair: adj[0]=notna, shifted[0]=notna -> yes
        assert abs(fwd.iloc[0, 0] - 0.0) < 1e-9
        # t=1: adj_close=NaN -> valid_pair=False -> NaN
        assert pd.isna(fwd.iloc[1, 0])


# ============================================================
# 11. rank-IC 基本不变量
# ============================================================

class TestRankIC:

    def test_perfect_positive_ic(self):
        """因子与收益完全正相关时 IC=1。"""
        from research.evaluate import _rank_ic_series
        idx = _dates(["2026-01-01"])
        cols = list(range(1, 101))
        # 因子 = 1..100, 收益也 = 1..100 -> perfect rank correlation
        factor = pd.DataFrame([list(range(1, 101))], index=idx, columns=cols, dtype="float64")
        ret = pd.DataFrame([list(range(1, 101))], index=idx, columns=cols, dtype="float64")
        ic = _rank_ic_series(factor, ret, min_coverage=10)
        assert abs(ic.iloc[0] - 1.0) < 1e-9

    def test_perfect_negative_ic(self):
        """因子与收益完全反向时 IC=-1。"""
        from research.evaluate import _rank_ic_series
        idx = _dates(["2026-01-01"])
        cols = list(range(1, 101))
        factor = pd.DataFrame([list(range(1, 101))], index=idx, columns=cols, dtype="float64")
        ret = pd.DataFrame([list(range(100, 0, -1))], index=idx, columns=cols, dtype="float64")
        ic = _rank_ic_series(factor, ret, min_coverage=10)
        assert abs(ic.iloc[0] - (-1.0)) < 1e-9

    def test_insufficient_coverage_returns_nan(self):
        """覆盖不足时 IC=NaN。"""
        from research.evaluate import _rank_ic_series
        idx = _dates(["2026-01-01"])
        factor = pd.DataFrame([[1.0, 2.0]], index=idx, columns=[1, 2])
        ret = pd.DataFrame([[0.1, 0.2]], index=idx, columns=[1, 2])
        ic = _rank_ic_series(factor, ret, min_coverage=100)
        assert pd.isna(ic.iloc[0])
