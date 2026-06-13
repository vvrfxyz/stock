import numpy as np
import pandas as pd
import pytest

from research.backtest import run_backtest


def test_backtest_recovers_return_across_temporary_price_gap():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    adj_close = pd.DataFrame({1: [100.0, np.nan, np.nan, 40.0]}, index=dates)
    weights = pd.DataFrame({1: [1.0, 1.0, 1.0, 1.0]}, index=dates)

    result = run_backtest("gap", weights, adj_close, cost_bps=0)

    assert result.daily_returns.loc["2024-01-03"] == 0.0
    assert result.daily_returns.loc["2024-01-04"] == 0.0
    assert result.daily_returns.loc["2024-01-05"] == -0.6


def test_backtest_reports_terminal_missing_positions():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    adj_close = pd.DataFrame({1: [100.0, 90.0, np.nan, np.nan]}, index=dates)
    weights = pd.DataFrame({1: [1.0, 1.0, 1.0, 1.0]}, index=dates)

    result = run_backtest("terminal", weights, adj_close, cost_bps=0)

    assert result.daily_returns.loc["2024-01-03"] == pytest.approx(-0.1)
    assert result.terminal_missing_position_days == 2
    assert result.metrics()["terminal_missing_position_days"] == 2.0


def test_gap_recovery_not_dropped_when_weight_zeroed_during_suspension():
    # 持有 -> 停牌期间策略把权重清零 -> 复牌：跨缺口收益必须作用在停牌前的仓位上。
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])
    adj_close = pd.DataFrame({1: [100.0, np.nan, np.nan, 40.0]}, index=dates)
    # t0 建仓 1.0；停牌期间(t1,t2)权重被清零；复牌日 t3 仍为 0
    weights = pd.DataFrame({1: [1.0, 0.0, 0.0, 0.0]}, index=dates)

    # 默认 hold_through_gaps=True：冻结停牌前仓位，复牌日 -60% 计入
    held = run_backtest("frozen", weights, adj_close, cost_bps=0)
    assert held.daily_returns.loc["2024-01-05"] == pytest.approx(-0.6)

    # 关闭后复现旧口径：held 在复牌日为 0，收益被吞掉
    dropped = run_backtest("dropped", weights, adj_close, cost_bps=0, hold_through_gaps=False)
    assert dropped.daily_returns.loc["2024-01-05"] == 0.0
