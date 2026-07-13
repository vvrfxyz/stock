"""Wave 13 路径动量因子：时间边界、OLS 恒等式与完整形成窗。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.factors.builtins.path_momentum import (
    FORMATION_OBS,
    information_discreteness_from_prices,
    rolling_market_residual_momentum,
)
from research.factors.protocol import get


def _explicit_signal(
    returns: pd.Series,
    market: pd.Series,
    *,
    row: int,
    estimation_window: int,
    min_estimation_obs: int,
    formation_start_lag: int,
    formation_end_lag: int,
) -> float:
    residuals = []
    for target in range(row - formation_start_lag, row - formation_end_lag + 1):
        start = max(0, target - estimation_window)
        y = returns.iloc[start:target]
        x = market.iloc[start:target]
        valid = y.notna() & x.notna()
        if int(valid.sum()) < min_estimation_obs:
            return np.nan
        design = np.column_stack(
            [np.ones(int(valid.sum())), x.loc[valid].to_numpy(dtype="float64")]
        )
        alpha, beta = np.linalg.lstsq(
            design,
            y.loc[valid].to_numpy(dtype="float64"),
            rcond=None,
        )[0]
        if pd.isna(returns.iloc[target]) or pd.isna(market.iloc[target]):
            return np.nan
        residuals.append(returns.iloc[target] - alpha - beta * market.iloc[target])
    return float(np.sum(residuals))


def test_residual_momentum_matches_explicit_rolling_ols():
    rng = np.random.default_rng(13)
    dates = pd.bdate_range("2024-01-02", periods=90)
    market = pd.Series(rng.normal(0.0004, 0.012, len(dates)), index=dates)
    returns = pd.DataFrame(
        {
            1: 0.0002 + 1.3 * market + rng.normal(0, 0.006, len(dates)),
            2: -0.0001 + 0.7 * market + rng.normal(0, 0.008, len(dates)),
        },
        index=dates,
    )
    market.iloc[0] = np.nan
    returns.iloc[0] = np.nan
    kwargs = {
        "estimation_window": 20,
        "min_estimation_obs": 15,
        "formation_start_lag": 12,
        "formation_end_lag": 5,
        "min_formation_obs": 8,
        "block_size": 1,
    }

    actual = rolling_market_residual_momentum(returns, market, **kwargs)
    row = 70
    for security_id in returns:
        expected = _explicit_signal(
            returns[security_id],
            market,
            row=row,
            estimation_window=20,
            min_estimation_obs=15,
            formation_start_lag=12,
            formation_end_lag=5,
        )
        assert actual.iloc[row][security_id] == pytest.approx(expected, abs=1e-12)


def test_residual_momentum_skips_the_most_recent_21_days():
    rng = np.random.default_rng(21)
    dates = pd.bdate_range("2022-01-03", periods=540)
    market = pd.Series(rng.normal(0.0005, 0.01, len(dates)), index=dates)
    returns = pd.DataFrame(
        {1: 0.0001 + 0.9 * market + rng.normal(0, 0.005, len(dates))},
        index=dates,
    )
    changed = returns.copy()
    changed.iloc[-21:, 0] += 0.25

    baseline = rolling_market_residual_momentum(returns, market).iloc[-1, 0]
    modified = rolling_market_residual_momentum(changed, market).iloc[-1, 0]

    assert modified == pytest.approx(baseline, abs=1e-12)


def test_residual_momentum_removes_a_pure_beta_trend():
    rng = np.random.default_rng(8)
    dates = pd.bdate_range("2022-01-03", periods=540)
    market = pd.Series(rng.normal(0.001, 0.006, len(dates)), index=dates)
    returns = pd.DataFrame({1: 0.0003 + 1.8 * market}, index=dates)

    signal = rolling_market_residual_momentum(returns, market).iloc[-1, 0]
    raw_formation_return = returns.iloc[-252:-21, 0].sum()

    assert raw_formation_return > 0.1
    assert signal == pytest.approx(0.0, abs=1e-12)


def test_residual_momentum_requires_every_formation_day():
    rng = np.random.default_rng(5)
    dates = pd.bdate_range("2022-01-03", periods=540)
    market = pd.Series(rng.normal(0, 0.01, len(dates)), index=dates)
    returns = pd.DataFrame(
        {1: 0.4 * market + rng.normal(0, 0.006, len(dates))},
        index=dates,
    )
    returns.iloc[-100, 0] = np.nan

    signal = rolling_market_residual_momentum(returns, market).iloc[-1, 0]

    assert np.isnan(signal)


def test_information_discreteness_counts_zero_days_in_fixed_denominator():
    formation_returns = np.array([0.01] * 10 + [-0.01] * 5 + [0.0] * 216)
    recent_returns = np.zeros(21)
    all_returns = np.concatenate(([0.0], formation_returns, recent_returns))
    prices = 100 * np.cumprod(1 + all_returns)
    frame = pd.DataFrame({1: prices}, index=pd.bdate_range("2024-01-02", periods=253))

    signal = information_discreteness_from_prices(frame).iloc[-1, 0]

    assert len(formation_returns) == FORMATION_OBS
    assert signal == pytest.approx((5 - 10) / FORMATION_OBS)


def test_path_momentum_factor_registration():
    residual = get("residual_momentum_12_1")
    discreteness = get("information_discreteness_12_1")

    assert residual.lookback_days == 504
    assert residual.lag_days == 1 and residual.pit_guarantee is True
    assert discreteness.lookback_days == 252
    assert discreteness.lag_days == 1 and discreteness.pit_guarantee is True
