"""Wave 13：市场残差动量与信息离散度。

`residual_momentum_12_1` 是日频、单因子市场模型的事前创新版本：每个交易日只用
此前 252 个配对有效收益估计 alpha/beta，再累计与普通 12-1 动量完全相同形成窗内的
一步外推残差。它不是 Blitz-Huij-Martens (2011) 月频 FF3 版本的直接复制。

`information_discreteness_12_1` 严格采用 Da-Gurun-Warachka (2014) 的基准定义：
    ID = sign(PRET) * (% negative days - % positive days)
低 ID 表示连续信息，高 ID 表示离散信息。ID 只供条件双排序研究使用，不应作为
无条件单调 rank-IC 因子解释。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.price_cache import adjusted_close_panel
from research.factors.protocol import FactorContext, register


ESTIMATION_WINDOW = 252
MIN_ESTIMATION_OBS = 126
FORMATION_START_LAG = 251
FORMATION_END_LAG = 21
FORMATION_OBS = FORMATION_START_LAG - FORMATION_END_LAG + 1


def _previous_window_sum(values: np.ndarray, window: int) -> np.ndarray:
    """逐列计算不含当前行的前 window 行和。输入须已把无效值置零。"""
    rows, cols = values.shape
    cumulative = np.empty((rows + 1, cols), dtype="float64")
    cumulative[0] = 0.0
    np.cumsum(values, axis=0, out=cumulative[1:])
    ends = np.arange(rows)
    starts = np.maximum(ends - window, 0)
    return cumulative[ends] - cumulative[starts]


def _formation_window_sum(
    values: np.ndarray,
    *,
    start_lag: int,
    end_lag: int,
    min_obs: int,
) -> np.ndarray:
    """累计每行对应的 [t-start_lag, t-end_lag] 闭区间，并执行有效数门槛。"""
    rows, cols = values.shape
    valid = np.isfinite(values)
    clean = np.where(valid, values, 0.0)

    cumulative = np.empty((rows + 1, cols), dtype="float64")
    cumulative[0] = 0.0
    np.cumsum(clean, axis=0, out=cumulative[1:])
    counts = np.empty((rows + 1, cols), dtype="int32")
    counts[0] = 0
    np.cumsum(valid, axis=0, dtype="int32", out=counts[1:])

    out = np.full((rows, cols), np.nan, dtype="float64")
    row_ids = np.arange(rows)
    starts = row_ids - start_lag
    ends = row_ids - end_lag + 1
    usable = starts >= 0
    if not usable.any():
        return out
    target = row_ids[usable]
    summed = cumulative[ends[usable]] - cumulative[starts[usable]]
    observed = counts[ends[usable]] - counts[starts[usable]]
    out[target] = np.where(observed >= min_obs, summed, np.nan)
    return out


def rolling_market_residual_momentum(
    returns: pd.DataFrame,
    market_return: pd.Series,
    *,
    estimation_window: int = ESTIMATION_WINDOW,
    min_estimation_obs: int = MIN_ESTIMATION_OBS,
    formation_start_lag: int = FORMATION_START_LAG,
    formation_end_lag: int = FORMATION_END_LAG,
    min_formation_obs: int = FORMATION_OBS,
    block_size: int = 256,
) -> pd.DataFrame:
    """块状计算滚动市场模型的一步外推残差 12-1 累计值。"""
    if block_size <= 0:
        raise ValueError("block_size must be positive")
    if formation_start_lag < formation_end_lag:
        raise ValueError("formation_start_lag must be >= formation_end_lag")

    market = market_return.reindex(returns.index).to_numpy(dtype="float64")
    market_valid = np.isfinite(market)
    rows, cols = returns.shape
    output = np.full((rows, cols), np.nan, dtype="float64")

    for left in range(0, cols, block_size):
        right = min(left + block_size, cols)
        y = returns.iloc[:, left:right].to_numpy(dtype="float64")
        valid = np.isfinite(y) & market_valid[:, None]
        n = _previous_window_sum(valid.astype("float64"), estimation_window)

        y_clean = np.where(valid, y, 0.0)
        x_clean = np.where(valid, market[:, None], 0.0)
        sum_y = _previous_window_sum(y_clean, estimation_window)
        sum_x = _previous_window_sum(x_clean, estimation_window)
        sum_yx = _previous_window_sum(y_clean * x_clean, estimation_window)
        sum_x2 = _previous_window_sum(x_clean * x_clean, estimation_window)

        with np.errstate(invalid="ignore", divide="ignore"):
            mean_y = sum_y / n
            mean_x = sum_x / n
            var_x = sum_x2 / n - mean_x * mean_x
            cov_yx = sum_yx / n - mean_y * mean_x
            beta = cov_yx / var_x
            alpha = mean_y - beta * mean_x
            residual = y - alpha - beta * market[:, None]
        residual[(n < min_estimation_obs) | (var_x <= 1e-18) | ~valid] = np.nan

        output[:, left:right] = _formation_window_sum(
            residual,
            start_lag=formation_start_lag,
            end_lag=formation_end_lag,
            min_obs=min_formation_obs,
        )

    return pd.DataFrame(output, index=returns.index, columns=returns.columns, dtype="float64")


def information_discreteness_from_prices(
    adjusted_close: pd.DataFrame,
    *,
    formation_start_lag: int = FORMATION_START_LAG,
    formation_end_lag: int = FORMATION_END_LAG,
    min_formation_obs: int = FORMATION_OBS,
) -> pd.DataFrame:
    """从复权价格计算 Da-Gurun-Warachka information discreteness。"""
    window = formation_start_lag - formation_end_lag + 1
    returns = adjusted_close.pct_change(fill_method=None)
    lagged = returns.shift(formation_end_lag)
    observed = lagged.notna().rolling(window, min_periods=1).sum()
    positive = lagged.gt(0).rolling(window, min_periods=1).sum()
    negative = lagged.lt(0).rolling(window, min_periods=1).sum()
    pret = adjusted_close.shift(formation_end_lag) / adjusted_close.shift(formation_start_lag + 1) - 1
    signal = np.sign(pret) * (negative - positive) / observed
    return signal.where(observed >= min_formation_obs).astype("float64")


@dataclass(frozen=True)
class ResidualMomentum12to1Factor:
    name: ClassVar[str] = "residual_momentum_12_1"
    lookback_days: ClassVar[int] = 504
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    estimation_window: int = ESTIMATION_WINDOW
    min_estimation_obs: int = MIN_ESTIMATION_OBS
    min_formation_obs: int = FORMATION_OBS
    block_size: int = 256

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adjusted_close = adjusted_close_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
            buffer_days=800,
        )
        returns = adjusted_close.pct_change(fill_method=None)
        market = returns.mean(axis=1)
        signal = rolling_market_residual_momentum(
            returns,
            market,
            estimation_window=self.estimation_window,
            min_estimation_obs=self.min_estimation_obs,
            min_formation_obs=self.min_formation_obs,
            block_size=self.block_size,
        )
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


@dataclass(frozen=True)
class InformationDiscreteness12to1Factor:
    name: ClassVar[str] = "information_discreteness_12_1"
    lookback_days: ClassVar[int] = 252
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    min_formation_obs: int = FORMATION_OBS

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        adjusted_close = adjusted_close_panel(
            ctx.engine,
            dates=ctx.dates,
            security_ids=ctx.security_universe.tolist(),
            buffer_days=420,
        )
        signal = information_discreteness_from_prices(
            adjusted_close,
            min_formation_obs=self.min_formation_obs,
        )
        return signal.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(ResidualMomentum12to1Factor())
register(InformationDiscreteness12to1Factor())
