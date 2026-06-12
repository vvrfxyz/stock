"""极简向量化回测引擎（日频、横截面权重矩阵）。

约定：
- weights.loc[t] 是用 t 日收盘信息决定、在 t 日收盘建立的目标权重；
  它赚取 t+1 日的收益（内部用 weights.shift(1) 对齐，调用方不要自己 shift）。
- 成本按换手 × cost_bps 双边计：turnover_t = sum(|w_t - w_{t-1}|)。
- 长表里的 NaN 收益视为 0（停牌/退市后无数据，相当于头寸冻结后清零）。

这是研究原型，不建模盘中滑点、做空费率、权重漂移再平衡。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

TRADING_DAYS = 252


@dataclass
class BacktestResult:
    name: str
    daily_returns: pd.Series = field(repr=False)
    equity: pd.Series = field(repr=False)
    turnover: pd.Series = field(repr=False)
    avg_positions: float = 0.0

    def metrics(self) -> dict[str, float]:
        r = self.daily_returns.dropna()
        if r.empty:
            return {}
        years = len(r) / TRADING_DAYS
        total = float(self.equity.iloc[-1] / self.equity.iloc[0] - 1)
        cagr = float((1 + total) ** (1 / years) - 1) if years > 0 else np.nan
        vol = float(r.std() * np.sqrt(TRADING_DAYS))
        sharpe = float(r.mean() / r.std() * np.sqrt(TRADING_DAYS)) if r.std() > 0 else np.nan
        dd = self.equity / self.equity.cummax() - 1
        return {
            "total_return": total,
            "cagr": cagr,
            "ann_vol": vol,
            "sharpe": sharpe,
            "max_drawdown": float(dd.min()),
            "ann_turnover": float(self.turnover.mean() * TRADING_DAYS),
            "avg_positions": self.avg_positions,
        }


def run_backtest(
    name: str,
    weights: pd.DataFrame,
    adj_close: pd.DataFrame,
    *,
    cost_bps: float = 10.0,
) -> BacktestResult:
    returns = adj_close.pct_change(fill_method=None)
    weights = weights.reindex(index=returns.index, columns=returns.columns).fillna(0.0)

    held = weights.shift(1).fillna(0.0)
    gross = (held * returns.fillna(0.0)).sum(axis=1)
    turnover = (weights - weights.shift(1).fillna(0.0)).abs().sum(axis=1)
    cost = turnover * cost_bps / 10_000
    net = gross - cost

    equity = (1 + net).cumprod()
    avg_positions = float((weights > 0).sum(axis=1).replace(0, np.nan).mean())
    return BacktestResult(
        name=name,
        daily_returns=net,
        equity=equity,
        turnover=turnover,
        avg_positions=avg_positions,
    )


def eligibility_mask(
    close: pd.DataFrame,
    dollar_volume: pd.DataFrame,
    *,
    min_price: float = 3.0,
    min_median_dollar_volume: float = 2_000_000.0,
    window: int = 63,
) -> pd.DataFrame:
    """逐日可交易性掩码：近 window 日中位成交额与最新原始价格双门槛。"""
    med_dv = dollar_volume.rolling(window, min_periods=window).median()
    return (med_dv >= min_median_dollar_volume) & (close >= min_price)


def rebalance_dates(index: pd.DatetimeIndex, freq: str) -> pd.DatetimeIndex:
    """从交易日索引取每期最后一个交易日（freq: 'M' / 'W'）。"""
    s = pd.Series(index, index=index)
    return pd.DatetimeIndex(s.groupby(index.to_period(freq)).last())


def hold_between_rebalances(weights_at_rebalance: pd.DataFrame, index: pd.DatetimeIndex) -> pd.DataFrame:
    """把再平衡日的目标权重前向填充到每个交易日。"""
    return weights_at_rebalance.reindex(index).ffill().fillna(0.0)
