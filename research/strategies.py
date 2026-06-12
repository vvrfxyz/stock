"""经典技术分析基线策略：信号 -> 目标权重矩阵。

所有函数输入宽表（index=date, columns=security_id），输出同形权重矩阵；
权重约定见 research.backtest（t 日权重赚 t+1 日收益，引擎负责 shift）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from research.backtest import hold_between_rebalances, rebalance_dates


def _equal_weight(selected: pd.DataFrame) -> pd.DataFrame:
    counts = selected.sum(axis=1)
    return selected.div(counts.where(counts > 0), axis=0).fillna(0.0)


def momentum_12_1(
    adj_close: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    top_frac: float = 0.1,
    freq: str = "M",
) -> pd.DataFrame:
    """12-1 横截面动量：按 t-252 到 t-21 的收益排序，做多前 top_frac，月度再平衡。"""
    signal = adj_close.shift(21) / adj_close.shift(252) - 1
    dates = rebalance_dates(adj_close.index, freq)
    sig = signal.loc[dates].where(eligible.loc[dates])
    rank = sig.rank(axis=1, pct=True)
    selected = (rank >= 1 - top_frac).astype(float)
    return hold_between_rebalances(_equal_weight(selected), adj_close.index)


def sma_trend(
    adj_close: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    fast: int = 50,
    slow: int = 200,
    freq: str = "W",
) -> pd.DataFrame:
    """时序双均线：SMA(fast) > SMA(slow) 的标的等权做多，周度再平衡。"""
    sma_fast = adj_close.rolling(fast, min_periods=fast).mean()
    sma_slow = adj_close.rolling(slow, min_periods=slow).mean()
    in_trend = (sma_fast > sma_slow) & eligible
    dates = rebalance_dates(adj_close.index, freq)
    selected = in_trend.loc[dates].astype(float)
    return hold_between_rebalances(_equal_weight(selected), adj_close.index)


def short_term_reversal(
    adj_close: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    lookback: int = 5,
    bottom_frac: float = 0.1,
    freq: str = "W",
) -> pd.DataFrame:
    """短期反转：做多近 lookback 日跌幅最大的 bottom_frac，周度再平衡。"""
    signal = adj_close.pct_change(lookback, fill_method=None)
    dates = rebalance_dates(adj_close.index, freq)
    sig = signal.loc[dates].where(eligible.loc[dates])
    rank = sig.rank(axis=1, pct=True)
    selected = (rank <= bottom_frac).astype(float)
    return hold_between_rebalances(_equal_weight(selected), adj_close.index)


def buy_and_hold(adj_close: pd.DataFrame, column) -> pd.DataFrame:
    """单标的买入持有（基准）。"""
    weights = pd.DataFrame(0.0, index=adj_close.index, columns=adj_close.columns)
    has_data = adj_close[column].notna()
    weights.loc[has_data, column] = 1.0
    return weights
