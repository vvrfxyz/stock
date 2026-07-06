"""极简向量化回测引擎（日频、横截面权重矩阵）。

约定：
- weights.loc[t] 是用 t 日收盘信息决定、在 t 日收盘建立的目标权重；
  它赚取 t+1 日的收益（内部用 weights.shift(1) 对齐，调用方不要自己 shift）。
- 成本按换手 × cost_bps 双边计：turnover_t = sum(|w_t - w_{t-1}|)。
- 收益用价格列自身 ffill 后 pct_change，停牌/缺口复牌的跳空收益会在复牌日计入；
  若持仓后价格永久缺失，引擎在指标中报告 terminal_missing_position_days；
  terminal_return 可为这些退市持仓注入一个收益假设（默认 None=保持旧口径不注入）：
  标量对所有退市持仓统一注入；pd.Series（index=security_id）按证券注入各自的
  真实退市收益，Series 缺失/NaN 的证券回退到 terminal_return_fallback，
  fallback 也为 None 时该证券不注入（等价于旧口径退市赚 0%）。

这是研究原型，不建模盘中滑点、做空费率、权重漂移再平衡。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

TRADING_DAYS = 252

# run_backtest 的价格派生中间量只依赖 adj_close 本身；评估层对同一面板做
# 数十次回测（分位 × horizon × 因子），逐次重算是 2026-07 实测的最大热点之一。
# 以 (id, shape) 为键 + 对象身份复核缓存，最多驻留 2 份（评估面板 + 基准面板）。
_DERIVED_CACHE: dict[tuple, dict] = {}


def _derived_from_prices(adj_close: pd.DataFrame) -> dict:
    key = (id(adj_close), adj_close.shape)
    hit = _DERIVED_CACHE.get(key)
    if hit is not None and hit["ref"] is adj_close:
        return hit
    ffilled = adj_close.ffill()
    returns = ffilled.pct_change(fill_method=None)
    valid_pair = adj_close.notna() & ffilled.shift(1).notna()
    returns = returns.where(valid_pair)
    missing = adj_close.isna()
    prev_missing = missing.shift(1, fill_value=False)
    entry = {
        "ref": adj_close,
        "returns": returns,
        "returns_filled": returns.fillna(0.0),
        "ever_future_price": adj_close.notna()[::-1].cummax()[::-1],
        "gap_entry": missing & ~prev_missing,
        "carry_zone": missing | (~missing & prev_missing),
    }
    if len(_DERIVED_CACHE) >= 2:
        _DERIVED_CACHE.clear()
    _DERIVED_CACHE[key] = entry
    return entry



@dataclass
class BacktestResult:
    name: str
    daily_returns: pd.Series = field(repr=False)
    equity: pd.Series = field(repr=False)
    turnover: pd.Series = field(repr=False)
    avg_positions: float = 0.0
    terminal_missing_position_days: int = 0

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
            "terminal_missing_position_days": float(self.terminal_missing_position_days),
        }


def _returns_with_gap_recovery(adj_close: pd.DataFrame) -> pd.DataFrame:
    """计算收益：停牌/缺失期间收益为 NaN，复牌日一次性补回跨缺口收益。"""
    returns = adj_close.ffill().pct_change(fill_method=None)
    valid_pair = adj_close.notna() & adj_close.ffill().shift(1).notna()
    return returns.where(valid_pair)


def _terminal_missing_position_days(held: pd.DataFrame, adj_close: pd.DataFrame) -> int:
    """统计持仓后价格永久缺失的 security-day，用于暴露退市/终止样本风险。"""
    ever_future_price = adj_close.notna()[::-1].cummax()[::-1]
    terminal_missing = held.gt(0) & adj_close.isna() & ~ever_future_price
    return int(terminal_missing.sum().sum())


def _hold_through_price_gaps(held: pd.DataFrame, adj_close: pd.DataFrame) -> pd.DataFrame:
    """停牌(价格 NaN)期间冻结持仓权重，使复牌日的跨缺口收益作用在停牌前的实际仓位上。

    `held` 是 weights.shift(1)；跨缺口收益一次性落在复牌日，而该日 held 可能已被策略
    清零（停牌期无法定价就减仓），导致真实盈亏被乘成 0 静默吞掉。修法：取每段缺口"进入
    缺口那一刻"持有的权重（gap entry 的 held），前向填充覆盖整段缺口 + 复牌当日，用它替换
    这些格子的 held。非缺口、未持有的格子保持原值。
    """
    missing = adj_close.isna()
    prev_missing = missing.shift(1, fill_value=False)
    gap_entry = missing & ~prev_missing                 # 每段缺口的第一天
    reprice_day = ~missing & prev_missing               # 缺口后第一天有价（补回收益落点）
    carry_zone = missing | reprice_day                  # 需要用冻结仓位的格子
    entry_held = held.where(gap_entry)                  # 仅 gap-entry 行有值
    frozen = entry_held.ffill().where(carry_zone)       # 冻结值铺到整段缺口 + 复牌日
    return held.where(~carry_zone, frozen).fillna(held)


def run_backtest(
    name: str,
    weights: pd.DataFrame,
    adj_close: pd.DataFrame,
    *,
    cost_bps: float = 10.0,
    hold_through_gaps: bool = True,
    terminal_return: float | pd.Series | None = None,
    terminal_return_fallback: float | None = None,
) -> BacktestResult:
    derived = _derived_from_prices(adj_close)
    returns = derived["returns"]
    weights = weights.reindex(index=returns.index, columns=returns.columns).fillna(0.0)

    held = weights.shift(1).fillna(0.0)
    ever_future_price = derived["ever_future_price"]
    terminal_mask = held.gt(0) & adj_close.isna() & ~ever_future_price
    terminal_missing_position_days = int(terminal_mask.sum().sum())
    # 退市/终止收益政策：持仓后价格永久缺失时，默认 _returns_with_gap_recovery 给 NaN，
    # fillna(0.0) 后等于静默赚 0%。terminal_return 让调用方为"退市当日"注入一个收益假设
    # （如 -1.0=-100%）。只在永久缺失的第一天（退市事件日）注入一次，避免重复相乘炸掉数学。
    # 标量=统一假设；pd.Series（index=security_id，值=已实现退市收益）=按证券注入，
    # Series 缺失/NaN 的证券回退到 terminal_return_fallback（None 则不注入，保持旧口径）。
    returns_filled = derived["returns_filled"]
    if terminal_return is not None and terminal_missing_position_days > 0:
        first_terminal = terminal_mask & ~terminal_mask.shift(1, fill_value=False)
        returns = returns.copy()
        if isinstance(terminal_return, pd.Series):
            # 向量化按列注入：把 Series 对齐到面板列（security_id），fallback 补洞后
            # 仍缺值的列不注入（等价于该证券沿用 terminal_return=None 的旧口径）。
            per_security = terminal_return.reindex(returns.columns).astype("float64")
            if terminal_return_fallback is not None:
                per_security = per_security.fillna(terminal_return_fallback)
            inject = first_terminal & per_security.notna()
            returns = returns.mask(inject, per_security, axis=1)
        else:
            returns[first_terminal] = terminal_return
        returns_filled = returns.fillna(0.0)
    # 停牌期冻结持仓，避免复牌跨缺口收益被清零的权重吞掉（默认开启；可关以复现旧口径）。
    if hold_through_gaps:
        carry_zone = derived["carry_zone"]
        entry_held = held.where(derived["gap_entry"])
        frozen = entry_held.ffill().where(carry_zone)
        effective_held = held.where(~carry_zone, frozen).fillna(held)
    else:
        effective_held = held
    gross = (effective_held * returns_filled).sum(axis=1)
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
        terminal_missing_position_days=terminal_missing_position_days,
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
