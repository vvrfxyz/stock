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

# 退市注入结果缓存（Series 口径）：retail_reality 1000 次 sim 传同一个 adj_sub 面板
# 对象 + 同一个 terminal_return Series，注入的**价格视角**部分（每列价格永久缺失首日
# 注入 per_security，与持仓无关）逐 sim 相同，可缓存。以 (id(adj_close), id(terminal_return),
# fallback) 为键 + 双对象身份复核；最多驻留 2 份（评估面板 + 基准面板）。
_TERMINAL_INJECTION_CACHE: dict[tuple, dict] = {}


def _price_perspective_injection(
    adj_close: pd.DataFrame,
    derived: dict,
    terminal_return: pd.Series,
    terminal_return_fallback: float | None,
) -> dict:
    """价格视角（与持仓无关）的退市注入：在每列价格**永久缺失首日**注入 per_security。

    注入点 = price_first_terminal（价格永久缺失段首日，只依赖 adj_close），非引擎现行
    的 first_terminal（依赖 held）。二者在"持仓先于退市开始"的正常列上落在同一日、
    取同一值，故价格视角注入的 returns_filled 对这些列与现行逐位一致；差异只出现在
    持仓晚于退市或未持仓的列上，由 run_backtest 的 gross 贡献守卫逐列甄别（见调用处）。
    只依赖 adj_close + terminal_return(Series) + fallback，跨 sim 缓存。
    """
    key = (id(adj_close), id(terminal_return), terminal_return_fallback)
    hit = _TERMINAL_INJECTION_CACHE.get(key)
    if hit is not None and hit["adj_ref"] is adj_close and hit["tr_ref"] is terminal_return:
        return hit
    returns = derived["returns"]
    price_terminal = adj_close.isna() & ~derived["ever_future_price"]
    price_first_terminal = price_terminal & ~price_terminal.shift(1, fill_value=False)
    per_security = terminal_return.reindex(returns.columns).astype("float64")
    if terminal_return_fallback is not None:
        per_security = per_security.fillna(terminal_return_fallback)
    per_security_notna = per_security.notna()
    inject = price_first_terminal & per_security_notna  # 按列广播
    returns_injected = returns.mask(inject, per_security, axis=1)
    entry = {
        "adj_ref": adj_close,
        "tr_ref": terminal_return,
        "returns_filled": returns_injected.fillna(0.0),
        "price_first_terminal": price_first_terminal,
        "per_security": per_security,
        "per_security_notna": per_security_notna,
        # 有价格永久缺失段且会注入的列（守卫只需在这些列上比较）
        "check_cols": returns.columns[
            (price_first_terminal.any(axis=0) & per_security_notna).to_numpy()
        ],
    }
    if len(_TERMINAL_INJECTION_CACHE) >= 2:
        _TERMINAL_INJECTION_CACHE.clear()
    _TERMINAL_INJECTION_CACHE[key] = entry
    return entry



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
    carry_zone = missing | (~missing & prev_missing)
    entry = {
        "ref": adj_close,
        "returns": returns,
        "returns_filled": returns.fillna(0.0),
        "ever_future_price": adj_close.notna()[::-1].cummax()[::-1],
        "gap_entry": missing & ~prev_missing,
        "carry_zone": carry_zone,
        # 有停牌/退市缺口的列（面板级、跨 sim 复用）：hold_through_gaps 冻结只需在这些
        # 列上做 where/ffill，其余列 effective_held 恒等于 held（carry_zone 全 False）。
        "gap_cols": adj_close.columns[carry_zone.any(axis=0).to_numpy()],
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
    cost_bps: float | pd.Series = 10.0,
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

    # 停牌期冻结持仓（默认开启；可关以复现旧口径）——数值与注入无关、只依赖 held + 缺口
    # 结构，故提前到注入之前算：退市注入的 gross 贡献守卫需要 effective_held。
    # 冻结只作用于有缺口的列（carry_zone.any，面板级缓存 gap_cols，~15%）：无缺口列
    # carry_zone 全 False -> where 恒保留 held、fillna 无操作 -> effective_held ≡ held，
    # 故只在 gap_cols 上做 where/ffill，其余列直接沿用 held（held 为本 sim 私有、之后不
    # 再引用，原地改挂 gap 列即得 effective_held，避免全面板 ffill 与整表复制）。
    if hold_through_gaps:
        gap_cols = derived["gap_cols"]
        if len(gap_cols):
            held_g = held[gap_cols].copy()  # 私有化 gap 列，令下面对 held 的改挂在位（不 CoW 整表）
            carry_g = derived["carry_zone"][gap_cols]
            frozen_g = held_g.where(derived["gap_entry"][gap_cols]).ffill().where(carry_g)
            held[gap_cols] = held_g.where(~carry_g, frozen_g).fillna(held_g)
        effective_held = held
    else:
        effective_held = held

    # 退市/终止收益政策：持仓后价格永久缺失时，默认 _returns_with_gap_recovery 给 NaN，
    # fillna(0.0) 后等于静默赚 0%。terminal_return 让调用方为"退市当日"注入一个收益假设
    # （如 -1.0=-100%）。只在永久缺失的第一天（退市事件日）注入一次，避免重复相乘炸掉数学。
    # 标量=统一假设；pd.Series（index=security_id，值=已实现退市收益）=按证券注入，
    # Series 缺失/NaN 的证券回退到 terminal_return_fallback（None 则不注入，保持旧口径）。
    returns_filled = derived["returns_filled"]
    if terminal_return is not None and terminal_missing_position_days > 0:
        if isinstance(terminal_return, pd.Series):
            # 价格视角注入结果缓存化（跨 sim 复用），配 gross 贡献语义守卫。
            cache = _price_perspective_injection(
                adj_close, derived, terminal_return, terminal_return_fallback
            )
            check_cols = cache["check_cols"]
            first_terminal = None
            unsafe_cols = check_cols[:0]
            if len(check_cols):
                # 守卫（逐列，只在有注入的退市列上）：比较**现行注入**（落在 first_terminal，
                # 依赖 held）与**价格视角注入**（落在 price_first_terminal）经 effective_held
                # 加权后的 gross 贡献。二者的每列贡献相等 <=> 该列缓存结果对 gross 逐位等价
                # （per_security 每列单值可提出，故比较 Σ effective_held 即可）。不等的列
                # （持仓晚于退市开始 / 尾巴中途重入 / 空头位于尾巴）逐列回退现行路径。
                tm_c = terminal_mask[check_cols]
                first_terminal = tm_c & ~tm_c.shift(1, fill_value=False)
                eh_c = effective_held[check_cols].to_numpy()
                curr_contrib = (eh_c * first_terminal.to_numpy()).sum(axis=0)
                cached_contrib = (
                    eh_c * cache["price_first_terminal"][check_cols].to_numpy()
                ).sum(axis=0)
                unsafe_cols = check_cols[curr_contrib != cached_contrib]
            if len(unsafe_cols) == 0:
                returns_filled = cache["returns_filled"]  # 快路径：只读复用，不 copy
            else:
                # 逐列回退：从缓存拷贝，仅对 unsafe 列按现行 first_terminal 口径重算注入。
                returns_filled = cache["returns_filled"].copy()
                base = returns[unsafe_cols]
                inject = first_terminal[unsafe_cols] & cache["per_security_notna"][unsafe_cols]
                returns_filled[unsafe_cols] = base.mask(
                    inject, cache["per_security"][unsafe_cols], axis=1
                ).fillna(0.0)
        else:
            first_terminal = terminal_mask & ~terminal_mask.shift(1, fill_value=False)
            returns = returns.copy()
            returns[first_terminal] = terminal_return
            returns_filled = returns.fillna(0.0)
    gross = (effective_held * returns_filled).sum(axis=1)
    trades = (weights - weights.shift(1).fillna(0.0)).abs()
    turnover = trades.sum(axis=1)
    # 成本 = Σ_i |Δw_it| × cost_bps_i / 1e4（每单位换手的单边成本）。
    # cost_bps 标量：整组合统一档（逐位等于旧 turnover×cost_bps/1e4）；
    # pd.Series（index=security_id）：逐证券成本（measured 模式的价差档），
    # 缺失证券的成本由调用方补齐（如 fallback 档），此处不再兜底。
    if isinstance(cost_bps, pd.Series):
        cost_vec = cost_bps.reindex(trades.columns).astype("float64")
        # 缺任一**发生换手**的证券成本即报错——静默跳 NaN（sum skipna）会让漏补成本的
        # 证券免费交易（与数值安全纪律相悖，宁炸不静默）。未换手列的缺失无害（贡献 0）。
        traded = trades.to_numpy().sum(axis=0) > 0
        missing = cost_vec.isna().to_numpy() & traded
        if missing.any():
            bad = trades.columns[missing].tolist()
            raise ValueError(
                f"cost_bps(Series) 缺换手证券的成本: {bad[:5]}"
                f"{'…' if len(bad) > 5 else ''}（共 {len(bad)} 只；调用方须补齐 fallback）"
            )
        cost = (trades * cost_vec.fillna(0.0)).sum(axis=1) / 10_000
    else:
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
