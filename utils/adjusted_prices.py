"""复权价格读取层。

daily_prices 永远只存 raw facts；复权价是读取时由
daily_prices × computed_adjustment_factors 现算的派生值，绝不回写事实表。

口径（与 raw_actions_v1 / Massive historical_adjustment_factor 对齐）：
- computed_adjustment_factors.cumulative_factor 表示"该事件及其之后所有事件"的累计因子。
- 后复权习惯下，bar 日期 d 应用的因子 = ex_date > d 的最近一个事件的 cumulative_factor；
  d 之后没有事件时因子为 1（最新价格即原始价格）。
- as_of 是防未来函数边界：只使用 ex_date <= as_of 的事件，
  回测在 as_of 时点"看不到"之后才发生的拆股/分红。
"""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import func

from data_models.models import ComputedAdjustmentFactor, DailyPrice, Security

DEFAULT_METHODOLOGY_VERSION = "raw_actions_v1"
_PRICE_FIELDS = ("open", "high", "low", "close", "vwap")


@dataclass(frozen=True)
class AdjustedBar:
    date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None
    vwap: Decimal | None
    trade_count: int | None
    adjustment_factor: Decimal
    raw_close: Decimal | None


def resolve_security_id(session, symbol_or_id: str | int) -> int:
    if isinstance(symbol_or_id, int):
        return symbol_or_id
    row = (
        session.query(Security.id)
        .filter(func.lower(Security.symbol) == str(symbol_or_id).lower())
        .one_or_none()
    )
    if row is None:
        raise LookupError(f"未找到 symbol: {symbol_or_id}")
    return row[0]


def load_factor_events(
    session,
    security_id: int,
    *,
    as_of: date,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
) -> list[tuple[date, Decimal]]:
    """返回按 ex_date 升序的 (ex_date, cumulative_factor)，仅含 ex_date <= as_of 的事件。"""
    rows = (
        session.query(ComputedAdjustmentFactor.date, ComputedAdjustmentFactor.cumulative_factor)
        .filter(
            ComputedAdjustmentFactor.security_id == security_id,
            ComputedAdjustmentFactor.methodology_version == methodology_version,
            ComputedAdjustmentFactor.factor_type == "historical_adjustment",
            ComputedAdjustmentFactor.date <= as_of,
        )
        .order_by(ComputedAdjustmentFactor.date.asc())
        .all()
    )
    # 同一 ex_date 可能有多行（同日多事件共享同一 cumulative），保留每个日期一条即可。
    by_date: dict[date, Decimal] = {}
    for ex_date, cumulative in rows:
        by_date[ex_date] = Decimal(cumulative)
    return sorted(by_date.items())


def factor_for_date(events: list[tuple[date, Decimal]], bar_date: date) -> Decimal:
    """bar 日期应用的因子 = 第一个 ex_date > bar_date 的事件的 cumulative_factor，否则 1。

    events 必须按 ex_date 升序。cumulative_factor 本身是"该事件及之后"的累计，
    所以直接取右侧第一个事件即可，无需再连乘。
    """
    if not events:
        return Decimal("1")
    dates = [item[0] for item in events]
    index = bisect_right(dates, bar_date)
    if index >= len(events):
        return Decimal("1")
    return events[index][1]


def get_adjusted_daily_bars(
    session,
    symbol_or_id: str | int,
    *,
    start: date | None = None,
    end: date | None = None,
    as_of: date | None = None,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
) -> list[AdjustedBar]:
    """读取 [start, end] 的后复权日线。

    :param as_of: 防未来函数边界；默认 end（或不限时为今天可见的全部事件）。
                  传入历史日期可复现"当时看到的复权序列"。
    """
    security_id = resolve_security_id(session, symbol_or_id)

    query = (
        session.query(DailyPrice)
        .filter(DailyPrice.security_id == security_id)
        .order_by(DailyPrice.date.asc())
    )
    if start:
        query = query.filter(DailyPrice.date >= start)
    if end:
        query = query.filter(DailyPrice.date <= end)
    bars = query.all()
    if not bars:
        return []

    effective_as_of = as_of or end or bars[-1].date
    events = load_factor_events(
        session,
        security_id,
        as_of=effective_as_of,
        methodology_version=methodology_version,
    )

    adjusted: list[AdjustedBar] = []
    for bar in bars:
        factor = factor_for_date(events, bar.date)
        values: dict[str, Decimal | None] = {}
        for field in _PRICE_FIELDS:
            raw_value = getattr(bar, field)
            values[field] = (Decimal(raw_value) * factor) if raw_value is not None else None
        adjusted.append(
            AdjustedBar(
                date=bar.date,
                open=values["open"],
                high=values["high"],
                low=values["low"],
                close=values["close"],
                volume=bar.volume,
                vwap=values["vwap"],
                trade_count=bar.trade_count,
                adjustment_factor=factor,
                raw_close=Decimal(bar.close) if bar.close is not None else None,
            )
        )
    return adjusted
