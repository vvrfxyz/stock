"""复权价格读取层。

daily_prices 永远只存 raw facts；复权价是读取时由
daily_prices × computed_adjustment_factors 现算的派生值，绝不回写事实表。

口径（与 raw_actions_v1 / Massive historical_adjustment_factor 对齐）：
- computed_adjustment_factors.cumulative_factor 表示“该事件及其之后所有事件”的累计因子。
- 后复权习惯下，bar 日期 d 应用的因子 = C(第一个 ex_date > d) / C(第一个 ex_date > as_of)；
  C 不存在时为 1。这个归一化会消除 as_of 之后已入库未来事件对历史链的污染。
- as_of 是防未来函数边界：回测在 as_of 时点“看不到”之后才发生的拆股/分红。
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
    """按 symbol / current_symbol / 历史 symbol 解析到 security_id。

    优先活跃行的 symbol 精确匹配，再查 current_symbol，最后查 symbol history。
    """
    if isinstance(symbol_or_id, int):
        return symbol_or_id
    sym = str(symbol_or_id).lower()
    # 1) 活跃 symbol
    row = (
        session.query(Security.id)
        .filter(func.lower(Security.symbol) == sym, Security.is_active.is_(True))
        .one_or_none()
    )
    if row is not None:
        return row[0]
    # 2) 活跃 current_symbol
    row = (
        session.query(Security.id)
        .filter(func.lower(Security.current_symbol) == sym, Security.is_active.is_(True))
        .one_or_none()
    )
    if row is not None:
        return row[0]
    # 3) 不限 active 的 symbol
    row = (
        session.query(Security.id)
        .filter(func.lower(Security.symbol) == sym)
        .order_by(Security.is_active.desc())
        .first()
    )
    if row is not None:
        return row[0]
    # 4) symbol history（SQLite 测试环境可能缺此表）
    try:
        from data_models.models import SecuritySymbolHistory
        row = (
            session.query(SecuritySymbolHistory.security_id)
            .filter(func.lower(SecuritySymbolHistory.symbol) == sym)
            .first()
        )
        if row is not None:
            return row[0]
    except Exception:
        pass
    raise LookupError(f"未找到 symbol: {symbol_or_id}")


def load_factor_events(
    session,
    security_id: int,
    *,
    as_of: date,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
) -> list[tuple[date, Decimal]]:
    """返回按 ex_date 升序的 (ex_date, cumulative_factor)。

    cumulative_factor 是全链后缀积，历史行可能已包含 as_of 之后的未来事件；
    因此这里必须加载完整事件链，读取时再用 factor_for_date 的 as_of 归一化。
    as_of 参数保留在签名中，表示调用方的可见性边界。
    """
    rows = (
        session.query(ComputedAdjustmentFactor.date, ComputedAdjustmentFactor.cumulative_factor)
        .filter(
            ComputedAdjustmentFactor.security_id == security_id,
            ComputedAdjustmentFactor.methodology_version == methodology_version,
            ComputedAdjustmentFactor.factor_type == "historical_adjustment",
        )
        .order_by(ComputedAdjustmentFactor.date.asc())
        .all()
    )
    # 同一 ex_date 可能有多行（同日多事件共享同一 cumulative），保留每个日期一条即可。
    by_date: dict[date, Decimal] = {}
    for ex_date, cumulative in rows:
        by_date[ex_date] = Decimal(cumulative)
    return sorted(by_date.items())


def _chain_after(events: list[tuple[date, Decimal]], boundary: date) -> Decimal:
    dates = [item[0] for item in events]
    index = bisect_right(dates, boundary)
    if index >= len(events):
        return Decimal("1")
    return events[index][1]


def factor_for_date(events: list[tuple[date, Decimal]], bar_date: date, *, as_of: date | None = None) -> Decimal:
    """bar 日期应用的 as-of 归一化后复权因子。

    events 必须按 ex_date 升序。cumulative_factor 本身是“该事件及之后”的全链累计，
    所以 as_of 时点可见的因子是 C(第一个 ex_date > bar_date) / C(第一个 ex_date > as_of)。
    未传 as_of 时保持旧的“以最新价为基准”语义（as_of 取最后事件日）。
    """
    if not events:
        return Decimal("1")
    effective_as_of = as_of or events[-1][0]
    denominator = _chain_after(events, effective_as_of)
    if denominator == 0:
        return Decimal("1")
    return _chain_after(events, bar_date) / denominator


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
        factor = factor_for_date(events, bar.date, as_of=effective_as_of)
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
