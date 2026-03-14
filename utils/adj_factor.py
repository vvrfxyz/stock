from __future__ import annotations

from bisect import bisect_left
from datetime import date, timedelta
from decimal import Decimal, getcontext
from typing import Optional, TYPE_CHECKING

from loguru import logger

from data_models.models import DailyPrice, StockDividend, StockSplit

if TYPE_CHECKING:
    from db_manager import DatabaseManager


def _iter_batches(rows: list[dict], batch_size: int):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def _map_event_date_to_trading_date(
    event_date: date,
    trading_dates_asc: list[date],
    max_shift_days: int = 7,
) -> Optional[date]:
    """
    将事件日期映射到“下一个可用的交易日”（>= event_date）。
    用于处理数据源偶发提供非交易日作为事件日的情况。
    """
    idx = bisect_left(trading_dates_asc, event_date)
    if idx >= len(trading_dates_asc):
        return None
    mapped = trading_dates_asc[idx]
    if max_shift_days >= 0 and (mapped - event_date) > timedelta(days=max_shift_days):
        return None
    return mapped


def recalc_adj_factor_for_security(
    db_manager: DatabaseManager,
    security_id: int,
    symbol: str,
    batch_size: int = 1000,
) -> int:
    """
    重新计算并回填某个 security 的 daily_prices.adj_factor。

    定义：
    - 前复权（以最新交易日为 1）
    - total return（含拆股 + 现金分红）
    - 查询侧：adj_close = close * adj_factor（OHLC 同理）
    """
    getcontext().prec = 28

    with db_manager.get_session() as session:
        price_rows_raw = (
            session.query(DailyPrice.date, DailyPrice.close)
            .filter(DailyPrice.security_id == security_id)
            .order_by(DailyPrice.date.desc())
            .all()
        )
        if not price_rows_raw:
            return 0

        price_rows: list[tuple[date, Optional[Decimal]]] = []
        for d, close_value in price_rows_raw:
            if not d:
                continue
            if close_value is None:
                price_rows.append((d, None))
            else:
                price_rows.append((d, Decimal(close_value)))

        if not price_rows:
            return 0

        trading_dates_desc = [d for d, _ in price_rows]
        trading_dates_asc = list(reversed(trading_dates_desc))

        # --- 拆股事件：同日多条按乘积合并 ---
        splits_by_date: dict[date, Decimal] = {}
        split_rows = (
            session.query(StockSplit.execution_date, StockSplit.split_to, StockSplit.split_from)
            .filter(StockSplit.security_id == security_id)
            .all()
        )
        for execution_date, split_to, split_from in split_rows:
            if not execution_date or split_to is None or split_from is None:
                continue
            try:
                r = Decimal(split_to) / Decimal(split_from)
            except Exception:
                continue
            if r <= 0:
                continue

            mapped_date = _map_event_date_to_trading_date(execution_date, trading_dates_asc)
            if mapped_date is None:
                logger.debug(f"[{symbol}] split@{execution_date} 超出价格日期范围，跳过。")
                continue
            if mapped_date != execution_date:
                logger.debug(f"[{symbol}] split 日期 {execution_date} 非交易日，映射为 {mapped_date}。")

            splits_by_date[mapped_date] = splits_by_date.get(mapped_date, Decimal('1')) * r

        # --- 分红事件：同日多条按现金金额求和合并 ---
        dividends_by_date: dict[date, Decimal] = {}
        dividend_rows = (
            session.query(StockDividend.ex_dividend_date, StockDividend.cash_amount)
            .filter(StockDividend.security_id == security_id)
            .all()
        )
        for ex_date, cash_amount in dividend_rows:
            if not ex_date or cash_amount is None:
                continue
            try:
                d = Decimal(cash_amount)
            except Exception:
                continue
            if d <= 0:
                continue

            mapped_date = _map_event_date_to_trading_date(ex_date, trading_dates_asc)
            if mapped_date is None:
                logger.debug(f"[{symbol}] dividend@{ex_date} 超出价格日期范围，跳过。")
                continue
            if mapped_date != ex_date:
                logger.debug(f"[{symbol}] dividend 日期 {ex_date} 非交易日，映射为 {mapped_date}。")

            dividends_by_date[mapped_date] = dividends_by_date.get(mapped_date, Decimal('0')) + d

    f = Decimal('1')
    updates: list[dict] = []
    for idx, (d, _close) in enumerate(price_rows):
        updates.append({'security_id': security_id, 'date': d, 'adj_factor': f})

        split_ratio = splits_by_date.get(d)
        if split_ratio:
            f = f / split_ratio

        cash_dividend = dividends_by_date.get(d)
        if cash_dividend:
            if idx + 1 >= len(price_rows):
                logger.debug(f"[{symbol}] {d} 分红事件缺少前一交易日 close，跳过。")
            else:
                c_prev = price_rows[idx + 1][1]
                if c_prev is None or c_prev <= 0:
                    logger.debug(f"[{symbol}] {d} 分红事件前一交易日 close 缺失/无效，跳过。")
                elif c_prev <= cash_dividend:
                    logger.debug(f"[{symbol}] {d} 分红事件现金({cash_dividend})>=C_prev({c_prev})，跳过。")
                else:
                    f = f * (c_prev - cash_dividend) / c_prev

    total_upserted = 0
    for batch in _iter_batches(updates, batch_size):
        total_upserted += db_manager.upsert_daily_prices(batch)

    return len(updates)
