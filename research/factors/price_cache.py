"""因子层共享价格面板缓存 + COPY 高速装载。

解决两个性能问题（2026-07-06 wave-1 评估实测痛点）：
1. 多个日线因子各自 compute() 里独立拉同一份价格面板——进程内按
   (url, start, end, universe 指纹) 记忆化，一次装载全族复用；
2. pd.read_sql 逐行物化慢——走 PostgreSQL COPY csv 通道直接喂 pandas C 解析器，
   31M 行量级实测 5-10 倍于 read_sql。

只读缓存，进程结束即失；evaluate 单进程单窗口的使用模式下内存占用 = 一份面板。
"""
from __future__ import annotations

import hashlib
import io
from datetime import date, timedelta

import pandas as pd

from research.data import apply_adjustment, load_factor_events, to_wide

_PANEL_CACHE: dict[tuple, pd.DataFrame] = {}


def _universe_fingerprint(ids: list[int]) -> str:
    return hashlib.md5(",".join(map(str, sorted(ids))).encode()).hexdigest()[:16]


def load_price_long_fast(
    engine,
    *,
    start: date,
    end: date,
    columns: str = "open, high, low, close, volume, vwap, trade_count",
    security_ids: list[int] | None = None,
    types: tuple[str, ...] = ("CS", "ETF"),
) -> pd.DataFrame:
    """COPY 通道版 load_price_long（可选列、可选 universe 过滤）。"""
    id_clause = ""
    if security_ids:
        id_list = ",".join(str(int(x)) for x in security_ids)
        id_clause = f"and p.security_id in ({id_list})"
    type_list = ",".join(f"'{t}'" for t in types)
    sql = f"""
        COPY (
            select p.security_id, p.date, {columns}
            from daily_prices p
            join securities s on s.id = p.security_id
            where p.date between '{start}' and '{end}'
              and upper(s.type) in ({type_list}) {id_clause}
            order by p.security_id, p.date
        ) TO STDOUT WITH (FORMAT csv, HEADER true)
    """
    raw = engine.raw_connection()
    try:
        buffer = io.BytesIO()
        with raw.cursor() as cursor:
            cursor.copy_expert(sql, buffer)
    finally:
        raw.close()
    buffer.seek(0)
    frame = pd.read_csv(buffer, parse_dates=["date"])
    return frame


def adjusted_close_panel(
    engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    buffer_days: int = 45,
) -> pd.DataFrame:
    """复权收盘宽表（含 lookback 预热段），进程内记忆化。"""
    start = (dates[0] - timedelta(days=buffer_days)).date()
    end = dates[-1].date()
    key = ("adj_close", str(engine.url), start, end, _universe_fingerprint(security_ids))
    if key in _PANEL_CACHE:
        return _PANEL_CACHE[key]
    prices = load_price_long_fast(
        engine, start=start, end=end, columns="close", security_ids=security_ids)
    events = load_factor_events(engine, as_of=end)
    prices = apply_adjustment(prices, events, as_of=end)
    panel = to_wide(prices, "adj_close")
    _PANEL_CACHE[key] = panel
    return panel


def raw_bar_panels(
    engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    columns: tuple[str, ...],
    buffer_days: int = 45,
) -> dict[str, pd.DataFrame]:
    """原始日线多列宽表（open/high/low/close/vwap/...），一次装载多列复用，记忆化。"""
    start = (dates[0] - timedelta(days=buffer_days)).date()
    end = dates[-1].date()
    key = ("raw_bars", str(engine.url), start, end, _universe_fingerprint(security_ids))
    if key not in _PANEL_CACHE:
        frame = load_price_long_fast(
            engine, start=start, end=end,
            columns="open, high, low, close, volume, vwap",
            security_ids=security_ids)
        _PANEL_CACHE[key] = frame
    frame = _PANEL_CACHE[key]
    return {col: frame.pivot_table(index="date", columns="security_id",
                                   values=col, aggfunc="last")
            for col in columns}


def clear_cache() -> None:
    _PANEL_CACHE.clear()
