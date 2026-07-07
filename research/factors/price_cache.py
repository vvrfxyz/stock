"""因子层共享价格面板缓存（COPY 高速装载 + 进程内记忆化）。

2026-07-07 性能重写（v2）：v1 的缓存键含精确起始日，而各因子 buffer_days
不同（70/90/.../400），8 个 TA 因子各自脱靶——同一份 30M 行 OHLCV 拉了 8 遍、
每遍再 pivot_table 5 列（groupby 聚合是 30M 行量级的纯开销）。v2 三刀：

1. **长表只装一次**：全列（o/h/l/c/v/vwap）一次 COPY，键=量化后窗口+宇宙指纹。
2. **宽表按列缓存**：每列只 pivot 一次（走 PK 无重复的快速 `pivot`，重复键才
   回退 pivot_table，同 data.to_wide 语义），后续调用零成本返回同一 DataFrame
   对象——消费方约定只读（reindex/where 都产生新对象，天然安全）。
3. **buffer 量化**：buffer_days 上取整到 {200, 420} 档，不同因子共享缓存条目。
   多出的历史对固定窗滚动值无影响（窗口只看最近 w 个观测）；对 ewm（macd）
   仅改善序列头部的初始化偏差，方向更正确，文档化接受。

内存账：长表 ~30M 行 × 8 列 ≈ 1.9GB + 宽表 ~255MB/列，20 年窗口全列驻留
约 4GB——单评估进程可承受；超限先 clear_cache()。
"""
from __future__ import annotations

import hashlib
import io
from datetime import date, timedelta

import pandas as pd

from research.data import apply_adjustment, load_factor_events

_PANEL_CACHE: dict[tuple, pd.DataFrame] = {}

BAR_COLUMNS = ("open", "high", "low", "close", "volume", "vwap")
_BUFFER_TIERS = (200, 420)


def _universe_fingerprint(ids: list[int]) -> str:
    joined = ",".join(str(int(x)) for x in sorted(ids))
    return hashlib.md5(joined.encode()).hexdigest()


def _quantize_buffer(buffer_days: int) -> int:
    for tier in _BUFFER_TIERS:
        if buffer_days <= tier:
            return tier
    return buffer_days  # 超出档位按原值（罕见调用自担脱靶）


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


def _long_bars(engine, *, start: date, end: date, security_ids: list[int]) -> pd.DataFrame:
    key = ("long_bars", str(engine.url), start, end, _universe_fingerprint(security_ids))
    if key not in _PANEL_CACHE:
        _PANEL_CACHE[key] = load_price_long_fast(
            engine, start=start, end=end,
            columns=", ".join(BAR_COLUMNS), security_ids=security_ids)
    return _PANEL_CACHE[key]


def _wide_bar(engine, *, start: date, end: date, security_ids: list[int], column: str) -> pd.DataFrame:
    key = ("wide", str(engine.url), start, end, _universe_fingerprint(security_ids), column)
    if key not in _PANEL_CACHE:
        frame = _long_bars(engine, start=start, end=end, security_ids=security_ids)
        try:
            wide = frame.pivot(index="date", columns="security_id", values=column)
        except ValueError:  # 重复键理论上不发生（PK），保底旧语义
            wide = frame.pivot_table(index="date", columns="security_id",
                                     values=column, aggfunc="last")
        _PANEL_CACHE[key] = wide
    return _PANEL_CACHE[key]


def _window(dates: pd.DatetimeIndex, buffer_days: int) -> tuple[date, date]:
    quantized = _quantize_buffer(buffer_days)
    return (dates[0] - timedelta(days=quantized)).date(), dates[-1].date()


def raw_bar_panels(
    engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    columns: tuple[str, ...],
    buffer_days: int = 45,
) -> dict[str, pd.DataFrame]:
    """原始日线多列宽表；长表单次装载 + 逐列单次 pivot，跨因子全命中。"""
    start, end = _window(dates, buffer_days)
    return {col: _wide_bar(engine, start=start, end=end,
                           security_ids=security_ids, column=col)
            for col in columns}


def adjusted_close_panel(
    engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    buffer_days: int = 45,
) -> pd.DataFrame:
    """复权收盘宽表（含 lookback 预热段），从共享长表派生，进程内记忆化。"""
    start, end = _window(dates, buffer_days)
    key = ("adj_close", str(engine.url), start, end, _universe_fingerprint(security_ids))
    if key not in _PANEL_CACHE:
        frame = _long_bars(engine, start=start, end=end, security_ids=security_ids)
        prices = frame[["security_id", "date", "close"]].copy()
        events = load_factor_events(engine, as_of=end)
        prices = apply_adjustment(prices, events, as_of=end)
        try:
            wide = prices.pivot(index="date", columns="security_id", values="adj_close")
        except ValueError:
            wide = prices.pivot_table(index="date", columns="security_id",
                                      values="adj_close", aggfunc="last")
        _PANEL_CACHE[key] = wide
    return _PANEL_CACHE[key]


def clear_cache() -> None:
    _PANEL_CACHE.clear()
