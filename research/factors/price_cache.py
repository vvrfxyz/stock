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

内存账（v3 修订）：宽表 ~255MB/列；长表在宽表物化后**立即清退**（v2 曾驻留
~1.9G 长表 + BytesIO 再叠 3-4G CSV 文本，253 上 6G 峰值 OOM 实录）；
COPY 走临时文件不过内存。需要的列按调用方声明急切物化，一次装载覆盖后续全部因子。
"""
from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import date, timedelta

import pandas as pd

from research.data import apply_adjustment, load_factor_events
from research.progress import Progress

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
    types: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """COPY 通道版 load_price_long（可选列、可选 universe 过滤）。

    types=None（默认）语义：security_ids 显式给定时不加类型门——universe 的
    类型口径由上游（evaluate --include-adr 等）裁定，这里再叠 ('CS','ETF') 门
    会把 opt-in 的 ADR 列静默剔掉（§E.6 双重过滤修复）；未给 ids 时回退
    ('CS','ETF') 保持旧行为。显式传 types 则原样生效。
    """
    id_clause = ""
    if security_ids:
        id_list = ",".join(str(int(x)) for x in security_ids)
        id_clause = f"and p.security_id in ({id_list})"
    if types is None:
        types = () if security_ids else ("CS", "ETF")
    type_clause = ""
    if types:
        type_list = ",".join(f"'{t}'" for t in types)
        type_clause = f"and upper(s.type) in ({type_list})"
    sql = f"""
        COPY (
            select p.security_id, p.date, {columns}
            from daily_prices p
            join securities s on s.id = p.security_id
            where p.date between '{start}' and '{end}'
              {type_clause} {id_clause}
            order by p.security_id, p.date
        ) TO STDOUT WITH (FORMAT csv, HEADER true)
    """
    # CSV 落盘临时文件再解析：BytesIO 会让 3-4G 的 CSV 文本与解析结果在内存
    # 共存（2026-07-07 在 253 上 6G 峰值 OOM 的直接原因之一），落盘后峰值只剩解析结果。
    raw = engine.raw_connection()
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    try:
        with raw.cursor() as cursor:
            cursor.copy_expert(sql, tmp)
        tmp.close()
        frame = pd.read_csv(tmp.name, parse_dates=["date"])
    finally:
        raw.close()
        tmp.close()
        os.unlink(tmp.name)
    return frame


def _materialize(engine, *, start: date, end: date, security_ids: list[int],
                 columns: tuple[str, ...], with_adjusted: bool) -> None:
    """急切物化：长表一次装载 -> 需要的宽表逐列 pivot + 复权派生 -> **清退长表**。

    长表（~1.9G/10 年全宇宙）在宽表齐备后是纯冗余，驻留即 OOM 风险
    （253 仅 11G 且与 PG/CH 同宿）。物化标记键防止重复装载。
    """
    fp = _universe_fingerprint(security_ids)
    url = str(engine.url)
    need = [c for c in columns
            if ("wide", url, start, end, fp, c) not in _PANEL_CACHE]
    need_adj = with_adjusted and ("adj_close", url, start, end, fp) not in _PANEL_CACHE
    if not need and not need_adj:
        return
    prog = Progress(f"panels[{start}~{end}]")
    load_cols = sorted(set(need) | ({"close"} if need_adj else set()))
    with prog.stage(f"COPY 长表 {load_cols}"):
        frame = load_price_long_fast(
            engine, start=start, end=end,
            columns=", ".join(load_cols), security_ids=security_ids)
    for col in need:
        with prog.stage(f"pivot {col}"):
            try:
                wide = frame.pivot(index="date", columns="security_id", values=col)
            except ValueError:  # 重复键理论上不发生（PK），保底旧语义
                wide = frame.pivot_table(index="date", columns="security_id",
                                         values=col, aggfunc="last")
            _PANEL_CACHE[("wide", url, start, end, fp, col)] = wide
    if need_adj:
        with prog.stage("复权派生 adj_close"):
            prices = frame[["security_id", "date", "close"]].copy()
            events = load_factor_events(engine, as_of=end)
            prices = apply_adjustment(prices, events, as_of=end)
            try:
                adj = prices.pivot(index="date", columns="security_id", values="adj_close")
            except ValueError:
                adj = prices.pivot_table(index="date", columns="security_id",
                                         values="adj_close", aggfunc="last")
            _PANEL_CACHE[("adj_close", url, start, end, fp)] = adj
    del frame  # 长表清退：宽表齐备后不留 GB 级冗余


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
    """原始日线多列宽表；长表单次装载即清退，逐列单次 pivot，跨因子全命中。"""
    start, end = _window(dates, buffer_days)
    _materialize(engine, start=start, end=end, security_ids=security_ids,
                 columns=columns, with_adjusted=False)
    fp = _universe_fingerprint(security_ids)
    return {col: _PANEL_CACHE[("wide", str(engine.url), start, end, fp, col)]
            for col in columns}


def adjusted_close_panel(
    engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
    buffer_days: int = 45,
) -> pd.DataFrame:
    """复权收盘宽表（含 lookback 预热段），与原始宽表共享一次装载，进程内记忆化。"""
    start, end = _window(dates, buffer_days)
    _materialize(engine, start=start, end=end, security_ids=security_ids,
                 columns=(), with_adjusted=True)
    return _PANEL_CACHE[("adj_close", str(engine.url), start, end,
                         _universe_fingerprint(security_ids))]


def clear_cache() -> None:
    _PANEL_CACHE.clear()
