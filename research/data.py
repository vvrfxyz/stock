"""研究用批量数据加载：daily_prices × computed_adjustment_factors -> pandas 面板。

与 utils/adjusted_prices 同口径（raw_actions_v1）：
- bar 日期 d 的因子 = C(第一个 ex_date > d) / C(第一个 ex_date > as_of)，C 不存在时为 1。
- 这个 as_of 归一化会消除 computed_adjustment_factors 全链后缀积中未来事件的污染。

区别在于这里一次 SQL 拉全市场、numpy 向量化套因子，供横截面研究使用；
单标的精确读取仍走 utils.adjusted_prices.get_adjusted_daily_bars。
"""
from __future__ import annotations

import os
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

DEFAULT_METHODOLOGY_VERSION = "raw_actions_v1"
# 2026-07 corporate-actions 20 年回填后（docs/corp_actions_archive_2026-07.md），
# MASSIVE 源事件与因子链覆盖到 2003；更早无价格、无事件，仍是硬地板。
FACTOR_TRUST_FLOOR = date(2003, 1, 1)


def research_engine(database_url: str | None = None) -> Engine:
    """优先 RESEARCH_DATABASE_URL（指向 253 生产库的只读连接），回退 DATABASE_URL。"""
    url = database_url or os.environ.get("RESEARCH_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("需要 RESEARCH_DATABASE_URL 或 DATABASE_URL")
    return create_engine(url)


def load_price_long(
    engine: Engine,
    *,
    start: date,
    end: date,
    types: tuple[str, ...] = ("CS",),
    include_inactive: bool = True,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """拉取 [start, end] 的原始日线（长表）。

    默认包含 is_active=False 的证券：建库后退市的标的保留在库里，
    纳入它们可以减轻（但不能消除）幸存者偏差。
    """
    active_clause = "" if include_inactive else "and s.is_active"
    id_clause = "and p.security_id = any(:security_ids)" if security_ids else ""
    sql = text(
        f"""
        select p.security_id, p.date,
               p.open::float8 as open, p.close::float8 as close,
               p.volume::float8 as volume, p.vwap::float8 as vwap
        from daily_prices p
        join securities s on s.id = p.security_id
        where p.date between :start and :end
          and s.type = any(:types) {active_clause} {id_clause}
        order by p.security_id, p.date
        """
    )
    params: dict = {"start": start, "end": end, "types": list(types)}
    if security_ids:
        params["security_ids"] = security_ids
    chunks = list(pd.read_sql_query(
        sql,
        engine,
        params=params,
        chunksize=500_000,
        parse_dates=["date"],
    ))
    if not chunks:
        return pd.DataFrame(columns=["security_id", "date", "open", "close", "volume", "vwap"])
    df = pd.concat(chunks, ignore_index=True)
    df["security_id"] = df["security_id"].astype(np.int32)
    return df


def load_factor_events(
    engine: Engine,
    *,
    as_of: date,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
) -> pd.DataFrame:
    sql = text(
        """
        select security_id, date as ex_date, max(cumulative_factor)::float8 as cumulative_factor
        from computed_adjustment_factors
        where methodology_version = :mv
          and factor_type = 'historical_adjustment'
        group by security_id, date
        order by security_id, ex_date
        """
    )
    return pd.read_sql_query(sql, engine, params={"mv": methodology_version, "as_of": as_of}, parse_dates=["ex_date"])


def apply_adjustment(prices: pd.DataFrame, events: pd.DataFrame, *, as_of: date | pd.Timestamp | None = None) -> pd.DataFrame:
    """为长表 prices 增加 adj_close 列（同口径后复权）。"""
    factor = np.ones(len(prices))
    if prices.empty:
        out = prices.copy()
        out["adj_close"] = out.get("close", pd.Series(dtype=float))
        return out
    effective_as_of = pd.Timestamp(as_of) if as_of is not None else prices["date"].max()
    bar_dates = prices["date"].to_numpy()
    sid_values = prices["security_id"].to_numpy()
    grouped = {
        sid: (g["ex_date"].to_numpy(), g["cumulative_factor"].to_numpy(dtype=float))
        for sid, g in events.groupby("security_id")
    }
    # prices 按 (security_id, date) 排序，逐证券切片做 searchsorted
    boundaries = np.flatnonzero(np.diff(sid_values)) + 1
    starts = np.concatenate(([0], boundaries))
    ends = np.concatenate((boundaries, [len(prices)]))
    for lo, hi in zip(starts, ends):
        ev = grouped.get(int(sid_values[lo]))
        if ev is None:
            continue
        ex_dates, cumulative = ev
        idx = np.searchsorted(ex_dates, bar_dates[lo:hi], side="right")
        seg = np.ones(hi - lo)
        in_range = idx < len(cumulative)
        seg[in_range] = cumulative[idx[in_range]]

        as_of_idx = np.searchsorted(ex_dates, effective_as_of.to_datetime64(), side="right")
        denominator = cumulative[as_of_idx] if as_of_idx < len(cumulative) else 1.0
        if denominator != 0:
            seg = seg / denominator
        factor[lo:hi] = seg
    out = prices.copy()
    out["adj_close"] = out["close"] * factor
    return out


def to_wide(prices: pd.DataFrame, column: str) -> pd.DataFrame:
    """长表 -> 宽表（index=date, columns=security_id）。"""
    return prices.pivot_table(index="date", columns="security_id", values=column, aggfunc="last")


def load_adjusted_panel(
    engine: Engine,
    *,
    start: date,
    end: date,
    types: tuple[str, ...] = ("CS",),
    as_of: date | None = None,
    include_inactive: bool = True,
) -> dict[str, pd.DataFrame]:
    """返回宽表字典：adj_close / close / volume / dollar_volume。"""
    if start < FACTOR_TRUST_FLOOR:
        raise ValueError(
            f"start={start} 早于因子可信窗口 {FACTOR_TRUST_FLOOR}；"
            "更早价格未保证复权，研究面板拒绝装载。"
        )
    effective_as_of = as_of or end
    prices = load_price_long(engine, start=start, end=end, types=types, include_inactive=include_inactive)
    events = load_factor_events(engine, as_of=effective_as_of)
    prices = apply_adjustment(prices, events, as_of=effective_as_of)
    prices["dollar_volume"] = prices["close"] * prices["volume"]
    return {
        "adj_close": to_wide(prices, "adj_close"),
        "close": to_wide(prices, "close"),
        "volume": to_wide(prices, "volume"),
        "dollar_volume": to_wide(prices, "dollar_volume"),
    }


def load_symbol_map(engine: Engine) -> pd.Series:
    df = pd.read_sql_query(text("select id, symbol from securities"), engine)
    return df.set_index("id")["symbol"]


def load_delisting_returns(engine: Engine) -> pd.Series:
    """逐证券实测退市收益（index=security_id int, values=float）。

    来源 delisting_events.delisting_return（docs/todo_crsp_grade_2026-07.md 任务 1）。
    同一证券多次退市（重新上市后再退）时只取**最近一次**退市事件：面板的终局
    是最后那次退市，借用更早退市周期的收益属于口径错误——最近一次无实测值的
    证券整体缺席，由 run_backtest 的 terminal_return_fallback 兜底（宁缺毋滥）。
    表未填充时返回空 Series，调用方应退回全局标量假设。
    """
    sql = text(
        """
        select security_id, delisting_return::float8 as delisting_return
        from (
            select distinct on (security_id) security_id, delisting_return
            from delisting_events
            order by security_id, delist_date desc
        ) latest
        where delisting_return is not null
        """
    )
    df = pd.read_sql_query(sql, engine)
    series = df.set_index("security_id")["delisting_return"].astype("float64")
    series.index = series.index.astype("int64")
    return series


def securities_with_uncovered_events(
    engine: Engine,
    *,
    start: date,
    end: date,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
) -> list[int]:
    """窗口内存在 corporate_actions 事件但无对应因子覆盖的证券。

    主要是因子构建曾只跑 is_active=True 导致的退市股缺口，以及外币分红缺 FX 汇率
    被构建跳过的事件；缺因子的拆股/分红会在价格序列里留下假跳空，这些证券必须从
    横截面样本中剔除。两个分支：

    - MASSIVE 事件按 source_event_id 事件级对齐（同因子构建口径），同日另一事件的
      因子行不会掩盖被跳过的事件。
    - 非 MASSIVE 事件（POLYGON legacy 合成行等）不参与因子构建：同日存在同类型
      MASSIVE 行即视为已被 vendor 事件接管（其因子覆盖由上一分支把关）；没有
      MASSIVE 对应行的孤行就是复权链上的洞——2003 归档导入的值冲突挂起
      （import_corporate_actions_archive R13）、未确认保留的合成行、归档漏抓的
      证券都靠这个分支机器剔除，人工裁决落库后本函数自动放行。
    """
    sql = text(
        """
        select distinct ca.security_id
        from corporate_actions ca
        where ca.ex_date between :start and :end
          and ca.action_type in ('SPLIT', 'DIVIDEND')
          and (
            (upper(ca.source) = 'MASSIVE'
             and not exists (
               select 1 from computed_adjustment_factors f
               where f.security_id = ca.security_id
                 and f.source_event_id = ca.source_event_id
                 and f.methodology_version = :mv))
            or
            (upper(ca.source) <> 'MASSIVE'
             and not exists (
               select 1 from corporate_actions m
               where m.security_id = ca.security_id
                 and m.action_type = ca.action_type
                 and m.ex_date = ca.ex_date
                 and upper(m.source) = 'MASSIVE'))
          )
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start": start, "end": end, "mv": methodology_version}).fetchall()
    return [r[0] for r in rows]
