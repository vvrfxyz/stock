"""PIT universe membership mask 构建。

回测和因子评估的最大系统性偏差源是用当前 universe 回看历史：
- 2025 年上市的 IPO 不应出现在 2024 年的 universe 中；
- 2024 年退市的证券应在退市日后从 universe 中消失；
- 从未有过交易价格的证券不属于"可投资 universe"。

本模块从 securities.list_date/delist_date + daily_prices 存在性构建
逐日 boolean mask，供研究层替代"全量面板 + eligibility_mask" 的 ad-hoc 模式。

三层 universe：
- ``listed``: list_date <= date, (delist_date > date OR delist_date IS NULL)
- ``has_price``: 当日在 daily_prices 中有价格行
- ``eligible``: has_price + 满足价格/流动性门槛（复用 backtest.eligibility_mask）

用法::

    from research.universe import build_universe_mask
    mask = build_universe_mask(engine, start=..., end=...)
    # mask["listed"]   — 每日哪些证券在上市状态
    # mask["has_price"] — 每日哪些证券有价格
    # mask["eligible"]  — 每日哪些证券可投资
"""
from __future__ import annotations

import hashlib
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.data import FACTOR_TRUST_FLOOR


def _load_security_dates(engine: Engine, *, types: tuple[str, ...] = ("CS",)) -> pd.DataFrame:
    """拉取所有证券的 id / list_date / delist_date。"""
    sql = text("""
        select s.id as security_id, s.list_date, s.delist_date
        from securities s
        where s.type = any(:types)
        order by s.id
    """)
    return pd.read_sql_query(sql, engine, params={"types": list(types)},
                             parse_dates=["list_date", "delist_date"])


def _build_listed_mask(
    security_dates: pd.DataFrame,
    dates: pd.DatetimeIndex,
    security_ids: list[int],
) -> pd.DataFrame:
    """构建上市状态 mask：list_date <= d AND (delist_date > d OR delist_date IS NULL)。"""
    mask = pd.DataFrame(False, index=dates, columns=security_ids)
    for _, row in security_dates.iterrows():
        sid = row["security_id"]
        if sid not in mask.columns:
            continue
        ld = row["list_date"]
        dd = row["delist_date"]
        if pd.isna(ld):
            # list_date 未知时保守假设始终在列
            start_idx = dates[0]
        else:
            start_idx = pd.Timestamp(ld)
        if pd.isna(dd):
            end_idx = dates[-1]
        else:
            end_idx = pd.Timestamp(dd)
        mask.loc[start_idx:end_idx, sid] = True
    return mask


def build_universe_mask(
    engine: Engine,
    *,
    start: date,
    end: date,
    types: tuple[str, ...] = ("CS",),
    adj_close: pd.DataFrame | None = None,
    close: pd.DataFrame | None = None,
    dollar_volume: pd.DataFrame | None = None,
    min_price: float = 3.0,
    min_median_dollar_volume: float = 2_000_000.0,
    liquidity_window: int = 63,
) -> dict[str, pd.DataFrame]:
    """构建分层 universe mask。

    Parameters
    ----------
    engine : 数据库引擎
    start, end : 面板日期范围
    types : 证券类型
    adj_close : 如果已有面板可传入避免重复加载；不传则只构建 listed 层
    close, dollar_volume : 用于 eligible 层；不传则 eligible = has_price
    min_price, min_median_dollar_volume, liquidity_window : eligible 门槛

    Returns
    -------
    dict with keys: "listed", "has_price", "eligible", "universe_hash"
    """
    security_dates = _load_security_dates(engine, types=types)

    # 日期索引来自面板或自行构建
    if adj_close is not None:
        dates = adj_close.index
        security_ids = list(adj_close.columns)
    else:
        dates = pd.bdate_range(start, end, freq="B")
        security_ids = sorted(security_dates["security_id"].unique().tolist())

    dates = pd.DatetimeIndex(dates)

    # Layer 1: listed
    listed = _build_listed_mask(security_dates, dates, security_ids)

    # Layer 2: has_price (从面板的非空性推断)
    if adj_close is not None:
        has_price = adj_close.notna() & listed
    else:
        has_price = listed.copy()

    # Layer 3: eligible (价格 + 流动性门槛)
    if close is not None and dollar_volume is not None:
        from research.backtest import eligibility_mask
        raw_eligible = eligibility_mask(
            close, dollar_volume,
            min_price=min_price,
            min_median_dollar_volume=min_median_dollar_volume,
            window=liquidity_window,
        )
        eligible = raw_eligible & has_price
    else:
        eligible = has_price.copy()

    # Universe hash: 可投资列集合的稳定标识
    eligible_ids = sorted(int(c) for c in eligible.columns[eligible.any(axis=0)])
    hash_input = f"{start}|{end}|{','.join(map(str, eligible_ids))}"
    universe_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    return {
        "listed": listed,
        "has_price": has_price,
        "eligible": eligible,
        "universe_hash": universe_hash,
    }


def universe_hash_from_ids(security_ids: list[int], start: date, end: date) -> str:
    """从 security_id 列表计算 universe hash（用于评估结果绑定）。"""
    sorted_ids = sorted(int(i) for i in security_ids)
    hash_input = f"{start}|{end}|{','.join(map(str, sorted_ids))}"
    return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
