"""研究层 PIT 13F 机构持仓面板（institutional_holdings）。

三个机构因子共用的加载层：
- n_holders: 持有该证券的不同 filer 数量（持仓广度）
- total_shares: 全体 filer 持股合计
- hhi: 按 market_value 占比的赫芬达尔集中度

PIT 口径：13F 截止日（period）后约 45 天才申报，故用 filing_date 作可见日，
period 作 staleness anchor。只取 shares_or_principal_type='SH' 的多头股票仓位
（排除期权 put_call、本金类 PRN）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel

_AGG_COLUMNS = ["security_id", "visible_date", "period", "n_holders", "total_value", "total_shares"]
_ROW_COLUMNS = ["security_id", "visible_date", "period", "filer_cik", "market_value"]


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def _empty_agg() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "period": pd.Series(dtype="datetime64[ns]"),
            "n_holders": pd.Series(dtype=np.float64),
            "total_value": pd.Series(dtype=np.float64),
            "total_shares": pd.Series(dtype=np.float64),
        }
    )


def load_institutional_aggregates(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """按 (security_id, period) 聚合 13F 持仓，附带该季度的 filing_date 可见日。

    可见日取该 (security_id, period) 内最晚的 filing_date：保证截面里所有
    filer 都已申报后才让该季度的聚合值可见，避免用到未来才到的申报行。
    HHI 在事件层一并算好。
    """
    if security_ids is not None and not security_ids:
        return _empty_agg()
    sql = text(
        """
        with rows as (
            select security_id, period, filing_date, filer_cik,
                   market_value::float8 as market_value,
                   shares_or_principal_amount::float8 as shares
            from institutional_holdings
            where security_id is not null
              and period is not null
              and filing_date is not null
              and shares_or_principal_type = 'SH'
              and put_call is null
              and (cast(:security_ids as bigint[]) is null
                   or security_id = any(cast(:security_ids as bigint[])))
        )
        select security_id,
               period,
               max(filing_date) as visible_date,
               count(distinct filer_cik) as n_holders,
               sum(market_value) as total_value,
               sum(shares) as total_shares,
               sum(power(market_value, 2)) as sum_sq_value
        from rows
        group by security_id, period
        order by security_id, period
        """
    )
    agg = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "period"],
    )
    if agg.empty:
        empty = _empty_agg()
        empty["hhi"] = pd.Series(dtype=np.float64)
        return empty
    agg = _to_ns(agg, ("visible_date", "period"))
    agg["security_id"] = agg["security_id"].astype(np.int64)
    for col in ("n_holders", "total_value", "total_shares", "sum_sq_value"):
        agg[col] = agg[col].astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        agg["hhi"] = agg["sum_sq_value"] / agg["total_value"].where(agg["total_value"] > 0) ** 2
    return agg


def load_institutional_holdings_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 200,
) -> dict[str, pd.DataFrame]:
    """加载季度 13F 聚合，返回若干 PIT 宽表。

    返回 dict:
    - 'n_holders'    : 不同 filer 数量
    - 'total_value'  : market_value 合计
    - 'total_shares' : 持股合计
    - 'hhi'          : market_value 占比的赫芬达尔指数
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    agg = load_institutional_aggregates(engine, security_ids=security_ids)

    def _panel(value_column: str) -> pd.DataFrame:
        panel = event_table_to_asof_panel(
            agg,
            dates=dates,
            value_column=value_column,
            visible_date_column="visible_date",
            staleness_anchor_column="period",
            visible_delay_days=0,
            max_staleness_days=max_staleness_days,
            security_universe=requested_security_ids,
        )
        if requested_security_ids is not None:
            panel = panel.reindex(columns=requested_security_ids)
        return panel.astype(np.float64)

    return {
        "n_holders": _panel("n_holders"),
        "total_value": _panel("total_value"),
        "total_shares": _panel("total_shares"),
        "hhi": _panel("hhi"),
    }


def load_delta_institutional_ownership_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 200,
) -> pd.DataFrame:
    """季度环比机构持股变动率：(本季 total_shares - 上季) / 上季。

    在事件层先按 period 排出每证券的上一季持股，算好比率再做 as-of，
    保证某日看到的是"截至该日最新已申报季度的环比"。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    requested_security_ids = None
    if security_ids is not None:
        requested_security_ids = pd.Index(security_ids, dtype=np.int64).drop_duplicates()
        security_ids = requested_security_ids.tolist()

    agg = load_institutional_aggregates(engine, security_ids=security_ids)
    if agg.empty:
        return pd.DataFrame(
            index=dates,
            columns=requested_security_ids if requested_security_ids is not None else pd.Index([], dtype=np.int64),
            dtype=np.float64,
        )

    agg = agg.sort_values(["security_id", "period"]).copy()
    prior = agg.groupby("security_id")["total_shares"].shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        agg["delta_ownership"] = (agg["total_shares"] - prior) / prior.where(prior > 0)

    panel = event_table_to_asof_panel(
        agg,
        dates=dates,
        value_column="delta_ownership",
        visible_date_column="visible_date",
        staleness_anchor_column="period",
        visible_delay_days=0,
        max_staleness_days=max_staleness_days,
        security_universe=requested_security_ids,
    )
    if requested_security_ids is not None:
        panel = panel.reindex(columns=requested_security_ids)
    return panel.astype(np.float64)
