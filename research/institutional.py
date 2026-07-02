"""研究层 PIT 13F 机构持仓面板（institutional_holdings）。

三个机构因子共用的加载层：
- n_holders: 持有该证券的不同 filer 数量（持仓广度）
- total_shares: 全体 filer 持股合计
- hhi: 按 market_value 占比的赫芬达尔集中度

PIT 口径：13F 截止日（period）后约 45 天才申报，故用 filing_date 作可见日，
period 作 staleness anchor。只取 shares_or_principal_type='SH' 的多头股票仓位
（排除期权 put_call、本金类 PRN）。

可见性只按原件（13F-HR）计算：修正件（13F-HR/A）可能迟到数月，若参与
visible_date 会把整季聚合推迟到修正日——旧季度遮蔽新季度、且在 staleness
门槛下整季永不可见。同一 filer 同期多份原件取 filing_date 最新的 accession，
对该 accession 内同证券的全部拆行（不同 discretion / voting authority）求和。
加载后按可见序做 period 单调守卫，迟到申报的旧季度事件直接丢弃
（同 research/fundamentals.py 事件流的守卫口径）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel


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
            "hhi": pd.Series(dtype=np.float64),
        }
    )


def _drop_period_regressions(agg: pd.DataFrame) -> pd.DataFrame:
    """可见序内 period 必须单调不减：迟到申报的旧季度事件会让 as-of 面板回退，丢弃。"""
    agg = agg.sort_values(["security_id", "visible_date", "period"])
    running_max = agg.groupby("security_id")["period"].cummax()
    return agg[agg["period"] >= running_max].reset_index(drop=True)


def load_institutional_aggregates(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """按 (security_id, period) 聚合 13F 持仓，附带该季度的 filing_date 可见日。

    只聚合原件（form_type 不含 '/A'；NULL 视同原件——写入层总会从 form index
    回填 form_type，NULL 仅是防御性容错）。同一 (filer_cik, period) 多份原件
    取 filing_date 最新的 accession，并对该 accession 内同证券多行求和。
    visible_date = 该 (security_id, period) 所含原件 accession 的 max(filing_date)。
    HHI 在事件层一并算好；返回前做 period 单调守卫。
    """
    if security_ids is not None and not security_ids:
        return _empty_agg()
    sql = text(
        """
        with filtered as (
            select security_id, period, filing_date, filer_cik, accession_number,
                   market_value::float8 as market_value,
                   shares_or_principal_amount::float8 as shares
            from institutional_holdings
            where security_id is not null
              and period is not null
              and filing_date is not null
              and shares_or_principal_type = 'SH'
              and put_call is null
              and market_value is not null
              and (form_type is null or form_type not like '%/A%')
              and (cast(:security_ids as bigint[]) is null
                   or security_id = any(cast(:security_ids as bigint[])))
        ),
        chosen as (
            -- 同一 (filer_cik, period) 多份原件时选定唯一 accession（filing_date 最新）
            select distinct on (filer_cik, period)
                   filer_cik, period, accession_number, filing_date
            from filtered
            order by filer_cik, period, filing_date desc, accession_number desc
        ),
        per_filer as (
            -- 选定 accession 内同证券多行（不同 discretion / voting authority 拆行）求和
            select f.security_id, f.period, f.filer_cik,
                   c.filing_date,
                   sum(f.market_value) as filer_value,
                   sum(f.shares) as filer_shares
            from filtered f
            join chosen c using (filer_cik, period, accession_number)
            group by f.security_id, f.period, f.filer_cik, c.filing_date
        )
        select security_id,
               period,
               max(filing_date) as visible_date,
               count(distinct filer_cik) as n_holders,
               sum(filer_value) as total_value,
               sum(filer_shares) as total_shares,
               sum(power(filer_value, 2)) as sum_sq_value
        from per_filer
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
        return _empty_agg()
    agg = _to_ns(agg, ("visible_date", "period"))
    agg["security_id"] = agg["security_id"].astype(np.int64)
    for col in ("n_holders", "total_value", "total_shares", "sum_sq_value"):
        agg[col] = agg[col].astype(np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        agg["hhi"] = agg["sum_sq_value"] / agg["total_value"].where(agg["total_value"] > 0) ** 2
    return _drop_period_regressions(agg)


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
    保证某日看到的是"截至该日最新已申报季度的环比"。聚合层的 period
    单调守卫保证上季基数的 visible_date 不晚于本季事件——基数不含未来值。
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
