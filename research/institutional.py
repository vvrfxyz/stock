"""研究层 PIT 13F 机构持仓面板（institutional_holdings）。

三个机构因子共用的加载层：
- n_holders: 持有该证券的不同 filer 数量（持仓广度）
- total_shares: 全体 filer 持股合计
- hhi: 按 market_value 占比的赫芬达尔集中度

PIT 口径：13F 截止日（period）后 45 天内申报，故用 filing_date 作可见日，
period 作 staleness anchor。只取 shares_or_principal_type='SH' 的多头股票仓位
（排除期权 put_call、本金类 PRN）。

两段式可见性（2026-07 引入）：单条聚合事件的 visible_date 取整组原件的
max(filing_date)，会被一家迟交机构拖垮——生产实测可见延迟 p50=98 天（法定
截止 45 天），叠加 staleness=200 天后 >25% 证券季度"生而过期"。改为对每个
(security_id, period) 最多发两条事件：

1. 准时批事件：只聚合截止日（period + ONTIME_DEADLINE_DAYS 天）前已申报的
   原件；visible_date = 该子集 max(filing_date)。语义：截止日时用当时已公开
   的申报出首版聚合，零前视。
2. 终版事件：聚合全部已选原件；visible_date = max(全部原件 filing_date)。

两者 visible_date 相同（无迟交者）时只发终版一条；准时批为空（全部迟交，
极罕见）时也只发终版。as-of 层（merge_asof backward + period 锚 staleness）
自然会：截止日起用准时批值，终版到达后切换到完整值；迟交超过 staleness 的
终版自然作废，但准时批在 staleness 窗口内始终有效——这正是修复点。

可见性只按原件（13F-HR）计算：修正件（13F-HR/A）可能迟到数月，若参与
visible_date 会把整季聚合推迟到修正日——旧季度遮蔽新季度、且在 staleness
门槛下整季永不可见。同一 filer 同期多份原件按口径分别选定唯一 accession
（终版取全局 filing_date 最新；准时批取截止日前 filing_date 最新），对该
accession 内同证券的全部拆行（不同 discretion / voting authority）求和。
加载后按可见序做 period 单调守卫：迟到申报的旧季度事件直接丢弃（同
research/fundamentals.py 事件流的守卫口径）；同 (security, period) 的两条
事件 period 相同，天然通过守卫，而旧期间迟到的终版若晚于新期间事件可见，
会被守卫丢弃——面板由该旧期间的准时批继续支撑，不回退。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel

# 13F 法定截止为 period 后 45 天，+1 天容忍截止日恰逢周末/节假日顺延的常见情形。
ONTIME_DEADLINE_DAYS = 46


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
    """可见序内 period 必须单调不减：迟到申报的旧季度事件会让 as-of 面板回退，丢弃。

    两段式下同 (security, period) 的准时批/终版两条事件 period 相同，按
    visible_date 先后天然通过；旧期间的终版若可见晚于新期间事件（straggler
    在下季申报后才补交），running max 已推进到新期间，该终版被丢弃——面板
    由旧期间的准时批支撑到 staleness 到期，不回退。
    """
    agg = agg.sort_values(["security_id", "visible_date", "period"])
    running_max = agg.groupby("security_id")["period"].cummax()
    return agg[agg["period"] >= running_max].reset_index(drop=True)


def load_institutional_aggregates(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """按 (security_id, period) 聚合 13F 持仓，两段式可见性，单次查询。

    只聚合原件（form_type 不含 '/A'；NULL 视同原件——写入层总会从 form index
    回填 form_type，NULL 仅是防御性容错）。对每个 (filer_cik, period) 按口径
    选定唯一 accession：终版口径取全局 filing_date 最新；准时批口径取截止日
    （period + ONTIME_DEADLINE_DAYS 天）前 filing_date 最新。选定 accession 内
    同证券多行求和。

    每个 (security_id, period) 最多两行：
    - 准时批：只含截止日前申报的 filer，visible_date = 该子集 max(filing_date)；
      仅当存在迟交者（准时批 visible_date < 终版 visible_date）时发出。
    - 终版：全部 filer，visible_date = max(全部 filing_date)；总是发出（该证券
      该期有任何原件行时）。

    HHI 在事件层一并算好；返回前做 period 单调守卫。

    SQL 设计：accession 目录（distinct 后仅万级行）上做窗口排名选出两种口径的
    accession，与千万级持仓行只做一次 join、一次 per-filer 聚合，再用 FILTER
    聚合一趟出两段，最后 UNION 展开——避免对 institutional_holdings 的多次扫描
    与千万级排序。
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
        accessions as (
            -- accession 目录：去重后仅万级行，排名在这里做，避免对千万级持仓行排序
            select distinct filer_cik, period, accession_number, filing_date
            from filtered
        ),
        ranked as (
            select filer_cik, period, accession_number, filing_date,
                   filing_date <= period + cast(:ontime_days as integer) as is_ontime,
                   row_number() over (
                       partition by filer_cik, period
                       order by filing_date desc, accession_number desc) as rn_all,
                   row_number() over (
                       partition by filer_cik, period,
                                    filing_date <= period + cast(:ontime_days as integer)
                       order by filing_date desc, accession_number desc) as rn_grp
            from accessions
        ),
        picks as (
            -- 每 (filer, period) 至多两份入选 accession：
            -- 终版口径取全局最新（in_final），准时批口径取截止日前最新（in_ontime）；
            -- 两口径命中同一 accession 时合并为一行、双 flag 置真
            select filer_cik, period, accession_number, filing_date,
                   (rn_all = 1) as in_final,
                   (is_ontime and rn_grp = 1) as in_ontime
            from ranked
            where rn_all = 1 or (is_ontime and rn_grp = 1)
        ),
        per_filer as (
            -- 选定 accession 内同证券多行（不同 discretion / voting authority 拆行）求和
            select f.security_id, f.period, f.filer_cik,
                   p.filing_date, p.in_final, p.in_ontime,
                   sum(f.market_value) as filer_value,
                   sum(f.shares) as filer_shares
            from filtered f
            join picks p using (filer_cik, period, accession_number)
            group by f.security_id, f.period, f.filer_cik,
                     p.filing_date, p.in_final, p.in_ontime
        ),
        staged as (
            -- 单趟 FILTER 聚合同时得到准时批/终版两段
            select security_id, period,
                   max(filing_date) filter (where in_final) as final_visible,
                   count(distinct filer_cik) filter (where in_final) as final_holders,
                   sum(filer_value) filter (where in_final) as final_value,
                   sum(filer_shares) filter (where in_final) as final_shares,
                   sum(power(filer_value, 2)) filter (where in_final) as final_sq,
                   max(filing_date) filter (where in_ontime) as ontime_visible,
                   count(distinct filer_cik) filter (where in_ontime) as ontime_holders,
                   sum(filer_value) filter (where in_ontime) as ontime_value,
                   sum(filer_shares) filter (where in_ontime) as ontime_shares,
                   sum(power(filer_value, 2)) filter (where in_ontime) as ontime_sq
            from per_filer
            group by security_id, period
        )
        select security_id, period, ontime_visible as visible_date,
               ontime_holders as n_holders, ontime_value as total_value,
               ontime_shares as total_shares, ontime_sq as sum_sq_value
        from staged
        where ontime_visible is not null
          and (final_visible is null or ontime_visible < final_visible)
        union all
        select security_id, period, final_visible,
               final_holders, final_value, final_shares, final_sq
        from staged
        where final_visible is not null
        order by security_id, period, visible_date
        """
    )
    agg = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids, "ontime_days": ONTIME_DEADLINE_DAYS},
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

    事件流为两段式（见模块 docstring）：merge_asof backward 使面板在准时批
    visible_date 起显示首版值，终版到达后切换到完整值；两条事件共享 period
    锚，staleness 判定一致。

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

    在事件层先配好上季基数、算好比率再做 as-of，保证某日看到的是"截至该日
    最新已申报季度的环比"。

    两段式配对规则：上季（守卫后事件流中该证券紧邻的上一 period）可能有
    准时批/终版两条候选，基数取 visible_date <= 本事件 visible_date 的最新
    一条——即"本事件可见时点上、上季已公开的最新聚合值"；无候选则 NaN。
    禁止上季终版可见晚于本季准时批时被用作基数（前视）：该场景下守卫已把
    迟到的上季终版丢弃，且 merge_asof backward 的 visible_date 规则独立兜底。
    本季的准时批/终版各自成一条 delta 事件（基数可不同），as-of 面板先显示
    准时批口径的环比，终版到达后切换。
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

    events = agg.copy().reset_index(drop=True)
    # 每证券的期间序列去重后 shift，得到紧邻上一 period（两段式下同期两事件共享同一上季）
    period_map = (
        events[["security_id", "period"]]
        .drop_duplicates()
        .sort_values(["security_id", "period"])
    )
    period_map["prior_period"] = period_map.groupby("security_id")["period"].shift(1)
    events = events.merge(period_map, on=["security_id", "period"], how="left")

    # 上季候选表：按 (security, prior_period) merge_asof backward 取
    # visible_date <= 本事件 visible_date 的最新一条作基数
    base = events[["security_id", "period", "visible_date", "total_shares"]].rename(
        columns={
            "period": "prior_period",
            "visible_date": "prior_visible_date",
            "total_shares": "prior_shares",
        }
    )
    events["prior_shares"] = np.nan
    has_prior = events["prior_period"].notna()
    if has_prior.any():
        paired = pd.merge_asof(
            events.loc[has_prior, ["security_id", "visible_date", "prior_period"]]
            .reset_index()
            .sort_values("visible_date", kind="mergesort"),
            base.sort_values("prior_visible_date", kind="mergesort"),
            left_on="visible_date",
            right_on="prior_visible_date",
            by=["security_id", "prior_period"],
            direction="backward",
        )
        events.loc[paired["index"].to_numpy(), "prior_shares"] = paired["prior_shares"].to_numpy()

    prior = events["prior_shares"]
    with np.errstate(divide="ignore", invalid="ignore"):
        events["delta_ownership"] = (events["total_shares"] - prior) / prior.where(prior > 0)

    panel = event_table_to_asof_panel(
        events,
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
