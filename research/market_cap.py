"""研究层 PIT 市值面板：raw close × PIT 股本事件流。

股本事件流两段拼接（seam 设计见 research/shares.py 模块 docstring）：
vendor 段 = historical_shares（2024-06-30 起，400 天 / visible_date 锚过期），
XBRL 段 = sec_fundamental_facts 股本概念（2009+，270 天 / period_end 锚过期，
visible_delay_days=1 已烘焙进 visible_date）。段间差异以逐事件列表达：
events 带 ``stale_after`` 列时 compute_market_cap_panel 改用逐事件过期
（全局 max_staleness_days 仅作缺失兜底），带 ``split_anchor`` 列时拆股滚动
锚取该列（缺失回退 visible_date）；不带这些列的旧事件帧行为逐位不变。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.factors.asof import event_table_to_asof_panel
from research.shares import load_xbrl_shares_events, stitch_shares_events

_SHARES_COLUMNS = ["security_id", "visible_date", "period_end_date", "total_shares"]
_SPLIT_COLUMNS = ["security_id", "ex_date", "split_from", "split_to"]
_STALE_AFTER_COLUMN = "stale_after"
_SPLIT_ANCHOR_COLUMN = "split_anchor"
_NS_PER_DAY = 86_400_000_000_000


def _empty_shares_events(include_source: bool = False) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "period_end_date": pd.Series(dtype="datetime64[ns]"),
            "total_shares": pd.Series(dtype=np.int64),
        }
    )
    if include_source:
        df["source"] = pd.Series(dtype=object)
    return df


def _empty_split_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "ex_date": pd.Series(dtype="datetime64[ns]"),
            "split_from": pd.Series(dtype=np.float64),
            "split_to": pd.Series(dtype=np.float64),
        }
    )


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """统一到 ns 精度，避免 merge_asof 两侧 dtype 不一致。"""
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def load_shares_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
    include_source: bool = False,
) -> pd.DataFrame:
    """加载 historical_shares 的 PIT 可见事件流（vendor 段）。

    include_source=True 时附加 ``source`` 列（MASSIVE/POLYGON），供
    stitch_shares_events 做 MASSIVE > POLYGON 的双源去重；默认关闭以保持
    既有消费方（short_interest 等）的列形状不变。
    """
    if security_ids is not None and not security_ids:
        return _empty_shares_events(include_source)
    sql = text(
        """
        select security_id, filing_date as visible_date, period_end_date, total_shares, source
        from historical_shares
        where total_shares is not null
          and (:security_ids is null or security_id = any(:security_ids))
        order by security_id, filing_date, period_end_date, source
        """
    )
    events = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["visible_date", "period_end_date"],
    )
    if events.empty:
        return _empty_shares_events(include_source)
    events = _to_ns(events, ("visible_date", "period_end_date"))
    events["security_id"] = events["security_id"].astype(np.int64)
    events["total_shares"] = events["total_shares"].astype(np.int64)
    if include_source:
        return events[_SHARES_COLUMNS + ["source"]]
    return events[_SHARES_COLUMNS]


def load_split_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 corporate_actions 的 SPLIT 事件（ex_date 提前公告，作 PIT 可见日安全）。

    distinct 按经济键去重：同一拆股的合成/vendor 双 ID 替身只保留一行。

    spinoff 伪拆股（adjustment_type='spinoff_pseudo_split'，2026-07 allowlist 恢复导入的
    归档 P 前缀行）是分拆日的价格调整因子，不是股份数变动——股本前滚绝不消费。
    按 (security_id, ex_date) 整日抑制：同日的无标记替身（POLYGON legacy 合成行）一并
    压掉，防止 distinct 去重后标记丢失。代价是"真实拆股与分拆伪因子同日"会被一并抑制
    （vendor 通常把两者折进一个复合因子，同日双行未见实例；出现时股本靠下一次 XBRL
    申报自愈）。
    """
    if security_ids is not None and not security_ids:
        return _empty_split_events()
    sql = text(
        """
        select distinct ca.security_id, ca.ex_date,
               ca.split_from::float8 as split_from, ca.split_to::float8 as split_to
        from corporate_actions ca
        where ca.action_type = 'SPLIT'
          and ca.split_from is not null and ca.split_from > 0
          and ca.split_to is not null and ca.split_to > 0
          and not exists (
              select 1 from corporate_actions m
              where m.security_id = ca.security_id
                and m.action_type = 'SPLIT'
                and m.ex_date = ca.ex_date
                and m.adjustment_type = 'spinoff_pseudo_split')
          and (:security_ids is null or ca.security_id = any(:security_ids))
        order by security_id, ex_date
        """
    )
    splits = pd.read_sql_query(
        sql,
        engine,
        params={"security_ids": security_ids},
        parse_dates=["ex_date"],
    )
    if splits.empty:
        return _empty_split_events()
    splits = _to_ns(splits, ("ex_date",))
    splits["security_id"] = splits["security_id"].astype(np.int64)
    splits["split_from"] = splits["split_from"].astype(np.float64)
    splits["split_to"] = splits["split_to"].astype(np.float64)
    return splits[_SPLIT_COLUMNS]


def _load_raw_close_wide(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None,
) -> pd.DataFrame:
    if len(dates) == 0:
        columns = pd.Index(security_ids or [], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)
    if security_ids is not None and not security_ids:
        return pd.DataFrame(index=dates, columns=pd.Index([], dtype=np.int64), dtype=np.float64)

    id_clause = "and security_id = any(:security_ids)" if security_ids is not None else ""
    sql = text(
        f"""
        select security_id, date, close::float8 as close
        from daily_prices
        where date = any(:dates)
          and close is not null
          {id_clause}
        order by security_id, date
        """
    )
    params: dict[str, object] = {"dates": [ts.date() for ts in dates]}
    if security_ids is not None:
        params["security_ids"] = security_ids
    prices = pd.read_sql_query(sql, engine, params=params, parse_dates=["date"])
    if prices.empty:
        columns = pd.Index(security_ids or [], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)
    prices = _to_ns(prices, ("date",))
    prices["security_id"] = prices["security_id"].astype(np.int64)
    wide = prices.pivot_table(index="date", columns="security_id", values="close", aggfunc="last")
    if security_ids is not None:
        wide = wide.reindex(columns=pd.Index(security_ids, dtype=np.int64))
    return wide.reindex(dates).astype(np.float64)


def _coerce_security_columns(columns: pd.Index) -> pd.Index:
    return pd.Index([int(col) for col in columns], dtype=np.int64)


def _normalize_split_events(splits: pd.DataFrame) -> pd.DataFrame:
    """清洗 SPLIT 事件并按证券累计 log 拆股比（cum_log_ratio）。"""
    sp = splits.reindex(columns=_SPLIT_COLUMNS).copy()
    sp = sp.dropna(subset=_SPLIT_COLUMNS)
    if sp.empty:
        return sp.assign(cum_log_ratio=pd.Series(dtype=np.float64))
    sp["split_from"] = sp["split_from"].astype(np.float64)
    sp["split_to"] = sp["split_to"].astype(np.float64)
    sp = sp[(sp["split_from"] > 0) & (sp["split_to"] > 0)]
    if sp.empty:
        return sp.assign(cum_log_ratio=pd.Series(dtype=np.float64))
    sp = _to_ns(sp, ("ex_date",))
    sp["security_id"] = sp["security_id"].astype(np.int64)
    sp = sp.drop_duplicates(subset=_SPLIT_COLUMNS)
    sp = sp.sort_values(["security_id", "ex_date"], kind="mergesort").reset_index(drop=True)
    log_ratio = np.log(sp["split_to"] / sp["split_from"])
    sp["cum_log_ratio"] = log_ratio.groupby(sp["security_id"]).cumsum()
    return sp


def _split_rollforward_shares(
    shares: pd.DataFrame,
    anchor_days: pd.DataFrame,
    splits: pd.DataFrame,
) -> pd.DataFrame:
    """把 as-of 股本快照按 (锚点日, t] 内的 SPLIT 比例滚动到观测日。

    快照值已含 ex_date <= 锚点日的拆股，故乘数取半开区间：
    multiplier = exp(cumlog(t) - cumlog(anchor))。无拆股证券乘数恒为 1。
    """
    sp = _normalize_split_events(splits)
    if sp.empty:
        return shares
    split_ids = shares.columns.intersection(pd.Index(sp["security_id"].unique(), dtype=np.int64))
    if len(split_ids) == 0:
        return shares
    sp = sp[sp["security_id"].isin(split_ids)]

    long = anchor_days.loc[:, split_ids].melt(
        ignore_index=False, var_name="security_id", value_name="anchor_days"
    )
    long = long.reset_index(names="date").dropna(subset=["anchor_days"])
    if long.empty:
        return shares
    long["security_id"] = long["security_id"].astype(np.int64)
    # anchor_days 是"天数 since epoch"的 float（float64 精确表示），round 后还原成时间戳
    long["anchor_date"] = pd.to_datetime(
        long["anchor_days"].round().astype(np.int64) * _NS_PER_DAY
    )

    right = sp[["security_id", "ex_date", "cum_log_ratio"]].sort_values("ex_date", kind="mergesort")
    at_t = pd.merge_asof(
        long.sort_values("date", kind="mergesort"),
        right.rename(columns={"cum_log_ratio": "cum_log_t"}),
        left_on="date",
        right_on="ex_date",
        by="security_id",
        direction="backward",
    ).drop(columns=["ex_date"])
    at_anchor = pd.merge_asof(
        at_t.sort_values("anchor_date", kind="mergesort"),
        right.rename(columns={"cum_log_ratio": "cum_log_anchor"}),
        left_on="anchor_date",
        right_on="ex_date",
        by="security_id",
        direction="backward",
    )
    at_anchor["multiplier"] = np.exp(
        at_anchor["cum_log_t"].fillna(0.0) - at_anchor["cum_log_anchor"].fillna(0.0)
    )
    multiplier = at_anchor.pivot_table(
        index="date", columns="security_id", values="multiplier", aggfunc="last"
    )
    multiplier = multiplier.reindex(index=shares.index, columns=shares.columns).fillna(1.0)
    return shares * multiplier


def compute_market_cap_panel(
    events: pd.DataFrame,
    prices_wide: pd.DataFrame,
    dates: pd.DatetimeIndex,
    max_staleness_days: int,
    visible_delay_days: int,
    splits: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """合成事件与 raw close 宽表，计算 PIT 市值宽表。

    提供 splits（corporate_actions 的 SPLIT 事件）时，把 as-of 股本快照按
    (快照拆股锚点日, t] 内的拆股比例滚动到观测日——否则拆股 ex 日到下一次
    股本快照之间市值恰错一个拆股比（raw close 已跳变、快照仍是旧股本）。

    段间 seam（stitched 事件流，见 research/shares.py）以逐事件列表达：

    - events 带 ``stale_after`` 列（该事件最后有效观测日，含当日）时改用
      逐事件过期，行内缺失回退 visible_date + max_staleness_days；
    - events 带 ``split_anchor`` 列时拆股滚动锚取该列（XBRL 段为 period_end
      ——申报值不含测量日之后的拆股），行内缺失回退 visible_date。

    不带这些列的旧事件帧走原路径，行为逐位不变。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    prices = prices_wide.copy()
    prices.index = pd.DatetimeIndex(pd.to_datetime(prices.index)).astype("datetime64[ns]")
    prices.columns = _coerce_security_columns(prices.columns)

    extras = [
        col
        for col in (_STALE_AFTER_COLUMN, _SPLIT_ANCHOR_COLUMN)
        if col in events.columns
    ]
    ev = events.reindex(columns=_SHARES_COLUMNS + extras).copy()
    if not ev.empty:
        ev = _to_ns(ev, ("visible_date", "period_end_date", *extras))
        ev = ev[pd.notna(ev["security_id"]) & pd.notna(ev["visible_date"])]
        ev = ev[pd.notna(ev["total_shares"])]
        ev["security_id"] = ev["security_id"].astype(np.int64)
        ev["total_shares"] = ev["total_shares"].astype(np.float64)
        if _STALE_AFTER_COLUMN in extras:
            ev[_STALE_AFTER_COLUMN] = ev[_STALE_AFTER_COLUMN].fillna(
                ev["visible_date"] + pd.Timedelta(days=max_staleness_days)
            )
        if _SPLIT_ANCHOR_COLUMN in extras:
            ev[_SPLIT_ANCHOR_COLUMN] = ev[_SPLIT_ANCHOR_COLUMN].fillna(ev["visible_date"])

    # 逐事件过期：stale_after 作锚、窗口 0 天 <=> date > stale_after 置 NaN，
    # 与旧口径 visible_date 锚 + max_staleness_days 窗在 vendor 段完全等价。
    if _STALE_AFTER_COLUMN in extras:
        staleness_anchor = _STALE_AFTER_COLUMN
        effective_staleness_days = 0
    else:
        staleness_anchor = "visible_date"
        effective_staleness_days = max_staleness_days

    event_ids = pd.Index(ev["security_id"].unique(), dtype=np.int64) if not ev.empty else pd.Index([], dtype=np.int64)
    security_ids = event_ids.union(prices.columns).sort_values()
    prices = prices.reindex(index=dates, columns=security_ids).astype(np.float64)
    if len(dates) == 0 or len(security_ids) == 0:
        return pd.DataFrame(index=dates, columns=security_ids, dtype=np.float64)

    shares = event_table_to_asof_panel(
        ev,
        dates=dates,
        value_column="total_shares",
        visible_date_column="visible_date",
        staleness_anchor_column=staleness_anchor,
        visible_delay_days=visible_delay_days,
        max_staleness_days=effective_staleness_days,
        security_universe=security_ids,
    )

    if splits is not None and not splits.empty and not ev.empty:
        # 第二次 asof 取每格选中快照的拆股锚点日（与 shares 面板同参数、
        # 同排序键，逐格对应同一事件行）；只对有拆股的证券展开长表。
        anchor_ev = ev.copy()
        anchor_source = (
            anchor_ev[_SPLIT_ANCHOR_COLUMN]
            if _SPLIT_ANCHOR_COLUMN in extras
            else anchor_ev["visible_date"]
        )
        anchor_ev["_anchor_days"] = (
            anchor_source.astype("int64") // _NS_PER_DAY
        ).astype(np.float64)
        anchor_days = event_table_to_asof_panel(
            anchor_ev,
            dates=dates,
            value_column="_anchor_days",
            visible_date_column="visible_date",
            staleness_anchor_column=staleness_anchor,
            visible_delay_days=visible_delay_days,
            max_staleness_days=effective_staleness_days,
            security_universe=security_ids,
        )
        shares = _split_rollforward_shares(shares, anchor_days, splits)

    return (prices * shares).astype(np.float64)


def load_market_cap_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 400,
    visible_delay_days: int = 0,
    include_xbrl: bool = True,
) -> pd.DataFrame:
    """一站式加载 raw close、PIT shares 与 SPLIT 事件，返回市值宽表。

    include_xbrl=True（默认）时股本事件流为 vendor + XBRL 拼接（vendor 段优先，
    见 research/shares.stitch_shares_events），把股本历史从 2024-06 推深到 2009+；
    验收口径要求默认开启（size/earnings_yield 的 IC 序列不得出现 2024-06 断点）。
    include_xbrl=False 退回纯 historical_shares 的旧行为。max_staleness_days /
    visible_delay_days 仅约束 vendor 段；XBRL 段的 270 天 / period_end 过期与
    +1 天可见延迟已烘焙在事件流内。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    events = load_shares_events(engine, security_ids=security_ids, include_source=include_xbrl)
    if include_xbrl:
        xbrl_events = load_xbrl_shares_events(engine, security_ids=security_ids)
        events = stitch_shares_events(
            events, xbrl_events, vendor_max_staleness_days=max_staleness_days
        )
    splits = load_split_events(engine, security_ids=security_ids)
    prices = _load_raw_close_wide(engine, dates=dates, security_ids=security_ids)
    return compute_market_cap_panel(
        events,
        prices,
        dates,
        max_staleness_days=max_staleness_days,
        visible_delay_days=visible_delay_days,
        splits=splits,
    )


def load_log_market_cap_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    security_ids: list[int] | None = None,
    max_staleness_days: int = 400,
    visible_delay_days: int = 0,
    include_xbrl: bool = True,
) -> pd.DataFrame:
    """返回 log(PIT 市值)，非正数与缺失值保留为 NaN。"""
    market_cap = load_market_cap_panel(
        engine,
        dates=dates,
        security_ids=security_ids,
        max_staleness_days=max_staleness_days,
        visible_delay_days=visible_delay_days,
        include_xbrl=include_xbrl,
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(market_cap.where(market_cap > 0))
