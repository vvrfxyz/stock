"""研究层 PIT 股本事件流：XBRL 段（sec_fundamental_facts）与 vendor 段拼接。

两段数据源、一条事件流（只读，绝不回写事实表）：

- XBRL 段：`sec_fundamental_facts` 里的股本概念
  ``dei:EntityCommonStockSharesOutstanding``（封面披露，优先）coalesce
  ``us-gaap:CommonStockSharesOutstanding``——与 research/fundamentals.py 的
  shares_outstanding MetricSpec 同一优先级。可见性沿 fundamentals 惯例：
  visible = max(filed_date, accepted_at 的美东自然日) + 1 天（visible_delay_days=1
  已烘焙进 visible_date），新鲜度 270 天、锚 period_end（stale_after 列）。
- vendor 段：`historical_shares`（2024-06-30 起，MASSIVE/POLYGON），由
  `research.market_cap.load_shares_events(include_source=True)` 提供，保持既有
  400 天 / visible_date 锚的过期口径（stale_after 在 stitch 时按段烘焙）。

拼接语义（`stitch_shares_events`，"vendor 段优先"）：

- 同一 (security_id, visible_date) 的 vendor 双源行按 MASSIVE > POLYGON 去重；
- 每只证券自第一条 vendor 事件的 visible_date 起，之后（含当日）可见的 XBRL
  事件全部被 vendor 段取代（丢弃）；更早的 XBRL 事件保留，as-of 合并时自然
  被首条 vendor 事件接管。

段间差异全部体现为逐事件列（seam 的实现方式）：

- ``stale_after``：该事件的最后有效观测日（含当日）。XBRL = period_end + 270 天；
  vendor = visible_date + vendor_max_staleness_days（默认 400）。
  compute_market_cap_panel 见到该列即改用逐事件过期，替代全局 max_staleness_days。
- ``split_anchor``：拆股滚动锚——该事件股本值"已含哪些拆股"的截止日。
  XBRL 股本按 period_end（封面测量日）口径申报，period_end 与 filed_date 之间
  发生的拆股不在申报值里，锚必须取 period_end；vendor 快照沿既有口径锚
  visible_date。

第一期限制（单类股）：dei 的多维度（多类股逐类）事实被 companyfacts 排除，
us-gaap:CommonStockSharesOutstanding 对多类股是公司总股本且只挂 CIK 下最小
security_id（如 Alphabet 只挂 googl，无 dei 行）——本模块不做跨类分摊，多类股
的逐类股本留给公司实体任务（todo_crsp_grade 任务 2）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

# dei 封面披露优先，us-gaap 资产负债表口径兜底（与 fundamentals.METRICS 一致）
XBRL_SHARES_CONCEPTS: tuple[str, ...] = (
    "EntityCommonStockSharesOutstanding",
    "CommonStockSharesOutstanding",
)
XBRL_VISIBLE_DELAY_DAYS = 1
XBRL_MAX_STALENESS_DAYS = 270
VENDOR_MAX_STALENESS_DAYS = 400

_VENDOR_SOURCE_PRIORITY = {"MASSIVE": 0, "POLYGON": 1}

STITCHED_COLUMNS = [
    "security_id",
    "visible_date",
    "period_end_date",
    "total_shares",
    "source",
    "stale_after",
    "split_anchor",
]


def _empty_stitched_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "visible_date": pd.Series(dtype="datetime64[ns]"),
            "period_end_date": pd.Series(dtype="datetime64[ns]"),
            "total_shares": pd.Series(dtype=np.int64),
            "source": pd.Series(dtype=object),
            "stale_after": pd.Series(dtype="datetime64[ns]"),
            "split_anchor": pd.Series(dtype="datetime64[ns]"),
        }
    )


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def load_xbrl_shares_events(
    engine: Engine,
    *,
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 XBRL 股本事件流（stitched 形状，source='XBRL'）。

    口径（沿 research/fundamentals.py）：

    - unit='shares'、value>0（库内存在 268 条 value<=0 的脏 dei 行）、
      security_id 非空（未映射 CIK 的事实不进面板）；
    - vintage 压缩：同 (security, concept, period) 只保留首报与数值变化的申报；
    - visible = max(filed_date, accepted_at 的美东自然日) + 1 天（盘后申报
      不得被 t 日收盘建仓看到；+1 天已烘焙进 visible_date）；
    - 同 (security, visible_date) 多行时 dei 优先于 us-gaap（MetricSpec 优先级），
      再取最大 period_end（同日多报取最新测量）；
    - 可见序内 period_end 单调不减：迟到重报旧期的事件会让 as-of 序列倒退，丢弃；
    - stale_after = period_end + 270 天；split_anchor = period_end
      （XBRL 股本是申报期测量日口径，不随其后拆股调整——语义见模块 docstring）。
    """
    if security_ids is not None and not security_ids:
        return _empty_stitched_events()
    sql = text(
        """
        select security_id, concept, period_end, filed_date, accepted_at, value
        from (
            select f.security_id, f.concept, f.period_end, f.filed_date,
                   (
                       select max(sf.accepted_at)
                       from sec_filings sf
                       where sf.accession_number = f.accession_number
                   ) as accepted_at,
                   f.value::float8 as value,
                   lag(f.value::float8) over (
                       partition by f.security_id, f.concept, f.unit,
                                    f.period_start, f.period_end
                       order by f.filed_date, f.accession_number
                   ) as prev_value
            from sec_fundamental_facts f
            where f.concept = any(:concepts)
              and f.unit = 'shares'
              and f.value > 0
              and f.security_id is not null
              and f.period_end between '1995-01-01' and '2035-12-31'
              and (:security_ids is null or f.security_id = any(:security_ids))
        ) v
        where v.prev_value is null or v.prev_value <> v.value
        order by security_id, filed_date, period_end
        """
    )
    df = pd.read_sql_query(
        sql,
        engine,
        params={"concepts": list(XBRL_SHARES_CONCEPTS), "security_ids": security_ids},
        parse_dates=["period_end", "filed_date", "accepted_at"],
    )
    if df.empty:
        return _empty_stitched_events()
    df = _to_ns(df, ("period_end", "filed_date"))

    # PIT 可见日 = max(filed_date, accepted_at 的美东自然日)（同 fundamentals）
    accepted = pd.to_datetime(df["accepted_at"], utc=True)
    accepted_date = (
        accepted.dt.tz_convert("America/New_York").dt.tz_localize(None).dt.normalize()
    )
    df["visible_date"] = df["filed_date"].where(
        accepted_date.isna() | (accepted_date <= df["filed_date"]),
        accepted_date,
    ) + pd.Timedelta(days=XBRL_VISIBLE_DELAY_DAYS)
    df["visible_date"] = df["visible_date"].astype("datetime64[ns]")

    # 同 (security, visible_date)：dei 优先，其次取最大 period_end
    rank_map = {concept: rank for rank, concept in enumerate(XBRL_SHARES_CONCEPTS)}
    df["_rank"] = df["concept"].map(rank_map)
    best_rank = df.groupby(["security_id", "visible_date"])["_rank"].transform("min")
    df = df[df["_rank"] == best_rank]
    df = df.sort_values(
        ["security_id", "visible_date", "period_end"], kind="mergesort"
    ).drop_duplicates(["security_id", "visible_date"], keep="last")

    # 可见序内 period_end 单调不减（同 fundamentals.build_metric_events）
    running_max = df.groupby("security_id")["period_end"].cummax()
    df = df[df["period_end"] >= running_max]
    if df.empty:
        return _empty_stitched_events()

    out = pd.DataFrame(
        {
            "security_id": df["security_id"].astype(np.int64),
            "visible_date": df["visible_date"],
            "period_end_date": df["period_end"],
            # 股本是整数股（float8 在 2^53 内精确），round 兜底浮点尾差
            "total_shares": df["value"].round().astype(np.int64),
            "source": "XBRL",
            "stale_after": df["period_end"]
            + pd.Timedelta(days=XBRL_MAX_STALENESS_DAYS),
            "split_anchor": df["period_end"],
        }
    ).reset_index(drop=True)
    return out[STITCHED_COLUMNS]


def stitch_shares_events(
    vendor_events: pd.DataFrame,
    xbrl_events: pd.DataFrame,
    *,
    vendor_max_staleness_days: int = VENDOR_MAX_STALENESS_DAYS,
) -> pd.DataFrame:
    """vendor 段优先拼接：vendor（historical_shares）+ XBRL 事件流 -> stitched 事件流。

    - vendor 双源去重：同 (security_id, visible_date) 按 MASSIVE > POLYGON 取一行
      （同源优先取最大 period_end_date）；vendor_events 无 source 列时跳过源间
      优先级，仅做 (security_id, visible_date) 去重；
    - 供给侧接缝：每只证券自首条 vendor 事件 visible_date 起（含当日），之后可见的
      XBRL 事件全部丢弃——vendor 段接管后 XBRL 不再回插；
    - 逐事件过期/拆股锚列按段烘焙（见模块 docstring）：vendor stale_after =
      visible_date + vendor_max_staleness_days、split_anchor = visible_date（既有
      口径）；XBRL 行若缺列则按 270 天 / period_end 规则补齐。
    """
    v = vendor_events.copy() if vendor_events is not None else pd.DataFrame()
    x = xbrl_events.copy() if xbrl_events is not None else pd.DataFrame()

    if not v.empty:
        v = _to_ns(v, ("visible_date", "period_end_date"))
        if "source" not in v.columns:
            v["source"] = "VENDOR"
        v["_prio"] = v["source"].map(_VENDOR_SOURCE_PRIORITY).fillna(99).astype(int)
        v = v.sort_values(
            ["security_id", "visible_date", "_prio", "period_end_date"],
            ascending=[True, True, True, False],
            kind="mergesort",
        ).drop_duplicates(["security_id", "visible_date"], keep="first")
        v = v.drop(columns=["_prio"])
        v["stale_after"] = v["visible_date"] + pd.Timedelta(days=vendor_max_staleness_days)
        v["split_anchor"] = v["visible_date"]
        v = v.reindex(columns=STITCHED_COLUMNS)

    if not x.empty:
        x = _to_ns(x, ("visible_date", "period_end_date"))
        if "source" not in x.columns:
            x["source"] = "XBRL"
        if "stale_after" not in x.columns:
            x["stale_after"] = x["period_end_date"] + pd.Timedelta(
                days=XBRL_MAX_STALENESS_DAYS
            )
        if "split_anchor" not in x.columns:
            x["split_anchor"] = x["period_end_date"]
        x = x.reindex(columns=STITCHED_COLUMNS)
        if not v.empty:
            vendor_start = v.groupby("security_id")["visible_date"].min()
            starts = x["security_id"].map(vendor_start)
            x = x[starts.isna() | (x["visible_date"] < starts)]

    parts = [df for df in (v, x) if not df.empty]
    if not parts:
        return _empty_stitched_events()
    stitched = pd.concat(parts, ignore_index=True)
    stitched = _to_ns(stitched, ("visible_date", "period_end_date", "stale_after", "split_anchor"))
    stitched["security_id"] = stitched["security_id"].astype(np.int64)
    stitched = stitched.sort_values(
        ["security_id", "visible_date", "period_end_date"], kind="mergesort"
    ).reset_index(drop=True)
    return stitched[STITCHED_COLUMNS]
