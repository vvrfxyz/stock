"""研究用基本面读取端归一化：sec_fundamental_facts -> point-in-time 指标面板。

口径约定（与 research/data.py 的防未来函数原则一致）：
- point-in-time 取"首次申报值"（as-originally-filed）：同一 (security, concept,
  period) 多次申报（重述/后续年报的比较期重列）只取 filed_date 最早一行，
  其可见日 = 该 filed_date。任意 as_of t 只使用 visible_date <= t 的事实，
  filed_date == t 当日记为可见。重述感知的 as-of 选择留待后续版本。
- flow 指标构造 TTM：年度事实（约 12 个月 duration）直接作为 TTM；
  季度/半年/三季 YTD 事实用 TTM = YTD + 上一财年全年 - 去年同期 YTD，
  三个分量必须来自同一 concept（避免营收同义概念混算），可见日取三者
  filed_date 的最大值；分量缺失则不产出该期 TTM（不外推）。
- 营收等同义概念的 coalesce 在 TTM 事件层做：同一 (security, period_end)
  多个 concept 产出事件时按 MetricSpec.concepts 的优先级取一个。
- instant 指标（资产负债表科目）取 as_of 时点最新可见的报告期值。
- 金额事实只取 unit='USD'（库内存在 CAD/CNY 等外币申报的 FPI 事实，
  不做折算、直接排除）；股数取 unit='shares'。
- 面板有新鲜度门槛 max_staleness_days：period_end 落后 as_of 超过该天数
  视为停止披露（退市/迟报），置 NaN，避免横截面里携带僵尸值。

EPS 不直接给 TTM（季度 EPS 简单求和受股本变动扭曲），用
net_income_ttm / shares_outstanding 在读取端自行推导。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class MetricSpec:
    name: str
    concepts: tuple[str, ...]  # coalesce 优先级，高 -> 低
    kind: str  # "flow"（TTM）| "instant"（时点值）
    unit: str = "USD"


METRICS: dict[str, MetricSpec] = {
    spec.name: spec
    for spec in (
        MetricSpec(
            "revenue_ttm",
            (
                "Revenues",
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "RevenueFromContractWithCustomerIncludingAssessedTax",
                "SalesRevenueNet",
            ),
            "flow",
        ),
        MetricSpec("net_income_ttm", ("NetIncomeLoss",), "flow"),
        MetricSpec("operating_income_ttm", ("OperatingIncomeLoss",), "flow"),
        MetricSpec("gross_profit_ttm", ("GrossProfit",), "flow"),
        MetricSpec(
            "operating_cash_flow_ttm",
            ("NetCashProvidedByUsedInOperatingActivities",),
            "flow",
        ),
        MetricSpec(
            "capex_ttm",
            ("PaymentsToAcquirePropertyPlantAndEquipment",),
            "flow",
        ),
        MetricSpec("assets", ("Assets",), "instant"),
        MetricSpec(
            "equity",
            (
                "StockholdersEquity",
                "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
            ),
            "instant",
        ),
        MetricSpec(
            "shares_outstanding",
            ("EntityCommonStockSharesOutstanding", "CommonStockSharesOutstanding"),
            "instant",
            unit="shares",
        ),
    )
}

DEFAULT_METRICS: tuple[str, ...] = tuple(METRICS)

# duration 分类边界：period_end - period_start 的天数。
# 上界覆盖 53 周财年（371 天）与 14 周季度，区间外（过渡期等异形 period）丢弃。
_DURATION_CLASSES: dict[str, tuple[int, int]] = {
    "Q": (80, 100),
    "H": (170, 192),
    "N": (258, 285),
    "Y": (350, 380),
}
_PRIOR_FY_TOLERANCE_DAYS = 7  # YTD 的 period_start - 1 天 与上一财年 period_end 的容差
_PRIOR_YTD_TOLERANCE_DAYS = 14  # 去年同期 YTD 的 period_end 与 (period_end - 365) 的容差


def _concept_lookup(metrics: tuple[str, ...]) -> pd.DataFrame:
    """concept -> (metric, 优先级序号, 单位) 的映射表。"""
    rows = []
    for name in metrics:
        spec = METRICS[name]
        for rank, concept in enumerate(spec.concepts):
            rows.append((concept, spec.name, rank, spec.kind, spec.unit))
    return pd.DataFrame(rows, columns=["concept", "metric", "rank", "kind", "unit"])


def load_fundamental_facts(
    engine: Engine,
    *,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
    types: tuple[str, ...] = ("CS",),
    security_ids: list[int] | None = None,
) -> pd.DataFrame:
    """拉取所需 concept 的首次申报事实（长表）。

    DISTINCT ON 取每个 (security, concept, period) filed_date 最早的一行；
    period_end 的合理性过滤顺带剔除库内极少数申报方笔误日期（如 2201 年）。
    """
    lookup = _concept_lookup(metrics)
    id_clause = "and f.security_id = any(:security_ids)" if security_ids else ""
    sql = text(
        f"""
        select distinct on (f.security_id, f.concept, f.period_start, f.period_end)
               f.security_id, f.concept, f.unit, f.is_instant,
               f.period_start, f.period_end, f.filed_date,
               f.value::float8 as value
        from sec_fundamental_facts f
        join securities s on s.id = f.security_id
        where f.concept = any(:concepts)
          and f.unit = any(:units)
          and f.security_id is not null
          and f.period_end between '1995-01-01' and '2035-12-31'
          and f.period_start between '1995-01-01' and '2035-12-31'
          and s.type = any(:types) {id_clause}
        order by f.security_id, f.concept, f.period_start, f.period_end,
                 f.filed_date, f.accession_number
        """
    )
    params: dict = {
        "concepts": lookup["concept"].tolist(),
        "units": sorted(lookup["unit"].unique().tolist()),
        "types": list(types),
    }
    if security_ids:
        params["security_ids"] = security_ids
    chunks = pd.read_sql_query(
        sql,
        engine,
        params=params,
        chunksize=500_000,
        parse_dates=["period_start", "period_end", "filed_date"],
    )
    df = pd.concat(list(chunks), ignore_index=True)
    df = _to_ns(df, ("period_start", "period_end", "filed_date"))
    # concept 与 unit 必须按 spec 配对（金额概念不会有 shares 单位，反之亦然；此处兜底）
    df = df.merge(lookup, on=["concept", "unit"], how="inner")
    df["security_id"] = df["security_id"].astype(np.int64)
    return df


def _annual_events(flow: pd.DataFrame) -> pd.DataFrame:
    annual = flow[flow["dur_class"] == "Y"]
    return pd.DataFrame(
        {
            "security_id": annual["security_id"],
            "concept": annual["concept"],
            "period_end": annual["period_end"],
            "visible_date": annual["filed_date"],
            "value": annual["value"],
        }
    )


def _derived_ttm_events(flow: pd.DataFrame) -> pd.DataFrame:
    """YTD + 上一财年 - 去年同期 YTD，按 (security, concept) 内部向量化匹配。"""
    ytd = flow[flow["dur_class"].isin(("Q", "H", "N"))].copy()
    annual = flow[flow["dur_class"] == "Y"]
    if ytd.empty or annual.empty:
        return _EMPTY_EVENTS.copy()

    # 上一财年：period_end 应恰为 YTD period_start 的前一天；分量锁定同一 concept
    ytd["fy_target"] = ytd["period_start"] - pd.Timedelta(days=1)
    fy = annual[["security_id", "concept", "period_end", "filed_date", "value"]].rename(
        columns={"period_end": "fy_end", "filed_date": "fy_filed", "value": "fy_value"}
    )
    merged = pd.merge_asof(
        ytd.sort_values("fy_target"),
        fy.sort_values("fy_end"),
        left_on="fy_target",
        right_on="fy_end",
        by=["security_id", "concept"],
        direction="nearest",
        tolerance=pd.Timedelta(days=_PRIOR_FY_TOLERANCE_DAYS),
    )

    # 去年同期 YTD：同 concept 同 duration class，period_end 在 (period_end - 365) ± 容差内
    prior = ytd[
        ["security_id", "concept", "dur_class", "period_end", "filed_date", "value"]
    ].rename(
        columns={
            "period_end": "prior_end",
            "filed_date": "prior_filed",
            "value": "prior_value",
        }
    )
    merged["prior_target"] = merged["period_end"] - pd.Timedelta(days=365)
    merged = pd.merge_asof(
        merged.sort_values("prior_target"),
        prior.sort_values("prior_end"),
        left_on="prior_target",
        right_on="prior_end",
        by=["security_id", "concept", "dur_class"],
        direction="nearest",
        tolerance=pd.Timedelta(days=_PRIOR_YTD_TOLERANCE_DAYS),
    )

    ok = merged.dropna(subset=["fy_value", "prior_value"])
    return pd.DataFrame(
        {
            "security_id": ok["security_id"],
            "concept": ok["concept"],
            "period_end": ok["period_end"],
            "visible_date": ok[["filed_date", "fy_filed", "prior_filed"]].max(axis=1),
            "value": ok["value"] + ok["fy_value"] - ok["prior_value"],
        }
    )


_EMPTY_EVENTS = pd.DataFrame(
    {
        "security_id": pd.Series(dtype=np.int64),
        "concept": pd.Series(dtype=object),
        "period_end": pd.Series(dtype="datetime64[ns]"),
        "visible_date": pd.Series(dtype="datetime64[ns]"),
        "value": pd.Series(dtype=np.float64),
    }
)


def _to_ns(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.DataFrame:
    """pandas 3.x 会按字面量推断 s/us 分辨率时间戳，merge_asof 要求两侧一致。"""
    for col in cols:
        df[col] = df[col].astype("datetime64[ns]")
    return df


def build_metric_events(
    facts: pd.DataFrame,
    *,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
) -> pd.DataFrame:
    """事实长表 -> 指标事件表 (security_id, metric, period_end, visible_date, value)。

    flow 指标产出 TTM 事件，instant 指标直接产出时点事件；同一
    (security, metric, period_end) 多 concept 竞争时按优先级 coalesce，
    年度直报与 YTD 推导重合时优先直报（少一层误差项）。
    """
    lookup = _concept_lookup(metrics)
    if "metric" not in facts.columns:
        facts = facts.merge(
            lookup[["concept", "metric", "rank", "kind"]], on="concept", how="inner"
        )
    facts = _to_ns(facts.copy(), ("period_start", "period_end", "filed_date"))

    out: list[pd.DataFrame] = []

    flow = facts[facts["kind"] == "flow"].copy()
    if not flow.empty:
        flow["dur_days"] = (flow["period_end"] - flow["period_start"]).dt.days
        flow["dur_class"] = ""
        for cls, (lo, hi) in _DURATION_CLASSES.items():
            mask = (flow["dur_days"] >= lo) & (flow["dur_days"] <= hi)
            flow.loc[mask, "dur_class"] = cls
        flow = flow[flow["dur_class"] != ""]
        for _, grp in flow.groupby("metric"):
            annual = _annual_events(grp)
            derived = _derived_ttm_events(grp)
            ev = pd.concat([annual.assign(src=0), derived.assign(src=1)], ignore_index=True)
            ev["metric"] = grp["metric"].iloc[0]
            out.append(ev)

    instant = facts[facts["kind"] == "instant"]
    if not instant.empty:
        ev = pd.DataFrame(
            {
                "security_id": instant["security_id"],
                "concept": instant["concept"],
                "metric": instant["metric"],
                "period_end": instant["period_end"],
                "visible_date": instant["filed_date"],
                "value": instant["value"],
                "src": 0,
            }
        )
        out.append(ev)

    if not out:
        return _EMPTY_EVENTS.assign(metric=pd.Series(dtype=object))

    events = pd.concat(out, ignore_index=True)
    rank_map = lookup.set_index(["metric", "concept"])["rank"]
    events["rank"] = rank_map.reindex(
        pd.MultiIndex.from_frame(events[["metric", "concept"]])
    ).to_numpy()

    # coalesce：同 (security, metric, period_end) 取 直报优先 -> 概念优先级 -> 先可见
    events = events.sort_values(
        ["security_id", "metric", "period_end", "src", "rank", "visible_date"]
    ).drop_duplicates(["security_id", "metric", "period_end"], keep="first")

    # 可见序内 period_end 必须单调不减：迟到的旧期事件会让 as-of 序列倒退，丢弃
    events = events.sort_values(["security_id", "metric", "visible_date", "period_end"])
    running_max = events.groupby(["security_id", "metric"])["period_end"].cummax()
    events = events[events["period_end"] >= running_max]

    return events[
        ["security_id", "metric", "period_end", "visible_date", "value"]
    ].reset_index(drop=True)


def asof_panel(
    events: pd.DataFrame,
    *,
    dates: pd.DatetimeIndex,
    max_staleness_days: int = 270,
) -> dict[str, pd.DataFrame]:
    """事件表 -> {metric: 宽表 (index=dates, columns=security_id)} 的 as-of 取数。

    每个 (date, security) 取 visible_date <= date 的最近事件；period_end 落后
    date 超过 max_staleness_days 的值视为停止披露，置 NaN。
    """
    dates = pd.DatetimeIndex(sorted(pd.to_datetime(dates))).astype("datetime64[ns]")
    panels: dict[str, pd.DataFrame] = {}
    staleness = pd.Timedelta(days=max_staleness_days)
    for metric, ev in events.groupby("metric"):
        ev = _to_ns(ev.copy(), ("period_end", "visible_date"))
        secs = ev["security_id"].unique()
        grid = pd.DataFrame(
            {
                "date": np.repeat(dates.to_numpy(), len(secs)),
                "security_id": np.tile(secs, len(dates)),
            }
        )
        joined = pd.merge_asof(
            grid.sort_values("date"),
            ev.sort_values("visible_date"),
            left_on="date",
            right_on="visible_date",
            by="security_id",
            direction="backward",
        )
        stale = joined["period_end"] < joined["date"] - staleness
        joined.loc[stale, "value"] = np.nan
        panels[metric] = joined.pivot_table(
            index="date", columns="security_id", values="value", aggfunc="last"
        ).reindex(dates)
    return panels


def load_fundamental_panel(
    engine: Engine,
    *,
    dates: list[date] | pd.DatetimeIndex,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
    types: tuple[str, ...] = ("CS",),
    security_ids: list[int] | None = None,
    max_staleness_days: int = 270,
) -> dict[str, pd.DataFrame]:
    """一站式：拉事实 -> 构造事件 -> as-of 面板。

    dates 通常传调仓日序列；返回 {metric: 宽表}，与
    research.data.load_adjusted_panel 的返回形态一致，可直接对齐相乘。
    """
    facts = load_fundamental_facts(
        engine, metrics=metrics, types=types, security_ids=security_ids
    )
    events = build_metric_events(facts, metrics=metrics)
    return asof_panel(
        events,
        dates=pd.DatetimeIndex(pd.to_datetime(list(dates))),
        max_staleness_days=max_staleness_days,
    )
