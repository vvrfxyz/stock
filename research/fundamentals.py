"""研究用基本面读取端归一化：sec_fundamental_facts -> point-in-time 指标面板。

口径约定（v2，重述感知；与 research/data.py 的防未来函数原则一致）：
- point-in-time 取"as-of 时点最新已申报值"（Compustat PIT 同语义）：同一
  (security, concept, period) 的多次申报构成 vintage 序列（原始申报 + 重述/
  比较期重列），任意 as_of t 使用 filed_date <= t 的最新 vintage，
  filed_date == t 当日的申报在 t+1 才记为可见（visible_delay_days=1，
  避免 t 日收盘建仓吃到盘后才公开的财报跳空）。加载层只保留首报与数值发生变化的 vintage。
- flow 指标构造 TTM：年度事实（约 12 个月 duration）直接作为 TTM；
  季度/半年/三季 YTD 事实用 TTM = YTD + 上一财年全年 - 去年同期 YTD，
  三个分量必须来自同一 concept（避免营收同义概念混算）。任一分量出现新
  vintage 都会产出一条新的 TTM 事件（visible_date = 该 vintage 的 filed_date），
  分量缺失则不产出（不外推）。
- 营收等同义概念的 coalesce 在事件流层做：同一 (security, metric, period_end)
  多个 concept 产出事件时，按 MetricSpec.concepts 优先级选定一个 concept，
  保留其全部 vintage 事件，丢弃其余 concept（保证修订序列口径一致）。
  营收链按 XBRL US fundamental accounting concepts 的标准营收族排序，
  含金融/行业总营收概念（银行保险券商不报 Revenues）。
- instant 指标（资产负债表科目）取 as_of 时点最新可见 vintage 的报告期值。
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

from research.factors.asof import event_table_to_asof_panel


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
                # 伞概念 / 现行准则
                "Revenues",
                "RevenueFromContractWithCustomerExcludingAssessedTax",
                "RevenueFromContractWithCustomerIncludingAssessedTax",
                # 旧 taxonomy（2018 前）
                "SalesRevenueNet",
                "SalesRevenueGoodsNet",
                "SalesRevenueServicesNet",
                # 金融 / 行业总营收（公司只会申报自己行业的那一个，排序影响很小）
                "RevenuesNetOfInterestExpense",
                "RegulatedAndUnregulatedOperatingRevenue",
                "InterestAndDividendIncomeOperating",
                "HealthCareOrganizationRevenue",
                "RealEstateRevenueNet",
                "OilAndGasRevenue",
                "FinancialServicesRevenue",
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

_EVENT_COLUMNS = ["security_id", "concept", "period_end", "visible_date", "value"]

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
    """拉取所需 concept 的 vintage 序列（长表）。

    每个 (security, concept, period) 按 filed_date 排序，只保留首报与数值
    相对上一 vintage 发生变化的行（同值的比较期重列被压缩掉）；
    period 的合理性过滤顺带剔除库内极少数申报方笔误日期（如 0202/2201 年）。
    """
    lookup = _concept_lookup(metrics)
    id_clause = "and f.security_id = any(:security_ids)" if security_ids else ""
    sql = text(
        f"""
        select security_id, concept, unit, is_instant,
               period_start, period_end, filed_date, accepted_at, value
        from (
            select f.security_id, f.concept, f.unit, f.is_instant,
                   f.period_start, f.period_end, f.filed_date,
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
            join securities s on s.id = f.security_id
            where f.concept = any(:concepts)
              and f.unit = any(:units)
              and f.security_id is not null
              and f.period_end between '1995-01-01' and '2035-12-31'
              and f.period_start between '1995-01-01' and '2035-12-31'
              and s.type = any(:types) {id_clause}
        ) v
        where v.prev_value is null or v.prev_value <> v.value
        order by security_id, concept, period_start, period_end, filed_date
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
        parse_dates=["period_start", "period_end", "filed_date", "accepted_at"],
    )
    df = pd.concat(list(chunks), ignore_index=True)
    df = _to_ns(df, ("period_start", "period_end", "filed_date"))
    # PIT 可见日取 filed_date 与 accepted_at::date 的较晚者：EDGAR 收盘后受理的申报
    # 当日不可见，filed_date 才是申报日历日，accepted_at 是真正落库时点。把较晚者并回
    # filed_date，下游 _vintage_events / _derived_ttm_events 自动沿用，无需各自改。
    if "accepted_at" in df.columns:
        accepted = pd.to_datetime(df["accepted_at"], utc=True)
        # 取美东自然日：SEC accepted_at 是 ET 的落库时刻，按 ET 折算成可见日历日。
        accepted_date = accepted.dt.tz_convert("America/New_York").dt.tz_localize(None).dt.normalize()
        df["filed_date"] = df["filed_date"].where(
            accepted_date.isna() | (accepted_date <= df["filed_date"]),
            accepted_date,
        )
        df = df.drop(columns=["accepted_at"])
    # concept 与 unit 必须按 spec 配对（金额概念不会有 shares 单位，反之亦然；此处兜底）
    df = df.merge(lookup, on=["concept", "unit"], how="inner")
    df["security_id"] = df["security_id"].astype(np.int64)
    return df


def _vintage_events(rows: pd.DataFrame) -> pd.DataFrame:
    """每个 vintage 直接成为一条事件（年度直报 TTM 与 instant 指标共用）。"""
    return pd.DataFrame(
        {
            "security_id": rows["security_id"],
            "concept": rows["concept"],
            "period_end": rows["period_end"],
            "visible_date": rows["filed_date"],
            "value": rows["value"],
        }
    )


def _derived_ttm_events(flow: pd.DataFrame) -> pd.DataFrame:
    """YTD + 上一财年 - 去年同期 YTD 的 vintage 感知事件流。

    先在期间层完成三方容差匹配（与 vintage 无关），再把每个三元组的三条
    vintage 序列按 filed_date 合并成阶梯函数：任一分量更新即产出新事件。
    """
    periods = flow[
        ["security_id", "concept", "dur_class", "dur_days", "period_start", "period_end"]
    ].drop_duplicates(["security_id", "concept", "period_start", "period_end"])
    ytd_p = periods[periods["dur_class"].isin(("Q", "H", "N"))].copy()
    fy_p = periods[periods["dur_class"] == "Y"]
    if ytd_p.empty or fy_p.empty:
        return _EMPTY_EVENTS.copy()

    # 上一财年：period_end 应恰为 YTD period_start 的前一天；分量锁定同一 concept
    ytd_p["fy_target"] = ytd_p["period_start"] - pd.Timedelta(days=1)
    fy = fy_p[["security_id", "concept", "period_start", "period_end"]].rename(
        columns={"period_start": "fy_ps", "period_end": "fy_end"}
    )
    matched = pd.merge_asof(
        ytd_p.sort_values("fy_target"),
        fy.sort_values("fy_end"),
        left_on="fy_target",
        right_on="fy_end",
        by=["security_id", "concept"],
        direction="nearest",
        tolerance=pd.Timedelta(days=_PRIOR_FY_TOLERANCE_DAYS),
    )

    # 去年同期 YTD：同 concept 同 duration class，period_end 距 (period_end - 365) 在容差内
    prior = ytd_p[["security_id", "concept", "dur_class", "period_start", "period_end"]].rename(
        columns={"period_start": "prior_ps", "period_end": "prior_end"}
    )
    matched["prior_target"] = matched["period_end"] - pd.Timedelta(days=365)
    matched = pd.merge_asof(
        matched.sort_values("prior_target"),
        prior.sort_values("prior_end"),
        left_on="prior_target",
        right_on="prior_end",
        by=["security_id", "concept", "dur_class"],
        direction="nearest",
        tolerance=pd.Timedelta(days=_PRIOR_YTD_TOLERANCE_DAYS),
    )
    matched = matched.dropna(subset=["fy_end", "prior_end"])
    if matched.empty:
        return _EMPTY_EVENTS.copy()
    # 同 (security, concept, period_end) 多个三元组（异形财年）时取 YTD 跨度最长的
    matched = matched.sort_values("dur_days", ascending=False).drop_duplicates(
        ["security_id", "concept", "period_end"]
    )
    matched = matched.reset_index(drop=True)
    matched["triple_id"] = matched.index

    # 三条 vintage 序列合并为阶梯函数
    vint = flow[
        ["security_id", "concept", "period_start", "period_end", "filed_date", "value"]
    ].rename(columns={"period_start": "v_ps", "period_end": "v_pe"})
    role_keys = {
        "ytd": ("period_start", "period_end"),
        "fy": ("fy_ps", "fy_end"),
        "prior": ("prior_ps", "prior_end"),
    }
    parts = []
    for role, (ps_col, pe_col) in role_keys.items():
        part = matched[["triple_id", "security_id", "concept", ps_col, pe_col]].merge(
            vint,
            left_on=["security_id", "concept", ps_col, pe_col],
            right_on=["security_id", "concept", "v_ps", "v_pe"],
        )
        parts.append(
            pd.DataFrame(
                {
                    "triple_id": part["triple_id"],
                    "filed": part["filed_date"],
                    "role": role,
                    "value": part["value"],
                }
            )
        )
    long = pd.concat(parts, ignore_index=True)
    wide = (
        long.sort_values(["triple_id", "filed"])
        .pivot_table(index=["triple_id", "filed"], columns="role", values="value", aggfunc="last")
        .reset_index()
        .sort_values(["triple_id", "filed"])
    )
    roles = ["ytd", "fy", "prior"]
    wide[roles] = wide.groupby("triple_id")[roles].ffill()
    wide = wide.dropna(subset=roles)
    wide["ttm"] = wide["ytd"] + wide["fy"] - wide["prior"]
    # 同一三元组内 TTM 值未变化的 vintage 不重复发事件
    wide = wide[wide["ttm"] != wide.groupby("triple_id")["ttm"].shift()]

    meta = matched[["triple_id", "security_id", "concept", "period_end"]]
    out = wide.merge(meta, on="triple_id")
    return pd.DataFrame(
        {
            "security_id": out["security_id"],
            "concept": out["concept"],
            "period_end": out["period_end"],
            "visible_date": out["filed"],
            "value": out["ttm"],
        }
    )


def build_metric_events(
    facts: pd.DataFrame,
    *,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
) -> pd.DataFrame:
    """事实 vintage 长表 -> 指标事件表 (security_id, metric, period_end, visible_date, value)。

    flow 指标产出 TTM 事件流，instant 指标产出时点事件流；重述以新事件形式
    在其 filed_date 进入流。同一 (security, metric, period_end) 多 concept
    竞争时按 (直报优先, 概念优先级) 选定唯一 concept 并保留其全部 vintage。
    """
    lookup = _concept_lookup(metrics)
    if "metric" not in facts.columns:
        facts = facts.merge(
            lookup[["concept", "metric", "rank", "kind"]], on="concept", how="inner"
        )
    facts = _to_ns(facts.copy(), ("period_start", "period_end", "filed_date"))
    # 同日对同一 period 的多次申报（如 8-K + 10-Q）只保留最后一条
    facts = facts.sort_values(
        ["security_id", "concept", "period_start", "period_end", "filed_date"]
    ).drop_duplicates(
        ["security_id", "concept", "period_start", "period_end", "filed_date"], keep="last"
    )

    out: list[pd.DataFrame] = []

    flow = facts[facts["kind"] == "flow"].copy()
    if not flow.empty:
        flow["dur_days"] = (flow["period_end"] - flow["period_start"]).dt.days
        flow["dur_class"] = ""
        for cls, (lo, hi) in _DURATION_CLASSES.items():
            mask = (flow["dur_days"] >= lo) & (flow["dur_days"] <= hi)
            flow.loc[mask, "dur_class"] = cls
        flow = flow[flow["dur_class"] != ""]
        for metric, grp in flow.groupby("metric"):
            annual = _vintage_events(grp[grp["dur_class"] == "Y"])
            derived = _derived_ttm_events(grp)
            ev = pd.concat([annual.assign(src=0), derived.assign(src=1)], ignore_index=True)
            ev["metric"] = metric
            out.append(ev)

    instant = facts[facts["kind"] == "instant"]
    if not instant.empty:
        ev = _vintage_events(instant).assign(src=0)
        ev["metric"] = instant["metric"].to_numpy()
        out.append(ev)

    if not out:
        return _EMPTY_EVENTS.assign(metric=pd.Series(dtype=object))[
            ["security_id", "metric", "period_end", "visible_date", "value"]
        ]

    events = pd.concat(out, ignore_index=True)
    rank_map = lookup.set_index(["metric", "concept"])["rank"]
    events["rank"] = rank_map.reindex(
        pd.MultiIndex.from_frame(events[["metric", "concept"]])
    ).to_numpy()

    # coalesce：同 (security, metric, period_end) 按 (直报优先, 概念优先级) 选定
    # 唯一 (concept, src) 流，保留其全部 vintage 事件
    winners = events.sort_values(
        ["security_id", "metric", "period_end", "src", "rank", "visible_date"]
    ).drop_duplicates(["security_id", "metric", "period_end"])[
        ["security_id", "metric", "period_end", "concept", "src"]
    ]
    events = events.merge(
        winners, on=["security_id", "metric", "period_end", "concept", "src"], how="inner"
    )

    # 可见序内 period_end 必须单调不减：迟到的旧期事件会让 as-of 序列倒退，丢弃；
    # 对当前最新报告期的重述（period_end 相等）保留
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
    visible_delay_days: int = 1,
) -> dict[str, pd.DataFrame]:
    """事件表 -> {metric: 宽表 (index=dates, columns=security_id)} 的 as-of 取数。

    每个 (date, security) 取 visible_date + visible_delay_days <= date 的最近事件（重述事件自然
    覆盖旧值）；默认后移一天，避免 filed_date 当日盘后 EDGAR 申报被 t 日收盘建仓提前看到。
    period_end 落后 date 超过 max_staleness_days 的值视为停止披露，置 NaN。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(dates)).astype("datetime64[ns]")
    panels: dict[str, pd.DataFrame] = {}
    for metric, ev in events.groupby("metric"):
        ev = _to_ns(ev.copy(), ("period_end", "visible_date"))
        panels[metric] = event_table_to_asof_panel(
            ev,
            dates=dates,
            value_column="value",
            visible_date_column="visible_date",
            staleness_anchor_column="period_end",
            visible_delay_days=visible_delay_days,
            max_staleness_days=max_staleness_days,
        )
    return panels


def load_fundamental_panel(
    engine: Engine,
    *,
    dates: list[date] | pd.DatetimeIndex,
    metrics: tuple[str, ...] = DEFAULT_METRICS,
    types: tuple[str, ...] = ("CS",),
    security_ids: list[int] | None = None,
    max_staleness_days: int = 270,
    visible_delay_days: int = 1,
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
        visible_delay_days=visible_delay_days,
    )
