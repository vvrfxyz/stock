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

# 研究面板默认证券类型：CS-only 是零污染铁律（docs/adr_expansion_plan_2026-07.md §A.2），
# ADR 家族经 --include-adr 显式 opt-in 并入（§E.6 工程，2026-07-07）。
# 股本口径敏感因子（size/earnings_yield/short_interest_ratio）在 evaluate 层
# 对 ADR 列另有 adr_unsafe 门（ADS 与公司股本混杂，禁入直至归一化，§E.3）。
DEFAULT_RESEARCH_TYPES: tuple[str, ...] = ("CS",)
ADR_TYPES: tuple[str, ...] = ("ADRC", "ADRP", "ADRR")
RESEARCH_TYPES_WITH_ADR: tuple[str, ...] = DEFAULT_RESEARCH_TYPES + ADR_TYPES

# daily_prices 中允许研究层拉取的价格列白名单（列名内联进 COPY SQL，必须受控）。
PRICE_COLUMNS = ("open", "close", "volume", "vwap")

# 评估层面板进程内缓存（照 research/factors/price_cache.py 的 _PANEL_CACHE 模式）：
# evaluate_all 逐因子调 run_evaluation，各自重新装载同一份 31M 行面板是长窗口
# wall-clock 主项之一。缓存 4 张宽表，命中时返回**新 dict、同 DataFrame 对象**——
# 调用方只 rebind dict 条目（drop/loc 都返回新 frame），从不原地改写宽表。
# 注意：缓存假设窗口内历史数据在进程存活期不变（研究只读用法成立）；
# 集成测试同 URL 换数据时须先 clear_panel_cache()。
_ADJUSTED_PANEL_CACHE: dict[tuple, dict[str, pd.DataFrame]] = {}
_ADJUSTED_PANEL_CACHE_MAX = 2  # 长窗口面板数 GB 级，最多驻留 2 个窗口（评估 + 对比窗口）


def clear_panel_cache() -> None:
    _ADJUSTED_PANEL_CACHE.clear()


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
    types: tuple[str, ...] = DEFAULT_RESEARCH_TYPES,
    include_inactive: bool = True,
    security_ids: list[int] | None = None,
    columns: tuple[str, ...] = PRICE_COLUMNS,
) -> pd.DataFrame:
    """拉取 [start, end] 的原始日线（长表）。

    默认包含 is_active=False 的证券：建库后退市的标的保留在库里，
    纳入它们可以减轻（但不能消除）幸存者偏差。

    2026-07 性能改造：走 PostgreSQL COPY csv 通道直接喂 pandas C 解析器——
    read_sql 逐行物化在 31M 行量级上慢 5-10 倍（wave-1 评估实测瓶颈）。
    参数全部为内部受控值（date/内部 int id/白名单 type/白名单列名），内联安全。
    columns 可裁剪价格列（长窗口面板只需 close/volume 时省 ~50% csv 字节与解析）。
    """
    import io

    unknown = set(columns) - set(PRICE_COLUMNS)
    if unknown:
        raise ValueError(f"不支持的价格列: {sorted(unknown)}；白名单 {PRICE_COLUMNS}")
    if not columns:
        raise ValueError("columns 不能为空")
    select_cols = ", ".join(f"p.{c}::float8 as {c}" for c in columns)
    active_clause = "" if include_inactive else "and s.is_active"
    id_clause = ""
    if security_ids:
        id_clause = f"and p.security_id in ({','.join(str(int(x)) for x in security_ids)})"
    type_list = ",".join(f"'{t}'" for t in types)
    copy_sql = f"""
        COPY (
            select p.security_id, p.date, {select_cols}
            from daily_prices p
            join securities s on s.id = p.security_id
            where p.date between '{start.isoformat()}' and '{end.isoformat()}'
              and s.type in ({type_list}) {active_clause} {id_clause}
            order by p.security_id, p.date
        ) TO STDOUT WITH (FORMAT csv, HEADER true)
    """
    raw = engine.raw_connection()
    try:
        buffer = io.BytesIO()
        with raw.cursor() as cursor:
            cursor.copy_expert(copy_sql, buffer)
    finally:
        raw.close()
    buffer.seek(0)
    df = pd.read_csv(buffer, parse_dates=["date"])
    del buffer  # csv 文本（长窗口 GB 级）与解析结果短暂共存，解析完立即释放
    if df.empty:
        return pd.DataFrame(columns=["security_id", "date", *columns])
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
    """长表 -> 宽表（index=date, columns=security_id）。

    (security_id, date) 是 daily_prices 的 PK，正常路径无重复键，直接 pivot——
    pivot_table(aggfunc="last") 的 groupby 聚合在 31M 行量级是纯开销（2026-07 实测）。
    仅在重复键（理论上不该发生）时回退旧的 pivot_table 聚合语义。
    已知边界差异：某列全 NaN 的证券 pivot 保留全 NaN 列而 pivot_table 静默丢列；
    生产 daily_prices 的 close/volume 无 NULL（2026-07 校验），保留列反而使
    load_adjusted_panel 的 4 张宽表列集合恒等，更一致。
    """
    try:
        return prices.pivot(index="date", columns="security_id", values=column)
    except ValueError:
        return prices.pivot_table(index="date", columns="security_id", values=column, aggfunc="last")


def load_adjusted_panel(
    engine: Engine,
    *,
    start: date,
    end: date,
    types: tuple[str, ...] = DEFAULT_RESEARCH_TYPES,
    as_of: date | None = None,
    include_inactive: bool = True,
) -> dict[str, pd.DataFrame]:
    """返回宽表字典：adj_close / close / volume / dollar_volume。

    进程内记忆化（见 _ADJUSTED_PANEL_CACHE 注释）：命中时返回新 dict、
    同 DataFrame 对象；调用方约定只 rebind dict 条目、不原地改写宽表。
    """
    if start < FACTOR_TRUST_FLOOR:
        raise ValueError(
            f"start={start} 早于因子可信窗口 {FACTOR_TRUST_FLOOR}；"
            "更早价格未保证复权，研究面板拒绝装载。"
        )
    effective_as_of = as_of or end
    cache_key = (str(engine.url), start, end, tuple(types), effective_as_of, include_inactive)
    cached = _ADJUSTED_PANEL_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached)
    # adj_close 由 close 派生、dollar_volume = close×volume：只拉 2 列长表
    prices = load_price_long(
        engine, start=start, end=end, types=types,
        include_inactive=include_inactive, columns=("close", "volume"),
    )
    events = load_factor_events(engine, as_of=effective_as_of)
    prices = apply_adjustment(prices, events, as_of=effective_as_of)
    prices["dollar_volume"] = prices["close"] * prices["volume"]
    panel = {
        "adj_close": to_wide(prices, "adj_close"),
        "close": to_wide(prices, "close"),
        "volume": to_wide(prices, "volume"),
        "dollar_volume": to_wide(prices, "dollar_volume"),
    }
    del prices, events  # 长表（长窗口 GB 级）在宽表建成后立即释放
    while len(_ADJUSTED_PANEL_CACHE) >= _ADJUSTED_PANEL_CACHE_MAX:
        _ADJUSTED_PANEL_CACHE.pop(next(iter(_ADJUSTED_PANEL_CACHE)))
    _ADJUSTED_PANEL_CACHE[cache_key] = panel
    return dict(panel)


def load_symbol_map(engine: Engine) -> pd.Series:
    df = pd.read_sql_query(text("select id, symbol from securities"), engine)
    return df.set_index("id")["symbol"]


def load_delisting_returns(
    engine: Engine,
    *,
    fund_closure_par: bool = True,
    redemption_par: bool = True,
) -> pd.Series:
    """逐证券实测退市收益（index=security_id int, values=float）。

    来源 delisting_events.delisting_return（docs/todo_crsp_grade_2026-07.md 任务 1）。
    同一证券多次退市（重新上市后再退）时只取**最近一次**退市事件：面板的终局
    是最后那次退市，借用更早退市周期的收益属于口径错误——最近一次无实测值的
    证券整体缺席，由 run_backtest 的 terminal_return_fallback 兜底（宁缺毋滥）。
    表未填充时返回空 Series，调用方应退回全局标量假设。

    fund_closure_par=True（默认）：ETF 清盘（reason_code='FUND_CLOSURE'）且
    final_price 在场、delisting_return 为 NULL 的行在**读取时**合成 0.0。
    理由（任务交接件"坑"节背书）：ETF 清盘的最终 NAV 分配常在退市后数周，
    final_price 已收敛到预期 NAV，持有人按面值平价退出，return≈0 是对的口径；
    事实表遵循"无实据不写数值"纪律，经验值只活在读取层——这正是那条经验值。
    实测值（含恰为 0.0 的实测）永远优先，不会被合成值覆盖。纯粹主义者可
    fund_closure_par=False 关闭，只拿实测行。

    redemption_par=True（默认）：SPAC 全类赎回清算（reason_code='LIQUIDATION'
    且 evidence 带 Form 25 12d2-2(a)(1)/(a)(2) 的 redemption_provision 标记）
    且 final_price 在场时同样合成 0.0——机制与 ETF 清盘 par 同构：赎回价 =
    信托账户固定金额，终价已收敛到赎回价，持有人按信托价平价退出。只认
    redemption_provision 证据行（真清算/破产式 LIQUIDATION 不带该标记，不合成）。
    """
    par_cases: list[str] = []
    if fund_closure_par:
        par_cases.append(
            "when reason_code = 'FUND_CLOSURE' and final_price is not null then 0.0"
        )
    if redemption_par:
        par_cases.append(
            "when reason_code = 'LIQUIDATION' and final_price is not null "
            "and evidence like '%redemption_provision%' then 0.0"
        )
    if par_cases:
        value_expr = (
            "coalesce(delisting_return, case " + " ".join(par_cases) + " end)"
        )
    else:
        value_expr = "delisting_return"
    sql = text(
        f"""
        select security_id, ({value_expr})::float8 as delisting_return
        from (
            select distinct on (security_id)
                   security_id, delisting_return, reason_code, final_price, evidence
            from delisting_events
            order by security_id, delist_date desc
        ) latest
        where ({value_expr}) is not null
        """
    )
    df = pd.read_sql_query(sql, engine)
    series = df.set_index("security_id")["delisting_return"].astype("float64")
    series.index = series.index.astype("int64")
    return series


def resolve_terminal_returns(
    realized: pd.Series,
    cli_value: float | None,
    use_realized: bool = True,
) -> tuple[float | pd.Series | None, float | None]:
    """决定传给 run_backtest 的 (terminal_return, terminal_return_fallback)。

    规则（docs/todo_crsp_grade_2026-07.md 任务 1 步骤 4）：优先逐证券实测
    delisting_return（Series），CLI 标量降级为未覆盖证券的 fallback；实测为空
    （表未填充）或显式 opt-out 时，行为与旧口径完全一致——只传 CLI 标量、无 fallback。
    """
    if not use_realized or realized.empty:
        return cli_value, None
    return realized, cli_value


def uncovered_gate_version(require_straddle: bool = True) -> str:
    """gate 口径版本串（进 evaluate 的 params_hash，新旧口径 trial 不互相顶替）。"""
    return "straddle_v2" if require_straddle else "legacy_v1"


def securities_with_uncovered_events(
    engine: Engine,
    *,
    start: date,
    end: date,
    methodology_version: str = DEFAULT_METHODOLOGY_VERSION,
    require_straddle: bool = True,
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

    跨立精化（require_straddle=True，默认，'straddle_v2' 口径）：只有"跨立"该证券
    价格序列的未覆盖事件才计为洞。原理——复权因子只作用于 ex_date 之前的价格行：
    事件前无任何价格行则无可调整行、不产生假跳空；事件后无任何价格行则缺失因子
    均匀作用于全序列（同乘一个常数），收益率不变。跨立性只依赖每证券 daily_prices
    的 min(date)/max(date)（exists(date<ex) <=> min<ex；exists(date>=ex) <=> max>=ex，
    内部缺口不影响存在性），故对洞证券集合做一次 join 聚合即可，不必逐行 EXISTS
    扫 3,100 万行价格表。无任何价格行的证券同样放行（面板里本就没有它的列）。
    实测（生产库，窗口 2003-01-01..2026-07-07）：剔除数 2,310 -> 794。

    跨立按证券**全历史**价格边界判定，不按 start/end 裁剪价格行——有意的保守选择：
    若按窗口裁剪，*_study.py 调用点（gate 传名义窗口、价格/信号面板却带
    buffer_days=200 前推暖机段）会漏剔"假跳空落在暖机段内"的污染证券；全史边界下
    窗口跨立蕴含全史跨立、绝不漏剔，代价只是窗口边缘可能多剔个别窗口内本就干净的
    证券（如 ex_date 恰在窗口首日且历史价格全在窗口前），且与 2026-07 生产探针的
    验收口径位级对齐。

    require_straddle=False 完全复现旧口径（'legacy_v1'：任何未覆盖事件都剔除），
    供旧结果复现与对账。
    """
    uncovered_sql = """
        select ca.security_id, ca.ex_date
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
    if require_straddle:
        sql = text(
            f"""
            with uncovered as ({uncovered_sql}),
            bounds as (
                select p.security_id, min(p.date) as min_date, max(p.date) as max_date
                from daily_prices p
                where p.security_id in (select distinct security_id from uncovered)
                group by p.security_id
            )
            select distinct u.security_id
            from uncovered u
            join bounds b on b.security_id = u.security_id
            where b.min_date < u.ex_date
              and b.max_date >= u.ex_date
            """
        )
    else:
        sql = text(f"select distinct security_id from ({uncovered_sql}) uncovered")
    with engine.connect() as conn:
        rows = conn.execute(sql, {"start": start, "end": end, "mv": methodology_version}).fetchall()
    return [r[0] for r in rows]
