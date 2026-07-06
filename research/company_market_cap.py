"""公司级 PIT 合并市值读取层（任务 2：companies / PERMCO 等价物的收益兑现）。

securities 是"证券"粒度（PERMNO 等价物）：双类股（GOOG/GOOGL、BRK.A/BRK.B）
在证券粒度是两只互不相干的票，合并市值算不出来。companies 按 CIK 把它们归到
同一公司实体。本模块把 research.market_cap 的证券级 PIT 市值面板按
securities.company_id 聚合成公司级合并市值面板——只做聚合原语，不改动
research/market_cap.py 本身。

第一期 common-equity 判别是**名称启发式**（``is_common_equity_name``）：
vendor 把交易所挂牌的 baby bond/优先股/存托凭证也标成 type='CS'
（rilyg/oxlcg/tmusi 的 notes、bhfan 的 depositary shares 等——86 组活跃
多证券 CIK 大多如此）。这些工具行在归组时照挂 company_id（它们确实属于该
公司，flag-don't-drop），但**不计入**合并市值——公司市值只应含普通股。
已知误伤（"Preferred Bank"、"Unit Corporation" 这类真名撞词）接受为
第一期精度损耗，后续应由结构化 share-class 字段替代名称正则。
scripts/build_companies.py 的归组分类必须 import 本模块的同一个判别函数，
保证读取层与归组层口径一致。

多类股基本面分母修正（earnings_yield 的 per-company 去重）是**后续消费方**；
本模块只提供公司级市值聚合原语，不做任何因子接线。
"""
from __future__ import annotations

import re

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from research.market_cap import load_market_cap_panel

# 非普通股工具行的名称特征（todo_crsp_grade_2026-07 任务 2 验证过的词表）：
# notes/preferred/depositary/units/warrants/rights/bonds/debentures、
# 到期年份（"due 2026"）、票息（"5.00%"）。大小写不敏感、词边界锚定
# （"United"/"Wright" 不会撞上 Units?/Rights?）。
_NON_COMMON_EQUITY_RE = re.compile(
    r"""
    \b(?:notes?|preferred|depositary|units?|warrants?|rights?|bonds?|debentures?)\b
    | \bdue\s+(?:19|20)\d{2}\b
    | %
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MAP_COLUMNS = ["security_id", "company_id", "security_name", "is_common_equity"]


def is_common_equity_name(name: str | None) -> bool:
    """按证券名称判别是否普通股（第一期启发式，归组与读取层共用）。

    命中 notes/preferred/depositary/units/warrants/rights/bonds/debentures、
    到期年份（due 20xx）或票息百分号的名称判为非普通股工具行。
    NULL/空名称视为普通股（没有工具行证据，且 type='CS' 本身主张普通股）。
    """
    if name is None:
        return True
    stripped = str(name).strip()
    if not stripped:
        return True
    return _NON_COMMON_EQUITY_RE.search(stripped) is None


def _empty_map() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "company_id": pd.Series(dtype=np.int64),
            "security_name": pd.Series(dtype=object),
            "is_common_equity": pd.Series(dtype=bool),
        }
    )


def load_security_company_map(
    engine: Engine,
    *,
    company_ids: list[int] | None = None,
) -> pd.DataFrame:
    """加载 证券 -> 公司 的成员映射（含 common-equity 判别列）。

    范围限定 type='CS' 且 company_id 非 NULL：第一期 companies 只对 CS 归组
    （ETF 发行人 CIK ≠ 基金实体，绝不归组）；即使未来有 ETF 被误挂
    company_id，这里也把它挡在公司市值聚合之外。
    """
    if company_ids is not None and not company_ids:
        return _empty_map()
    sql = text(
        """
        select id as security_id, company_id, name as security_name
        from securities
        where company_id is not null
          and type = 'CS'
          and (:company_ids is null or company_id = any(:company_ids))
        order by id
        """
    )
    members = pd.read_sql_query(sql, engine, params={"company_ids": company_ids})
    if members.empty:
        return _empty_map()
    members["security_id"] = members["security_id"].astype(np.int64)
    members["company_id"] = members["company_id"].astype(np.int64)
    members["is_common_equity"] = members["security_name"].map(is_common_equity_name).astype(bool)
    return members[_MAP_COLUMNS]


def aggregate_company_market_cap(
    security_panel: pd.DataFrame,
    membership: pd.DataFrame,
) -> pd.DataFrame:
    """把证券级市值宽表按成员映射求和成公司级宽表（纯函数）。

    membership 需含 security_id/company_id 两列，**调用方负责先过滤成员**
    （load_company_market_cap_panel 已按 is_common_equity 过滤）。求和用
    ``min_count=1``：全 NaN 的公司-日期格保持 NaN（而不是 0），部分成员缺
    市值时返回可得成员之和（会低估合并市值——缺口在证券级面板补齐，不在
    聚合层猜）。不在 membership 里的证券列直接忽略。
    """
    index = security_panel.index
    mapped = membership.reindex(columns=["security_id", "company_id"]).dropna()
    if mapped.empty or security_panel.shape[1] == 0:
        return pd.DataFrame(index=index, columns=pd.Index([], dtype=np.int64), dtype=np.float64)
    mapped = mapped.astype({"security_id": np.int64, "company_id": np.int64})
    mapped = mapped.drop_duplicates(subset=["security_id"], keep="first")
    mapping = dict(zip(mapped["security_id"], mapped["company_id"]))

    panel = security_panel.copy()
    panel.columns = pd.Index([int(col) for col in panel.columns], dtype=np.int64)
    member_cols = [col for col in panel.columns if col in mapping]
    if not member_cols:
        return pd.DataFrame(index=index, columns=pd.Index([], dtype=np.int64), dtype=np.float64)
    sub = panel.loc[:, member_cols].astype(np.float64)
    group_keys = pd.Index([mapping[col] for col in sub.columns], dtype=np.int64, name="company_id")
    out = sub.T.groupby(group_keys).sum(min_count=1).T
    out.columns = pd.Index(out.columns, dtype=np.int64)
    return out.astype(np.float64)


def load_company_market_cap_panel(
    engine: Engine,
    *,
    dates: pd.DatetimeIndex,
    company_ids: list[int] | None = None,
    max_staleness_days: int = 400,
    visible_delay_days: int = 0,
    include_xbrl: bool = True,
) -> pd.DataFrame:
    """一站式加载公司级 PIT 合并市值宽表（index=dates, columns=company_id）。

    流程：securities 取成员映射 -> 过滤到 common-equity 成员（第一期名称
    启发式，见模块 docstring）-> research.market_cap.load_market_cap_panel
    取证券级 PIT 市值 -> 按 company_id 求和。max_staleness_days /
    visible_delay_days / include_xbrl 语义与 load_market_cap_panel 一致，
    原样透传。

    显式传 company_ids 时，无成员/无数据的公司返回全 NaN 列（与
    market_cap 对缺失证券的行为一致）。
    """
    dates = pd.DatetimeIndex(pd.to_datetime(list(dates))).astype("datetime64[ns]")
    requested = (
        pd.Index(sorted({int(cid) for cid in company_ids}), dtype=np.int64)
        if company_ids is not None
        else None
    )
    members = load_security_company_map(engine, company_ids=company_ids)
    members = members[members["is_common_equity"]]
    if members.empty:
        columns = requested if requested is not None else pd.Index([], dtype=np.int64)
        return pd.DataFrame(index=dates, columns=columns, dtype=np.float64)

    security_panel = load_market_cap_panel(
        engine,
        dates=dates,
        security_ids=[int(sid) for sid in members["security_id"]],
        max_staleness_days=max_staleness_days,
        visible_delay_days=visible_delay_days,
        include_xbrl=include_xbrl,
    )
    panel = aggregate_company_market_cap(security_panel, members)
    if requested is not None:
        panel = panel.reindex(columns=requested)
    return panel.astype(np.float64)
