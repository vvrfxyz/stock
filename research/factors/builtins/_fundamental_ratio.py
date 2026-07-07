"""wave-12 基本面质量/盈利族的公司级广播原语（共享读取层辅助）。

这一族（gross_profitability / accruals / operating_profitability）与
earnings_yield 的分母口径不同——分子分母**同源** sec_fundamental_facts、
同挂 CIK 锚证券（resolve_cik_map：活跃优先、id 最小），分母是 assets 不是
市值，因此不需要 company_market_cap 那套公司级合并市值。但仍需处理与
earnings_yield 相同的"事实只挂锚证券"问题：非锚类股（goog、brk.b）名下
基本面为空，证券级直除会让它们永远 NaN。

本模块提供**读取层**的公司级广播原语：把锚证券的指标值按 company_id 广播
回全部成员列（无 company_id 的证券用自身值，逐位不变）。分子分母都走同一
广播，保证同一公司的所有成员看到同一套（锚证券的）基本面。

已接受口径（沿用 earnings_yield，todo_crsp_phase2 任务 B）：
- 成员映射（securities.company_id）是当前快照、非 PIT——已接受的非 PIT 维度；
  指标面板本身仍是 PIT 的。
- 广播取"每公司首个非 NaN 成员列"（列序 security_id 升序，镜像 resolve_cik_map
  的 id 升序决胜）：正常态每公司恰一列非 NaN（锚证券）；锚翻转过渡窗内两代锚
  的 TTM 可同时新鲜，取首列而非求和，绝不双计。
- period_end 面板与值面板 NaN 逐格一致（asof_panel include_period_end 由同一
  as-of 选择产出），故对二者做同一广播选到同一锚列，period_end 对齐门槛成立。

一切在 compute() 内存中完成，绝不回写任何事实表。
"""
from __future__ import annotations

import pandas as pd

from research.company_market_cap import load_security_company_map


def build_membership(engine, universe: pd.Index) -> dict[int, int]:
    """universe 内证券 -> company_id 映射（仅含挂了 company_id 的 CS 成员）。

    返回 sid_to_cid：既含 universe 内成员，也含这些公司在 universe 外的其他
    成员（锚证券可能不在 universe 内，如 goog 在 googl 不在的窗口）——供
    expanded_ids 扩展加载用。
    """
    universe_set = {int(s) for s in universe}
    membership = load_security_company_map(engine)
    in_universe = membership[membership["security_id"].isin(universe_set)]
    company_ids = {int(c) for c in in_universe["company_id"].unique()}
    members = membership[membership["company_id"].isin(company_ids)]
    return {
        int(sid): int(cid)
        for sid, cid in zip(members["security_id"], members["company_id"])
    }


def expanded_security_ids(universe: pd.Index, sid_to_cid: dict[int, int]) -> list[int]:
    """加载范围 = universe ∪ 相关公司全部成员（含 universe 外的锚证券）。"""
    return sorted({int(s) for s in universe} | set(sid_to_cid))


def company_broadcast(
    panel: pd.DataFrame,
    universe: pd.Index,
    sid_to_cid: dict[int, int],
) -> pd.DataFrame:
    """把指标面板广播到 universe 列：成员取公司锚值，无 company_id 者取自身值。

    panel: index=dates, columns=证券 id（expanded 加载结果，可能多于/少于 universe）。
    返回: index=panel.index, columns=universe 的 float64 宽表。
    """
    universe = pd.Index([int(s) for s in universe], dtype="int64")
    cols = pd.Index([int(c) for c in panel.columns], dtype="int64")
    panel = panel.copy()
    panel.columns = cols

    with_company = [s for s in universe if s in sid_to_cid]
    without_company = [s for s in universe if s not in sid_to_cid]

    pieces: list[pd.DataFrame] = []

    if without_company:
        # 证券级旧口径：自身列（缺列则整列 NaN）
        own = panel.reindex(columns=pd.Index(without_company, dtype="int64"))
        pieces.append(own)

    if with_company:
        member_cols = sorted(c for c in cols if c in sid_to_cid)
        if member_cols:
            sub = panel.loc[:, member_cols].astype("float64")
            group_keys = pd.Index(
                [sid_to_cid[c] for c in member_cols],
                dtype="int64",
                name="company_id",
            )
            # 每公司首个非 NaN 成员列（列序即 security_id 升序，镜像 resolve_cik_map）
            company_val = sub.T.groupby(group_keys).first().T
        else:
            company_val = pd.DataFrame(
                index=panel.index, columns=pd.Index([], dtype="int64"), dtype="float64"
            )
        company_of = [sid_to_cid[s] for s in with_company]
        broadcast = company_val.reindex(columns=company_of)
        broadcast.columns = pd.Index(with_company, dtype="int64")
        pieces.append(broadcast)

    if not pieces:
        return pd.DataFrame(index=panel.index, columns=universe, dtype="float64")
    out = pd.concat(pieces, axis=1)
    return out.reindex(columns=universe).astype("float64")
