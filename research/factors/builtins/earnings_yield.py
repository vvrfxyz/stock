"""earnings_yield：公司级分母/分子的盈利收益率因子（二期接线）。

sec_fundamental_facts 的事实入库时只挂 CIK 锚证券（resolve_cik_map：活跃
优先、id 最小——Alphabet 的 NetIncomeLoss 只挂 googl，goog 名下零事实），
证券级 ni/mcap 直除会让非锚类股（goog/brk.b）永远 NaN，且锚证券的分母
（自身单类市值）系统性低估。本因子在**读取层**做公司 join 修正：

- 分子 = 公司级 net_income_ttm：把 security_ids 扩展为 universe ∪ 相关公司
  全部成员后加载，按 company_id 对成员列取**首个非 NaN**（列序 security_id
  升序，镜像 resolve_cik_map 的 id 升序决胜）——正常态每公司恰一列非 NaN
  （锚证券）；锚翻转后的过渡窗内两代锚的 TTM 可同时新鲜，取首列而非求和
  避免双计净利——再广播回全部成员列。
- 分母 = 公司级合并市值（research.company_market_cap，上市 common-equity
  类之和），按成员映射广播回证券列。
- 无 company_id 的证券保持证券级旧口径（自身 ni / 自身市值），行为逐位不变。
- 公司**没有任何** common-equity 成员时（退市 LP common units 这类名称正则
  误伤且无 FIGI 的单成员公司），公司级分母恒 NaN——这类成员整体回退证券级
  旧口径，避免从可算值退化成 NaN。

已接受口径（todo_crsp_phase2 任务 B）：

- 成员映射（securities.company_id）是**当前快照**，无历史版本——这是一个
  已接受的非 PIT 维度；分子分母面板本身仍是 PIT 的。
- 分母 = 上市类合并市值（Alphabet 缺未上市 B 类约 7%），分子 = 公司级
  净利，两者有轻微口径错配，第一版记录口径即可。
- 挂了 company_id 但被判为非 common-equity 的成员（工具行误标）同样收到
  公司级值：其市值不进分母，但因子值 = 公司盈利收益率。

一切在 compute() 内存中完成，绝不回写任何事实表。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import pandas as pd

from research.company_market_cap import (
    load_company_market_cap_panel,
    load_security_company_map,
)
from research.factors.protocol import FactorContext, register
from research.fundamentals import load_fundamental_panel
from research.market_cap import load_market_cap_panel


@dataclass(frozen=True)
class EarningsYieldFactor:
    name: ClassVar[str] = "earnings_yield"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        universe = pd.Index([int(sid) for sid in ctx.security_universe], dtype="int64")

        # 成员映射一次性全量加载（一条 SQL），本地切片出 universe 涉及的公司；
        # load_company_market_cap_panel 内部按 company_ids 走限定查询，不会
        # 再触发全市场扫描。
        membership = load_security_company_map(ctx.engine)
        universe_set = set(universe.tolist())
        # 只有存在至少一个 common-equity 成员的公司才有非 NaN 的公司级分母；
        # 全员被判非 common 的退化公司（模块 docstring）整体走证券级旧口径。
        common_companies = set(
            membership.loc[membership["is_common_equity"], "company_id"].astype(int)
        )
        in_universe = membership[
            membership["security_id"].isin(universe_set)
            & membership["company_id"].isin(common_companies)
        ]
        company_ids = sorted({int(cid) for cid in in_universe["company_id"].unique()})
        members = membership[membership["company_id"].isin(company_ids)]
        sid_to_cid = {
            int(sid): int(cid)
            for sid, cid in zip(members["security_id"], members["company_id"])
        }

        with_company = pd.Index(
            [sid for sid in universe if sid in sid_to_cid], dtype="int64"
        )
        without_company = pd.Index(
            [sid for sid in universe if sid not in sid_to_cid], dtype="int64"
        )

        # 分子加载范围 = universe ∪ 相关公司全部成员：锚证券可能不在 universe
        # 内（goog 在、googl 不在的窗口），且锚可能是被误标的非 common 行
        # （resolve_cik_map 不看名称），取全员最稳。
        expanded_ids = sorted(universe_set | set(sid_to_cid))
        fundamentals = load_fundamental_panel(
            ctx.engine,
            dates=ctx.dates,
            metrics=("net_income_ttm",),
            security_ids=expanded_ids,
        )
        ni = fundamentals.get(
            "net_income_ttm",
            pd.DataFrame(index=ctx.dates, columns=pd.Index([], dtype="int64"), dtype="float64"),
        ).copy()
        ni.columns = pd.Index([int(col) for col in ni.columns], dtype="int64")

        numerator_pieces: list[pd.DataFrame] = []
        denominator_pieces: list[pd.DataFrame] = []

        if len(without_company):
            # 旧口径原样保留：自身 ni / 自身证券级市值（逐位一致）。
            security_mcap = load_market_cap_panel(
                ctx.engine, dates=ctx.dates, security_ids=without_company.tolist()
            )
            numerator_pieces.append(ni.reindex(columns=without_company))
            denominator_pieces.append(security_mcap.reindex(columns=without_company))

        if len(with_company):
            member_cols = sorted(col for col in ni.columns if col in sid_to_cid)
            if member_cols:
                sub = ni.loc[:, member_cols].astype("float64")
                group_keys = pd.Index(
                    [sid_to_cid[col] for col in member_cols],
                    dtype="int64",
                    name="company_id",
                )
                # 正常态每公司恰一列非 NaN（事实只挂锚证券）。锚翻转过渡窗内
                # 两代锚的 TTM 可同时新鲜（如 gliba/gncma），groupby.first()
                # 按列序（security_id 升序，镜像 resolve_cik_map 的 id 升序
                # 决胜）取首个非 NaN，绝不把两代锚的净利加总双计。
                company_ni = sub.T.groupby(group_keys).first().T
            else:
                company_ni = pd.DataFrame(
                    index=ni.index, columns=pd.Index([], dtype="int64"), dtype="float64"
                )
            company_mcap = load_company_market_cap_panel(
                ctx.engine, dates=ctx.dates, company_ids=company_ids
            )

            company_of = [sid_to_cid[sid] for sid in with_company]
            member_ni = company_ni.reindex(columns=company_of)
            member_ni.columns = with_company
            member_mcap = company_mcap.reindex(columns=company_of)
            member_mcap.columns = with_company
            numerator_pieces.append(member_ni)
            denominator_pieces.append(member_mcap)

        if not numerator_pieces:
            return pd.DataFrame(
                index=ctx.dates, columns=ctx.security_universe, dtype="float64"
            )
        numerator = pd.concat(numerator_pieces, axis=1)
        denominator = pd.concat(denominator_pieces, axis=1)
        ratio = numerator / denominator.where(denominator > 0)
        return ratio.reindex(index=ctx.dates, columns=ctx.security_universe).astype("float64")


register(EarningsYieldFactor())
