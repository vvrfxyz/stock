"""内部人集群买入因子（wave-7；family=insider_cluster）。

假设（Cohen-Malloy-Pomorski 2012 + 集群文献）：多个**非例行**内部人在近窗口
共同用真金买入（Form 4 code=P 公开市场购买）是强正信号；卖出多为流动性动机
不对称地弱。"例行"定义（CMP）：该 owner 在此前连续 3 年的同一日历月都有
P 买入 -> 该月的买入判例行，剔除。

数据边界（2026-07 探针）：`insider_transactions` P/TRANSACTION 非衍生 34 万行、
2003-07+，约 350 只/季有任意 P 申报、746 只/半年有 ≥2 买家——**条件因子**：
无事件名字 = NaN（不进横截面），评估宇宙即"内部人活跃子集"（~250-350 只/日，
过 min_coverage=100 但偏薄，报告须披露逐日覆盖）。

信号（t 日）：过去一季度（91 自然日 ≈ 63 交易日，按 filing_date，PIT 可见性边界）
非例行**去重买家数**，
加 1e-9 × min(购买总额, $10M) 连续小项破并列（大额买入优先）。
lag_days=1：filing_date 当日盘后可见，次日建仓。

实现说明：事件稀疏（23 年 34 万行），逐证券 groupby 在事件条上算滚动去重
买家数是零成本路径；面板级 NaN 默认。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd
from sqlalchemy import text

from research.factors.protocol import FactorContext, register

EVENT_SQL = """
select security_id, filing_date, owner_cik,
       coalesce(transaction_value,
                transaction_shares * transaction_price_per_share, 0)::float8 as dollars
from insider_transactions
where transaction_code = 'P'
  and record_type = 'TRANSACTION'
  and security_type = 'NON_DERIVATIVE'
  and security_id is not null
  and owner_cik is not null
  and filing_date between :start and :end
"""


def load_purchase_events(engine, *, start, end) -> pd.DataFrame:
    with engine.connect() as conn:
        rows = conn.execute(text(EVENT_SQL), {"start": start, "end": end}).fetchall()
    df = pd.DataFrame(rows, columns=["security_id", "filing_date", "owner_cik", "dollars"])
    if not df.empty:
        df["filing_date"] = pd.to_datetime(df["filing_date"])
    return df


def mark_routine(events: pd.DataFrame) -> pd.Series:
    """CMP 例行判定：owner 在此前连续 3 年同月均有 P 买入 -> 本月买入例行。

    按 owner 全历史（跨证券）判定——例行性是人的属性，不是持仓的属性。
    """
    if events.empty:
        return pd.Series(dtype=bool)
    ym = events["filing_date"].dt.year * 100 + events["filing_date"].dt.month
    owner_months = pd.DataFrame({"owner_cik": events["owner_cik"], "ym": ym}).drop_duplicates()
    key = set(map(tuple, owner_months.to_numpy()))
    prior = np.array([
        (o, y - 100) in key and (o, y - 200) in key and (o, y - 300) in key
        for o, y in zip(events["owner_cik"], ym)
    ])
    return pd.Series(prior, index=events.index)


@dataclass(frozen=True)
class InsiderClusterFactor:
    name: ClassVar[str] = "insider_cluster"
    lookback_days: ClassVar[int] = 63
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True
    window_days: int = 91  # 自然日（≈63 交易日）；filing_date 是自然日事件
    dollar_cap: float = 10_000_000.0

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        start = (ctx.dates[0] - pd.Timedelta(days=self.window_days + 14)).date()
        # 例行判定需要各 owner 此前 3 年历史：事件拉取窗口再往前推 3 年
        hist_start = (ctx.dates[0] - pd.Timedelta(days=self.window_days + 14 + 3 * 366)).date()
        events = load_purchase_events(ctx.engine, start=hist_start, end=ctx.dates[-1].date())
        out = pd.DataFrame(np.nan, index=ctx.dates, columns=ctx.security_universe)
        if events.empty:
            return out
        events = events[~mark_routine(events)]
        events = events[events["filing_date"] >= pd.Timestamp(start)]
        events = events[events["security_id"].isin(set(ctx.security_universe))]

        window = pd.Timedelta(days=self.window_days)
        for sid, g in events.groupby("security_id"):
            g = g.sort_values("filing_date")
            fd = g["filing_date"].to_numpy()
            # 事件日集合上向前滚动：每个交易日 t 的 [t-63d, t] 去重买家/总额
            lo = np.searchsorted(fd, (ctx.dates - window).values, side="left")
            hi = np.searchsorted(fd, ctx.dates.values, side="right")
            has_events = hi > lo
            if not has_events.any():
                continue
            owners = g["owner_cik"].to_numpy()
            dollars = g["dollars"].to_numpy()
            vals = np.full(len(ctx.dates), np.nan)
            for i in np.flatnonzero(has_events):
                sl = slice(lo[i], hi[i])
                vals[i] = (len(set(owners[sl]))
                           + 1e-9 * min(float(np.nansum(dollars[sl])), self.dollar_cap))
            out[sid] = vals
        return out.astype("float64")


register(InsiderClusterFactor())
