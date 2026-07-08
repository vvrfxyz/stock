"""f_score（H5, Piotroski 2000 组件化子集）——盈利/应计质量 + 融资信号的综合分。

预注册见 docs/wave12_fundamental_hypotheses.md H5 节（含 2026-07-08"子集钉死"追加记录）。
方向为正（高 F = 基本面强 → 高后续收益）；符号原样、compile 不翻。

## 子集钉死（2026-07-08，覆盖率量化后，core 基线 NI&OCF&assets present）
Piotroski 9 组件按 >80% 覆盖率取子集，实际入选 **5 个**（其余因 XBRL 数据现实出局）：
1. **ROA**    = 1 if net_income_ttm > 0（assets>0 时 ROA>0 ⟺ NI>0）
2. **CFO**    = 1 if operating_cash_flow_ttm > 0
3. **ACCRUAL**= 1 if CFO > NI（现金流量口径应计质量，同 A 缩放故 CFO>NI）
4. **ΔROA**   = 1 if ROA_t − ROA_{t-1y} > 0（两腿各用 cur/prior 的 NI、Assets 算 ROA 再相减）
5. **EQ_OFFER**= 1 if shares_t ≤ shares_{t-1y}（未增发普通股）
出局：ΔLEVER(LTD 44%)、ΔMARGIN(GP 36%)、ΔLIQUID(77%)、ΔTURN(77%)——覆盖率不达标。

## 缺失分母口径（2026-07-08 裁决）：partial / k_available，下限 k≥4
Piotroski 原文 = 完整案例法（9 组件齐全才算）；本实现改良为 **F = Σ可得组件1/0分 /
k_available**（[0,1] 均值），**k_available < 4 → NaN**。理由：缺失记 0 会系统性压低
数据稀疏公司（小盘）分、把 F 与 size 虚假相关（CLAUDE.md 纪律点名的陷阱）；完整案例
法损覆盖。均值缩放覆盖中性、无偏。

## EQ_OFFER 拆股污染（2026-07-08 裁决）
XBRL 股本是申报口径**不随拆股回溯**（research/shares.py 已知），拆股年 shares 跳增会被
误判成增发。处理：用 corporate_actions 的 SPLIT 事件，若某证券的 split ex_date 落在该
YoY 窗口 (prior_pe, cur_pe] 内，则该证券该窗口 EQ_OFFER **置 NaN**（恰落进上面的分母
缩放语义，链条自洽）；不做拆股比例调整（更精但新增机器和错误面，保守优先）。

分子分母同源 sec_fundamental_facts、同挂 CIK 锚证券，经 company_broadcast 广播回成员列
（无 company_id 者用自身值）。不设 adr_unsafe。绝不回写事实表。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pandas as pd

from research.factors.builtins._fundamental_ratio import (
    build_membership,
    company_broadcast,
    expanded_security_ids,
)
from research.factors.protocol import FactorContext, register
from research.fundamentals import load_fundamental_panel, load_yoy_pair_panels
from research.market_cap import load_split_events

_MIN_COMPONENTS = 4  # k_available 下限（预注册裁决）


def _epoch_days(s: pd.Series) -> np.ndarray:
    """时间序列 -> 自纪元天数（int64），**分辨率无关**。

    pandas 3.x 里 pd.Timestamp 字面量建的列是 datetime64[us]（非 ns），
    `astype("int64") // 86_400_000_000_000`（纳秒常量）会静默算出微秒天数
    （day=19 而非 19523），拆股窗口判定/period_end 对齐门无声失效。
    datetime64[D] 转换对 ns/us/s 任何源分辨率一律输出正确的自纪元天数。
    """
    return s.to_numpy().astype("datetime64[D]").astype("int64")


def _score(condition: pd.DataFrame, valid: pd.DataFrame) -> pd.DataFrame:
    """条件 -> 1.0/0.0（valid 处）/ NaN（invalid 处）。"""
    return condition.astype("float64").where(valid)


def _apply_split_mask(
    eq: pd.DataFrame,
    cur_pe: pd.DataFrame,
    prior_pe: pd.DataFrame,
    splits: pd.DataFrame,
) -> pd.DataFrame:
    """拆股 ex_date 落在 YoY 窗口 (prior_pe, cur_pe] 内的 (证券,日) 格置 NaN。

    逐"有拆股的证券"（稀疏事件维度）取列一次、OR 掉其各 split 窗口——不做逐日/全证券循环。
    """
    if splits.empty:
        return eq
    eq = eq.copy()
    split_days = pd.Series(_epoch_days(splits["ex_date"]), index=splits.index)
    cols = set(eq.columns)
    for sec, grp in split_days.groupby(splits["security_id"]):
        if sec not in cols:
            continue
        pp = prior_pe[sec].to_numpy()
        cp = cur_pe[sec].to_numpy()
        contaminated = np.zeros(len(eq.index), dtype=bool)
        for s in grp.to_numpy():
            contaminated |= (pp < s) & (s <= cp)
        if contaminated.any():
            eq.iloc[contaminated, eq.columns.get_loc(sec)] = np.nan
    return eq


@dataclass(frozen=True)
class FScoreFactor:
    name: ClassVar[str] = "f_score"
    lookback_days: ClassVar[int] = 0
    lag_days: ClassVar[int] = 1
    pit_guarantee: ClassVar[bool] = True

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        universe = pd.Index([int(s) for s in ctx.security_universe], dtype="int64")
        sid_to_cid = build_membership(ctx.engine, universe)
        expanded = expanded_security_ids(universe, sid_to_cid)
        cols = pd.Index(expanded, dtype="int64")
        dates = pd.DatetimeIndex(pd.to_datetime(ctx.dates))

        def reidx(df: pd.DataFrame) -> pd.DataFrame:
            return df.reindex(index=dates, columns=cols)

        # 当前面板（ROA/CFO/ACCRUAL 用最新时点，不需要配对）
        cur = load_fundamental_panel(
            ctx.engine, dates=dates,
            metrics=("net_income_ttm", "operating_cash_flow_ttm", "assets"),
            security_ids=expanded,
        )
        empty = pd.DataFrame(index=dates, columns=cols, dtype="float64")
        ni = reidx(cur.get("net_income_ttm", empty))
        ocf = reidx(cur.get("operating_cash_flow_ttm", empty))
        assets = reidx(cur.get("assets", empty))

        # YoY 配对面板（ΔROA 用 NI/A 两腿原值；EQ_OFFER 用 shares 两腿 + period_end 窗口）
        ni_p = load_yoy_pair_panels(ctx.engine, dates=dates, source_metric="net_income_ttm", security_ids=expanded)
        a_p = load_yoy_pair_panels(ctx.engine, dates=dates, source_metric="assets", security_ids=expanded)
        sh_p = load_yoy_pair_panels(ctx.engine, dates=dates, source_metric="shares_outstanding", security_ids=expanded)

        # --- 5 组件（1.0/0.0/NaN）---
        c_roa = _score(ni > 0, ni.notna() & (assets > 0))
        c_cfo = _score(ocf > 0, ocf.notna())
        c_accrual = _score(ocf > ni, ocf.notna() & ni.notna())

        ni_cur, ni_prior = reidx(ni_p["cur"]), reidx(ni_p["prior"])
        a_cur, a_prior = reidx(a_p["cur"]), reidx(a_p["prior"])
        roa_cur = ni_cur / a_cur.where(a_cur > 0)
        roa_prior = ni_prior / a_prior.where(a_prior > 0)
        c_droa = _score(roa_cur - roa_prior > 0, roa_cur.notna() & roa_prior.notna())

        sh_cur, sh_prior = reidx(sh_p["cur"]), reidx(sh_p["prior"])
        c_eq = _score(sh_cur <= sh_prior, sh_cur.notna() & sh_prior.notna())
        splits = load_split_events(ctx.engine, security_ids=expanded)
        c_eq = _apply_split_mask(c_eq, reidx(sh_p["cur_pe"]), reidx(sh_p["prior_pe"]), splits)

        # --- 合成：partial / k_available，k<4 -> NaN（分母缩放，覆盖中性）---
        comps = [c_roa, c_cfo, c_accrual, c_droa, c_eq]
        k = sum(c.notna().astype("int64") for c in comps)  # 可得组件数
        score_sum = sum(c.fillna(0.0) for c in comps)
        f = score_sum / k.where(k >= _MIN_COMPONENTS)  # k<4 -> NaN；无 inf

        broadcast = company_broadcast(f, universe, sid_to_cid)
        return broadcast.reindex(
            index=ctx.dates, columns=ctx.security_universe
        ).astype("float64")


register(FScoreFactor())
