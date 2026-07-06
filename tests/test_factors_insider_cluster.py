"""insider_cluster 因子单测：CMP 例行过滤、窗口语义、去重买家计数、金额破并列。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.insider_cluster as mod
from research.factors.protocol import FactorContext, get


@pytest.fixture
def ctx():
    dates = pd.bdate_range("2026-01-05", periods=60)
    return FactorContext(engine=None, dates=dates,
                         security_universe=pd.Index([1, 2, 3], dtype="int64"), as_of=None)


def _patch_events(monkeypatch, rows):
    df = pd.DataFrame(rows, columns=["security_id", "filing_date", "owner_cik", "dollars"])
    df["filing_date"] = pd.to_datetime(df["filing_date"])
    monkeypatch.setattr(mod, "load_purchase_events", lambda engine, *, start, end: df.copy())


def test_distinct_buyer_count_and_window(ctx, monkeypatch):
    _patch_events(monkeypatch, [
        (1, "2026-01-10", "A", 1e5), (1, "2026-01-15", "B", 1e5), (1, "2026-01-20", "A", 1e5),
        (2, "2025-11-01", "C", 1e5),   # 距评估尾 >91 自然日 -> 窗口外
    ])
    out = get("insider_cluster").compute(ctx)
    last = out.iloc[-1]
    assert 2.0 <= last[1] < 2.001          # 去重买家 2（A 重复不重计）
    assert np.isnan(last[2])               # 旧事件滑出窗口
    assert np.isnan(last[3])               # 无事件 = NaN（条件因子）
    # 事件发生前的日子也是 NaN
    assert np.isnan(out.loc["2026-01-08", 1])


def test_routine_owner_filtered(ctx, monkeypatch):
    rows = [(1, f"{y}-01-15", "R", 1e5) for y in (2023, 2024, 2025, 2026)]  # 连续 4 年 1 月
    rows.append((2, "2026-01-15", "N", 1e5))                                # 非例行对照
    _patch_events(monkeypatch, rows)
    out = get("insider_cluster").compute(ctx)
    assert np.isnan(out.iloc[-1][1])       # 2026-01 的 R 买入：前 3 年同月都买过 -> 例行剔除
    assert out.iloc[-1][2] >= 1.0


def test_dollar_tiebreak_orders_same_count(ctx, monkeypatch):
    _patch_events(monkeypatch, [
        (1, "2026-02-10", "A", 9_000_000.0),
        (2, "2026-02-10", "B", 10_000.0),
    ])
    out = get("insider_cluster").compute(ctx)
    last = out.iloc[-1]
    assert last[1] > last[2] > 1.0         # 同为 1 买家，大额优先
    assert last[1] - last[2] < 0.05        # 但只是破并列的小项


def test_registration():
    f = get("insider_cluster")
    assert f.window_days == 91 and f.lag_days == 1
