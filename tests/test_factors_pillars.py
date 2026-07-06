"""classic_pillars 三支柱因子单元测试：方向、跳月语义、min_periods、注册。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.classic_pillars as mod
from research.factors.protocol import FactorContext, get


@pytest.fixture
def ctx():
    dates = pd.bdate_range("2024-01-01", periods=300)
    return FactorContext(engine=None, dates=dates,
                         security_universe=pd.Index([1, 2, 3], dtype="int64"), as_of=None)


def _patch_prices(monkeypatch, prices):
    monkeypatch.setattr(mod, "adjusted_close_panel",
                        lambda engine, *, dates, security_ids, buffer_days: prices)


def test_momentum_skips_recent_month(ctx, monkeypatch):
    # 证券1：前 11 个月翻倍后最近 21 日暴跌——12-1 动量应仍为正（跳过近月）
    n = len(ctx.dates)
    p1 = np.concatenate([np.linspace(100, 200, n - 21), np.linspace(200, 120, 21)])
    p2 = np.full(n, 100.0)                        # 无动量
    p3 = np.linspace(200, 100, n)                 # 负动量
    prices = pd.DataFrame({1: p1, 2: p2, 3: p3}, index=ctx.dates)
    _patch_prices(monkeypatch, prices)
    out = get("momentum_12_1").compute(ctx)
    last = out.iloc[-1]
    assert last[1] > 0.5          # 跳过近月暴跌，看到的仍是翻倍段
    assert abs(last[2]) < 1e-9
    assert last[3] < 0
    assert out.iloc[100].isna().all() or np.isnan(out.iloc[100, 0]) is False  # 预热期语义由 shift 保证


def test_high_52w_nearness_bounded(ctx, monkeypatch):
    n = len(ctx.dates)
    p1 = np.linspace(100, 200, n)                 # 一路新高 → 接近 1
    p2 = np.concatenate([np.linspace(100, 200, n // 2), np.linspace(200, 100, n - n // 2)])
    prices = pd.DataFrame({1: p1, 2: p2, 3: np.full(n, 50.0)}, index=ctx.dates)
    _patch_prices(monkeypatch, prices)
    out = get("high_52w").compute(ctx)
    last = out.iloc[-1]
    assert last[1] == pytest.approx(1.0, abs=1e-9)
    assert last[2] == pytest.approx(0.5, rel=0.05)   # 从高点腰斩
    assert last[3] == pytest.approx(1.0, abs=1e-9)
    assert ((out <= 1.0 + 1e-12) | out.isna()).all().all()


def test_low_vol_prefers_quiet_names(ctx, monkeypatch):
    rng = np.random.default_rng(5)
    n = len(ctx.dates)
    quiet = 100 * np.exp(np.cumsum(rng.normal(0, 0.005, n)))
    wild = 100 * np.exp(np.cumsum(rng.normal(0, 0.05, n)))
    prices = pd.DataFrame({1: quiet, 2: wild, 3: np.full(n, 100.0)}, index=ctx.dates)
    _patch_prices(monkeypatch, prices)
    out = get("low_vol").compute(ctx)
    last = out.iloc[-1]
    assert last[1] > last[2]                     # 安静的名字分高（负波动更大）
    assert last[3] == pytest.approx(0.0, abs=1e-12)  # 零波动 → 0 为最高分
    # min_periods 语义：第 30 日（<42）应为 NaN
    assert out.iloc[30].isna().all()


def test_registration():
    assert get("momentum_12_1").lookback_days == 252
    assert get("high_52w").lookback_days == 252
    assert get("low_vol").window == 63
