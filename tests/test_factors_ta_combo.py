"""ta_combo 三因子单测：集成方差缩减语义、量能条件化方向、ATR 归一化、注册。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.ta_combo  # noqa: F401  # 触发注册
import research.factors.builtins.ta_zoo as zoo
from research.factors.protocol import FactorContext, get


@pytest.fixture
def ctx():
    dates = pd.bdate_range("2024-01-01", periods=200)
    return FactorContext(engine=None, dates=dates,
                         security_universe=pd.Index([1, 2, 3], dtype="int64"), as_of=None)


def _patch(monkeypatch, ctx, close, volume=None, high=None, low=None):
    volume = volume if volume is not None else close * 0 + 1e6
    high = high if high is not None else close * 1.01
    low = low if low is not None else close * 0.99
    raw = {"open": close.copy(), "high": high, "low": low, "close": close, "volume": volume}
    # ta_combo 复用 ta_zoo._panels，打 zoo 的装载器即可全覆盖
    monkeypatch.setattr(zoo, "raw_bar_panels",
                        lambda engine, *, dates, security_ids, columns, buffer_days:
                        {c: raw[c] for c in columns})
    monkeypatch.setattr(zoo, "adjusted_close_panel",
                        lambda engine, *, dates, security_ids, buffer_days: close)


def test_ensemble_orders_overbought_below_oversold(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.linspace(100, 200, n),      # 单边暴涨=超买
                          2: np.linspace(200, 100, n),      # 单边暴跌=超卖
                          3: np.full(n, 150.0)}, index=ctx.dates)
    _patch(monkeypatch, ctx, close)
    out = get("reversal_ensemble").compute(ctx)
    last = out.iloc[-1]
    assert last[2] > last[1]                     # 超卖分高（反转正向定义）
    assert ((out >= 0) & (out <= 1) | out.isna()).all().all()   # 秩均值落 [0,1]


def test_ensemble_requires_all_members(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.linspace(100, 200, n), 2: np.linspace(200, 100, n),
                          3: np.full(n, 150.0)}, index=ctx.dates)
    _patch(monkeypatch, ctx, close, volume=pd.DataFrame(
        {1: np.full(n, 1e6), 2: np.full(n, 1e6), 3: np.zeros(n)}, index=ctx.dates))
    out = get("reversal_ensemble").compute(ctx)
    assert out[3].iloc[-1:].isna().all()         # 证券3 零量→mfi NaN→集成 NaN（全员口径）


def test_volume_confirmation_amplifies(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.linspace(200, 100, n), 2: np.linspace(200, 100, n),
                          3: np.full(n, 150.0)}, index=ctx.dates)
    vol = pd.DataFrame(1e6, index=ctx.dates, columns=close.columns)
    vol.iloc[-1, 0] = 2.5e6                       # 证券1 末日放量 2.5 倍
    _patch(monkeypatch, ctx, close, volume=vol)
    out = get("volume_confirmed_reversal").compute(ctx)
    base = get("bollinger_b").compute(ctx)
    last, base_last = out.iloc[-1], base.iloc[-1]
    assert base_last[1] == pytest.approx(base_last[2], rel=1e-6)   # 同价格路径同裸信号
    assert last[1] > last[2] > 0                  # 放量者条件化后更强（正超卖信号放大）
    # cap 生效：10 倍量也只按 3 倍计（对零居中信号）
    vol.iloc[-1, 0] = 1e7
    _patch(monkeypatch, ctx, close, volume=vol)
    capped = get("volume_confirmed_reversal").compute(ctx)
    assert capped.iloc[-1][1] == pytest.approx((base_last[1] + 0.5) * 3.0, rel=1e-6)


def test_atr_trend_normalizes_volatility(ctx, monkeypatch):
    rng = np.random.default_rng(6)
    n = len(ctx.dates)
    drift = np.linspace(0, 0.3, n)
    quiet = 100 * np.exp(drift + np.cumsum(rng.normal(0, 0.003, n)))
    wild = 100 * np.exp(drift + np.cumsum(rng.normal(0, 0.04, n)))
    close = pd.DataFrame({1: quiet, 2: wild, 3: np.full(n, 100.0)}, index=ctx.dates)
    high = close * pd.DataFrame({1: np.full(n, 1.004), 2: np.full(n, 1.05),
                                 3: np.full(n, 1.0)}, index=ctx.dates)
    low = close * pd.DataFrame({1: np.full(n, 0.996), 2: np.full(n, 0.95),
                                3: np.full(n, 1.0)}, index=ctx.dates)
    _patch(monkeypatch, ctx, close, high=high, low=low)
    out = get("atr_trend").compute(ctx)
    last = out.iloc[-1]
    assert last[1] > last[2]                      # 同漂移：安静者 ATR 小→归一化趋势强
    assert np.isnan(last[3])                      # 零区间零位移→ATR=0→NaN


def test_registration():
    assert get("reversal_ensemble").lag_days == 1
    assert get("volume_confirmed_reversal").cap == 3.0
    assert get("atr_trend").atr_window == 21
