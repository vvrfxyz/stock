"""ta_zoo 八指标单测：方向、边界（零量/零区间/NaN）、注册。合成面板 mock 双装载器。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.ta_zoo as mod
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
    monkeypatch.setattr(mod, "raw_bar_panels",
                        lambda engine, *, dates, security_ids, columns, buffer_days:
                        {c: raw[c] for c in columns})
    monkeypatch.setattr(mod, "adjusted_close_panel",
                        lambda engine, *, dates, security_ids, buffer_days: close)


def test_obv_and_adline_favor_accumulation(ctx, monkeypatch):
    n = len(ctx.dates)
    rng = np.random.default_rng(2)
    base = 100 + np.cumsum(rng.normal(0, 0.1, n))
    close = pd.DataFrame({1: base + np.linspace(0, 5, n),      # 温和上行
                          2: base - np.linspace(0, 5, n),      # 温和下行
                          3: base}, index=ctx.dates)
    vol = close * 0 + 1e6
    vol[1] = np.where(close[1].diff() > 0, 3e6, 5e5)           # 上涨放量=吸筹
    vol[2] = np.where(close[2].diff() > 0, 5e5, 3e6)           # 下跌放量=派发
    _patch(monkeypatch, ctx, close, volume=pd.DataFrame(vol, index=ctx.dates, columns=close.columns))
    obv = get("obv_slope").compute(ctx)
    assert obv.iloc[-1][1] > 0 > obv.iloc[-1][2]
    ad = get("adline_slope").compute(ctx)
    assert ad.shape == (len(ctx.dates), 3)


def test_oscillators_negative_after_runup(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.linspace(100, 200, n),          # 单边暴涨：RSI/MFI 高、%B 高
                          2: np.linspace(200, 100, n),
                          3: np.full(n, 150.0)}, index=ctx.dates)
    _patch(monkeypatch, ctx, close)
    for name in ("rsi_14", "mfi_14", "bollinger_b"):
        out = get(name).compute(ctx)
        assert out.iloc[-1][1] < out.iloc[-1][2], name          # 超买者分低（负向定义）


def test_trend_family_positive_in_uptrend(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.linspace(100, 200, n),
                          2: np.linspace(200, 100, n),
                          3: np.full(n, 150.0)}, index=ctx.dates)
    _patch(monkeypatch, ctx, close)
    for name, hi_expect in (("macd_hist", 1), ("donchian_pos", 1), ("sma_gap_50", 1)):
        out = get(name).compute(ctx)
        assert out.iloc[-1][hi_expect] > out.iloc[-1][2], name
    assert get("donchian_pos").compute(ctx).iloc[-1][1] == pytest.approx(1.0, abs=1e-9)


def test_zero_range_and_zero_volume_are_nan(ctx, monkeypatch):
    n = len(ctx.dates)
    close = pd.DataFrame({1: np.full(n, 10.0), 2: np.linspace(90, 110, n),
                          3: np.linspace(90, 110, n)}, index=ctx.dates)
    vol = close * 0 + 1e6
    vol[1] = 0.0
    _patch(monkeypatch, ctx, close, volume=pd.DataFrame(vol, index=ctx.dates, columns=close.columns),
           high=close.copy(), low=close.copy())                  # 证券1 恒价+零量+零区间
    assert np.isnan(get("obv_slope").compute(ctx).iloc[-1][1])
    assert np.isnan(get("adline_slope").compute(ctx).iloc[-1][1])
    assert np.isnan(get("rsi_14").compute(ctx).iloc[-1][1])     # gain+loss=0
    assert np.isnan(get("bollinger_b").compute(ctx).iloc[-1][1])
    assert np.isnan(get("donchian_pos").compute(ctx).iloc[-1][1])


def test_registration():
    for name in ("obv_slope", "adline_slope", "mfi_14", "rsi_14",
                 "macd_hist", "bollinger_b", "donchian_pos", "sma_gap_50"):
        assert get(name).lag_days == 1, name
