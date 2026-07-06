"""residual_vol 因子单测：恒等式 vs 显式逐窗 OLS、beta 剥离语义、注册。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.residual_vol as mod
from research.factors.protocol import FactorContext, get


@pytest.fixture
def ctx():
    dates = pd.bdate_range("2024-01-01", periods=160)
    return FactorContext(engine=None, dates=dates,
                         security_universe=pd.Index([1, 2, 3, 4], dtype="int64"), as_of=None)


def _patch(monkeypatch, prices):
    monkeypatch.setattr(mod, "adjusted_close_panel",
                        lambda engine, *, dates, security_ids, buffer_days: prices)


def test_matches_explicit_ols_residual_std(ctx, monkeypatch):
    rng = np.random.default_rng(11)
    n = len(ctx.dates)
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.02, (n, 4)), axis=0)),
                          index=ctx.dates, columns=ctx.security_universe)
    _patch(monkeypatch, prices)
    out = get("residual_vol").compute(ctx)

    rets = prices.pct_change(fill_method=None)
    mkt = rets.mean(axis=1)
    t = -1  # 最后一日，显式 63 窗 OLS
    win = slice(n - 63, n)
    for sid in ctx.security_universe:
        y = rets[sid].iloc[win].to_numpy()
        x = mkt.iloc[win].to_numpy()
        beta = np.cov(x, y, ddof=0)[0, 1] / np.var(x)
        resid = (y - y.mean()) - beta * (x - x.mean())
        expected = -np.sqrt(np.mean(resid ** 2))
        assert out[sid].iloc[t] == pytest.approx(expected, rel=1e-9)


def test_pure_beta_name_scores_higher_than_idio_name(ctx, monkeypatch):
    rng = np.random.default_rng(3)
    n = len(ctx.dates)
    common = rng.normal(0, 0.02, n)
    # 证券1：纯 beta（2 倍市场，无特质）；证券2：零 beta 高特质
    r1 = 2 * common
    r2 = rng.normal(0, 0.02, n)
    r3 = common + rng.normal(0, 0.002, n)
    r4 = common
    rets = pd.DataFrame({1: r1, 2: r2, 3: r3, 4: r4}, index=ctx.dates)
    prices = 100 * (1 + rets).cumprod()
    _patch(monkeypatch, prices)
    out = get("residual_vol").compute(ctx)
    last = out.iloc[-1]
    # 高 beta 但零特质的证券1 应显著好于（分高于）纯特质的证券2——总波动版会把它们排反
    assert last[1] > last[2]
    total_vol = rets.rolling(63, min_periods=42).std().iloc[-1]
    assert total_vol[1] > total_vol[2] * 1.5  # 而它的总波动其实更大：证明剥离生效


def test_zero_variance_instrument_is_nan_not_top_signal(ctx, monkeypatch):
    rng = np.random.default_rng(4)
    n = len(ctx.dates)
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.02, (n, 4)), axis=0)),
                          index=ctx.dates, columns=ctx.security_universe)
    prices[1] = 10.0  # SPAC 信托价/挂牌工具：价格纹丝不动
    _patch(monkeypatch, prices)
    out = get("residual_vol").compute(ctx)
    assert out[1].iloc[80:].isna().all()          # 不是"最强低波动信号"，是 NaN
    assert out[[2, 3, 4]].iloc[-1].notna().all()


def test_min_days_gate(ctx, monkeypatch):
    rng = np.random.default_rng(9)
    n = len(ctx.dates)
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.02, (n, 4)), axis=0)),
                          index=ctx.dates, columns=ctx.security_universe)
    prices.iloc[: n - 30, 0] = np.nan  # 证券1 只有最近 30 日：< min_days=42
    _patch(monkeypatch, prices)
    out = get("residual_vol").compute(ctx)
    assert np.isnan(out.iloc[-1][1])
    assert out.iloc[-1][[2, 3, 4]].notna().all()


def test_registration():
    f = get("residual_vol")
    assert f.window == 63 and f.min_days == 42 and f.lag_days == 1
