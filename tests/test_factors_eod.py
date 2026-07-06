"""eod_pressure 因子族单元测试：方向、流量条件化、注册。"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.factors.builtins.eod_pressure as mod
from research.factors.protocol import FactorContext, get


@pytest.fixture
def ctx():
    dates = pd.bdate_range("2026-01-05", periods=30)
    return FactorContext(engine=None, dates=dates,
                         security_universe=pd.Index([1, 2, 3], dtype="int64"), as_of=None)


def _patch_loader(monkeypatch, ctx, ret_last30, share):
    def fake_loader(dates, security_ids, columns, **kwargs):
        panels = {"ret_last30": ret_last30, "vol_last30_share": share}
        return {c: panels[c] for c in columns}
    monkeypatch.setattr(mod, "load_minute_feature_panel", fake_loader)


def test_eod_reversal_fades_last30_move(ctx, monkeypatch):
    ret = pd.DataFrame(0.0, index=ctx.dates, columns=ctx.security_universe)
    ret[1] = -0.02   # 尾盘被砸 → 信号应为正（次日反弹候选）
    ret[2] = +0.02   # 尾盘冲刺 → 信号应为负
    ret.iloc[5, 2] = np.nan
    _patch_loader(monkeypatch, ctx, ret, ret * 0)
    out = get("eod_reversal").compute(ctx)
    assert (out[1] > 0).all() and (out[2] < 0).all()
    assert np.isnan(out.iloc[5, 2])
    assert out.shape == (len(ctx.dates), 3)


def test_eod_reversal_flow_amplifies_abnormal_volume(ctx, monkeypatch):
    ret = pd.DataFrame(-0.01, index=ctx.dates, columns=ctx.security_universe)
    share = pd.DataFrame(0.10, index=ctx.dates, columns=ctx.security_universe)
    share.iloc[-1, 1] = 0.20   # 证券2 最后一日尾盘量占比翻倍
    share[3] = 0.0             # 证券3 无量 → NaN
    _patch_loader(monkeypatch, ctx, ret, share)
    out = get("eod_reversal_flow").compute(ctx)
    # 基线期后：异常量的名字信号强于正常量的名字（同样的价格位移）
    assert out.iloc[-1, 1] > out.iloc[-1, 0] > 0
    assert out[3].isna().all()
    # 封顶：异常度不超过 3 倍
    share.iloc[-1, 1] = 5.0
    _patch_loader(monkeypatch, ctx, ret, share)
    capped = get("eod_reversal_flow").compute(ctx)
    assert capped.iloc[-1, 1] <= 0.01 * 3.0 + 1e-12


def test_registration():
    assert get("eod_reversal").lag_days == 1
    assert get("eod_reversal_flow").lookback_days == 21
