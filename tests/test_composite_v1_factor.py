"""composite_v1 注册因子金测试（路线图 W0-P0 验收）。

锁定：注册 builtin `composite_v1` 的复合构造与 composite_study 共享的 build 函数
在含 NaN / 并列 / 全缺行的合成面板上，与本文件独立重写的参照实现位级一致（1e-12）；
外加元属性（name / 成分写死 / adr_unsafe / lookback）与 compute 接线断言。

参照实现刻意用逐行 Python 循环重写（与生产的向量化实现独立），才是真正的对拍。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from research.factors.builtins import composite_v1
from research.factors.builtins.composite_v1 import (
    COMPONENTS,
    CompositeV1Factor,
    build_composite,
    combine_ranks,
    eligible_component_ranks,
    residualize_high_52w,
    rowwise_ols_residual_rank,
)
from research.factors.protocol import FactorContext, get


# ---- 独立参照实现（逐行循环，与向量化实现无共享代码）----

def _ref_residual_rank(y_rank: pd.DataFrame, x_rank: pd.DataFrame) -> pd.DataFrame:
    """逐行横截面 OLS 残差 -> 当日重排 [0,1]。参照实现，逐行显式。"""
    y, x = y_rank.to_numpy(), x_rank.to_numpy()
    resid = np.full_like(y, np.nan, dtype="float64")
    for r in range(y.shape[0]):
        m = ~np.isnan(y[r]) & ~np.isnan(x[r])
        if m.sum() == 0:
            continue
        xv, yv = x[r, m], y[r, m]
        dx, dy = xv - xv.mean(), yv - yv.mean()
        denom = float((dx * dx).sum())
        with np.errstate(invalid="ignore", divide="ignore"):
            beta = (dx * dy).sum() / denom  # denom==0 -> nan/inf，与向量化同
        resid[r, m] = dy - beta * dx
    return pd.DataFrame(resid, index=y_rank.index, columns=y_rank.columns).rank(axis=1, pct=True)


def _ref_combine(ranks: dict[str, pd.DataFrame], names) -> pd.DataFrame:
    """0.5 中性填补聚合 + low_vol 在场门。参照实现，逐格显式。"""
    names = list(names)
    lv = ranks["low_vol"]
    idx, cols = lv.index, lv.columns
    k = len(names)
    arrs = [ranks[n].to_numpy() for n in names]
    lv_np = lv.to_numpy()
    out = np.full((len(idx), len(cols)), np.nan, dtype="float64")
    for t in range(len(idx)):
        for i in range(len(cols)):
            if np.isnan(lv_np[t, i]):        # low_vol 在场门
                continue
            present = [a[t, i] for a in arrs if not np.isnan(a[t, i])]
            out[t, i] = (sum(present) + 0.5 * (k - len(present))) / k
    return pd.DataFrame(out, index=idx, columns=cols)


def _ref_build(ranks: dict[str, pd.DataFrame], names) -> pd.DataFrame:
    r = dict(ranks)
    if "high_52w" in names:
        r["high_52w"] = _ref_residual_rank(ranks["high_52w"], ranks["low_vol"])
    return _ref_combine(r, names)


# ---- 合成面板 ----

@pytest.fixture
def synthetic_ranks() -> dict[str, pd.DataFrame]:
    """三成分的百分位秩面板（含 NaN / 并列 / 一整行 low_vol 全缺触发在场门）。"""
    dates = pd.date_range("2020-01-01", periods=8, freq="B")
    secs = pd.Index([10, 20, 30, 40, 50, 60, 70], dtype="int64")
    rng = np.random.default_rng(0)
    ranks: dict[str, pd.DataFrame] = {}
    for j, name in enumerate(COMPONENTS):
        raw = rng.normal(size=(len(dates), len(secs)))
        mask = rng.random(size=raw.shape) < 0.18   # ~18% 缺失
        raw[mask] = np.nan
        raw[2, 1] = raw[2, 3]                        # 制造并列
        ranks[name] = pd.DataFrame(raw, index=dates, columns=secs).rank(axis=1, pct=True)
    ranks["low_vol"].iloc[5, :] = np.nan             # 整行主干缺失（在场门）
    ranks["low_vol"].iloc[0, 0] = np.nan             # 单格主干缺失
    return ranks


# ---- 位级一致性 ----

def test_residualize_matches_reference(synthetic_ranks):
    got = rowwise_ols_residual_rank(synthetic_ranks["high_52w"], synthetic_ranks["low_vol"])
    ref = _ref_residual_rank(synthetic_ranks["high_52w"], synthetic_ranks["low_vol"])
    assert_frame_equal(got, ref, rtol=0, atol=1e-12)


def test_combine_ranks_matches_reference(synthetic_ranks):
    got = combine_ranks(synthetic_ranks, COMPONENTS)
    ref = _ref_combine(synthetic_ranks, COMPONENTS)
    assert_frame_equal(got, ref, rtol=0, atol=1e-12)


def test_build_composite_matches_reference(synthetic_ranks):
    got = build_composite(synthetic_ranks, COMPONENTS)
    ref = _ref_build(synthetic_ranks, COMPONENTS)
    assert_frame_equal(got, ref, rtol=0, atol=1e-12)


def test_low_vol_gate_masks_exactly(synthetic_ranks):
    composite = build_composite(synthetic_ranks, COMPONENTS)
    # 复合分为 NaN 当且仅当主干 low_vol 秩缺失
    assert (composite.isna() == synthetic_ranks["low_vol"].isna()).all().all()


def test_residualize_does_not_mutate_caller(synthetic_ranks):
    before = synthetic_ranks["high_52w"].copy()
    residualize_high_52w(synthetic_ranks, COMPONENTS)
    assert_frame_equal(synthetic_ranks["high_52w"], before)


# ---- compute 接线（mock 成分 + eligibility，免 DB）----

class _MockFactor:
    def __init__(self, panel: pd.DataFrame):
        self._panel = panel

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        return self._panel


def test_compute_wires_eligible_rank_and_build(monkeypatch, synthetic_ranks):
    dates = synthetic_ranks["low_vol"].index
    secs = synthetic_ranks["low_vol"].columns
    ctx = FactorContext(engine=None, dates=dates, security_universe=secs, as_of=dates[-1])

    rng = np.random.default_rng(1)
    panels = {n: pd.DataFrame(rng.normal(size=(len(dates), len(secs))), index=dates, columns=secs)
              for n in COMPONENTS}
    # eligible：随机 bool + 一整列不可交易，验证"先掩后排名"
    elig_np = rng.random(size=(len(dates), len(secs))) < 0.75
    elig_np[:, 2] = False
    eligible = pd.DataFrame(elig_np, index=dates, columns=secs)

    monkeypatch.setattr(composite_v1, "get", lambda name: _MockFactor(panels[name]))
    monkeypatch.setattr(composite_v1, "composite_eligibility", lambda ctx: eligible)

    got = CompositeV1Factor().compute(ctx)

    expected_ranks = {n: panels[n].where(eligible).rank(axis=1, pct=True) for n in COMPONENTS}
    expected = build_composite(expected_ranks, COMPONENTS).reindex(
        index=dates, columns=secs).astype("float64")
    assert_frame_equal(got, expected, rtol=0, atol=1e-12)

    # eligible_component_ranks 与手工掩码+排名一致（先掩后排名语义）
    got_ranks = eligible_component_ranks(ctx, eligible, COMPONENTS)
    for n in COMPONENTS:
        assert_frame_equal(got_ranks[n], expected_ranks[n])


# ---- 元属性 ----

def test_components_are_hardwired_skeleton():
    assert COMPONENTS == ("low_vol", "high_52w", "size")


def test_metadata():
    f = get("composite_v1")
    assert isinstance(f, CompositeV1Factor)
    assert f.name == "composite_v1"
    assert f.adr_unsafe is True           # 成分含 size（股本口径）
    assert f.lag_days == 1
    assert f.pit_guarantee is True
    # lookback = 成分最大回看（high_52w 252 主导）
    assert f.lookback_days == 252


def test_factory_is_frozen_dataclass():
    f = CompositeV1Factor()
    with pytest.raises(Exception):
        f.name = "x"      # frozen dataclass 拒绝赋值


# ---------------------------------------------------------------------------
# 审核 #7：composite_eligibility 本体免 DB 测试（不 mock 它自己——mock 它的两个
# 数据依赖 raw_bar_panels / securities_with_uncovered_events）
# ---------------------------------------------------------------------------

class TestCompositeEligibility:
    def _wire(self, monkeypatch, *, n_days=70, uncovered=()):
        import research.factors.builtins.composite_v1 as mod

        dates = pd.bdate_range("2025-01-02", periods=n_days)
        universe = pd.Index([101, 102, 103], dtype="int64")
        close = pd.DataFrame(
            {101: 10.0, 102: 10.0, 103: 1.0},  # 103 价格低于 3 美元门
            index=dates, columns=universe, dtype="float64")
        volume = pd.DataFrame(1_000_000.0, index=dates, columns=universe)  # $10M/日（101/102 过门）

        def fake_bars(engine, *, dates, security_ids, columns, buffer_days):
            assert buffer_days == mod._ELIGIBILITY_BUFFER_DAYS
            return {"close": close, "volume": volume}

        monkeypatch.setattr(mod, "raw_bar_panels", fake_bars)
        monkeypatch.setattr(mod, "securities_with_uncovered_events",
                            lambda engine, *, start, end: list(uncovered))
        ctx = FactorContext(engine=None, dates=dates, security_universe=universe,
                            as_of=dates[-1])
        return mod, ctx, dates, universe

    def test_price_gate_and_warmup(self, monkeypatch):
        mod, ctx, dates, universe = self._wire(monkeypatch)
        out = mod.composite_eligibility(ctx)
        assert out.index.equals(dates) and out.columns.equals(universe)
        assert out.dtypes.eq(bool).all()
        # 63 日滚动中位需暖机：前 62 行全 False（min_periods=window）
        assert not out.iloc[:62].any().any()
        # 暖机后：101/102 双门皆过；103 被 3 美元价格门挡下
        assert out.iloc[65][101] and out.iloc[65][102]
        assert not out[103].any()

    def test_uncovered_gate_zeroes_whole_column(self, monkeypatch):
        mod, ctx, dates, universe = self._wire(monkeypatch, uncovered=(102,))
        out = mod.composite_eligibility(ctx)
        assert not out[102].any()          # gate 整列剔除
        assert out.iloc[65][101]           # 其余列不受影响

    def test_universe_extra_column_filled_false(self, monkeypatch):
        # ctx.security_universe 比 bar 面板多一列（无价格数据）→ 该列必须补 False 而非 NaN/报错
        mod, ctx, dates, universe = self._wire(monkeypatch)
        wide = pd.Index([101, 102, 103, 999], dtype="int64")
        ctx2 = FactorContext(engine=None, dates=dates, security_universe=wide, as_of=dates[-1])
        out = mod.composite_eligibility(ctx2)
        assert out.columns.equals(wide)
        assert not out[999].any()
        assert out.dtypes.eq(bool).all()
