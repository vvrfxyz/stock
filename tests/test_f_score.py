"""f_score（H5 Piotroski 5 组件子集）语义金测试（纯合成 / mock，无 DB）。

覆盖：5 组件方向 1/0 归属、partial/k_available 分母缩放（k<4→NaN）、EQ_OFFER 拆股窗口
置 NaN、company 广播、元属性。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.factors.protocol import FactorContext, get
import research.factors.builtins.f_score  # noqa: F401  触发 register

DATES = pd.DatetimeIndex(pd.to_datetime(["2024-03-15", "2024-06-15"]))
_DAY = 86_400_000_000_000
CUR_PE = pd.Timestamp("2023-12-31").value // _DAY
PRIOR_PE = pd.Timestamp("2022-12-31").value // _DAY
SPLIT_DAY = pd.Timestamp("2023-06-15")  # 落在 (2022-12-31, 2023-12-31]


def _panel(mapping: dict[int, float]) -> pd.DataFrame:
    cols = pd.Index(sorted(mapping), dtype="int64")
    return pd.DataFrame(
        {sid: [mapping[sid]] * len(DATES) for sid in cols},
        index=DATES, columns=cols, dtype="float64",
    )


def _pair(cur, prior, has_pe=True):
    d = {"cur": _panel(cur), "prior": _panel(prior)}
    if has_pe:
        d["cur_pe"] = _panel({s: CUR_PE for s in cur})
        d["prior_pe"] = _panel({s: PRIOR_PE for s in cur})
    else:
        d["cur_pe"] = _panel({s: np.nan for s in cur})
        d["prior_pe"] = _panel({s: np.nan for s in cur})
    return d


def _empty_membership():
    return pd.DataFrame({
        "security_id": pd.Series(dtype=np.int64), "company_id": pd.Series(dtype=np.int64),
        "security_name": pd.Series(dtype=object), "is_common_equity": pd.Series(dtype=bool),
    })


def _membership(rows):
    return pd.DataFrame({
        "security_id": pd.Series([r[0] for r in rows], dtype=np.int64),
        "company_id": pd.Series([r[1] for r in rows], dtype=np.int64),
        "security_name": pd.Series(["x"] * len(rows), dtype=object),
        "is_common_equity": pd.Series([True] * len(rows), dtype=bool),
    })


def _install(monkeypatch, *, ni, ocf, assets, ni_pair, a_pair, sh_pair, splits, membership):
    import research.factors.builtins.f_score as mod
    import research.factors.builtins._fundamental_ratio as fr

    def fake_lfp(engine, *, dates, metrics, security_ids=None, **kw):
        m = {"net_income_ttm": ni, "operating_cash_flow_ttm": ocf, "assets": assets}
        return {k: m[k] for k in metrics if k in m}

    def fake_pairs(engine, *, dates, source_metric, security_ids=None, **kw):
        return {"net_income_ttm": ni_pair, "assets": a_pair, "shares_outstanding": sh_pair}[source_metric]

    monkeypatch.setattr(mod, "load_fundamental_panel", fake_lfp)
    monkeypatch.setattr(mod, "load_yoy_pair_panels", fake_pairs)
    monkeypatch.setattr(mod, "load_split_events", lambda engine, **kw: splits)
    monkeypatch.setattr(fr, "load_security_company_map", lambda engine, **kw: membership)


def _ctx(universe):
    return FactorContext(engine=object(), dates=DATES,
                         security_universe=pd.Index(universe, dtype="int64"))


def _no_splits():
    return pd.DataFrame({"security_id": pd.Series(dtype=np.int64),
                         "ex_date": pd.Series(dtype="datetime64[ns]"),
                         "split_from": pd.Series(dtype="float64"),
                         "split_to": pd.Series(dtype="float64")})


class TestFScoreComponentsAndDenominator:
    def test_five_securities_full_matrix(self, monkeypatch):
        # sec1 全1; sec2 全0; sec3 k=3<4→NaN; sec4 ΔROA 翻0→0.8; sec5 EQ 被拆股置NaN→k=4,F=1;
        ni     = _panel({1: 100, 2: -50, 3: 100, 4: 100, 5: 100})
        ocf    = _panel({1: 120, 2: -60, 3: 110, 4: 120, 5: 120})
        assets = _panel({1: 1000, 2: 1000, 3: 1000, 4: 1000, 5: 1000})
        # ni/assets 配对：sec3 无对（NaN）→ ΔROA NaN；sec4 ΔROA<0
        ni_pair = _pair({1: 100, 2: -50, 4: 100, 5: 100}, {1: 80, 2: -30, 4: 120, 5: 80})
        a_pair  = _pair({1: 1000, 2: 1000, 4: 1000, 5: 1000}, {1: 1000, 2: 1000, 4: 1000, 5: 1000})
        # shares 配对：sec3 无对→EQ NaN；sec5 增发(2000>1000)但拆股窗口→NaN
        sh_pair = _pair({1: 1000, 2: 1200, 4: 1000, 5: 2000}, {1: 1000, 2: 1000, 4: 1000, 5: 1000})
        splits = pd.DataFrame({"security_id": [5], "ex_date": [SPLIT_DAY],
                               "split_from": [1.0], "split_to": [2.0]})
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=splits, membership=_empty_membership())
        out = get("f_score").compute(_ctx([1, 2, 3, 4, 5]))
        row = out.loc[DATES[0]]
        assert row[1] == pytest.approx(1.0)   # 5/5
        assert row[2] == pytest.approx(0.0)   # 0/5
        assert np.isnan(row[3])               # k=3<4
        assert row[4] == pytest.approx(0.8)   # 4/5（ΔROA=0）
        assert row[5] == pytest.approx(1.0)   # EQ 被拆股置NaN → k=4, 分子4 → 4/4

    def test_roa_direction(self, monkeypatch):
        # 仅 ROA 翻转：NI<0（ROA=0）其余保持 1；NI 也进 ΔROA/ACCRUAL 故用大 OCF 保 ACCRUAL=1
        ni = _panel({1: -10}); ocf = _panel({1: 50}); assets = _panel({1: 1000})
        ni_pair = _pair({1: -10}, {1: -5})   # ROA_cur=-0.01 < prior -0.005 → ΔROA=0
        a_pair = _pair({1: 1000}, {1: 1000})
        sh_pair = _pair({1: 1000}, {1: 1000})
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=_no_splits(), membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        # ROA=0, CFO=1, ACCRUAL(50>-10)=1, ΔROA=0, EQ=1 → 3/5
        assert out.loc[DATES[0], 1] == pytest.approx(0.6)

    def test_accrual_direction(self, monkeypatch):
        # CFO<NI → ACCRUAL=0（现金流量差于账面盈利）
        ni = _panel({1: 100}); ocf = _panel({1: 50}); assets = _panel({1: 1000})
        ni_pair = _pair({1: 100}, {1: 80}); a_pair = _pair({1: 1000}, {1: 1000})
        sh_pair = _pair({1: 1000}, {1: 1000})
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=_no_splits(), membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        # ROA=1,CFO=1,ACCRUAL(50>100=0),ΔROA=1,EQ=1 → 4/5
        assert out.loc[DATES[0], 1] == pytest.approx(0.8)

    def test_eq_offer_direction(self, monkeypatch):
        # 增发（shares 增）→ EQ=0；无拆股
        ni = _panel({1: 100}); ocf = _panel({1: 120}); assets = _panel({1: 1000})
        ni_pair = _pair({1: 100}, {1: 80}); a_pair = _pair({1: 1000}, {1: 1000})
        sh_pair = _pair({1: 1500}, {1: 1000})  # 增发
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=_no_splits(), membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        # 1,1,1,1,EQ=0 → 4/5
        assert out.loc[DATES[0], 1] == pytest.approx(0.8)

    def test_k_below_floor_is_nan(self, monkeypatch):
        # 只有 3 组件可得（无 YoY 对 → ΔROA、EQ NaN）→ k=3<4 → NaN
        ni = _panel({1: 100}); ocf = _panel({1: 120}); assets = _panel({1: 1000})
        no = _pair({}, {})  # 空配对面板
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=no, a_pair=no,
                 sh_pair=no, splits=_no_splits(), membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        assert np.isnan(out.loc[DATES[0], 1])

    def test_split_outside_window_not_masked(self, monkeypatch):
        # 拆股 ex_date 在窗口外（2021 年）→ 不置 NaN，EQ 正常计
        ni = _panel({1: 100}); ocf = _panel({1: 120}); assets = _panel({1: 1000})
        ni_pair = _pair({1: 100}, {1: 80}); a_pair = _pair({1: 1000}, {1: 1000})
        sh_pair = _pair({1: 2000}, {1: 1000})  # 看似增发
        splits = pd.DataFrame({"security_id": [1], "ex_date": [pd.Timestamp("2021-06-15")],
                               "split_from": [1.0], "split_to": [2.0]})
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=splits, membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        # 窗口外拆股不 mask → EQ=0（2000>1000）→ 4/5
        assert out.loc[DATES[0], 1] == pytest.approx(0.8)


class TestFScoreBroadcast:
    def test_company_broadcast(self, monkeypatch):
        membership = _membership([(10, 100), (11, 100)])  # 锚 10，成员 11
        ni = _panel({10: 100, 12: 100}); ocf = _panel({10: 120, 12: 120})
        assets = _panel({10: 1000, 12: 1000})
        ni_pair = _pair({10: 100, 12: 100}, {10: 80, 12: 80})
        a_pair = _pair({10: 1000, 12: 1000}, {10: 1000, 12: 1000})
        sh_pair = _pair({10: 1000, 12: 1000}, {10: 1000, 12: 1000})
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=_no_splits(), membership=membership)
        out = get("f_score").compute(_ctx([10, 11, 12]))
        assert out.loc[DATES[0], 10] == pytest.approx(1.0)
        assert out.loc[DATES[0], 11] == pytest.approx(1.0)   # 广播自锚 10
        assert out.loc[DATES[0], 12] == pytest.approx(1.0)   # 无公司，自身值


class TestFScoreMetadata:
    def test_metadata(self):
        f = get("f_score")
        assert f.name == "f_score"
        assert f.lookback_days == 0 and f.lag_days == 1 and f.pit_guarantee is True
        assert not hasattr(f, "adr_unsafe")


class TestFScoreResolutionAgnostic:
    """datetime64[us] 分辨率输入锁：pandas 3.x 的 pd.Timestamp 列是 us，拆股窗口判定
    必须分辨率无关（_epoch_days 走 datetime64[D] 转换，非 astype(int64)//NS_ns）。"""

    def test_split_mask_works_with_us_resolution_dtype(self, monkeypatch):
        ni = _panel({1: 100}); ocf = _panel({1: 120}); assets = _panel({1: 1000})
        ni_pair = _pair({1: 100}, {1: 80}); a_pair = _pair({1: 1000}, {1: 1000})
        sh_pair = _pair({1: 2000}, {1: 1000})  # 看似增发
        # 显式 us 分辨率的 ex_date（模拟 pandas 3.x Timestamp 列）
        ex = pd.Series([SPLIT_DAY]).astype("datetime64[us]")
        splits = pd.DataFrame({"security_id": [1], "ex_date": ex,
                               "split_from": [1.0], "split_to": [2.0]})
        assert str(splits["ex_date"].dtype) == "datetime64[us]"
        _install(monkeypatch, ni=ni, ocf=ocf, assets=assets, ni_pair=ni_pair, a_pair=a_pair,
                 sh_pair=sh_pair, splits=splits, membership=_empty_membership())
        out = get("f_score").compute(_ctx([1]))
        # 拆股落窗口内 -> EQ 置 NaN -> k=4, 分子 4 -> F=1.0（若分辨率 bug 未修则 mask 失效 -> EQ=0 -> 0.8）
        assert out.loc[DATES[0], 1] == pytest.approx(1.0)

    def test_epoch_days_matches_across_resolutions(self):
        from research.factors.builtins.f_score import _epoch_days
        ns = pd.Series([pd.Timestamp("2023-06-15")]).astype("datetime64[ns]")
        us = pd.Series([pd.Timestamp("2023-06-15")]).astype("datetime64[us]")
        s = pd.Series([pd.Timestamp("2023-06-15")]).astype("datetime64[s]")
        assert _epoch_days(ns)[0] == _epoch_days(us)[0] == _epoch_days(s)[0]
        assert _epoch_days(us)[0] == (pd.Timestamp("2023-06-15").value // 86_400_000_000_000)
