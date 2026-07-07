"""wave-12 基本面族 H1-H3 因子语义测试（纯合成 / mock，无 DB）。

覆盖：
- asof_panel include_period_end 的 period_end 面板与值面板逐格 NaN 一致（纯函数）。
- gross_profitability：直报优先 / 兜底减法仅在 period_end 一致时启用 / 不一致置 NaN /
  assets<=0 置 NaN。
- accruals：符号原样（NI>CFO 为正）/ period_end 对齐门槛 / assets<=0 置 NaN。
- operating_profitability：单腿 / assets。
- company_id 广播语义（锚证券值广播回成员；无 company_id 用自身值）。
- 元属性（名字 / 无 adr_unsafe / register 可 get）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.factors.protocol import FactorContext, get
from research.fundamentals import asof_panel


DATES = pd.DatetimeIndex(pd.to_datetime(["2023-06-15", "2023-09-15"]))


def _panel(mapping: dict[int, list[float]], index=DATES) -> pd.DataFrame:
    """{security_id: [每 date 的值]} -> 宽表（列 int64）。"""
    cols = pd.Index(sorted(mapping), dtype="int64")
    data = {sid: mapping[sid] for sid in cols}
    return pd.DataFrame(data, index=index, columns=cols, dtype="float64")


def _empty_membership() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series(dtype=np.int64),
            "company_id": pd.Series(dtype=np.int64),
            "security_name": pd.Series(dtype=object),
            "is_common_equity": pd.Series(dtype=bool),
        }
    )


def _membership(rows: list[tuple[int, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": pd.Series([r[0] for r in rows], dtype=np.int64),
            "company_id": pd.Series([r[1] for r in rows], dtype=np.int64),
            "security_name": pd.Series(["x"] * len(rows), dtype=object),
            "is_common_equity": pd.Series([True] * len(rows), dtype=bool),
        }
    )


def _patch(monkeypatch, factor_module: str, panels: dict, membership: pd.DataFrame):
    """给某因子模块 mock load_fundamental_panel + 共享层 load_security_company_map。"""
    import importlib

    mod = importlib.import_module(factor_module)

    def fake_lfp(engine, *, dates, metrics, security_ids=None, include_period_end=False, **kw):
        want = set(metrics)
        out = {}
        for key, panel in panels.items():
            base = key.split("__")[0]
            if base in want:
                if key.endswith("__period_end") and not include_period_end:
                    continue
                out[key] = panel.reindex(index=pd.DatetimeIndex(pd.to_datetime(dates)))
        return out

    monkeypatch.setattr(mod, "load_fundamental_panel", fake_lfp)
    import research.factors.builtins._fundamental_ratio as fr

    monkeypatch.setattr(fr, "load_security_company_map", lambda engine, **kw: membership)


def _ctx(universe: list[int]) -> FactorContext:
    return FactorContext(
        engine=object(),
        dates=DATES,
        security_universe=pd.Index(universe, dtype="int64"),
    )


# --------------------------------------------------------------------------- #
# asof_panel include_period_end（纯函数）
# --------------------------------------------------------------------------- #
def _events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(
        rows, columns=["security_id", "metric", "period_end", "visible_date", "value"]
    )
    for col in ("period_end", "visible_date"):
        df[col] = pd.to_datetime(df[col]).astype("datetime64[ns]")
    return df


class TestAsofPeriodEnd:
    def test_period_end_panel_tracks_selected_event(self):
        dates = pd.DatetimeIndex(pd.to_datetime(["2023-03-01", "2023-06-01"]))
        events = _events(
            (1, "revenue_ttm", "2022-12-31", "2023-02-15", 100.0),
            (1, "revenue_ttm", "2023-03-31", "2023-05-10", 120.0),
        )
        panels = asof_panel(
            events, dates=dates, max_staleness_days=None, include_period_end=True
        )
        assert "revenue_ttm" in panels and "revenue_ttm__period_end" in panels
        vals = panels["revenue_ttm"][1]
        pe = panels["revenue_ttm__period_end"][1]
        assert vals.tolist() == [100.0, 120.0]
        # period_end 面板 = 所选事件的 period_end（自纪元天数）
        expected = [
            pd.Timestamp("2022-12-31").value // 86_400_000_000_000,
            pd.Timestamp("2023-03-31").value // 86_400_000_000_000,
        ]
        assert pe.tolist() == expected

    def test_period_end_nan_matches_value_nan(self):
        # 门槛前无可见事件 -> 值与 period_end 都 NaN
        dates = pd.DatetimeIndex(pd.to_datetime(["2023-01-01"]))
        events = _events((1, "revenue_ttm", "2023-03-31", "2023-05-10", 120.0))
        panels = asof_panel(events, dates=dates, include_period_end=True)
        assert panels["revenue_ttm"][1].isna().all()
        assert panels["revenue_ttm__period_end"][1].isna().all()

    def test_default_has_no_period_end_keys(self):
        events = _events((1, "revenue_ttm", "2023-03-31", "2023-05-10", 120.0))
        panels = asof_panel(events, dates=DATES)
        assert list(panels) == ["revenue_ttm"]


# --------------------------------------------------------------------------- #
# gross_profitability
# --------------------------------------------------------------------------- #
class TestGrossProfitability:
    def test_direct_fallback_period_end_and_assets_guard(self, monkeypatch):
        pe_ok, pe_bad = 20000.0, 19000.0
        panels = {
            # sec1 直报；sec2/sec3 无直报走兜底；sec4 直报但 assets<=0
            "gross_profit_ttm": _panel({1: [30.0, 30.0], 4: [50.0, 50.0]}),
            "revenue_ttm": _panel({2: [100.0, 100.0], 3: [100.0, 100.0]}),
            "cost_of_revenue_ttm": _panel({2: [60.0, 60.0], 3: [60.0, 60.0]}),
            "assets": _panel({1: [100.0, 100.0], 2: [200.0, 200.0], 3: [50.0, 50.0], 4: [0.0, 0.0]}),
            "revenue_ttm__period_end": _panel({2: [pe_ok, pe_ok], 3: [pe_ok, pe_ok]}),
            # sec2 两腿 period_end 一致 -> 兜底启用；sec3 错位 -> NaN
            "cost_of_revenue_ttm__period_end": _panel({2: [pe_ok, pe_ok], 3: [pe_bad, pe_bad]}),
        }
        _patch(monkeypatch, "research.factors.builtins.gross_profitability", panels, _empty_membership())
        out = get("gross_profitability").compute(_ctx([1, 2, 3, 4]))

        assert out.loc[DATES[0], 1] == pytest.approx(0.30)  # 直报 30/100
        assert out.loc[DATES[0], 2] == pytest.approx(0.20)  # 兜底 (100-60)/200
        assert np.isnan(out.loc[DATES[0], 3])  # period_end 错位 -> NaN
        assert np.isnan(out.loc[DATES[0], 4])  # assets<=0 -> NaN

    def test_direct_takes_priority_over_fallback(self, monkeypatch):
        # 有直报时忽略 rev-cost（即便兜底会给不同值）
        panels = {
            "gross_profit_ttm": _panel({1: [30.0, 30.0]}),
            "revenue_ttm": _panel({1: [100.0, 100.0]}),
            "cost_of_revenue_ttm": _panel({1: [10.0, 10.0]}),
            "assets": _panel({1: [100.0, 100.0]}),
            "revenue_ttm__period_end": _panel({1: [20000.0, 20000.0]}),
            "cost_of_revenue_ttm__period_end": _panel({1: [20000.0, 20000.0]}),
        }
        _patch(monkeypatch, "research.factors.builtins.gross_profitability", panels, _empty_membership())
        out = get("gross_profitability").compute(_ctx([1]))
        assert out.loc[DATES[0], 1] == pytest.approx(0.30)  # 30/100，非 (100-10)/100


# --------------------------------------------------------------------------- #
# accruals
# --------------------------------------------------------------------------- #
class TestAccruals:
    def test_sign_alignment_and_assets_guard(self, monkeypatch):
        pe_ok, pe_bad = 20000.0, 19000.0
        panels = {
            "net_income_ttm": _panel({1: [50.0, 50.0], 2: [10.0, 10.0], 3: [40.0, 40.0], 4: [40.0, 40.0]}),
            "operating_cash_flow_ttm": _panel({1: [20.0, 20.0], 2: [40.0, 40.0], 3: [10.0, 10.0], 4: [10.0, 10.0]}),
            "assets": _panel({1: [100.0, 100.0], 2: [100.0, 100.0], 3: [100.0, 100.0], 4: [0.0, 0.0]}),
            "net_income_ttm__period_end": _panel({1: [pe_ok, pe_ok], 2: [pe_ok, pe_ok], 3: [pe_ok, pe_ok], 4: [pe_ok, pe_ok]}),
            "operating_cash_flow_ttm__period_end": _panel({1: [pe_ok, pe_ok], 2: [pe_ok, pe_ok], 3: [pe_bad, pe_bad], 4: [pe_ok, pe_ok]}),
        }
        _patch(monkeypatch, "research.factors.builtins.accruals", panels, _empty_membership())
        out = get("accruals").compute(_ctx([1, 2, 3, 4]))

        # NI>CFO -> 高应计为正（不翻符号）
        assert out.loc[DATES[0], 1] == pytest.approx(0.30)  # (50-20)/100
        # NI<CFO -> 负
        assert out.loc[DATES[0], 2] == pytest.approx(-0.30)  # (10-40)/100
        # period_end 错位 -> NaN
        assert np.isnan(out.loc[DATES[0], 3])
        # assets<=0 -> NaN
        assert np.isnan(out.loc[DATES[0], 4])


# --------------------------------------------------------------------------- #
# operating_profitability + company broadcast
# --------------------------------------------------------------------------- #
class TestOperatingProfitability:
    def test_basic_ratio_and_assets_guard(self, monkeypatch):
        panels = {
            "operating_income_ttm": _panel({1: [40.0, 40.0], 2: [25.0, 25.0]}),
            "assets": _panel({1: [200.0, 200.0], 2: [0.0, 0.0]}),
        }
        _patch(monkeypatch, "research.factors.builtins.operating_profitability", panels, _empty_membership())
        out = get("operating_profitability").compute(_ctx([1, 2]))
        assert out.loc[DATES[0], 1] == pytest.approx(0.20)  # 40/200
        assert np.isnan(out.loc[DATES[0], 2])  # assets<=0

    def test_company_broadcast(self, monkeypatch):
        # sec10 是锚证券（挂事实），sec11 同公司无事实，sec12 无公司
        membership = _membership([(10, 100), (11, 100)])
        panels = {
            # 事实只挂锚证券 10；11 名下缺列（广播补齐）
            "operating_income_ttm": _panel({10: [40.0, 40.0], 12: [25.0, 25.0]}),
            "assets": _panel({10: [200.0, 200.0], 12: [100.0, 100.0]}),
        }
        _patch(monkeypatch, "research.factors.builtins.operating_profitability", panels, membership)
        out = get("operating_profitability").compute(_ctx([10, 11, 12]))
        assert out.loc[DATES[0], 10] == pytest.approx(0.20)  # 40/200
        assert out.loc[DATES[0], 11] == pytest.approx(0.20)  # 广播自公司锚 10
        assert out.loc[DATES[0], 12] == pytest.approx(0.25)  # 无公司，自身 25/100


# --------------------------------------------------------------------------- #
# 元属性
# --------------------------------------------------------------------------- #
class TestMetadata:
    @pytest.mark.parametrize(
        "name", ["gross_profitability", "accruals", "operating_profitability"]
    )
    def test_registered_metadata(self, name):
        f = get(name)
        assert f.name == name
        assert f.lookback_days == 0
        assert f.lag_days == 1
        assert f.pit_guarantee is True
        # 分子分母同源、不含股本/市值口径 -> 不设 adr_unsafe
        assert not hasattr(f, "adr_unsafe")
