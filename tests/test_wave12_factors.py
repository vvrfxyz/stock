"""wave-12 基本面族 H1-H4 因子语义测试（纯合成 / mock，无 DB）。

覆盖：
- asof_panel include_period_end 的 period_end 面板与值面板逐格 NaN 一致（纯函数）。
- gross_profitability：直报优先 / 兜底减法仅在 period_end 一致时启用 / 不一致置 NaN /
  assets<=0 置 NaN。
- accruals：符号原样（NI>CFO 为正）/ period_end 对齐门槛 / assets<=0 置 NaN。
- operating_profitability：单腿 / assets。
- asset_growth（H4）：事件层 YoY 上一期值机制（配对窗 / 重述重发 / 单调护栏 /
  prior<=0 置 NaN）+ 因子符号原样 + company 广播。
- company_id 广播语义（锚证券值广播回成员；无 company_id 用自身值）。
- 元属性（名字 / 无 adr_unsafe / register 可 get）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.factors.protocol import FactorContext, get
from research.fundamentals import asof_panel, build_yoy_ratio_events

# 显式 import 触发四个因子的 register()，不依赖测试顺序副作用
import research.factors.builtins.accruals  # noqa: E402,F401
import research.factors.builtins.asset_growth  # noqa: E402,F401
import research.factors.builtins.gross_profitability  # noqa: E402,F401
import research.factors.builtins.operating_profitability  # noqa: E402,F401


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
        "name",
        ["gross_profitability", "accruals", "operating_profitability", "asset_growth"],
    )
    def test_registered_metadata(self, name):
        f = get(name)
        assert f.name == name
        assert f.lookback_days == 0
        assert f.lag_days == 1
        assert f.pit_guarantee is True
        # 分子分母同源、不含股本/市值口径 -> 不设 adr_unsafe
        assert not hasattr(f, "adr_unsafe")


# --------------------------------------------------------------------------- #
# asset_growth（H4）：事件层 YoY 上一期值机制
# --------------------------------------------------------------------------- #
def _yoy_facts(*rows) -> pd.DataFrame:
    """(security_id, concept, period_end, filed_date, value) -> facts 长表。"""
    df = pd.DataFrame(
        rows, columns=["security_id", "concept", "period_end", "filed_date", "value"]
    )
    for col in ("period_end", "filed_date"):
        df[col] = pd.to_datetime(df[col]).astype("datetime64[ns]")
    df["value"] = df["value"].astype("float64")
    return df


def _yoy_events(facts):
    return build_yoy_ratio_events(
        facts, source_metric="assets", out_metric="asset_growth"
    )


class TestYoYMechanism:
    def test_basic_yoy_ratio(self):
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        ev = _yoy_events(facts)
        assert len(ev) == 1
        row = ev.iloc[0]
        assert row["metric"] == "asset_growth"
        assert row["period_end"] == pd.Timestamp("2023-12-31")
        assert row["visible_date"] == pd.Timestamp("2024-02-15")  # max(两腿 filed)
        assert row["value"] == pytest.approx(0.20)  # 1200/1000-1

    def test_visible_date_is_max_of_both_legs(self):
        # 上一期在自己年份的 10-K 先报（2023-02-15），当前期后报（2024-02-15）
        # -> 首个联合事件可见日 = 后到腿 = 2024-02-15
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        ev = _yoy_events(facts)
        assert ev.iloc[0]["visible_date"] == pd.Timestamp("2024-02-15")

    def test_restatement_of_prior_leg_reemits(self):
        # 上一期 Assets 被重述（2022-12-31: 1000 -> 800），YoY 在重述 filed_date 重发
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
            (1, "Assets", "2022-12-31", "2024-05-01", 800.0),  # 重述
        )
        ev = _yoy_events(facts).sort_values("visible_date").reset_index(drop=True)
        assert len(ev) == 2
        assert ev.loc[0, "value"] == pytest.approx(0.20)  # 1200/1000-1
        assert ev.loc[0, "visible_date"] == pd.Timestamp("2024-02-15")
        assert ev.loc[1, "value"] == pytest.approx(0.50)  # 1200/800-1
        assert ev.loc[1, "visible_date"] == pd.Timestamp("2024-05-01")

        # as-of 语义：重述前看旧值，重述后看新值（visible_delay=1）
        dates = pd.DatetimeIndex(
            pd.to_datetime(["2024-01-01", "2024-03-01", "2024-06-01"])
        )
        panel = asof_panel(ev, dates=dates, max_staleness_days=None)["asset_growth"]
        col = panel[1]
        assert np.isnan(col.iloc[0])  # 首个事件可见前
        assert col.iloc[1] == pytest.approx(0.20)  # 重述前
        assert col.iloc[2] == pytest.approx(0.50)  # 重述后

    def test_restatement_of_current_leg_reemits(self):
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
            (1, "Assets", "2023-12-31", "2024-05-01", 1500.0),  # 当前腿重述
        )
        ev = _yoy_events(facts).sort_values("visible_date").reset_index(drop=True)
        assert ev["value"].tolist() == pytest.approx([0.20, 0.50])  # 1200 -> 1500

    def test_no_prior_year_no_event(self):
        facts = _yoy_facts((1, "Assets", "2023-12-31", "2024-02-15", 1200.0))
        assert _yoy_events(facts).empty

    def test_quarter_gap_not_paired_as_prior_year(self):
        # 只有相隔 91 天的两期 -> 不在 [330,400] 窗内 -> 不产 YoY
        facts = _yoy_facts(
            (1, "Assets", "2023-09-30", "2023-11-01", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        assert _yoy_events(facts).empty

    def test_quarterly_reporter_pairs_same_quarter_last_year(self):
        # 季度报：2023-12-31 与 2022-12-31 配对（~365 天），非上一季度
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-09-30", "2023-11-01", 1150.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        ev = _yoy_events(facts)
        assert len(ev) == 1
        assert ev.iloc[0]["period_end"] == pd.Timestamp("2023-12-31")
        assert ev.iloc[0]["value"] == pytest.approx(0.20)  # 1200/1000-1

    def test_prior_non_positive_gives_no_event(self):
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 0.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        assert _yoy_events(facts).empty

    def test_monotonic_guard_drops_late_old_period_reemit(self):
        # 迟到的更旧 cur_pe 重述（2021 assets 改动 -> 2022 的 YoY 重发）不得倒退 as-of 序列
        facts = _yoy_facts(
            (1, "Assets", "2021-12-31", "2022-02-15", 900.0),
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
            (1, "Assets", "2021-12-31", "2024-06-01", 500.0),  # 迟到重述老年份
        )
        ev = _yoy_events(facts)
        dates = pd.DatetimeIndex(pd.to_datetime(["2024-07-01"]))
        panel = asof_panel(ev, dates=dates, max_staleness_days=None)["asset_growth"]
        # 2023 的 YoY（period_end 2023-12-31, 值 0.20）仍是最新，未被 2022 迟到重述倒退
        assert panel[1].iloc[0] == pytest.approx(0.20)


class TestAssetGrowthFactor:
    def _patch_panel(self, monkeypatch, panel, membership):
        import research.factors.builtins.asset_growth as mod

        def fake_lyp(engine, *, dates, source_metric, out_metric, security_ids=None, **kw):
            return panel.reindex(index=pd.DatetimeIndex(pd.to_datetime(dates)))

        monkeypatch.setattr(mod, "load_yoy_ratio_panel", fake_lyp)
        import research.factors.builtins._fundamental_ratio as fr

        monkeypatch.setattr(fr, "load_security_company_map", lambda engine, **kw: membership)

    def test_sign_unflipped_and_reindex(self, monkeypatch):
        # 高增长为正、收缩为负（compute 不翻符号）
        panel = _panel({1: [0.30, 0.30], 2: [-0.15, -0.15]})
        self._patch_panel(monkeypatch, panel, _empty_membership())
        out = get("asset_growth").compute(_ctx([1, 2]))
        assert out.loc[DATES[0], 1] == pytest.approx(0.30)
        assert out.loc[DATES[0], 2] == pytest.approx(-0.15)

    def test_company_broadcast(self, monkeypatch):
        membership = _membership([(10, 100), (11, 100)])
        panel = _panel({10: [0.25, 0.25], 12: [0.40, 0.40]})  # 锚 10 有值，11 缺
        self._patch_panel(monkeypatch, panel, membership)
        out = get("asset_growth").compute(_ctx([10, 11, 12]))
        assert out.loc[DATES[0], 10] == pytest.approx(0.25)
        assert out.loc[DATES[0], 11] == pytest.approx(0.25)  # 广播自公司锚 10
        assert out.loc[DATES[0], 12] == pytest.approx(0.40)  # 无公司，自身值


# --------------------------------------------------------------------------- #
# build_yoy_ratio_events(return_pair=True)：配对值流（H5 F-score ΔROA/EQ_OFFER 用）
# --------------------------------------------------------------------------- #
class TestYoYReturnPair:
    def test_pair_emits_cur_prior_and_period_ends(self):
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
        )
        ev = build_yoy_ratio_events(facts, source_metric="assets", out_metric="assets", return_pair=True)
        assert set(ev.columns) == {"security_id", "metric", "period_end", "prior_period_end",
                                   "visible_date", "cur_value", "prior_value"}
        r = ev.iloc[0]
        assert r["cur_value"] == 1200.0 and r["prior_value"] == 1000.0
        assert r["period_end"] == pd.Timestamp("2023-12-31")
        assert r["prior_period_end"] == pd.Timestamp("2022-12-31")
        assert r["visible_date"] == pd.Timestamp("2024-02-15")

    def test_pair_restatement_reemits(self):
        # 上一期重述 -> 配对流在重述 filed 重发（cur 不变、prior 变）
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", 1000.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 1200.0),
            (1, "Assets", "2022-12-31", "2024-05-01", 800.0),
        )
        ev = build_yoy_ratio_events(facts, source_metric="assets", out_metric="assets",
                                    return_pair=True).sort_values("visible_date").reset_index(drop=True)
        assert len(ev) == 2
        assert ev.loc[0, "prior_value"] == 1000.0
        assert ev.loc[1, "prior_value"] == 800.0 and ev.loc[1, "cur_value"] == 1200.0

    def test_pair_no_prior_positive_guard(self):
        # return_pair 不施 prior>0 病理门（负/零上一期值仍配对，供 ΔROA 用负 ROA）
        facts = _yoy_facts(
            (1, "Assets", "2022-12-31", "2023-02-15", -50.0),
            (1, "Assets", "2023-12-31", "2024-02-15", 100.0),
        )
        ev = build_yoy_ratio_events(facts, source_metric="assets", out_metric="assets", return_pair=True)
        assert len(ev) == 1 and ev.iloc[0]["prior_value"] == -50.0
