"""技术因子族（intraday_moments / intraday_flow / bar_geometry / classic_price）单元测试。

分钟族 monkeypatch minute_loader；日线族 monkeypatch data 加载器——不连库，
锁定形状契约、方向号、无效日处理与注册。
"""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from research.factors.protocol import FactorContext, get


def _ctx(dates, ids):
    return FactorContext(engine=None, dates=dates, security_universe=pd.Index(ids, dtype="int64"))


DATES = pd.bdate_range("2025-01-01", periods=40)
IDS = [1, 2]


class TestIntradayMoments:
    def test_realized_skew_sign_and_invalid_zero(self, monkeypatch):
        # 证券 1 持续正偏度 -> 信号为负；证券 2 特征全为 0（无效日）-> NaN
        rskew = pd.DataFrame(
            {1: [2.0] * len(DATES), 2: [0.0] * len(DATES)}, index=DATES)

        def fake_loader(dates, ids, columns, **kwargs):
            return {"rskew": rskew}

        import research.factors.builtins.intraday_moments as mod
        monkeypatch.setattr(mod, "load_minute_feature_panel", fake_loader)
        out = get("realized_skew").compute(_ctx(DATES, IDS))
        assert out.shape == (len(DATES), 2)
        assert out[1].iloc[-1] == pytest.approx(-2.0)
        assert np.isnan(out[2].iloc[-1])  # 0 视为无效不进均值

    def test_signed_jump_ratio(self, monkeypatch):
        rv = pd.DataFrame({1: [4.0] * len(DATES)}, index=DATES)
        rv_up = pd.DataFrame({1: [3.0] * len(DATES)}, index=DATES)
        rv_down = pd.DataFrame({1: [1.0] * len(DATES)}, index=DATES)

        def fake_loader(dates, ids, columns, **kwargs):
            return {"rv": rv, "rv_up": rv_up, "rv_down": rv_down}

        import research.factors.builtins.intraday_moments as mod
        monkeypatch.setattr(mod, "load_minute_feature_panel", fake_loader)
        out = get("signed_jump").compute(_ctx(DATES, [1]))
        # RSJ = (3-1)/4 = 0.5，负向 -> -0.5
        assert out[1].iloc[-1] == pytest.approx(-0.5)


class TestIntradayFlow:
    def test_last30_persistence_mean(self, monkeypatch):
        ret_last30 = pd.DataFrame({1: [0.01] * len(DATES)}, index=DATES)

        def fake_loader(dates, ids, columns, **kwargs):
            return {"ret_last30": ret_last30}

        import research.factors.builtins.intraday_flow as mod
        monkeypatch.setattr(mod, "load_minute_feature_panel", fake_loader)
        out = get("last30_persistence").compute(_ctx(DATES, [1]))
        assert out[1].iloc[-1] == pytest.approx(0.01)

    def test_smart_money_gap_direction(self, monkeypatch):
        panels = {
            "ret_last30": pd.DataFrame({1: [0.02] * len(DATES)}, index=DATES),
            "ret_first30": pd.DataFrame({1: [0.005] * len(DATES)}, index=DATES),
        }

        def fake_loader(dates, ids, columns, **kwargs):
            return panels

        import research.factors.builtins.intraday_flow as mod
        monkeypatch.setattr(mod, "load_minute_feature_panel", fake_loader)
        out = get("smart_money_gap").compute(_ctx(DATES, [1]))
        assert out[1].iloc[-1] == pytest.approx(0.015)


class TestBarGeometry:
    def _frame(self, rows):
        frame = pd.DataFrame(rows)
        frame["date"] = pd.to_datetime(frame["date"])
        return frame

    def test_shadow_asymmetry_upper_heavy_negative(self, monkeypatch):
        # 长上影：high 远超 body，low 贴着 body -> sasym>0 -> 因子为负
        rows = [{"security_id": 1, "date": d, "open": 10.0, "high": 12.0,
                 "low": 9.9, "close": 10.1} for d in DATES]

        import research.factors.builtins.bar_geometry as mod
        monkeypatch.setattr(mod, "_load_bar_panels", lambda ctx, buffer_days, columns: self._frame(rows))
        out = get("shadow_asymmetry").compute(_ctx(DATES, [1]))
        assert out[1].iloc[-1] < 0

    def test_close_vwap_pressure_above_vwap_negative(self, monkeypatch):
        rows = [{"security_id": 1, "date": d, "close": 10.2, "vwap": 10.0} for d in DATES]

        import research.factors.builtins.bar_geometry as mod
        monkeypatch.setattr(mod, "_load_bar_panels", lambda ctx, buffer_days, columns: self._frame(rows))
        out = get("close_vwap_pressure").compute(_ctx(DATES, [1]))
        assert out[1].iloc[-1] == pytest.approx(-np.log(10.2 / 10.0))

    def test_zero_range_bar_is_nan(self, monkeypatch):
        rows = [{"security_id": 1, "date": d, "open": 10.0, "high": 10.0,
                 "low": 10.0, "close": 10.0} for d in DATES]

        import research.factors.builtins.bar_geometry as mod
        monkeypatch.setattr(mod, "_load_bar_panels", lambda ctx, buffer_days, columns: self._frame(rows))
        out = get("shadow_asymmetry").compute(_ctx(DATES, [1]))
        assert out[1].isna().all()


class TestClassicPrice:
    def test_max_lottery_penalizes_spike(self, monkeypatch):
        # 证券 1 有一根 +50% 的暴涨日；证券 2 平稳——彩票分更负
        idx = DATES
        p1 = pd.Series(100.0, index=idx)
        p1.iloc[25:] = 150.0  # 一日 +50%
        p2 = pd.Series(np.linspace(100, 104, len(idx)), index=idx)
        panel = pd.DataFrame({1: p1, 2: p2})

        import research.factors.builtins.classic_price as mod
        monkeypatch.setattr(mod, "_adj_close_panel", lambda ctx, buffer_days: panel)
        out = get("max_lottery").compute(_ctx(DATES, IDS))
        assert out[1].iloc[-1] < out[2].iloc[-1]

    def test_short_term_reversal_favors_losers(self, monkeypatch):
        idx = DATES
        loser = pd.Series(np.linspace(100, 70, len(idx)), index=idx)
        winner = pd.Series(np.linspace(100, 130, len(idx)), index=idx)
        panel = pd.DataFrame({1: loser, 2: winner})

        import research.factors.builtins.classic_price as mod
        monkeypatch.setattr(mod, "_adj_close_panel", lambda ctx, buffer_days: panel)
        out = get("short_term_reversal").compute(_ctx(DATES, IDS))
        assert out[1].iloc[-1] > out[2].iloc[-1]


def test_all_new_factors_registered():
    for name in ("realized_skew", "signed_jump", "last30_persistence",
                 "smart_money_gap", "shadow_asymmetry", "close_vwap_pressure",
                 "max_lottery", "short_term_reversal"):
        factor = get(name)
        assert factor.pit_guarantee and factor.lag_days >= 1
