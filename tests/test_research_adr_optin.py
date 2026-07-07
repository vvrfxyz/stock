"""研究层 ADR opt-in（方案 §E.6）语义锁定。

三道防线，缺一即测试失败：
1. 默认口径永远 CS-only（DEFAULT_RESEARCH_TYPES 内容锁定，零污染铁律 §A.2）；
2. --include-adr 显式并入 ADR 家族，且股本口径敏感因子（adr_unsafe）的
   ADR 列在 evaluate 层被置 NaN（§E.3 禁入直至 ADS 归一化）；
3. universe ids 显式给定时 price_cache 不再叠加类型门（双重过滤修复）。
"""
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from research.data import ADR_TYPES, DEFAULT_RESEARCH_TYPES, RESEARCH_TYPES_WITH_ADR


class TestTypeConstants:
    def test_default_is_cs_only(self):
        assert DEFAULT_RESEARCH_TYPES == ("CS",)

    def test_adr_family_exact(self):
        assert ADR_TYPES == ("ADRC", "ADRP", "ADRR")
        assert RESEARCH_TYPES_WITH_ADR == ("CS", "ADRC", "ADRP", "ADRR")
        assert "ETF" not in RESEARCH_TYPES_WITH_ADR


class TestAdrUnsafeMarkers:
    """§E.3：恰好这三个因子除以股本/市值，多标漏标都是错。"""

    SHARE_SENSITIVE = {"size", "earnings_yield", "short_interest_ratio"}

    def test_exactly_the_share_sensitive_factors_are_marked(self):
        import research.evaluate  # noqa: F401  # 触发全部 builtins 注册
        from research.factors.protocol import get, list_factors

        marked = {name for name in list_factors()
                  if getattr(get(name), "adr_unsafe", False)}
        assert marked == self.SHARE_SENSITIVE


class TestFundamentalsIfrsConcepts:
    def test_ifrs_concepts_appended_after_us_gaap(self):
        from research.fundamentals import METRICS

        ni = METRICS["net_income_ttm"].concepts
        assert ni[0] == "NetIncomeLoss"  # us-gaap 优先级不变
        assert "ProfitLossAttributableToOwnersOfParent" in ni
        assert ni.index("ProfitLossAttributableToOwnersOfParent") < ni.index("ProfitLoss")

        assert "Revenue" in METRICS["revenue_ttm"].concepts
        assert "ProfitLossFromOperatingActivities" in METRICS["operating_income_ttm"].concepts
        assert "CashFlowsFromUsedInOperatingActivities" in METRICS["operating_cash_flow_ttm"].concepts
        eq = METRICS["equity"].concepts
        assert eq[0] == "StockholdersEquity"
        assert "EquityAttributableToOwnersOfParent" in eq

    def test_amount_metrics_still_usd_only(self):
        # 混币种防线：TWD/EUR 申报的 FPI 在 FX 归一化前宁缺毋混
        from research.fundamentals import METRICS

        for spec in METRICS.values():
            assert spec.unit in ("USD", "shares")


class _FakeCursor:
    def __init__(self, sink):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def copy_expert(self, sql, buffer):
        self._sink.append(sql)
        buffer.write(b"security_id,date,close\n")


class _FakeRaw:
    def __init__(self, sink):
        self._sink = sink

    def cursor(self):
        return _FakeCursor(self._sink)

    def close(self):
        pass


class _FakeEngine:
    def __init__(self):
        self.sqls: list[str] = []
        self.url = "fake://"

    def raw_connection(self):
        return _FakeRaw(self.sqls)


class TestPriceCacheDoubleFilterFix:
    def test_explicit_ids_drop_type_gate(self):
        from datetime import date
        from research.factors.price_cache import load_price_long_fast

        engine = _FakeEngine()
        load_price_long_fast(engine, start=date(2025, 1, 1), end=date(2025, 2, 1),
                             columns="close", security_ids=[1, 2, 3])
        assert "s.type" not in engine.sqls[0]
        assert "p.security_id in (1,2,3)" in engine.sqls[0]

    def test_no_ids_keeps_legacy_cs_etf_gate(self):
        from datetime import date
        from research.factors.price_cache import load_price_long_fast

        engine = _FakeEngine()
        load_price_long_fast(engine, start=date(2025, 1, 1), end=date(2025, 2, 1),
                             columns="close")
        assert "'CS','ETF'" in engine.sqls[0]

    def test_explicit_types_still_honored(self):
        from datetime import date
        from research.factors.price_cache import load_price_long_fast

        engine = _FakeEngine()
        load_price_long_fast(engine, start=date(2025, 1, 1), end=date(2025, 2, 1),
                             columns="close", security_ids=[1], types=("CS",))
        assert "'CS'" in engine.sqls[0]


@dataclass(frozen=True)
class _UnsafeConstFactor:
    name = "unsafe_const"
    lookback_days = 0
    lag_days = 1
    pit_guarantee = True
    adr_unsafe = True

    def compute(self, ctx):
        return pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)


def _run(monkeypatch, *, types, adr_ids, factor):
    import research.evaluate as ev

    dates = pd.bdate_range("2025-01-02", periods=30)
    universe = [101, 102, 201]  # 201 是 ADR
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }
    calls = {"type_ids": 0}

    def fake_type_ids(engine, t):
        calls["type_ids"] += 1
        return adr_ids

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *a, **k: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *a, **k: [])
    monkeypatch.setattr(ev, "load_delisting_returns", lambda *a, **k: pd.Series(dtype="float64"))
    monkeypatch.setattr(ev, "_load_type_ids", fake_type_ids)
    monkeypatch.setattr(ev, "_git_meta", lambda: (None, False), raising=False)

    result = ev.run_evaluation(
        factor,
        engine=object(),
        start=dates.min().date(),
        end=dates.max().date(),
        horizons=(1,),
        eval_start=dates[2].date(),
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
        risk_free_series=None,
        types=types,
    )
    return result, calls


class TestAdrUnsafeGate:
    def test_adr_columns_naned_for_unsafe_factor_when_adr_included(self, monkeypatch):
        result, calls = _run(
            monkeypatch,
            types=RESEARCH_TYPES_WITH_ADR,
            adr_ids={201},
            factor=_UnsafeConstFactor(),
        )
        assert calls["type_ids"] == 1
        assert result.diagnostics["adr_gated_columns"] == 1

    def test_cs_only_universe_never_queries_type_ids(self, monkeypatch):
        result, calls = _run(
            monkeypatch,
            types=DEFAULT_RESEARCH_TYPES,
            adr_ids={201},
            factor=_UnsafeConstFactor(),
        )
        assert calls["type_ids"] == 0
        assert result.diagnostics["adr_gated_columns"] == 0
