"""evaluate 层退市终局收益（realized terminal returns）接线测试（2026-07 任务 E 前置）。

覆盖：
- _quantile_metrics：terminal_return / terminal_return_fallback 透传到 gross+net 两跑；
  标量、Series、fallback 三种口径的注入数值语义。
- run_evaluation：默认读 delisting_events 实测（resolve 语义同 run_baselines），
  口径进 config/params_hash（trials.parquet 新旧口径可区分），fund_closure_par 透传。
- CLI：evaluate 的 --terminal-return/--no-delisting-returns/--no-fund-closure-par，
  run_baselines 的 --terminal-return-fallback/--no-fund-closure-par（纯 parser 件）。
- markdown 报告 Notes 节：多头腿注入口径的 caveat。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np
import pandas as pd
import pytest

import research.evaluate as ev
from research.evaluate import _quantile_metrics, _write_markdown_report, evaluate_factor
from research.factors.protocol import FactorContext

REALIZED = pd.Series({120: -1.0, 999: 0.05})


def test_resolve_terminal_returns_lives_in_research_data():
    """resolve_terminal_returns 已搬到 research.data；run_baselines 只是 re-export。"""
    import research.data as rd
    import research.run_baselines as rb

    assert rb.resolve_terminal_returns is rd.resolve_terminal_returns
    assert ev.resolve_terminal_returns is rd.resolve_terminal_returns


# ---------------------------------------------------------------------------
# _quantile_metrics：注入数值语义（无 DB）
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def delisting_panel():
    """120 只（满足 tradable>=100），col 120 因子值最高（进 q5）且第 30 天起退市。"""
    dates = pd.bdate_range("2025-01-02", periods=60)
    cols = pd.Index(range(1, 121), dtype="int64")
    prices = pd.DataFrame(100.0, index=dates, columns=cols)
    prices.loc[dates[30]:, 120] = np.nan          # 永久缺失 = 退市
    factor = pd.DataFrame(
        np.tile(np.arange(1, 121, dtype="float64"), (len(dates), 1)), index=dates, columns=cols)
    eligible = pd.DataFrame(True, index=dates, columns=cols)
    return {"prices": prices, "factor": factor, "eligible": eligible}


def _qm(panel, **terminal_kwargs):
    return _quantile_metrics(
        panel["factor"], panel["eligible"], panel["prices"],
        horizons=(5,), n_quantiles=5, cost_bps=10.0, **terminal_kwargs)


class TestQuantileMetricsInjection:
    def test_scalar_injection_hits_gross_and_net(self, delisting_panel):
        base = _qm(delisting_panel)
        injected = _qm(delisting_panel, terminal_return=-1.0)

        # q5 含退市证券：-100% 注入拉低收益，gross/net 两条 Sharpe 都必须变
        assert injected.loc[(5, "q5"), "ann_return"] < base.loc[(5, "q5"), "ann_return"]
        assert injected.loc[(5, "q5"), "sharpe_gross"] != base.loc[(5, "q5"), "sharpe_gross"]
        assert injected.loc[(5, "q5"), "sharpe_net"] != base.loc[(5, "q5"), "sharpe_net"]
        # q1 不含退市证券，不受影响
        assert injected.loc[(5, "q1"), "ann_return"] == base.loc[(5, "q1"), "ann_return"]

    def test_series_injection_matches_scalar_for_covered_security(self, delisting_panel):
        scalar = _qm(delisting_panel, terminal_return=-1.0)
        series = _qm(delisting_panel, terminal_return=pd.Series({120: -1.0}))
        pd.testing.assert_frame_equal(series, scalar)

    def test_fallback_fills_series_holes(self, delisting_panel):
        scalar = _qm(delisting_panel, terminal_return=-1.0)
        fallback = _qm(delisting_panel, terminal_return=pd.Series({999: 0.05}),
                       terminal_return_fallback=-1.0)
        pd.testing.assert_frame_equal(fallback, scalar)

    def test_series_without_fallback_leaves_uncovered_at_old_convention(self, delisting_panel):
        base = _qm(delisting_panel)
        uncovered = _qm(delisting_panel, terminal_return=pd.Series({999: 0.05}))
        pd.testing.assert_frame_equal(uncovered, base)


# ---------------------------------------------------------------------------
# run_evaluation 接线（monkeypatch，无 DB）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConstFactor:
    name = "const"

    def compute(self, ctx: FactorContext) -> pd.DataFrame:
        return pd.DataFrame(1.0, index=ctx.dates, columns=ctx.security_universe)


def _wire(monkeypatch, *, realized: pd.Series, expect_par_kwarg: list | None = None):
    dates = pd.bdate_range("2025-01-02", periods=20)
    universe = pd.Index(range(1, 121), dtype="int64")
    panel = {
        "adj_close": pd.DataFrame(100.0, index=dates, columns=universe),
        "close": pd.DataFrame(100.0, index=dates, columns=universe),
        "dollar_volume": pd.DataFrame(10_000_000.0, index=dates, columns=universe),
    }

    def fake_load_delisting(engine, *, fund_closure_par=True, redemption_par=True,
                            exchange_drop_fallback=None):
        if expect_par_kwarg is not None:
            expect_par_kwarg.append(fund_closure_par)
        return realized

    monkeypatch.setattr(ev, "load_adjusted_panel", lambda *a, **k: panel)
    monkeypatch.setattr(ev, "securities_with_uncovered_events", lambda *a, **k: [])
    monkeypatch.setattr(ev, "load_delisting_returns", fake_load_delisting)
    return dates


def _run(dates, **kwargs):
    return ev.run_evaluation(
        ConstFactor(),
        engine=object(),
        start=dates.min().date(),
        end=dates.max().date(),
        horizons=(1,),
        eval_start=dates[2].date(),
        min_median_dollar_volume=1,
        eligibility_window=1,
        trials_path=None,
        risk_free_series=None,
        **kwargs,
    )


class TestRunEvaluationWiring:
    def test_realized_series_threads_to_evaluate_factor_with_cli_fallback(self, monkeypatch):
        dates = _wire(monkeypatch, realized=REALIZED)
        captured = {}
        real_evaluate_factor = ev.evaluate_factor

        def spy(*args, **kwargs):
            captured.update(kwargs)
            return real_evaluate_factor(*args, **kwargs)

        monkeypatch.setattr(ev, "evaluate_factor", spy)
        result = _run(dates, terminal_return=-0.3)

        assert captured["terminal_return"] is REALIZED
        assert captured["terminal_return_fallback"] == -0.3
        assert result.config["terminal_return_mode"] == "realized_series"
        assert result.config["terminal_return_scalar"] is None
        assert result.config["terminal_return_fallback"] == -0.3
        assert result.config["fund_closure_par"] is True

    def test_opt_out_never_touches_loader_and_keeps_scalar_mode(self, monkeypatch):
        dates = _wire(monkeypatch, realized=REALIZED)
        monkeypatch.setattr(
            ev, "load_delisting_returns",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("必须不读 delisting_events")))

        result = _run(dates, terminal_return=-0.3, use_delisting_returns=False)

        assert result.config["terminal_return_mode"] == "scalar"
        assert result.config["terminal_return_scalar"] == -0.3
        assert result.config["terminal_return_fallback"] is None
        assert result.config["fund_closure_par"] is None

    def test_empty_table_degrades_to_old_convention(self, monkeypatch):
        dates = _wire(monkeypatch, realized=pd.Series(dtype="float64"))
        result = _run(dates)
        assert result.config["terminal_return_mode"] == "none"
        assert result.config["terminal_return_scalar"] is None

    def test_fund_closure_par_passthrough(self, monkeypatch):
        seen: list = []
        dates = _wire(monkeypatch, realized=REALIZED, expect_par_kwarg=seen)
        _run(dates, fund_closure_par=False)
        _run(dates)
        assert seen == [False, True]

    def test_params_hash_distinguishes_terminal_modes(self, monkeypatch):
        """CRITICAL：口径进 params_hash，trials.parquet 新旧口径不得互相顶替。"""
        dates = _wire(monkeypatch, realized=REALIZED)
        realized_mode = _run(dates, terminal_return=-0.3)
        scalar_mode = _run(dates, terminal_return=-0.3, use_delisting_returns=False)
        none_mode = _run(dates, use_delisting_returns=False)

        hashes = {realized_mode.params_hash, scalar_mode.params_hash, none_mode.params_hash}
        assert len(hashes) == 3
        # fallback 也参与口径区分
        other_fallback = _run(dates, terminal_return=-1.0)
        assert other_fallback.params_hash != realized_mode.params_hash

    def test_exchange_drop_fallback_threads_to_loader(self, monkeypatch):
        """--exchange-drop-fallback 穿透到 load_delisting_returns（合成发生在读取层）。"""
        seen: list = []
        dates = _wire(monkeypatch, realized=REALIZED)

        def spy_loader(engine, *, fund_closure_par=True, redemption_par=True,
                       exchange_drop_fallback=None):
            seen.append(exchange_drop_fallback)
            return REALIZED

        monkeypatch.setattr(ev, "load_delisting_returns", spy_loader)
        _run(dates)
        _run(dates, exchange_drop_fallback=-0.30)
        assert seen == [None, -0.30]

    def test_exchange_drop_fallback_enters_params_hash(self, monkeypatch):
        """CRITICAL：EXCHANGE_DROP 合成口径进 params_hash——默认 None（旧口径）与
        -0.30 的 trial 必须可区分，不同 fallback 值之间也必须可区分。"""
        dates = _wire(monkeypatch, realized=REALIZED)
        default_mode = _run(dates)
        crsp_mode = _run(dates, exchange_drop_fallback=-0.30)
        deep_mode = _run(dates, exchange_drop_fallback=-0.55)

        assert default_mode.config["exchange_drop_fallback"] is None
        assert crsp_mode.config["exchange_drop_fallback"] == -0.30
        assert len({default_mode.params_hash, crsp_mode.params_hash, deep_mode.params_hash}) == 3

    def test_exchange_drop_fallback_normalized_outside_realized_mode(self, monkeypatch):
        """opt-out（不读 delisting_events）时 fallback 不起作用：config 归一为 None，
        避免无谓的 hash 分裂（同 fund_closure_par 先例）。"""
        dates = _wire(monkeypatch, realized=REALIZED)
        monkeypatch.setattr(
            ev, "load_delisting_returns",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("必须不读 delisting_events")))

        with_fallback = _run(dates, terminal_return=-0.3, use_delisting_returns=False,
                             exchange_drop_fallback=-0.30)
        without = _run(dates, terminal_return=-0.3, use_delisting_returns=False)

        assert with_fallback.config["exchange_drop_fallback"] is None
        assert with_fallback.params_hash == without.params_hash


# ---------------------------------------------------------------------------
# CLI parser（evaluate + run_baselines 纯 parser 件）
# ---------------------------------------------------------------------------

class TestEvaluateParseArgs:
    def test_defaults(self):
        args = ev.parse_args(["--factors", "size"])
        assert args.terminal_return is None
        assert args.no_delisting_returns is False
        assert args.no_fund_closure_par is False
        assert args.exchange_drop_fallback is None

    def test_terminal_return_float_and_none_semantics(self):
        assert ev.parse_args(["--factors", "size", "--terminal-return", "-0.3"]).terminal_return == -0.3
        assert ev.parse_args(["--factors", "size", "--terminal-return", "none"]).terminal_return is None

    def test_flags_parse(self):
        args = ev.parse_args(["--factors", "size", "--no-delisting-returns", "--no-fund-closure-par"])
        assert args.no_delisting_returns is True
        assert args.no_fund_closure_par is True

    def test_exchange_drop_fallback_parses_float(self):
        args = ev.parse_args(["--factors", "size", "--exchange-drop-fallback", "-0.30"])
        assert args.exchange_drop_fallback == -0.30


class TestRunBaselinesFallbackFlag:
    def test_unset_keeps_legacy_semantics(self):
        from research.run_baselines import parse_args

        args = parse_args([])
        assert args.terminal_return_fallback_explicit is False
        assert args.terminal_return_fallback is None

    def test_explicit_float(self):
        from research.run_baselines import parse_args

        args = parse_args(["--terminal-return-fallback", "-0.15"])
        assert args.terminal_return_fallback_explicit is True
        assert args.terminal_return_fallback == -0.15

    def test_explicit_none_means_no_fallback(self):
        from research.run_baselines import parse_args

        args = parse_args(["--terminal-return-fallback", "none"])
        assert args.terminal_return_fallback_explicit is True
        assert args.terminal_return_fallback is None

    def test_no_fund_closure_par_flag(self):
        from research.run_baselines import parse_args

        assert parse_args([]).no_fund_closure_par is False
        assert parse_args(["--no-fund-closure-par"]).no_fund_closure_par is True


# ---------------------------------------------------------------------------
# markdown 报告 Notes 节
# ---------------------------------------------------------------------------

def _eval_result(terminal_return=None):
    dates = pd.bdate_range("2025-01-02", periods=40)
    cols = pd.Index(range(1, 121), dtype="int64")
    rng = np.random.default_rng(11)
    factor = pd.DataFrame(rng.normal(size=(len(dates), len(cols))), index=dates, columns=cols)
    fwd = pd.DataFrame(rng.normal(size=(len(dates), len(cols))), index=dates, columns=cols)
    eligible = pd.DataFrame(True, index=dates, columns=cols)
    return evaluate_factor(factor, {1: fwd}, eligibility=eligible, horizons=(1,),
                           min_coverage=50, terminal_return=terminal_return)


def test_markdown_report_carries_long_leg_caveat_when_injecting(tmp_path):
    result = _eval_result(terminal_return=-1.0)
    path = _write_markdown_report(result, tmp_path)
    text = path.read_text(encoding="utf-8")
    assert "## Notes" in text
    assert "Terminal-return mode: `scalar`" in text
    assert "only covers long legs (held > 0)" in text


def test_markdown_report_notes_mode_none_without_caveat(tmp_path):
    result = _eval_result(terminal_return=None)
    text = _write_markdown_report(result, tmp_path).read_text(encoding="utf-8")
    assert "Terminal-return mode: `none`" in text
    assert "only covers long legs" not in text
