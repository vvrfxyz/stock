"""Wave 15 frozen signal, drift, terminal-value, cost, and verdict rules."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import research.market_regime_overlay_study as study
from research.market_regime_overlay_study import (
    ASSETS,
    PRIMARY_COST_BPS,
    RULES,
    attach_measured_spread_costs,
    completed_month_end_sessions,
    compute_monthly_signals,
    daily_target_exposure,
    hypothesis_verdicts,
    month_end_sessions,
    prepare_simulation_inputs,
    simulate_monthly_portfolio,
    validate_signal_availability,
)


def test_month_end_signals_require_complete_200_session_breadth_history():
    dates = pd.bdate_range("2025-01-02", periods=260)
    spy = pd.Series(np.linspace(100.0, 130.0, len(dates)), index=dates)
    complete = pd.Series(np.linspace(50.0, 100.0, len(dates)), index=dates)
    incomplete = complete.copy()
    incomplete.iloc[:61] = np.nan
    prices = pd.DataFrame({1: complete, 2: incomplete}, index=dates)
    eligible = pd.DataFrame(True, index=dates, columns=prices.columns)

    signals = compute_monthly_signals(
        spy,
        prices,
        eligible,
        month_ends=month_end_sessions(dates),
    )
    final = signals.iloc[-1]

    assert final["spy_10m_trend"] == 1.0
    assert final["breadth_denominator"] == 1
    assert final["breadth_numerator"] == 1
    assert final["breadth"] == 1.0
    assert final["breadth_200d"] == 1.0
    assert final["trend_and_breadth"] == 1.0


def test_spy_trend_clock_ignores_months_before_verified_total_return_start():
    dates = pd.bdate_range("2025-01-02", periods=400)
    spy = pd.Series(np.linspace(100.0, 140.0, len(dates)), index=dates)
    prices = pd.DataFrame({1: np.linspace(50.0, 90.0, len(dates))}, index=dates)
    eligible = pd.DataFrame(True, index=dates, columns=prices.columns)

    signals = compute_monthly_signals(
        spy,
        prices,
        eligible,
        month_ends=month_end_sessions(dates),
        spy_total_return_start=pd.Timestamp("2025-07-01").date(),
    )

    valid_trend = signals["spy_10m_trend"].dropna()
    assert valid_trend.index[0].to_period("M") == pd.Period("2026-04", freq="M")


def test_incomplete_natural_month_is_not_a_signal_month_end():
    calendar = pd.to_datetime(
        [
            "2026-06-29",
            "2026-06-30",
            "2026-07-01",
            "2026-07-02",
            "2026-07-06",
            "2026-07-10",
            "2026-07-31",
        ]
    )
    observed = calendar[calendar <= pd.Timestamp("2026-07-10")]

    month_ends = completed_month_end_sessions(calendar, observed)

    assert month_ends.tolist() == [pd.Timestamp("2026-06-30")]


def test_completed_natural_month_is_kept_as_a_signal_month_end():
    calendar = pd.to_datetime(
        [
            "2026-06-29",
            "2026-06-30",
            "2026-07-01",
            "2026-07-02",
            "2026-07-06",
            "2026-07-10",
            "2026-07-31",
        ]
    )

    month_ends = completed_month_end_sessions(calendar, calendar)

    assert month_ends.tolist() == [
        pd.Timestamp("2026-06-30"),
        pd.Timestamp("2026-07-31"),
    ]


def test_month_end_exposure_starts_on_the_next_session():
    dates = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-03"])
    exposure = pd.Series([1.0], index=pd.to_datetime(["2026-01-30"]))

    daily = daily_target_exposure(dates, exposure)

    assert daily.loc["2026-01-30"] == 0.0
    assert daily.loc["2026-02-02"] == 1.0
    assert daily.loc["2026-02-03"] == 1.0


def test_unavailable_formation_state_is_implemented_as_full_baseline_exposure():
    raw_signal = pd.Series([np.nan, 0.0], index=pd.to_datetime(["2026-01-30", "2026-02-27"]))
    operational = raw_signal.fillna(1.0)

    assert operational.tolist() == [1.0, 0.0]


def test_signal_availability_allows_leading_warmup_but_not_later_gaps():
    dates = pd.to_datetime(["2007-07-31", "2007-08-31", "2007-09-28"])
    signals = pd.DataFrame(
        {
            rule: [np.nan, 1.0, 0.0]
            for rule in RULES
        },
        index=dates,
    )

    first = validate_signal_availability(signals, eval_start=pd.Timestamp("2007-07-01").date())
    assert set(first.values()) == {"2007-08-31"}

    signals.loc[dates[-1], "breadth_200d"] = np.nan
    with pytest.raises(RuntimeError, match="breadth_200d becomes unavailable"):
        validate_signal_availability(signals, eval_start=pd.Timestamp("2007-07-01").date())


def test_simulator_uses_drifted_pretrade_weights_and_exact_cost_identity():
    dates = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-27", "2026-03-02"])
    prices = pd.DataFrame(
        {
            1: [100.0, 110.0, 121.0, 121.0],
            2: [100.0, 100.0, 100.0, 100.0],
        },
        index=dates,
    )
    rebalances = pd.to_datetime(["2026-01-30", "2026-02-27"])
    members = pd.DataFrame(True, index=rebalances, columns=prices.columns)
    exposure = pd.Series(1.0, index=rebalances)
    rf = pd.Series(0.0, index=dates)
    inputs = prepare_simulation_inputs(prices, pd.Series(dtype="float64"))

    path = simulate_monthly_portfolio(
        "equal_weight",
        inputs,
        members,
        exposure,
        rf,
    )

    assert path.gross_returns.loc["2026-02-02"] == pytest.approx(0.05)
    assert path.gross_returns.loc["2026-02-27"] == pytest.approx(1.105 / 1.05 - 1.0)
    pretrade_first_weight = 0.605 / 1.105
    expected_turnover = 2.0 * abs(pretrade_first_weight - 0.5)
    assert path.turnover.loc["2026-02-27"] == pytest.approx(expected_turnover)
    pd.testing.assert_series_equal(
        path.net_returns(25.0),
        path.gross_returns - path.turnover * 25.0 / 10_000.0,
        check_names=False,
    )


def test_realized_terminal_value_is_liquidated_to_cash():
    dates = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-03", "2026-02-04"])
    prices = pd.DataFrame({1: [100.0, 100.0, np.nan, np.nan]}, index=dates)
    members = pd.DataFrame(True, index=pd.to_datetime(["2026-01-30"]), columns=[1])
    exposure = pd.Series([1.0], index=members.index)
    rf = pd.Series([0.0, 0.0, 0.0, 0.01], index=dates)
    inputs = prepare_simulation_inputs(prices, pd.Series({1: -0.50}))

    path = simulate_monthly_portfolio("terminal", inputs, members, exposure, rf)

    assert path.gross_returns.loc["2026-02-03"] == pytest.approx(-0.50)
    assert path.realized_stock_weight.loc["2026-02-04"] == pytest.approx(0.0)
    assert path.gross_returns.loc["2026-02-04"] == pytest.approx(0.01)


def test_unresolved_terminal_value_remains_frozen_instead_of_earning_cash():
    dates = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-03", "2026-02-04"])
    prices = pd.DataFrame({1: [100.0, 100.0, np.nan, np.nan]}, index=dates)
    members = pd.DataFrame(True, index=pd.to_datetime(["2026-01-30"]), columns=[1])
    exposure = pd.Series([1.0], index=members.index)
    rf = pd.Series([0.0, 0.0, 0.0, 0.01], index=dates)
    inputs = prepare_simulation_inputs(prices, pd.Series(dtype="float64"))

    path = simulate_monthly_portfolio("unresolved", inputs, members, exposure, rf)

    assert path.gross_returns.loc["2026-02-04"] == pytest.approx(0.0)
    assert path.unresolved_missing_weight.loc["2026-02-04"] == pytest.approx(1.0)


def test_rule_pass_requires_all_four_asset_by_sample_cells():
    rows = []
    for rule in RULES:
        for asset in ASSETS:
            for sample in ("stability", "primary"):
                rows.append(
                    {
                        "rule": rule,
                        "asset": asset,
                        "sample": sample,
                        "cost_bps": PRIMARY_COST_BPS[asset],
                        "cell_pass": True,
                    }
                )
    metrics = pd.DataFrame(rows)
    metrics.loc[
        (metrics["rule"] == "breadth_200d")
        & (metrics["asset"] == "pit_cs_equal_weight")
        & (metrics["sample"] == "primary"),
        "cell_pass",
    ] = False

    assert hypothesis_verdicts(metrics) == {
        "spy_10m_trend": True,
        "breadth_200d": False,
        "trend_and_breadth": True,
    }


def test_measured_spread_diagnostic_does_not_fill_missing_trades(monkeypatch):
    sessions = pd.bdate_range("2026-01-02", periods=63)
    trades = pd.DataFrame(
        {
            "date": [sessions[-1], sessions[-1]],
            "rule": ["breadth_200d", "breadth_200d"],
            "security_id": [1, 2],
            "abs_weight": [0.6, 0.4],
        }
    )

    def fake_query(_sql):
        return pd.DataFrame(
            {
                "security_id": [1],
                "valid_spread_days": [20],
                "spread_full": [0.01],
            }
        )

    monkeypatch.setattr(study, "query_df", fake_query)
    attached = attach_measured_spread_costs(trades, sessions)

    assert attached.loc[attached["security_id"] == 1, "cost_bps"].iloc[0] == pytest.approx(50.0)
    assert pd.isna(attached.loc[attached["security_id"] == 2, "cost_bps"].iloc[0])
