"""Wave 14 frozen event timing, signal, return, and decision rules."""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
import research.earnings_gap_study as earnings_gap_study

from research.earnings_gap_study import (
    PRIMARY_COST_BPS,
    attach_reaction_days,
    compute_event_signals,
    dedupe_earliest_disclosures,
    dedupe_same_reaction_day,
    hypothesis_verdicts,
    _sessions_for_sample,
    load_earnings_gap_filings,
    prepare_complete_return_paths,
    summarize_signal,
)


def _calendar() -> pd.DataFrame:
    dates = pd.to_datetime(["2026-06-12", "2026-06-13", "2026-06-14", "2026-06-15"])
    return pd.DataFrame(
        {
            "trade_date": dates,
            "is_open": [True, False, False, True],
            "open_at": pd.to_datetime(
                ["2026-06-12 13:30:00Z", None, None, "2026-06-15 13:30:00Z"], utc=True
            ),
            "close_at": pd.to_datetime(
                ["2026-06-12 20:00:00Z", None, None, "2026-06-15 20:00:00Z"], utc=True
            ),
        }
    ).set_index("trade_date", drop=False)


def _events(times: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "security_id": range(1, len(times) + 1),
            "accession_number": [f"a{value}" for value in range(len(times))],
            "accepted_at": pd.to_datetime(times, utc=True),
            "period_of_report": pd.Timestamp("2026-03-31"),
        }
    )


def test_xnys_reaction_mapping_handles_pre_after_non_session_and_intraday_boundaries():
    mapped = attach_reaction_days(
        _events(
            [
                "2026-06-12 13:29:59Z",
                "2026-06-12 20:00:01Z",
                "2026-06-14 16:00:00Z",
                "2026-06-12 13:30:00Z",
                "2026-06-12 20:00:00Z",
            ]
        ),
        _calendar(),
    )

    assert mapped["timing"].tolist() == [
        "pre_market",
        "after_market",
        "non_session",
        "intraday",
        "intraday",
    ]
    assert mapped.loc[0, "reaction_date"] == pd.Timestamp("2026-06-12")
    assert mapped.loc[1, "reaction_date"] == pd.Timestamp("2026-06-15")
    assert mapped.loc[2, "reaction_date"] == pd.Timestamp("2026-06-15")
    assert mapped.loc[3, "reaction_date"] is pd.NaT or pd.isna(mapped.loc[3, "reaction_date"])
    assert mapped.loc[4, "reaction_date"] is pd.NaT or pd.isna(mapped.loc[4, "reaction_date"])


def test_earliest_filing_and_same_reaction_day_rules_are_deterministic():
    events = pd.DataFrame(
        {
            "security_id": [1, 1, 1, 1],
            "period_of_report": pd.to_datetime(["2026-03-31", "2026-03-31", "2025-12-31", "2025-12-31"]),
            "accepted_at": pd.to_datetime(
                [
                    "2026-05-01 20:00:00Z",
                    "2026-05-01 19:00:00Z",
                    "2026-05-01 19:00:00Z",
                    "2026-05-01 19:00:00Z",
                ],
                utc=True,
            ),
            "accession_number": ["b", "a", "z", "y"],
            "reaction_date": pd.to_datetime(["2026-05-04"] * 4),
        }
    )

    per_period = dedupe_earliest_disclosures(events)
    same_day = dedupe_same_reaction_day(per_period)

    assert per_period["accession_number"].tolist() == ["y", "a"]
    assert same_day["accession_number"].tolist() == ["a"]


def _raw_panels() -> tuple[pd.DataFrame, dict[str, pd.DataFrame], pd.Timestamp]:
    dates = pd.bdate_range("2026-01-02", periods=70)
    reaction = dates[65]
    close = np.full(len(dates), 100.0)
    opening = np.full(len(dates), 100.0)
    high = np.full(len(dates), 102.0)
    low = np.full(len(dates), 98.0)
    volume = np.full(len(dates), 30_000.0)
    opening[65] = 110.0
    high[65] = 112.0
    low[65] = 109.0
    volume[65] = 75_000.0
    panels = {
        "open": pd.DataFrame({1: opening}, index=dates),
        "high": pd.DataFrame({1: high}, index=dates),
        "low": pd.DataFrame({1: low}, index=dates),
        "close": pd.DataFrame({1: close}, index=dates),
        "volume": pd.DataFrame({1: volume}, index=dates),
    }
    events = pd.DataFrame(
        {
            "security_id": [1],
            "reaction_date": [reaction],
            "accepted_at": pd.to_datetime(["2026-04-03 12:00:00Z"]),
            "accession_number": ["a"],
        }
    )
    return events, panels, reaction


def test_raw_atr_volume_signal_uses_only_prior_bars():
    events, panels, _reaction = _raw_panels()
    initial = compute_event_signals(events, panels)
    changed = {name: panel.copy() for name, panel in panels.items()}
    changed["close"].iloc[66:, 0] = 1.0
    changed["high"].iloc[66:, 0] = 1.0
    changed["low"].iloc[66:, 0] = 1.0
    changed["volume"].iloc[66:, 0] = 1.0
    after_future_change = compute_event_signals(events, changed)

    row = initial.iloc[0]
    assert row["atr20"] == pytest.approx(4.0)
    assert row["gap_atr"] == pytest.approx(2.5)
    assert row["volume_ratio"] == pytest.approx(2.5)
    assert row["gap_atr_volume_confirmed"] == pytest.approx(6.25)
    np.testing.assert_allclose(
        initial[["gap_atr", "gap_atr_volume_confirmed"]].to_numpy(),
        after_future_change[["gap_atr", "gap_atr_volume_confirmed"]].to_numpy(),
    )


def test_complete_paths_enter_after_reaction_day_close():
    dates = pd.bdate_range("2026-01-02", periods=24)
    adjusted = pd.DataFrame({1: 100.0, 3379: 100.0}, index=dates)
    adjusted.loc[dates[9], 1] = 200.0
    adjusted.loc[dates[10], 1] = 220.0
    adjusted.loc[dates[11], 1] = 220.0
    events = pd.DataFrame(
        {
            "security_id": [1],
            "accession_number": ["a"],
            "reaction_date": [dates[9]],
            "gap_atr": [1.0],
            "gap_atr_volume_confirmed": [1.0],
        }
    )

    complete, asset_paths, spy_paths = prepare_complete_return_paths(events, adjusted, horizon=2)
    summary, daily, cohorts = summarize_signal(
        complete,
        asset_paths,
        spy_paths,
        dates,
        signal_name="gap_atr",
        sample_start=dates[9].date(),
        calendar_end=dates[-1],
        horizons=(1, 2),
        costs_bps=(0.0,),
    )

    assert asset_paths[0].tolist() == pytest.approx([0.10, 0.0])
    assert summary.loc[summary["horizon"] == 1, "gross_event_car"].iloc[0] == pytest.approx(0.10)
    reaction_day = daily[(daily["horizon"] == 1) & (daily["date"] == dates[9])].iloc[0]
    assert reaction_day["gross_return"] == pytest.approx(0.0)
    assert summary["calendar_days"].nunique() == 1
    assert cohorts.loc[cohorts["horizon"] == 1, "gross_car"].iloc[0] == pytest.approx(0.10)
    h2_returns = daily[daily["horizon"] == 2].set_index("date")["gross_return"]
    assert h2_returns.loc[dates[10]] == pytest.approx(0.05)
    assert h2_returns.loc[dates[11]] == pytest.approx(0.0)


def test_incomplete_tail_path_is_not_a_valid_event():
    dates = pd.bdate_range("2026-01-02", periods=21)
    adjusted = pd.DataFrame({1: np.linspace(100.0, 120.0, len(dates)), 3379: 100.0}, index=dates)
    events = pd.DataFrame({"security_id": [1], "reaction_date": [dates[-2]]})

    complete, asset_paths, spy_paths = prepare_complete_return_paths(events, adjusted, horizon=20)

    assert complete.empty
    assert asset_paths.shape == (0, 20)
    assert spy_paths.shape == (0, 20)


def test_stability_sample_keeps_observed_post_boundary_outcomes():
    dates = pd.bdate_range("2015-12-28", periods=30)
    calendar = pd.DataFrame(
        {
            "trade_date": dates,
            "is_open": True,
            "open_at": pd.to_datetime(dates).tz_localize("UTC"),
            "close_at": pd.to_datetime(dates).tz_localize("UTC"),
        }
    ).set_index("trade_date", drop=False)

    sessions = _sessions_for_sample(
        calendar,
        sample_start=date(2015, 12, 28),
        sample_end=date(2015, 12, 31),
        observed_end=dates[-1].date(),
    )

    assert sessions[-1] == pd.Timestamp("2016-01-28")


def _summary_row(signal: str, horizon: int, *, mean: float, t: float, car: float) -> dict[str, float | int | str]:
    return {
        "signal": signal,
        "horizon": horizon,
        "cost_bps": PRIMARY_COST_BPS,
        "net_calendar_mean": mean,
        "net_calendar_nw_t": t,
        "net_event_car": car,
    }


def _passing_summaries() -> tuple[pd.DataFrame, pd.DataFrame]:
    primary_rows = []
    stability_rows = []
    for signal, means, car in (
        ("gap_atr", {1: 0.001, 5: 0.002, 20: 0.003}, 0.04),
        ("gap_atr_volume_confirmed", {1: 0.002, 5: 0.003, 20: 0.004}, 0.06),
    ):
        for horizon, mean in means.items():
            primary_rows.append(
                _summary_row(signal, horizon, mean=mean, t=3.1 if horizon == 20 else 1.0, car=car)
            )
        stability_rows.append(_summary_row(signal, 20, mean=0.001, t=0.5, car=car))
    return pd.DataFrame(primary_rows), pd.DataFrame(stability_rows)


def test_h1_h2_verdict_requires_every_preregistered_gate():
    primary, stability = _passing_summaries()

    assert hypothesis_verdicts(primary, stability) == {
        "gap_atr": True,
        "gap_atr_volume_confirmed": True,
    }
    primary.loc[
        (primary["signal"] == "gap_atr_volume_confirmed") & (primary["horizon"] == 5),
        "net_calendar_mean",
    ] = 0.001
    assert hypothesis_verdicts(primary, stability)["gap_atr_volume_confirmed"] is False


def test_filing_loader_requires_item_token_and_accepted_time(monkeypatch):
    captured = {}

    def fake_read_sql_query(sql, engine, *, params, parse_dates):
        captured["sql"] = sql.text
        captured["engine"] = engine
        captured["params"] = params
        captured["parse_dates"] = parse_dates
        return pd.DataFrame(
            {
                "security_id": [1],
                "accession_number": ["inside"],
                "form_type": ["8-K"],
                "filing_date": [pd.Timestamp("2026-05-01")],
                "accepted_at": [pd.Timestamp("2026-05-02 01:00:00Z")],
                "period_of_report": [pd.Timestamp("2026-03-31")],
                "items": ["1.01, 2.02, 9.01"],
            }
        )

    monkeypatch.setattr(earnings_gap_study.pd, "read_sql_query", fake_read_sql_query)
    loaded = load_earnings_gap_filings(object(), accepted_end=date(2026, 5, 3))

    assert loaded["accession_number"].tolist() == ["inside"]
    assert captured["params"] == {"forms": ["8-K", "8-K/A"], "accepted_end": date(2026, 5, 3)}
    assert captured["parse_dates"] == ["filing_date", "accepted_at", "period_of_report"]
    assert "accepted_at is not null" in captured["sql"]
    assert "accepted_at < :accepted_end" in captured["sql"]
    assert "items ~ '(^|,)[[:space:]]*2\\.02[[:space:]]*(,|$)'" in captured["sql"]
