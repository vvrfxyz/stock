from __future__ import annotations

import numpy as np
import pandas as pd

from research.factors.asof import event_table_to_asof_panel


def _events(*rows) -> pd.DataFrame:
    df = pd.DataFrame(
        rows,
        columns=["security_id", "visible_date", "period_end", "value"],
    )
    for col in ("visible_date", "period_end"):
        df[col] = pd.to_datetime(df[col]).astype("datetime64[ns]")
    return df


def test_basic_asof_lookup():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]))
    events = _events(
        (1, "2026-01-01", "2026-01-01", 10.0),
        (1, "2026-01-03", "2026-01-03", 20.0),
    )

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    assert panel[1].tolist() == [10.0, 10.0, 20.0, 20.0]


def test_pit_no_future_leak():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-06-08", "2026-06-09"]))
    events = _events((1, "2026-06-10", "2026-06-10", 10.0))

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    assert panel.isna().all().all()


def test_max_staleness_caps_old_events():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-04-15"]))
    events = _events((1, "2026-01-01", "2026-01-01", 10.0))

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        max_staleness_days=10,
    )

    assert np.isnan(panel.loc["2026-04-15", 1])


def test_no_staleness_keeps_old_events():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01"]))
    events = _events((1, "2020-01-01", "2020-01-01", 7.0))

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        max_staleness_days=None,
    )

    assert panel.loc["2026-01-01", 1] == 7.0


def test_visible_delay_shifts_visibility():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"]))
    events = _events((1, "2026-01-01", "2026-01-01", 5.0))

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        visible_delay_days=2,
    )

    assert np.isnan(panel.loc["2026-01-01", 1])
    assert np.isnan(panel.loc["2026-01-02", 1])
    assert panel.loc["2026-01-03", 1] == 5.0


def test_security_universe_includes_missing_security():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01"]))
    events = _events((1, "2026-01-01", "2026-01-01", 10.0))

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        security_universe=[3, 1, 2],
    )

    assert panel.columns.tolist() == [1, 2, 3]
    assert panel.loc["2026-01-01", 1] == 10.0
    assert panel[2].isna().all()
    assert panel[3].isna().all()


def test_security_universe_default_uses_event_ids():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01"]))
    events = _events(
        (2, "2026-01-01", "2026-01-01", 20.0),
        (1, "2026-01-01", "2026-01-01", 10.0),
    )

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    assert panel.columns.tolist() == [1, 2]


def test_empty_events_returns_all_nan():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01", "2026-01-02"]))
    events = _events()

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        security_universe=[1, 2],
    )

    assert panel.shape == (2, 2)
    assert panel.columns.tolist() == [1, 2]
    assert panel.isna().all().all()


def test_empty_dates_returns_empty():
    dates = pd.DatetimeIndex([], dtype="datetime64[ns]")
    events = _events((1, "2026-01-01", "2026-01-01", 10.0))

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        security_universe=[1],
    )

    assert panel.shape == (0, 1)
    assert panel.columns.tolist() == [1]


def test_dates_order_and_duplicates_preserved():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-03", "2026-01-01", "2026-01-03", "2026-01-02"]))
    events = _events((1, "2026-01-02", "2026-01-02", 10.0))

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    pd.testing.assert_index_equal(panel.index, dates.astype("datetime64[ns]"))
    assert panel[1].tolist()[0] == 10.0
    assert np.isnan(panel[1].tolist()[1])
    assert panel[1].tolist()[2:] == [10.0, 10.0]


def test_multiple_events_same_security_same_effective_visible_date_anchor_max_wins():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-05"]))
    events = _events(
        (1, "2026-01-05", "2026-01-01", 10.0),
        (1, "2026-01-05", "2026-01-03", 20.0),
        (2, "2026-01-05", "2026-01-02", 30.0),
        (2, "2026-01-05", "2026-01-02", 40.0),
    )

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    assert panel.loc["2026-01-05", 1] == 20.0
    assert panel.loc["2026-01-05", 2] == 40.0


def test_nan_values_in_events_filtered():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-03"]))
    events = _events(
        (1, "2026-01-01", "2026-01-01", 10.0),
        (1, "2026-01-02", "2026-01-02", np.nan),
    )

    panel = event_table_to_asof_panel(events, dates=dates, value_column="value")

    assert panel.loc["2026-01-03", 1] == 10.0


def test_explicit_staleness_anchor_column():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01", "2026-01-02"]))
    events = pd.DataFrame(
        {
            "security_id": [1],
            "visible_date": pd.to_datetime(["2026-01-01"]).astype("datetime64[ns]"),
            "total_shares": [100.0],
        }
    )

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="total_shares",
        staleness_anchor_column="visible_date",
        max_staleness_days=0,
    )

    assert panel.loc["2026-01-01", 1] == 100.0
    assert np.isnan(panel.loc["2026-01-02", 1])


def test_anchor_choice_changes_staleness_behavior():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-04-20"]))
    events = _events((1, "2026-04-10", "2026-01-01", 10.0))

    visible_anchor = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        staleness_anchor_column="visible_date",
        max_staleness_days=30,
    )
    period_anchor = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        staleness_anchor_column="period_end",
        max_staleness_days=30,
    )

    assert visible_anchor.loc["2026-04-20", 1] == 10.0
    assert np.isnan(period_anchor.loc["2026-04-20", 1])


def test_nat_in_staleness_anchor_filtered_when_distinct_from_visible_date():
    dates = pd.DatetimeIndex(pd.to_datetime(["2026-01-01", "2026-01-02"]))
    events = _events(
        (1, "2026-01-01", pd.NaT, 10.0),
        (1, "2026-01-02", "2026-01-02", 20.0),
    )

    panel = event_table_to_asof_panel(
        events,
        dates=dates,
        value_column="value",
        staleness_anchor_column="period_end",
        max_staleness_days=30,
    )

    assert np.isnan(panel.loc["2026-01-01", 1])
    assert panel.loc["2026-01-02", 1] == 20.0
