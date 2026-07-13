"""Wave 14 preregistered earnings-gap continuation study.

The study deliberately contains only `gap_atr` and
`gap_atr_volume_confirmed`. It reads SEC filings, the XNYS calendar, raw daily
bars, adjustment factors, and corporate-action facts; it never writes fact
tables. The only write is the research output and its study ledger entry.

Run:
    python -m research.earnings_gap_study
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from research._trials_store import append_study
from research.data import research_engine, securities_with_uncovered_events
from research.evaluate import _markdown_table, _newey_west_t, default_nw_lag
from research.factors.price_cache import adjusted_close_panel, clear_cache, raw_bar_panels

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
SPY_SECURITY_ID = 3379
PRIMARY_START = date(2016, 1, 4)
STABILITY_START = date(2007, 7, 2)
STABILITY_END = date(2015, 12, 31)
HORIZONS = (1, 5, 20)
MAX_HORIZON = max(HORIZONS)
COST_BPS = (10.0, 25.0, 40.0)
PRIMARY_COST_BPS = 25.0
MIN_PRICE = 3.0
MIN_MEDIAN_DOLLAR_VOLUME = 2_000_000.0
NY_TZ = "America/New_York"


def load_xnys_calendar(engine, *, start: date, end: date) -> pd.DataFrame:
    """Load every required XNYS calendar row; absent rows are data errors."""
    frame = pd.read_sql_query(
        text(
            """
            select trade_date, is_open, open_at, close_at
            from trading_calendars
            where exchange_mic = 'XNYS'
              and trade_date between :start and :end
            order by trade_date
            """
        ),
        engine,
        params={"start": start, "end": end},
        parse_dates=["trade_date", "open_at", "close_at"],
    )
    if frame.empty:
        raise RuntimeError(f"XNYS calendar is empty for {start} through {end}")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    if frame["trade_date"].duplicated().any():
        raise RuntimeError("XNYS calendar has duplicate trade_date rows")
    frame["open_at"] = pd.to_datetime(frame["open_at"], utc=True)
    frame["close_at"] = pd.to_datetime(frame["close_at"], utc=True)
    open_rows = frame["is_open"].fillna(False)
    if frame.loc[open_rows, ["open_at", "close_at"]].isna().any().any():
        raise RuntimeError("open XNYS session lacks open_at or close_at")
    return frame.set_index("trade_date", drop=False)


def _next_open_session(calendar: pd.DataFrame, day: pd.Timestamp) -> pd.Timestamp:
    sessions = pd.DatetimeIndex(calendar.index[calendar["is_open"].fillna(False)])
    position = int(sessions.searchsorted(day, side="right"))
    if position >= len(sessions):
        raise RuntimeError(f"XNYS calendar has no next open session after {day.date()}")
    return sessions[position]


def attach_reaction_days(events: pd.DataFrame, calendar: pd.DataFrame) -> pd.DataFrame:
    """Map accepted timestamps under the frozen XNYS timing rule.

    Intraday timestamps, including exact open and close boundaries, remain in
    the returned audit frame with a null reaction day instead of being silently
    discarded.
    """
    required = {"accepted_at"}
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"events missing columns: {sorted(missing)}")
    out = events.copy()
    accepted = pd.to_datetime(out["accepted_at"], utc=True, errors="coerce")
    if accepted.isna().any():
        raise ValueError("reaction mapping requires non-null accepted_at")
    local_days = accepted.dt.tz_convert(NY_TZ).dt.tz_localize(None).dt.normalize()
    missing_days = local_days[~local_days.isin(calendar.index)]
    if not missing_days.empty:
        examples = ", ".join(str(day.date()) for day in missing_days.iloc[:5])
        raise RuntimeError(f"XNYS calendar missing event-date rows: {examples}")

    reaction_days: list[pd.Timestamp | pd.NaT] = []
    timing: list[str] = []
    for timestamp, local_day in zip(accepted, local_days, strict=True):
        session = calendar.loc[local_day]
        if not bool(session["is_open"]):
            reaction_days.append(_next_open_session(calendar, local_day))
            timing.append("non_session")
            continue
        open_at = pd.Timestamp(session["open_at"])
        close_at = pd.Timestamp(session["close_at"])
        if timestamp < open_at:
            reaction_days.append(local_day)
            timing.append("pre_market")
        elif timestamp > close_at:
            reaction_days.append(_next_open_session(calendar, local_day))
            timing.append("after_market")
        else:
            reaction_days.append(pd.NaT)
            timing.append("intraday")
    out["acceptance_local_date"] = local_days.to_numpy()
    out["reaction_date"] = pd.to_datetime(reaction_days)
    out["timing"] = timing
    return out


def dedupe_earliest_disclosures(events: pd.DataFrame) -> pd.DataFrame:
    """Keep the earliest original disclosure for each security/reporting period."""
    required = {"security_id", "period_of_report", "accepted_at", "accession_number"}
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"events missing columns: {sorted(missing)}")
    if events[list(required)].isna().any().any():
        raise ValueError("deduplication requires non-null identity, period, time, and accession")
    ordered = events.sort_values(
        ["security_id", "period_of_report", "accepted_at", "accession_number"],
        kind="stable",
    )
    return ordered.drop_duplicates(["security_id", "period_of_report"], keep="first").copy()


def dedupe_same_reaction_day(events: pd.DataFrame) -> pd.DataFrame:
    """Apply the preregistered first-information rule before eligibility checks."""
    required = {"security_id", "reaction_date", "accepted_at", "accession_number"}
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"events missing columns: {sorted(missing)}")
    if events["reaction_date"].isna().any():
        raise ValueError("same-reaction-day dedupe requires mapped reaction_date")
    ordered = events.sort_values(
        ["security_id", "reaction_date", "accepted_at", "accession_number"],
        kind="stable",
    )
    return ordered.drop_duplicates(["security_id", "reaction_date"], keep="first").copy()


def load_earnings_gap_filings(
    engine,
    *,
    accepted_end: date,
) -> pd.DataFrame:
    """Load all prior Item 2.02 8-K and 8-K/A rows for the source audit."""
    frame = pd.read_sql_query(
        text(
            """
            select security_id, accession_number, form_type, filing_date,
                   accepted_at, period_of_report, items
            from sec_filings
            where source = 'SEC_EDGAR'
              and form_type = any(:forms)
              and accepted_at is not null
              and accepted_at < :accepted_end
              and items ~ '(^|,)[[:space:]]*2\\.02[[:space:]]*(,|$)'
            order by filing_date, accepted_at, accession_number
            """
        ),
        engine,
        params={
            "forms": ["8-K", "8-K/A"],
            "accepted_end": accepted_end,
        },
        parse_dates=["filing_date", "accepted_at", "period_of_report"],
    )
    if frame.empty:
        raise RuntimeError("no SEC_EDGAR Item 2.02 8-K or 8-K/A filings in study window")
    return frame


def load_reaction_day_actions(
    engine,
    *,
    security_ids: list[int],
    start: date,
    end: date,
) -> set[tuple[int, date]]:
    if not security_ids:
        return set()
    frame = pd.read_sql_query(
        text(
            """
            select distinct security_id, ex_date
            from corporate_actions
            where security_id = any(:security_ids)
              and action_type in ('DIVIDEND', 'SPLIT')
              and ex_date between :start and :end
            """
        ),
        engine,
        params={"security_ids": security_ids, "start": start, "end": end},
        parse_dates=["ex_date"],
    )
    return {(int(row.security_id), pd.Timestamp(row.ex_date).date()) for row in frame.itertuples()}


def exclude_reaction_day_actions(events: pd.DataFrame, action_days: set[tuple[int, date]]) -> pd.DataFrame:
    keep = [
        (int(row.security_id), pd.Timestamp(row.reaction_date).date()) not in action_days
        for row in events.itertuples(index=False)
    ]
    return events.loc[keep].copy()


def compute_event_signals(
    events: pd.DataFrame,
    raw_bars: dict[str, pd.DataFrame],
    *,
    min_price: float = MIN_PRICE,
    min_median_dollar_volume: float = MIN_MEDIAN_DOLLAR_VOLUME,
) -> pd.DataFrame:
    """Compute frozen raw-bar signals with no use of a reaction-day future bar."""
    required_bars = {"open", "high", "low", "close", "volume"}
    if required_bars - set(raw_bars):
        raise ValueError(f"raw_bars missing columns: {sorted(required_bars - set(raw_bars))}")
    index = pd.DatetimeIndex(raw_bars["close"].index)
    columns = pd.Index(raw_bars["close"].columns)
    for name, panel in raw_bars.items():
        if not index.equals(pd.DatetimeIndex(panel.index)) or not columns.equals(pd.Index(panel.columns)):
            raise ValueError(f"raw bar panel {name} is not aligned")
    position_for_date = {pd.Timestamp(day): position for position, day in enumerate(index)}
    position_for_security = {int(security_id): position for position, security_id in enumerate(columns)}
    arrays = {name: panel.to_numpy(dtype="float64") for name, panel in raw_bars.items()}
    rows: list[dict[str, Any]] = []
    for event in events.itertuples(index=False):
        reaction_date = pd.Timestamp(event.reaction_date)
        reaction_pos = position_for_date.get(reaction_date)
        security_pos = position_for_security.get(int(event.security_id))
        if reaction_pos is None or security_pos is None or reaction_pos < 63:
            continue
        close = arrays["close"]
        high = arrays["high"]
        low = arrays["low"]
        opening = arrays["open"]
        volume = arrays["volume"]
        reaction = np.array(
            [
                opening[reaction_pos, security_pos],
                high[reaction_pos, security_pos],
                low[reaction_pos, security_pos],
                close[reaction_pos, security_pos],
                volume[reaction_pos, security_pos],
                close[reaction_pos - 1, security_pos],
            ]
        )
        if not np.isfinite(reaction).all() or (reaction <= 0).any():
            continue
        prior_high = high[reaction_pos - 20:reaction_pos, security_pos]
        prior_low = low[reaction_pos - 20:reaction_pos, security_pos]
        prior_close_for_tr = close[reaction_pos - 21:reaction_pos - 1, security_pos]
        prior_volume = volume[reaction_pos - 20:reaction_pos, security_pos]
        prior_dollar_volume = (
            close[reaction_pos - 63:reaction_pos, security_pos]
            * volume[reaction_pos - 63:reaction_pos, security_pos]
        )
        if (
            not np.isfinite(prior_high).all()
            or not np.isfinite(prior_low).all()
            or not np.isfinite(prior_close_for_tr).all()
            or not np.isfinite(prior_volume).all()
            or not np.isfinite(prior_dollar_volume).all()
            or (prior_high <= 0).any()
            or (prior_low <= 0).any()
            or (prior_close_for_tr <= 0).any()
            or (prior_volume <= 0).any()
            or (prior_dollar_volume <= 0).any()
        ):
            continue
        true_range = np.maximum.reduce(
            [
                prior_high - prior_low,
                np.abs(prior_high - prior_close_for_tr),
                np.abs(prior_low - prior_close_for_tr),
            ]
        )
        atr20 = float(true_range.mean())
        previous_close = float(close[reaction_pos - 1, security_pos])
        atr_pct = atr20 / previous_close
        if not math.isfinite(atr_pct) or atr_pct <= 0:
            continue
        if previous_close < min_price or float(np.median(prior_dollar_volume)) < min_median_dollar_volume:
            continue
        gap = float(opening[reaction_pos, security_pos] / previous_close - 1.0)
        gap_atr = gap / atr_pct
        volume_ratio = min(float(volume[reaction_pos, security_pos] / np.median(prior_volume)), 3.0)
        volume_confirmed = gap_atr * volume_ratio
        if (
            not math.isfinite(gap_atr)
            or not math.isfinite(volume_ratio)
            or not math.isfinite(volume_confirmed)
            or gap_atr == 0.0
            or volume_confirmed == 0.0
        ):
            continue
        payload = event._asdict()
        payload.update(
            {
                "reaction_pos": int(reaction_pos),
                "gap": gap,
                "atr20": atr20,
                "atr_pct": atr_pct,
                "gap_atr": gap_atr,
                "volume_ratio": volume_ratio,
                "gap_atr_volume_confirmed": volume_confirmed,
            }
        )
        rows.append(payload)
    return pd.DataFrame(rows)


def prepare_complete_return_paths(
    events: pd.DataFrame,
    adjusted_close: pd.DataFrame,
    *,
    spy_security_id: int = SPY_SECURITY_ID,
    horizon: int = MAX_HORIZON,
) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Keep only events with all post-close asset and SPY returns through horizon."""
    if spy_security_id not in adjusted_close.columns:
        raise RuntimeError("SPY adjusted-close column is absent")
    if events.empty:
        return events.copy(), np.empty((0, horizon)), np.empty((0, horizon))
    returns = adjusted_close.pct_change(fill_method=None).to_numpy(dtype="float64")
    dates = pd.DatetimeIndex(adjusted_close.index)
    columns = pd.Index(adjusted_close.columns)
    date_pos = {pd.Timestamp(day): position for position, day in enumerate(dates)}
    security_pos = {int(value): position for position, value in enumerate(columns)}
    spy_pos = security_pos[spy_security_id]
    kept: list[int] = []
    asset_paths: list[np.ndarray] = []
    spy_paths: list[np.ndarray] = []
    for event_index, event in enumerate(events.itertuples(index=False)):
        reaction_pos = date_pos.get(pd.Timestamp(event.reaction_date))
        asset_pos = security_pos.get(int(event.security_id))
        if reaction_pos is None or asset_pos is None or reaction_pos + horizon >= len(dates):
            continue
        asset = returns[reaction_pos + 1:reaction_pos + horizon + 1, asset_pos]
        spy = returns[reaction_pos + 1:reaction_pos + horizon + 1, spy_pos]
        if not np.isfinite(asset).all() or not np.isfinite(spy).all():
            continue
        kept.append(event_index)
        asset_paths.append(asset)
        spy_paths.append(spy)
    kept_events = events.iloc[kept].reset_index(drop=True).copy()
    if not asset_paths:
        return kept_events, np.empty((0, horizon)), np.empty((0, horizon))
    return kept_events, np.vstack(asset_paths), np.vstack(spy_paths)


def summarize_signal(
    events: pd.DataFrame,
    asset_paths: np.ndarray,
    spy_paths: np.ndarray,
    calendar_dates: pd.DatetimeIndex,
    *,
    signal_name: str,
    sample_start: date,
    calendar_end: pd.Timestamp | None = None,
    horizons: tuple[int, ...] = HORIZONS,
    costs_bps: tuple[float, ...] = COST_BPS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Summarize cohort CAR and the overlapping calendar-time portfolio."""
    if signal_name not in events:
        raise ValueError(f"events lacks signal {signal_name}")
    if "accession_number" not in events:
        raise ValueError("events lacks accession_number for cohort audit output")
    if len(events) != len(asset_paths) or len(events) != len(spy_paths):
        raise ValueError("events and return paths must have equal lengths")
    if events.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    date_pos = {pd.Timestamp(day): position for position, day in enumerate(calendar_dates)}
    start_position = int(pd.DatetimeIndex(calendar_dates).searchsorted(pd.Timestamp(sample_start), side="left"))
    if calendar_end is None:
        end_position = len(calendar_dates) - 1
    else:
        end_position = date_pos.get(pd.Timestamp(calendar_end), -1)
        if end_position < start_position:
            raise ValueError("calendar_end is absent from or precedes the calendar-time sample")
    rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    cohort_rows: list[dict[str, Any]] = []
    groups = list(events.groupby("reaction_date", sort=True).indices.values())
    for horizon in horizons:
        required_end = max(
            date_pos[pd.Timestamp(events.iloc[indexes[0]].reaction_date)] + horizon
            for indexes in groups
        )
        if required_end > end_position:
            raise ValueError("calendar_end precedes a complete event holding path")
        length = end_position - start_position + 1
        gross_daily = np.zeros(length, dtype="float64")
        entry_exit_count = np.zeros(length, dtype="int32")
        active_cohorts = np.zeros(length, dtype="int32")
        cohort_cars: list[float] = []
        cohort_net_exposures: list[float] = []
        cohort_results: list[dict[str, Any]] = []
        for indexes in groups:
            cohort = events.iloc[list(indexes)]
            positions = np.asarray(list(indexes), dtype=int)
            signal = cohort[signal_name].to_numpy(dtype="float64")
            denominator = float(np.abs(signal).sum())
            if not math.isfinite(denominator) or denominator == 0.0:
                raise RuntimeError(f"{signal_name} has a zero gross-normalization cohort")
            weights = signal / denominator
            abnormal = weights @ asset_paths[positions, :horizon]
            abnormal -= weights.sum() * spy_paths[positions[0], :horizon]
            reaction_position = date_pos[pd.Timestamp(cohort.iloc[0].reaction_date)]
            offset = reaction_position - start_position
            gross_daily[offset + 1:offset + horizon + 1] += abnormal / horizon
            entry_exit_count[offset] += 1
            entry_exit_count[offset + horizon] += 1
            active_cohorts[offset + 1:offset + horizon + 1] += 1
            gross_car = float(abnormal.sum())
            net_exposure = float(weights.sum())
            cohort_cars.append(gross_car)
            cohort_net_exposures.append(net_exposure)
            cohort_results.append(
                {
                    "reaction_date": pd.Timestamp(cohort.iloc[0].reaction_date),
                    "signal": signal_name,
                    "horizon": int(horizon),
                    "n_events": int(len(cohort)),
                    "gross_car": gross_car,
                    "net_exposure": net_exposure,
                    "accession_numbers": ",".join(sorted(cohort["accession_number"].astype(str))),
                }
            )
        for cost_bps in costs_bps:
            per_side_cost = cost_bps / 10_000.0
            net_daily = gross_daily - entry_exit_count * per_side_cost / horizon
            nw_lag = default_nw_lag(horizon, len(net_daily))
            rows.append(
                {
                    "signal": signal_name,
                    "horizon": int(horizon),
                    "cost_bps": float(cost_bps),
                    "n_events": int(len(events)),
                    "n_cohorts": int(len(groups)),
                    "gross_event_car": float(np.mean(cohort_cars)),
                    "net_event_car": float(np.mean(cohort_cars) - 2 * per_side_cost),
                    "gross_calendar_mean": float(gross_daily.mean()),
                    "net_calendar_mean": float(net_daily.mean()),
                    "net_calendar_nw_t": _newey_west_t(pd.Series(net_daily), nw_lag),
                    "nw_lag": int(nw_lag),
                    "calendar_days": int(len(net_daily)),
                    "mean_active_cohorts": float(active_cohorts.mean()),
                    "mean_gross_exposure": float(active_cohorts.mean() / horizon),
                    "mean_abs_net_exposure": float(
                        np.mean(np.abs(cohort_net_exposures)) * active_cohorts.mean() / horizon
                    ),
                    }
                )
            cohort_rows.extend(
                {
                    **cohort_result,
                    "cost_bps": float(cost_bps),
                    "net_car": float(cohort_result["gross_car"] - 2 * per_side_cost),
                }
                for cohort_result in cohort_results
            )
            for offset, timestamp in enumerate(calendar_dates[start_position:end_position + 1]):
                daily_rows.append(
                    {
                        "date": pd.Timestamp(timestamp),
                        "signal": signal_name,
                        "horizon": int(horizon),
                        "cost_bps": float(cost_bps),
                        "gross_return": float(gross_daily[offset]),
                        "net_return": float(net_daily[offset]),
                        "active_cohorts": int(active_cohorts[offset]),
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(daily_rows), pd.DataFrame(cohort_rows)


def hypothesis_verdicts(primary: pd.DataFrame, stability: pd.DataFrame) -> dict[str, bool]:
    """Apply all frozen H1/H2 gates at the 25 bps per-side primary cost."""
    def row(frame: pd.DataFrame, signal: str, horizon: int) -> pd.Series:
        selected = frame[
            (frame["signal"] == signal)
            & (frame["horizon"] == horizon)
            & (frame["cost_bps"] == PRIMARY_COST_BPS)
        ]
        if len(selected) != 1:
            raise ValueError(f"expected one {signal} h{horizon} primary-cost row, found {len(selected)}")
        return selected.iloc[0]

    primary_gap = {horizon: row(primary, "gap_atr", horizon) for horizon in HORIZONS}
    primary_volume = {
        horizon: row(primary, "gap_atr_volume_confirmed", horizon)
        for horizon in HORIZONS
    }
    stability_gap = row(stability, "gap_atr", 20)
    stability_volume = row(stability, "gap_atr_volume_confirmed", 20)
    h1 = bool(
        primary_gap[20]["net_calendar_mean"] > 0
        and primary_gap[20]["net_calendar_nw_t"] >= 3.0
        and primary_gap[1]["net_calendar_mean"] > 0
        and primary_gap[5]["net_calendar_mean"] > 0
        and stability_gap["net_calendar_mean"] > 0
        and primary_gap[20]["net_event_car"] > 0
    )
    h2 = bool(
        primary_volume[20]["net_calendar_mean"] > 0
        and primary_volume[20]["net_calendar_nw_t"] >= 3.0
        and primary_volume[1]["net_calendar_mean"] > 0
        and primary_volume[5]["net_calendar_mean"] > 0
        and stability_volume["net_calendar_mean"] > 0
        and primary_volume[20]["net_event_car"] > 0
        and all(
            primary_volume[horizon]["net_calendar_mean"]
            > primary_gap[horizon]["net_calendar_mean"]
            for horizon in HORIZONS
        )
        and primary_volume[20]["net_event_car"]
        >= 1.25 * primary_gap[20]["net_event_car"]
    )
    return {"gap_atr": h1, "gap_atr_volume_confirmed": h2}


def _assert_spy_adjustment_coverage(engine, *, start: date, end: date) -> None:
    with engine.connect() as connection:
        count = connection.execute(
            text(
                """
                select count(*)
                from computed_adjustment_factors
                where security_id = :security_id
                  and methodology_version = 'raw_actions_v1'
                  and factor_type = 'historical_adjustment'
                  and date between :start and :end
                """
            ),
            {"security_id": SPY_SECURITY_ID, "start": start, "end": end},
        ).scalar_one()
    expected = max(2, int((end - start).days / 365.25 * 4) - 3)
    if count < expected:
        raise RuntimeError(
            f"SPY adjustment-factor coverage {count} < required {expected} for {start} through {end}"
        )


def _latest_spy_price_date(engine) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text("select max(date) from daily_prices where security_id = :security_id"),
            {"security_id": SPY_SECURITY_ID},
        ).scalar_one()
    if value is None:
        raise RuntimeError("SPY has no daily_prices rows")
    return value


def latest_complete_spy_session(calendar: pd.DataFrame, *, spy_price_end: date) -> date:
    """Bound the study at an observed XNYS session whose close has passed."""
    now = pd.Timestamp(datetime.now(timezone.utc))
    eligible = calendar[
        calendar["is_open"].fillna(False)
        & (calendar["trade_date"] <= pd.Timestamp(spy_price_end))
        & (calendar["close_at"] <= now)
    ]
    if eligible.empty:
        raise RuntimeError("no completed XNYS session with an observed SPY daily bar")
    return pd.Timestamp(eligible["trade_date"].iloc[-1]).date()


def _sessions_for_sample(
    calendar: pd.DataFrame,
    *,
    sample_start: date,
    sample_end: date,
    observed_end: date,
) -> pd.DatetimeIndex:
    sessions = pd.DatetimeIndex(calendar.index[calendar["is_open"].fillna(False)])
    sample_end_position = int(sessions.searchsorted(pd.Timestamp(sample_end), side="left"))
    observed_end_position = int(sessions.searchsorted(pd.Timestamp(observed_end), side="left"))
    start_position = int(sessions.searchsorted(pd.Timestamp(sample_start), side="left"))
    if sample_end_position >= len(sessions) or sessions[sample_end_position].date() != sample_end:
        raise RuntimeError(f"sample end is not an XNYS session: {sample_end}")
    if observed_end_position >= len(sessions) or sessions[observed_end_position].date() != observed_end:
        raise RuntimeError(f"observed SPY end is not an XNYS session: {observed_end}")
    if observed_end_position < start_position:
        raise RuntimeError(f"observed SPY end precedes sample start: {observed_end}")
    # Formation dates define sample membership. Historical stability events at
    # the sample boundary retain their already-observed h20 outcome path.
    end_position = min(sample_end_position + MAX_HORIZON, observed_end_position)
    lookback_position = max(0, start_position - 70)
    return sessions[lookback_position:end_position + 1]


def _json_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def run_sample(
    engine,
    *,
    source_events: pd.DataFrame,
    calendar: pd.DataFrame,
    sample_name: str,
    sample_start: date,
    sample_end: date,
    observed_end: date,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Run one predeclared window and return summary, daily series, events, audit."""
    audit: dict[str, int] = {"source_timing_candidates": int(len(source_events))}
    timed = attach_reaction_days(source_events, calendar)
    audit["intraday_excluded"] = int((timed["timing"] == "intraday").sum())
    mapped = timed[timed["reaction_date"].notna()].copy()
    in_window = mapped[
        (mapped["reaction_date"] >= pd.Timestamp(sample_start))
        & (mapped["reaction_date"] <= pd.Timestamp(sample_end))
    ].copy()
    audit["outside_reaction_window"] = int(len(mapped) - len(in_window))
    same_day = dedupe_same_reaction_day(in_window)
    audit["same_reaction_day_removed"] = int(len(in_window) - len(same_day))
    action_days = load_reaction_day_actions(
        engine,
        security_ids=sorted(int(value) for value in same_day["security_id"].unique()),
        start=sample_start,
        end=sample_end,
    )
    no_action = exclude_reaction_day_actions(same_day, action_days)
    audit["reaction_day_action_removed"] = int(len(same_day) - len(no_action))

    sample_sessions = _sessions_for_sample(
        calendar,
        sample_start=sample_start,
        sample_end=sample_end,
        observed_end=observed_end,
    )
    gate_start = sample_sessions[0].date()
    gate_end = sample_sessions[-1].date()
    bad_ids = set(
        securities_with_uncovered_events(
            engine,
            start=gate_start,
            end=gate_end,
            require_straddle=True,
        )
    )
    integrity_ok = no_action[~no_action["security_id"].isin(bad_ids)].copy()
    audit["uncovered_adjustment_security_removed"] = int(len(no_action) - len(integrity_ok))
    ids = sorted({int(value) for value in integrity_ok["security_id"]} | {SPY_SECURITY_ID})
    if len(ids) == 1:
        raise RuntimeError(f"{sample_name}: no event securities survive integrity gates")

    raw = raw_bar_panels(
        engine,
        dates=sample_sessions,
        security_ids=ids,
        columns=("open", "high", "low", "close", "volume"),
        buffer_days=200,
    )
    raw = {
        name: panel.reindex(index=sample_sessions, columns=ids)
        for name, panel in raw.items()
    }
    signals = compute_event_signals(integrity_ok, raw)
    audit["raw_or_liquidity_or_signal_removed"] = int(len(integrity_ok) - len(signals))
    adjusted = adjusted_close_panel(
        engine,
        dates=sample_sessions,
        security_ids=ids,
        buffer_days=200,
    ).reindex(index=sample_sessions, columns=ids)
    spy = adjusted[SPY_SECURITY_ID]
    if spy.loc[pd.Timestamp(sample_start):].isna().any():
        raise RuntimeError(f"{sample_name}: SPY adjusted close has a gap in the observed sample")
    complete, asset_paths, spy_paths = prepare_complete_return_paths(signals, adjusted)
    audit["incomplete_20_session_path_removed"] = int(len(signals) - len(complete))
    audit["valid_events"] = int(len(complete))
    if complete.empty:
        raise RuntimeError(f"{sample_name}: no complete events after all frozen gates")
    summaries = []
    daily = []
    cohorts = []
    for signal_name in ("gap_atr", "gap_atr_volume_confirmed"):
        summary, daily_returns, cohort_cars = summarize_signal(
            complete,
            asset_paths,
            spy_paths,
            sample_sessions,
            signal_name=signal_name,
            sample_start=sample_start,
            calendar_end=sample_sessions[-1],
        )
        summaries.append(summary)
        daily.append(daily_returns)
        cohorts.append(cohort_cars)
    summary = pd.concat(summaries, ignore_index=True)
    daily_frame = pd.concat(daily, ignore_index=True)
    cohort_frame = pd.concat(cohorts, ignore_index=True)
    complete["sample"] = sample_name
    return summary, daily_frame, cohort_frame, complete, audit


def _criterion_values(primary: pd.DataFrame, stability: pd.DataFrame) -> dict[str, float]:
    values: dict[str, float] = {}
    for sample_name, frame in (("primary", primary), ("stability", stability)):
        selected = frame[frame["cost_bps"] == PRIMARY_COST_BPS]
        for row in selected.itertuples(index=False):
            prefix = f"{sample_name}_{row.signal}_h{row.horizon}"
            values[f"{prefix}_net_calendar_mean"] = float(row.net_calendar_mean)
            values[f"{prefix}_net_calendar_nw_t"] = float(row.net_calendar_nw_t)
            values[f"{prefix}_net_event_car"] = float(row.net_event_car)
    return values


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    engine = research_engine()
    spy_price_end = _latest_spy_price_date(engine)
    if spy_price_end < PRIMARY_START:
        raise RuntimeError(f"SPY data ends before primary sample: {spy_price_end}")
    calendar = load_xnys_calendar(
        engine,
        start=STABILITY_START - timedelta(days=500),
        end=spy_price_end + timedelta(days=14),
    )
    observed_end = latest_complete_spy_session(calendar, spy_price_end=spy_price_end)
    primary_end = observed_end
    _assert_spy_adjustment_coverage(engine, start=STABILITY_START, end=observed_end)
    source = load_earnings_gap_filings(
        engine,
        accepted_end=primary_end + timedelta(days=14),
    )
    audit_source = {
        "item_202_original_8k": int((source["form_type"] == "8-K").sum()),
        "item_202_amended_8ka": int((source["form_type"] == "8-K/A").sum()),
    }
    original = source[source["form_type"] == "8-K"].copy()
    required = original.dropna(
        subset=["security_id", "accepted_at", "period_of_report", "accession_number"]
    ).copy()
    audit_source["original_missing_required_fields"] = int(len(original) - len(required))
    deduped = dedupe_earliest_disclosures(required)
    audit_source["same_security_period_removed"] = int(len(required) - len(deduped))
    timing_start = pd.Timestamp(STABILITY_START - timedelta(days=14), tz="UTC")
    timing_end = pd.Timestamp(primary_end + timedelta(days=14), tz="UTC")
    timing_candidates = deduped[
        (deduped["accepted_at"] >= timing_start)
        & (deduped["accepted_at"] < timing_end)
    ].copy()
    audit_source["outside_timing_buffer_after_global_dedupe"] = int(
        len(deduped) - len(timing_candidates)
    )

    primary, primary_daily, primary_cohorts, primary_events, primary_audit = run_sample(
        engine,
        source_events=timing_candidates,
        calendar=calendar,
        sample_name="primary",
        sample_start=PRIMARY_START,
        sample_end=primary_end,
        observed_end=observed_end,
    )
    clear_cache()
    stability, stability_daily, stability_cohorts, stability_events, stability_audit = run_sample(
        engine,
        source_events=timing_candidates,
        calendar=calendar,
        sample_name="stability",
        sample_start=STABILITY_START,
        sample_end=STABILITY_END,
        observed_end=observed_end,
    )
    verdicts = hypothesis_verdicts(primary, stability)
    summary = pd.concat(
        [primary.assign(sample="primary"), stability.assign(sample="stability")],
        ignore_index=True,
    )
    events = pd.concat([primary_events, stability_events], ignore_index=True)
    daily = pd.concat(
        [primary_daily.assign(sample="primary"), stability_daily.assign(sample="stability")],
        ignore_index=True,
    )
    cohorts = pd.concat(
        [primary_cohorts.assign(sample="primary"), stability_cohorts.assign(sample="stability")],
        ignore_index=True,
    )
    audit = {"source": audit_source, "primary": primary_audit, "stability": stability_audit}
    OUTPUT_DIR.mkdir(exist_ok=True)
    stem = f"earnings_gap_{PRIMARY_START}_{primary_end}"
    json_path = OUTPUT_DIR / f"{stem}.json"
    markdown_path = OUTPUT_DIR / f"{stem}.md"
    event_path = OUTPUT_DIR / f"{stem}_events.parquet"
    daily_path = OUTPUT_DIR / f"{stem}_calendar_returns.parquet"
    cohort_path = OUTPUT_DIR / f"{stem}_cohort_cars.parquet"
    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "config": {
            "family": "earnings_gap",
            "source": "SEC_EDGAR original 8-K Item 2.02",
            "calendar": "trading_calendars XNYS",
            "primary_window": [str(PRIMARY_START), str(primary_end)],
            "stability_window": [str(STABILITY_START), str(STABILITY_END)],
            "observed_spy_end": str(observed_end),
            "horizons": list(HORIZONS),
            "cost_bps_per_side": list(COST_BPS),
            "signal_definitions": ["gap_atr", "gap_atr_volume_confirmed"],
        },
        "audit": audit,
        "verdicts": verdicts,
        "summary": _json_records(summary),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    events.to_parquet(event_path, index=False)
    daily.to_parquet(daily_path, index=False)
    cohorts.to_parquet(cohort_path, index=False)
    primary_table = primary[primary["cost_bps"] == PRIMARY_COST_BPS].copy()
    stability_table = stability[stability["cost_bps"] == PRIMARY_COST_BPS].copy()
    markdown_path.write_text(
        "\n".join(
            [
                f"# Wave 14 earnings-gap study: {PRIMARY_START} to {primary_end}",
                "",
                "Original SEC_EDGAR 8-K Item 2.02 only. Positions enter after the reaction-day close.",
                "",
                "## Primary, 25 bps per side",
                "",
                _markdown_table(primary_table.round(8), include_index=False),
                "",
                "## Stability, 25 bps per side",
                "",
                _markdown_table(stability_table.round(8), include_index=False),
                "",
                "## Frozen decisions",
                "",
                f"- H1 `gap_atr`: {'PASS' if verdicts['gap_atr'] else 'FAIL'}",
                f"- H2 `gap_atr_volume_confirmed`: {'PASS' if verdicts['gap_atr_volume_confirmed'] else 'FAIL'}",
                "",
                "## Audit counts",
                "",
                "```json",
                json.dumps(audit, ensure_ascii=False, indent=2),
                "```",
                "",
                f"Structured summary: `{json_path.name}`",
                f"Complete events: `{event_path.name}`",
                f"Calendar returns: `{daily_path.name}`",
                f"Cohort CARs: `{cohort_path.name}`",
            ]
        ),
        encoding="utf-8",
    )
    params = {
        "study_version": "wave14_earnings_gap_v1",
        "source": "SEC_EDGAR original 8-K Item 2.02",
        "dedupe": "earliest accepted_at then accession per security_id x period_of_report; then earliest per security_id x reaction_day",
        "calendar": "trading_calendars XNYS; intraday boundaries excluded",
        "raw_signal": "20 prior TR ATR, 20 prior median volume, 63 prior median dollar volume",
        "holding": "reaction-day close to next 1/5/20 closes; complete 20-session paths only",
        "cost_bps_per_side": PRIMARY_COST_BPS,
        "stability_start": str(STABILITY_START),
        "stability_end": str(STABILITY_END),
    }
    criteria_values = _criterion_values(primary, stability)
    append_study(
        study="earnings_gap",
        factor_name="gap_atr",
        verdict=verdicts["gap_atr"],
        criteria="H1: primary h20 net mean>0 and NW t>=3; h1/h5>0; stability h20>0; valid-event net CAR>0",
        params=params,
        eval_start=PRIMARY_START,
        eval_end=primary_end,
        report_path=str(markdown_path),
        criterion_values=criteria_values,
    )
    append_study(
        study="earnings_gap",
        factor_name="gap_atr_volume_confirmed",
        verdict=verdicts["gap_atr_volume_confirmed"],
        criteria="H2: H1 gates for volume signal; h1/h5/h20 calendar mean>gap_atr; h20 event CAR>=125% gap_atr",
        params=params,
        eval_start=PRIMARY_START,
        eval_end=primary_end,
        report_path=str(markdown_path),
        criterion_values=criteria_values,
    )
    print(_markdown_table(summary.round(8), include_index=False), flush=True)
    print(f"H1 gap_atr: {'PASS' if verdicts['gap_atr'] else 'FAIL'}", flush=True)
    print(
        f"H2 gap_atr_volume_confirmed: {'PASS' if verdicts['gap_atr_volume_confirmed'] else 'FAIL'}",
        flush=True,
    )
    print(f"report: {markdown_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
