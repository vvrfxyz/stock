"""Wave 15 preregistered market trend and price-breadth overlay study.

The family contains exactly three monthly rules: ``spy_10m_trend``,
``breadth_200d``, and ``trend_and_breadth``. The implementation models
drifted holdings between month-end rebalances, DTB3 cash, explicit stock
turnover, and terminal-value transfer to cash. It never writes fact tables.

Run on the research host:
    scripts/run_research.sh wave15-market-regime -- \
        .venv/bin/python -m research.market_regime_overlay_study
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

from research._trials_store import append_study
from research.data import (
    apply_adjustment,
    load_delisting_returns,
    load_factor_events,
    research_engine,
    securities_with_uncovered_events,
    to_wide,
)
from research.evaluate import _markdown_table
from research.factors.price_cache import load_price_long_fast
from research.minute_bars import query_df
from research.progress import Progress
from research.universe import build_universe_mask
from utils.risk_free_rates import load_risk_free_daily_returns

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
SPY_SECURITY_ID = 3379
PANEL_START = date(2006, 1, 3)
SPY_TOTAL_RETURN_START = date(2007, 3, 16)
SIMULATION_START = SPY_TOTAL_RETURN_START
STABILITY_START = date(2007, 7, 2)
STABILITY_END = date(2015, 12, 31)
PRIMARY_START = date(2016, 1, 4)
RULES = ("spy_10m_trend", "breadth_200d", "trend_and_breadth")
ASSETS = ("spy", "pit_cs_equal_weight")
SPY_COSTS_BPS = (1.0, 2.0, 5.0)
STOCK_COSTS_BPS = (10.0, 25.0, 40.0)
PRIMARY_COST_BPS = {"spy": 2.0, "pit_cs_equal_weight": 25.0}
CRISIS_YEARS = (2008, 2020, 2022)
TRADING_DAYS = 252
STUDY_VERSION = "wave15_market_regime_overlay_v2"
OUTPUT_STEM_PREFIX = "market_regime_overlay_v2"


@dataclass
class PortfolioPath:
    name: str
    gross_returns: pd.Series
    turnover: pd.Series
    target_exposure: pd.Series
    realized_stock_weight: pd.Series
    unresolved_missing_weight: pd.Series
    trades: pd.DataFrame

    def net_returns(self, cost_bps: float) -> pd.Series:
        return self.gross_returns - self.turnover * float(cost_bps) / 10_000.0


@dataclass
class SimulationInputs:
    dates: pd.DatetimeIndex
    columns: pd.Index
    returns: np.ndarray
    missing: np.ndarray
    terminal_events: dict[int, tuple[np.ndarray, np.ndarray]]


def load_calendar_sessions(
    engine,
    *,
    start: date,
    spy_price_end: date,
) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex]:
    """Return observed sessions and month ends from a complete XNYS calendar."""
    calendar_end = (pd.Timestamp(spy_price_end) + pd.offsets.MonthEnd(0)).date()
    frame = pd.read_sql_query(
        text(
            """
            select trade_date, is_open, close_at
            from trading_calendars
            where exchange_mic = 'XNYS'
              and trade_date between :start and :end
            order by trade_date
            """
        ),
        engine,
        params={"start": start, "end": calendar_end},
        parse_dates=["trade_date", "close_at"],
    )
    if frame.empty:
        raise RuntimeError(f"XNYS calendar is empty for {start} through {calendar_end}")
    if frame["trade_date"].duplicated().any():
        raise RuntimeError("XNYS calendar contains duplicate dates")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.normalize()
    expected_coverage_end = (
        pd.Timestamp(spy_price_end) + pd.offsets.BMonthEnd(0)
    ).normalize()
    actual_coverage_end = pd.Timestamp(frame["trade_date"].max()).normalize()
    if actual_coverage_end != expected_coverage_end:
        raise RuntimeError(
            "XNYS calendar does not cover the full observed natural month: "
            f"{actual_coverage_end.date()} != {expected_coverage_end.date()}"
        )

    open_rows = frame.loc[frame["is_open"]].copy()
    close_at = pd.to_datetime(open_rows["close_at"], utc=True, errors="coerce")
    if close_at.isna().any():
        raise RuntimeError("an open XNYS session lacks close_at")
    now = pd.Timestamp(datetime.now(timezone.utc))
    complete = open_rows.loc[
        (open_rows["trade_date"] <= pd.Timestamp(spy_price_end)) & (close_at <= now),
        "trade_date",
    ]
    if complete.empty:
        raise RuntimeError("no completed XNYS session has an observed SPY bar")
    sessions = pd.DatetimeIndex(pd.to_datetime(complete)).normalize()
    all_open_sessions = pd.DatetimeIndex(open_rows["trade_date"]).normalize()
    completed_month_ends = completed_month_end_sessions(all_open_sessions, sessions)
    return sessions, completed_month_ends


def latest_spy_price_date(engine) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text("select max(date) from daily_prices where security_id = :security_id"),
            {"security_id": SPY_SECURITY_ID},
        ).scalar_one()
    if value is None:
        raise RuntimeError("SPY has no daily_prices rows")
    return value


def assert_spy_adjustment_coverage(engine, *, start: date, end: date) -> None:
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
            f"SPY adjustment-factor coverage {count} < required {expected} "
            f"for {start} through {end}"
        )


def load_cs_and_spy_panels(
    engine,
    *,
    start: date,
    end: date,
    sessions: pd.DatetimeIndex,
) -> tuple[dict[str, pd.DataFrame], pd.Series]:
    """Load CS close/volume once through COPY and derive adjusted closes."""
    prices = load_price_long_fast(
        engine,
        start=start,
        end=end,
        columns="close, volume",
        types=("CS",),
    )
    if prices.empty:
        raise RuntimeError("CS daily-price panel is empty")
    prices["security_id"] = prices["security_id"].astype(np.int32)
    factor_events = load_factor_events(engine, as_of=end)
    adjusted = apply_adjustment(
        prices[["security_id", "date", "close"]],
        factor_events,
        as_of=end,
    )
    adj_close = to_wide(adjusted, "adj_close").reindex(sessions)
    del adjusted
    close = to_wide(prices, "close").reindex(index=sessions, columns=adj_close.columns)
    volume = to_wide(prices, "volume").reindex(index=sessions, columns=adj_close.columns)
    del prices

    spy_long = load_price_long_fast(
        engine,
        start=start,
        end=end,
        columns="close",
        security_ids=[SPY_SECURITY_ID],
        types=(),
    )
    if spy_long.empty:
        raise RuntimeError("SPY daily-price panel is empty")
    spy_adjusted = apply_adjustment(spy_long, factor_events, as_of=end)
    spy = to_wide(spy_adjusted, "adj_close").reindex(sessions)
    del spy_long, spy_adjusted, factor_events
    if SPY_SECURITY_ID not in spy.columns or spy[SPY_SECURITY_ID].isna().any():
        raise RuntimeError("SPY adjusted close has a gap in the study sessions")
    return {
        "adj_close": adj_close,
        "close": close,
        "volume": volume,
    }, spy[SPY_SECURITY_ID].astype("float64")


def month_end_sessions(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    series = pd.Series(index, index=index)
    return pd.DatetimeIndex(series.groupby(index.to_period("M")).last())


def completed_month_end_sessions(
    calendar_sessions: pd.DatetimeIndex,
    observed_sessions: pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    """Keep only natural-month XNYS closes reached by the observed sample."""
    calendar_sessions = pd.DatetimeIndex(calendar_sessions).normalize()
    observed_sessions = pd.DatetimeIndex(observed_sessions).normalize()
    if calendar_sessions.empty or observed_sessions.empty:
        raise ValueError("calendar and observed sessions must be non-empty")
    if calendar_sessions.has_duplicates or not calendar_sessions.is_monotonic_increasing:
        raise ValueError("calendar sessions must be unique and increasing")
    if observed_sessions.has_duplicates or not observed_sessions.is_monotonic_increasing:
        raise ValueError("observed sessions must be unique and increasing")
    if not observed_sessions.isin(calendar_sessions).all():
        raise ValueError("observed sessions are absent from the full XNYS calendar")
    month_ends = month_end_sessions(calendar_sessions)
    completed = month_ends[month_ends <= observed_sessions[-1]]
    if completed.empty:
        raise ValueError("observed sessions do not reach a completed natural month")
    return completed


def compute_monthly_signals(
    spy_adjusted_close: pd.Series,
    cs_adjusted_close: pd.DataFrame,
    eligible: pd.DataFrame,
    *,
    month_ends: pd.DatetimeIndex,
    spy_total_return_start: date = SPY_TOTAL_RETURN_START,
) -> pd.DataFrame:
    """Compute the three frozen month-end rules with complete-window gates."""
    dates = pd.DatetimeIndex(cs_adjusted_close.index)
    if not dates.equals(pd.DatetimeIndex(eligible.index)):
        raise ValueError("eligible and adjusted-close dates are not aligned")
    if not cs_adjusted_close.columns.equals(eligible.columns):
        raise ValueError("eligible and adjusted-close columns are not aligned")
    month_ends = pd.DatetimeIndex(month_ends).normalize()
    if month_ends.empty:
        raise ValueError("month-end calendar is empty")
    if month_ends.has_duplicates or not month_ends.is_monotonic_increasing:
        raise ValueError("month-end calendar must be unique and increasing")
    if not month_ends.isin(dates).all():
        raise ValueError("month-end calendar contains dates absent from price panels")
    spy_monthly = spy_adjusted_close.reindex(month_ends)
    spy_trend_input = spy_monthly.where(
        spy_monthly.index >= pd.Timestamp(spy_total_return_start)
    )
    spy_ma10 = spy_trend_input.rolling(10, min_periods=10).mean()
    trend_on = (spy_monthly > spy_ma10).where(spy_ma10.notna())

    sma200 = cs_adjusted_close.rolling(200, min_periods=200).mean()
    valid = sma200.loc[month_ends].notna() & eligible.loc[month_ends]
    denominator = valid.sum(axis=1).astype("int64")
    numerator = (
        (cs_adjusted_close.loc[month_ends] > sma200.loc[month_ends]) & valid
    ).sum(axis=1).astype("int64")
    breadth = numerator / denominator.where(denominator > 0)
    breadth_on = (breadth > 0.5).where(breadth.notna())

    combined_available = trend_on.notna() & breadth_on.notna()
    combined = pd.Series(np.nan, index=month_ends, dtype="float64")
    combined.loc[combined_available] = np.where(
        trend_on.loc[combined_available].astype(bool)
        & breadth_on.loc[combined_available].astype(bool),
        1.0,
        0.5,
    )
    return pd.DataFrame(
        {
            "spy_adjusted_close": spy_monthly,
            "spy_10m_average": spy_ma10,
            "breadth": breadth,
            "breadth_numerator": numerator,
            "breadth_denominator": denominator,
            "spy_10m_trend": trend_on.astype("float64"),
            "breadth_200d": breadth_on.astype("float64"),
            "trend_and_breadth": combined,
        },
        index=month_ends,
    )


def validate_signal_availability(
    signals: pd.DataFrame,
    *,
    eval_start: date,
) -> dict[str, str]:
    """Allow leading warm-up gaps, but reject missing states after first validity."""
    evaluation = signals.loc[signals.index >= pd.Timestamp(eval_start)]
    if evaluation.empty:
        raise RuntimeError(f"no month-end signals on or after {eval_start}")
    first_valid: dict[str, str] = {}
    for rule in RULES:
        valid = evaluation[rule].notna()
        if not valid.any():
            raise RuntimeError(f"{rule} never becomes available on or after {eval_start}")
        first = pd.Timestamp(valid.index[np.flatnonzero(valid.to_numpy())[0]])
        if evaluation.loc[first:, rule].isna().any():
            first_missing = evaluation.loc[first:, rule].index[
                evaluation.loc[first:, rule].isna()
            ][0]
            raise RuntimeError(
                f"{rule} becomes unavailable after first validity: "
                f"{pd.Timestamp(first_missing).date()}"
            )
        first_valid[rule] = str(first.date())
    return first_valid


def gap_recovery_returns(adjusted_close: pd.DataFrame) -> pd.DataFrame:
    """Return matrix with zero during gaps and the full move on repricing."""
    filled = adjusted_close.ffill()
    returns = filled.pct_change(fill_method=None)
    valid_pair = adjusted_close.notna() & filled.shift(1).notna()
    return returns.where(valid_pair).fillna(0.0).astype("float64")


def terminal_event_map(
    adjusted_close: pd.DataFrame,
    terminal_returns: pd.Series,
    *,
    fallback_return: float | None = None,
    fallback_ids: set[int] | None = None,
) -> dict[int, tuple[np.ndarray, np.ndarray]]:
    """Map first permanent-missing positions to liquidating terminal returns."""
    missing = adjusted_close.isna().to_numpy()
    valid = ~missing
    has_price = valid.any(axis=0)
    last_valid = np.full(valid.shape[1], -1, dtype=np.int64)
    last_valid[has_price] = valid.shape[0] - 1 - np.argmax(valid[::-1, has_price], axis=0)
    series = terminal_returns.reindex(adjusted_close.columns).astype("float64")
    fallback_ids = fallback_ids or set()
    grouped: dict[int, list[tuple[int, float]]] = {}
    for column_position, security_id in enumerate(adjusted_close.columns):
        event_position = int(last_valid[column_position] + 1)
        if last_valid[column_position] < 0 or event_position >= len(adjusted_close):
            continue
        value = series.iloc[column_position]
        if pd.isna(value):
            if fallback_return is None or int(security_id) not in fallback_ids:
                continue
            value = float(fallback_return)
        if not math.isfinite(float(value)) or float(value) < -1.0:
            raise ValueError(f"invalid terminal return for security_id={security_id}: {value}")
        grouped.setdefault(event_position, []).append((column_position, float(value)))
    return {
        position: (
            np.asarray([item[0] for item in items], dtype=np.int64),
            np.asarray([item[1] for item in items], dtype="float64"),
        )
        for position, items in grouped.items()
    }


def daily_target_exposure(
    dates: pd.DatetimeIndex,
    exposure_at_rebalance: pd.Series,
) -> pd.Series:
    """Month-end signal becomes the target exposure for the next session."""
    exposure = exposure_at_rebalance.reindex(dates).ffill().shift(1).fillna(0.0)
    return exposure.astype("float64").rename("target_exposure")


def prepare_simulation_inputs(
    adjusted_close: pd.DataFrame,
    terminal_returns: pd.Series,
    *,
    terminal_fallback: float | None = None,
    terminal_fallback_ids: set[int] | None = None,
) -> SimulationInputs:
    dates = pd.DatetimeIndex(adjusted_close.index)
    columns = pd.Index(adjusted_close.columns)
    returns = gap_recovery_returns(adjusted_close).to_numpy(dtype="float64", copy=True)
    missing = adjusted_close.isna().to_numpy()
    terminal_events = terminal_event_map(
        adjusted_close,
        terminal_returns,
        fallback_return=terminal_fallback,
        fallback_ids=terminal_fallback_ids,
    )
    for position, (column_positions, values) in terminal_events.items():
        returns[position, column_positions] = values
    return SimulationInputs(
        dates=dates,
        columns=columns,
        returns=returns,
        missing=missing,
        terminal_events=terminal_events,
    )


def simulate_monthly_portfolio(
    name: str,
    inputs: SimulationInputs,
    members_at_rebalance: pd.DataFrame,
    exposure_at_rebalance: pd.Series,
    risk_free_returns: pd.Series,
    *,
    collect_trades: bool = False,
) -> PortfolioPath:
    """Simulate drifted stock and cash values between month-end rebalances.

    The loop runs over sparse month-end and terminal-event boundaries. Each
    intervening daily block is advanced with vectorized cumulative factors.
    """
    dates = inputs.dates
    columns = inputs.columns
    if not dates.equals(pd.DatetimeIndex(risk_free_returns.index)):
        raise ValueError("risk-free returns are not aligned to portfolio dates")
    if not members_at_rebalance.columns.equals(columns):
        raise ValueError("rebalance membership columns are not aligned")
    if not members_at_rebalance.index.equals(exposure_at_rebalance.index):
        raise ValueError("rebalance membership and exposure dates are not aligned")
    if not members_at_rebalance.index.isin(dates).all():
        raise ValueError("rebalance dates are absent from the price panel")
    finite_exposure = exposure_at_rebalance.dropna()
    if ((finite_exposure < 0.0) | (finite_exposure > 1.0)).any():
        raise ValueError("target exposure must be within [0, 1]")

    returns = inputs.returns
    missing = inputs.missing
    rf = risk_free_returns.to_numpy(dtype="float64")
    terminal_events = inputs.terminal_events

    date_positions = {timestamp: position for position, timestamp in enumerate(dates)}
    rebalance_positions = {
        date_positions[pd.Timestamp(timestamp)]: pd.Timestamp(timestamp)
        for timestamp in members_at_rebalance.index
    }
    boundaries = sorted(set(rebalance_positions) | set(terminal_events) | {len(dates) - 1})
    gross = np.zeros(len(dates), dtype="float64")
    turnover = np.zeros(len(dates), dtype="float64")
    realized_weight = np.zeros(len(dates), dtype="float64")
    missing_weight = np.zeros(len(dates), dtype="float64")
    stock_values = np.zeros(len(columns), dtype="float64")
    cash_value = 1.0
    cursor = 0
    trade_frames: list[pd.DataFrame] = []

    for boundary in boundaries:
        if boundary < cursor:
            continue
        segment = slice(cursor, boundary + 1)
        prior_total = float(stock_values.sum() + cash_value)
        if prior_total <= 0.0 or not math.isfinite(prior_total):
            raise RuntimeError(f"{name}: non-positive portfolio value before {dates[cursor].date()}")
        stock_factors = np.cumprod(1.0 + returns[segment], axis=0)
        stock_path = stock_factors * stock_values
        cash_path = np.cumprod(1.0 + rf[segment]) * cash_value
        totals = stock_path.sum(axis=1) + cash_path
        if not np.isfinite(totals).all() or (totals <= 0.0).any():
            raise RuntimeError(f"{name}: invalid portfolio value through {dates[boundary].date()}")
        previous = np.r_[prior_total, totals[:-1]]
        gross[segment] = totals / previous - 1.0
        realized_weight[segment] = stock_path.sum(axis=1) / totals
        missing_weight[segment] = (stock_path * missing[segment]).sum(axis=1) / totals
        stock_values = stock_path[-1].copy()
        cash_value = float(cash_path[-1])
        total = float(totals[-1])

        if boundary in terminal_events:
            positions, _values = terminal_events[boundary]
            cash_value += float(stock_values[positions].sum())
            stock_values[positions] = 0.0
            total = float(stock_values.sum() + cash_value)
            missing_weight[boundary] = float(
                stock_values[missing[boundary]].sum() / total
            )

        if boundary in rebalance_positions:
            rebalance_date = rebalance_positions[boundary]
            desired_exposure = exposure_at_rebalance.loc[rebalance_date]
            if pd.notna(desired_exposure):
                desired = float(desired_exposure)
                pretrade = stock_values / total
                frozen = missing[boundary] & (stock_values > 0.0)
                frozen_weight = float(pretrade[frozen].sum())
                if frozen_weight > 1.0 + 1e-10:
                    raise RuntimeError(f"{name}: frozen weight exceeds 100%")
                target = np.zeros(len(columns), dtype="float64")
                target[frozen] = pretrade[frozen]
                remaining = max(0.0, desired - frozen_weight)
                members = members_at_rebalance.loc[rebalance_date].to_numpy(
                    dtype=bool, copy=True
                )
                members &= ~missing[boundary]
                member_count = int(members.sum())
                if remaining > 1e-12 and member_count == 0:
                    raise RuntimeError(
                        f"{name}: no tradable members for target exposure {desired:.2f} "
                        f"on {rebalance_date.date()}"
                    )
                if member_count:
                    target[members] = remaining / member_count
                target_sum = float(target.sum())
                if target_sum > 1.0 + 1e-10:
                    raise RuntimeError(f"{name}: target stock weight exceeds 100%")
                absolute_trade = np.abs(target - pretrade)
                turnover[boundary] = float(absolute_trade.sum())
                if collect_trades:
                    traded = np.flatnonzero(absolute_trade > 1e-15)
                    if len(traded):
                        trade_frames.append(
                            pd.DataFrame(
                                {
                                    "date": rebalance_date,
                                    "rule": name,
                                    "security_id": columns[traded].to_numpy(dtype="int64"),
                                    "abs_weight": absolute_trade[traded],
                                }
                            )
                        )
                stock_values = target * total
                cash_value = (1.0 - target_sum) * total
                missing_weight[boundary] = float(target[frozen].sum())
        cursor = boundary + 1

    if cursor != len(dates):
        raise RuntimeError(f"{name}: simulation stopped before the final date")
    trades = (
        pd.concat(trade_frames, ignore_index=True)
        if trade_frames
        else pd.DataFrame(columns=["date", "rule", "security_id", "abs_weight"])
    )
    return PortfolioPath(
        name=name,
        gross_returns=pd.Series(gross, index=dates, name="gross_return"),
        turnover=pd.Series(turnover, index=dates, name="turnover"),
        target_exposure=daily_target_exposure(dates, exposure_at_rebalance),
        realized_stock_weight=pd.Series(
            realized_weight, index=dates, name="realized_stock_weight"
        ),
        unresolved_missing_weight=pd.Series(
            missing_weight, index=dates, name="unresolved_missing_weight"
        ),
        trades=trades,
    )


def portfolio_metrics(
    returns: pd.Series,
    risk_free_returns: pd.Series,
    *,
    turnover: pd.Series,
    target_exposure: pd.Series,
    realized_stock_weight: pd.Series,
    unresolved_missing_weight: pd.Series,
    baseline_net_returns: pd.Series,
    baseline_gross_returns: pd.Series,
) -> dict[str, float]:
    frame = pd.concat(
        {
            "return": returns,
            "rf": risk_free_returns,
            "turnover": turnover,
            "target": target_exposure,
            "realized": realized_stock_weight,
            "missing": unresolved_missing_weight,
            "baseline_net": baseline_net_returns,
            "baseline_gross": baseline_gross_returns,
        },
        axis=1,
    ).dropna()
    if frame.empty:
        raise ValueError("cannot compute metrics on an empty return sample")
    values = frame["return"].to_numpy(dtype="float64")
    if (values <= -1.0).any():
        raise ValueError("portfolio return reached or fell below -100%")
    equity = np.r_[1.0, np.cumprod(1.0 + values)]
    drawdown = equity / np.maximum.accumulate(equity) - 1.0
    excess = frame["return"] - frame["rf"]
    excess_std = float(excess.std(ddof=1))
    downside = np.minimum(excess.to_numpy(dtype="float64"), 0.0)
    downside_deviation = float(np.sqrt(np.mean(downside**2)))
    worst_count = max(1, int(math.ceil(0.05 * len(values))))
    worst = np.partition(values, worst_count - 1)[:worst_count]
    down_days = frame["baseline_net"] < 0.0
    false_kill = (frame["target"] < 1.0 - 1e-12) & (
        frame["baseline_gross"] > frame["rf"]
    )
    years = len(frame) / TRADING_DAYS
    return {
        "n_days": float(len(frame)),
        "total_return": float(equity[-1] - 1.0),
        "cagr": float(equity[-1] ** (1.0 / years) - 1.0),
        "ann_vol": float(frame["return"].std(ddof=1) * np.sqrt(TRADING_DAYS)),
        "sharpe": (
            float(excess.mean() / excess_std * np.sqrt(TRADING_DAYS))
            if excess_std > 0.0
            else float("nan")
        ),
        "sortino": (
            float(excess.mean() / downside_deviation * np.sqrt(TRADING_DAYS))
            if downside_deviation > 0.0
            else float("nan")
        ),
        "max_drawdown": float(drawdown.min()),
        "expected_shortfall_95": float(worst.mean()),
        "down_capture": (
            float(frame.loc[down_days, "return"].mean()
                  / frame.loc[down_days, "baseline_net"].mean())
            if down_days.any()
            else float("nan")
        ),
        "ann_turnover": float(frame["turnover"].mean() * TRADING_DAYS),
        "avg_target_exposure": float(frame["target"].mean()),
        "avg_realized_stock_weight": float(frame["realized"].mean()),
        "false_kill_days": float(false_kill.sum()),
        "false_kill_missed_excess_sum": float(
            ((1.0 - frame.loc[false_kill, "target"])
             * (frame.loc[false_kill, "baseline_gross"]
                - frame.loc[false_kill, "rf"])).sum()
        ),
        "unresolved_missing_weight_days": float((frame["missing"] > 1e-12).sum()),
        "mean_unresolved_missing_weight": float(frame["missing"].mean()),
        "max_unresolved_missing_weight": float(frame["missing"].max()),
    }


def sample_slices(end: date) -> dict[str, tuple[date, date]]:
    return {
        "stability": (STABILITY_START, STABILITY_END),
        "primary": (PRIMARY_START, end),
    }


def evaluate_paths(
    paths: dict[str, dict[str, PortfolioPath]],
    risk_free_returns: pd.Series,
    *,
    end: date,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for asset, asset_paths in paths.items():
        costs = SPY_COSTS_BPS if asset == "spy" else STOCK_COSTS_BPS
        baseline = asset_paths["buy_and_hold"]
        for sample, (sample_start, sample_end) in sample_slices(end).items():
            indexer = slice(pd.Timestamp(sample_start), pd.Timestamp(sample_end))
            rf = risk_free_returns.loc[indexer]
            baseline_gross = baseline.gross_returns.loc[indexer]
            for cost_bps in costs:
                baseline_net = baseline.net_returns(cost_bps).loc[indexer]
                baseline_metrics = portfolio_metrics(
                    baseline_net,
                    rf,
                    turnover=baseline.turnover.loc[indexer],
                    target_exposure=baseline.target_exposure.loc[indexer],
                    realized_stock_weight=baseline.realized_stock_weight.loc[indexer],
                    unresolved_missing_weight=baseline.unresolved_missing_weight.loc[indexer],
                    baseline_net_returns=baseline_net,
                    baseline_gross_returns=baseline_gross,
                )
                for rule in RULES:
                    path = asset_paths[rule]
                    metrics = portfolio_metrics(
                        path.net_returns(cost_bps).loc[indexer],
                        rf,
                        turnover=path.turnover.loc[indexer],
                        target_exposure=path.target_exposure.loc[indexer],
                        realized_stock_weight=path.realized_stock_weight.loc[indexer],
                        unresolved_missing_weight=path.unresolved_missing_weight.loc[indexer],
                        baseline_net_returns=baseline_net,
                        baseline_gross_returns=baseline_gross,
                    )
                    row: dict[str, Any] = {
                        "asset": asset,
                        "sample": sample,
                        "rule": rule,
                        "cost_bps": float(cost_bps),
                    }
                    row.update(metrics)
                    row.update({f"baseline_{key}": value for key, value in baseline_metrics.items()})
                    row["drawdown_improvement"] = (
                        metrics["max_drawdown"] - baseline_metrics["max_drawdown"]
                    )
                    row["sharpe_improvement"] = metrics["sharpe"] - baseline_metrics["sharpe"]
                    row["cagr_loss"] = baseline_metrics["cagr"] - metrics["cagr"]
                    row["passes_drawdown"] = row["drawdown_improvement"] >= 0.10
                    row["passes_sharpe"] = row["sharpe_improvement"] >= 0.10
                    row["passes_cagr"] = row["cagr_loss"] <= 0.02
                    row["cell_pass"] = bool(
                        row["passes_drawdown"]
                        and row["passes_sharpe"]
                        and row["passes_cagr"]
                    )
                    rows.append(row)
    return pd.DataFrame(rows)


def hypothesis_verdicts(metrics: pd.DataFrame) -> dict[str, bool]:
    verdicts: dict[str, bool] = {}
    for rule in RULES:
        selected = metrics[
            (metrics["rule"] == rule)
            & metrics.apply(
                lambda row: row["cost_bps"] == PRIMARY_COST_BPS[row["asset"]],
                axis=1,
            )
        ]
        cells = selected.set_index(["asset", "sample"])["cell_pass"]
        expected = pd.MultiIndex.from_product([ASSETS, ("stability", "primary")])
        if not expected.isin(cells.index).all() or len(cells) != len(expected):
            raise ValueError(f"{rule}: expected four unique primary-cost decision cells")
        verdicts[rule] = bool(cells.reindex(expected).all())
    return verdicts


def crisis_metrics(
    paths: dict[str, dict[str, PortfolioPath]],
    *,
    risk_free_returns: pd.Series,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for asset, asset_paths in paths.items():
        cost_bps = PRIMARY_COST_BPS[asset]
        for year in CRISIS_YEARS:
            year_dates = risk_free_returns.index[risk_free_returns.index.year == year]
            if year_dates.empty:
                raise RuntimeError(f"study panel has no XNYS sessions for crisis year {year}")
            for rule, path in asset_paths.items():
                returns = path.net_returns(cost_bps).reindex(year_dates)
                equity = np.r_[1.0, np.cumprod(1.0 + returns.to_numpy(dtype="float64"))]
                drawdown = equity / np.maximum.accumulate(equity) - 1.0
                rows.append(
                    {
                        "asset": asset,
                        "year": year,
                        "rule": rule,
                        "cost_bps": cost_bps,
                        "total_return": float(equity[-1] - 1.0),
                        "max_drawdown": float(drawdown.min()),
                        "avg_target_exposure": float(path.target_exposure.reindex(year_dates).mean()),
                    }
                )
    return pd.DataFrame(rows)


def daily_output(
    paths: dict[str, dict[str, PortfolioPath]],
    risk_free_returns: pd.Series,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for asset, asset_paths in paths.items():
        primary_cost = PRIMARY_COST_BPS[asset]
        for rule, path in asset_paths.items():
            frames.append(
                pd.DataFrame(
                    {
                        "date": path.gross_returns.index,
                        "asset": asset,
                        "rule": rule,
                        "gross_return": path.gross_returns.to_numpy(),
                        "turnover": path.turnover.to_numpy(),
                        "primary_cost_bps": primary_cost,
                        "primary_net_return": path.net_returns(primary_cost).to_numpy(),
                        "risk_free_return": risk_free_returns.reindex(
                            path.gross_returns.index
                        ).to_numpy(),
                        "target_exposure": path.target_exposure.to_numpy(),
                        "realized_stock_weight": path.realized_stock_weight.to_numpy(),
                        "unresolved_missing_weight": path.unresolved_missing_weight.to_numpy(),
                    }
                )
            )
    return pd.concat(frames, ignore_index=True)


def _weighted_quantile(values: pd.Series, weights: pd.Series, quantile: float) -> float:
    valid = values.notna() & weights.notna() & (weights > 0.0)
    if not valid.any():
        return float("nan")
    order = np.argsort(values.loc[valid].to_numpy(dtype="float64"), kind="stable")
    sorted_values = values.loc[valid].to_numpy(dtype="float64")[order]
    sorted_weights = weights.loc[valid].to_numpy(dtype="float64")[order]
    cutoff = quantile * sorted_weights.sum()
    position = int(np.searchsorted(np.cumsum(sorted_weights), cutoff, side="left"))
    return float(sorted_values[min(position, len(sorted_values) - 1)])


def attach_measured_spread_costs(
    trades: pd.DataFrame,
    sessions: pd.DatetimeIndex,
    *,
    progress: Progress | None = None,
) -> pd.DataFrame:
    """Attach exact 63-session CS half-spread costs without missing-value fill."""
    if trades.empty:
        return trades.assign(valid_spread_days=pd.Series(dtype="int64"), cost_bps=np.nan)
    frames: list[pd.DataFrame] = []
    grouped = list(trades.groupby("date", sort=True))
    session_positions = {timestamp: position for position, timestamp in enumerate(sessions)}
    for group_number, (timestamp, group) in enumerate(grouped, 1):
        day = pd.Timestamp(timestamp)
        position = session_positions.get(day)
        if position is None:
            raise RuntimeError(f"trade date is not an XNYS session: {day.date()}")
        window = sessions[max(0, position - 62):position + 1]
        ids = sorted(int(value) for value in group["security_id"].unique())
        date_values = ",".join(f"'{value.date().isoformat()}'" for value in window)
        id_values = ",".join(str(value) for value in ids)
        result = query_df(
            f"""
            SELECT security_id,
                   countIf(n_bars >= 100 AND isFinite(cs_spread)) AS valid_spread_days,
                   medianIf(cs_spread, n_bars >= 100 AND isFinite(cs_spread)) AS spread_full
            FROM stock.minute_daily_features FINAL
            WHERE d IN ({date_values})
              AND security_id IN ({id_values})
            GROUP BY security_id
            ORDER BY security_id
            """
        )
        if result.empty:
            result = pd.DataFrame(columns=["security_id", "valid_spread_days", "spread_full"])
        result["security_id"] = pd.to_numeric(result["security_id"], errors="raise").astype("int64")
        result["valid_spread_days"] = pd.to_numeric(
            result["valid_spread_days"], errors="coerce"
        ).fillna(0).astype("int64")
        result["spread_full"] = pd.to_numeric(result["spread_full"], errors="coerce")
        result["cost_bps"] = (result["spread_full"] * 5_000.0).where(
            result["valid_spread_days"] >= 20
        )
        merged = group.merge(
            result[["security_id", "valid_spread_days", "cost_bps"]],
            on="security_id",
            how="left",
            validate="many_to_one",
        )
        merged["valid_spread_days"] = merged["valid_spread_days"].fillna(0).astype("int64")
        frames.append(merged)
        if progress is not None and (
            group_number == 1 or group_number == len(grouped) or group_number % 12 == 0
        ):
            progress.log(f"spread windows {group_number}/{len(grouped)}")
    return pd.concat(frames, ignore_index=True)


def latest_minute_feature_date() -> date:
    frame = query_df(
        "SELECT max(d) AS latest_feature_date FROM stock.minute_daily_features"
    )
    if frame.empty or pd.isna(frame.loc[0, "latest_feature_date"]):
        raise RuntimeError("stock.minute_daily_features has no source freshness date")
    return pd.Timestamp(frame.loc[0, "latest_feature_date"]).date()


def summarize_measured_spreads(
    trades: pd.DataFrame,
    *,
    study_end: date,
    sessions: pd.DatetimeIndex,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for sample, (sample_start, sample_end) in sample_slices(study_end).items():
        selected = trades[
            (trades["date"] >= pd.Timestamp(sample_start))
            & (trades["date"] <= pd.Timestamp(sample_end))
        ]
        years = int(
            ((sessions >= pd.Timestamp(sample_start)) & (sessions <= pd.Timestamp(sample_end))).sum()
        ) / TRADING_DAYS
        for rule, group in selected.groupby("rule", sort=True):
            covered = group["cost_bps"].notna()
            total_weight = float(group["abs_weight"].sum())
            covered_weight = float(group.loc[covered, "abs_weight"].sum())
            covered_cost_drag = float(
                (group.loc[covered, "abs_weight"]
                 * group.loc[covered, "cost_bps"] / 10_000.0).sum()
            )
            rows.append(
                {
                    "sample": sample,
                    "rule": rule,
                    "total_trade_weight": total_weight,
                    "covered_trade_weight": covered_weight,
                    "trade_weight_coverage": (
                        covered_weight / total_weight if total_weight > 0.0 else float("nan")
                    ),
                    "weighted_mean_cost_bps": (
                        float(np.average(
                            group.loc[covered, "cost_bps"],
                            weights=group.loc[covered, "abs_weight"],
                        ))
                        if covered_weight > 0.0
                        else float("nan")
                    ),
                    "weighted_p25_cost_bps": _weighted_quantile(
                        group.loc[covered, "cost_bps"],
                        group.loc[covered, "abs_weight"],
                        0.25,
                    ),
                    "weighted_median_cost_bps": _weighted_quantile(
                        group.loc[covered, "cost_bps"],
                        group.loc[covered, "abs_weight"],
                        0.50,
                    ),
                    "weighted_p75_cost_bps": _weighted_quantile(
                        group.loc[covered, "cost_bps"],
                        group.loc[covered, "abs_weight"],
                        0.75,
                    ),
                    "covered_cost_drag_sum": covered_cost_drag,
                    "covered_cost_drag_ann_arithmetic": (
                        covered_cost_drag / years if years > 0 else float("nan")
                    ),
                }
            )
    return pd.DataFrame(rows)


def _delisted_ids(engine, *, end: date) -> set[int]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                select id
                from securities
                where type = 'CS'
                  and delist_date is not null
                  and delist_date <= :end
                """
            ),
            {"end": end},
        ).fetchall()
    return {int(row[0]) for row in rows}


def _criterion_values(metrics: pd.DataFrame, rule: str) -> dict[str, float]:
    selected = metrics[
        (metrics["rule"] == rule)
        & metrics.apply(
            lambda row: row["cost_bps"] == PRIMARY_COST_BPS[row["asset"]], axis=1
        )
    ]
    values: dict[str, float] = {}
    for row in selected.itertuples(index=False):
        prefix = f"{row.asset}_{row.sample}"
        values[f"{prefix}_drawdown_improvement"] = float(row.drawdown_improvement)
        values[f"{prefix}_sharpe_improvement"] = float(row.sharpe_improvement)
        values[f"{prefix}_cagr_loss"] = float(row.cagr_loss)
        values[f"{prefix}_cell_pass"] = float(bool(row.cell_pass))
    return values


def _json_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _main_table(metrics: pd.DataFrame) -> pd.DataFrame:
    selected = metrics[
        metrics.apply(
            lambda row: row["cost_bps"] == PRIMARY_COST_BPS[row["asset"]], axis=1
        )
    ].copy()
    return selected[
        [
            "asset",
            "sample",
            "rule",
            "cost_bps",
            "cagr",
            "baseline_cagr",
            "sharpe",
            "baseline_sharpe",
            "max_drawdown",
            "baseline_max_drawdown",
            "drawdown_improvement",
            "sharpe_improvement",
            "cagr_loss",
            "cell_pass",
        ]
    ]


def run_study() -> dict[str, Any]:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    engine = research_engine()
    progress = Progress("wave15_market_regime", total=10, warn_gb=7.0)
    try:
        with progress.stage("calendar and source bounds", item=1):
            spy_end = latest_spy_price_date(engine)
            sessions, completed_month_ends = load_calendar_sessions(
                engine,
                start=PANEL_START,
                spy_price_end=spy_end,
            )
            observed_end = sessions[-1].date()
            if observed_end < PRIMARY_START:
                raise RuntimeError(f"observed end precedes primary sample: {observed_end}")
            assert_spy_adjustment_coverage(
                engine,
                start=SPY_TOTAL_RETURN_START,
                end=observed_end,
            )

        with progress.stage("load CS and SPY adjusted panels", item=2):
            panels, spy = load_cs_and_spy_panels(
                engine,
                start=PANEL_START,
                end=observed_end,
                sessions=sessions,
            )
            bad_ids = set(
                securities_with_uncovered_events(
                    engine,
                    start=PANEL_START,
                    end=observed_end,
                    require_straddle=True,
                )
            )
            drop = panels["adj_close"].columns.intersection(pd.Index(bad_ids))
            if len(drop):
                for key in panels:
                    panels[key] = panels[key].drop(columns=drop)

        with progress.stage("PIT eligibility and frozen signals", item=3):
            universe = build_universe_mask(
                engine,
                start=PANEL_START,
                end=observed_end,
                adj_close=panels["adj_close"],
                close=panels["close"],
                dollar_volume=panels["close"] * panels["volume"],
            )
            universe_hash = str(universe["universe_hash"])
            eligible = universe["eligible"]
            signals = compute_monthly_signals(
                spy,
                panels["adj_close"],
                eligible,
                month_ends=completed_month_ends,
            )
            first_valid_signal_dates = validate_signal_availability(
                signals,
                eval_start=STABILITY_START,
            )
            month_ends = signals.index[
                signals.index >= pd.Timestamp(SIMULATION_START)
            ]
            signals = signals.loc[month_ends].copy()
            stock_members = eligible.loc[month_ends].copy()
            del panels["close"], panels["volume"], eligible, universe

        with progress.stage("cash and terminal-value inputs", item=4):
            simulation_sessions = sessions[sessions >= pd.Timestamp(SIMULATION_START)]
            risk_free = load_risk_free_daily_returns(engine, simulation_sessions)
            realized_terminal = load_delisting_returns(engine)
            delisted_ids = _delisted_ids(engine, end=observed_end)
            baseline_stock_exposure = pd.Series(
                np.where(stock_members.sum(axis=1) > 0, 1.0, np.nan),
                index=month_ends,
                dtype="float64",
            )
            spy_members = pd.DataFrame(True, index=month_ends, columns=[SPY_SECURITY_ID])
            baseline_spy_exposure = pd.Series(1.0, index=month_ends, dtype="float64")
            operational_exposures = {
                rule: signals[rule].fillna(1.0).astype("float64") for rule in RULES
            }

        paths: dict[str, dict[str, PortfolioPath]] = {asset: {} for asset in ASSETS}
        with progress.stage("simulate SPY paths", item=5):
            spy_frame = spy.reindex(simulation_sessions).to_frame(SPY_SECURITY_ID)
            spy_inputs = prepare_simulation_inputs(
                spy_frame,
                pd.Series(dtype="float64"),
            )
            paths["spy"]["buy_and_hold"] = simulate_monthly_portfolio(
                "buy_and_hold",
                spy_inputs,
                spy_members,
                baseline_spy_exposure,
                risk_free,
            )
            for rule in RULES:
                paths["spy"][rule] = simulate_monthly_portfolio(
                    rule,
                    spy_inputs,
                    spy_members,
                    operational_exposures[rule],
                    risk_free,
                )

        stock_adj = panels["adj_close"].reindex(simulation_sessions)
        with progress.stage("simulate PIT equal-weight paths", item=6):
            stock_inputs = prepare_simulation_inputs(stock_adj, realized_terminal)
            paths["pit_cs_equal_weight"]["buy_and_hold"] = simulate_monthly_portfolio(
                "buy_and_hold",
                stock_inputs,
                stock_members,
                baseline_stock_exposure,
                risk_free,
                collect_trades=True,
            )
            for rule in RULES:
                paths["pit_cs_equal_weight"][rule] = simulate_monthly_portfolio(
                    rule,
                    stock_inputs,
                    stock_members,
                    operational_exposures[rule],
                    risk_free,
                    collect_trades=True,
                )

        with progress.stage("fixed-cost metrics and verdicts", item=7):
            metrics = evaluate_paths(paths, risk_free, end=observed_end)
            verdicts = hypothesis_verdicts(metrics)
            crises = crisis_metrics(paths, risk_free_returns=risk_free)
            daily = daily_output(paths, risk_free)
            del stock_inputs

        with progress.stage("-30% missing delisting sensitivity", item=8):
            sensitivity_paths: dict[str, dict[str, PortfolioPath]] = {
                "pit_cs_equal_weight": {}
            }
            sensitivity_inputs = prepare_simulation_inputs(
                stock_adj,
                realized_terminal,
                terminal_fallback=-0.30,
                terminal_fallback_ids=delisted_ids,
            )
            sensitivity_paths["pit_cs_equal_weight"]["buy_and_hold"] = simulate_monthly_portfolio(
                "buy_and_hold",
                sensitivity_inputs,
                stock_members,
                baseline_stock_exposure,
                risk_free,
            )
            for rule in RULES:
                sensitivity_paths["pit_cs_equal_weight"][rule] = simulate_monthly_portfolio(
                    rule,
                    sensitivity_inputs,
                    stock_members,
                    operational_exposures[rule],
                    risk_free,
                )
            sensitivity_metrics = evaluate_paths(
                sensitivity_paths,
                risk_free,
                end=observed_end,
            )
            del sensitivity_inputs, stock_adj, panels

        with progress.stage("measured spread trade coverage", item=9):
            minute_feature_end = latest_minute_feature_date()
            stock_trades = pd.concat(
                [path.trades for path in paths["pit_cs_equal_weight"].values()],
                ignore_index=True,
            )
            stock_trades = stock_trades[
                stock_trades["date"] >= pd.Timestamp(STABILITY_START)
            ].copy()
            measured_trades = attach_measured_spread_costs(
                stock_trades,
                sessions,
                progress=progress,
            )
            spread_summary = summarize_measured_spreads(
                measured_trades,
                study_end=observed_end,
                sessions=sessions,
            )

        with progress.stage("write outputs and ledger verdicts", item=10):
            OUTPUT_DIR.mkdir(exist_ok=True)
            stem = f"{OUTPUT_STEM_PREFIX}_{STABILITY_START}_{observed_end}"
            json_path = OUTPUT_DIR / f"{stem}.json"
            markdown_path = OUTPUT_DIR / f"{stem}.md"
            daily_path = OUTPUT_DIR / f"{stem}_daily.parquet"
            signal_path = OUTPUT_DIR / f"{stem}_signals.parquet"
            trade_path = OUTPUT_DIR / f"{stem}_measured_trades.parquet"
            metrics_path = OUTPUT_DIR / f"{stem}_metrics.parquet"
            sensitivity_path = OUTPUT_DIR / f"{stem}_delisting_sensitivity.parquet"
            crisis_path = OUTPUT_DIR / f"{stem}_crises.parquet"
            spread_path = OUTPUT_DIR / f"{stem}_spread_summary.parquet"

            signals.reset_index(names="date").to_parquet(signal_path, index=False)
            daily.to_parquet(daily_path, index=False)
            measured_trades.to_parquet(trade_path, index=False)
            metrics.to_parquet(metrics_path, index=False)
            sensitivity_metrics.to_parquet(sensitivity_path, index=False)
            crises.to_parquet(crisis_path, index=False)
            spread_summary.to_parquet(spread_path, index=False)
            audit = {
                "observed_end": str(observed_end),
                "n_sessions": int(len(sessions)),
                "n_cs_columns": int(stock_members.shape[1]),
                "universe_hash": universe_hash,
                "uncovered_adjustment_securities_removed": int(len(drop)),
                "mean_month_end_eligible": float(stock_members.sum(axis=1).mean()),
                "last_completed_month_end": str(signals.index[-1].date()),
                "first_valid_signal_dates": first_valid_signal_dates,
                "min_eval_breadth_denominator": int(
                    signals.loc[
                        signals.index
                        >= pd.Timestamp(first_valid_signal_dates["breadth_200d"]),
                        "breadth_denominator",
                    ].min()
                ),
                "realized_terminal_returns": int(len(realized_terminal)),
                "known_delisted_ids": int(len(delisted_ids)),
                "minute_spread_feature_end": str(minute_feature_end),
                "measured_spread_trade_match_end": (
                    str(measured_trades.loc[measured_trades["cost_bps"].notna(), "date"].max().date())
                    if measured_trades["cost_bps"].notna().any()
                    else None
                ),
            }
            payload = {
                "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "config": {
                    "family": "market_regime_overlay",
                    "study_version": STUDY_VERSION,
                    "panel_start": str(PANEL_START),
                    "spy_total_return_start": str(SPY_TOTAL_RETURN_START),
                    "simulation_start": str(SIMULATION_START),
                    "stability_window": [str(STABILITY_START), str(STABILITY_END)],
                    "primary_window": [str(PRIMARY_START), str(observed_end)],
                    "rules": list(RULES),
                    "assets": list(ASSETS),
                    "spy_costs_bps_per_side": list(SPY_COSTS_BPS),
                    "stock_costs_bps_per_side": list(STOCK_COSTS_BPS),
                    "primary_cost_bps": PRIMARY_COST_BPS,
                    "terminal_sensitivity_fallback": -0.30,
                    "measured_spread": "63 XNYS sessions; >=20 valid CS days; half full spread; no fill",
                },
                "audit": audit,
                "verdicts": verdicts,
                "main_metrics": _json_records(_main_table(metrics)),
                "spread_summary": _json_records(spread_summary),
            }
            json_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            markdown_path.write_text(
                "\n".join(
                    [
                        f"# Wave 15 market-regime overlay v2: {STABILITY_START} to {observed_end}",
                        "",
                        "Month-end signals take effect on the next XNYS return. Cash earns DTB3; "
                        "stock turnover uses drifted pretrade weights.",
                        "",
                        "## Frozen verdicts",
                        "",
                        *[
                            f"- `{rule}`: {'PASS' if verdicts[rule] else 'FAIL'}"
                            for rule in RULES
                        ],
                        "",
                        "## Primary decision cells",
                        "",
                        _markdown_table(_main_table(metrics).round(6), include_index=False),
                        "",
                        "## Crisis years at primary cost",
                        "",
                        _markdown_table(crises.round(6), include_index=False),
                        "",
                        "## Measured spread coverage (diagnostic only)",
                        "",
                        _markdown_table(spread_summary.round(6), include_index=False),
                        "",
                        "## Audit",
                        "",
                        "```json",
                        json.dumps(audit, ensure_ascii=False, indent=2),
                        "```",
                        "",
                        f"Structured summary: `{json_path.name}`",
                        f"Daily paths: `{daily_path.name}`",
                        f"Month-end signals: `{signal_path.name}`",
                        f"Fixed-cost metrics: `{metrics_path.name}`",
                        f"Delisting sensitivity: `{sensitivity_path.name}`",
                        f"Measured trades: `{trade_path.name}`",
                    ]
                ),
                encoding="utf-8",
            )
            params = {
                "study_version": STUDY_VERSION,
                "calendar": "trading_calendars XNYS; month-end close; next-session effect",
                "assets": list(ASSETS),
                "primary_cost_bps": PRIMARY_COST_BPS,
                "cash": "FRED DTB3 actual/360",
                "stock_universe": "PIT CS eligible; $3 and 63d median dollar volume $2m",
                "terminal": "realized delisting returns; unresolved frozen; -30% sensitivity",
                "stability_start": str(STABILITY_START),
                "stability_end": str(STABILITY_END),
            }
            for rule in RULES:
                append_study(
                    study="market_regime_overlay",
                    factor_name=rule,
                    verdict=verdicts[rule],
                    criteria=(
                        "All four asset x sample cells at primary cost require drawdown "
                        "improvement >=10pp, Sharpe improvement >=0.10, and CAGR loss <=2pp"
                    ),
                    params={**params, "rule": rule},
                    eval_start=PRIMARY_START,
                    eval_end=observed_end,
                    report_path=str(markdown_path),
                    criterion_values=_criterion_values(metrics, rule),
                )
        return {
            "observed_end": observed_end,
            "verdicts": verdicts,
            "metrics": metrics,
            "sensitivity_metrics": sensitivity_metrics,
            "crises": crises,
            "spread_summary": spread_summary,
            "audit": audit,
            "json_path": json_path,
            "markdown_path": markdown_path,
            "daily_path": daily_path,
        }
    finally:
        progress.done()


def main() -> int:
    result = run_study()
    print(_markdown_table(_main_table(result["metrics"]).round(6), include_index=False), flush=True)
    for rule, verdict in result["verdicts"].items():
        print(f"{rule}: {'PASS' if verdict else 'FAIL'}", flush=True)
    print(f"report: {result['markdown_path']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
