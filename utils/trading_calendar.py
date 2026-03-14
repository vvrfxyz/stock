from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from loguru import logger

try:
    import exchange_calendars as xc
    import pandas as pd
except ImportError:  # pragma: no cover
    xc = None  # type: ignore[assignment]
    pd = None  # type: ignore[assignment]


_MARKET_TO_CALENDAR = {
    "US": "XNYS",
    "HK": "XHKG",
    "CNA": "XSHG",
}

_MARKET_CLOSE_CONFIG = {
    "US": (ZoneInfo("America/New_York"), time(hour=16, minute=0)),
    "HK": (ZoneInfo("Asia/Hong_Kong"), time(hour=16, minute=0)),
    "CNA": (ZoneInfo("Asia/Shanghai"), time(hour=15, minute=0)),
}

_FALLBACK_WARNING_EMITTED = False


def _warn_fallback_once(reason: str) -> None:
    global _FALLBACK_WARNING_EMITTED
    if _FALLBACK_WARNING_EMITTED:
        return
    logger.warning(
        "交易日历依赖不可用，已降级为工作日规则: {}。节假日可能不准确，但脚本会继续执行。",
        reason,
    )
    _FALLBACK_WARNING_EMITTED = True


def _require_supported_market(market: str) -> str:
    market_upper = (market or "").upper()
    if market_upper not in _MARKET_TO_CALENDAR:
        raise ValueError(f"Unsupported market: {market!r}. Supported: {sorted(_MARKET_TO_CALENDAR)}")
    return market_upper


def _is_weekday(session_date: date) -> bool:
    return session_date.weekday() < 5


def _previous_weekday(session_date: date) -> date:
    current = session_date
    while not _is_weekday(current):
        current -= timedelta(days=1)
    return current


def _shift_weekdays(session_date: date, sessions: int) -> date:
    current = session_date
    step = 1 if sessions > 0 else -1
    remaining = abs(sessions)
    while remaining:
        current += timedelta(days=step)
        if _is_weekday(current):
            remaining -= 1
    return current


def _get_market_close_datetime(market: str, now: datetime) -> tuple[datetime, date]:
    market_upper = _require_supported_market(market)
    market_tz, close_time = _MARKET_CLOSE_CONFIG[market_upper]
    market_now = now.astimezone(market_tz)
    close_dt = datetime.combine(market_now.date(), close_time, tzinfo=market_tz)
    return close_dt, market_now.date()


def _get_last_completed_trading_date_fallback(market: str, now: datetime) -> date:
    close_dt, market_date = _get_market_close_datetime(market, now)
    candidate = market_date if now.astimezone(close_dt.tzinfo) > close_dt else market_date - timedelta(days=1)
    return _previous_weekday(candidate)


@lru_cache(maxsize=None)
def _get_calendar(market: str):
    market_upper = _require_supported_market(market)
    calendar_name = _MARKET_TO_CALENDAR[market_upper]
    if xc is None:  # pragma: no cover
        raise RuntimeError("exchange_calendars is unavailable")
    return xc.get_calendar(calendar_name)


def get_last_completed_trading_date(market: str, now: datetime | None = None) -> date:
    """
    Return the most recent trading session date whose market close time is <= `now`.

    Notes:
    - This is intentionally *close-aware* (not just a weekday/holiday check) to avoid
      treating today's session as complete when it's still in progress.
    - `now` is interpreted in UTC if naive.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    if xc is None or pd is None:  # pragma: no cover
        missing = []
        if xc is None:
            missing.append("exchange_calendars")
        if pd is None:
            missing.append("pandas")
        _warn_fallback_once(", ".join(missing))
        return _get_last_completed_trading_date_fallback(market, now)

    calendar = _get_calendar(market)

    now_ts = pd.Timestamp(now)
    session = calendar.date_to_session(now_ts.date(), direction="previous")
    session_close = calendar.session_close(session)

    if now_ts <= session_close:
        session = calendar.previous_session(session)

    return session.date()


def shift_trading_date(market: str, session_date: date, sessions: int) -> date:
    """Shift a trading session date by `sessions` sessions (negative for previous)."""
    if sessions == 0:
        return session_date

    if xc is None or pd is None:  # pragma: no cover
        missing = []
        if xc is None:
            missing.append("exchange_calendars")
        if pd is None:
            missing.append("pandas")
        _warn_fallback_once(", ".join(missing))
        return _shift_weekdays(session_date, sessions)

    calendar = _get_calendar(market)
    session_label = pd.Timestamp(session_date)
    shifted = calendar.session_offset(session_label, sessions)
    return shifted.date()


def describe_trading_date(market: str, session_date: date) -> str:
    """Human-friendly debug string for logs."""
    try:
        if xc is None or pd is None:  # pragma: no cover
            return f"{session_date.isoformat()} ({market}, fallback=weekday-only)"
        calendar = _get_calendar(market)
        label = pd.Timestamp(session_date)
        open_time, close_time = calendar.session_open_close(label)
        return f"{session_date.isoformat()} ({market}, open={open_time}, close={close_time})"
    except Exception as exc:  # pragma: no cover
        logger.debug(f"Failed to describe trading date {session_date} for market={market!r}: {exc}")
        return f"{session_date.isoformat()} ({market})"
