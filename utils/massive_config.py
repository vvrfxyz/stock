from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Iterable, TypeVar

T = TypeVar("T")

ALLOWED_US_SECURITY_TYPES = ("CS", "ETF", "ADRC")
MASSIVE_RATE_LIMIT = 5
MASSIVE_RATE_SECONDS = 60
MASSIVE_FREE_HISTORY_DAYS = 730
MASSIVE_BASE_URL = "https://api.massive.com"


def get_massive_api_keys() -> list[str]:
    raw = os.getenv("MASSIVE_API_KEYS") or os.getenv("POLYGON_API_KEYS")
    if not raw:
        raise ValueError("环境变量 MASSIVE_API_KEYS 未设置（兼容读取 POLYGON_API_KEYS）。")
    keys = [item.strip() for item in raw.split(",") if item.strip()]
    if not keys:
        raise ValueError("MASSIVE_API_KEYS 为空。")
    return keys


def get_massive_history_floor(end_date: date) -> date:
    return end_date - timedelta(days=MASSIVE_FREE_HISTORY_DAYS)


def normalize_security_type(type_code: str | None) -> str:
    return (type_code or "").upper()


def is_supported_us_security_type(type_code: str | None) -> bool:
    return normalize_security_type(type_code) in ALLOWED_US_SECURITY_TYPES


def normalize_market(market: str | None) -> str:
    return (market or "US").upper()


def enforce_us_market(market: str | None) -> str:
    market_upper = normalize_market(market)
    if market_upper != "US":
        raise ValueError(f"Massive 免费层重构当前仅支持 US 市场，收到: {market_upper}")
    return market_upper


def iter_chunks(items: Iterable[T], chunk_size: int) -> list[list[T]]:
    chunk: list[T] = []
    chunks: list[list[T]] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            chunks.append(chunk)
            chunk = []
    if chunk:
        chunks.append(chunk)
    return chunks


def get_quarter_snapshot_dates(start_date: date, end_date: date) -> list[date]:
    if start_date > end_date:
        return []

    dates: list[date] = []
    year = start_date.year
    quarter_ends = ((3, 31), (6, 30), (9, 30), (12, 31))

    while year <= end_date.year:
        for month, day in quarter_ends:
            snapshot = date(year, month, day)
            if start_date <= snapshot <= end_date:
                dates.append(snapshot)
        year += 1

    if end_date not in dates:
        dates.append(end_date)
    return sorted(set(dates))
