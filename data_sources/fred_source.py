"""FRED CSV adapters for public reference series."""
from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

import requests

DEFAULT_SERIES_ID = "DTB3"
_FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
_DEFAULT_TIMEOUT = 60


def fetch_fred_series(
    series_id: str = DEFAULT_SERIES_ID,
    *,
    since: date | None = None,
    session: requests.Session | None = None,
) -> list[dict]:
    """下载 FRED public CSV 并返回 risk_free_rates 行。"""
    normalized = series_id.upper()
    http = session or requests
    response = http.get(
        _FRED_CSV_URL.format(series_id=normalized),
        timeout=_DEFAULT_TIMEOUT,
        headers={"User-Agent": "stock-pipeline fred sync"},
    )
    response.raise_for_status()
    return parse_fred_rate_csv(response.text, series_id=normalized, since=since)


def parse_fred_rate_csv(csv_text: str, *, series_id: str = DEFAULT_SERIES_ID, since: date | None = None) -> list[dict]:
    """解析 FRED two-column CSV。'.' 缺失值跳过，rate_pct 保留原始百分比。"""
    normalized = series_id.upper()
    rows = []
    reader = csv.DictReader(io.StringIO(csv_text))
    fieldnames = reader.fieldnames or []
    date_column = "observation_date" if "observation_date" in fieldnames else "DATE" if "DATE" in fieldnames else None
    if date_column is None:
        raise ValueError("FRED CSV missing observation_date/DATE column")
    series_column = normalized if normalized in fieldnames else series_id if series_id in fieldnames else None
    if series_column is None:
        raise ValueError(f"FRED CSV missing {normalized} column")
    for record in reader:
        date_text = (record.get(date_column) or "").strip()
        if not date_text:
            raise ValueError("FRED CSV row missing observation date")
        try:
            rate_date = date.fromisoformat(date_text)
        except ValueError as exc:
            raise ValueError(f"FRED CSV invalid observation date {date_text!r}") from exc
        if since and rate_date < since:
            continue
        value = (record.get(series_column) or "").strip()
        if not value or value == ".":
            continue
        try:
            rate_pct = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"FRED CSV invalid {normalized} rate {value!r} for {rate_date}") from exc
        rows.append({"date": rate_date, "series_id": normalized, "rate_pct": rate_pct})
    if not rows:
        raise ValueError(f"FRED CSV contained no {normalized} rows after filters")
    return rows
