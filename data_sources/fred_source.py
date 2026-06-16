"""FRED JSON API adapter for public reference series."""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal, InvalidOperation

import requests

DEFAULT_SERIES_ID = "DTB3"
_FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
_API_KEY_ENV = "FRED_API_KEY"
_DEFAULT_TIMEOUT = 60


def fetch_fred_series(
    series_id: str = DEFAULT_SERIES_ID,
    *,
    since: date | None = None,
    session: requests.Session | None = None,
) -> list[dict]:
    """从 FRED JSON API 拉取 series observations 并返回 risk_free_rates 行。"""
    api_key = os.environ.get(_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(
            f"环境变量 {_API_KEY_ENV} 未设置；FRED JSON API 需要 key，"
            "申请地址 https://fredaccount.stlouisfed.org/apikeys。"
        )
    normalized = series_id.upper()
    params = {
        "series_id": normalized,
        "api_key": api_key,
        "file_type": "json",
    }
    if since is not None:
        params["observation_start"] = since.isoformat()
    http = session or requests
    response = http.get(
        _FRED_API_URL,
        params=params,
        timeout=_DEFAULT_TIMEOUT,
        headers={"User-Agent": "stock-pipeline fred sync"},
    )
    response.raise_for_status()
    return parse_fred_observations(response.json(), series_id=normalized)


def parse_fred_observations(payload: dict, *, series_id: str = DEFAULT_SERIES_ID) -> list[dict]:
    """解析 FRED observations JSON。'.' 缺失值跳过，rate_pct 保留原始百分比。"""
    normalized = series_id.upper()
    observations = payload.get("observations")
    if observations is None:
        raise ValueError(f"FRED JSON missing 'observations' for {normalized}")
    rows = []
    for record in observations:
        date_text = (record.get("date") or "").strip()
        if not date_text:
            raise ValueError(f"FRED JSON {normalized} row missing observation date")
        try:
            rate_date = date.fromisoformat(date_text)
        except ValueError as exc:
            raise ValueError(f"FRED JSON {normalized} invalid observation date {date_text!r}") from exc
        value = (record.get("value") or "").strip()
        if not value or value == ".":
            continue
        try:
            rate_pct = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError(f"FRED JSON {normalized} invalid rate {value!r} for {rate_date}") from exc
        rows.append({"date": rate_date, "series_id": normalized, "rate_pct": rate_pct})
    if not rows:
        raise ValueError(f"FRED JSON contained no {normalized} rows")
    return rows
