from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests
from loguru import logger
from requests import HTTPError

from data_sources.base import DataSourceInterface
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import MASSIVE_BASE_URL, iter_chunks, is_supported_us_security_type

_DIVIDEND_QUANT = Decimal("1.0000000000")
_SPLIT_QUANT = Decimal("1.0000000000")


def _parse_date(value: Optional[Any]) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    raw = value[:10]
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _normalize_lookup_date(value: Optional[Any]) -> Optional[str]:
    parsed = _parse_date(value)
    if parsed:
        return parsed.isoformat()
    if isinstance(value, str) and value:
        return value[:10]
    return None


def normalize_volume_value(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))
    except Exception:
        return None


class MassiveSource(DataSourceInterface):
    def __init__(
        self,
        rate_limiter: KeyRateLimiter,
        session: Optional[requests.Session] = None,
        base_url: str = MASSIVE_BASE_URL,
        timeout: int = 20,
    ):
        self.rate_limiter = rate_limiter
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _prepare_request(
        self,
        path: Optional[str] = None,
        url: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> tuple[str, Optional[dict[str, Any]]]:
        api_key = self.rate_limiter.acquire_key()
        if url:
            parsed = urlparse(url)
            query_items = parse_qsl(parsed.query, keep_blank_values=True)
            if not any(key.lower() == "apikey" for key, _ in query_items):
                query_items.append(("apiKey", api_key))
            final_url = urlunparse(parsed._replace(query=urlencode(query_items, doseq=True)))
            return final_url, None

        final_params = dict(params or {})
        final_params.setdefault("apiKey", api_key)
        return f"{self.base_url}{path}", final_params

    def _request_json(
        self,
        path: Optional[str] = None,
        url: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
        allow_404: bool = False,
    ) -> Optional[dict[str, Any]]:
        request_url, request_params = self._prepare_request(path=path, url=url, params=params)
        response = self.session.get(request_url, params=request_params, timeout=self.timeout)
        if allow_404 and response.status_code == 404:
            return None
        try:
            response.raise_for_status()
        except HTTPError:
            logger.error("Massive 请求失败: {} {}", response.status_code, response.text[:500])
            raise
        payload = response.json()
        if isinstance(payload, dict) and payload.get("status") == "ERROR":
            raise RuntimeError(f"Massive 返回错误: {payload}")
        return payload

    def _paginate_results(
        self,
        path: str,
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        all_results: list[dict[str, Any]] = []
        next_url: Optional[str] = None
        while True:
            payload = self._request_json(path=path if next_url is None else None, url=next_url, params=params if next_url is None else None)
            if not payload:
                break
            results = payload.get("results") or []
            if isinstance(results, dict):
                all_results.append(results)
            else:
                all_results.extend(results)
            next_url = payload.get("next_url")
            if not next_url:
                break
        return all_results

    def _build_reference_payload(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": item["ticker"].lower(),
            "is_active": item.get("active"),
            "name": item.get("name"),
            "exchange": item.get("primary_exchange"),
            "currency": (item.get("currency_name") or "").upper() or None,
            "market": (item.get("locale") or "").upper() or None,
            "type": item.get("type"),
            "list_date": _parse_date(item.get("list_date")),
            "delist_date": _parse_date(item.get("delisted_utc")),
            "cik": item.get("cik"),
            "composite_figi": item.get("composite_figi"),
            "share_class_figi": item.get("share_class_figi"),
        }

    def _build_overview_payload(self, symbol: str, item: dict[str, Any]) -> dict[str, Any]:
        address = item.get("address") or {}
        branding = item.get("branding") or {}
        payload = {
            "symbol": symbol.lower(),
            "is_active": item.get("active"),
            "name": item.get("name"),
            "exchange": item.get("primary_exchange"),
            "currency": (item.get("currency_name") or "").upper() or None,
            "market": (item.get("locale") or "").upper() or None,
            "type": item.get("type"),
            "list_date": _parse_date(item.get("list_date")),
            "delist_date": _parse_date(item.get("delisted_utc")),
            "cik": item.get("cik"),
            "composite_figi": item.get("composite_figi"),
            "share_class_figi": item.get("share_class_figi"),
            "market_cap": item.get("market_cap"),
            "phone_number": item.get("phone_number"),
            "description": item.get("description"),
            "homepage_url": item.get("homepage_url"),
            "total_employees": item.get("total_employees"),
            "sic_code": item.get("sic_code"),
            "industry": item.get("sic_description"),
            "address_line1": address.get("address1"),
            "city": address.get("city"),
            "state": address.get("state"),
            "postal_code": address.get("postal_code"),
            "logo_url": branding.get("logo_url"),
            "icon_url": branding.get("icon_url"),
        }
        return {key: value for key, value in payload.items() if value is not None or key == "symbol"}

    def list_active_tickers(
        self,
        market: str = "stocks",
        allowed_types: Optional[Iterable[str]] = None,
        limit: int = 1000,
        locale: Optional[str] = "us",
    ) -> list[dict[str, Any]]:
        params = {
            "market": market,
            "active": "true",
            "sort": "ticker",
            "order": "asc",
            "limit": min(limit, 1000),
        }
        allowed = {item.upper() for item in allowed_types or []}
        results = self._paginate_results("/v3/reference/tickers", params=params)
        if locale:
            locale_upper = locale.upper()
            results = [item for item in results if (item.get("locale") or "").upper() == locale_upper]
        if not allowed:
            return results
        return [item for item in results if is_supported_us_security_type(item.get("type")) and (item.get("type") or "").upper() in allowed]

    def get_ticker_overview(self, symbol: str, lookup_date: Optional[Any] = None, allow_missing: bool = False) -> Optional[dict[str, Any]]:
        params: dict[str, Any] = {}
        lookup_date_str = _normalize_lookup_date(lookup_date)
        if lookup_date_str:
            params["date"] = lookup_date_str
        payload = self._request_json(
            path=f"/v3/reference/tickers/{symbol.upper()}",
            params=params or None,
            allow_404=allow_missing,
        )
        if not payload:
            return None
        return payload.get("results")

    def get_security_info(self, symbol: str, fallback_date: Optional[Any] = None) -> Optional[dict]:
        details = self.get_ticker_overview(symbol, allow_missing=True)
        if details:
            return self._build_overview_payload(symbol, details)

        fallback_date_str = _normalize_lookup_date(fallback_date)
        if not fallback_date_str:
            return None

        details = self.get_ticker_overview(symbol, lookup_date=fallback_date_str, allow_missing=True)
        if not details:
            return None

        payload = self._build_overview_payload(symbol, details)
        payload["is_active"] = False
        return payload

    def get_historical_data(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        adjusted: bool = False,
    ) -> pd.DataFrame:
        if not start or not end:
            raise ValueError("Massive 历史价格请求必须提供 start 和 end 日期。")

        path = f"/v2/aggs/ticker/{symbol.upper()}/range/1/day/{start}/{end}"
        params = {
            "adjusted": str(adjusted).lower(),
            "sort": "asc",
            "limit": 50000,
        }
        results = self._paginate_results(path, params=params)
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        df["Date"] = (
            pd.to_datetime(df["t"], unit="ms", utc=True)
            .dt.tz_convert("America/New_York")
            .dt.date
        )
        df.set_index("Date", inplace=True)
        df.rename(
            columns={
                "o": "Open",
                "h": "High",
                "l": "Low",
                "c": "Close",
                "v": "Volume",
                "vw": "vwap",
            },
            inplace=True,
        )
        if "vwap" not in df.columns:
            df["vwap"] = None
        df["Volume"] = df["Volume"].apply(normalize_volume_value)
        df["turnover"] = None
        valid_turnover = df["Volume"].notna() & df["vwap"].notna()
        df.loc[valid_turnover, "turnover"] = df.loc[valid_turnover, "Volume"] * df.loc[valid_turnover, "vwap"]
        df["Dividends"] = 0.0
        df["Stock Splits"] = 0.0
        return df[["Open", "High", "Low", "Close", "Volume", "vwap", "turnover", "Dividends", "Stock Splits"]]

    def get_grouped_daily_data(self, target_date: str, adjusted: bool = False, include_otc: bool = False) -> list[Dict[str, Any]]:
        params = {
            "adjusted": str(adjusted).lower(),
            "include_otc": str(include_otc).lower(),
        }
        payload = self._request_json(
            path=f"/v2/aggs/grouped/locale/us/market/stocks/{target_date}",
            params=params,
            allow_404=True,
        )
        if not payload:
            return []
        return payload.get("results") or []

    def get_dividends_batch(self, symbols: list[str], start_date: Optional[str] = None, chunk_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params: dict[str, Any] = {
                "ticker.any_of": ",".join(chunk),
                "limit": 5000,
                "sort": "ex_dividend_date.asc",
            }
            if start_date:
                params["ex_dividend_date.gte"] = start_date
            for item in self._paginate_results("/stocks/v1/dividends", params=params):
                cash_amount_raw = item.get("cash_amount")
                cash_amount = None
                if cash_amount_raw is not None:
                    cash_amount = Decimal(str(cash_amount_raw)).quantize(_DIVIDEND_QUANT)
                records.append(
                    {
                        "ticker": (item.get("ticker") or "").lower(),
                        "ex_dividend_date": _parse_date(item.get("ex_dividend_date")),
                        "declaration_date": _parse_date(item.get("declaration_date")),
                        "record_date": _parse_date(item.get("record_date")),
                        "pay_date": _parse_date(item.get("pay_date")),
                        "cash_amount": cash_amount,
                        "currency": (item.get("currency") or "").upper() or None,
                        "frequency": item.get("frequency"),
                    }
                )
        return [item for item in records if item["ticker"] and item["ex_dividend_date"] and item["cash_amount"] is not None]

    def get_dividends(self, symbol: str, start_date: Optional[str] = None) -> list[dict[str, Any]]:
        records = self.get_dividends_batch([symbol], start_date=start_date)
        for item in records:
            item.pop("ticker", None)
        return records

    def get_splits_batch(self, symbols: list[str], start_date: Optional[str] = None, chunk_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params: dict[str, Any] = {
                "ticker.any_of": ",".join(chunk),
                "limit": 5000,
                "sort": "execution_date.asc",
            }
            if start_date:
                params["execution_date.gte"] = start_date
            for item in self._paginate_results("/stocks/v1/splits", params=params):
                split_to_raw = item.get("split_to")
                split_from_raw = item.get("split_from")
                records.append(
                    {
                        "ticker": (item.get("ticker") or "").lower(),
                        "execution_date": _parse_date(item.get("execution_date")),
                        "declaration_date": None,
                        "split_to": Decimal(str(split_to_raw)).quantize(_SPLIT_QUANT) if split_to_raw is not None else None,
                        "split_from": Decimal(str(split_from_raw)).quantize(_SPLIT_QUANT) if split_from_raw is not None else None,
                    }
                )
        return [
            item
            for item in records
            if item["ticker"] and item["execution_date"] and item["split_to"] is not None and item["split_from"] is not None
        ]

    def get_splits(self, symbol: str, start_date: Optional[str] = None) -> list[dict[str, Any]]:
        records = self.get_splits_batch([symbol], start_date=start_date)
        for item in records:
            item.pop("ticker", None)
        return records

    def get_latest_floats_batch(self, symbols: list[str], chunk_size: int = 100) -> dict[str, dict[str, Any]]:
        by_symbol: dict[str, dict[str, Any]] = {}
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params = {
                "ticker.any_of": ",".join(chunk),
                "limit": 5000,
                "sort": "ticker.asc,effective_date.desc",
            }
            for item in self._paginate_results("/stocks/vX/float", params=params):
                ticker = (item.get("ticker") or "").lower()
                if not ticker:
                    continue
                effective_date = _parse_date(item.get("effective_date"))
                current = by_symbol.get(ticker)
                if current and current.get("effective_date") and (
                    effective_date is None or current["effective_date"] >= effective_date
                ):
                    continue
                by_symbol[ticker] = {
                    "effective_date": effective_date,
                    "free_float": item.get("free_float"),
                    "free_float_percent": item.get("free_float_percent"),
                }
        return by_symbol
