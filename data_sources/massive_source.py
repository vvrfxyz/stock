from __future__ import annotations

import threading
import time
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd
import requests
from loguru import logger
from requests import HTTPError
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException

from data_sources.base import DataSourceInterface
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import MASSIVE_BASE_URL, iter_chunks, is_supported_us_security_type
from utils.secret_masking import (
    mask_api_key_in_url as _mask_api_key_in_url,
    mask_api_keys_in_text as _mask_api_keys_in_text,
)

_DIVIDEND_QUANT = Decimal("1.0000000000")
_SPLIT_QUANT = Decimal("1.0000000000")
_TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_PG_BIGINT_MIN = -(2 ** 63)
_PG_BIGINT_MAX = 2 ** 63 - 1


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


def _parse_timestamp(value: Optional[Any]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


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
        normalized = int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))
    except Exception:
        return None
    if normalized < _PG_BIGINT_MIN or normalized > _PG_BIGINT_MAX:
        return None
    return normalized


def normalize_bigint_value(value: Optional[Any]) -> Optional[int]:
    return normalize_volume_value(value)


class MassiveSource(DataSourceInterface):
    def __init__(
        self,
        rate_limiter: KeyRateLimiter,
        session: Optional[requests.Session] = None,
        base_url: str = MASSIVE_BASE_URL,
        timeout: int = 20,
        max_retries: int = 2,
        retry_backoff_seconds: float = 1.0,
        pool_size: int = 32,
    ):
        self.rate_limiter = rate_limiter
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max(0, max_retries)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.pool_size = max(1, pool_size)
        self._thread_local = threading.local()
        self._owned_sessions: list[requests.Session] = []
        self._owned_sessions_lock = threading.Lock()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=self.pool_size, pool_maxsize=self.pool_size)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _get_session(self):
        if self.session is not None:
            return self.session

        current = getattr(self._thread_local, "session", None)
        if current is None:
            current = self._create_session()
            self._thread_local.session = current
            with self._owned_sessions_lock:
                self._owned_sessions.append(current)
        return current

    def _reset_current_thread_session(self) -> None:
        if self.session is not None:
            return

        current = getattr(self._thread_local, "session", None)
        if current is None:
            return

        close = getattr(current, "close", None)
        if callable(close):
            close()

        self._thread_local.session = None
        with self._owned_sessions_lock:
            self._owned_sessions = [session for session in self._owned_sessions if session is not current]

    def _get_retry_delay(self, attempt: int, response: Optional[Any] = None) -> float:
        retry_after = None
        headers = getattr(response, "headers", None) or {}
        raw_retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw_retry_after:
            try:
                retry_after = float(raw_retry_after)
            except (TypeError, ValueError):
                retry_after = None

        if retry_after is not None:
            return max(0.0, retry_after)
        return self.retry_backoff_seconds * (2 ** attempt)

    def close(self) -> None:
        if self.session is not None:
            close = getattr(self.session, "close", None)
            if callable(close):
                close()
            return

        with self._owned_sessions_lock:
            owned_sessions = self._owned_sessions
            self._owned_sessions = []

        for current in owned_sessions:
            close = getattr(current, "close", None)
            if callable(close):
                close()

    def _prepare_request(
        self,
        path: Optional[str] = None,
        url: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> tuple[str, Optional[dict[str, Any]], str]:
        api_key = self.rate_limiter.acquire_key()
        if url:
            parsed = urlparse(url)
            query_items = [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key.lower() != "apikey"
            ]
            query_items.append(("apiKey", api_key))
            final_url = urlunparse(parsed._replace(query=urlencode(query_items, doseq=True)))
            return final_url, None, api_key

        final_params = dict(params or {})
        final_params.setdefault("apiKey", api_key)
        return f"{self.base_url}{path}", final_params, api_key

    def _request_json(
        self,
        path: Optional[str] = None,
        url: Optional[str] = None,
        params: Optional[dict[str, Any]] = None,
        allow_404: bool = False,
    ) -> Optional[dict[str, Any]]:
        request_label = _mask_api_key_in_url(url) or path or "<unknown>"
        for attempt in range(self.max_retries + 1):
            request_url, request_params, api_key = self._prepare_request(path=path, url=url, params=params)
            response = None
            try:
                response = self._get_session().get(request_url, params=request_params, timeout=self.timeout)
            except RequestException as exc:
                if attempt >= self.max_retries:
                    # 原始异常消息可能含明文 apiKey；掩码后重抛并用 from None
                    # 切断异常链，防止 traceback 渲染把未脱敏消息带进日志。
                    raise RuntimeError(
                        f"Massive 请求网络异常: {request_label} - {_mask_api_keys_in_text(exc)}"
                    ) from None
                delay = self._get_retry_delay(attempt)
                logger.warning(
                    "Massive 请求网络异常，{:.1f} 秒后重试({}/{}): {} - {}",
                    delay,
                    attempt + 1,
                    self.max_retries,
                    request_label,
                    _mask_api_keys_in_text(exc),
                )
                self._reset_current_thread_session()
                time.sleep(delay)
                continue

            try:
                if allow_404 and response.status_code == 404:
                    return None

                if response.status_code in _TRANSIENT_STATUS_CODES and attempt < self.max_retries:
                    delay = self._get_retry_delay(attempt, response=response)
                    if response.status_code == 429:
                        headers = getattr(response, "headers", None) or {}
                        has_retry_after = "Retry-After" in headers
                        # 429 通常是 key 的 RPM 被打满。
                        # 这里把当前 key 先 block 掉，让下一次尝试尽量换一个 key；
                        # 没有 Retry-After 时，对 429 更保守一些，直接按窗口长度 block。
                        block_seconds = delay if delay > 0 else float(getattr(self.rate_limiter, "per_seconds", 60))
                        if delay <= 0 or (not has_retry_after and delay < 5):
                            block_seconds = float(getattr(self.rate_limiter, "per_seconds", 60))
                        block_key = getattr(self.rate_limiter, "block_key", None)
                        if callable(block_key):
                            block_key(api_key, block_seconds)
                        if has_retry_after:
                            block_all = getattr(self.rate_limiter, "block_all", None)
                            if callable(block_all):
                                block_all(delay)
                    logger.warning(
                        "Massive 返回临时错误 {}，{:.1f} 秒后重试({}/{}): {}",
                        response.status_code,
                        delay,
                        attempt + 1,
                        self.max_retries,
                        request_label,
                    )
                    # 429 场景下，已经 block 了 key；此处无需长时间 sleep，让请求尽快切到可用 key。
                    if response.status_code == 429:
                        if has_retry_after:
                            time.sleep(delay)
                        else:
                            time.sleep(min(delay, 1.0))
                    else:
                        time.sleep(delay)
                    continue

                # 避免把 apiKey 写入异常信息/日志（requests 的 HTTPError 会拼接 response.url）。
                raw_response_url = getattr(response, "url", None) or request_url
                response.url = _mask_api_key_in_url(raw_response_url) or raw_response_url
                response.raise_for_status()

                payload = response.json()
                if isinstance(payload, dict) and payload.get("status") == "ERROR":
                    raise RuntimeError(f"Massive 返回错误: {payload}")
                return payload
            except HTTPError:
                logger.error("Massive 请求失败: {} {}", response.status_code, _mask_api_keys_in_text(response.text)[:500])
                raise
            finally:
                close = getattr(response, "close", None)
                if callable(close):
                    close()

        return None

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
        symbol = item["ticker"].lower()
        payload = {
            "symbol": symbol,
            "current_symbol": symbol,
            "is_active": item.get("active"),
            "name": item.get("name"),
            "exchange": item.get("primary_exchange"),
            "currency": (item.get("currency_name") or "").upper() or None,
            "currency_symbol": (item.get("currency_symbol") or "").upper() or None,
            "base_currency_name": item.get("base_currency_name"),
            "base_currency_symbol": (item.get("base_currency_symbol") or "").upper() or None,
            "market": (item.get("locale") or "").upper() or None,
            "vendor_market": item.get("market"),
            "locale": item.get("locale"),
            "type": item.get("type"),
            "list_date": _parse_date(item.get("list_date")),
            "delist_date": _parse_date(item.get("delisted_utc")),
            "vendor_last_updated_at": _parse_timestamp(item.get("last_updated_utc")),
            "cik": item.get("cik"),
            "composite_figi": item.get("composite_figi"),
            "share_class_figi": item.get("share_class_figi"),
        }
        # /v3/reference/tickers 列表响应不带 list_date 等字段：None 原样下发会让
        # 每日 universe 同步把 details 辛苦回填的值抹掉（2026-07-06 全舰队 list_date
        # 被抹事故，防回收 clamp 因此失效）。与 _build_overview_payload 同口径剥离 None。
        return {key: value for key, value in payload.items() if value is not None or key == "symbol"}

    def _build_overview_payload(self, symbol: str, item: dict[str, Any]) -> dict[str, Any]:
        address = item.get("address") or {}
        branding = item.get("branding") or {}
        normalized_symbol = symbol.lower()
        payload = {
            "symbol": normalized_symbol,
            "current_symbol": normalized_symbol,
            "is_active": item.get("active"),
            "name": item.get("name"),
            "exchange": item.get("primary_exchange"),
            "currency": (item.get("currency_name") or "").upper() or None,
            "currency_symbol": (item.get("currency_symbol") or "").upper() or None,
            "base_currency_name": item.get("base_currency_name"),
            "base_currency_symbol": (item.get("base_currency_symbol") or "").upper() or None,
            "market": (item.get("locale") or "").upper() or None,
            "vendor_market": item.get("market"),
            "locale": item.get("locale"),
            "type": item.get("type"),
            "list_date": _parse_date(item.get("list_date")),
            "delist_date": _parse_date(item.get("delisted_utc")),
            "cik": item.get("cik"),
            "composite_figi": item.get("composite_figi"),
            "share_class_figi": item.get("share_class_figi"),
            "ticker_root": item.get("ticker_root"),
            "ticker_suffix": item.get("ticker_suffix"),
            "round_lot": normalize_bigint_value(item.get("round_lot")),
            "share_class_shares_outstanding": normalize_bigint_value(item.get("share_class_shares_outstanding")),
            "weighted_shares_outstanding": normalize_bigint_value(item.get("weighted_shares_outstanding")),
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

    def list_delisted_tickers(
        self,
        market: str = "stocks",
        limit: int = 1000,
        locale: Optional[str] = "us",
    ) -> list[dict[str, Any]]:
        # active=false 与 sort 参数组合会返回空结果（vendor 行为），退市名单不能排序
        params = {
            "market": market,
            "active": "false",
            "limit": min(limit, 1000),
        }
        results = self._paginate_results("/v3/reference/tickers", params=params)
        if locale:
            locale_upper = locale.upper()
            results = [item for item in results if (item.get("locale") or "").upper() == locale_upper]
        return results

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
                "n": "trade_count",
            },
            inplace=True,
        )
        if "vwap" not in df.columns:
            df["vwap"] = None
        if "trade_count" not in df.columns:
            df["trade_count"] = None
        if "otc" not in df.columns:
            df["otc"] = None
        df["Volume"] = df["Volume"].apply(normalize_volume_value)
        df["trade_count"] = df["trade_count"].apply(normalize_volume_value)
        return df[["Open", "High", "Low", "Close", "Volume", "vwap", "trade_count", "otc"]]

    def get_minute_aggs(
        self,
        symbol: str,
        start: str,
        end: str,
    ) -> list[Dict[str, Any]]:
        """未复权 1 分钟聚合（含盘前盘后 04:00-20:00 ET），原样返回 vendor 行。

        50k 行/请求上限下，一个请求约可覆盖 52 个交易日（960 bar/日），
        更长窗口由 _paginate_results 翻页。行字段：t(ms epoch UTC)/o/h/l/c/v/vw/n。
        """
        if not start or not end:
            raise ValueError("Massive 分钟聚合请求必须提供 start 和 end 日期。")
        path = f"/v2/aggs/ticker/{symbol.upper()}/range/1/minute/{start}/{end}"
        params = {
            "adjusted": "false",
            "sort": "asc",
            "limit": 50000,
        }
        return self._paginate_results(path, params=params)

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

    def get_open_close_data(self, symbol: str, target_date: str, adjusted: bool = False) -> Optional[dict[str, Any]]:
        payload = self._request_json(
            path=f"/v1/open-close/{symbol.upper()}/{target_date}",
            params={"adjusted": str(adjusted).lower()},
            allow_404=True,
        )
        if not payload:
            return None
        return payload

    def get_ticker_events(self, symbol_or_id: str) -> dict[str, Any] | None:
        payload = self._request_json(
            path=f"/vX/reference/tickers/{symbol_or_id.upper()}/events",
            allow_404=True,
        )
        if not payload:
            return None
        return payload.get("results")

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
                        "source_event_id": item.get("id"),
                        "distribution_type": item.get("distribution_type"),
                        "historical_adjustment_factor": item.get("historical_adjustment_factor"),
                        "split_adjusted_cash_amount": item.get("split_adjusted_cash_amount"),
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
                        "source_event_id": item.get("id"),
                        "adjustment_type": item.get("adjustment_type"),
                        "historical_adjustment_factor": item.get("historical_adjustment_factor"),
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

    def get_float_batch(self, symbols: list[str], chunk_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params = {
                "ticker.any_of": ",".join(chunk),
                "limit": 5000,
                "sort": "ticker.asc,effective_date.desc",
            }
            for item in self._paginate_results("/stocks/vX/float", params=params):
                records.append(
                    {
                        "ticker": (item.get("ticker") or "").lower(),
                        "effective_date": _parse_date(item.get("effective_date")),
                        "free_float": normalize_bigint_value(item.get("free_float")),
                        "free_float_percent": item.get("free_float_percent"),
                    }
                )
        return [item for item in records if item["ticker"] and item["effective_date"] and item["free_float"] is not None]

    def get_short_interest_batch(self, symbols: list[str], start_date: Optional[str] = None, chunk_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params: dict[str, Any] = {
                "ticker.any_of": ",".join(chunk),
                "limit": 50000,
                "sort": "settlement_date.desc",
            }
            if start_date:
                params["settlement_date.gte"] = start_date
            for item in self._paginate_results("/stocks/v1/short-interest", params=params):
                records.append(
                    {
                        "ticker": (item.get("ticker") or "").lower(),
                        "settlement_date": _parse_date(item.get("settlement_date")),
                        "short_interest": normalize_bigint_value(item.get("short_interest")),
                        "avg_daily_volume": normalize_bigint_value(item.get("avg_daily_volume")),
                        "days_to_cover": item.get("days_to_cover"),
                    }
                )
        return [item for item in records if item["ticker"] and item["settlement_date"] and item["short_interest"] is not None]

    def get_short_volume_batch(self, symbols: list[str], start_date: Optional[str] = None, chunk_size: int = 100) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        volume_fields = [
            "short_volume",
            "total_volume",
            "exempt_volume",
            "non_exempt_volume",
            "adf_short_volume",
            "adf_short_volume_exempt",
            "nasdaq_carteret_short_volume",
            "nasdaq_carteret_short_volume_exempt",
            "nasdaq_chicago_short_volume",
            "nasdaq_chicago_short_volume_exempt",
            "nyse_short_volume",
            "nyse_short_volume_exempt",
        ]
        for chunk in iter_chunks([symbol.upper() for symbol in symbols if symbol], chunk_size):
            params: dict[str, Any] = {
                "ticker.any_of": ",".join(chunk),
                "limit": 50000,
                "sort": "date.desc",
            }
            if start_date:
                params["date.gte"] = start_date
            for item in self._paginate_results("/stocks/v1/short-volume", params=params):
                record = {
                    "ticker": (item.get("ticker") or "").lower(),
                    "date": _parse_date(item.get("date")),
                    "short_volume_ratio": item.get("short_volume_ratio"),
                }
                for field_name in volume_fields:
                    record[field_name] = normalize_bigint_value(item.get(field_name))
                records.append(record)
        return [item for item in records if item["ticker"] and item["date"] and item["short_volume"] is not None]

    def get_news(self, symbols: list[str], published_after: Optional[str] = None, limit: int = 1000) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for symbol in [symbol.upper() for symbol in symbols if symbol]:
            params: dict[str, Any] = {
                "ticker": symbol,
                "limit": min(max(limit, 1), 1000),
                "sort": "published_utc",
                "order": "desc",
            }
            if published_after:
                params["published_utc.gte"] = published_after
            for item in self._paginate_results("/v2/reference/news", params=params):
                publisher = item.get("publisher") or {}
                records.append(
                    {
                        "source_article_id": item.get("id"),
                        "published_utc": _parse_timestamp(item.get("published_utc")),
                        "title": item.get("title"),
                        "author": item.get("author"),
                        "description": item.get("description"),
                        "article_url": item.get("article_url"),
                        "amp_url": item.get("amp_url"),
                        "image_url": item.get("image_url"),
                        "publisher_name": publisher.get("name"),
                        "publisher_homepage_url": publisher.get("homepage_url"),
                        "publisher_logo_url": publisher.get("logo_url"),
                        "publisher_favicon_url": publisher.get("favicon_url"),
                        "tickers": [(ticker or "").lower() for ticker in item.get("tickers") or [] if ticker],
                        "keywords": item.get("keywords") or [],
                        "insights": item.get("insights") or [],
                    }
                )
        deduped = {
            item["source_article_id"]: item
            for item in records
            if item.get("source_article_id") and item.get("published_utc")
        }
        return list(deduped.values())
