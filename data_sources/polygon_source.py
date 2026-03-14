# data_sources/polygon_source.py (最终优化版)
import os
import json
import pandas as pd
import requests
from loguru import logger
from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime
from decimal import Decimal
from polygon import RESTClient
from polygon.exceptions import BadResponse
from requests.exceptions import HTTPError

from .base import DataSourceInterface
from utils.key_rate_limiter import KeyRateLimiter # 引入新的类

_DIVIDEND_QUANT = Decimal("1.0000000000")  # aligns with StockDividend.cash_amount scale=10
_SPLIT_QUANT = Decimal("1.0000000000")  # aligns with StockSplit split_to/split_from scale=10

def _parse_date_string(date_str: str) -> Optional[date]:
    """安全地将 YYYY-MM-DD 格式的字符串解析为 date 对象"""
    if not date_str: return None
    try:
        # 支持带'Z'的ISO 8601格式
        if 'Z' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
        return date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None


def _normalize_lookup_date(value: Optional[Any]) -> Optional[str]:
    """Normalize a date-like value into Polygon's YYYY-MM-DD query format."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        parsed = _parse_date_string(value)
        if parsed:
            return parsed.isoformat()
        return value[:10]
    return None


def _format_polygon_error(exc: Exception) -> str:
    """Normalize Polygon error payloads so logs stay readable and safe for Loguru."""
    message = str(exc)
    try:
        payload = json.loads(message)
    except (TypeError, ValueError):
        return message

    if isinstance(payload, dict):
        parts = []
        status = payload.get("status")
        error = payload.get("error")
        request_id = payload.get("request_id")
        if status:
            parts.append(f"status={status}")
        if error:
            parts.append(f"error={error}")
        if request_id:
            parts.append(f"request_id={request_id}")
        if parts:
            return ", ".join(parts)
    return message


def _is_ticker_not_found_error(message: str) -> bool:
    message_upper = message.upper()
    return "NOT_FOUND" in message_upper or "UNKNOWN TICKER" in message_upper


class PolygonSource(DataSourceInterface):
    """
    使用 Polygon.io 作为数据源的实现，通过 KeyRateLimiter 进行精准速率控制。
    """

    def __init__(self, rate_limiter: KeyRateLimiter):
        """
        初始化 PolygonSource。
        :param rate_limiter: 一个配置好的 KeyRateLimiter 实例。
        """
        self.rate_limiter = rate_limiter
        logger.info("[PolygonSource] 初始化成功，将使用 KeyRateLimiter 进行API调用。")

    def _get_client(self) -> RESTClient:
        """通过速率限制器获取一个key，并创建API客户端。"""
        key_to_use = self.rate_limiter.acquire_key()
        return RESTClient(key_to_use)

    def _build_security_info_payload(self, symbol: str, details: Any) -> Dict[str, Any]:
        address = getattr(details, 'address', None)
        branding = getattr(details, 'branding', None)

        update_data = {
            'symbol': symbol.lower(),
            'is_active': getattr(details, 'active', None),
            'name': getattr(details, 'name', None),
            'exchange': getattr(details, 'primary_exchange', None),
            'currency': getattr(details, 'currency_name', None),
            'market': getattr(details, 'locale', None),
            'type': getattr(details, 'type', None),
            'list_date': _parse_date_string(getattr(details, 'list_date', None)),
            'delist_date': _parse_date_string(getattr(details, 'delisted_utc', None)),
            'cik': getattr(details, 'cik', None),
            'composite_figi': getattr(details, 'composite_figi', None),
            'share_class_figi': getattr(details, 'share_class_figi', None),
            'market_cap': getattr(details, 'market_cap', None),
            'phone_number': getattr(details, 'phone_number', None),
            'description': getattr(details, 'description', None),
            'homepage_url': getattr(details, 'homepage_url', None),
            'total_employees': getattr(details, 'total_employees', None),
            'sic_code': getattr(details, 'sic_code', None),
            'industry': getattr(details, 'sic_description', None),
            'address_line1': getattr(address, 'address1', None) if address else None,
            'city': getattr(address, 'city', None) if address else None,
            'state': getattr(address, 'state', None) if address else None,
            'postal_code': getattr(address, 'postal_code', None) if address else None,
            'logo_url': getattr(branding, 'logo_url', None) if branding else None,
            'icon_url': getattr(branding, 'icon_url', None) if branding else None,
        }
        # 仅保留非 None 的字段，避免用“缺失字段的 None”覆盖数据库里已有的有效值。
        # 注意：False/0 等有效值不受影响（仅过滤 None）。
        return {k: v for k, v in update_data.items() if (v is not None) or (k == 'symbol')}

    def _retry_security_info_with_date(
        self,
        client: RESTClient,
        symbol: str,
        symbol_upper: str,
        lookup_date: str,
    ) -> Optional[Dict[str, Any]]:
        try:
            logger.info(
                "[{}] Polygon 当前日期下未找到 ticker，改用历史日期 {} 查询详情。",
                symbol,
                lookup_date,
            )
            details = client.get_ticker_details(symbol_upper, date=lookup_date)
            update_data = self._build_security_info_payload(symbol, details)
            # 当前时点已无法按 ticker 查询到详情，按当前同步语义将其视为 inactive。
            update_data['is_active'] = False
            return update_data
        except BadResponse as retry_exc:
            formatted_retry_error = _format_polygon_error(retry_exc)
            logger.warning(
                "[{}] Polygon 历史日期 {} 仍未返回 ticker 详情: {}",
                symbol,
                lookup_date,
                formatted_retry_error,
            )
            return None
        except HTTPError as retry_exc:
            response = getattr(retry_exc, "response", None)
            status_code = getattr(response, "status_code", "UNKNOWN")
            logger.warning(
                "[{}] Polygon 历史日期 {} 查询详情失败: {} - {}",
                symbol,
                lookup_date,
                status_code,
                _format_polygon_error(retry_exc),
            )
            return None

    def get_security_info(self, symbol: str, fallback_date: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """
        获取单个证券的详细信息。
        速率控制由 _get_client() 内部处理。
        """
        client = self._get_client()
        symbol_upper = symbol.upper()
        fallback_date_str = _normalize_lookup_date(fallback_date)
        try:
            logger.debug(f"正在为 {symbol_upper} 调用 Polygon Ticker Details API...")
            details = client.get_ticker_details(symbol_upper)
            return self._build_security_info_payload(symbol, details)
        except HTTPError as e:
            if e.response.status_code == 429:
                # 理论上，由于我们的精准控制，这个错误不应该频繁出现
                logger.critical("[{}] 遭遇速率限制 (429)! KeyRateLimiter 未能完全避免。请检查配置或网络延迟。", symbol)
            elif e.response.status_code == 404:
                if fallback_date_str:
                    retry_data = self._retry_security_info_with_date(client, symbol, symbol_upper, fallback_date_str)
                    if retry_data:
                        return retry_data
                logger.warning("[{}] 在 Polygon API 中未找到 (404)。", symbol)
            else:
                logger.error(
                    "[{}] 请求 Polygon API 时发生 HTTP 错误: {} - {}",
                    symbol,
                    e.response.status_code,
                    _format_polygon_error(e),
                )
            return None
        except BadResponse as e:
            formatted_error = _format_polygon_error(e)
            if _is_ticker_not_found_error(formatted_error):
                if fallback_date_str:
                    retry_data = self._retry_security_info_with_date(client, symbol, symbol_upper, fallback_date_str)
                    if retry_data:
                        return retry_data
                logger.warning("[{}] Polygon 未返回该 ticker 的详情: {}", symbol, formatted_error)
            else:
                logger.warning("[{}] Polygon 返回了非 200 响应: {}", symbol, formatted_error)
            return None
        except Exception as e:
            logger.opt(exception=e).error(
                "[{}] 请求 Polygon API 时发生未知错误: {}",
                symbol,
                _format_polygon_error(e),
            )
            return None

    def get_dividends(self, symbol: str) -> List[Dict[str, Any]]:
        """获取指定股票的全部历史分红数据"""
        client = self._get_client()
        dividends_data = []
        try:
            logger.debug(f"为 {symbol.upper()} 获取全部分红数据...")
            # 使用分页器自动处理分页
            for dividend in client.list_dividends(symbol.upper(), limit=1000):
                cash_amount_raw = getattr(dividend, 'cash_amount', None)
                cash_amount = None
                if cash_amount_raw is not None:
                    try:
                        cash_amount = (
                            cash_amount_raw
                            if isinstance(cash_amount_raw, Decimal)
                            else Decimal(str(cash_amount_raw))
                        ).quantize(_DIVIDEND_QUANT)
                    except Exception:
                        cash_amount = None

                # 预定义所有可能的键，确保每个字典结构一致
                record = {
                    'ex_dividend_date': _parse_date_string(getattr(dividend, 'ex_dividend_date', None)),
                    'declaration_date': _parse_date_string(getattr(dividend, 'declaration_date', None)),
                    'record_date': _parse_date_string(getattr(dividend, 'record_date', None)),
                    'pay_date': _parse_date_string(getattr(dividend, 'pay_date', None)),
                    'cash_amount': cash_amount,
                    'currency': getattr(dividend, 'currency', None),
                    'frequency': getattr(dividend, 'frequency', None),
                }
                dividends_data.append(record)
                # 过滤掉无效记录，但此时所有字典的键都是完整的
            return [d for d in dividends_data if d['ex_dividend_date'] and d['cash_amount'] is not None]
        except Exception as e:
            logger.opt(exception=e).error("为 {} 从 Polygon 获取分红数据时出错: {}", symbol, _format_polygon_error(e))
            return []

    def _get_client_and_key(self) -> Tuple[RESTClient, str]:
        """
        通过速率限制器获取一个key，并创建API客户端。
        【新】同时返回客户端实例和它所使用的 key。
        """
        key_to_use = self.rate_limiter.acquire_key()
        client = RESTClient(key_to_use)
        return client, key_to_use

    def get_grouped_daily_data(self, target_date: str) -> List[Dict[str, Any]]:
        """
        获取指定日期的分组每日聚合数据 (Grouped Daily Aggregates)。
        【最终版 v3】: 使用独立的 requests 请求，并将 key 的获取和使用解耦。

        :param target_date: 'YYYY-MM-DD' 格式的日期字符串。
        :return: 包含当天所有股票行情数据的字典列表。
        """
        # 调用新的方法，同时获取 client 和 api_key
        # 注意：这里的 client 实际上我们用不到了，但为了逻辑完整性保留
        _client, api_key = self._get_client_and_key()
        # Polygon API 的基础 URL 和端点
        base_url = "https://api.polygon.io"
        endpoint = f"/v2/aggs/grouped/locale/us/market/stocks/{target_date}"
        full_url = base_url + endpoint
        # 构建请求参数
        params = {
            'adjusted': 'false',
            'apiKey': api_key  # 直接使用我们从 KeyRateLimiter 获取的 key
        }
        try:
            logger.debug(f"正在为日期 {target_date} 调用 Polygon Grouped Daily Aggs API (via requests)...")

            response = requests.get(full_url, params=params, timeout=10)
            response.raise_for_status()

            response_json = response.json()
            results = response_json.get('results', [])

            return results

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning("日期 {} 在 Polygon API 中未找到数据 (404)，可能为非交易日。", target_date)
            else:
                logger.error(
                    "为日期 {} 请求 Grouped Daily API 时发生 HTTP 错误: {} - {}",
                    target_date,
                    e.response.status_code,
                    _format_polygon_error(e),
                )
            return []
        except requests.exceptions.RequestException as e:
            logger.opt(exception=e).error("为日期 {} 请求 Grouped Daily API 时发生网络错误: {}", target_date, e)
            return []
        except Exception as e:
            logger.opt(exception=e).error(
                "为日期 {} 解析 Grouped Daily API 响应时发生未知错误: {}",
                target_date,
                _format_polygon_error(e),
            )
            return []

    def get_splits(self, symbol: str) -> List[Dict[str, Any]]:
        """获取指定股票的全部历史拆股数据"""
        client = self._get_client()
        splits_data = []
        try:
            logger.debug(f"为 {symbol.upper()} 获取全部拆股数据...")
            # 使用分页器自动处理分页
            for split in client.list_splits(symbol.upper(), limit=1000):
                split_to_raw = getattr(split, 'split_to', None)
                split_from_raw = getattr(split, 'split_from', None)
                split_to = None
                split_from = None
                if split_to_raw is not None:
                    try:
                        split_to = (
                            split_to_raw
                            if isinstance(split_to_raw, Decimal)
                            else Decimal(str(split_to_raw))
                        ).quantize(_SPLIT_QUANT)
                    except Exception:
                        split_to = None
                if split_from_raw is not None:
                    try:
                        split_from = (
                            split_from_raw
                            if isinstance(split_from_raw, Decimal)
                            else Decimal(str(split_from_raw))
                        ).quantize(_SPLIT_QUANT)
                    except Exception:
                        split_from = None

                # 预定义所有可能的键，确保每个字典结构一致
                record = {
                    'execution_date': _parse_date_string(getattr(split, 'execution_date', None)),
                    'declaration_date': None,  # 始终包含此键
                    'split_to': split_to,
                    'split_from': split_from,
                }
                splits_data.append(record)
                # 过滤掉无效记录
            return [
                s
                for s in splits_data
                if s['execution_date'] and s['split_to'] is not None and s['split_from'] is not None
            ]
        except Exception as e:
            logger.opt(exception=e).error("为 {} 从 Polygon 获取拆股数据时出错: {}", symbol, _format_polygon_error(e))
            return []
    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None,
                            adjusted: bool = False,
                            ) -> pd.DataFrame:
        """获取历史日线数据。"""
        client = self._get_client()

        if end is None:
            end = date.today().strftime('%Y-%m-%d')
        if start is None:
            logger.warning("get_historical_data 未提供 start 日期，可能获取全部可用历史（最多2年）。")

        try:
            logger.debug(f"为 {symbol.upper()} 从 {start or '开始'} 到 {end} 获取日线数据...")
            resp = client.get_aggs(
                ticker=symbol.upper(),
                multiplier=1,
                timespan='day',
                from_=start,
                to=end,
                adjusted=adjusted,
                limit=50000
            )
            if not resp:
                return pd.DataFrame()

            df = pd.DataFrame(resp)
            df['Date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.date
            df.set_index('Date', inplace=True)

            df.rename(columns={
                'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume',
            }, inplace=True)

            if 'vwap' not in df.columns:
                # 兼容不同客户端字段命名
                if 'vw' in df.columns:
                    df.rename(columns={'vw': 'vwap'}, inplace=True)
                elif 'vmap' in df.columns:
                    df.rename(columns={'vmap': 'vwap'}, inplace=True)

            if 'vwap' in df.columns:
                df['turnover'] = df['Volume'] * df['vwap']
            else:
                df['vwap'] = None
                df['turnover'] = None
            df['Dividends'] = 0.0
            df['Stock Splits'] = 0.0

            return df[['Open', 'High', 'Low', 'Close', 'Volume', 'vwap', 'turnover', 'Dividends', 'Stock Splits']]
        except Exception as e:
            logger.opt(exception=e).error("为 {} 从 Polygon 获取历史数据时出错: {}", symbol, _format_polygon_error(e))
            return pd.DataFrame()
