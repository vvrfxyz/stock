# data_sources/polygon_source.py (最终优化版)
import os
import pandas as pd
from loguru import logger
from typing import Optional, List, Dict, Any
from datetime import date, datetime

from polygon import RESTClient
from requests.exceptions import HTTPError

from .base import DataSourceInterface
from utils.key_rate_limiter import KeyRateLimiter # 引入新的类

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

    def get_security_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取单个证券的详细信息。
        速率控制由 _get_client() 内部处理。
        """
        client = self._get_client()
        try:
            logger.debug(f"正在为 {symbol.upper()} 调用 Polygon Ticker Details API...")
            details = client.get_ticker_details(symbol.upper())
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
            return update_data
        except HTTPError as e:
            if e.response.status_code == 429:
                # 理论上，由于我们的精准控制，这个错误不应该频繁出现
                logger.critical(f"[{symbol}] 遭遇速率限制 (429)! KeyRateLimiter 未能完全避免。请检查配置或网络延迟。")
            elif e.response.status_code == 404:
                logger.warning(f"[{symbol}] 在 Polygon API 中未找到 (404)。")
            else:
                logger.error(f"[{symbol}] 请求 Polygon API 时发生 HTTP 错误: {e.response.status_code} - {e}")
            return None
        except Exception as e:
            logger.error(f"[{symbol}] 请求 Polygon API 时发生未知错误: {e}", exc_info=True)
            return None

    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None,
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
                adjusted=False,
                limit=50000
            )
            if not resp:
                return pd.DataFrame()

            df = pd.DataFrame(resp)
            df['Date'] = pd.to_datetime(df['t'], unit='ms').dt.date
            df.set_index('Date', inplace=True)

            df.rename(columns={
                'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume',
                'vw': 'vwap'
            }, inplace=True)

            df['turnover'] = df['Volume'] * df['vwap']
            df['Dividends'] = 0.0
            df['Stock Splits'] = 0.0

            return df[['Open', 'High', 'Low', 'Close', 'Volume', 'vwap', 'turnover', 'Dividends', 'Stock Splits']]
        except Exception as e:
            logger.error(f"为 {symbol} 从 Polygon 获取历史数据时出错: {e}", exc_info=True)
            return pd.DataFrame()
