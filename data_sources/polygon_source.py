# data_sources/polygon_source.py
import os
import time
import pandas as pd
from loguru import logger
from typing import Optional, List, Dict
from datetime import date, datetime
import threading

from polygon import RESTClient
from requests.exceptions import HTTPError

from .base import DataSourceInterface
from data_models.models import MarketType, AssetType  # 确保可以从项目根目录导入


# --- Helper Functions ---
def _map_polygon_market(locale: str) -> Optional[MarketType]:
    """将 Polygon 的 locale 映射到我们的 MarketType 枚举"""
    if not locale: return None
    locale_upper = locale.upper()
    if locale_upper == 'US': return MarketType.US
    if locale_upper == 'GLOBAL': return MarketType.US
    return None


def _map_polygon_asset_type(pg_type: str) -> Optional[AssetType]:
    """将 Polygon 的 type 映射到我们的 AssetType 枚举"""
    if not pg_type: return None
    type_map = {
        'CS': AssetType.STOCK,
        'ETF': AssetType.ETF,
        'ETN': AssetType.ETF,
        'WARRANT': AssetType.WARRANT,
        'INDEX': AssetType.INDEX,
        'MUTUAL FUND': AssetType.MUTUAL_FUND,
        'PREFERRED STOCK': AssetType.PREFERRED_STOCK,
        'ADRC': AssetType.STOCK,
    }
    mapped_type = type_map.get(pg_type.upper())
    if not mapped_type:
        logger.warning(f"遇到未知的 Polygon asset type: '{pg_type}', 将其归类为 STOCK。")
        return AssetType.STOCK
    return mapped_type


def _parse_date_string(date_str: str) -> Optional[date]:
    """安全地将 YYYY-MM-DD 格式的字符串解析为 date 对象"""
    if not date_str: return None
    try:
        if 'Z' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
        return date.fromisoformat(date_str)
    except (ValueError, TypeError):
        return None


# --- Main Class ---
class PolygonSource(DataSourceInterface):
    """
    使用 Polygon.io 作为数据源的实现，内置 API Key 轮询机制。
    """

    def __init__(self, delay_between_calls: float = 2):
        api_keys_str = os.getenv("POLYGON_API_KEYS")
        if not api_keys_str:
            raise ValueError("环境变量 POLYGON_API_KEYS 未设置。")
        self.api_keys: List[str] = [key.strip() for key in api_keys_str.split(',') if key.strip()]
        if not self.api_keys:
            raise ValueError("环境变量 POLYGON_API_KEYS 中没有找到有效的 API Key。")
        self._key_index: int = 0
        self._lock = threading.Lock()
        self.delay = delay_between_calls
        logger.info(f"[PolygonSource] 初始化成功，加载了 {len(self.api_keys)} 个 API Key。")

    def _get_client(self) -> RESTClient:
        with self._lock:
            key_to_use = self.api_keys[self._key_index]
            self._key_index = (self._key_index + 1) % len(self.api_keys)
            logger.trace(f"使用 Polygon API Key (索引: {self._key_index})")
        return RESTClient(key_to_use)

    def get_security_info(self, symbol: str) -> Optional[Dict]:
        client = self._get_client()
        time.sleep(self.delay)
        try:
            logger.debug(f"正在为 {symbol.upper()} 调用 Polygon Ticker Details API...")
            details = client.get_ticker_details(symbol.upper())
            address = getattr(details, 'address', None)
            branding = getattr(details, 'branding', None)

            update_data = {
                'symbol': symbol.lower(),
                'is_active': getattr(details, 'active', False),
                'name': getattr(details, 'name', None),
                'exchange': getattr(details, 'primary_exchange', None),
                'currency': getattr(details, 'currency_name', None),
                'market': _map_polygon_market(getattr(details, 'locale', None)),
                'type': _map_polygon_asset_type(getattr(details, 'type', None)),
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
            # 清理 None 值
            return {k: v for k, v in update_data.items() if v is not None}
        except HTTPError as e:
            if e.response.status_code == 404:
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
        client = self._get_client()
        time.sleep(self.delay)

        if end is None:
            end = date.today().strftime('%Y-%m-%d')
        if start is None:
            # Polygon免费API限制2年数据，这里不设默认start，让调用者决定
            logger.warning("get_historical_data 未提供 start 日期，可能获取全部可用历史（最多2年）。")

        try:
            logger.debug(f"为 {symbol.upper()} 从 {start or '开始'} 到 {end} 获取日线数据...")
            resp = client.get_aggs(
                ticker=symbol.upper(),
                multiplier=1,
                timespan='day',
                from_=start,
                to=end,
                adjusted=False,  # 获取原始价格，复权由我们自己计算
                limit=50000
            )
            if not resp:
                return pd.DataFrame()

            df = pd.DataFrame(resp)
            df['Date'] = pd.to_datetime(df['t'], unit='ms').dt.date
            df.set_index('Date', inplace=True)

            df.rename(columns={
                'o': 'Open', 'h': 'High', 'l': 'Low', 'c': 'Close', 'v': 'Volume',
                'vw': 'vwap'  # 直接获取VWAP作为平均价
            }, inplace=True)

            # 计算成交额
            df['turnover'] = df['Volume'] * df['vwap']

            # 为兼容旧流程，添加空的'Dividends'和'Stock Splits'列
            df['Dividends'] = 0.0
            df['Stock Splits'] = 0.0

            return df[['Open', 'High', 'Low', 'Close', 'Volume', 'vwap', 'turnover', 'Dividends', 'Stock Splits']]
        except Exception as e:
            logger.error(f"为 {symbol} 从 Polygon 获取历史数据时出错: {e}", exc_info=True)
            return pd.DataFrame()

    def get_all_daily_prices(self, trade_date: str) -> pd.DataFrame:
        """使用 Grouped Daily Aggregates 一次性获取指定日期的所有股票数据"""
        client = self._get_client()
        time.sleep(self.delay)
        try:
            logger.info(f"正在为日期 {trade_date} 获取所有股票的分组日线数据...")
            aggs = client.get_grouped_daily_aggs(trade_date, adjusted=False)

            data = [
                {
                    'symbol': agg.ticker.lower(),
                    'date': datetime.fromtimestamp(agg.timestamp / 1000).date(),
                    'open': agg.open,
                    'high': agg.high,
                    'low': agg.low,
                    'close': agg.close,
                    'volume': agg.volume,
                    'vwap': agg.vwap,
                    'turnover': agg.volume * agg.vwap if agg.vwap else 0,
                }
                for agg in aggs
            ]

            if not data:
                logger.warning(f"日期 {trade_date} 未返回任何分组日线数据。")
                return pd.DataFrame()

            df = pd.DataFrame(data)
            logger.success(f"成功获取 {len(df)} 条 {trade_date} 的日线数据。")
            return df

        except Exception as e:
            logger.error(f"获取 {trade_date} 的分组日线数据时出错: {e}", exc_info=True)
            return pd.DataFrame()

