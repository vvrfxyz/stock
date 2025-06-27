import os
import time
import pandas as pd
from loguru import logger
from typing import Optional, List
from datetime import date, datetime
import threading

from polygon import RESTClient
from requests.exceptions import HTTPError

from .base import DataSourceInterface
from data_models.models import MarketType, AssetType  # 确保可以从项目根目录导入


# --- Helper Functions (从您的 update_details_from_polygon.py 脚本中提取并优化) ---

def _map_polygon_market(locale: str) -> Optional[MarketType]:
    """将 Polygon 的 locale 映射到我们的 MarketType 枚举"""
    if not locale: return None
    locale_upper = locale.upper()
    if locale_upper == 'US': return MarketType.US
    if locale_upper == 'GLOBAL': return MarketType.US  # GLOBAL 通常指美国市场资产
    # 可以根据需要添加其他市场的映射，例如 'ca' -> MarketType.CA
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

    def __init__(self, delay_between_calls: float = 1):
        """
        初始化 PolygonSource。
        从环境变量 POLYGON_API_KEYS 中读取一个或多个逗号分隔的 API Key。

        :param delay_between_calls: 每次API调用前的延迟（秒），用于主动限速。
                                    例如，5个Key，每个Key限制60次/分钟，理论上每秒可调用5次。
                                    0.2秒的延迟是比较安全的选择。
        """
        api_keys_str = os.getenv("POLYGON_API_KEYS")
        if not api_keys_str:
            raise ValueError("环境变量 POLYGON_API_KEYS 未设置。请在 .env 文件中提供一个或多个逗号分隔的 API Key。")

        self.api_keys: List[str] = [key.strip() for key in api_keys_str.split(',') if key.strip()]
        if not self.api_keys:
            raise ValueError("环境变量 POLYGON_API_KEYS 中没有找到有效的 API Key。")

        self._key_index: int = 0
        self._lock = threading.Lock()  # 使用线程锁确保多线程环境下的安全
        self.delay = delay_between_calls

        logger.info(f"[PolygonSource] 初始化成功，加载了 {len(self.api_keys)} 个 API Key。")

    def _get_client(self) -> RESTClient:
        """
        轮询获取下一个 API Key 并创建 RESTClient 实例。
        这是一个线程安全的方法。
        """
        with self._lock:
            key_to_use = self.api_keys[self._key_index]
            # 更新索引，为下一次调用做准备
            self._key_index = (self._key_index + 1) % len(self.api_keys)
            logger.trace(f"使用 Polygon API Key (索引: {self._key_index})")

        return RESTClient(key_to_use)

    def get_security_info(self, symbol: str) -> Optional[dict]:
        """
        获取单个证券的详细信息。
        :param symbol: 证券代码。
        :return: 包含信息的字典，如果无法获取则返回 None。
        """
        client = self._get_client()
        time.sleep(self.delay)  # 在发起请求前主动延迟

        try:
            logger.debug(f"正在为 {symbol.upper()} 调用 Polygon Ticker Details API...")
            details_response = client.get_ticker_details(symbol.upper())

            # 将 Polygon 的响应对象映射为您需要的字典格式
            address = getattr(details_response, 'address', None)
            branding = getattr(details_response, 'branding', None)

            update_data = {
                'symbol': symbol.lower(),
                'is_active': getattr(details_response, 'active', False),
            }

            potential_updates = {
                'name': getattr(details_response, 'name', None),
                'exchange': getattr(details_response, 'primary_exchange', None),
                'currency': getattr(details_response, 'currency_name', None),
                'market': _map_polygon_market(getattr(details_response, 'locale', None)),
                'type': _map_polygon_asset_type(getattr(details_response, 'type', None)),
                'list_date': _parse_date_string(getattr(details_response, 'list_date', None)),
                'delist_date': _parse_date_string(getattr(details_response, 'delisted_utc', None)),
                'cik': getattr(details_response, 'cik', None),
                'composite_figi': getattr(details_response, 'composite_figi', None),
                'share_class_figi': getattr(details_response, 'share_class_figi', None),
                'market_cap': getattr(details_response, 'market_cap', None),
                'phone_number': getattr(details_response, 'phone_number', None),
                'description': getattr(details_response, 'description', None),
                'homepage_url': getattr(details_response, 'homepage_url', None),
                'total_employees': getattr(details_response, 'total_employees', None),
                'sic_code': getattr(details_response, 'sic_code', None),
                'industry': getattr(details_response, 'sic_description', None),
                'address_line1': getattr(address, 'address1', None) if address else None,
                'city': getattr(address, 'city', None) if address else None,
                'state': getattr(address, 'state', None) if address else None,
                'postal_code': getattr(address, 'postal_code', None) if address else None,
                'logo_url': getattr(branding, 'logo_url', None) if branding else None,
                'icon_url': getattr(branding, 'icon_url', None) if branding else None,
            }

            for key, value in potential_updates.items():
                if value is not None:
                    update_data[key] = value

            return update_data

        except HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"[{symbol}] 在 Polygon API 中未找到 (404)。")
            elif e.response.status_code == 429:
                logger.error(f"[{symbol}] 请求过于频繁 (429 Too Many Requests)。请考虑增加 delay_between_calls 的值。")
            else:
                logger.error(f"[{symbol}] 请求 Polygon API 时发生 HTTP 错误: {e.response.status_code} - {e}")
            return None  # 在出错时返回 None
        except Exception as e:
            logger.error(f"[{symbol}] 请求 Polygon API 时发生未知错误: {e}", exc_info=True)
            return None

    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None,
                            interval: str = "1d",
                            ) -> pd.DataFrame:
        """
        获取历史市场数据。
        注意：Polygon 的 period/interval 与 yfinance 不同，这里做适配。

        :param symbol: 证券代码。
        :param start: 开始日期 'YYYY-MM-DD'。
        :param end: 结束日期 'YYYY-MM-DD'。如果为None，则默认为今天。
        :param interval: 数据间隔，支持 '1d' (day), '1wk' (week), '1mo' (month)。
        :return: 包含历史数据的 pandas DataFrame。
        """
        client = self._get_client()
        time.sleep(self.delay)

        # 转换 interval 到 Polygon 的 timespan
        timespan_map = {'1d': 'day', '1wk': 'week', '1mo': 'month'}
        if interval not in timespan_map:
            logger.error(f"不支持的 interval: '{interval}'. PolygonSource 支持 '1d', '1wk', '1mo'。")
            return pd.DataFrame()
        timespan = timespan_map[interval]

        # Polygon API 需要一个结束日期
        if end is None:
            end = date.today().strftime('%Y-%m-%d')

        # Polygon API 对免费用户有2年的历史数据限制，这里设置一个默认的最早开始日期
        if start is None:
            start = (date.today() - pd.Timedelta(days=730)).strftime('%Y-%m-%d')

        try:
            logger.debug(f"为 {symbol.upper()} 从 {start} 到 {end} 获取 {timespan} 数据...")
            resp = client.get_aggs(
                ticker=symbol.upper(),
                multiplier=1,
                timespan=timespan,
                from_=start,
                to=end,
                adjusted=True,  # 使用 Polygon 的复权数据
                limit=50000  # 设置一个较大的 limit
            )

            if not resp:
                return pd.DataFrame()

            df = pd.DataFrame(resp)
            # Polygon 返回的 't' 是毫秒级 Unix 时间戳
            df['Date'] = pd.to_datetime(df['t'], unit='ms').dt.tz_localize('UTC')
            df.set_index('Date', inplace=True)

            # 重命名列以匹配 yfinance 的输出，方便后续处理
            # 注意：Polygon 的 adjusted=True 数据不直接提供 Dividends 和 Stock Splits 列
            # 如果您需要这些，需要另外调用其他API端点 (v3/reference/dividends, v3/reference/splits)
            df.rename(columns={
                'o': 'Open',
                'h': 'High',
                'l': 'Low',
                'c': 'Close',
                'v': 'Volume',
                'vw': 'VWAP'  # Volume Weighted Average Price
            }, inplace=True)

            # 添加 yfinance 中存在的但 Polygon 不直接提供的列，并填充默认值
            if 'Dividends' not in df.columns:
                df['Dividends'] = 0.0
            if 'Stock Splits' not in df.columns:
                df['Stock Splits'] = 0.0

            return df[['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits']]

        except Exception as e:
            logger.error(f"为 {symbol} 从 Polygon 获取历史数据时出错: {e}", exc_info=True)
            return pd.DataFrame()

