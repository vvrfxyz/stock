# data_sources/yfinance_source.py
import yfinance as yf
import pandas as pd
from loguru import logger
from typing import Optional

from .base import DataSourceInterface

class YFinanceSource(DataSourceInterface):
    """
    使用 yfinance 作为数据源的具体实现。
    """

    def get_security_info(self, symbol: str) -> Optional[dict]:
        """
        从 yfinance 获取证券基本信息。
        """
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            # yfinance 在 ticker 无效时可能返回空的 info 或只有少量字段的 info
            if not info or info.get('quoteType') is None:
                logger.warning(f"[YFinanceSource] 无法为 {symbol} 获取有效的 'info' 数据。")
                return None
            return info
        except Exception as e:
            logger.error(f"[YFinanceSource] 调用 yfinance 获取 {symbol} 信息时出错: {e}")
            return None

    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            period: str = "max",
                            interval: str = "1d",
                            auto_adjust: bool = False
                            ) -> pd.DataFrame:
        """
        从 yfinance 获取历史市场数据。
        """
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                period=period,
                interval=interval,
                start=start,
                auto_adjust=auto_adjust,
                # back_adjust=True 可以在某些情况下帮助解决复权问题，但我们自己计算，所以设为False
                back_adjust=False
            )
            return df
        except Exception as e:
            logger.error(f"[YFinanceSource] 调用 yfinance 获取 {symbol} 历史数据时出错: {e}")
            return pd.DataFrame() # 返回空 DataFrame 以保证类型一致性

