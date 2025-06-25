# data_sources/finnhub_source.py
import os
import finnhub
import pandas as pd
from loguru import logger
from typing import Optional
from dotenv import load_dotenv

from .base import DataSourceInterface

# 加载环境变量
load_dotenv()


class FinnhubSource(DataSourceInterface):
    """
    使用 finnhub.io 作为数据源的实现。
    注意：此实现已被修改，仅用于检查市场开盘状态，不提供数据获取功能。
    """

    def __init__(self):
        self.api_key = os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY 未在 .env 文件中设置。")
        self.client = finnhub.Client(api_key=self.api_key)
        logger.info("[FinnhubSource] Finnhub 客户端初始化成功 (仅用于市场状态检查)。")

    def is_market_open(self, exchange_code: str) -> bool:
        """
        使用 Finnhub API 检查指定交易所是否正在开盘。

        :param exchange_code: 交易所代码，例如 'US' (美股), 'HK' (港股)。
        :return: 如果市场开放则返回 True，否则返回 False。
        """
        try:
            status = self.client.market_status(exchange=exchange_code)
            is_open = status.get('isOpen', False)
            if is_open:
                logger.trace(f"Finnhub 报告交易所 '{exchange_code}' 状态: {status}")
            return is_open
        except Exception as e:
            logger.error(f"[FinnhubSource] 检查交易所 '{exchange_code}' 状态时出错: {e}", exc_info=True)
            # 在出错的情况下，保守地返回 False，允许程序继续执行，避免因API临时问题导致更新中断
            return False

    def get_security_info(self, symbol: str) -> Optional[dict]:
        """
        此方法已停用。FinnhubSource 不再用于获取证券信息。
        """
        logger.warning("[FinnhubSource] get_security_info 已被禁用。请使用其他数据源。")
        raise NotImplementedError("FinnhubSource 不再支持 get_security_info 方法。")

    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            period: str = "max",
                            interval: str = "1d",
                            auto_adjust: bool = False
                            ) -> pd.DataFrame:
        """
        此方法已停用。FinnhubSource 不再用于获取历史数据。
        """
        logger.warning("[FinnhubSource] get_historical_data 已被禁用。请使用其他数据源。")
        raise NotImplementedError("FinnhubSource 不再支持 get_historical_data 方法。")

