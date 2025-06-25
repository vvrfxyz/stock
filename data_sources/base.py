# data_sources/base.py
import abc
import pandas as pd
from typing import Optional

class DataSourceInterface(abc.ABC):
    """
    数据源接口（抽象基类）。
    定义了所有数据源实现都必须遵循的契约。
    """

    @abc.abstractmethod
    def get_security_info(self, symbol: str) -> Optional[dict]:
        """
        获取单个证券的基本信息。

        :param symbol: 证券代码。
        :return: 包含信息的字典，格式应与 yfinance 的 info 字典类似以便复用。
                 如果无法获取，则返回 None。
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            period: str = "max",
                            interval: str = "1d",
                            auto_adjust: bool = False
                            ) -> pd.DataFrame:
        """
        获取历史市场数据。

        :param symbol: 证券代码。
        :param start: 开始日期，格式 'YYYY-MM-DD'。
        :param period: 数据周期，如 '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'。
        :param interval: 数据间隔，如 '1d', '1wk', '1mo'。
        :param auto_adjust: 是否自动调整 (此参数主要为兼容 yfinance)。
        :return: 包含历史数据的 pandas DataFrame。
                 列应包括：'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits'。
        """
        raise NotImplementedError

