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
        获取单个证券的详细信息（用于更新 Security 表）。

        :param symbol: 证券代码。
        :return: 包含信息的字典（键应与 Security 模型字段名对齐）。
                 如果无法获取则返回 None。
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            end: Optional[str] = None,
                            adjusted: bool = False
                            ) -> pd.DataFrame:
        """
        获取历史日线数据。

        :param symbol: 证券代码。
        :param start: 开始日期，格式 'YYYY-MM-DD'，None 表示由数据源决定。
        :param end: 结束日期，格式 'YYYY-MM-DD'，None 表示由数据源决定。
        :param adjusted: 是否使用复权数据（依数据源支持情况而定）。
        :return: 包含历史数据的 pandas DataFrame。
                 建议列包括：'Open', 'High', 'Low', 'Close', 'Volume'（可选：'vwap', 'turnover' 等）。
        """
        raise NotImplementedError
