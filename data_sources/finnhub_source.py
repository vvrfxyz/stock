# data_sources/finnhub_source.py
import os
import finnhub
import pandas as pd
from loguru import logger
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

from .base import DataSourceInterface

# 加载环境变量
load_dotenv()

class FinnhubSource(DataSourceInterface):
    """
    使用 finnhub.io 作为数据源的具体实现。
    """
    def __init__(self):
        self.api_key = os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            raise ValueError("FINNHUB_API_KEY 未在 .env 文件中设置。")
        self.client = finnhub.Client(api_key=self.api_key)
        logger.info("[FinnhubSource] Finnhub 客户端初始化成功。")

    def get_security_info(self, symbol: str) -> Optional[dict]:
        """
        从 Finnhub 获取证券基本信息。
        注意：Finnhub 的 'info' 字段与 yfinance 不同，我们只映射部分关键字段。
        """
        try:
            profile = self.client.company_profile2(symbol=symbol)
            if not profile:
                logger.warning(f"[FinnhubSource] 无法为 {symbol} 获取有效的 'profile' 数据。")
                return None

            # 模拟 yfinance 的 info 字典结构，以便复用
            info = {
                'quoteType': profile.get('ipo') and 'EQUITY' or 'INDEX', # 简单判断
                'longName': profile.get('name'),
                'shortName': profile.get('name'),
                'exchange': profile.get('exchange'),
                'currency': profile.get('currency'),
                'sector': profile.get('finnhubIndustry'), # Finnhub 使用 finnhubIndustry
                'industry': profile.get('finnhubIndustry'),
                'symbol': profile.get('ticker'),
            }
            return info
        except Exception as e:
            logger.error(f"[FinnhubSource] 调用 Finnhub 获取 {symbol} 信息时出错: {e}")
            return None

    def get_historical_data(self,
                            symbol: str,
                            start: Optional[str] = None,
                            period: str = "max",
                            interval: str = "1d",
                            auto_adjust: bool = False
                            ) -> pd.DataFrame:
        """
        从 Finnhub 获取历史市场数据。
        注意：Finnhub 免费 API 不在 stock_candles 中提供股息和拆分数据。
              我们将返回包含这些列但值为0的DataFrame以保持兼容性。
        """
        if interval != "1d":
            logger.warning(f"[FinnhubSource] Finnhub 源当前实现仅支持 '1d' 间隔，收到了 '{interval}'。")
            return pd.DataFrame()

        try:
            # 确定时间范围
            end_ts = int(datetime.now().timestamp())
            if period == 'max':
                # Finnhub API 对历史数据有不同限制，这里设一个较早的日期
                start_ts = int(datetime(1970, 1, 2).timestamp())
            elif start:
                start_ts = int(pd.to_datetime(start).timestamp())
            else:
                # 如果 period 不是 'max' 且没有 start，我们可以根据 period 计算
                # 为了简化，我们只处理 'max' 和 'start'
                logger.error("[FinnhubSource] 请提供 'start' 日期或使用 'period=max'。")
                return pd.DataFrame()

            # 调用API
            res = self.client.stock_candles(symbol, 'D', start_ts, end_ts)

            if res.get('s') != 'ok' or not res.get('t'):
                logger.warning(f"[FinnhubSource] Finnhub 未返回 {symbol} 的有效历史数据。状态: {res.get('s')}")
                return pd.DataFrame()

            # 构建 DataFrame
            df = pd.DataFrame({
                'Date': [datetime.fromtimestamp(t) for t in res['t']],
                'Open': res['o'],
                'High': res['h'],
                'Low': res['l'],
                'Close': res['c'],
                'Volume': res['v']
            })
            df.set_index('Date', inplace=True)
            df.index = df.index.tz_localize('UTC').tz_convert('America/New_York').normalize()


            # 添加兼容性列 (yfinance 会提供这些)
            df['Dividends'] = 0.0
            df['Stock Splits'] = 0.0
            # Finnhub 免费数据是未复权的，没有 Adj Close
            # 为了让 reverse_engineer_adj_factors 能运行，我们先用Close填充
            # 这意味着使用 Finnhub 作为主数据源时，复权因子将全部为1
            if 'Adj Close' not in df.columns:
                 df['Adj Close'] = df['Close']

            return df

        except Exception as e:
            logger.error(f"[FinnhubSource] 调用 Finnhub 获取 {symbol} 历史数据时出错: {e}", exc_info=True)
            return pd.DataFrame()

