# data_updater.py
import numpy as np
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime

from db_manager import DatabaseManager
from data_models.models import MarketType, AssetType, ActionType, DailyPrice, CorporateAction, SpecialAdjustment


def _map_yfinance_ticker_to_market_type(symbol: str) -> tuple[MarketType, AssetType]:
    symbol_upper = symbol.upper()
    if ".SS" in symbol_upper or ".SZ" in symbol_upper:
        return MarketType.CNA, AssetType.STOCK
    if ".HK" in symbol_upper:
        return MarketType.HK, AssetType.STOCK
    return MarketType.US, AssetType.STOCK


def update_stock_info(db_manager: DatabaseManager, symbol: str):
    logger.info(f"开始为 {symbol} 获取基本信息...")
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info

        if not info or info.get('quoteType') is None:
            logger.warning(f"无法获取 {symbol} 的有效信息，可能已退市或代码无效。跳过。")
            return

        market, asset_type = _map_yfinance_ticker_to_market_type(symbol)

        security_data = {
            'symbol': symbol,
            'name': info.get('longName') or info.get('shortName'),
            'market': market,
            'type': asset_type,
            'exchange': info.get('exchange'),
            'currency': info.get('currency'),
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'is_active': True
        }

        security_data = {k: v for k, v in security_data.items() if v is not None}
        db_manager.upsert_security_info(security_data)

    except Exception as e:
        logger.error(f"为 {symbol} 更新基本信息时出错: {e}")


def reverse_engineer_adj_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    从雅虎财经数据中反向工程复权因子。
    该函数会计算两个核心因子：
    1. our_factor: 累积复权因子，用于将任何一天的 Close 价格直接转换为 Adj Close。
    2. event_factor: 事件因子，仅在事件发生前一天不为1，用于描述单次复权事件的乘数。
    """
    logger.info("开始反向工程复权因子，识别黑盒事件...")
    # 1. 数据预处理
    df.rename(columns={'Adj Close': 'yahoo_adj_close', 'Stock Splits': 'split_ratio'}, inplace=True)
    df.sort_index(ascending=True, inplace=True)

    # 修复 Pandas FutureWarning，使用直接赋值替代 inplace=True
    df['Close'] = df['Close'].replace(0, np.nan)
    df['Close'] = df['Close'].ffill()

    # 2. 计算雅虎的“实际”每日累积复权因子 (我们的“黄金标准”)
    df['yahoo_factor'] = df['yahoo_adj_close'].div(df['Close']).replace([np.inf, -np.inf], np.nan).ffill()

    # 3. 初始化我们将要计算的列
    df['our_factor'] = 1.0
    df['event_factor'] = 1.0
    df['black_box_factor'] = np.nan

    if df.empty:
        return df

    # 4. 从后向前迭代，构建我们自己的复权因子链条
    last_day_index = df.index[-1]
    if pd.notna(df.loc[last_day_index, 'yahoo_factor']):
        df.loc[last_day_index, 'our_factor'] = df.loc[last_day_index, 'yahoo_factor']
    else:
        # 如果最后一天因子无效，则设为1
        df.loc[last_day_index, 'our_factor'] = 1.0
        logger.warning(f"最后交易日 {last_day_index.date()} 的雅虎因子无效，已设置为1.0。")

    last_our_factor = df.loc[last_day_index, 'our_factor']

    for i in range(len(df) - 2, -1, -1):
        today_index = df.index[i]
        tomorrow_index = df.index[i + 1]
        # --- a. 根据白盒事件计算今天的“理论”累积因子 ---

        # 【修复】获取今天的收盘价作为分红计算的基准
        today_close = df.loc[today_index, 'Close']

        dividend_on_tomorrow = df.loc[tomorrow_index, 'Dividends']
        split_ratio_on_tomorrow = df.loc[tomorrow_index, 'split_ratio']
        # 【修复】使用 today_close 计算分红调整比例
        dividend_adj_ratio = (
                                         today_close - dividend_on_tomorrow) / today_close if today_close > 0 and dividend_on_tomorrow > 0 else 1.0

        split_ratio = df.loc[tomorrow_index, 'split_ratio']
        # yfinance中，无拆股事件时split_ratio为0
        if split_ratio > 0 and split_ratio != 1.0:
            split_adj_ratio = 1.0 / split_ratio
        else:
            split_adj_ratio = 1.0

        theoretical_event_factor = dividend_adj_ratio * split_adj_ratio
        theoretical_our_factor_today = last_our_factor * theoretical_event_factor
        # --- b. 获取雅虎的“实际”累积因子 ---
        yahoo_factor_today = df.loc[today_index, 'yahoo_factor']
        # 如果雅虎因子无效，跳过本次比较，直接沿用理论值
        if pd.isna(yahoo_factor_today):
            logger.warning(f"[{today_index.date()}] 雅虎因子无效，使用理论值。")
            our_factor_today = theoretical_our_factor_today
        # --- c. 比较理论与现实，识别黑盒事件 ---
        elif not np.isclose(theoretical_our_factor_today, yahoo_factor_today, rtol=1e-6, atol=1e-9):
            black_box_correction = yahoo_factor_today / theoretical_our_factor_today
            df.loc[today_index, 'black_box_factor'] = black_box_correction
            logger.warning(
                f"[{today_index.date()}] 发现黑盒事件! "
                f"理论因子: {theoretical_our_factor_today:.8f}, "
                f"雅虎因子: {yahoo_factor_today:.8f}, "
                f"修正系数: {black_box_correction:.8f}"
            )

            our_factor_today = yahoo_factor_today
        else:
            our_factor_today = theoretical_our_factor_today
        # --- d. 存储计算结果 ---
        df.loc[today_index, 'our_factor'] = our_factor_today

        event_factor_today = our_factor_today / last_our_factor if last_our_factor != 0 else 1.0
        df.loc[today_index, 'event_factor'] = event_factor_today
        # --- e. 更新循环变量 ---
        last_our_factor = our_factor_today

    logger.success("复权因子反向工程完成。")
    return df

def update_historical_data(db_manager: DatabaseManager, symbol: str):
    logger.info(f"开始为 {symbol} 获取全部历史数据...")
    try:
        # 1. 直接获取 Security ID，而不是整个对象
        security_id = db_manager.get_or_create_security_id(symbol)

        # 2. 获取数据
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 没有可用的历史价格数据。")
            return

        # 传递一个副本以避免 SettingWithCopyWarning
        processed_df = reverse_engineer_adj_factors(df.copy())
        processed_df.reset_index(inplace=True)

        # 3. 准备价格数据 (DailyPrice)
        prices_to_insert = []
        for row in processed_df.to_dict('records'):
            prices_to_insert.append({
                'security_id': security_id,
                'date': row['Date'].date(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'adj_close': row.get('yahoo_adj_close'), # 存储雅虎的后复权价，作为参考
                'adj_factor': row.get('our_factor'), # 存储我们自己计算的、最精确的因子
                'event_factor': row.get('event_factor')
            })

        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # 4. 准备公司行动数据 (CorporateAction)
        actions_to_insert = []
        # 从处理过的 dataframe 中提取白盒事件
        actions_df = processed_df[(processed_df['Dividends'] > 0) | (processed_df['split_ratio'] > 0)]

        for row in actions_df.to_dict('records'):
            if row['Dividends'] > 0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.DIVIDEND,
                     'value': row['Dividends']})
            if row['split_ratio'] > 0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.SPLIT,
                     'value': row['split_ratio']})

        if actions_to_insert:
            db_manager.bulk_upsert(CorporateAction, actions_to_insert,
                                   ['security_id', 'event_date', 'event_type'],
                                   constraint='_security_date_type_uc')

        # 5. 准备并存储黑盒事件数据 (SpecialAdjustment)
        adjustments_to_insert = []
        adjustments_df = processed_df[processed_df['black_box_factor'].notna()]

        if not adjustments_df.empty:
            logger.info(f"为 {symbol} 找到 {len(adjustments_df)} 个特殊调整事件，正在存入数据库...")
            for row in adjustments_df.to_dict('records'):
                adjustments_to_insert.append({
                    'security_id': security_id,
                    'event_date': row['Date'].date(),
                    'adjustment_factor': row['black_box_factor'],
                    'description': f"Unexplained adjustment factor detected on {row['Date'].date()}"
                })
            db_manager.bulk_upsert(
                SpecialAdjustment, adjustments_to_insert,
                index_elements=['security_id', 'event_date'],
                constraint='_security_date_uc'
            )

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)  # 添加 exc_info=True 以获取更详细的堆栈跟踪
