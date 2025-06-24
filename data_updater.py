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


# --- 【核心修改部分：根据新逻辑简化】 ---
def reverse_engineer_adj_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    从雅虎财经数据中反向工程复权因子，并计算前复权价格。
    - adj_factor (后复权因子) = adj_close / close
    - event_factor (日度事件因子) = adj_factor(t) / adj_factor(t-1)
    - forward_adj_close (前复权价) = close * cumprod(event_factor)
    """
    logger.info("开始计算后复权因子、事件因子和前复权价格...")
    # 1. 数据预处理
    df.rename(columns={'Adj Close': 'yahoo_adj_close', 'Stock Splits': 'split_ratio'}, inplace=True)
    df.sort_index(ascending=True, inplace=True)
    for col in ['split_ratio', 'Dividends']:
        df[col] = df[col].fillna(0)
    df['Close'] = df['Close'].replace(0, np.nan).ffill()
    df['yahoo_adj_close'] = df['yahoo_adj_close'].replace(0, np.nan).ffill()
    if df.empty or 'yahoo_adj_close' not in df.columns or df['Close'].isnull().all():
        logger.warning("数据框为空或缺少关键价格列，无法计算。")
        df['adj_factor'] = 1.0
        df['event_factor'] = 1.0
        df['forward_adj_close'] = df['Close']
        return df
    # 2. 计算后复权因子 (adj_factor) - 保持完整精度
    df['adj_factor'] = df['yahoo_adj_close'] / df['Close']
    df['adj_factor'] = df['adj_factor'].replace([np.inf, -np.inf], np.nan).bfill().ffill()
    # 3. 计算日度事件因子 (event_factor) - 保持完整精度
    df['prev_adj_factor'] = df['adj_factor'].shift(1)
    # 使用 fillna(1.0) 来处理第一天的 NaN 值
    df['event_factor'] = (df['adj_factor'] / df['prev_adj_factor']).fillna(1.0)

    # 4. 清理与最终四舍五入（仅用于存储或展示）
    df.drop(columns=['prev_adj_factor'], inplace=True)

    # 对需要存入数据库的列进行四舍五入
    df['adj_factor'] = df['adj_factor'].round(6)
    df['event_factor'] = df['event_factor'].round(6)
    logger.success("复权因子和前复权价格计算完成。")
    return df


def update_historical_data(db_manager: DatabaseManager, symbol: str):
    logger.info(f"开始为 {symbol} 获取全部历史数据...")
    try:
        security_id = db_manager.get_or_create_security_id(symbol)
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 没有可用的历史价格数据。")
            return

        processed_df = reverse_engineer_adj_factors(df.copy())
        processed_df.reset_index(inplace=True)

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
                'adj_close': row.get('yahoo_adj_close'),
                'adj_factor': row.get('adj_factor'),
                'event_factor': row.get('event_factor')
            })

        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # 逻辑点 3: 白盒事件正常记录
        actions_to_insert = []
        actions_df = processed_df[(processed_df['Dividends'] > 0) | (
                (processed_df['split_ratio'] > 0) & (processed_df['split_ratio'] != 1.0))]

        for row in actions_df.to_dict('records'):
            if row['Dividends'] > 0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.DIVIDEND,
                     'value': row['Dividends']})
            if row['split_ratio'] > 0 and row['split_ratio'] != 1.0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.SPLIT,
                     'value': row['split_ratio']})

        if actions_to_insert:
            db_manager.bulk_upsert(CorporateAction, actions_to_insert,
                                   ['security_id', 'event_date', 'event_type'],
                                   constraint='_security_date_type_uc')

        # 逻辑点 4: 黑盒事件暂不记录 (相关代码已移除)
        # 原先在此处有对 special_adjustments 的处理，现已删除。

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)

