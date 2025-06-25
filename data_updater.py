# data_updater.py
import numpy as np
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta

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
    # ... (此函数内容无变化, 故省略以保持简洁)
    logger.info("开始简化计算复权因子 (v2 - 已修正)...")
    df_copy = df.copy()
    if 'Stock Splits' in df_copy.columns:
        df_copy.rename(columns={'Stock Splits': 'split_ratio'}, inplace=True)
    df_copy.sort_index(ascending=True, inplace=True)
    required_cols = ['Close', 'Adj Close', 'Dividends', 'split_ratio']
    if not all(col in df_copy.columns for col in required_cols):
        missing_cols = [col for col in required_cols if col not in df_copy.columns]
        raise ValueError(f"输入数据缺少必要列: {missing_cols}")
    for col in ['Dividends', 'split_ratio']:
        df_copy[col] = df_copy[col].fillna(0)
    for col in ['Close', 'Adj Close']:
        df_copy[col] = df_copy[col].replace(0, np.nan).ffill()
    if df_copy.empty or df_copy['Close'].isnull().all():
        logger.warning("数据框为空或缺少有效的价格数据，返回默认因子。")
        for col in ['adj_factor', 'cal_event_factor', 'event_factor']:
            df_copy[col] = 1.0
        return df_copy
    df_copy['adj_factor'] = df_copy['Adj Close'] / df_copy['Close']
    df_copy['adj_factor'] = df_copy['adj_factor'].replace([np.inf, -np.inf], np.nan)
    df_copy['adj_factor'] = df_copy['adj_factor'].bfill()
    df_copy['adj_factor'] = df_copy['adj_factor'].ffill()
    df_copy['cal_event_factor'] = df_copy['adj_factor'] / df_copy['adj_factor'].shift(1)
    if not df_copy.empty:
        df_copy.loc[df_copy.index[0], 'cal_event_factor'] = 1.0
    df_copy.loc[np.isclose(df_copy['cal_event_factor'], 1.0), 'cal_event_factor'] = 1.0
    df_copy['event_factor'] = 1.0
    df_copy['prev_close'] = df_copy['Close'].shift(1)
    mask_div = (df_copy['Dividends'] > 0) & (df_copy['prev_close'] > 0)
    df_copy.loc[mask_div, 'event_factor'] = \
        df_copy['prev_close'] / (df_copy['prev_close'] - df_copy['Dividends'])
    mask_split = (df_copy['split_ratio'] > 0) & (df_copy['split_ratio'] != 1.0)
    df_copy.loc[mask_split, 'event_factor'] *= df_copy['split_ratio']
    df_copy.drop(columns=['prev_close'], inplace=True)
    for factor_col in ['adj_factor', 'cal_event_factor', 'event_factor']:
        if factor_col in df_copy.columns:
            df_copy[factor_col] = df_copy[factor_col].round(6)
    logger.info("简化版复权因子计算完成。")
    final_cols = ['Close', 'Adj Close', 'Dividends', 'split_ratio']
    factor_cols = ['adj_factor', 'cal_event_factor', 'event_factor']
    other_cols = [col for col in df.columns if col not in ['Close', 'Adj Close', 'Dividends', 'Stock Splits']]
    all_expected_cols = other_cols + final_cols + factor_cols
    final_ordered_cols = [col for col in all_expected_cols if col in df_copy.columns]
    return df_copy[final_ordered_cols]


# --- OPTIMIZATION START: 修改函数签名和实现 ---
def update_historical_data(db_manager: DatabaseManager, symbol: str, full_refresh: bool = False):
    """
    更新股票的历史价格数据。
    默认为增量更新，可选择全量刷新。
    :param db_manager: DatabaseManager 实例。
    :param symbol: 股票代码。
    :param full_refresh: 若为 True，则强制获取全部历史数据。
    """
    try:
        security_id = db_manager.get_or_create_security_id(symbol)
        ticker = yf.Ticker(symbol)
        start_date = None

        if full_refresh:
            logger.info(f"开始为 {symbol} 执行【全量刷新】...")
            # 使用 period='max' 进行全量获取
            df = ticker.history(period="max", interval="1d", auto_adjust=False)
        else:
            # 增量更新逻辑
            last_date = db_manager.get_last_price_date(security_id)
            if last_date:
                # 从已知的最后一天开始获取，以覆盖当天可能的数据修正
                start_date = last_date
                logger.info(f"开始为 {symbol} 执行【增量更新】，从 {start_date} 开始...")
                df = ticker.history(start=start_date, interval="1d", auto_adjust=False)
            else:
                # 数据库中无数据，自动执行首次全量获取
                logger.info(f"数据库中无 {symbol} 数据，自动执行【首次全量获取】...")
                df = ticker.history(period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 在指定时间段内没有可用的历史价格数据。")
            return
# --- OPTIMIZATION END ---

        # 以下数据处理和入库逻辑保持不变
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
                'adj_close': row.get('Adj Close'),
                'adj_factor': row.get('adj_factor'),
                'event_factor': row.get('event_factor'),
                'cal_event_factor': row.get('cal_event_factor')
            })

        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

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

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)
