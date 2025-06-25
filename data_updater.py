# data_updater.py
import numpy as np
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta

from db_manager import DatabaseManager
from data_models.models import MarketType, AssetType, ActionType, DailyPrice, CorporateAction, SpecialAdjustment, \
    Security


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
            logger.warning(f"无法获取 {symbol} 的有效信息，可能已退市或代码无效。将is_active设为False。")
            # --- OPTIMIZATION START: 如果股票无效，则在数据库中标记为非活跃 ---
            security_id = db_manager.get_or_create_security_id(symbol)
            with db_manager.get_session() as session:
                session.query(Security).filter(Security.id == security_id).update({'is_active': False})
            # --- OPTIMIZATION END ---
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
            'is_active': True,  # 确保获取到信息的股票是活跃的
            'list_date': pd.to_datetime(info.get('firstTradeDateEpochUtc'), unit='s').date() if info.get(
                'firstTradeDateEpochUtc') else None
        }

        security_data = {k: v for k, v in security_data.items() if v is not None}
        db_manager.upsert_security_info(security_data)

    except Exception as e:
        logger.error(f"为 {symbol} 更新基本信息时出错: {e}")


def reverse_engineer_adj_factors(df: pd.DataFrame) -> pd.DataFrame:
    # ... (此函数内容无变化)
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


def update_historical_data(db_manager: DatabaseManager, symbol: str, full_refresh: bool = False):
    """
    更新股票的历史价格数据。
    默认为增量更新，可选择全量刷新。
    成功后，更新 Security 表中的状态。
    """
    try:
        # --- OPTIMIZATION: 在获取数据前先获取ID，并使用新的 get_last_price_date ---
        security_id = db_manager.get_or_create_security_id(symbol)
        if not security_id:
            logger.error(f"无法为 {symbol} 获取或创建 security_id，跳过历史数据更新。")
            return

        ticker = yf.Ticker(symbol)
        start_date = None

        if full_refresh:
            logger.info(f"开始为 {symbol} (ID: {security_id}) 执行【全量刷新】...")
            df = ticker.history(period="max", interval="1d", auto_adjust=False)
        else:
            last_date = db_manager.get_last_price_date(security_id)
            if last_date:
                start_date = last_date - timedelta(days=2)  # 多获取几天以防数据修正
                logger.info(f"开始为 {symbol} (ID: {security_id}) 执行【增量更新】，从 {start_date} 开始...")
                df = ticker.history(start=start_date, interval="1d", auto_adjust=False)
            else:
                logger.info(f"数据库中无 {symbol} (ID: {security_id}) 数据，自动执行【首次全量获取】...")
                df = ticker.history(period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 在指定时间段内没有可用的历史价格数据。")
            # --- OPTIMIZATION START: 如果没有获取到数据，但之前有，则更新状态为最后一天 ---
            last_db_date = db_manager.get_last_price_date(security_id)
            if last_db_date:
                db_manager.update_security_latest_price_date(security_id, last_db_date)
            # --- OPTIMIZATION END ---
            return

        processed_df = reverse_engineer_adj_factors(df.copy())
        processed_df.reset_index(inplace=True)

        prices_to_insert = [
            # ... (字典推导式保持不变)
            {
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
            } for row in processed_df.to_dict('records')
        ]

        if not prices_to_insert:
            logger.warning(f"为 {symbol} 处理后没有价格数据可插入。")
            return

        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # --- OPTIMIZATION START: 数据入库后，更新状态 ---
        latest_date_in_batch = max(p['date'] for p in prices_to_insert)
        db_manager.update_security_latest_price_date(security_id, latest_date_in_batch)
        # --- OPTIMIZATION END ---

        # ... (CorporateAction 的处理逻辑保持不变) ...
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
