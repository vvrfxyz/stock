# data_updater.py
import numpy as np
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta, timezone

from db_manager import DatabaseManager
from data_models.models import MarketType, AssetType, ActionType, DailyPrice, CorporateAction, Security
# 导入数据源抽象接口
from data_sources.base import DataSourceInterface


def _map_yfinance_info_to_types(symbol: str, info: dict) -> tuple[MarketType, AssetType]:
    """
    根据 yfinance 的 info 字典和 symbol 映射到内部的市场和资产类型。
    """
    # 1. 确定资产类型 (AssetType)
    quote_type = info.get('quoteType')
    if quote_type == 'ETF':
        asset_type = AssetType.ETF
    elif quote_type == 'INDEX':
        asset_type = AssetType.INDEX
    elif quote_type == 'CRYPTOCURRENCY':
        asset_type = AssetType.CRYPTO
    # yfinance 对外汇的表示不统一，这里可以根据需要扩展
    elif quote_type == 'CURRENCY':
        asset_type = AssetType.FOREX
    else:
        asset_type = AssetType.STOCK  # 默认为股票

    # 2. 确定市场类型 (MarketType)
    symbol_upper = symbol.upper()
    if ".SS" in symbol_upper or ".SZ" in symbol_upper:
        market = MarketType.CNA
    elif ".HK" in symbol_upper:
        market = MarketType.HK
    elif asset_type == AssetType.CRYPTO:
        market = MarketType.CRYPTO
    elif asset_type == AssetType.FOREX:
        market = MarketType.FOREX
    else:
        # 默认为美股，对于指数等也可以归类于此
        market = MarketType.US
        if asset_type == AssetType.INDEX:
            market = MarketType.INDEX

    return market, asset_type


def update_stock_info(db_manager: DatabaseManager, symbol: str, data_source: DataSourceInterface,
                      force_update: bool = False):
    """使用传入的 data_source 更新股票基本信息"""
    symbol = symbol.lower()

    if not force_update:
        with db_manager.get_session() as session:
            security = session.query(Security.info_last_updated_at).filter(Security.symbol == symbol).first()
            if security and security.info_last_updated_at > (datetime.now(timezone.utc) - timedelta(days=30)):
                logger.trace(f"[{symbol}] 的基本信息在30天内已更新，跳过此次API请求。")
                return

    logger.info(f"开始为 {symbol} 获取基本信息...")
    try:
        # 使用抽象接口获取数据
        info = data_source.get_security_info(symbol)

        if not info:
            logger.warning(f"无法获取 {symbol} 的有效信息，可能已退市或代码无效。将is_active设为False。")
            with db_manager.get_session() as session:
                # 尝试找到记录并更新，如果找不到则不操作
                session.query(Security).filter(Security.symbol == symbol).update({'is_active': False})
            return

        # 使用新的映射函数
        market, asset_type = _map_yfinance_info_to_types(symbol, info)

        security_data = {
            'symbol': symbol,
            'name': info.get('longName') or info.get('shortName'),
            'market': market,
            'type': asset_type,
            'exchange': info.get('exchange'),
            'currency': info.get('currency'),
            'sector': info.get('sector'),
            'industry': info.get('industry'),
            'is_active': True,
            'list_date': pd.to_datetime(info.get('firstTradeDateEpochUtc'), unit='s').date() if info.get(
                'firstTradeDateEpochUtc') else None
        }

        security_data = {k: v for k, v in security_data.items() if v is not None}
        db_manager.upsert_security_info(security_data)

    except Exception as e:
        logger.error(f"为 {symbol} 更新基本信息时出错: {e}", exc_info=True)


def reverse_engineer_adj_factors(df: pd.DataFrame) -> pd.DataFrame:
    # ... 此函数内部逻辑不变 ...
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


def update_historical_data(db_manager: DatabaseManager, symbol: str, data_source: DataSourceInterface,
                           full_refresh: bool = False):
    """使用传入的 data_source 更新历史价格数据"""
    try:
        security_id = db_manager.get_or_create_security_id(symbol)
        if not security_id:
            logger.error(f"无法为 {symbol} 获取或创建 security_id，跳过历史数据更新。")
            return

        if not full_refresh:
            latest_db_details = db_manager.get_latest_daily_price_details(security_id)
            if latest_db_details:
                verification_date = latest_db_details['date']
                stored_adj_factor = latest_db_details['adj_factor']

                logger.info(
                    f"开始为 {symbol} (ID: {security_id}) 执行【增量更新】，从 {verification_date} 开始获取数据以进行验证...")
                start_date_for_fetch = verification_date
                # 使用抽象接口获取数据
                df = data_source.get_historical_data(symbol, start=start_date_for_fetch.strftime('%Y-%m-%d'),
                                                     interval="1d", auto_adjust=False)

                if df.empty:
                    logger.warning(f"[{symbol}] 在 {start_date_for_fetch} 之后没有从数据源获取到新数据，可能已是最新。")
                    db_manager.update_security_latest_price_date(security_id, verification_date)
                    return

                processed_df_for_check = reverse_engineer_adj_factors(df.copy())
                processed_df_for_check.reset_index(inplace=True)

                check_row = processed_df_for_check[processed_df_for_check['Date'].dt.date == verification_date]
                if not check_row.empty:
                    new_adj_factor = check_row.iloc[0]['adj_factor']
                    if not np.isclose(stored_adj_factor, new_adj_factor, atol=1e-6):
                        logger.warning(
                            f"[{symbol}] 数据校验失败: 日期 {verification_date} 的 adj_factor 不匹配。"
                            f"数据库值: {stored_adj_factor:.6f}, 新计算值: {new_adj_factor:.6f}。触发全量更新！"
                        )
                        update_historical_data(db_manager, symbol, data_source, full_refresh=True)
                        return
                    else:
                        logger.info(f"[{symbol}] 数据校验通过，日期 {verification_date} 的 adj_factor 一致。")
                else:
                    logger.warning(
                        f"[{symbol}] 从数据源获取的数据中不包含验证日期 {verification_date}，将尝试全量更新以修复。")
                    update_historical_data(db_manager, symbol, data_source, full_refresh=True)
                    return
            else:
                logger.info(f"数据库中无 {symbol} (ID: {security_id}) 数据，自动执行【首次全量获取】...")
                full_refresh = True

        if full_refresh:
            logger.info(f"开始为 {symbol} (ID: {security_id}) 执行【全量刷新】...")
            # 使用抽象接口获取数据
            df = data_source.get_historical_data(symbol, period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 在指定时间段内没有可用的历史价格数据。")
            last_db_date = db_manager.get_last_price_date(security_id)
            if last_db_date:
                db_manager.update_security_latest_price_date(security_id, last_db_date)
            return

        processed_df = reverse_engineer_adj_factors(df.copy())
        processed_df.reset_index(inplace=True)
        prices_to_insert = [
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

        latest_date_in_batch = max(p['date'] for p in prices_to_insert)
        db_manager.update_security_latest_price_date(security_id, latest_date_in_batch)

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

        if full_refresh:
            db_manager.update_security_full_refresh_timestamp(security_id)

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)

