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
    根据明确的定义，简化计算复权因子。

    1. adj_factor: 后复权累积因子 (adj_close / close)。
    2. cal_event_factor: adj_factor的每日变化率。
    3. event_factor: 基于分红和拆股事件的前复权事件因子。
    """
    print("开始简化计算复权因子...")

    # --- 步骤 0: 数据预处理 (与原版基本一致) ---
    # 为了代码清晰，直接使用新列名，避免混淆
    df_copy = df.copy()
    df_copy.rename(columns={
        'Adj Close': 'adj_close',
        'Stock Splits': 'split_ratio',
        'Dividends': 'dividends',
        'Close': 'close'
    }, inplace=True)

    df_copy.sort_index(ascending=True, inplace=True)

    for col in ['split_ratio', 'dividends']:
        df_copy[col] = df_copy[col].fillna(0)

    # 填充价格数据中的0或NaN值，防止计算错误
    for col in ['close', 'adj_close']:
        df_copy[col] = df_copy[col].replace(0, np.nan).ffill()

    if df_copy.empty or 'close' not in df_copy.columns or df_copy['close'].isnull().all():
        print("警告: 数据框为空或缺少关键价格列，无法计算。")
        df['adj_factor'] = 1.0
        df['cal_event_factor'] = 1.0
        df['event_factor'] = 1.0
        return df

    # --- 步骤 1: 计算 adj_factor (后复权累积因子) ---
    # 定义: adj_factor = adj_close / close
    df_copy['adj_factor'] = df_copy['adj_close'] / df_copy['close']
    # 处理可能出现的无穷大值或空值
    df_copy['adj_factor'].replace([np.inf, -np.inf], np.nan, inplace=True)
    df_copy['adj_factor'].bfill(inplace=True)  # 从后向前填充，保证序列末端有值
    df_copy['adj_factor'].ffill(inplace=True)  # 从前向后填充，处理剩余空值

    # --- 步骤 2 & 3: 计算 cal_event_factor 并修正 ---
    # 定义: cal_event_factor = adj_factor(t) / adj_factor(t-1)
    df_copy['cal_event_factor'] = df_copy['adj_factor'] / df_copy['adj_factor'].shift(1)
    df_copy['cal_event_factor'].iloc[0] = 1.0  # 第一天的变化率定义为1

    # 修正接近1的浮点数误差
    # 使用 np.isclose 比直接比较更安全
    df_copy.loc[np.isclose(df_copy['cal_event_factor'], 1.0), 'cal_event_factor'] = 1.0

    # --- 步骤 4: 计算 event_factor (前复权事件因子) ---
    # 定义: 仅根据分红和拆股事件计算，用于前复权
    # 前复权因子 = Price_Before / Price_After
    df_copy['event_factor'] = 1.0
    df_copy['prev_close'] = df_copy['close'].shift(1)

    # 分红事件因子: prev_close / (prev_close - dividend)
    mask_div = (df_copy['dividends'] > 0) & (df_copy['prev_close'] > 0)
    df_copy.loc[mask_div, 'event_factor'] = df_copy['prev_close'][mask_div] / \
                                            (df_copy['prev_close'][mask_div] - df_copy['dividends'][mask_div])

    # 拆股事件因子: split_ratio
    # 如果当天同时有分红和拆股，则累乘
    mask_split = (df_copy['split_ratio'] > 0) & (df_copy['split_ratio'] != 1.0)
    df_copy.loc[mask_split, 'event_factor'] *= df_copy['split_ratio'][mask_split]

    # --- 步骤 5: 清理并整合到原DataFrame ---
    # 将计算出的核心字段添加回原始DataFrame
    df['adj_factor'] = df_copy['adj_factor'].round(6)
    df['cal_event_factor'] = df_copy['cal_event_factor'].round(6)
    df['event_factor'] = df_copy['event_factor'].round(6)

    print("简化版复权因子计算完成。")
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
                'event_factor': row.get('event_factor'),
                'cal_event_factor': row.get('cal_event_factor')
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

