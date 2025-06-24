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


# --- 【核心修改部分：时点校正版】 ---
def reverse_engineer_adj_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    从雅虎财经数据中反向工程复权因子，采用时点校正策略。
    直接计算理论事件因子和实际事件因子，然后进行比较。
    """
    logger.info("开始反向工程复权因子 (时点校正版)...")

    # 1. 数据预处理
    df.rename(columns={'Adj Close': 'yahoo_adj_close', 'Stock Splits': 'split_ratio'}, inplace=True)
    df.sort_index(ascending=True, inplace=True)

    for col in ['split_ratio', 'Dividends']:
        if col in df.columns:
            df[col] = df[col].fillna(0)
        else:
            df[col] = 0.0

    df['Close'] = df['Close'].replace(0, np.nan).ffill()
    df['yahoo_adj_close'] = df['yahoo_adj_close'].replace(0, np.nan).ffill()

    if df.empty or 'yahoo_adj_close' not in df.columns or df['Close'].isnull().all():
        logger.warning("数据框为空或缺少关键价格列，无法进行复权因子计算。")
        df['adj_factor'] = 1.0
        df['event_factor'] = 1.0
        df['black_box_factor'] = np.nan
        return df

    # 2. 计算累积复权因子 (adj_factor) - 用于高性能计算
    df['adj_factor'] = df['yahoo_adj_close'] / df['Close']
    df['adj_factor'] = df['adj_factor'].replace([np.inf, -np.inf], np.nan).bfill()

    # 3. 计算实际事件因子 (event_factor) - 这是包含所有事件的“黄金标准”
    # event_factor_t-1 = adj_factor_t-1 / adj_factor_t
    df['next_adj_factor'] = df['adj_factor'].shift(-1)
    df['event_factor'] = df['adj_factor'] / df['next_adj_factor']
    df['event_factor'].fillna(1.0, inplace=True)
    df.drop(columns=['next_adj_factor'], inplace=True)

    # 4. 计算理论事件因子 (theoretical_event_factor) - 这是仅包含白盒事件的理论值
    # 我们需要 T-1 日的收盘价和 T 日的公司行动
    df['prev_close'] = df['Close'].shift(1)

    # 计算理论分红乘数
    dividend_multiplier = pd.Series(1.0, index=df.index)
    mask_div = (df['Dividends'] > 0) & (df['prev_close'] > 0)
    dividend_multiplier[mask_div] = (df['prev_close'][mask_div] - df['Dividends'][mask_div]) / df['prev_close'][
        mask_div]

    # 计算理论拆股乘数
    split_multiplier = pd.Series(1.0, index=df.index)
    mask_split = (df['split_ratio'] > 0) & (df['split_ratio'] != 1.0)
    split_multiplier[mask_split] = 1.0 / df['split_ratio'][mask_split]

    # 理论总乘数 (在 T 日发生)
    theoretical_multiplier_on_T = dividend_multiplier * split_multiplier

    # 将 T 日的理论乘数赋给 T-1 日，形成我们的 theoretical_event_factor
    df['theoretical_event_factor'] = theoretical_multiplier_on_T.shift(1).fillna(1.0)

    df.drop(columns=['prev_close'], inplace=True)

    # 5. 识别黑盒事件
    df['black_box_factor'] = np.nan
    # 比较实际事件因子和理论事件因子
    # 当一个显著的实际事件发生时，检查它是否能被理论事件解释
    significant_events_mask = ~np.isclose(df['event_factor'], 1.0, rtol=1e-8, atol=1e-9)
    explained_events_mask = np.isclose(df['event_factor'], df['theoretical_event_factor'], rtol=1e-5)

    unexplained_events_mask = significant_events_mask & ~explained_events_mask

    if unexplained_events_mask.any():
        for date in df.index[unexplained_events_mask]:
            actual = df.loc[date, 'event_factor']
            theoretical = df.loc[date, 'theoretical_event_factor']

            if theoretical == 0:
                correction = np.inf
            else:
                correction = actual / theoretical

            df.loc[date, 'black_box_factor'] = correction

            logger.warning(
                f"[{date.date()}] 发现黑盒事件! "
                f"实际事件因子: {actual:.8f}, "
                f"理论事件因子: {theoretical:.8f}, "
                f"修正系数: {correction:.8f}"
            )

    df.drop(columns=['theoretical_event_factor'], inplace=True)

    logger.success("复权因子反向工程完成。")
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

        adjustments_df = processed_df[
            pd.notna(processed_df['black_box_factor']) &
            np.isfinite(processed_df['black_box_factor'])
            ]

        if not adjustments_df.empty:
            logger.info(f"为 {symbol} 找到 {len(adjustments_df)} 个特殊调整事件，正在存入数据库...")
            adjustments_to_insert = []
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
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)
