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
    通过混合模型精确计算复权因子，输出混合结果：
    - adj_factor: 用于【后复权】的累积调整因子。
    - event_factor: 用于【前复权】的独立事件调整因子。
    """
    logger.info("开始通过混合模型计算复权因子（后复权累积因子 + 前复权事件因子）...")
    # 1. 数据预处理 (与原版相同)
    df.rename(columns={'Adj Close': 'yahoo_adj_close', 'Stock Splits': 'split_ratio'}, inplace=True)
    df.sort_index(ascending=True, inplace=True)
    for col in ['split_ratio', 'Dividends']:
        df[col] = df[col].fillna(0)
    df['Close'] = df['Close'].replace(0, np.nan).ffill()
    df['yahoo_adj_close'] = df['yahoo_adj_close'].replace(0, np.nan).ffill()
    if df.empty or 'Close' not in df.columns or df['Close'].isnull().all():
        logger.warning("数据框为空或缺少关键价格列，无法计算。")
        df['adj_factor'] = 1.0
        df['event_factor'] = 1.0
        return df
    # 2. 计算“白盒”理论事件因子 (theoretical_event_factor) (与原版相同)
    # 这是后复权视角下的事件因子，即 Price_After / Price_Before
    df['theoretical_event_factor'] = 1.0
    df['prev_close'] = df['Close'].shift(1)
    mask_div = (df['Dividends'] > 0) & (df['prev_close'] > 0)
    df.loc[mask_div, 'theoretical_event_factor'] = (df['prev_close'][mask_div] - df['Dividends'][mask_div]) / \
                                                   df['prev_close'][mask_div]
    mask_split = (df['split_ratio'] > 0) & (df['split_ratio'] != 1.0)
    df.loc[mask_split, 'theoretical_event_factor'] *= (1.0 / df['split_ratio'][mask_split])
    # 3. 计算“实际”事件因子 (actual_event_factor) (与原版相同)
    temp_adj_factor = df['yahoo_adj_close'] / df['Close']
    temp_adj_factor = temp_adj_factor.replace([np.inf, -np.inf], np.nan).bfill().ffill()
    df['actual_event_factor'] = temp_adj_factor.shift(1) / temp_adj_factor
    df['actual_event_factor'].iloc[0] = 1.0
    # 4. 识别并隔离“黑盒”事件 (核心逻辑) (与原版相同)
    df['black_box_factor'] = 1.0
    tolerance = 1e-5
    unexplained_mask = (
            (np.abs(df['actual_event_factor'] - df['theoretical_event_factor']) > tolerance) &
            (df['theoretical_event_factor'] != 0)
    )
    if unexplained_mask.any():
        logger.warning(f"发现 {unexplained_mask.sum()} 个疑似黑盒事件（如配股、增发等）。")
        df.loc[unexplained_mask, 'black_box_factor'] = df['actual_event_factor'][unexplained_mask] / \
                                                       df['theoretical_event_factor'][unexplained_mask]
        for date in df.index[unexplained_mask]:
            logger.info(f"  - 日期: {date.date()}, "
                        f"实际因子: {df.loc[date, 'actual_event_factor']:.6f}, "
                        f"理论因子: {df.loc[date, 'theoretical_event_factor']:.6f}, "
                        f"推断黑盒因子: {df.loc[date, 'black_box_factor']:.6f}")
    # 5. 生成最终的、干净的后复权事件因子 (backward_event_factor)
    # 为了清晰，我们称之为 backward_event_factor，因为它用于计算后复权 adj_factor
    df['backward_event_factor'] = df['theoretical_event_factor'] * df['black_box_factor']
    # 6. 生成最终的【后复权】adj_factor (基于我们干净的 backward_event_factor)
    # 此部分逻辑保持原样，不做任何改动
    reversed_event_factor = df['backward_event_factor'].iloc[::-1]
    reversed_adj_factor = pd.Series(np.nan, index=reversed_event_factor.index)
    reversed_adj_factor.iloc[0] = 1.0
    for i in range(1, len(reversed_adj_factor)):
        reversed_adj_factor.iloc[i] = reversed_adj_factor.iloc[i - 1] * reversed_event_factor.iloc[i - 1]
    df['adj_factor'] = reversed_adj_factor.iloc[::-1]
    last_yahoo_factor = df['yahoo_adj_close'].iloc[-1] / df['Close'].iloc[-1]
    df['adj_factor'] *= last_yahoo_factor
    # ==============================================================================
    # 7. 【核心修改】生成最终的【前复权】event_factor
    # 定义：前复权事件因子是后复权事件因子的倒数。
    # 在非事件日，两者都为1。在事件日，如果后复权因子是 E (<1)，则前复权因子是 1/E (>1)。
    # 我们直接使用已经计算好的 backward_event_factor 来生成最终的 event_factor 列。

    # 使用 np.where 进行高效、安全的转换
    # 条件：当 backward_event_factor 不为1且不为0时，取其倒数，否则保持为1
    df['event_factor'] = np.where(
        (df['backward_event_factor'] != 1.0) & (df['backward_event_factor'] != 0),
        1.0 / df['backward_event_factor'],
        1.0
    )
    # ==============================================================================
    # 8. 清理与最终四舍五入
    df.drop(columns=['prev_close', 'theoretical_event_factor', 'actual_event_factor', 'backward_event_factor'],
            inplace=True)
    df['adj_factor'] = df['adj_factor'].round(6)
    df['event_factor'] = df['event_factor'].round(6)
    df['black_box_factor'] = df['black_box_factor'].round(6)

    logger.success("混合复权因子计算完成：adj_factor (后复权), event_factor (前复权)。")
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

