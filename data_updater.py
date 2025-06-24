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
    1. adj_factor: 累积复权因子，用于高性能计算。 (adj_price = raw_price * adj_factor)
    2. event_factor: 事件因子，仅在事件发生前一天不为1.0，代表单次调整的乘数。
    同时，它会识别理论计算（股息、拆分）无法解释的“黑盒”调整事件。
    """
    logger.info("开始反向工程复权因子...")
    # 1. 数据预处理
    df.rename(columns={'Adj Close': 'yahoo_adj_close', 'Stock Splits': 'split_ratio'}, inplace=True)
    df.sort_index(ascending=True, inplace=True)

    if 'split_ratio' in df.columns:
        df['split_ratio'].fillna(0, inplace=True)
    else:
        df['split_ratio'] = 0.0

    df['Close'] = df['Close'].replace(0, np.nan)
    df['Close'] = df['Close'].ffill()
    if df.empty or 'yahoo_adj_close' not in df.columns:
        logger.warning("数据框为空或缺少 'Adj Close' 列，无法进行复权因子计算。")
        df['adj_factor'] = 1.0
        df['event_factor'] = 1.0
        df['black_box_factor'] = np.nan
        return df
    # 2. 计算雅虎的累积复权因子 (我们的黄金标准)
    # 这个因子直接将任何一天的名义收盘价调整到与最新收盘价可比的水平。
    df['adj_factor'] = df['yahoo_adj_close'].div(df['Close']).replace([np.inf, -np.inf], np.nan)
    # 从后向前填充，因为最近的因子最可靠，可以修复数据末尾可能存在的NaN。
    df['adj_factor'] = df['adj_factor'].bfill()
    # 3. 初始化 event_factor 和 black_box_factor
    df['event_factor'] = 1.0
    df['black_box_factor'] = np.nan
    # 4. 从后向前迭代，计算每日事件因子并识别黑盒事件
    # 我们从倒数第二天开始，因为事件因子是关于两天之间关系的
    for i in range(len(df) - 2, -1, -1):
        today_row = df.iloc[i]
        tomorrow_row = df.iloc[i + 1]
        # --- a. 计算基于雅虎数据的“实际”事件因子 ---
        # 这是两天累积因子之间的比率，代表了这一天发生的总调整
        adj_factor_today = today_row['adj_factor']
        adj_factor_tomorrow = tomorrow_row['adj_factor']
        if pd.isna(adj_factor_today) or pd.isna(adj_factor_tomorrow) or adj_factor_tomorrow == 0:
            continue  # 如果因子无效，无法计算比率，跳过
        # actual_event_factor 是连接今天和明天价格的真实乘数
        actual_event_factor = adj_factor_today / adj_factor_tomorrow
        df.loc[today_row.name, 'event_factor'] = actual_event_factor
        # --- b. 根据已知的公司行动（白盒事件）计算“理论”事件因子 ---
        dividend_on_tomorrow = tomorrow_row['Dividends']
        split_ratio_on_tomorrow = tomorrow_row['split_ratio']
        # 股息调整因子：(P_t-1 - D_t) / P_t-1
        # 使用今天的收盘价作为计算基准
        today_close = today_row['Close']
        dividend_adj_ratio = 1.0
        if dividend_on_tomorrow > 0 and today_close > 0:
            dividend_adj_ratio = (today_close - dividend_on_tomorrow) / today_close
        # 拆股调整因子：1 / split_ratio
        # yfinance中，无拆股事件时split_ratio为0或1
        split_adj_ratio = 1.0
        if split_ratio_on_tomorrow > 1.0:  # 拆股
            split_adj_ratio = 1.0 / split_ratio_on_tomorrow
        elif 0 < split_ratio_on_tomorrow < 1.0:  # 并股
            split_adj_ratio = 1.0 / split_ratio_on_tomorrow

        theoretical_event_factor = dividend_adj_ratio * split_adj_ratio
        # --- c. 比较实际与理论，识别黑盒事件 ---
        # 如果实际事件因子显著不为1，但与我们的理论计算不符，则认为存在黑盒事件
        # 使用 rtol=1e-4 (0.01%) 作为容忍度，避免浮点误差
        is_significant_event = not np.isclose(actual_event_factor, 1.0, rtol=1e-9, atol=1e-9)
        is_explained = np.isclose(actual_event_factor, theoretical_event_factor, rtol=1e-4, atol=1e-9)
        if is_significant_event and not is_explained:
            # 理论因子不能为0，否则无法计算修正系数
            if theoretical_event_factor == 0:
                black_box_correction = np.inf
            else:
                black_box_correction = actual_event_factor / theoretical_event_factor
            df.loc[today_row.name, 'black_box_factor'] = black_box_correction
            logger.warning(
                f"[{today_row.name.date()}] 发现黑盒事件! "
                f"实际事件因子: {actual_event_factor:.8f}, "
                f"理论事件因子: {theoretical_event_factor:.8f} (Div: {dividend_adj_ratio:.6f}, Split: {split_adj_ratio:.6f}), "
                f"修正系数: {black_box_correction:.8f}"
            )
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

        adjustments_df = processed_df[
            pd.notna(processed_df['black_box_factor']) &
            np.isfinite(processed_df['black_box_factor'])
            ]

        # 5. 准备并存储黑盒事件数据 (SpecialAdjustment)
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
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)  # 添加 exc_info=True 以获取更详细的堆栈跟踪
