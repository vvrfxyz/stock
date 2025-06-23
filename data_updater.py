# data_updater.py
import yfinance as yf
import pandas as pd
from loguru import logger
from datetime import datetime

from db_manager import DatabaseManager
from data_models.models import MarketType, AssetType, ActionType, DailyPrice, CorporateAction


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


def update_historical_data(db_manager: DatabaseManager, symbol: str):
    logger.info(f"开始为 {symbol} 获取全部历史数据...")
    try:
        # --- 【关键修改】 ---
        # 1. 直接获取 Security ID，而不是整个对象
        security_id = db_manager.get_or_create_security_id(symbol)

        # 2. 获取数据
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", interval="1d", auto_adjust=False)

        if df.empty:
            logger.warning(f"{symbol} 没有可用的历史价格数据。")
            return

        df.rename(columns={'Adj Close': 'adj_close_backward'}, inplace=True)
        df.reset_index(inplace=True)

        # 3. 准备价格数据 (DailyPrice)
        prices_to_insert = []
        for row in df.to_dict('records'):
            prices_to_insert.append({
                'security_id': security_id,
                'date': row['Date'].date(),
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'adj_close_backward': row.get('adj_close_backward'),
            })

        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # 4. 准备公司行动数据 (CorporateAction)
        actions_to_insert = []
        actions_df = df[(df['Dividends'] > 0) | (df['Stock Splits'] > 0)]

        for row in actions_df.to_dict('records'):
            if row['Dividends'] > 0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.DIVIDEND,
                     'value': row['Dividends']})
            if row['Stock Splits'] > 0:
                actions_to_insert.append(
                    {'security_id': security_id, 'event_date': row['Date'].date(), 'event_type': ActionType.SPLIT,
                     'value': row['Stock Splits']})

        db_manager.bulk_upsert(CorporateAction, actions_to_insert,
                               ['security_id', 'event_date', 'event_type'],
                               constraint='_security_date_type_uc')

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)  # 添加 exc_info=True 以获取更详细的堆栈跟踪
