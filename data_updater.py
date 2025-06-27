# data_updater.py
import pandas as pd
from loguru import logger
from datetime import datetime, timedelta, timezone

from db_manager import DatabaseManager
from data_models.models import DailyPrice, Security
from data_sources.base import DataSourceInterface


def update_stock_info(db_manager: DatabaseManager, symbol: str, data_source: DataSourceInterface,
                      force_update: bool = False):
    """使用传入的 data_source 更新股票基本信息"""
    symbol = symbol.lower()

    # 检查是否需要更新（例如，30天内已更新过则跳过）
    if not force_update:
        with db_manager.get_session() as session:
            security = session.query(Security.info_last_updated_at).filter(Security.symbol == symbol).first()
            if security and security.info_last_updated_at and security.info_last_updated_at > (
                    datetime.now(timezone.utc) - timedelta(days=30)):
                logger.trace(f"[{symbol}] 的基本信息在30天内已更新，跳过。")
                return

    logger.info(f"开始为 {symbol} 从 {data_source.__class__.__name__} 获取基本信息...")
    try:
        info = data_source.get_security_info(symbol)
        if not info:
            logger.warning(f"无法获取 {symbol} 的有效信息，可能已退市或代码无效。")
            with db_manager.get_session() as session:
                session.query(Security).filter(Security.symbol == symbol).update({'is_active': False},
                                                                                 synchronize_session=False)
                session.commit()
            return

        db_manager.upsert_security_info(info)

    except Exception as e:
        logger.error(f"为 {symbol} 更新基本信息时出错: {e}", exc_info=True)


def update_historical_data(db_manager: DatabaseManager, symbol: str, data_source: DataSourceInterface,
                           full_refresh: bool = False):
    """使用传入的数据源为单个股票更新历史价格数据（补充模式）"""
    try:
        # 获取 security_id，如果不存在则会创建
        security = db_manager.get_security_by_symbol(symbol)
        if not security:
            logger.error(f"无法为 {symbol} 获取或创建 Security 记录，跳过历史数据更新。")
            return
        security_id = security.id

        start_date = None
        if not full_refresh:
            last_db_date = db_manager.get_last_price_date(security_id)
            if last_db_date:
                logger.info(f"为 {symbol} (ID: {security_id}) 执行【增量更新】，从 {last_db_date} 之后开始。")
                start_date = (last_db_date + timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                logger.info(f"数据库中无 {symbol} (ID: {security_id}) 的价格数据，自动执行【首次全量获取】。")
                full_refresh = True

        if full_refresh:
            logger.info(f"开始为 {symbol} (ID: {security_id}) 执行【全量刷新】...")
            # 对于全量刷新，不指定开始日期，让数据源获取所有可用数据
            start_date = security.list_date.strftime('%Y-%m-%d') if security.list_date else None

        df = data_source.get_historical_data(symbol, start=start_date)

        if df.empty:
            logger.warning(f"[{symbol}] 在指定时间段内没有从数据源获取到新数据。")
            # 即使没有新数据，也更新一下时间戳，表示我们检查过了
            last_db_date = db_manager.get_last_price_date(security_id)
            if last_db_date:
                db_manager.update_security_latest_price_date(security_id, last_db_date)
            return

        # 准备插入数据
        df.reset_index(inplace=True)
        prices_to_insert = [
            {
                'security_id': security_id,
                'date': row['Date'],
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume'],
                'vwap': row.get('vwap'),
                'turnover': row.get('turnover'),
                # turnover_rate 需要总股本，这里暂时不计算
                'turnover_rate': None,
            } for _, row in df.iterrows()
        ]

        if not prices_to_insert:
            logger.warning(f"为 {symbol} 处理后没有价格数据可插入。")
            return

        # 批量插入/更新
        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # 更新 security 表中的最新价格日期
        latest_date_in_batch = max(p['date'] for p in prices_to_insert)
        db_manager.update_security_latest_price_date(security_id, latest_date_in_batch)

        if full_refresh:
            db_manager.update_security_full_refresh_timestamp(security_id)

    except Exception as e:
        logger.error(f"为 {symbol} 更新历史数据时出错: {e}", exc_info=True)

