# scripts/update_daily_bulk.py
import os
import sys
from datetime import date, timedelta
import pandas as pd
from loguru import logger

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from data_sources.polygon_source import PolygonSource
from data_models.models import DailyPrice, MarketType


def setup_logging():
    logger.remove()
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add(sys.stderr, level="INFO", format=log_format)
    os.makedirs(os.path.join(project_root, "logs"), exist_ok=True)
    logger.add(os.path.join(project_root, "logs/update_daily_bulk_{time}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def main():
    setup_logging()
    db_manager = None
    try:
        db_manager = DatabaseManager()
        polygon_source = PolygonSource()

        # 1. 确定要获取数据的日期 (通常是上一个交易日)
        # 假设我们为美股市场更新
        target_market = MarketType.US
        # 获取今天或之前的最后一个交易日
        trade_date_to_fetch = db_manager.get_latest_trading_day(target_market, date.today())

        if not trade_date_to_fetch:
            logger.error(f"无法确定市场 {target_market.name} 的最新交易日，程序退出。")
            return

        logger.info(f"目标更新日期为: {trade_date_to_fetch}")

        # 2. 从 Polygon 获取该日期的所有数据
        daily_data_df = polygon_source.get_all_daily_prices(trade_date_to_fetch.strftime('%Y-%m-%d'))

        if daily_data_df.empty:
            logger.warning("未能从 Polygon 获取任何日线数据，程序结束。")
            return

        # 3. 获取数据库中所有股票的 symbol -> id 映射，以提高效率
        logger.info("正在从数据库加载 symbol 到 id 的映射...")
        with db_manager.get_session() as session:
            securities_map = {s.symbol: (s.id, s.shares_outstanding) for s in session.query(db_manager.Security).all()}
        logger.success(f"加载了 {len(securities_map)} 个股票的映射。")

        # 4. 准备插入数据
        prices_to_insert = []
        for _, row in daily_data_df.iterrows():
            symbol = row['symbol']
            if symbol in securities_map:
                security_id, shares_outstanding = securities_map[symbol]

                prices_to_insert.append({
                    'security_id': security_id,
                    'date': row['date'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume'],
                    'vwap': row['vwap'],
                    'turnover': row['turnover'],
                    'turnover_rate': 0
                })
            else:
                logger.trace(f"跳过未在数据库中找到的 symbol: {symbol}")

        if not prices_to_insert:
            logger.warning("没有可插入数据库的数据。")
            return

        # 5. 批量插入/更新数据
        logger.info(f"准备向数据库批量插入/更新 {len(prices_to_insert)} 条价格数据...")
        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])

        # 6. 更新 security 表中的最新日期 (可选，但推荐)
        # 这是一个更复杂的操作，需要按 security_id 分组更新，为简化暂略
        # 单独运行 main.py 也能达到修正这些日期的目的
        logger.success("批量数据更新完成！")

    except Exception as e:
        logger.critical(f"批量更新过程中发生严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()
