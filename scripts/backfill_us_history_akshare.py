# scripts/backfill_us_history_akshare.py
import os
import sys
import time
from datetime import date, timedelta
import pandas as pd
import akshare as ak
from loguru import logger

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from data_models.models import DailyPrice, Security, MarketType


def setup_logging():
    logger.remove()
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add(sys.stderr, level="INFO", format=log_format)
    os.makedirs(os.path.join(project_root, "logs"), exist_ok=True)
    logger.add(os.path.join(project_root, "logs/backfill_akshare_{time}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def backfill_stock(db_manager: DatabaseManager, security: Security, end_date: str):
    """为单个股票从 akshare 获取历史数据并填充"""
    if not security.em_code:
        logger.warning(f"股票 {security.symbol} (ID: {security.id}) 缺少 em_code，无法从 akshare 回填。")
        return

    logger.info(f"--- 开始为 {security.symbol} (em_code: {security.em_code}) 从 akshare 回填数据，截止到 {end_date} ---")

    try:
        # akshare 使用 em_code 查询，例如 "105.NVDA"
        hist_df = ak.stock_us_hist(symbol=security.em_code, period="daily", start_date="19700101",
                                   end_date=end_date.replace("-", ""), adjust="")

        if hist_df.empty:
            logger.warning(f"akshare 未返回 {security.em_code} 在 {end_date} 之前的任何历史数据。")
            return

        # 数据清洗和转换
        hist_df.rename(columns={
            '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
            '成交量': 'volume', '成交额': 'turnover', '换手率': 'turnover_rate'
        }, inplace=True)

        hist_df['date'] = pd.to_datetime(hist_df['date']).dt.date
        # 计算 vwap
        hist_df['vwap'] = hist_df['turnover'] / hist_df['volume']
        # 将volume从“股”转为“手”（如果akshare返回的是手，则不需要除以100）
        # 假设akshare返回的是“股”
        hist_df['volume'] = hist_df['volume']

        prices_to_insert = [
            {
                'security_id': security.id,
                'date': row.date,
                'open': row.open,
                'high': row.high,
                'low': row.low,
                'close': row.close,
                'volume': row.volume,
                'turnover': row.turnover,
                'vwap': row.vwap,
                'turnover_rate': row.turnover_rate,
            }
            for row in hist_df.itertuples()
        ]

        if not prices_to_insert:
            logger.info("没有需要插入的历史数据。")
            return

        logger.info(f"找到 {len(prices_to_insert)} 条历史数据，准备写入数据库...")
        db_manager.bulk_upsert(DailyPrice, prices_to_insert, ['security_id', 'date'])
        logger.success(f"成功为 {security.symbol} 回填了历史数据。")

    except Exception as e:
        logger.error(f"为 {security.symbol} 从 akshare 回填数据时出错: {e}", exc_info=True)


def main():
    setup_logging()
    db_manager = None
    try:
        db_manager = DatabaseManager()

        # 1. 定义回填的时间范围
        # 我们只回填早于2年前的数据，因为近2年的数据由Polygon提供
        backfill_cutoff_date = date.today() - timedelta(days=365 * 2)
        logger.info(f"将使用 akshare 回填早于 {backfill_cutoff_date} 的美股历史数据。")

        # 2. 从数据库中找到需要回填的股票
        # 条件：美股、活跃、有em_code、上市日期早于我们的回填截止日期
        with db_manager.get_session() as session:
            securities_to_backfill = session.query(Security).filter(
                Security.market == MarketType.US,
                Security.is_active == True,
                Security.em_code.isnot(None),
                Security.list_date < backfill_cutoff_date
            ).all()

        if not securities_to_backfill:
            logger.success("没有找到需要回填历史数据的股票。")
            return

        logger.info(f"找到 {len(securities_to_backfill)} 支可能需要回填的股票。")

        # 3. 循环处理每支股票
        total = len(securities_to_backfill)
        for i, security in enumerate(securities_to_backfill):
            logger.info(f"进度: {i + 1}/{total}")

            # 检查该股票是否已经有足够早的数据，避免重复工作
            first_price_date = db_manager.get_first_price_date(security.id)  # 你需要在db_manager中实现这个方法
            if first_price_date and first_price_date <= (security.list_date + timedelta(days=5)):
                logger.info(f"股票 {security.symbol} 的数据已足够完整（从 {first_price_date} 开始），跳过。")
                continue

            # 定义此股票的回填结束日期（即我们数据库中已有数据的最早日期，或2年前）
            end_date_str = (first_price_date or backfill_cutoff_date).strftime('%Y-%m-%d')

            backfill_stock(db_manager, security, end_date_str)

            # akshare 可能会有限速，增加延迟
            time.sleep(1)

    except Exception as e:
        logger.critical(f"回填脚本执行过程中发生严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()

