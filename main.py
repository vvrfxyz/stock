# main.py
import sys
import os
from datetime import datetime
from loguru import logger
from db_manager import DatabaseManager
from data_updater import update_stock_info, update_historical_data


def setup_logging():
    """配置 Loguru 日志记录器。"""
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    os.makedirs("logs", exist_ok=True)
    logger.add("logs/app_{time}.log", rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")


def main():
    """主执行函数"""
    setup_logging()

    # --- OPTIMIZATION START: 解析命令行参数 ---
    # 检查是否存在 '--full-refresh' 参数
    is_full_refresh = '--full-refresh' in sys.argv
    if is_full_refresh:
        logger.warning("检测到 --full-refresh 参数，将对所有股票执行全量数据更新。")
    else:
        logger.info("将执行默认的增量更新模式。使用 --full-refresh 参数可强制全量更新。")
    # --- OPTIMIZATION END ---

    db_manager = None
    try:
        db_manager = DatabaseManager()

        # (可选) 首次运行时，取消注释以创建表结构
        # db_manager.create_tables()

        # 定义要处理的股票列表
        symbols_to_process = ['AAPL', '0700.HK', '600519.SS']

        for symbol in symbols_to_process:
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 更新基本信息 (逻辑不变)
            update_stock_info(db_manager, symbol)

            # --- OPTIMIZATION START: 传递 full_refresh 参数 ---
            # 更新历史数据，并传入刷新模式标志
            update_historical_data(db_manager, symbol, full_refresh=is_full_refresh)
            # --- OPTIMIZATION END ---

            logger.info(f"========== 完成处理 {symbol} ==========\n")
    except Exception as e:
        if db_manager is None:
            logger.critical(f"程序启动失败，无法初始化数据库管理器: {e}", exc_info=True)
        else:
            logger.critical(f"程序在执行过程中遇到未处理的异常: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("程序执行完毕。")


if __name__ == '__main__':
    main()

