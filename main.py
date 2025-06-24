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
    logger.add(sys.stderr, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")
    
    # 确保 logs 目录存在
    os.makedirs("logs", exist_ok=True)
    logger.add("logs/app_{time}.log", rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")

def main():
    """主执行函数"""
    setup_logging()

    db_manager = None
    try:
        # 1. 初始化数据库管理器
        db_manager = DatabaseManager()

        # (可选) ...
        # db_manager.create_tables()
        # 2. 定义要处理的股票列表
        symbols_to_process = ['AAPL']
        # 3. 循环处理每只股票
        for symbol in symbols_to_process:
            logger.info(f"========== 开始处理 {symbol} ==========")

            update_stock_info(db_manager, symbol)

            update_historical_data(db_manager, symbol)

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