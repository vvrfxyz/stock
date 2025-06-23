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

    try:
        # 1. 初始化数据库管理器
        db_manager = DatabaseManager()
        
        # (可选) 如果是第一次运行，创建数据库表。
        # 在生产环境中，更推荐使用 alembic migrations 来管理表结构。
        # db_manager.create_tables()

    except Exception as e:
        logger.critical(f"程序启动失败，无法连接到数据库: {e}")
        return

    # 2. 定义要处理的股票列表
    symbols_to_process = ['AAPL']

    # 3. 循环处理每只股票
    for symbol in symbols_to_process:
        logger.info(f"========== 开始处理 {symbol} ==========")
        
        # 首先，更新或创建股票的基本信息
        update_stock_info(db_manager, symbol)
        
        # 然后，更新其全部历史价格和公司行动
        # 【关键修改】调用新的函数，不再需要日期参数
        update_historical_data(db_manager, symbol)
        
        logger.info(f"========== 完成处理 {symbol} ==========\n")

if __name__ == '__main__':
    main()
