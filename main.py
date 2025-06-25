# main.py
import sys
import os
from datetime import datetime, date
from loguru import logger

from data_models.models import Security
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

    # --- 修改: 解析命令行参数以支持多种模式 ---
    args = sys.argv[1:]
    is_full_refresh = '--full-refresh' in args
    # 筛选出非标志的参数，作为股票代码
    target_symbols = [arg for arg in args if not arg.startswith('--')]

    if is_full_refresh:
        logger.warning("检测到 --full-refresh 参数，将对所有目标股票执行【全量数据更新】。")

    if target_symbols:
        logger.info(f"指定模式：将只处理以下股票: {', '.join(target_symbols)}")
    else:
        logger.info("默认模式：将自动检测并更新所有数据落后的股票。")
    # --- 修改结束 ---

    db_manager = None
    try:
        db_manager = DatabaseManager()

        # (可选) 首次运行时，或修改模型后，运行此命令创建/更新表结构
        # db_manager.create_tables()

        # --- 修改: 根据解析的参数确定要处理的股票列表 ---
        symbols_to_process = []
        if target_symbols:
            # 如果命令行指定了股票代码，则只处理这些股票
            symbols_to_process = target_symbols
        elif is_full_refresh:
            # 如果指定了全量刷新但未指定股票，则处理所有活跃股票
            with db_manager.get_session() as session:
                results = session.query(Security.symbol).filter(Security.is_active == True).all()
                symbols_to_process = [r[0] for r in results]
                logger.info(f"全量刷新模式：找到 {len(symbols_to_process)} 个活跃股票进行处理。")
        else:
            # 默认情况，增量更新所有数据落后的股票
            today = date.today()
            symbols_to_process = db_manager.get_securities_to_update(target_date=today)
        # --- 修改结束 ---

        if not symbols_to_process:
            logger.info("没有需要更新的股票。程序执行完毕。")
            return

        for symbol in symbols_to_process:
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 1. 更新基本信息 (如果股票失效，会自动标记)
            update_stock_info(db_manager, symbol)

            # 2. 更新历史数据
            # 即使只指定了股票代码，is_full_refresh 标志依然有效，用于强制全量更新
            update_historical_data(db_manager, symbol, full_refresh=is_full_refresh)

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
