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
        today = date.today()

        # 1. 确定需要自动全量刷新的股票
        auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh())
        if auto_full_refresh_symbols:
            logger.info(
                f"检测到 {len(auto_full_refresh_symbols)} 支股票需要自动全量刷新: {', '.join(list(auto_full_refresh_symbols)[:5])}...")

        # 2. 确定需要增量更新的股票
        # (可以复用你已有的 get_securities_to_update 逻辑)
        incremental_update_symbols = set(db_manager.get_securities_to_update(target_date=today))

        # 3. 合并并确定最终处理列表
        symbols_to_process = set()
        if target_symbols:  # 如果命令行指定了股票
            symbols_to_process = set(target_symbols)
        else:
            symbols_to_process = auto_full_refresh_symbols.union(incremental_update_symbols)
        if not symbols_to_process:
            logger.info("没有需要更新的股票。程序执行完毕。")
            return
        for symbol in symbols_to_process:
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 获取股票基本信息，特别是 market
            security = db_manager.get_security_by_symbol(symbol)  # 假设有这个helper
            if not security:
                logger.warning(f"无法在数据库中找到 {symbol} 的记录，跳过。")
                continue
            # 需求 1: 检查是否为交易日
            is_trading_day = db_manager.is_trading_day(security.market, today)

            # 无论是否交易日，都可尝试更新 info (内部有30天冷却)
            # 这满足了“非交易日只触发info检测”的需求
            update_stock_info(db_manager, symbol)
            if not is_trading_day:
                logger.info(f"今天是 {security.market} 市场的非交易日，跳过 {symbol} 的价格数据更新。")
                continue
            # 确定是否需要全量刷新
            # 强制全量刷新 > 自动全量刷新
            is_full_refresh_final = is_full_refresh or (symbol in auto_full_refresh_symbols)

            if is_full_refresh_final and not is_full_refresh:
                logger.info(f"[{symbol}] 触发自动全量更新。")
            # 更新历史数据
            update_historical_data(db_manager, symbol, full_refresh=is_full_refresh_final)
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
