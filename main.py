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

    args = sys.argv[1:]
    is_full_refresh = '--full-refresh' in args
    target_symbols = [arg for arg in args if not arg.startswith('--')]

    if is_full_refresh:
        logger.warning("检测到 --full-refresh 参数，将对所有目标股票执行【全量数据更新】。")

    if target_symbols:
        logger.info(f"指定模式：将只处理以下股票: {', '.join(target_symbols)}")
    else:
        logger.info("默认模式：将自动检测并更新所有数据落后的股票。")

    db_manager = None
    try:
        db_manager = DatabaseManager()
        today = date.today()

        # 1. & 2. 确定初始待处理列表 (这里的逻辑可以保持不变，作为初步筛选)
        auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh())
        if auto_full_refresh_symbols:
            logger.info(
                f"检测到 {len(auto_full_refresh_symbols)} 支股票需要自动全量刷新: {', '.join(list(auto_full_refresh_symbols)[:5])}...")

        incremental_update_symbols = set(db_manager.get_securities_to_update(target_date=today))

        # 3. 合并并确定最终处理列表
        if target_symbols:
            symbols_to_process = set(target_symbols)
        else:
            symbols_to_process = auto_full_refresh_symbols.union(incremental_update_symbols)

        if not symbols_to_process:
            logger.success("所有股票数据均已是最新，无需更新。程序执行完毕。")
            return

        for symbol in symbols_to_process:
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 无论如何都先尝试更新基本信息 (内部有30天冷却)
            update_stock_info(db_manager, symbol)

            # --- 核心逻辑修改 ---
            security = db_manager.get_security_by_symbol(symbol)
            if not security or not security.is_active:
                logger.warning(f"无法找到 {symbol} 的有效记录或该股票已标记为不活跃，跳过价格更新。")
                continue

            # 获取该市场的最后一个实际交易日
            latest_market_trade_date = db_manager.get_latest_trading_day(security.market, today)

            if not latest_market_trade_date:
                logger.error(f"无法获取 {security.market} 市场的交易日历信息，跳过 {symbol} 的价格更新。")
                continue

            # 获取该股票在数据库中的最新日期
            last_db_date = db_manager.get_last_price_date(security.id)

            # 决定是否需要更新价格数据
            needs_price_update = False
            if last_db_date is None:
                needs_price_update = True
                logger.info(f"[{symbol}] 数据库中无价格数据，需要进行首次获取。")
            elif last_db_date < latest_market_trade_date:
                needs_price_update = True
                logger.info(
                    f"[{symbol}] 数据落后 (数据库最新: {last_db_date}, 市场最新: {latest_market_trade_date})，需要更新。")

            if not needs_price_update:
                logger.info(f"[{symbol}] 价格数据已是最新 ({last_db_date})，无需更新。")
                logger.info(f"========== 完成处理 {symbol} ==========\n")
                continue
            # --- 核心逻辑修改结束 ---

            # 确定是否需要全量刷新 (强制命令行 > 自动周期)
            is_full_refresh_final = is_full_refresh or (symbol in auto_full_refresh_symbols)

            if is_full_refresh_final and not is_full_refresh:
                logger.info(f"[{symbol}] 触发自动全量更新周期。")

            # 执行历史数据更新
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

