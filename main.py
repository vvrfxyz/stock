# main.py
import sys
import os
from datetime import datetime, date, timedelta
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
    target_symbols = [arg.lower() for arg in args if not arg.startswith('--')]

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

        # --- OPTIMIZATION START: 优化逻辑 ---
        # 只有在没有指定目标股票时，才进行自动检测
        if target_symbols:
            symbols_to_process = set(target_symbols)
            # 在指定模式下，我们仍然需要知道哪些股票需要自动全量刷新，以便应用 full_refresh 逻辑
            # 但我们可以只查询我们关心的股票，而不是全部
            auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh()).intersection(
                symbols_to_process)
        else:
            # 自动模式：查询所有需要更新的股票
            logger.info("正在自动检测需要更新的股票...")
            auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh())
            if auto_full_refresh_symbols:
                logger.info(
                    f"检测到 {len(auto_full_refresh_symbols)} 支股票需要自动全量刷新: {', '.join(list(auto_full_refresh_symbols)[:5])}...")

            incremental_update_symbols = set(db_manager.get_securities_to_update(target_date=today))
            symbols_to_process = auto_full_refresh_symbols.union(incremental_update_symbols)
        # --- OPTIMIZATION END ---

        if not symbols_to_process:
            logger.success("所有股票数据均已是最新，或未指定目标，无需更新。程序执行完毕。")
            return

        logger.info(f"共找到 {len(symbols_to_process)} 支股票待处理。")

        for symbol in sorted(list(symbols_to_process)):  # 排序以保证执行顺序一致
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 无论如何都先尝试更新基本信息 (内部有30天冷却)
            update_stock_info(db_manager, symbol)

            # --- 核心逻辑：基于交易日历判断是否需要更新 ---
            security_details = db_manager.get_security_details_for_update(symbol)
            if not security_details or not security_details.is_active:
                logger.warning(f"无法找到 {symbol} 的有效记录或该股票已标记为不活跃，跳过价格更新。")
                continue

            security_id, security_market, _ = security_details

            # 1. 获取该市场的最后一个实际交易日 (这是关键!)
            # 优化：对于美股，可以假设昨天是交易日，以减少日历查询。这里保持原逻辑。
            latest_market_trade_date = db_manager.get_latest_trading_day(security_market, today - timedelta(days=1))
            if not latest_market_trade_date:
                logger.error(f"无法获取 {security_market} 市场的交易日历信息，跳过 {symbol} 的价格更新。")
                continue

            # 2. 获取该股票在数据库中的最新日期
            last_db_date = db_manager.get_last_price_date(security_id)

            # 3. 决策：比较数据库日期和市场最新交易日
            needs_price_update = False
            if last_db_date is None:
                needs_price_update = True
                logger.info(f"[{symbol}] 数据库中无价格数据，需要进行首次获取。")
            elif last_db_date < latest_market_trade_date:
                needs_price_update = True
                logger.info(
                    f"[{symbol}] 数据落后 (数据库最新: {last_db_date}, 市场最新交易日: {latest_market_trade_date})，需要更新。")

            if not needs_price_update and not is_full_refresh and symbol not in auto_full_refresh_symbols:
                logger.success(f"[{symbol}] 价格数据已是最新 (覆盖到 {last_db_date})，无需更新。")
                logger.info(f"========== 完成处理 {symbol} ==========\n")
                continue

            # --- 核心逻辑结束 ---

            # 如果需要更新，则执行更新流程
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
