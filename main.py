# main.py
import sys
import os
import argparse
from datetime import datetime, date, timedelta
from loguru import logger

from db_manager import DatabaseManager
from data_updater import update_stock_info, update_historical_data
# 导入所有具体的数据源实现
from data_sources.yfinance_source import YFinanceSource
from data_sources.finnhub_source import FinnhubSource


def setup_logging():
    # ... 函数保持不变 ...
    """配置 Loguru 日志记录器。"""
    logger.remove()
    logger.add(sys.stderr, level="INFO",
               format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>")

    os.makedirs("logs", exist_ok=True)
    logger.add("logs/app_{time}.log", rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")


def parse_arguments():
    """使用 argparse 解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="股票历史数据更新工具。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'symbols',
        nargs='*',  # 0个或多个
        help="要处理的股票代码列表。如果未提供，则进入自动检测模式。"
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',  # 存储为 True/False
        help="对所有目标股票强制执行全量数据更新，忽略增量更新逻辑。"
    )
    # --- 新增参数 ---
    parser.add_argument(
        '--source',
        type=str,
        choices=['yfinance', 'finnhub'],
        default='yfinance',
        help="选择主数据源 (默认为 yfinance)。将使用另一数据源进行对比。"
    )
    return parser.parse_args()


def main():
    """主执行函数"""
    setup_logging()
    args = parse_arguments()

    is_full_refresh = args.full_refresh
    target_symbols = [symbol.lower() for symbol in args.symbols]
    primary_source_name = args.source

    if is_full_refresh:
        logger.warning("检测到 --full-refresh 参数，将对所有目标股票执行【全量数据更新】。")

    if target_symbols:
        logger.info(f"指定模式：将只处理以下股票: {', '.join(target_symbols)}")
    else:
        logger.info("默认模式：将自动检测并更新所有数据落后的股票。")

    db_manager = None
    try:
        db_manager = DatabaseManager()

        # --- 修改：实例化所有数据源 ---
        logger.info("正在初始化所有数据源...")
        all_sources = {
            'yfinance': YFinanceSource(),
            'finnhub': FinnhubSource()
        }
        primary_data_source = all_sources[primary_source_name]
        secondary_data_source = all_sources['finnhub' if primary_source_name == 'yfinance' else 'yfinance']
        logger.success(f"主数据源: {primary_data_source.__class__.__name__}, "
                       f"对比数据源: {secondary_data_source.__class__.__name__}")

        today = date.today()

        # ... 后续的 symbols_to_process 获取逻辑保持不变 ...
        if target_symbols:
            symbols_to_process = set(target_symbols)
            auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh()).intersection(
                symbols_to_process)
        else:
            logger.info("正在自动检测需要更新的股票...")
            auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh())
            if auto_full_refresh_symbols:
                logger.info(
                    f"检测到 {len(auto_full_refresh_symbols)} 支股票需要自动全量刷新: {', '.join(list(auto_full_refresh_symbols)[:5])}...")

            incremental_update_symbols = set(db_manager.get_securities_to_update(target_date=today))
            symbols_to_process = auto_full_refresh_symbols.union(incremental_update_symbols)

        if not symbols_to_process:
            logger.success("所有股票数据均已是最新，或未指定目标，无需更新。程序执行完毕。")
            return

        logger.info(f"共找到 {len(symbols_to_process)} 支股票待处理。")


        for symbol in sorted(list(symbols_to_process)):
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 使用主数据源更新基本信息
            update_stock_info(db_manager, symbol, primary_data_source)

            security_details = db_manager.get_security_details_for_update(symbol)
            if not security_details or not security_details.is_active:
                logger.warning(f"无法找到 {symbol} 的有效记录或该股票已标记为不活跃，跳过价格更新。")
                continue

            security_id, security_market, _ = security_details

            # ... 后续的更新决策逻辑保持不变 ...
            latest_market_trade_date = db_manager.get_latest_trading_day(security_market, today - timedelta(days=1))
            if not latest_market_trade_date:
                logger.error(f"无法获取 {security_market} 市场的交易日历信息，跳过 {symbol} 的价格更新。")
                continue

            last_db_date = db_manager.get_last_price_date(security_id)

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

            is_full_refresh_final = is_full_refresh or (symbol in auto_full_refresh_symbols)
            if is_full_refresh_final and not is_full_refresh:
                logger.info(f"[{symbol}] 触发自动全量更新周期。")

            # --- 修改：调用更新函数时传入两个数据源 ---
            update_historical_data(
                db_manager,
                symbol,
                data_source=primary_data_source,
                full_refresh=is_full_refresh_final,
                secondary_source_for_comparison=secondary_data_source
            )
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
