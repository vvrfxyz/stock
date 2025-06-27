# main.py
import sys
import os
import argparse
from datetime import date
from loguru import logger

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.polygon_source import PolygonSource
from data_sources.finnhub_source import FinnhubSource
from db_manager import DatabaseManager
from data_updater import update_stock_info, update_historical_data


def setup_logging():
    """配置 Loguru 日志记录器。"""
    logger.remove()
    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    logger.add(sys.stderr, level="INFO", format=log_format)
    os.makedirs("logs", exist_ok=True)
    logger.add("logs/data_supplement_{time}.log", rotation="10 MB", retention="10 days", level="DEBUG")
    logger.info("日志记录器设置完成。")


def parse_arguments():
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="股票历史数据补充工具。主要用于更新数据落后或有缺失的股票。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('symbols', nargs='*', help="要处理的股票代码列表。如果未提供，则进入自动检测模式。")
    parser.add_argument('--full-refresh', action='store_true', help="对所有目标股票强制执行全量数据更新。")
    parser.add_argument('--market-check', type=str, default='US',
                        help="检查此交易所的开盘状态 (如: US, HK)。开盘则退出。")
    return parser.parse_args()


def main():
    """主执行函数"""
    setup_logging()
    args = parse_arguments()

    is_full_refresh = args.full_refresh
    target_symbols = [symbol.lower() for symbol in args.symbols]

    db_manager = None
    try:
        # 1. 市场状态检查
        finnhub_source = FinnhubSource()
        exchange_to_check = args.market_check.upper()
        logger.info(f"正在检查交易所 '{exchange_to_check}' 的开盘状态...")
        if finnhub_source.is_market_open(exchange_to_check):
            logger.warning(f"交易所 '{exchange_to_check}' 正在开盘，程序终止。")
            return
        logger.success(f"交易所 '{exchange_to_check}' 已收盘，继续执行。")

        # 2. 初始化数据源和数据库管理器
        db_manager = DatabaseManager()
        data_source = PolygonSource()
        logger.info(f"数据源已设置为: {data_source.__class__.__name__}")

        # 3. 确定要处理的股票列表
        if target_symbols:
            symbols_to_process = set(target_symbols)
            logger.info(f"指定模式：将处理 {len(symbols_to_process)} 个股票: {', '.join(target_symbols)}")
        else:
            logger.info("自动模式：检测数据落后的股票...")
            today = date.today()
            # 获取所有活跃但数据日期不是最新的股票
            symbols_to_process = set(db_manager.get_securities_to_update(target_date=today))

        # 如果有强制全量刷新标志，则加入需要自动全量刷新的股票
        if is_full_refresh:
            logger.warning("检测到 --full-refresh 参数，将对所有目标股票执行【全量数据更新】。")
        else:
            auto_full_refresh_symbols = set(db_manager.get_securities_for_auto_full_refresh())
            if auto_full_refresh_symbols:
                logger.info(f"检测到 {len(auto_full_refresh_symbols)} 支股票需要自动全量刷新。")
                symbols_to_process.update(auto_full_refresh_symbols)

        if not symbols_to_process:
            logger.success("所有股票数据均已是最新，无需补充。")
            return

        logger.info(f"共找到 {len(symbols_to_process)} 支股票待处理。")

        # 4. 循环处理
        for symbol in sorted(list(symbols_to_process)):
            logger.info(f"========== 开始处理 {symbol} ==========")

            # 步骤 A: 更新股票基本信息
            update_stock_info(db_manager, symbol, data_source, force_update=is_full_refresh)

            # 步骤 B: 更新历史价格数据
            security = db_manager.get_security_by_symbol(symbol)
            if not security or not security.is_active:
                logger.warning(f"{symbol} 记录无效或不活跃，跳过价格更新。")
                continue

            # 决定是否对当前股票执行全量刷新
            force_this_symbol = is_full_refresh or (symbol in auto_full_refresh_symbols)

            update_historical_data(
                db_manager,
                symbol,
                data_source=data_source,
                full_refresh=force_this_symbol
            )
            logger.info(f"========== 完成处理 {symbol} ==========\n")

    except Exception as e:
        logger.critical(f"程序执行过程中遇到未处理的异常: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("数据补充脚本执行完毕。")


if __name__ == '__main__':
    main()
