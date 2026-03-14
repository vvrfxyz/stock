import os
import sys
import time
import argparse
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

from loguru import logger
from sqlalchemy import func
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security
from utils.adj_factor import recalc_adj_factor_for_security


MAX_CONCURRENT_WORKERS = 8
UPSERT_BATCH_SIZE = 1000


def setup_logging():
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(
        os.path.join(log_dir, f"recalc_adj_factor_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="重新计算并回填 daily_prices.adj_factor（前复权 + Total Return：含拆股+现金分红）。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('symbols', nargs='*', help="要处理的股票代码列表 (e.g., 'aapl', 'nvda')。")
    parser.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    parser.add_argument('--market', type=str, help="仅处理指定市场的股票 (例如: US, HK, CNA)。")
    parser.add_argument('--limit', type=int, default=0, help="限制处理的股票数量，用于测试。0 表示不限制。")
    parser.add_argument(
        '--workers',
        type=int,
        default=MAX_CONCURRENT_WORKERS,
        help=f"并发执行的线程数 (默认: {MAX_CONCURRENT_WORKERS})。",
    )
    return parser


def get_securities_to_process(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    symbols = [s.lower() for s in args.symbols if s]
    with db_manager.get_session() as session:
        query = session.query(Security).filter(Security.is_active == True)

        if symbols:
            query = query.filter(Security.symbol.in_(symbols))
        elif args.market:
            query = query.filter(func.upper(Security.market) == args.market.upper())

        if args.limit > 0:
            query = query.limit(args.limit)

        return query.order_by(Security.symbol.asc()).all()

def process_security(security: Security, db_manager: DatabaseManager) -> tuple[str, str, int]:
    symbol = security.symbol

    try:
        rows = recalc_adj_factor_for_security(
            db_manager=db_manager,
            security_id=security.id,
            symbol=symbol,
            batch_size=UPSERT_BATCH_SIZE,
        )
        if rows <= 0:
            logger.warning(f"[{symbol}] 无日线价格数据，跳过。")
            return symbol, "SKIP_NO_PRICES", 0

        logger.success(f"[{symbol}] 回填 adj_factor 完成，共处理 {rows} 行。")
        return symbol, "SUCCESS", rows

    except Exception as e:
        logger.error(f"[{symbol}] 计算/回填 adj_factor 失败: {e}", exc_info=True)
        return symbol, "ERROR", 0


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    symbols = [s.lower() for s in args.symbols if s]
    if not any([symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供 symbols，或使用 --all / --market 标志。")
        parser.print_help()
        return

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities = get_securities_to_process(db_manager, args)
        if not securities:
            logger.success("✅ 根据您的条件，没有找到需要处理的股票。任务完成。")
            return

        total_count = len(securities)
        logger.info(f"共找到 {total_count} 支股票需要计算 adj_factor，将使用最多 {args.workers} 个并发线程。")

        results_counter = Counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, sec, db_manager): sec
                for sec in securities
            }

            for future in tqdm(as_completed(future_to_security), total=total_count, desc="回填 adj_factor"):
                try:
                    _symbol, status, _count = future.result()
                    results_counter[status] += 1
                except Exception as exc:
                    sec = future_to_security[future]
                    logger.error(f"任务 {sec.symbol} 生成了未捕获的异常: {exc}", exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info(f"  成功: {results_counter['SUCCESS']}")
        logger.info(f"  跳过(无价格): {results_counter['SKIP_NO_PRICES']}")
        logger.info(f"  错误: {results_counter['ERROR'] + results_counter['FATAL_ERROR']}")
        logger.info("----------------------")

    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        end_time = time.monotonic()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=end_time - start_time)}")


if __name__ == "__main__":
    main()
