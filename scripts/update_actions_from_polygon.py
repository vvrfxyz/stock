# scripts/update_actions_from_polygon.py (新建文件)
import json
import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

from loguru import logger
from sqlalchemy import or_, func
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security
from data_sources.polygon_source import PolygonSource
from utils.key_rate_limiter import KeyRateLimiter

# --- 配置区 ---
# 公司行动数据变化不频繁，更新周期可以长一些
ACTIONS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 15  # 公司行动API调用可能比详情API更重，适当降低并发

# Polygon API 的速率限制配置 (分红和拆股API的限制通常与Ticker Details一样)
POLYGON_RATE_LIMIT = 5
POLYGON_RATE_SECONDS = 60


def setup_logging():
    """配置 Loguru 日志记录器"""
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        "\n<level>{exception}</level>"  # <--- 在新的一行添加这个占位符
    )
    # 将终端日志级别临时设为 DEBUG，以便看到所有信息
    logger.add(sys.stderr, level="DEBUG", format=log_format)
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    # 确保文件日志也使用新的格式
    logger.add(os.path.join(log_dir, f"update_polygon_actions_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG", format=log_format)
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """创建并返回 ArgumentParser 对象。"""
    parser = argparse.ArgumentParser(
        description="使用 Polygon.io API 并发更新数据库中股票的公司行动（分红和拆股）数据。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('symbols', nargs='*', help="要更新的股票代码列表。如果为空，则依赖其他标志。")
    parser.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    parser.add_argument('--market', type=str, help="仅处理指定市场的股票 (例如: US, HK, CNA)。")
    parser.add_argument('--force', action='store_true',
                        help=f"强制更新，忽略 {ACTIONS_UPDATE_INTERVAL_DAYS} 天的时间检查。")
    parser.add_argument('--limit', type=int, default=0, help="限制处理的股票数量，用于测试。0表示不限制。")
    parser.add_argument('--workers', type=int, default=MAX_CONCURRENT_WORKERS,
                        help=f"并发执行的线程数 (默认: {MAX_CONCURRENT_WORKERS})。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    """根据命令行参数，从数据库查询需要更新公司行动数据的证券列表。"""
    with db_manager.get_session() as session:
        query = session.query(Security).filter(Security.is_active == True)

        if args.symbols:
            symbols_lower = [s.lower() for s in args.symbols]
            query = query.filter(Security.symbol.in_(symbols_lower))
        elif args.market:
            query = query.filter(func.upper(Security.market) == args.market.upper())

        if not args.force:
            update_before_date = datetime.now(timezone.utc) - timedelta(days=ACTIONS_UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.actions_last_updated_at.is_(None),
                    Security.actions_last_updated_at < update_before_date
                )
            )

        query = query.order_by(Security.actions_last_updated_at.asc().nulls_first())

        if args.limit > 0:
            query = query.limit(args.limit)

        return query.all()


def process_security(security: Security, polygon_source: PolygonSource, db_manager: DatabaseManager) -> tuple[str, str]:
    """
    处理单个股票的公司行动数据：API获取 -> DB存储 -> 更新时间戳。
    这是每个线程将要执行的工作单元。
    """
    symbol = security.symbol
    try:
        # 1. 从API获取数据
        dividends = polygon_source.get_dividends(symbol)
        splits = polygon_source.get_splits(symbol)

        if dividends and security.currency:  # 仅当有分红数据和股票本身有货币单位时才尝试修复
            repaired_count = 0
            # 从 security 记录中获取标准化的（大写的）货币单位
            standard_currency = security.currency.upper()
            for item in dividends:
                # 如果 currency 是 None 或空字符串
                if not item.get('currency'):
                    item['currency'] = standard_currency  # 使用标准的大写货币单位
                    repaired_count += 1
            if repaired_count > 0:
                logger.info(f"[{symbol}] 自动修复了 {repaired_count} 条分红记录的缺失货币单位为 '{security.currency}'。")

        if dividends:
            logger.debug(f"[{symbol}] 从 Polygon 获取到的分红数据 (即将入库):\n"
                         f"{json.dumps(dividends, indent=2, default=str)}")
        if splits:
            logger.debug(f"[{symbol}] 从 Polygon 获取到的拆股数据 (即将入库):\n"
                         f"{json.dumps(splits, indent=2, default=str)}")

        # 2. 存储数据到数据库
        if dividends:
            db_manager.upsert_dividends(security.id, dividends)
        if splits:
            db_manager.upsert_splits(security.id, splits)

        # 3. 无论是否有新数据，都更新时间戳，表示已检查过
        db_manager.update_security_timestamp(security.id, 'actions_last_updated_at')

        if not dividends and not splits:
            logger.info(f"[{symbol}] 未找到新的公司行动数据。")
            return symbol, "SUCCESS_NO_ACTIONS"

        log_msg = f"[{symbol}] 处理完成。分红: {len(dividends)}条, 拆股: {len(splits)}条。"
        logger.success(log_msg)
        return symbol, "SUCCESS"

    except Exception as e:
        logger.error(f"处理股票 {symbol} 的公司行动时发生严重错误: {e}", exc_info=True)
        return symbol, "ERROR"


def main():
    """脚本主入口"""
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    if not any([args.symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供股票代码，或使用 --all / --market 标志。")
        parser.print_help()
        return

    db_manager = None
    try:
        # --- 初始化共享资源 ---
        api_keys_str = os.getenv("POLYGON_API_KEYS")
        if not api_keys_str:
            raise ValueError("环境变量 POLYGON_API_KEYS 未设置。")
        api_keys = [key.strip() for key in api_keys_str.split(',') if key.strip()]

        rate_limiter = KeyRateLimiter(
            keys=api_keys,
            rate_limit=POLYGON_RATE_LIMIT,
            per_seconds=POLYGON_RATE_SECONDS
        )

        polygon_source = PolygonSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()
        # --- 初始化结束 ---

        securities_to_process = get_securities_to_update(db_manager, args)

        if not securities_to_process:
            logger.success("✅ 根据您的条件，没有找到需要更新公司行动数据的股票。任务完成。")
            return

        total_count = len(securities_to_process)
        logger.info(f"共找到 {total_count} 支股票需要更新公司行动。将使用最多 {args.workers} 个并发线程。")
        logger.info(f"速率限制已启用: 每个Key最多 {POLYGON_RATE_LIMIT} 次 / {POLYGON_RATE_SECONDS} 秒。")

        results_counter = Counter()

        # --- 临时改为单线程循环进行调试 ---
        logger.warning("！！！注意：当前为单线程调试模式！！！")
        for security in tqdm(securities_to_process, desc="更新公司行动 (单线程调试)"):
            try:
                # 直接调用 process_security 函数，而不是通过 executor.submit
                symbol, status = process_security(security, polygon_source, db_manager)
                results_counter[status] += 1
            except Exception as exc:
                # 这里的异常将是原始的、未经包装的异常
                logger.error(f"处理股票 {security.symbol} 时发生致命错误", exc_info=True)
                results_counter["FATAL_ERROR"] += 1
        # --- 单线程调试模式结束 ---

        # with ThreadPoolExecutor(max_workers=args.workers) as executor:
        #     future_to_security = {
        #         executor.submit(process_security, security, polygon_source, db_manager): security
        #         for security in securities_to_process
        #     }
        #
        #     for future in tqdm(as_completed(future_to_security), total=total_count, desc="更新公司行动"):
        #         try:
        #             symbol, status = future.result()
        #             results_counter[status] += 1
        #         except Exception as exc:
        #             security = future_to_security[future]
        #             logger.error(f"任务 {security.symbol} 生成了未捕T获的异常: {exc}", exc_info=True)
        #             results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info(f"  成功 (有数据): {results_counter['SUCCESS']}")
        logger.info(f"  成功 (无数据): {results_counter['SUCCESS_NO_ACTIONS']}")
        logger.info(f"  错误: {results_counter['ERROR'] + results_counter['FATAL_ERROR']}")
        logger.info("----------------------")

    except ValueError as e\
            :
        logger.critical(f"初始化失败: {e}")
    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        end_time = time.monotonic()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=end_time - start_time)}")


if __name__ == "__main__":
    main()
