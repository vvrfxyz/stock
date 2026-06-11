import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from requests.exceptions import RequestException
from loguru import logger
from sqlalchemy import func, or_
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
    is_supported_us_security_type,
)
from utils.script_logging import setup_logging as configure_script_logging

UPDATE_INTERVAL_DAYS = 30
MAX_CONCURRENT_WORKERS = 24


def setup_logging():
    configure_script_logging("update_massive_details")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive API 更新数据库中的美股详情信息。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部活跃保留类型证券。")
    parser.add_argument("--market", type=str, default="US", help="仅处理指定市场，当前仅支持 US。")
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    return parser


def get_massive_reference_fallback_date(security: Security):
    """优先使用价格最新日期，再退化到详情更新时间和上市日期。"""
    if security.price_data_latest_date:
        return security.price_data_latest_date
    if security.info_last_updated_at:
        return security.info_last_updated_at.date()
    return security.list_date


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(func.upper(Security.market) == enforce_us_market(args.market))

        if args.symbols:
            query = query.filter(Security.symbol.in_([item.lower() for item in args.symbols]))
        else:
            query = query.filter(Security.is_active == True, func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))

        if not args.force:
            update_before = datetime.now(timezone.utc) - timedelta(days=UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.info_last_updated_at.is_(None),
                    Security.info_last_updated_at < update_before,
                )
            )

        query = query.order_by(Security.info_last_updated_at.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def ensure_missing_symbols_exist(
    db_manager: DatabaseManager,
    source: MassiveSource,
    symbols: list[str],
) -> int:
    inserted = 0
    if not symbols:
        return inserted

    with db_manager.get_session() as session:
        existing = {
            symbol
            for (symbol,) in session.query(Security.symbol).filter(Security.symbol.in_(symbols)).all()
        }

    missing = [symbol for symbol in symbols if symbol not in existing]
    if not missing:
        return inserted

    new_rows: list[dict] = []
    for symbol in missing:
        payload = source.get_security_info(symbol)
        if not payload:
            logger.warning("[{}] Massive 未返回详情，无法插入新证券。", symbol)
            continue
        if not is_supported_us_security_type(payload.get("type")):
            logger.warning("[{}] Massive type={} 不在保留范围内，跳过。", symbol, payload.get("type"))
            continue
        new_rows.append(payload)

    if new_rows:
        inserted = db_manager.upsert_securities_by_symbol(new_rows, touch_info_timestamp=True)
    return inserted


def process_security(security: Security, source: MassiveSource, db_manager: DatabaseManager) -> tuple[str, str]:
    symbol = security.symbol
    try:
        fallback_date = get_massive_reference_fallback_date(security)
        payload = source.get_security_info(symbol, fallback_date=fallback_date)
        if not payload:
            return symbol, "SKIPPED_NO_DATA"

        payload["id"] = security.id
        db_manager.upsert_security_info(payload)
        return symbol, "SUCCESS"
    except RequestException as e:
        logger.error("[{}] 更新 Massive 详情失败(网络异常): {}", symbol, e)
        return symbol, "ERROR"
    except Exception as e:
        logger.opt(exception=e).error("[{}] 更新 Massive 详情失败: {}", symbol, e)
        return symbol, "ERROR"


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    symbols = [item.lower() for item in args.symbols if item]
    if not any([symbols, args.all, args.market]):
        parser.print_help()
        return 0

    db_manager = None
    source = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        inserted = ensure_missing_symbols_exist(db_manager, source, symbols)
        if inserted:
            logger.info("已补插入 {} 支数据库中缺失的 symbol。", inserted)

        securities = get_securities_to_update(db_manager, args)
        if not securities:
            logger.success("没有需要更新详情的证券。")
            return 0

        results_counter = Counter()
        logger.info("共 {} 支证券需要更新详情，将使用最多 {} 个线程。", len(securities), args.workers)

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, security, source, db_manager): security
                for security in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="更新 Massive 详情"):
                try:
                    _symbol, status = future.result()
                    results_counter[status] += 1
                except Exception as exc:
                    security = future_to_security[future]
                    logger.opt(exception=exc).error("任务 {} 发生未捕获异常: {}", security.symbol, exc)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  跳过(无数据): {}", results_counter["SKIPPED_NO_DATA"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("----------------------")
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_details 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
