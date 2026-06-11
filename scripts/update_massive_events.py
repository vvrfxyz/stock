import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

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
)
from utils.script_logging import setup_logging as configure_script_logging

EVENTS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 8


def setup_logging():
    configure_script_logging("update_massive_events")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Ticker Events 更新 symbol history。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部活跃 CS/ETF。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([symbol.lower() for symbol in args.symbols]))
        else:
            query = query.filter(Security.is_active == True)
        if not args.force:
            update_before = datetime.now(timezone.utc) - timedelta(days=EVENTS_UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.events_last_updated_at.is_(None),
                    Security.events_last_updated_at < update_before,
                )
            )
        query = query.order_by(Security.events_last_updated_at.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def process_security(security: Security, source: MassiveSource, db_manager: DatabaseManager) -> tuple[str, str, int]:
    try:
        payload = source.get_ticker_events(security.symbol)
        events = (payload or {}).get("events") or []
        rows_by_key = {}
        for event in events:
            if event.get("type") != "ticker_change":
                continue
            ticker = ((event.get("ticker_change") or {}).get("ticker") or "").lower()
            event_date = event.get("date")
            if not ticker or not event_date:
                continue
            rows_by_key[(security.id, ticker, "MASSIVE", event_date)] = {
                "security_id": security.id,
                "symbol": ticker,
                "exchange": security.exchange,
                "source": "MASSIVE",
                "source_event_id": f"{security.id}:{ticker}:{event_date}",
                "event_type": event.get("type"),
                "start_date": event_date,
            }
        rows = list(rows_by_key.values())
        inserted = db_manager.upsert_symbol_history(rows) if rows else 0
        db_manager.update_security_timestamp(security.id, "events_last_updated_at")
        return security.symbol, "SUCCESS", inserted
    except Exception as exc:
        logger.opt(exception=exc).error("[{}] ticker events 更新失败: {}", security.symbol, exc)
        return security.symbol, "ERROR", 0


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    source = None
    try:
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        securities = get_securities_to_update(db_manager, args)
        if not securities:
            logger.success("没有需要更新 ticker events 的证券。")
            return 0

        results_counter = Counter()
        total_rows = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, security, source, db_manager): security
                for security in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="更新 Massive events"):
                try:
                    _symbol, status, count = future.result()
                except Exception as exc:
                    security = future_to_security[future]
                    logger.opt(exception=exc).error("任务 {} 发生未捕获异常: {}", security.symbol, exc)
                    results_counter["FATAL_ERROR"] += 1
                    continue
                results_counter[status] += 1
                total_rows += count

        logger.info("--- ticker events 更新统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("  写入 symbol history 行数: {}", total_rows)
        logger.info("------------------------------")
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_events 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
