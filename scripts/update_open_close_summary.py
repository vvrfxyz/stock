import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta

from loguru import logger
from sqlalchemy import func
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import DailyPrice, Security
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

MAX_CONCURRENT_WORKERS = 12
UPSERT_BATCH_SIZE = 1000


def setup_logging():
    configure_script_logging("update_open_close_summary")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Daily Ticker Summary 回填盘前/盘后价格。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理所有保留类型证券。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--start-date", type=str, required=True, help="开始日期 YYYY-MM-DD。")
    parser.add_argument("--end-date", type=str, required=True, help="结束日期 YYYY-MM-DD。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已有 pre_market/after_hours。")
    return parser


def _iter_batches(rows: list[dict], batch_size: int):
    for index in range(0, len(rows), batch_size):
        yield rows[index:index + batch_size]


def _get_dates(start_date: str, end_date: str) -> list[date]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        return []
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def get_security_scope(db_manager: DatabaseManager, args: argparse.Namespace) -> dict[int, str]:
    symbols = [item.lower() for item in args.symbols if item]
    with db_manager.get_session() as session:
        query = (
            session.query(Security.id, Security.symbol)
            .filter(func.upper(Security.market) == enforce_us_market(args.market))
            .filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
        )
        if symbols:
            query = query.filter(Security.symbol.in_(symbols))
        elif not args.all:
            query = query.filter(Security.is_active == True)
        query = query.order_by(Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        rows = query.all()
    return {security_id: symbol for security_id, symbol in rows}


def get_candidates_for_date(
    db_manager: DatabaseManager,
    target_date: date,
    security_scope: dict[int, str],
    overwrite: bool,
) -> list[tuple[int, str]]:
    with db_manager.get_session() as session:
        query = session.query(DailyPrice.security_id).filter(DailyPrice.date == target_date)
        if not overwrite:
            query = query.filter(DailyPrice.pre_market.is_(None), DailyPrice.after_hours.is_(None))
        rows = query.all()

    candidates: list[tuple[int, str]] = []
    for (security_id,) in rows:
        symbol = security_scope.get(security_id)
        if symbol:
            candidates.append((security_id, symbol))
    return candidates


def process_security_date(
    security_id: int,
    symbol: str,
    target_date: date,
    source: MassiveSource,
) -> tuple[str, str, dict | None]:
    try:
        payload = source.get_open_close_data(symbol, target_date.isoformat(), adjusted=False)
        if not payload:
            return symbol, "SKIP_NO_DATA", None

        pre_market = payload.get("preMarket")
        after_hours = payload.get("afterHours")
        if pre_market is None and after_hours is None:
            return symbol, "SKIP_NO_SESSION_DATA", None

        return (
            symbol,
            "SUCCESS",
            {
                "security_id": security_id,
                "date": target_date,
                "otc": payload.get("otc"),
                "pre_market": pre_market,
                "after_hours": after_hours,
            },
        )
    except Exception as exc:
        logger.opt(exception=exc).error("[{} {}] 更新盘前盘后失败: {}", symbol, target_date, exc)
        return symbol, "ERROR", None


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args(argv)

    db_manager = None
    source = None
    try:
        dates = _get_dates(args.start_date, args.end_date)
        if not dates:
            raise ValueError("日期范围为空。")

        security_scope = {}
        db_manager = DatabaseManager()
        security_scope = get_security_scope(db_manager, args)
        if not security_scope:
            logger.success("没有需要处理的证券。")
            return 0

        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)

        results_counter = Counter()
        total_rows = 0
        for target_date in dates:
            candidates = get_candidates_for_date(db_manager, target_date, security_scope, args.overwrite)
            if not candidates:
                logger.info("[{}] 没有需要回填盘前盘后价格的记录。", target_date)
                continue

            updates: list[dict] = []
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                future_to_item = {
                    executor.submit(process_security_date, security_id, symbol, target_date, source): (security_id, symbol)
                    for security_id, symbol in candidates
                }
                for future in tqdm(
                    as_completed(future_to_item),
                    total=len(candidates),
                    desc=f"盘前盘后 {target_date.isoformat()}",
                ):
                    _symbol, status, row = future.result()
                    results_counter[status] += 1
                    if row:
                        updates.append(row)

            for batch in _iter_batches(updates, UPSERT_BATCH_SIZE):
                total_rows += db_manager.upsert_daily_prices(batch)

        logger.info("--- 盘前盘后统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无摘要: {}", results_counter["SKIP_NO_DATA"])
        logger.info("  无盘前盘后值: {}", results_counter["SKIP_NO_SESSION_DATA"])
        logger.info("  错误: {}", results_counter["ERROR"])
        logger.info("  写入行数: {}", total_rows)
        logger.info("--------------------")
        return 1 if results_counter["ERROR"] else 0
    except Exception as exc:
        logger.opt(exception=exc).critical("update_open_close_summary 执行失败: {}", exc)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
