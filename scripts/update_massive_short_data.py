import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from loguru import logger
from sqlalchemy import func
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
    get_massive_history_floor,
    iter_chunks,
)
from utils.trading_calendar import get_last_completed_trading_date
from utils.script_logging import setup_logging as configure_script_logging

MAX_CONCURRENT_WORKERS = 2
API_BATCH_SIZE = 100


def setup_logging():
    configure_script_logging("update_massive_short_data")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive 更新 short interest / short volume。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部活跃 CS/ETF。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--force", action="store_true", help="强制刷新 Massive 可覆盖窗口。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="批次并发数。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace, end_date: date) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([symbol.lower() for symbol in args.symbols]))
        else:
            query = query.filter(Security.is_active == True)
        query = query.order_by(Security.short_data_last_updated_at.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        securities = query.all()

    if args.force:
        return securities

    max_dates = db_manager.get_security_short_max_dates([security.id for security in securities])

    def _needs_update(security: Security) -> bool:
        dates = max_dates.get(security.id, {})
        interest_date = dates.get("interest")
        volume_date = dates.get("volume")
        return (
            interest_date is None
            or volume_date is None
            or volume_date < end_date
        )

    def _sort_key(security: Security) -> tuple[date, str]:
        dates = max_dates.get(security.id, {})
        interest_date = dates.get("interest")
        volume_date = dates.get("volume")
        known_dates = [d for d in (interest_date, volume_date) if d is not None]
        latest_known = min(known_dates) if known_dates else date.min
        return latest_known, security.symbol

    return sorted((security for security in securities if _needs_update(security)), key=_sort_key)


def _group_by_ticker(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        ticker = row.get("ticker")
        if ticker:
            grouped.setdefault(ticker, []).append(row)
    return grouped


def process_batch(
    securities: list[Security],
    source: MassiveSource,
    db_manager: DatabaseManager,
    history_floor: date,
    force: bool,
) -> tuple[Counter, int, int]:
    symbols = [security.symbol for security in securities]
    symbol_to_security = {security.symbol: security for security in securities}
    security_id_to_symbol = {security.id: security.symbol for security in securities}
    counter = Counter()

    max_dates = db_manager.get_security_short_max_dates([security.id for security in securities])
    interest_start_by_symbol: dict[str, date] = {}
    volume_start_by_symbol: dict[str, date] = {}
    for security_id, dates in max_dates.items():
        symbol = security_id_to_symbol[security_id]
        interest_latest = dates.get("interest")
        volume_latest = dates.get("volume")
        interest_start_by_symbol[symbol] = (
            history_floor if force or interest_latest is None else max(interest_latest + timedelta(days=1), history_floor)
        )
        volume_start_by_symbol[symbol] = (
            history_floor if force or volume_latest is None else max(volume_latest + timedelta(days=1), history_floor)
        )

    interest_batch_start = min(interest_start_by_symbol.values())
    volume_batch_start = min(volume_start_by_symbol.values())
    interests = source.get_short_interest_batch(symbols, start_date=interest_batch_start.isoformat(), chunk_size=API_BATCH_SIZE)
    volumes = source.get_short_volume_batch(symbols, start_date=volume_batch_start.isoformat(), chunk_size=API_BATCH_SIZE)

    interest_rows = []
    for row in interests:
        security = symbol_to_security.get(row.get("ticker"))
        if not security:
            continue
        if row["settlement_date"] < interest_start_by_symbol[security.symbol]:
            continue
        row = dict(row)
        row["security_id"] = security.id
        row["source"] = "MASSIVE"
        interest_rows.append(row)

    volume_rows = []
    for row in volumes:
        security = symbol_to_security.get(row.get("ticker"))
        if not security:
            continue
        if row["date"] < volume_start_by_symbol[security.symbol]:
            continue
        row = dict(row)
        row["security_id"] = security.id
        row["source"] = "MASSIVE"
        volume_rows.append(row)

    interest_count = db_manager.upsert_short_interests(interest_rows) if interest_rows else 0
    volume_count = db_manager.upsert_short_volumes(volume_rows) if volume_rows else 0

    interests_by_symbol = _group_by_ticker(interest_rows)
    volumes_by_symbol = _group_by_ticker(volume_rows)
    db_manager.update_security_timestamps([security.id for security in securities], "short_data_last_updated_at")
    for security in securities:
        if interests_by_symbol.get(security.symbol) or volumes_by_symbol.get(security.symbol):
            counter["SUCCESS"] += 1
        else:
            counter["SUCCESS_NO_DATA"] += 1
    return counter, interest_count, volume_count


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    source = None
    try:
        end_date = get_last_completed_trading_date(args.market)
        history_floor = get_massive_history_floor(end_date)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        securities = get_securities_to_update(db_manager, args, end_date)
        if not securities:
            logger.success("没有需要更新 short data 的证券。")
            return 0

        batches = iter_chunks(securities, API_BATCH_SIZE)
        results_counter = Counter()
        total_interests = 0
        total_volumes = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, batch, source, db_manager, history_floor, args.force): batch
                for batch in batches
            }
            for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc="更新 Massive short data"):
                try:
                    batch_counter, interest_count, volume_count = future.result()
                except Exception as exc:
                    batch = future_to_batch[future]
                    logger.opt(exception=exc).error(
                        "批次 {}-{} 发生未捕获异常: {}", batch[0].symbol, batch[-1].symbol, exc
                    )
                    results_counter["FATAL_ERROR"] += len(batch)
                    continue
                results_counter.update(batch_counter)
                total_interests += interest_count
                total_volumes += volume_count

        logger.info("--- short data 更新统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无数据: {}", results_counter["SUCCESS_NO_DATA"])
        logger.info("  批次失败证券数: {}", results_counter["FATAL_ERROR"])
        logger.info("  short_interest 写入行数: {}", total_interests)
        logger.info("  short_volume 写入行数: {}", total_volumes)
        logger.info("---------------------------")
        return 1 if results_counter["FATAL_ERROR"] else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_short_data 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
