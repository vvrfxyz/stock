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

from data_models.models import HistoricalShare, Security
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
    get_quarter_snapshot_dates,
)
from utils.trading_calendar import get_last_completed_trading_date

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
        os.path.join(log_dir, f"update_massive_shares_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Ticker Overview / Float 更新 historical_shares。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部保留类型证券。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--full-refresh", action="store_true", help="回填 Massive 可覆盖的最近 2 年季度快照。")
    parser.add_argument("--start-date", type=str, help="起始日期 YYYY-MM-DD。仅在 full-refresh 时生效。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    return parser


def _quarter_start(target_date: date) -> date:
    month = ((target_date.month - 1) // 3) * 3 + 1
    return date(target_date.year, month, 1)


def _extract_total_shares(overview: dict | None) -> int | None:
    if not overview:
        return None
    for key in ("weighted_shares_outstanding", "share_class_shares_outstanding"):
        value = overview.get(key)
        if value in (None, ""):
            continue
        try:
            shares = int(value)
        except Exception:
            continue
        if shares > 0:
            return shares
    return None


def get_securities_to_process(
    db_manager: DatabaseManager,
    args: argparse.Namespace,
    end_date: date,
) -> list[Security]:
    current_quarter_start = _quarter_start(end_date)
    with db_manager.get_session() as session:
        rows = (
            session.query(Security, func.max(HistoricalShare.change_date))
            .outerjoin(HistoricalShare, HistoricalShare.security_id == Security.id)
            .filter(func.upper(Security.market) == enforce_us_market(args.market))
            .filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
            .group_by(Security.id)
            .order_by(Security.symbol.asc())
            .all()
        )

    symbols = {symbol.lower() for symbol in args.symbols if symbol}
    selected: list[Security] = []
    for security, latest_change_date in rows:
        if symbols and security.symbol not in symbols:
            continue
        if not args.full_refresh and not symbols and security.is_active is not True:
            continue
        if not args.full_refresh and not symbols and latest_change_date and latest_change_date >= current_quarter_start:
            continue
        selected.append(security)

    if args.limit > 0:
        selected = selected[: args.limit]
    return selected


def process_security(
    security: Security,
    source: MassiveSource,
    snapshot_dates: list[date],
) -> tuple[str, str, list[dict]]:
    symbol = security.symbol
    rows: list[dict] = []
    try:
        for snapshot_date in snapshot_dates:
            overview = source.get_ticker_overview(symbol, lookup_date=snapshot_date, allow_missing=True)
            total_shares = _extract_total_shares(overview)
            if total_shares is None:
                continue
            rows.append(
                {
                    "security_id": security.id,
                    "change_date": snapshot_date,
                    "total_shares": total_shares,
                }
            )
        if not rows:
            return symbol, "SUCCESS_NO_DATA", []
        deduped = {(row["security_id"], row["change_date"]): row for row in rows}
        return symbol, "SUCCESS", list(deduped.values())
    except Exception as e:
        logger.error("[{}] Massive shares 更新失败: {}", symbol, e, exc_info=True)
        return symbol, "ERROR", []


def _iter_batches(rows: list[dict], batch_size: int):
    for index in range(0, len(rows), batch_size):
        yield rows[index : index + batch_size]


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS)
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        end_date = get_last_completed_trading_date(args.market)
        history_floor = get_massive_history_floor(end_date)
        requested_start_date = date.fromisoformat(args.start_date) if args.start_date else history_floor
        start_date = max(requested_start_date, history_floor)
        snapshot_dates = get_quarter_snapshot_dates(start_date, end_date) if args.full_refresh else [end_date]

        securities = get_securities_to_process(db_manager, args, end_date)
        if not securities:
            logger.success("没有需要更新 shares 的证券。")
            return

        results_counter = Counter()
        all_rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, security, source, snapshot_dates): security
                for security in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="更新 Massive shares"):
                try:
                    symbol, status, rows = future.result()
                    results_counter[status] += 1
                    if rows:
                        all_rows.extend(rows)
                except Exception as exc:
                    security = future_to_security[future]
                    logger.error("任务 {} 发生未捕获异常: {}", security.symbol, exc, exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        float_rows: list[dict] = []
        latest_floats = source.get_latest_floats_batch([security.symbol for security in securities])
        for security in securities:
            float_info = latest_floats.get(security.symbol)
            if not float_info:
                continue
            effective_date = float_info.get("effective_date")
            free_float = float_info.get("free_float")
            if not effective_date or free_float in (None, ""):
                continue
            if effective_date < start_date:
                continue
            try:
                float_rows.append(
                    {
                        "security_id": security.id,
                        "change_date": effective_date,
                        "float_shares": int(free_float),
                    }
                )
            except Exception:
                continue

        if all_rows:
            for batch in _iter_batches(all_rows, UPSERT_BATCH_SIZE):
                db_manager.upsert_historical_shares(batch)
        if float_rows:
            for batch in _iter_batches(float_rows, UPSERT_BATCH_SIZE):
                db_manager.upsert_historical_shares(batch)

        logger.info("--- shares 更新统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无数据: {}", results_counter["SUCCESS_NO_DATA"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("  total_shares 行数: {}", len(all_rows))
        logger.info("  float_shares 行数: {}", len(float_rows))
        logger.info("----------------------")
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_shares 执行失败: {}", e)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
