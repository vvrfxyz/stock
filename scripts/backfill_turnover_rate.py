import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from decimal import Decimal, getcontext
from typing import Optional

from loguru import logger
from sqlalchemy import func, update
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import DailyPrice, HistoricalShare, Security
from db_manager import DatabaseManager
from utils.massive_config import ALLOWED_US_SECURITY_TYPES

MAX_CONCURRENT_WORKERS = 8
UPSERT_BATCH_SIZE = 2000


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
        os.path.join(log_dir, f"rebuild_turnover_rate_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="基于 historical_shares 重建 daily_prices.turnover_rate（优先 float_shares）。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部保留类型证券。")
    parser.add_argument("--market", type=str, default="US", help="仅处理指定市场。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    parser.add_argument("--start-date", type=str, help="开始日期 YYYY-MM-DD。")
    parser.add_argument("--end-date", type=str, help="结束日期 YYYY-MM-DD。")
    parser.add_argument("--only-null", dest="only_null", action="store_true", help="仅填充 turnover_rate 为空的行。")
    parser.add_argument("--overwrite", dest="only_null", action="store_false", help="覆盖已有 turnover_rate。")
    parser.add_argument("--clear-first", action="store_true", help="先清空目标范围内的 turnover_rate，再重建。")
    parser.set_defaults(only_null=True)
    return parser


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _iter_batches(rows: list[dict], batch_size: int):
    for index in range(0, len(rows), batch_size):
        yield rows[index : index + batch_size]


def _pick_denominator(total_shares: Optional[int], float_shares: Optional[int]) -> Optional[int]:
    if float_shares and float_shares > 0:
        return float_shares
    if total_shares and total_shares > 0:
        return total_shares
    return None


def get_securities_to_process(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    symbols = [item.lower() for item in args.symbols if item]
    with db_manager.get_session() as session:
        query = session.query(Security).filter(func.upper(Security.market) == (args.market or "US").upper())

        if symbols:
            query = query.filter(Security.symbol.in_(symbols))
        else:
            query = query.filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))

        query = query.order_by(Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def _fetch_shares(session, security_id: int) -> list[tuple[date, Optional[int], Optional[int]]]:
    rows = (
        session.query(HistoricalShare.change_date, HistoricalShare.total_shares, HistoricalShare.float_shares)
        .filter(HistoricalShare.security_id == security_id)
        .filter((HistoricalShare.total_shares.isnot(None)) | (HistoricalShare.float_shares.isnot(None)))
        .order_by(HistoricalShare.change_date.asc())
        .all()
    )
    cleaned: list[tuple[date, Optional[int], Optional[int]]] = []
    for change_date, total_shares, float_shares in rows:
        if not change_date:
            continue
        try:
            total_int = int(total_shares) if total_shares is not None else None
        except Exception:
            total_int = None
        try:
            float_int = int(float_shares) if float_shares is not None else None
        except Exception:
            float_int = None
        if _pick_denominator(total_int, float_int) is None:
            continue
        cleaned.append((change_date, total_int, float_int))
    return cleaned


def _fetch_prices(
    session,
    security_id: int,
    only_null: bool,
    start_date: Optional[date],
    end_date: Optional[date],
) -> list[tuple[date, int]]:
    query = (
        session.query(DailyPrice.date, DailyPrice.volume)
        .filter(DailyPrice.security_id == security_id)
        .filter(DailyPrice.volume.isnot(None))
    )
    if only_null:
        query = query.filter(DailyPrice.turnover_rate.is_(None))
    if start_date:
        query = query.filter(DailyPrice.date >= start_date)
    if end_date:
        query = query.filter(DailyPrice.date <= end_date)

    rows = query.order_by(DailyPrice.date.asc()).all()
    cleaned: list[tuple[date, int]] = []
    for row_date, volume in rows:
        if not row_date or volume is None:
            continue
        try:
            volume_int = int(volume)
        except Exception:
            continue
        if volume_int < 0:
            continue
        cleaned.append((row_date, volume_int))
    return cleaned


def clear_turnover_rate(
    db_manager: DatabaseManager,
    security_id: int,
    start_date: Optional[date],
    end_date: Optional[date],
) -> None:
    with db_manager.get_session() as session:
        stmt = update(DailyPrice).where(DailyPrice.security_id == security_id)
        if start_date:
            stmt = stmt.where(DailyPrice.date >= start_date)
        if end_date:
            stmt = stmt.where(DailyPrice.date <= end_date)
        stmt = stmt.values(turnover_rate=None)
        session.execute(stmt)
        session.commit()


def process_security(
    security: Security,
    db_manager: DatabaseManager,
    only_null: bool,
    clear_first: bool,
    start_date: Optional[date],
    end_date: Optional[date],
) -> tuple[str, str, int]:
    symbol = security.symbol
    getcontext().prec = 28

    try:
        if clear_first:
            clear_turnover_rate(db_manager, security.id, start_date, end_date)

        with db_manager.get_session() as session:
            shares_rows = _fetch_shares(session, security.id)
            if not shares_rows:
                return symbol, "SKIP_NO_SHARES", 0

            price_rows = _fetch_prices(
                session,
                security.id,
                only_null=only_null and not clear_first,
                start_date=start_date,
                end_date=end_date,
            )
            if not price_rows:
                return symbol, "SUCCESS_NO_ROWS", 0

        updates: list[dict] = []
        shares_index = -1
        current_total: Optional[int] = None
        current_float: Optional[int] = None
        for price_date, volume in price_rows:
            while shares_index + 1 < len(shares_rows) and shares_rows[shares_index + 1][0] <= price_date:
                shares_index += 1
                _change_date, current_total, current_float = shares_rows[shares_index]

            denominator = _pick_denominator(current_total, current_float)
            if not denominator:
                continue

            updates.append(
                {
                    "security_id": security.id,
                    "date": price_date,
                    "turnover_rate": Decimal(volume) / Decimal(denominator),
                }
            )

        if not updates:
            return symbol, "SUCCESS_NO_ROWS", 0

        for batch in _iter_batches(updates, UPSERT_BATCH_SIZE):
            db_manager.upsert_daily_prices(batch)
        return symbol, "SUCCESS", len(updates)
    except Exception as e:
        logger.error("[{}] rebuild_turnover_rate 失败: {}", symbol, e, exc_info=True)
        return symbol, "ERROR", 0


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if args.start_date and start_date is None:
        raise ValueError("--start-date 必须是 YYYY-MM-DD 格式。")
    if args.end_date and end_date is None:
        raise ValueError("--end-date 必须是 YYYY-MM-DD 格式。")

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities = get_securities_to_process(db_manager, args)
        if not securities:
            logger.success("没有需要重建 turnover_rate 的证券。")
            return

        results_counter = Counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(
                    process_security,
                    security,
                    db_manager,
                    args.only_null,
                    args.clear_first,
                    start_date,
                    end_date,
                ): security
                for security in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="重建 turnover_rate"):
                try:
                    _symbol, status, _count = future.result()
                    results_counter[status] += 1
                except Exception as exc:
                    security = future_to_security[future]
                    logger.error("任务 {} 发生未捕获异常: {}", security.symbol, exc, exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- turnover_rate 重建统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无 shares: {}", results_counter["SKIP_NO_SHARES"])
        logger.info("  无行可写: {}", results_counter["SUCCESS_NO_ROWS"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("------------------------------")
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
