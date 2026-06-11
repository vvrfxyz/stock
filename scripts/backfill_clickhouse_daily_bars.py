import argparse
import os
import sys
import time
from datetime import date, timedelta

from loguru import logger
from sqlalchemy import and_, func, or_

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import DailyPrice, Security
from db_manager import DatabaseManager
from utils.clickhouse_client import ClickHouseClient
from utils.script_logging import setup_logging as configure_script_logging


def setup_logging():
    configure_script_logging("backfill_clickhouse_daily_bars")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从 PostgreSQL daily_prices 分批回填 ClickHouse 日线表。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--start-date", type=str, help="开始日期 YYYY-MM-DD。")
    parser.add_argument("--end-date", type=str, help="结束日期 YYYY-MM-DD。")
    parser.add_argument("--source", type=str, default="POSTGRESQL", help="写入 ClickHouse 的 source 标签。")
    parser.add_argument("--batch-size", type=int, default=10000, help="每批写入行数。")
    parser.add_argument("--limit", type=int, default=0, help="最多回填多少行；0 表示不限制。")
    return parser


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _next_month(value: date) -> date:
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def _get_price_date_bounds(db_manager: DatabaseManager) -> tuple[date | None, date | None]:
    with db_manager.get_session() as session:
        return (
            session.query(func.min(DailyPrice.date), func.max(DailyPrice.date))
            .filter(DailyPrice.open.isnot(None))
            .filter(DailyPrice.high.isnot(None))
            .filter(DailyPrice.low.isnot(None))
            .filter(DailyPrice.close.isnot(None))
            .one()
        )


def _fetch_batch(
    db_manager: DatabaseManager,
    *,
    start_date: date | None,
    end_date: date | None,
    last_security_id: int | None,
    last_date: date | None,
    batch_size: int,
) -> list[dict]:
    with db_manager.get_session() as session:
        query = (
            session.query(
                DailyPrice.security_id,
                DailyPrice.date,
                DailyPrice.open,
                DailyPrice.high,
                DailyPrice.low,
                DailyPrice.close,
                DailyPrice.volume,
                DailyPrice.vwap,
                DailyPrice.trade_count,
                Security.symbol.label("vendor_symbol"),
            )
            .join(Security, Security.id == DailyPrice.security_id)
            .filter(DailyPrice.open.isnot(None))
            .filter(DailyPrice.high.isnot(None))
            .filter(DailyPrice.low.isnot(None))
            .filter(DailyPrice.close.isnot(None))
        )

        if start_date:
            query = query.filter(DailyPrice.date >= start_date)
        if end_date:
            query = query.filter(DailyPrice.date <= end_date)
        if last_security_id is not None and last_date is not None:
            query = query.filter(
                or_(
                    DailyPrice.date > last_date,
                    and_(DailyPrice.date == last_date, DailyPrice.security_id > last_security_id),
                )
            )

        records = (
            query.order_by(DailyPrice.date.asc(), DailyPrice.security_id.asc())
            .limit(batch_size)
            .all()
        )

    return [
        {
            "security_id": record.security_id,
            "date": record.date,
            "open": record.open,
            "high": record.high,
            "low": record.low,
            "close": record.close,
            "volume": record.volume,
            "vwap": record.vwap,
            "trade_count": record.trade_count,
            "vendor_symbol": record.vendor_symbol,
        }
        for record in records
    ]


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("--batch-size 必须大于 0。")

    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if start_date and end_date and start_date > end_date:
        raise ValueError("--start-date 不能晚于 --end-date。")

    db_manager = None
    try:
        db_manager = DatabaseManager()
        clickhouse_client = ClickHouseClient.from_env(strict=True)
        clickhouse_client.ensure_schema()

        total_written = 0
        source = args.source.upper()
        min_price_date, max_price_date = _get_price_date_bounds(db_manager)
        if min_price_date is None or max_price_date is None:
            logger.warning("PostgreSQL daily_prices 没有可回填的完整 OHLC 记录。")
            return

        current_month = _month_start(start_date or min_price_date)
        final_date = end_date or max_price_date

        while current_month <= final_date:
            window_start = max(current_month, start_date or current_month)
            window_end = min(_next_month(current_month) - timedelta(days=1), final_date)
            last_security_id = None
            last_date = None

            while True:
                remaining = args.limit - total_written if args.limit > 0 else args.batch_size
                if args.limit > 0 and remaining <= 0:
                    break

                batch_size = min(args.batch_size, remaining) if args.limit > 0 else args.batch_size
                rows = _fetch_batch(
                    db_manager,
                    start_date=window_start,
                    end_date=window_end,
                    last_security_id=last_security_id,
                    last_date=last_date,
                    batch_size=batch_size,
                )
                if not rows:
                    break

                written = clickhouse_client.write_daily_bars(rows, source=source)
                total_written += written
                last_security_id = rows[-1]["security_id"]
                last_date = rows[-1]["date"]
                logger.info(
                    "已回填 ClickHouse 日线: 本批 {} 行，累计 {} 行，窗口 {} - {}，当前游标 {} / {}。",
                    written,
                    total_written,
                    window_start,
                    window_end,
                    last_date,
                    last_security_id,
                )

                if written == 0:
                    break

            if args.limit > 0 and total_written >= args.limit:
                break
            current_month = _next_month(current_month)

        logger.success("ClickHouse 日线回填完成，累计写入 {} 行。", total_written)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
