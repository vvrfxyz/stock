import argparse
import os
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta, date

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
    get_massive_history_floor,
)
from utils.clickhouse_client import ClickHouseClient
from utils.trading_calendar import get_last_completed_trading_date
from utils.script_logging import setup_logging as configure_script_logging

MAX_CONCURRENT_WORKERS = 18


def _clean_scalar(value, *, cast_int: bool = False):
    if value is None:
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    if cast_int:
        try:
            return int(value)
        except Exception:
            return None
    return value


def _sync_price_latest_date_from_existing_rows(
    security: Security,
    db_manager: DatabaseManager,
) -> date | None:
    """
    覆盖更新场景下，security.price_data_latest_date 可能落后于库里已有历史。
    用 daily_prices 的真实 max(date) 回写 metadata，避免后续增量判断失真。
    """
    actual_max_date = db_manager.get_security_price_max_date(security.id)
    tracked_latest_date = security.price_data_latest_date
    if actual_max_date and (tracked_latest_date is None or actual_max_date > tracked_latest_date):
        db_manager.update_security_price_latest_date(security.id, actual_max_date, is_full_run=False)
        logger.info(
            "[{}] 已对齐 price_data_latest_date: {} -> {}。",
            security.symbol,
            tracked_latest_date,
            actual_max_date,
        )
    return actual_max_date


def _finalize_price_metadata_after_successful_write(
    security: Security,
    db_manager: DatabaseManager,
    actual_max_date: date,
    *,
    is_full_run: bool,
) -> None:
    """
    价格写入成功后，统一回写 price_data_latest_date，并在 full-refresh 时刷新成功时间戳。
    即使 latest_date 没有变化，只要 full-refresh 成功，也应更新 full_data_last_updated_at。
    """
    tracked_latest_date = security.price_data_latest_date
    if not actual_max_date:
        return
    if is_full_run or tracked_latest_date is None or actual_max_date > tracked_latest_date:
        db_manager.update_security_price_latest_date(security.id, actual_max_date, is_full_run=is_full_run)
        if tracked_latest_date is None or actual_max_date > tracked_latest_date:
            logger.info(
                "[{}] 已对齐 price_data_latest_date: {} -> {}。",
                security.symbol,
                tracked_latest_date,
                actual_max_date,
            )


def setup_logging():
    configure_script_logging("update_massive_prices")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Custom Bars 获取美股日线数据并写入数据库。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--full-refresh", action="store_true", help="强制刷新 Massive 可覆盖的最近 2 年窗口。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace, end_trading_date: date) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            Security.is_active == True,
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
        )

        if args.symbols:
            query = query.filter(Security.symbol.in_([item.lower() for item in args.symbols]))

        if not args.full_refresh:
            query = query.filter(
                or_(
                    Security.price_data_latest_date.is_(None),
                    Security.price_data_latest_date < end_trading_date,
                )
            )

        query = query.order_by(Security.price_data_latest_date.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def process_security(
    security: Security,
    source: MassiveSource,
    db_manager: DatabaseManager,
    clickhouse_client: ClickHouseClient,
    full_refresh: bool,
    end_trading_date: date,
) -> tuple[str, str, int]:
    symbol = security.symbol
    history_floor = get_massive_history_floor(end_trading_date)

    try:
        end_date = end_trading_date.isoformat()
        if full_refresh or security.price_data_latest_date is None:
            start_dt = history_floor
            is_full_run = True
        else:
            next_date = security.price_data_latest_date + timedelta(days=1)
            start_dt = max(next_date, history_floor)
            is_full_run = start_dt == history_floor

        if start_dt > end_trading_date:
            return symbol, "SUCCESS_UP_TO_DATE", 0

        df = source.get_historical_data(symbol=symbol, start=start_dt.isoformat(), end=end_date, adjusted=False)
        if df.empty:
            actual_max_date = _sync_price_latest_date_from_existing_rows(security, db_manager)
            if actual_max_date and actual_max_date >= end_trading_date:
                return symbol, "SUCCESS_UP_TO_DATE", 0
            logger.info("[{}] Massive 在 {} - {} 未返回价格数据。", symbol, start_dt, end_date)
            return symbol, "SUCCESS_NO_NEW_DATA", 0

        df["date"] = df.index
        df.reset_index(drop=True, inplace=True)
        df["security_id"] = security.id
        df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True,
        )
        required_cols = [
            "security_id",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
            "otc",
        ]
        rows = []
        for row in df[required_cols].to_dict("records"):
            rows.append(
                {
                    "security_id": row["security_id"],
                    "date": row["date"],
                    "open": _clean_scalar(row["open"]),
                    "high": _clean_scalar(row["high"]),
                    "low": _clean_scalar(row["low"]),
                    "close": _clean_scalar(row["close"]),
                    "volume": _clean_scalar(row["volume"], cast_int=True),
                    "vwap": _clean_scalar(row["vwap"]),
                    "trade_count": _clean_scalar(row["trade_count"], cast_int=True),
                    "otc": _clean_scalar(row["otc"]),
                }
            )
        db_manager.upsert_daily_prices(rows)
        clickhouse_client.write_daily_bars(rows, source="MASSIVE", vendor_symbol=symbol)
        latest_date_in_db = db_manager.get_security_price_max_date(security.id)
        if latest_date_in_db is None:
            latest_date_in_db = df["date"].max()
        _finalize_price_metadata_after_successful_write(
            security,
            db_manager,
            latest_date_in_db,
            is_full_run=is_full_run,
        )
        return symbol, "SUCCESS", len(rows)
    except Exception as e:
        logger.opt(exception=e).error("[{}] 更新 Massive 日线失败: {}", symbol, e)
        return symbol, "ERROR", 0


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()
        clickhouse_client = ClickHouseClient.from_env()

        end_trading_date = get_last_completed_trading_date(args.market)
        securities = get_securities_to_update(db_manager, args, end_trading_date)
        if not securities:
            logger.success("没有需要更新 Massive 日线数据的证券。")
            return 0

        results_counter = Counter()
        total_rows = 0
        logger.info("共 {} 支证券需要更新 Massive 日线，截止交易日 {}。", len(securities), end_trading_date)

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, sec, source, db_manager, clickhouse_client, args.full_refresh, end_trading_date): sec
                for sec in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="更新 Massive 日线"):
                try:
                    _symbol, status, count = future.result()
                    results_counter[status] += 1
                    total_rows += count
                except Exception as exc:
                    security = future_to_security[future]
                    logger.opt(exception=exc).error("任务 {} 发生未捕获异常: {}", security.symbol, exc)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无新数据: {}", results_counter["SUCCESS_NO_NEW_DATA"])
        logger.info("  已是最新: {}", results_counter["SUCCESS_UP_TO_DATE"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("  写入行数: {}", total_rows)
        logger.info("----------------------")
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_prices 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
