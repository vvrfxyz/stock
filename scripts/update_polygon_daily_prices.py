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
from utils.trading_calendar import get_last_completed_trading_date

MAX_CONCURRENT_WORKERS = 18


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
        os.path.join(log_dir, f"update_massive_prices_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


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
    history_floor = get_massive_history_floor(end_trading_date)
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
        securities = query.all()

    if args.full_refresh:
        return securities

    filtered: list[Security] = []
    for security in securities:
        if security.price_data_latest_date and security.price_data_latest_date >= end_trading_date:
            continue
        if security.price_data_latest_date and security.price_data_latest_date < history_floor:
            filtered.append(security)
            continue
        filtered.append(security)
    return filtered


def process_security(
    security: Security,
    source: MassiveSource,
    db_manager: DatabaseManager,
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
        required_cols = ["security_id", "date", "open", "high", "low", "close", "volume", "turnover", "vwap"]
        rows = df[required_cols].to_dict("records")
        db_manager.upsert_daily_prices(rows)
        latest_date_in_df = df["date"].max()
        db_manager.update_security_price_latest_date(security.id, latest_date_in_df, is_full_run)
        return symbol, "SUCCESS", len(rows)
    except Exception as e:
        logger.error("[{}] 更新 Massive 日线失败: {}", symbol, e, exc_info=True)
        return symbol, "ERROR", 0


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

        end_trading_date = get_last_completed_trading_date(args.market)
        securities = get_securities_to_update(db_manager, args, end_trading_date)
        if not securities:
            logger.success("没有需要更新 Massive 日线数据的证券。")
            return

        results_counter = Counter()
        total_rows = 0
        logger.info("共 {} 支证券需要更新 Massive 日线，截止交易日 {}。", len(securities), end_trading_date)

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, sec, source, db_manager, args.full_refresh, end_trading_date): sec
                for sec in securities
            }
            for future in tqdm(as_completed(future_to_security), total=len(securities), desc="更新 Massive 日线"):
                try:
                    _symbol, status, count = future.result()
                    results_counter[status] += 1
                    total_rows += count
                except Exception as exc:
                    security = future_to_security[future]
                    logger.error("任务 {} 发生未捕获异常: {}", security.symbol, exc, exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无新数据: {}", results_counter["SUCCESS_NO_NEW_DATA"])
        logger.info("  已是最新: {}", results_counter["SUCCESS_UP_TO_DATE"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("  写入行数: {}", total_rows)
        logger.info("----------------------")
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_prices 执行失败: {}", e)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
