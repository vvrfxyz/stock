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

from data_models.models import DailyPrice, Security
from data_sources.massive_source import MassiveSource, normalize_volume_value
from db_manager import DatabaseManager
from utils.clickhouse_client import ClickHouseClient
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
)
from utils.script_logging import setup_logging as configure_script_logging

MAX_CONCURRENT_WORKERS = 8


def setup_logging():
    configure_script_logging("update_massive_grouped_daily")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Daily Market Summary 刷新指定日期范围内的已存在日线记录。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--start-date", type=str, required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="并发线程数。")
    return parser


def get_dates_to_process(start_str: str, end_str: str) -> list[date]:
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    if start > end:
        return []
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def process_date(
    target_date: date,
    source: MassiveSource,
    db_manager: DatabaseManager,
    clickhouse_client: ClickHouseClient,
    symbol_to_id_map: dict[str, int],
    id_to_symbol_map: dict[int, str],
) -> tuple[str, str, int]:
    date_str = target_date.isoformat()
    try:
        with db_manager.get_session() as session:
            existing_security_ids = {
                security_id
                for (security_id,) in session.query(DailyPrice.security_id).filter(DailyPrice.date == target_date)
            }

        if not existing_security_ids:
            return date_str, "SKIPPED_NO_EXISTING_DATA", 0

        daily_aggs = source.get_grouped_daily_data(date_str, adjusted=False, include_otc=False)
        if not daily_aggs:
            return date_str, "SUCCESS_NO_API_DATA", 0

        updates: dict[int, dict] = {}
        for agg in daily_aggs:
            symbol = (agg.get("T") or "").lower()
            security_id = symbol_to_id_map.get(symbol)
            if security_id is None or security_id not in existing_security_ids:
                continue

            volume = normalize_volume_value(agg.get("v"))
            trade_count = agg.get("n")
            mapping = {
                "security_id": security_id,
                "date": target_date,
            }
            for column_name, api_key in (
                ("open", "o"),
                ("high", "h"),
                ("low", "l"),
                ("close", "c"),
                ("vwap", "vw"),
            ):
                value = agg.get(api_key)
                if value is not None:
                    mapping[column_name] = value
            if volume is not None:
                mapping["volume"] = volume
            if trade_count is not None:
                mapping["trade_count"] = trade_count
            if agg.get("otc") is not None:
                mapping["otc"] = agg.get("otc")
            if len(mapping) > 2:
                updates[security_id] = mapping

        if not updates:
            return date_str, "SUCCESS_NO_INTERSECTION", 0

        update_rows = list(updates.values())
        db_manager.bulk_update_mappings(DailyPrice, update_rows)
        ch_rows = [
            dict(row, vendor_symbol=id_to_symbol_map.get(row["security_id"], ""))
            for row in update_rows
        ]
        clickhouse_client.write_daily_bars(ch_rows, source="MASSIVE")
        db_manager.ensure_security_price_latest_date_at_least(list(updates.keys()), target_date)
        return date_str, "SUCCESS", len(updates)
    except Exception as e:
        logger.opt(exception=e).error("[{}] grouped daily 刷新失败: {}", date_str, e)
        return date_str, "ERROR", 0


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

        with db_manager.get_session() as session:
            securities = (
                session.query(Security.id, Security.symbol)
                .filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
                .filter(func.upper(Security.market) == "US")
                .all()
            )
        symbol_to_id_map = {symbol.lower(): security_id for security_id, symbol in securities}
        id_to_symbol_map = {security_id: symbol.lower() for security_id, symbol in securities}

        dates_to_process = get_dates_to_process(args.start_date, args.end_date)
        if not dates_to_process:
            logger.warning("日期范围为空，已跳过。")
            return 0

        results_counter = Counter()
        total_updated = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_date = {
                executor.submit(
                    process_date, dt, source, db_manager, clickhouse_client, symbol_to_id_map, id_to_symbol_map
                ): dt
                for dt in dates_to_process
            }
            for future in tqdm(as_completed(future_to_date), total=len(dates_to_process), desc="刷新 Massive grouped daily"):
                try:
                    _date_str, status, count = future.result()
                    results_counter[status] += 1
                    total_updated += count
                except Exception as exc:
                    dt = future_to_date[future]
                    logger.opt(exception=exc).error("任务 {} 发生未捕获异常: {}", dt.isoformat(), exc)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        for status, count in results_counter.items():
            logger.info("  {}: {}", status, count)
        logger.info("  更新记录数: {}", total_updated)
        logger.info("----------------------")
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_grouped_daily 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
