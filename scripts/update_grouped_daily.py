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
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
)
from utils.script_logging import setup_logging as configure_script_logging
from utils.trading_calendar import get_last_completed_trading_date, shift_trading_date

MAX_CONCURRENT_WORKERS = 8

# 回填保险丝：只在最近 N 个交易日内允许 upsert 插入（N=10，覆盖周六 recent 窗口）。
# symbol->id 映射按"今日 symbol"构建，更早的历史日期归属需要 PIT symbol 解析
# （改名/回收会把 bar 错挂到当前占用该 symbol 的身份），在那之前对远期日期
# 退回旧语义：只更新该日已存在的 (security_id, date) 行，不 INSERT。
RECENT_UPSERT_WINDOW_SESSIONS = 10


def setup_logging():
    configure_script_logging("update_massive_grouped_daily")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive Daily Market Summary 回填/刷新指定日期范围内的日线记录。",
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


def load_symbol_to_id_map(session) -> dict[str, int]:
    """构建 active 证券的 lowercase symbol -> security_id 映射。

    symbol 是可变属性而非持久键：只取 is_active=True；同一 lowercase symbol
    命中多个 active security_id 时告警并整体剔除，绝不 last-wins。
    """
    rows = (
        session.query(Security.id, Security.symbol)
        .filter(Security.is_active.is_(True))
        .filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
        .filter(func.upper(Security.market) == "US")
        .all()
    )
    candidates: dict[str, set[int]] = {}
    for security_id, symbol in rows:
        candidates.setdefault(symbol.lower(), set()).add(security_id)

    mapping: dict[str, int] = {}
    for symbol, ids in candidates.items():
        if len(ids) > 1:
            logger.warning("symbol {} 映射到多个 active security_id {}，已整体剔除。", symbol, sorted(ids))
            continue
        mapping[symbol] = next(iter(ids))
    return mapping


def load_null_watermark_ids(session) -> set[int]:
    """price_data_latest_date 为 NULL 的证券集合。

    NULL 水位是 update_massive_prices 全量回填的唯一自动触发条件，
    grouped daily 不得为这些证券盖戳，否则回填入口被永久关闭。
    """
    return {
        security_id
        for (security_id,) in session.query(Security.id).filter(Security.price_data_latest_date.is_(None))
    }


def process_date(
    target_date: date,
    source: MassiveSource,
    db_manager: DatabaseManager,
    symbol_to_id_map: dict[str, int],
    *,
    allow_insert: bool = True,
    skip_stamp_ids: set[int] | frozenset[int] = frozenset(),
) -> tuple[str, str, int]:
    date_str = target_date.isoformat()
    try:
        existing_security_ids: set[int] | None = None
        if not allow_insert:
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

        rows: dict[int, dict] = {}
        seen_tickers: dict[int, str] = {}
        for agg in daily_aggs:
            raw_ticker = agg.get("T") or ""
            symbol = raw_ticker.lower()
            security_id = symbol_to_id_map.get(symbol)
            if security_id is None:
                continue
            if existing_security_ids is not None and security_id not in existing_security_ids:
                continue

            if security_id in seen_tickers:
                prev = seen_tickers[security_id]
                if raw_ticker != prev:
                    logger.warning(
                        "[{}] ticker 碰撞: {} 与 {} 同映射 security_id={}，保留首条 {}",
                        date_str, raw_ticker, prev, security_id, prev,
                    )
                continue
            seen_tickers[security_id] = raw_ticker

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
                rows[security_id] = mapping

        if not rows:
            return date_str, "SUCCESS_NO_INTERSECTION", 0

        price_rows = list(rows.values())
        if allow_insert:
            written = db_manager.upsert_daily_prices(price_rows)
        else:
            written = db_manager.bulk_update_mappings(DailyPrice, price_rows)
        stamp_ids = [security_id for security_id in rows if security_id not in skip_stamp_ids]
        if stamp_ids:
            db_manager.ensure_security_price_latest_date_at_least(stamp_ids, target_date)
        return date_str, "SUCCESS", written
    except Exception as e:
        logger.opt(exception=e).error("[{}] grouped daily 刷新失败: {}", date_str, e)
        return date_str, "ERROR", 0


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args(argv)

    db_manager = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        last_completed = get_last_completed_trading_date(args.market)
        end_date = date.fromisoformat(args.end_date)
        if end_date > last_completed:
            logger.warning(
                "--end-date {} 超出最近已完成交易日 {}，超出部分跳过。",
                end_date.isoformat(), last_completed.isoformat(),
            )
            end_date = last_completed

        dates_to_process = get_dates_to_process(args.start_date, end_date.isoformat())
        if not dates_to_process:
            logger.warning("日期范围为空，已跳过。")
            return 0

        with db_manager.get_session() as session:
            symbol_to_id_map = load_symbol_to_id_map(session)
            null_watermark_ids = load_null_watermark_ids(session)

        upsert_floor = shift_trading_date(args.market, last_completed, -(RECENT_UPSERT_WINDOW_SESSIONS - 1))
        legacy_dates = [dt for dt in dates_to_process if dt < upsert_floor]
        if legacy_dates:
            logger.warning(
                "{} 个日期早于近窗下限 {}（最近 {} 个交易日），退回仅更新已存在行、不回填插入。",
                len(legacy_dates), upsert_floor.isoformat(), RECENT_UPSERT_WINDOW_SESSIONS,
            )

        results_counter = Counter()
        total_updated = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_date = {
                executor.submit(
                    process_date,
                    dt,
                    source,
                    db_manager,
                    symbol_to_id_map,
                    allow_insert=dt >= upsert_floor,
                    skip_stamp_ids=null_watermark_ids,
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
        errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
        return 1 if errors else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_grouped_daily 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
