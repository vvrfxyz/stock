import argparse
import os
import sys
from collections import Counter
from datetime import date, timedelta

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import get_massive_history_floor, iter_chunks
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)
from utils.trading_calendar import get_last_completed_trading_date

MAX_CONCURRENT_WORKERS = 2
API_BATCH_SIZE = 100


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive 更新 short interest / short volume。",
        default_workers=MAX_CONCURRENT_WORKERS,
        all_help="处理全部活跃 CS/ETF。",
    )
    parser.add_argument("--force", action="store_true", help="强制刷新 Massive 可覆盖窗口。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace, end_date: date) -> list[Security]:
    securities = select_us_securities(
        db_manager,
        args,
        active_scope="unless_symbols",
        order_column="short_data_last_updated_at",
    )

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
        # 死票回收防护：list_date 之前的做空数据属于该 symbol 的旧身份
        # （同 update_massive_prices 的回填 clamp）。
        list_date = symbol_to_security[symbol].list_date
        if list_date:
            interest_start_by_symbol[symbol] = max(interest_start_by_symbol[symbol], list_date)
            volume_start_by_symbol[symbol] = max(volume_start_by_symbol[symbol], list_date)

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


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    end_date = get_last_completed_trading_date(args.market)
    history_floor = get_massive_history_floor(end_date)

    securities = get_securities_to_update(db_manager, args, end_date)
    if not securities:
        logger.success("没有需要更新 short data 的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    batches = iter_chunks(securities, API_BATCH_SIZE)
    outputs, results_counter = run_concurrently(
        batches,
        lambda batch: process_batch(batch, source, db_manager, history_floor, args.force),
        max_workers=args.workers,
        desc="更新 Massive short data",
    )
    total_interests = 0
    total_volumes = 0
    for batch_counter, interest_count, volume_count in outputs:
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
    errors = results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    stats = {"processed": len(securities), "written": total_interests + total_volumes, "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_short_data", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
