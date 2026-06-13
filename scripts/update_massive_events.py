import argparse
import os
import sys

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)

EVENTS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 8


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive Ticker Events 更新 symbol history。",
        default_workers=MAX_CONCURRENT_WORKERS,
        all_help="处理全部活跃 CS/ETF。",
    )
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    return select_us_securities(
        db_manager,
        args,
        active_scope="unless_symbols",
        staleness_column="events_last_updated_at",
        staleness_days=EVENTS_UPDATE_INTERVAL_DAYS,
        skip_staleness=args.force,
    )


def process_security(security: Security, source: MassiveSource, db_manager: DatabaseManager) -> tuple[str, str, int]:
    try:
        payload = source.get_ticker_events(security.symbol)
        events = (payload or {}).get("events") or []
        rows_by_key = {}
        for event in events:
            if event.get("type") != "ticker_change":
                continue
            ticker = ((event.get("ticker_change") or {}).get("ticker") or "").lower()
            event_date = event.get("date")
            if not ticker or not event_date:
                continue
            rows_by_key[(security.id, ticker, "MASSIVE", event_date)] = {
                "security_id": security.id,
                "symbol": ticker,
                "exchange": security.exchange,
                "source": "MASSIVE",
                "source_event_id": f"{security.id}:{ticker}:{event_date}",
                "event_type": event.get("type"),
                "start_date": event_date,
            }
        rows = list(rows_by_key.values())
        inserted = db_manager.upsert_symbol_history(rows) if rows else 0
        db_manager.update_security_timestamp(security.id, "events_last_updated_at")
        return security.symbol, "SUCCESS", inserted
    except Exception as exc:
        logger.opt(exception=exc).error("[{}] ticker events 更新失败: {}", security.symbol, exc)
        return security.symbol, "ERROR", 0


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要更新 ticker events 的证券。")
        return 0

    outputs, results_counter = run_concurrently(
        securities,
        lambda security: process_security(security, source, db_manager),
        max_workers=args.workers,
        desc="更新 Massive events",
    )
    total_rows = 0
    for _symbol, status, count in outputs:
        results_counter[status] += 1
        total_rows += count

    logger.info("--- ticker events 更新统计 ---")
    logger.info("  成功: {}", results_counter["SUCCESS"])
    logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
    logger.info("  写入 symbol history 行数: {}", total_rows)
    logger.info("------------------------------")
    return 1 if results_counter["ERROR"] + results_counter["FATAL_ERROR"] else 0


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_events", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
