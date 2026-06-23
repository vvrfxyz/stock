import argparse
import os
import sys

from requests.exceptions import RequestException
from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import is_supported_us_security_type
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)

UPDATE_INTERVAL_DAYS = 30
MAX_CONCURRENT_WORKERS = 24


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive API 更新数据库中的美股详情信息。",
        default_workers=MAX_CONCURRENT_WORKERS,
    )
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    return parser


def get_massive_reference_fallback_date(security: Security):
    """优先使用价格最新日期，再退化到详情更新时间和上市日期。"""
    if security.price_data_latest_date:
        return security.price_data_latest_date
    if security.info_last_updated_at:
        return security.info_last_updated_at.date()
    return security.list_date


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    return select_us_securities(
        db_manager,
        args,
        type_scope="unless_symbols",
        active_scope="unless_symbols",
        staleness_column="info_last_updated_at",
        staleness_days=UPDATE_INTERVAL_DAYS,
        skip_staleness=args.force,
    )


def ensure_missing_symbols_exist(
    db_manager: DatabaseManager,
    source: MassiveSource,
    symbols: list[str],
) -> int:
    inserted = 0
    if not symbols:
        return inserted

    with db_manager.get_session() as session:
        # 只把"活跃行"算作已存在：某 symbol 仅以退市(inactive)行存在时，
        # 复用该代码重新上市的新证券仍应作为新活跃行插入（与 active-only 部分唯一索引一致）。
        existing = {
            symbol
            for (symbol,) in session.query(Security.symbol)
            .filter(Security.symbol.in_(symbols), Security.is_active.is_(True))
            .all()
        }

    missing = [symbol for symbol in symbols if symbol not in existing]
    if not missing:
        return inserted

    new_rows: list[dict] = []
    for symbol in missing:
        payload = source.get_security_info(symbol)
        if not payload:
            logger.warning("[{}] Massive 未返回详情，无法插入新证券。", symbol)
            continue
        if not is_supported_us_security_type(payload.get("type")):
            logger.warning("[{}] Massive type={} 不在保留范围内，跳过。", symbol, payload.get("type"))
            continue
        new_rows.append(payload)

    if new_rows:
        inserted = db_manager.upsert_securities_by_symbol(new_rows, touch_info_timestamp=True)
    return inserted


def process_security(security: Security, source: MassiveSource, db_manager: DatabaseManager) -> tuple[str, str]:
    symbol = security.symbol
    try:
        fallback_date = get_massive_reference_fallback_date(security)
        payload = source.get_security_info(symbol, fallback_date=fallback_date)
        if not payload:
            return symbol, "SKIPPED_NO_DATA"

        payload["id"] = security.id
        db_manager.upsert_security_info(payload)
        return symbol, "SUCCESS"
    except RequestException as e:
        logger.error("[{}] 更新 Massive 详情失败(网络异常): {}", symbol, e)
        return symbol, "ERROR"
    except Exception as e:
        logger.opt(exception=e).error("[{}] 更新 Massive 详情失败: {}", symbol, e)
        return symbol, "ERROR"


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    symbols = [item.lower() for item in args.symbols if item]
    inserted = ensure_missing_symbols_exist(db_manager, source, symbols)
    if inserted:
        logger.info("已补插入 {} 支数据库中缺失的 symbol。", inserted)

    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要更新详情的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    logger.info("共 {} 支证券需要更新详情，将使用最多 {} 个线程。", len(securities), args.workers)
    outputs, results_counter = run_concurrently(
        securities,
        lambda security: process_security(security, source, db_manager),
        max_workers=args.workers,
        desc="更新 Massive 详情",
    )
    for _symbol, status in outputs:
        results_counter[status] += 1

    logger.info("--- 任务执行统计 ---")
    logger.info("  成功: {}", results_counter["SUCCESS"])
    logger.info("  跳过(无数据): {}", results_counter["SKIPPED_NO_DATA"])
    logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
    logger.info("----------------------")
    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    stats = {"processed": len(securities), "written": results_counter["SUCCESS"], "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_details", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
