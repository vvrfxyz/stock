import argparse
import os
import sys
from collections import Counter
from datetime import date

from loguru import logger
from sqlalchemy import func

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import HistoricalShare, Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    enforce_us_market,
    get_massive_history_floor,
    get_quarter_snapshot_dates,
)
from utils.massive_task import build_standard_parser, run_concurrently, run_massive_task
from utils.trading_calendar import get_last_completed_trading_date

MAX_CONCURRENT_WORKERS = 8
UPSERT_BATCH_SIZE = 1000


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive Ticker Overview / Float 更新 historical_shares。",
        default_workers=MAX_CONCURRENT_WORKERS,
        all_help="处理全部保留类型证券。",
    )
    parser.add_argument("--full-refresh", action="store_true", help="回填 Massive 可覆盖的最近 2 年季度快照。")
    parser.add_argument("--start-date", type=str, help="起始日期 YYYY-MM-DD。仅在 full-refresh 时生效。")
    return parser


def _quarter_start(target_date: date) -> date:
    month = ((target_date.month - 1) // 3) * 3 + 1
    return date(target_date.year, month, 1)


def _extract_total_shares(overview: dict | None) -> int | None:
    if not overview:
        return None
    # 优先保留 share class 的实际 outstanding shares；
    # weighted_shares_outstanding 是 period-weighted 口径，只在前者缺失时兜底。
    for key in ("share_class_shares_outstanding", "weighted_shares_outstanding"):
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
            session.query(Security, func.max(HistoricalShare.filing_date))
            .outerjoin(HistoricalShare, HistoricalShare.security_id == Security.id)
            .filter(func.upper(Security.market) == enforce_us_market(args.market))
            .filter(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
            .group_by(Security.id)
            .order_by(Security.symbol.asc())
            .all()
        )

    symbols = {symbol.lower() for symbol in args.symbols if symbol}
    selected: list[Security] = []
    for security, latest_filing_date in rows:
        if symbols and security.symbol not in symbols:
            continue
        if not args.full_refresh and not symbols and security.is_active is not True:
            continue
        if not args.full_refresh and not symbols and latest_filing_date and latest_filing_date >= current_quarter_start:
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
                    "filing_date": snapshot_date,
                    "period_end_date": snapshot_date,
                    "total_shares": total_shares,
                    "source": "MASSIVE",
                }
            )
        if not rows:
            return symbol, "SUCCESS_NO_DATA", []
        deduped = {(row["security_id"], row["filing_date"], row["source"]): row for row in rows}
        return symbol, "SUCCESS", list(deduped.values())
    except Exception as e:
        logger.opt(exception=e).error("[{}] Massive shares 更新失败: {}", symbol, e)
        return symbol, "ERROR", []


def _iter_batches(rows: list[dict], batch_size: int):
    for index in range(0, len(rows), batch_size):
        yield rows[index : index + batch_size]


def _attach_float_fields(
    all_rows: list[dict],
    floats_by_symbol: dict[str, list[dict]],
    security_id_to_symbol: dict[int, str],
) -> int:
    """
    将 effective_date <= filing_date 的最近一条 float 附加到股本快照行。
    找不到当时已生效的 float 时保持为空：filing_date 是防未来函数边界，
    不允许把未来才生效的 float 写进历史快照（historical_floats 保有完整序列）。
    """
    matched = 0
    for row in all_rows:
        symbol = security_id_to_symbol.get(row["security_id"])
        if not symbol:
            continue
        candidates = [
            item
            for item in floats_by_symbol.get(symbol, [])
            if item.get("effective_date") and item["effective_date"] <= row["filing_date"]
        ]
        if not candidates:
            continue
        float_info = candidates[-1]
        row["float_shares"] = float_info.get("free_float")
        row["free_float_percent"] = float_info.get("free_float_percent")
        matched += 1
    return matched


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    end_date = get_last_completed_trading_date(args.market)
    history_floor = get_massive_history_floor(end_date)
    requested_start_date = date.fromisoformat(args.start_date) if args.start_date else history_floor
    start_date = max(requested_start_date, history_floor)
    snapshot_dates = get_quarter_snapshot_dates(start_date, end_date) if args.full_refresh else [end_date]

    securities = get_securities_to_process(db_manager, args, end_date)
    if not securities:
        logger.success("没有需要更新 shares 的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    outputs, results_counter = run_concurrently(
        securities,
        lambda security: process_security(security, source, snapshot_dates),
        max_workers=args.workers,
        desc="更新 Massive shares",
    )
    all_rows: list[dict] = []
    for _symbol, status, rows in outputs:
        results_counter[status] += 1
        if rows:
            all_rows.extend(rows)

    float_rows = source.get_float_batch([security.symbol for security in securities])
    floats_by_symbol: dict[str, list[dict]] = {}
    for row in float_rows:
        ticker = row.get("ticker")
        effective_date = row.get("effective_date")
        if not ticker or not effective_date:
            continue
        floats_by_symbol.setdefault(ticker, []).append(row)
    for rows in floats_by_symbol.values():
        rows.sort(key=lambda item: item["effective_date"])

    symbol_to_security = {security.symbol: security for security in securities}
    security_id_to_symbol = {security.id: security.symbol for security in securities}
    historical_float_rows: list[dict] = []
    for row in float_rows:
        security = symbol_to_security.get(row.get("ticker"))
        if not security:
            continue
        historical_float_rows.append(
            {
                "security_id": security.id,
                "effective_date": row.get("effective_date"),
                "free_float": row.get("free_float"),
                "free_float_percent": row.get("free_float_percent"),
                "source": "MASSIVE",
            }
        )

    for row in all_rows:
        row.setdefault("float_shares", None)
        row.setdefault("free_float_percent", None)
    float_match_count = _attach_float_fields(all_rows, floats_by_symbol, security_id_to_symbol)

    if all_rows:
        for batch in _iter_batches(all_rows, UPSERT_BATCH_SIZE):
            db_manager.upsert_historical_shares(batch)
    if historical_float_rows:
        for batch in _iter_batches(historical_float_rows, UPSERT_BATCH_SIZE):
            db_manager.upsert_historical_floats(batch)

    db_manager.update_security_timestamps([security.id for security in securities], "shares_last_updated_at")

    logger.info("--- shares 更新统计 ---")
    logger.info("  成功: {}", results_counter["SUCCESS"])
    logger.info("  无数据: {}", results_counter["SUCCESS_NO_DATA"])
    logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
    logger.info("  total_shares 行数: {}", len(all_rows))
    logger.info("  historical_floats 行数: {}", len(historical_float_rows))
    logger.info("  float_shares 匹配行数: {}", float_match_count)
    logger.info("----------------------")
    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    if exit_code != 0:
        logger.error("shares 更新存在失败 symbol，本轮退出码设为 {}，以便外层重跑该 chunk。", exit_code)
    stats = {"processed": len(securities), "written": len(all_rows) + len(historical_float_rows), "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_shares", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
