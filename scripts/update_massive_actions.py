import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

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

ACTIONS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 8
API_BATCH_SIZE = 100
VENDOR_FACTOR_QUANT = Decimal("1.000000000000")


def _infer_currency(security: Security) -> str | None:
    if security.currency:
        return security.currency.upper()
    return "USD"


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive API 批量更新公司行动（分红、拆股）。",
        default_workers=MAX_CONCURRENT_WORKERS,
    )
    parser.add_argument("--force", action="store_true", help="强制刷新 Massive 可覆盖的最近 2 年窗口。")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=0,
        help="只拉取最近 N 天的新事件（忽略 90 天间隔，选取全部活跃证券）。"
             "用于每日轻量补新，弥补周日全量被跳过时的事件缺口。",
    )
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    return select_us_securities(
        db_manager,
        args,
        staleness_column="actions_last_updated_at",
        staleness_days=ACTIONS_UPDATE_INTERVAL_DAYS,
        skip_staleness=bool(args.force or args.recent_days),
    )


def _get_batch_start_date(
    securities: list[Security],
    history_floor: date,
    force: bool,
    recent_days: int = 0,
) -> str:
    if recent_days > 0:
        if any(security.actions_last_updated_at is None for security in securities):
            return history_floor.isoformat()
        return max(history_floor, date.today() - timedelta(days=recent_days)).isoformat()
    if force:
        return history_floor.isoformat()

    candidate_dates = []
    for security in securities:
        if not security.actions_last_updated_at:
            # 批内任一证券从未拉取过 actions 时，整批必须回到可覆盖窗口起点，
            # 否则该证券会只拿到其它证券增量窗口内的事件并被打上时间戳，历史事件永久缺失。
            return history_floor.isoformat()
        candidate_dates.append((security.actions_last_updated_at - timedelta(days=7)).date())
    return max(history_floor, min(candidate_dates)).isoformat()


def _group_by_ticker(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        ticker = (row.get("ticker") or "").lower()
        if ticker:
            grouped[ticker].append(row)
    return grouped


def _strip_ticker(rows: list[dict]) -> list[dict]:
    return [{key: value for key, value in row.items() if key != "ticker"} for row in rows]


def _to_adjustment_factor(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(VENDOR_FACTOR_QUANT)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _format_factor_key_decimal(value) -> str | None:
    """与 update_adjustment_factors._format_decimal 保持同一规范形式，确保 fallback factor_key 两侧可 join。"""
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, TypeError, ValueError):
        return str(value)


def _build_vendor_factor_rows(
    security: Security,
    dividends: list[dict],
    splits: list[dict],
    as_of_date: date,
) -> list[dict]:
    rows = []
    for item in dividends:
        adjustment_factor = _to_adjustment_factor(item.get("historical_adjustment_factor"))
        ex_date = item.get("ex_dividend_date") or item.get("ex_date")
        if adjustment_factor is None or not ex_date:
            continue
        source_event_id = item.get("source_event_id")
        factor_key = (
            f"dividend:{source_event_id}"
            if source_event_id
            else f"dividend:{ex_date}:{_format_factor_key_decimal(item.get('cash_amount'))}"
        )
        rows.append(
            {
                "security_id": security.id,
                "date": ex_date,
                "source": "MASSIVE",
                "factor_type": "historical_adjustment",
                "factor_key": factor_key,
                "source_event_id": source_event_id,
                "adjustment_factor": adjustment_factor,
                "as_of_date": as_of_date,
            }
        )

    for item in splits:
        adjustment_factor = _to_adjustment_factor(item.get("historical_adjustment_factor"))
        ex_date = item.get("execution_date") or item.get("ex_date")
        if adjustment_factor is None or not ex_date:
            continue
        source_event_id = item.get("source_event_id")
        factor_key = (
            f"split:{source_event_id}"
            if source_event_id
            else (
                f"split:{ex_date}:"
                f"{_format_factor_key_decimal(item.get('split_from'))}:"
                f"{_format_factor_key_decimal(item.get('split_to'))}"
            )
        )
        rows.append(
            {
                "security_id": security.id,
                "date": ex_date,
                "source": "MASSIVE",
                "factor_type": "historical_adjustment",
                "factor_key": factor_key,
                "source_event_id": source_event_id,
                "adjustment_factor": adjustment_factor,
                "as_of_date": as_of_date,
            }
        )
    return rows


def process_batch(
    securities: list[Security],
    source: MassiveSource,
    db_manager: DatabaseManager,
    history_floor,
    force: bool,
    recent_days: int = 0,
) -> tuple[Counter, list[Security]]:
    results_counter = Counter()
    changed: list[Security] = []
    batch_start = _get_batch_start_date(securities, history_floor, force, recent_days)
    symbols = [security.symbol for security in securities]

    dividends = source.get_dividends_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    splits = source.get_splits_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    dividends_by_symbol = _group_by_ticker(dividends)
    splits_by_symbol = _group_by_ticker(splits)
    as_of_date = get_last_completed_trading_date("US")

    for security in securities:
        symbol = security.symbol
        try:
            security_dividends = _strip_ticker(dividends_by_symbol.get(symbol, []))
            security_splits = _strip_ticker(splits_by_symbol.get(symbol, []))

            if security_dividends:
                inferred_currency = _infer_currency(security)
                normalized = []
                for item in security_dividends:
                    if not item.get("currency"):
                        item["currency"] = inferred_currency
                    if item.get("currency"):
                        normalized.append(item)
                security_dividends = normalized

            inserted_dividends = db_manager.upsert_dividends(security.id, security_dividends) if security_dividends else 0
            inserted_splits = db_manager.upsert_splits(security.id, security_splits) if security_splits else 0
            inserted_vendor_factors = db_manager.upsert_vendor_adjustment_factors(
                _build_vendor_factor_rows(security, security_dividends, security_splits, as_of_date)
            )
            db_manager.update_security_timestamp(security.id, "actions_last_updated_at")

            if inserted_dividends + inserted_splits + inserted_vendor_factors > 0:
                changed.append(security)
                results_counter["SUCCESS"] += 1
            elif security_dividends or security_splits:
                results_counter["SUCCESS_DUPLICATE_ONLY"] += 1
            else:
                results_counter["SUCCESS_NO_ACTIONS"] += 1
        except Exception as e:
            logger.opt(exception=e).error("[{}] Massive 公司行动落库失败: {}", symbol, e)
            results_counter["ERROR"] += 1
    return results_counter, changed


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    end_date = get_last_completed_trading_date(args.market)
    history_floor = get_massive_history_floor(end_date)
    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要更新 Massive 公司行动的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    batches = iter_chunks(securities, API_BATCH_SIZE)
    outputs, results_counter = run_concurrently(
        batches,
        lambda batch: process_batch(batch, source, db_manager, history_floor, args.force, args.recent_days),
        max_workers=args.workers,
        desc="更新 Massive 公司行动",
    )
    total_changed = 0
    for batch_counter, changed in outputs:
        results_counter.update(batch_counter)
        total_changed += len(changed)

    logger.info("--- 公司行动统计 ---")
    logger.info("  成功(有新增): {}", results_counter["SUCCESS"])
    logger.info("  成功(仅重复): {}", results_counter["SUCCESS_DUPLICATE_ONLY"])
    logger.info("  成功(无 actions): {}", results_counter["SUCCESS_NO_ACTIONS"])
    logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
    logger.info("--------------------")
    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    stats = {"processed": len(securities), "written": total_changed, "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_actions", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
