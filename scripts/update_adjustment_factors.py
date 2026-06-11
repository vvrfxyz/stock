import argparse
import hashlib
import json
import os
import sys
import time
from bisect import bisect_left
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, localcontext

from loguru import logger
from sqlalchemy import func
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import CorporateAction, DailyPrice, Security, VendorAdjustmentFactor
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
from utils.script_logging import setup_logging as configure_script_logging


METHODOLOGY_VERSION = "raw_actions_v1"
FACTOR_QUANT = Decimal("1.000000000000")
DEFAULT_TOLERANCE = Decimal("0.000010")


def setup_logging():
    configure_script_logging("update_adjustment_factors")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="重建内部复权因子 cache，并与供应商 reference 因子对账。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理所有活跃 CS/ETF。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--source", type=str, default="MASSIVE", help="公司行动/供应商因子来源。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--methodology-version", default=METHODOLOGY_VERSION, help="内部计算口径版本。")
    parser.add_argument("--tolerance", default=str(DEFAULT_TOLERANCE), help="对账容忍误差。")
    parser.add_argument(
        "--refresh-vendor-daily-bars",
        action="store_true",
        help="额外拉取 Massive adjusted=true/false 日线并保存 daily bar reference 因子。",
    )
    parser.add_argument("--daily-start-date", type=str, help="refresh vendor daily bars 的开始日期。")
    parser.add_argument("--daily-end-date", type=str, help="refresh vendor daily bars 的结束日期。")
    return parser


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _quantize_factor(value: Decimal) -> Decimal:
    return value.quantize(FACTOR_QUANT)


def _format_decimal(value) -> str | None:
    if value is None:
        return None
    decimal_value = _to_decimal(value)
    if decimal_value is None:
        return None
    return format(decimal_value.normalize(), "f")


def build_factor_key(action) -> str:
    source_event_id = getattr(action, "source_event_id", None)
    action_type = (getattr(action, "action_type", "") or "").upper()
    if action_type == "DIVIDEND":
        if source_event_id:
            return f"dividend:{source_event_id}"
        return f"dividend:{getattr(action, 'ex_date', None)}:{_format_decimal(getattr(action, 'cash_amount', None))}"
    if action_type == "SPLIT":
        if source_event_id:
            return f"split:{source_event_id}"
        return (
            f"split:{getattr(action, 'ex_date', None)}:"
            f"{_format_decimal(getattr(action, 'split_from', None))}:"
            f"{_format_decimal(getattr(action, 'split_to', None))}"
        )
    return f"{action_type.lower()}:{source_event_id or getattr(action, 'ex_date', None)}"


def _is_synthetic_source_event_id(source_event_id: str | None) -> bool:
    if not source_event_id:
        return True
    return source_event_id.startswith("massive-dividend:") or source_event_id.startswith("massive-split:")


def _economic_action_key(action) -> tuple:
    action_type = (getattr(action, "action_type", "") or "").upper()
    if action_type == "DIVIDEND":
        return (
            action_type,
            getattr(action, "ex_date", None),
            _format_decimal(getattr(action, "cash_amount", None)),
            getattr(action, "currency", None),
        )
    if action_type == "SPLIT":
        return (
            action_type,
            getattr(action, "ex_date", None),
            _format_decimal(getattr(action, "split_from", None)),
            _format_decimal(getattr(action, "split_to", None)),
        )
    return (
        action_type,
        getattr(action, "ex_date", None),
        getattr(action, "source_event_id", None),
    )


def _prefer_action(candidate, current):
    if current is None:
        return candidate
    candidate_is_synthetic = _is_synthetic_source_event_id(getattr(candidate, "source_event_id", None))
    current_is_synthetic = _is_synthetic_source_event_id(getattr(current, "source_event_id", None))
    if current_is_synthetic and not candidate_is_synthetic:
        return candidate
    return current


def dedupe_economic_actions(actions) -> tuple[list, int]:
    deduped = {}
    duplicate_count = 0
    for action in actions:
        key = _economic_action_key(action)
        preferred = _prefer_action(action, deduped.get(key))
        if key in deduped:
            duplicate_count += 1
        deduped[key] = preferred
    return list(deduped.values()), duplicate_count


def _find_previous_close(price_dates: list[date], close_by_date: dict[date, Decimal], ex_date: date) -> Decimal | None:
    index = bisect_left(price_dates, ex_date) - 1
    if index < 0:
        return None
    return close_by_date.get(price_dates[index])


def _event_payload(action, previous_close: Decimal | None, single_event_factor: Decimal | None) -> dict:
    return {
        "action_type": getattr(action, "action_type", None),
        "cash_amount": _format_decimal(getattr(action, "cash_amount", None)),
        "ex_date": getattr(action, "ex_date", None).isoformat() if getattr(action, "ex_date", None) else None,
        "previous_close": _format_decimal(previous_close),
        "single_event_factor": _format_decimal(single_event_factor),
        "source": getattr(action, "source", None),
        "source_event_id": getattr(action, "source_event_id", None),
        "split_from": _format_decimal(getattr(action, "split_from", None)),
        "split_to": _format_decimal(getattr(action, "split_to", None)),
    }


def _event_hash(event_payloads: list[dict]) -> str:
    raw = json.dumps(event_payloads, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _single_event_factor(action, previous_close: Decimal | None) -> Decimal | None:
    action_type = (getattr(action, "action_type", "") or "").upper()
    with localcontext() as ctx:
        ctx.prec = 34
        if action_type == "DIVIDEND":
            cash_amount = _to_decimal(getattr(action, "cash_amount", None))
            if cash_amount is None or previous_close is None or previous_close <= 0:
                return None
            if cash_amount < 0 or cash_amount >= previous_close:
                return None
            return _quantize_factor((previous_close - cash_amount) / previous_close)

        if action_type == "SPLIT":
            split_from = _to_decimal(getattr(action, "split_from", None))
            split_to = _to_decimal(getattr(action, "split_to", None))
            if split_from is None or split_to is None or split_from <= 0 or split_to <= 0:
                return None
            return _quantize_factor(split_from / split_to)

    return None


def compute_adjustment_factor_rows(
    security_id: int,
    actions,
    price_dates: list[date],
    close_by_date: dict[date, Decimal],
    *,
    methodology_version: str,
    as_of_date: date,
) -> tuple[list[dict], Counter]:
    event_items = []
    stats = Counter()
    actions, duplicate_count = dedupe_economic_actions(actions)
    if duplicate_count:
        stats["DEDUPLICATED_ECONOMIC_EVENTS"] += duplicate_count
    for action in actions:
        ex_date = getattr(action, "ex_date", None)
        if not ex_date:
            stats["SKIP_NO_DATE"] += 1
            continue
        previous_close = None
        if (getattr(action, "action_type", "") or "").upper() == "DIVIDEND":
            previous_close = _find_previous_close(price_dates, close_by_date, ex_date)
        single_factor = _single_event_factor(action, previous_close)
        if single_factor is None:
            stats[f"SKIP_{(getattr(action, 'action_type', '') or 'UNKNOWN').upper()}"] += 1
            continue
        event_items.append(
            {
                "action": action,
                "date": ex_date,
                "factor_key": build_factor_key(action),
                "previous_close": previous_close,
                "single_event_factor": single_factor,
            }
        )

    event_items.sort(key=lambda item: (item["date"], item["factor_key"]))
    event_hash = _event_hash(
        [
            _event_payload(item["action"], item["previous_close"], item["single_event_factor"])
            for item in event_items
        ]
    )

    rows_by_key: dict[str, dict] = {}
    cumulative = Decimal("1")
    for event_date in sorted({item["date"] for item in event_items}, reverse=True):
        same_date_items = [item for item in event_items if item["date"] == event_date]
        date_factor = Decimal("1")
        for item in same_date_items:
            date_factor *= item["single_event_factor"]
        cumulative = _quantize_factor(cumulative * date_factor)
        for item in same_date_items:
            action = item["action"]
            rows_by_key[item["factor_key"]] = {
                "security_id": security_id,
                "date": item["date"],
                "methodology_version": methodology_version,
                "factor_type": "historical_adjustment",
                "factor_key": item["factor_key"],
                "source_event_id": getattr(action, "source_event_id", None),
                "action_type": getattr(action, "action_type", None),
                "single_event_factor": item["single_event_factor"],
                "cumulative_factor": cumulative,
                "previous_close": item["previous_close"],
                "event_hash": event_hash,
                "as_of_date": as_of_date,
            }
    return [rows_by_key[key] for key in sorted(rows_by_key)], stats


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
            Security.is_active == True,
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([item.lower() for item in args.symbols]))
        query = query.order_by(Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def _load_actions_and_prices(db_manager: DatabaseManager, security_id: int, source: str):
    with db_manager.get_session() as session:
        actions = (
            session.query(CorporateAction)
            .filter(CorporateAction.security_id == security_id)
            .filter(func.upper(CorporateAction.source) == source.upper())
            .filter(CorporateAction.action_type.in_(["DIVIDEND", "SPLIT"]))
            .order_by(CorporateAction.ex_date.asc(), CorporateAction.action_type.asc(), CorporateAction.source_event_id.asc())
            .all()
        )
        prices = (
            session.query(DailyPrice.date, DailyPrice.close)
            .filter(DailyPrice.security_id == security_id)
            .filter(DailyPrice.close.isnot(None))
            .order_by(DailyPrice.date.asc())
            .all()
        )
    close_by_date = {row.date: _to_decimal(row.close) for row in prices if _to_decimal(row.close) is not None}
    price_dates = sorted(close_by_date)
    return actions, price_dates, close_by_date


def compare_with_vendor(
    db_manager: DatabaseManager,
    security_id: int,
    methodology_version: str,
    source: str,
    tolerance: Decimal,
) -> dict:
    from sqlalchemy import text

    sql = text(
        """
        SELECT
            c.date,
            c.factor_key,
            c.action_type,
            c.cumulative_factor,
            v.adjustment_factor,
            ABS(c.cumulative_factor - v.adjustment_factor) AS abs_diff
        FROM computed_adjustment_factors c
        JOIN vendor_adjustment_factors v
          ON v.security_id = c.security_id
         AND v.factor_type = c.factor_type
         AND v.factor_key = c.factor_key
         AND upper(v.source) = upper(:source)
        WHERE c.security_id = :security_id
          AND c.methodology_version = :methodology_version
          AND c.factor_type = 'historical_adjustment'
        ORDER BY abs_diff DESC, c.date DESC
        """
    )
    with db_manager.engine.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "security_id": security_id,
                "methodology_version": methodology_version,
                "source": source,
            },
        ).mappings().all()

    if not rows:
        return {
            "matched": 0,
            "failed": 0,
            "max_abs_diff": None,
            "rows": [],
        }

    failed = [
        row
        for row in rows
        if _to_decimal(row["abs_diff"]) is not None and _to_decimal(row["abs_diff"]) > tolerance
    ]
    max_abs_diff = _to_decimal(rows[0]["abs_diff"])
    return {
        "matched": len(rows),
        "failed": len(failed),
        "max_abs_diff": max_abs_diff,
        "rows": rows[:10],
    }


def refresh_vendor_daily_bar_factors(
    source: MassiveSource,
    db_manager: DatabaseManager,
    security: Security,
    start_date: date,
    end_date: date,
) -> int:
    raw = source.get_historical_data(security.symbol, start=start_date.isoformat(), end=end_date.isoformat(), adjusted=False)
    adjusted = source.get_historical_data(security.symbol, start=start_date.isoformat(), end=end_date.isoformat(), adjusted=True)
    if raw.empty or adjusted.empty:
        return 0

    raw_close = raw[["Close"]].rename(columns={"Close": "raw_close"})
    adjusted_close = adjusted[["Close"]].rename(columns={"Close": "adjusted_close"})
    joined = raw_close.join(adjusted_close, how="inner")

    rows = []
    for row_date, row in joined.iterrows():
        raw_value = _to_decimal(row["raw_close"])
        adjusted_value = _to_decimal(row["adjusted_close"])
        if raw_value is None or adjusted_value is None or raw_value <= 0:
            continue
        rows.append(
            {
                "security_id": security.id,
                "date": row_date,
                "source": "MASSIVE",
                "factor_type": "daily_bar_adjusted_close",
                "factor_key": f"daily_bar_adjusted_close:{row_date.isoformat()}",
                "adjustment_factor": _quantize_factor(adjusted_value / raw_value),
                "raw_close": raw_value,
                "adjusted_close": adjusted_value,
                "as_of_date": end_date,
            }
        )
    return db_manager.upsert_vendor_adjustment_factors(rows)


def process_security(
    security: Security,
    db_manager: DatabaseManager,
    args: argparse.Namespace,
    as_of_date: date,
    tolerance: Decimal,
) -> tuple[str, str, int, dict, Counter]:
    actions, price_dates, close_by_date = _load_actions_and_prices(db_manager, security.id, args.source)
    if not actions:
        db_manager.replace_computed_adjustment_factors(security.id, args.methodology_version, [])
        return security.symbol, "SUCCESS_NO_ACTIONS", 0, {}, Counter()

    rows, stats = compute_adjustment_factor_rows(
        security.id,
        actions,
        price_dates,
        close_by_date,
        methodology_version=args.methodology_version,
        as_of_date=as_of_date,
    )
    inserted = db_manager.replace_computed_adjustment_factors(security.id, args.methodology_version, rows)
    comparison = compare_with_vendor(db_manager, security.id, args.methodology_version, args.source, tolerance)
    if comparison.get("matched", 0) and comparison.get("failed", 0) == 0:
        status = "SUCCESS_MATCHED_VENDOR"
    elif comparison.get("matched", 0):
        status = "SUCCESS_VENDOR_MISMATCH"
    else:
        status = "SUCCESS_NO_VENDOR_REFERENCE"
    return security.symbol, status, inserted, comparison, stats


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args()

    db_manager = None
    source = None
    try:
        enforce_us_market(args.market)
        tolerance = Decimal(str(args.tolerance))
        as_of_date = get_last_completed_trading_date(args.market)
        db_manager = DatabaseManager()
        securities = get_securities_to_update(db_manager, args)
        if not securities:
            logger.success("没有需要重建调整因子的证券。")
            return 0

        if args.refresh_vendor_daily_bars:
            api_keys = get_massive_api_keys()
            rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive-adjustment")
            source = MassiveSource(rate_limiter=rate_limiter)
            daily_end = date.fromisoformat(args.daily_end_date) if args.daily_end_date else as_of_date
            daily_start = date.fromisoformat(args.daily_start_date) if args.daily_start_date else get_massive_history_floor(daily_end)
            logger.info("将刷新 vendor daily adjusted/raw reference: {} -> {}", daily_start, daily_end)
        else:
            daily_start = None
            daily_end = None

        status_counter = Counter()
        total_rows = 0
        vendor_daily_rows = 0
        mismatch_examples = []
        skipped_counter = Counter()

        for security in tqdm(securities, desc="重建调整因子"):
            symbol, status, row_count, comparison, stats = process_security(
                security,
                db_manager,
                args,
                as_of_date,
                tolerance,
            )
            status_counter[status] += 1
            total_rows += row_count
            skipped_counter.update(stats)

            if status == "SUCCESS_VENDOR_MISMATCH":
                mismatch_examples.append((symbol, comparison))

            if source is not None and daily_start and daily_end:
                try:
                    vendor_daily_rows += refresh_vendor_daily_bar_factors(source, db_manager, security, daily_start, daily_end)
                except Exception as exc:
                    logger.opt(exception=exc).error("[{}] 刷新 vendor daily adjusted/raw reference 失败: {}", symbol, exc)

            if args.symbols and comparison.get("matched"):
                logger.info(
                    "[{}] matched={} failed={} max_abs_diff={}",
                    symbol,
                    comparison["matched"],
                    comparison["failed"],
                    comparison["max_abs_diff"],
                )
                for row in comparison["rows"][:5]:
                    logger.info(
                        "  {} {} vendor={} computed={} diff={}",
                        row["date"],
                        row["factor_key"],
                        row["adjustment_factor"],
                        row["cumulative_factor"],
                        row["abs_diff"],
                    )

        logger.info("--- 调整因子重建统计 ---")
        for key, value in status_counter.most_common():
            logger.info("  {}: {}", key, value)
        logger.info("  computed rows written: {}", total_rows)
        logger.info("  vendor daily rows written: {}", vendor_daily_rows)
        if skipped_counter:
            for key, value in skipped_counter.most_common():
                logger.info("  {}: {}", key, value)
        if mismatch_examples:
            logger.warning("发现 vendor mismatch 样例:")
            for symbol, comparison in mismatch_examples[:10]:
                logger.warning("[{}] max_abs_diff={} failed={}", symbol, comparison["max_abs_diff"], comparison["failed"])
                for row in comparison["rows"][:3]:
                    logger.warning(
                        "  {} {} vendor={} computed={} diff={}",
                        row["date"],
                        row["factor_key"],
                        row["adjustment_factor"],
                        row["cumulative_factor"],
                        row["abs_diff"],
                    )
        logger.info("--------------------------")

        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_adjustment_factors 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
