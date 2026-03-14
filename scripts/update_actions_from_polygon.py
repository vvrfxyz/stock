import argparse
import os
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import func, or_
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.adj_factor import recalc_adj_factor_for_security
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
    get_massive_history_floor,
    iter_chunks,
)
from utils.trading_calendar import get_last_completed_trading_date

ACTIONS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 8
MAX_ADJ_FACTOR_WORKERS = 8
ADJ_FACTOR_UPSERT_BATCH_SIZE = 1000
API_BATCH_SIZE = 100


def _infer_currency(security: Security) -> str | None:
    if security.currency:
        return security.currency.upper()
    return "USD"


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
        os.path.join(log_dir, f"update_massive_actions_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive API 批量更新公司行动（分红、拆股）。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部活跃保留类型证券。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--force", action="store_true", help="强制刷新 Massive 可覆盖的最近 2 年窗口。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="批次并发数。")
    recalc_group = parser.add_mutually_exclusive_group()
    recalc_group.add_argument("--recalc-adj-factor", action="store_true", help="actions 更新后重算 adj_factor。")
    recalc_group.add_argument("--skip-recalc-adj-factor", action="store_true", help="跳过 adj_factor 重算。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
            Security.is_active == True,
        )

        if args.symbols:
            query = query.filter(Security.symbol.in_([item.lower() for item in args.symbols]))

        if not args.force:
            update_before = datetime.now(timezone.utc) - timedelta(days=ACTIONS_UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.actions_last_updated_at.is_(None),
                    Security.actions_last_updated_at < update_before,
                )
            )

        query = query.order_by(Security.actions_last_updated_at.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def _get_batch_start_date(securities: list[Security], history_floor: datetime.date, force: bool) -> str:
    if force:
        return history_floor.isoformat()

    candidate_dates = []
    for security in securities:
        if security.actions_last_updated_at:
            candidate_dates.append((security.actions_last_updated_at - timedelta(days=7)).date())
    if not candidate_dates:
        return history_floor.isoformat()
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


def process_batch(
    securities: list[Security],
    source: MassiveSource,
    db_manager: DatabaseManager,
    history_floor,
    force: bool,
) -> tuple[Counter, list[Security]]:
    results_counter = Counter()
    changed: list[Security] = []
    batch_start = _get_batch_start_date(securities, history_floor, force)
    symbols = [security.symbol for security in securities]

    dividends = source.get_dividends_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    splits = source.get_splits_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    dividends_by_symbol = _group_by_ticker(dividends)
    splits_by_symbol = _group_by_ticker(splits)

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
            db_manager.update_security_timestamp(security.id, "actions_last_updated_at")

            if inserted_dividends + inserted_splits > 0:
                changed.append(security)
                results_counter["SUCCESS"] += 1
            elif security_dividends or security_splits:
                results_counter["SUCCESS_DUPLICATE_ONLY"] += 1
            else:
                results_counter["SUCCESS_NO_ACTIONS"] += 1
        except Exception as e:
            logger.error("[{}] Massive 公司行动落库失败: {}", symbol, e, exc_info=True)
            results_counter["ERROR"] += 1
    return results_counter, changed


def recalc_adj_factor_for_securities(
    securities: list[Security],
    db_manager: DatabaseManager,
    workers: int,
) -> Counter:
    if not securities:
        return Counter()

    recalc_workers = max(1, min(workers, MAX_ADJ_FACTOR_WORKERS))
    logger.info("开始为 {} 支股票重算 adj_factor，将使用最多 {} 个并发线程。", len(securities), recalc_workers)
    results_counter = Counter()

    def _recalc_task(sec: Security) -> tuple[str, str]:
        rows = recalc_adj_factor_for_security(
            db_manager=db_manager,
            security_id=sec.id,
            symbol=sec.symbol,
            batch_size=ADJ_FACTOR_UPSERT_BATCH_SIZE,
        )
        return sec.symbol, "SUCCESS" if rows > 0 else "SKIP_NO_PRICES"

    with ThreadPoolExecutor(max_workers=recalc_workers) as executor:
        future_to_security = {executor.submit(_recalc_task, sec): sec for sec in securities}
        for future in tqdm(as_completed(future_to_security), total=len(securities), desc="重算 adj_factor"):
            try:
                _symbol, status = future.result()
                results_counter[status] += 1
            except Exception as exc:
                security = future_to_security[future]
                logger.error("[{}] adj_factor 重算失败: {}", security.symbol, exc, exc_info=True)
                results_counter["ERROR"] += 1
    return results_counter


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    if not any([args.symbols, args.all, args.market]):
        parser.print_help()
        return

    db_manager = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS)
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        end_date = get_last_completed_trading_date(args.market)
        history_floor = get_massive_history_floor(end_date)
        securities = get_securities_to_update(db_manager, args)
        if not securities:
            logger.success("没有需要更新 Massive 公司行动的证券。")
            return

        results_counter = Counter()
        changed_securities: list[Security] = []
        batches = iter_chunks(securities, API_BATCH_SIZE)

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, batch, source, db_manager, history_floor, args.force): batch
                for batch in batches
            }
            for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc="更新 Massive 公司行动"):
                try:
                    batch_counter, batch_changed = future.result()
                    results_counter.update(batch_counter)
                    changed_securities.extend(batch_changed)
                except Exception as exc:
                    batch = future_to_batch[future]
                    logger.error("批次 {}-{} 发生未捕获异常: {}", batch[0].symbol, batch[-1].symbol, exc, exc_info=True)
                    results_counter["FATAL_ERROR"] += len(batch)

        logger.info("--- 公司行动统计 ---")
        logger.info("  成功(有新增): {}", results_counter["SUCCESS"])
        logger.info("  成功(仅重复): {}", results_counter["SUCCESS_DUPLICATE_ONLY"])
        logger.info("  成功(无 actions): {}", results_counter["SUCCESS_NO_ACTIONS"])
        logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
        logger.info("--------------------")

        should_recalc = not args.skip_recalc_adj_factor
        if should_recalc:
            unique_changed = {security.id: security for security in changed_securities}
            recalc_adj_factor_for_securities(list(unique_changed.values()), db_manager, args.workers)
    except Exception as e:
        logger.opt(exception=e).critical("update_actions 执行失败: {}", e)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
