import argparse
import os
import sys
import time
from collections import Counter
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
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
    iter_chunks,
)
from utils.script_logging import setup_logging as configure_script_logging

NEWS_UPDATE_INTERVAL_DAYS = 1
NEWS_LOOKBACK_DAYS = 7
MAX_CONCURRENT_WORKERS = 4
API_BATCH_SIZE = 50


def setup_logging():
    configure_script_logging("update_massive_news")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 Massive News 更新新闻与情绪 insight。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbols", nargs="*", help="要更新的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理全部活跃 CS/ETF。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    parser.add_argument("--lookback-days", type=int, default=NEWS_LOOKBACK_DAYS, help="抓取最近 N 天新闻。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理证券数量。")
    parser.add_argument("--workers", type=int, default=MAX_CONCURRENT_WORKERS, help="批次并发数。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            func.upper(Security.market) == enforce_us_market(args.market),
            func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES),
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([symbol.lower() for symbol in args.symbols]))
        else:
            query = query.filter(Security.is_active == True)
        if not args.force:
            update_before = datetime.now(timezone.utc) - timedelta(days=NEWS_UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.news_last_updated_at.is_(None),
                    Security.news_last_updated_at < update_before,
                )
            )
        query = query.order_by(Security.news_last_updated_at.asc().nulls_first(), Security.symbol.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def process_batch(
    securities: list[Security],
    source: MassiveSource,
    db_manager: DatabaseManager,
    published_after: str,
) -> tuple[Counter, int, int]:
    symbols = [security.symbol for security in securities]
    symbol_to_id = {security.symbol: security.id for security in securities}
    rows = source.get_news(symbols, published_after=published_after)
    article_count, insight_count = db_manager.upsert_news_articles(rows, symbol_to_id=symbol_to_id) if rows else (0, 0)

    tickers_with_news = {
        ticker
        for row in rows
        for ticker in row.get("tickers", [])
    }
    counter = Counter()
    db_manager.update_security_timestamps([security.id for security in securities], "news_last_updated_at")
    for security in securities:
        if security.symbol in tickers_with_news:
            counter["SUCCESS"] += 1
        else:
            counter["SUCCESS_NO_DATA"] += 1
    return counter, article_count, insight_count


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    source = None
    try:
        published_after = (datetime.now(timezone.utc) - timedelta(days=max(args.lookback_days, 1))).isoformat()
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        securities = get_securities_to_update(db_manager, args)
        if not securities:
            logger.success("没有需要更新 news 的证券。")
            return 0

        batches = iter_chunks(securities, API_BATCH_SIZE)
        results_counter = Counter()
        total_articles = 0
        total_insights = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, batch, source, db_manager, published_after): batch
                for batch in batches
            }
            for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc="更新 Massive news"):
                try:
                    batch_counter, article_count, insight_count = future.result()
                except Exception as exc:
                    batch = future_to_batch[future]
                    logger.opt(exception=exc).error(
                        "批次 {}-{} 发生未捕获异常: {}", batch[0].symbol, batch[-1].symbol, exc
                    )
                    results_counter["FATAL_ERROR"] += len(batch)
                    continue
                results_counter.update(batch_counter)
                total_articles += article_count
                total_insights += insight_count

        logger.info("--- news 更新统计 ---")
        logger.info("  成功: {}", results_counter["SUCCESS"])
        logger.info("  无数据: {}", results_counter["SUCCESS_NO_DATA"])
        logger.info("  批次失败证券数: {}", results_counter["FATAL_ERROR"])
        logger.info("  news_articles 写入行数: {}", total_articles)
        logger.info("  news_article_insights 写入行数: {}", total_insights)
        logger.info("--------------------")
        return 1 if results_counter["FATAL_ERROR"] else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_massive_news 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
