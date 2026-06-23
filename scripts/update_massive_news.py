import argparse
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import iter_chunks
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)

NEWS_UPDATE_INTERVAL_DAYS = 1
NEWS_LOOKBACK_DAYS = 7
MAX_CONCURRENT_WORKERS = 4
API_BATCH_SIZE = 50


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive News 更新新闻与情绪 insight。",
        default_workers=MAX_CONCURRENT_WORKERS,
        all_help="处理全部活跃 CS/ETF。",
    )
    parser.add_argument("--force", action="store_true", help="强制更新，忽略时间检查。")
    parser.add_argument("--lookback-days", type=int, default=NEWS_LOOKBACK_DAYS, help="抓取最近 N 天新闻。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    return select_us_securities(
        db_manager,
        args,
        active_scope="unless_symbols",
        staleness_column="news_last_updated_at",
        staleness_days=NEWS_UPDATE_INTERVAL_DAYS,
        skip_staleness=args.force,
    )


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


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    published_after = (datetime.now(timezone.utc) - timedelta(days=max(args.lookback_days, 1))).isoformat()

    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要更新 news 的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    batches = iter_chunks(securities, API_BATCH_SIZE)
    outputs, results_counter = run_concurrently(
        batches,
        lambda batch: process_batch(batch, source, db_manager, published_after),
        max_workers=args.workers,
        desc="更新 Massive news",
    )
    total_articles = 0
    total_insights = 0
    for batch_counter, article_count, insight_count in outputs:
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
    errors = results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    stats = {"processed": len(securities), "written": total_articles + total_insights, "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_news", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
