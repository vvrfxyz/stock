import argparse
import os
import sys
import time
from datetime import timedelta

from loguru import logger
from sqlalchemy import func, update
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
)


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
        os.path.join(log_dir, f"sync_massive_universe_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="同步 Massive 活跃美股 universe，只保留普通股 / ETF / ADR。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--market", type=str, default="US", help="市场，当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理的 ticker 数量，用于测试。")
    parser.add_argument(
        "--skip-mark-missing-inactive",
        action="store_true",
        help="跳过将 Massive 活跃列表之外的保留类型证券标记为 inactive。",
    )
    return parser


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS)
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        reference_rows = source.list_active_tickers(allowed_types=ALLOWED_US_SECURITY_TYPES)
        if args.limit > 0:
            reference_rows = reference_rows[: args.limit]
        if not reference_rows:
            logger.warning("Massive 未返回任何可保留的活跃 US ticker。")
            return

        upsert_rows = [source._build_reference_payload(item) for item in tqdm(reference_rows, desc="整理 ticker 引用数据")]
        changed = db_manager.upsert_securities_by_symbol(upsert_rows, touch_info_timestamp=False)
        active_symbols = {row["symbol"] for row in upsert_rows}

        marked_inactive = 0
        should_mark_missing_inactive = not args.skip_mark_missing_inactive and args.limit == 0
        if not should_mark_missing_inactive and args.limit > 0 and not args.skip_mark_missing_inactive:
            logger.warning("检测到 --limit，已自动跳过 missing->inactive 标记，避免测试范围外数据被误伤。")
        if should_mark_missing_inactive:
            with db_manager.get_session() as session:
                stmt = (
                    update(Security)
                    .where(func.upper(Security.market) == "US")
                    .where(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
                    .where(Security.is_active == True)
                    .where(~Security.symbol.in_(active_symbols))
                    .values(is_active=False)
                )
                result = session.execute(stmt)
                session.commit()
                marked_inactive = result.rowcount or 0

        logger.success(
            "Massive universe 同步完成: fetched={} upserted={} marked_inactive={}",
            len(upsert_rows),
            changed,
            marked_inactive,
        )
    except Exception as e:
        logger.opt(exception=e).critical("sync_massive_universe 执行失败: {}", e)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    main()
