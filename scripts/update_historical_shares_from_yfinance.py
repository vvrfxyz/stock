import os
import sys
import time
import argparse
import random
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from typing import Optional

import pandas as pd
from loguru import logger
from sqlalchemy import func
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security


MAX_CONCURRENT_WORKERS = 4
UPSERT_BATCH_SIZE = 2000


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
        os.path.join(log_dir, f"update_historical_shares_{{time}}.log"),
        rotation="10 MB",
        retention="10 days",
        level="DEBUG",
    )
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="使用 YFinance 回填 historical_shares（total_shares）。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('symbols', nargs='*', help="要处理的股票代码列表 (e.g., 'aapl', 'nvda')。")
    parser.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    parser.add_argument('--market', type=str, help="仅处理指定市场的股票 (例如: US, HK, CNA)。")
    parser.add_argument('--limit', type=int, default=0, help="限制处理的股票数量，用于测试。0 表示不限制。")
    parser.add_argument(
        '--workers',
        type=int,
        default=MAX_CONCURRENT_WORKERS,
        help=f"并发执行的线程数 (默认: {MAX_CONCURRENT_WORKERS})。",
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2010-01-01',
        help="拉取股本历史的起始日期 (YYYY-MM-DD)，默认 2010-01-01。",
    )
    return parser


def _iter_batches(rows: list[dict], batch_size: int):
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def get_securities_to_process(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    symbols = [s.lower() for s in args.symbols if s]
    with db_manager.get_session() as session:
        query = session.query(Security).filter(Security.is_active == True)

        if symbols:
            query = query.filter(Security.symbol.in_(symbols))
        elif args.market:
            query = query.filter(func.upper(Security.market) == args.market.upper())

        if args.limit > 0:
            query = query.limit(args.limit)

        return query.order_by(Security.symbol.asc()).all()


def _pick_series(df_or_series) -> Optional[pd.Series]:
    if df_or_series is None:
        return None
    if isinstance(df_or_series, pd.Series):
        return df_or_series
    if isinstance(df_or_series, pd.DataFrame):
        if df_or_series.empty:
            return df_or_series.iloc[:, 0] if df_or_series.shape[1] > 0 else None
        for col in ['Shares', 'shares', 'Share Count', 'shareCount', 'Total Shares', 'total_shares', 'totalShares']:
            if col in df_or_series.columns:
                return df_or_series[col]
        if df_or_series.shape[1] == 1:
            return df_or_series.iloc[:, 0]
        numeric_cols = [c for c in df_or_series.columns if pd.api.types.is_numeric_dtype(df_or_series[c])]
        if numeric_cols:
            return df_or_series[numeric_cols[0]]
        return df_or_series.iloc[:, 0]
    return None


def _series_to_records(series: pd.Series) -> list[tuple[object, int]]:
    if series is None or series.empty:
        return []

    series = series.dropna()
    if series.empty:
        return []

    try:
        series.index = pd.to_datetime(series.index, errors='coerce')
    except Exception:
        return []

    series = series[~series.index.isna()].sort_index()
    if series.empty:
        return []

    by_date = series.groupby(series.index.date).last()
    results: list[tuple[object, int]] = []
    for change_date, value in by_date.items():
        try:
            shares = int(value)
        except Exception:
            continue
        if shares <= 0:
            continue
        results.append((change_date, shares))
    return results


def process_security(security: Security, db_manager: DatabaseManager, start_date: str) -> tuple[str, str, int]:
    symbol = security.symbol
    try:
        try:
            import yfinance as yf
        except Exception as e:
            logger.error(f"[{symbol}] 导入 yfinance 失败，请先安装依赖: {e}")
            return symbol, "ERROR_MISSING_DEP", 0

        time.sleep(random.uniform(0.2, 0.6))
        ticker = yf.Ticker(symbol.upper())
        shares_obj = ticker.get_shares_full(start=start_date)

        series = _pick_series(shares_obj)
        records = _series_to_records(series)
        if not records:
            logger.warning(f"[{symbol}] 未获取到有效的 shares 历史数据。")
            return symbol, "SUCCESS_NO_DATA", 0

        share_rows = [
            {
                'security_id': security.id,
                'change_date': change_date,
                'total_shares': total_shares,
            }
            for change_date, total_shares in records
        ]

        total_upserted = 0
        for batch in _iter_batches(share_rows, UPSERT_BATCH_SIZE):
            total_upserted += db_manager.upsert_historical_shares(batch)

        logger.success(f"[{symbol}] 回填 historical_shares 完成，共处理 {len(share_rows)} 行。")
        return symbol, "SUCCESS", len(share_rows)

    except Exception as e:
        logger.error(f"[{symbol}] 回填 historical_shares 失败: {e}", exc_info=True)
        return symbol, "ERROR", 0


def main():
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    symbols = [s.lower() for s in args.symbols if s]
    if not any([symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供 symbols，或使用 --all / --market 标志。")
        parser.print_help()
        return

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities = get_securities_to_process(db_manager, args)
        if not securities:
            logger.success("✅ 根据您的条件，没有找到需要处理的股票。任务完成。")
            return

        total_count = len(securities)
        logger.info(f"共找到 {total_count} 支股票需要回填历史股本，将使用最多 {args.workers} 个并发线程。")

        results_counter = Counter()
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, sec, db_manager, args.start_date): sec
                for sec in securities
            }

            for future in tqdm(as_completed(future_to_security), total=total_count, desc="回填 historical_shares"):
                try:
                    _symbol, status, _count = future.result()
                    results_counter[status] += 1
                except Exception as exc:
                    sec = future_to_security[future]
                    logger.error(f"任务 {sec.symbol} 生成了未捕获的异常: {exc}", exc_info=True)
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info(f"  成功: {results_counter['SUCCESS']}")
        logger.info(f"  成功(无数据): {results_counter['SUCCESS_NO_DATA']}")
        logger.info(f"  错误(缺依赖): {results_counter['ERROR_MISSING_DEP']}")
        logger.info(f"  错误: {results_counter['ERROR'] + results_counter['FATAL_ERROR']}")
        logger.info("----------------------")

    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        end_time = time.monotonic()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=end_time - start_time)}")


if __name__ == "__main__":
    main()
