# scripts/update_em_us_daily_prices.py (正确版本)

import os
import sys
import time
import argparse
import random
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

import akshare
import pandas as pd
from loguru import logger
from sqlalchemy import or_, func
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security

# --- 配置区 ---
# Akshare 抓取网页，并发不宜过高，且需要随机延时以防封禁
MAX_CONCURRENT_WORKERS = 4
# 默认增量更新时，检查最近两天的数据是否已同步
INCREMENTAL_CHECK_DAYS = 2


def setup_logging():
    """配置 Loguru 日志记录器"""
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(log_dir, f"update_em_us_prices_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """创建并返回 ArgumentParser 对象。"""
    parser = argparse.ArgumentParser(
        description="使用 Akshare 从东方财富获取美股历史日线数据并存储到数据库。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    # 标识符改为 'em_codes'
    parser.add_argument('em_codes', nargs='*',
                        help="要更新的股票东方财富代码列表 (e.g., '105.NVDA')。如果为空，则依赖其他标志。")
    parser.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票 (默认: 'US')。")
    parser.add_argument('--full-refresh', action='store_true',
                        help="强制对选定范围内的所有股票进行全量刷新，忽略其现有数据。")
    parser.add_argument('--limit', type=int, default=0, help="限制处理的股票数量，用于测试。0表示不限制。")
    parser.add_argument('--workers', type=int, default=MAX_CONCURRENT_WORKERS,
                        help=f"并发执行的线程数 (默认: {MAX_CONCURRENT_WORKERS})。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    """根据命令行参数，从数据库查询需要更新日线数据的证券列表。"""
    with db_manager.get_session() as session:
        query = session.query(Security).filter(
            Security.is_active == True,
            Security.em_code.isnot(None),  # 必须有 em_code
            func.upper(Security.market) == args.market.upper()
        )

        if args.em_codes:
            query = query.filter(Security.em_code.in_(args.em_codes))

        if not args.full_refresh:
            # 增量模式：只选择那些数据不是最新的股票
            latest_required_date = date.today() - timedelta(days=INCREMENTAL_CHECK_DAYS)
            query = query.filter(
                or_(
                    Security.price_data_latest_date.is_(None),
                    Security.price_data_latest_date < latest_required_date
                )
            )

        # 优先处理没有数据的
        query = query.order_by(Security.price_data_latest_date.asc().nulls_first())

        if args.limit > 0:
            query = query.limit(args.limit)

        return query.all()


def process_security(security: Security, db_manager: DatabaseManager, full_refresh: bool) -> tuple[str, str, int]:
    """
    处理单个美股的日线数据：API获取 -> 数据清洗 -> DB存储 -> 更新时间戳。
    """
    em_code = security.em_code
    is_full_run = False

    try:
        # 1. 确定获取数据的起止日期
        end_date = datetime.now().strftime('%Y%m%d')
        if full_refresh or security.price_data_latest_date is None:
            start_date = '19700101'  # 从一个很早的日期开始，获取全部历史
            is_full_run = True
            logger.debug(f"[{em_code}] 全量更新，起始日期: {start_date}")
        else:
            start_date = (security.price_data_latest_date + timedelta(days=1)).strftime('%Y%m%d')
            logger.debug(f"[{em_code}] 增量更新，起始日期: {start_date}")

        if start_date > end_date:
            logger.info(f"[{em_code}] 数据已是最新，无需更新。")
            return em_code, "SUCCESS_UP_TO_DATE", 0

        # 2. 调用 Akshare API 获取数据（带随机延时）
        time.sleep(random.uniform(1.0, 2.0))
        df = akshare.stock_us_hist(symbol=em_code, period="daily", start_date=start_date, end_date=end_date, adjust="")

        if df.empty:
            logger.info(f"[{em_code}] 在时间范围 {start_date}-{end_date} 未获取到新数据。")
            # 即使没数据，如果是增量模式，也更新时间戳到昨天，避免频繁查询
            if not full_refresh:
                db_manager.update_security_price_latest_date(security.id, date.today() - timedelta(days=1), is_full_run)
            return em_code, "SUCCESS_NO_NEW_DATA", 0

        # 3. 数据清洗和格式化
        df.rename(columns={
            '日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
            '成交量': 'volume', '成交额': 'turnover', '换手率': 'turnover_rate'
        }, inplace=True)

        df['date'] = pd.to_datetime(df['date']).dt.date
        # **重要**: 将换手率从百分比转为小数
        df['turnover_rate'] = pd.to_numeric(df['turnover_rate'], errors='coerce') / 100.0

        # 4. 准备入库数据
        df['security_id'] = security.id
        df['vwap'] = pd.Series(None, index=df.index, dtype='float64')

        required_cols = ['security_id', 'date', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'turnover_rate',
                         'vwap']
        price_data = df[required_cols].to_dict('records')

        # 5. 存储到数据库
        rows_affected = db_manager.upsert_daily_prices(price_data)

        # 6. 更新 Security 表的时间戳
        latest_date_in_df = df['date'].max()
        db_manager.update_security_price_latest_date(security.id, latest_date_in_df, is_full_run)

        logger.success(f"[{em_code}] 成功同步 {len(price_data)} 条日线数据，最新日期: {latest_date_in_df}。")
        return em_code, "SUCCESS", len(price_data)

    except Exception as e:
        logger.error(f"处理股票 {em_code} 日线数据时发生严重错误: {e}", exc_info=True)
        return em_code, "ERROR", 0


def main():
    """脚本主入口"""
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities_to_process = get_securities_to_update(db_manager, args)

        if not securities_to_process:
            logger.success("✅ 根据您的条件，没有找到需要更新日线数据的股票。任务完成。")
            return

        total_count = len(securities_to_process)
        logger.info(f"共找到 {total_count} 支美股需要从东方财富更新日线数据。将使用最多 {args.workers} 个并发线程。")
        if args.full_refresh:
            logger.warning("⚠️ 已启用 --full-refresh 模式，将对所有选定股票进行全量数据刷新！")

        results_counter = Counter()
        total_rows_synced = 0

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_security = {
                executor.submit(process_security, security, db_manager, args.full_refresh): security
                for security in securities_to_process
            }

            for future in tqdm(as_completed(future_to_security), total=total_count, desc="更新美股日线(东方财富)"):
                try:
                    em_code, status, rows_count = future.result()
                    results_counter[status] += 1
                    total_rows_synced += rows_count
                except Exception as exc:
                    security = future_to_security[future]
                    logger.error(f"任务 {security.em_code} 生成了未捕获的异常: {exc}")
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        logger.info(f"  成功 (有新数据): {results_counter['SUCCESS']}")
        logger.info(f"  成功 (无新数据): {results_counter['SUCCESS_NO_NEW_DATA']}")
        logger.info(f"  成功 (已是最新): {results_counter['SUCCESS_UP_TO_DATE']}")
        logger.info(f"  错误: {results_counter['ERROR'] + results_counter['FATAL_ERROR']}")
        logger.info(f"  总共同步数据行数: {total_rows_synced}")
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
