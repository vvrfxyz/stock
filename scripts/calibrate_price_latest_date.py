# scripts/calibrate_price_latest_date.py
import os
import sys
import time
import argparse
from datetime import timedelta

from loguru import logger
from sqlalchemy import func, update, select

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, DailyPrice


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
    logger.add(os.path.join(log_dir, f"calibrate_price_date_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """创建并返回 ArgumentParser 对象。"""
    parser = argparse.ArgumentParser(
        description="根据 daily_prices 表中的实际数据，校准 securities 表中的 price_data_latest_date 字段。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('--dry-run', action='store_true',
                        help="模拟执行并显示将要更新的记录，但不会实际修改数据库。")
    return parser


def calibrate_latest_price_dates(db_manager: DatabaseManager, dry_run: bool = False):
    """
    执行校准操作。
    """
    logger.info("开始校准 price_data_latest_date 字段...")

    try:
        with db_manager.get_session() as session:
            # 步骤 1: 创建一个子查询，用于计算每个 security_id 的最新日期
            # SQL: SELECT security_id, MAX(date) AS max_date FROM daily_prices GROUP BY security_id
            subquery = (
                select(
                    DailyPrice.security_id,
                    func.max(DailyPrice.date).label('max_date')
                )
                .group_by(DailyPrice.security_id)
                .subquery('latest_dates')  # 将其转换为一个命名的子查询
            )

            if dry_run:
                logger.info("--- [模拟运行] ---")
                logger.info("将查找需要更新的记录...")
                # 在模拟运行时，我们查询出需要更新的股票及其新旧日期
                # SQL: SELECT s.id, s.symbol, s.price_data_latest_date, ld.max_date
                #      FROM securities s JOIN latest_dates ld ON s.id = ld.security_id
                #      WHERE s.price_data_latest_date IS NULL OR s.price_data_latest_date != ld.max_date
                query_to_check = (
                    select(
                        Security.id,
                        Security.symbol,
                        Security.price_data_latest_date,
                        subquery.c.max_date
                    )
                    .join(subquery, Security.id == subquery.c.security_id)
                    .where(
                        (Security.price_data_latest_date != subquery.c.max_date) |
                        (Security.price_data_latest_date.is_(None))
                    )
                )

                results = session.execute(query_to_check).all()

                if not results:
                    logger.success("✅ 所有记录的 price_data_latest_date 均已是最新，无需校准。")
                    return

                logger.info(f"发现 {len(results)} 条记录需要校准：")
                for row in results:
                    logger.info(f"  - Symbol: {row.symbol:<10} (ID: {row.id}) | "
                                f"当前日期: {row.price_data_latest_date} -> "
                                f"目标日期: {row.max_date}")
                logger.info("--- [模拟运行结束] ---")

            else:
                # 步骤 2: 构建 UPDATE 语句
                # 使用 SQLAlchemy Core 的多表 UPDATE 语法 (PostgreSQL 支持)
                # SQL: UPDATE securities
                #      SET price_data_latest_date = ld.max_date
                #      FROM latest_dates ld
                #      WHERE securities.id = ld.security_id
                #        AND (securities.price_data_latest_date IS NULL OR securities.price_data_latest_date != ld.max_date);
                stmt = (
                    update(Security)
                    .values(price_data_latest_date=subquery.c.max_date)
                    .where(Security.id == subquery.c.security_id)
                    .where(
                        (Security.price_data_latest_date != subquery.c.max_date) |
                        (Security.price_data_latest_date.is_(None))
                    )
                )

                # 步骤 3: 执行更新并获取受影响的行数
                logger.info("正在执行批量更新操作...")
                result = session.execute(stmt)
                session.commit()

                rows_affected = result.rowcount
                logger.success(f"✅ 校准完成！成功更新了 {rows_affected} 条记录。")

    except Exception as e:
        logger.error(f"校准过程中发生错误: {e}", exc_info=True)


def main():
    """脚本主入口"""
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    db_manager = None
    try:
        db_manager = DatabaseManager()
        calibrate_latest_price_dates(db_manager, dry_run=args.dry_run)
    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        end_time = time.monotonic()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=end_time - start_time)}")


if __name__ == "__main__":
    main()
