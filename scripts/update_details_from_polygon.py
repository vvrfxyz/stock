# update_details_from_polygon.py (已优化)
import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import or_, func

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security
from data_sources.polygon_source import PolygonSource

# --- 配置区 ---
UPDATE_INTERVAL_DAYS = 30  # 更新周期（天）


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
    logger.add(os.path.join(log_dir, f"update_polygon_details_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """创建并返回 ArgumentParser 对象。"""
    parser = argparse.ArgumentParser(
        description="使用 Polygon.io API 更新数据库中股票的详细信息。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('symbols', nargs='*', help="要更新的股票代码列表。如果为空，则依赖其他标志。")
    parser.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    parser.add_argument('--market', type=str, help="仅处理指定市场的股票 (例如: US, HK, CNA)。")
    parser.add_argument('--force', action='store_true', help=f"强制更新，忽略 {UPDATE_INTERVAL_DAYS} 天的时间检查。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    """
    根据命令行参数，从数据库查询需要更新的证券列表。
    核心优化：将更新时间检查逻辑直接放入数据库查询。
    """
    with db_manager.get_session() as session:
        query = session.query(Security).filter(Security.is_active == True)

        # 1. 根据市场或股票代码列表进行筛选
        if args.symbols:
            symbols_lower = [s.lower() for s in args.symbols]
            query = query.filter(Security.symbol.in_(symbols_lower))
        elif args.market:
            # 修复: 直接使用字符串进行不区分大小写的比较
            query = query.filter(func.upper(Security.market) == args.market.upper())

        # 2. **核心优化**: 如果不是强制更新，则在查询中直接过滤掉不需要更新的记录
        if not args.force:
            thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=UPDATE_INTERVAL_DAYS)
            query = query.filter(
                or_(
                    Security.info_last_updated_at.is_(None),  # 从未更新过的
                    Security.info_last_updated_at < thirty_days_ago  # 更新时间超过30天的
                )
            )

        # 3. 排序，让最久未更新的优先处理
        query = query.order_by(Security.info_last_updated_at.asc().nulls_first())

        return query.all()


def main():
    """脚本主入口"""
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

    if not any([args.symbols, args.all, args.market]):
        logger.warning("没有指定任何操作。请提供股票代码，或使用 --all / --market 标志。")
        parser.print_help()
        return

    db_manager = None
    try:
        db_manager = DatabaseManager()
        polygon_source = PolygonSource()

        # 1. 获取待处理的股票列表 (已优化)
        securities_to_process = get_securities_to_update(db_manager, args)

        if not securities_to_process:
            logger.success("✅ 根据您的条件，没有找到需要更新的股票。任务完成。")
            return

        logger.info(f"共找到 {len(securities_to_process)} 支股票需要更新详细信息。")

        # 2. 循环处理
        total = len(securities_to_process)
        for i, security in enumerate(securities_to_process, 1):
            symbol = security.symbol
            logger.info(f"--- [进度: {i}/{total}] 开始处理: {symbol} (ID: {security.id}) ---")

            try:
                # 从 Polygon 获取最新数据
                update_data = polygon_source.get_security_info(symbol)
                if not update_data:
                    logger.warning(f"[{symbol}] 无法从 PolygonSource 获取有效数据，跳过数据库更新。")
                    continue

                # 关键点：将数据库中的 id 加入到待更新数据中，用于定位记录
                update_data['id'] = security.id

                # 更新数据库（使用智能的 upsert_security_info）
                db_manager.upsert_security_info(update_data)

            except Exception as e:
                logger.error(f"处理股票 {symbol} 时发生严重错误，已跳过。错误: {e}", exc_info=True)
                continue

    except ValueError as e:
        logger.critical(f"初始化失败: {e}")
    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("🏁 脚本执行完毕。")


if __name__ == "__main__":
    main()
