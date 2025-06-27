import os
import sys
import time
import argparse
from datetime import datetime, timedelta, timezone

from loguru import logger

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, MarketType
# 导入新的 PolygonSource
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
    logger.add(os.path.join(log_dir, "update_polygon_details_{time}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def create_parser() -> argparse.ArgumentParser:
    """创建并返回 ArgumentParser 对象。"""
    parser = argparse.ArgumentParser(
        description="使用 Polygon.io API 更新数据库中股票的详细信息。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    parser.add_argument('--all', action='store_true', help="更新所有活跃股票。")
    parser.add_argument('--market', type=str, help="仅更新指定市场的股票 (US, HK, CNA等)。")
    parser.add_argument('--force', action='store_true', help=f"强制更新，忽略 {UPDATE_INTERVAL_DAYS} 天检查。")
    return parser


def update_security_details(db_manager: DatabaseManager, polygon_source: PolygonSource, security: Security,
                            force: bool):
    """为单个股票获取数据并更新数据库"""
    symbol = security.symbol
    logger.info(f"--- 开始处理: {symbol} ---")

    # 1. 检查是否需要更新
    if not force and security.info_last_updated_at:
        last_update_aware = security.info_last_updated_at.astimezone(timezone.utc)
        if last_update_aware > (datetime.now(timezone.utc) - timedelta(days=UPDATE_INTERVAL_DAYS)):
            logger.info(f"[{symbol}] 的信息在 {UPDATE_INTERVAL_DAYS} 天内已更新，跳过。")
            return

    # 2. 从新的数据源获取信息
    update_data = polygon_source.get_security_info(symbol)

    if not update_data:
        logger.warning(f"[{symbol}] 无法从 PolygonSource 获取有效数据，跳过数据库更新。")
        # 考虑如果 API 404，是否将 is_active 设为 False
        if polygon_source.get_security_info(symbol) is None:  # 再次检查是否是 API 错误
            db_manager.upsert_security_info({'symbol': symbol, 'is_active': False})
        return

    # 3. 如果 API 返回的数据中不包含 market 或 type，保留数据库中的旧值
    if 'market' not in update_data or update_data['market'] is None:
        update_data['market'] = security.market
    if 'type' not in update_data or update_data['type'] is None:
        update_data['type'] = security.type

    # 4. 更新数据库
    try:
        db_manager.upsert_security_info(update_data)
        logger.success(f"成功更新了 [{symbol}] 的详细信息。")
    except Exception as e:
        logger.error(f"[{symbol}] 更新数据库时出错: {e}", exc_info=True)


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
        # 初始化数据库管理器和新的 PolygonSource
        db_manager = DatabaseManager()
        polygon_source = PolygonSource()  # 初始化一次，内部自动管理 Key 轮询

        # 获取待处理的股票列表 (这部分逻辑保持不变)
        securities_to_process = []
        with db_manager.get_session() as session:
            query = session.query(Security).filter(Security.is_active == True)
            if args.symbols:
                symbols_lower = [s.lower() for s in args.symbols]
                query = query.filter(Security.symbol.in_(symbols_lower))
            elif args.market:
                market_enum = MarketType[args.market.upper()]
                query = query.filter(Security.market == market_enum)

            query = query.order_by(Security.info_last_updated_at.asc().nulls_first())
            securities_to_process = query.all()

        if not securities_to_process:
            logger.success("根据条件，没有找到需要处理的股票。")
            return

        logger.info(f"共找到 {len(securities_to_process)} 支股票待处理。")

        # 循环处理
        total = len(securities_to_process)
        for i, security in enumerate(securities_to_process):
            logger.info(f"进度: {i + 1}/{total}")
            # 将 polygon_source 实例传入
            update_security_details(db_manager, polygon_source, security, force=args.force)
            # 注意：延迟已在 PolygonSource 内部处理，这里不再需要 time.sleep()

    except ValueError as e:
        # 捕获 PolygonSource 初始化时的环境变量错误
        logger.critical(f"初始化失败: {e}")
    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()
