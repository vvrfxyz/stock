import sys
import os
import argparse
import time
from datetime import datetime, timedelta, timezone
from loguru import logger

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_sources.yfinance_source import YFinanceSource
from data_models.models import Security, CorporateAction, ActionType, MarketType


def setup_logging():
    """配置 Loguru 日志记录器"""
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)

    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(log_dir, "corporate_actions_{time}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def sync_actions_for_symbol(db_manager: DatabaseManager, data_source: YFinanceSource, symbol: str,
                            force_resync: bool = False):
    """
    为单个股票代码从yfinance获取分红和拆股数据，并同步到数据库。
    成功处理后（无论有无事件），更新其在 securities 表中的状态时间戳。
    """
    logger.info(f"--- 开始处理: {symbol} ---")

    # 1. 从数据库获取 security 对象
    security = db_manager.get_security_by_symbol(symbol)
    if not security:
        logger.warning(f"数据库中未找到代码 '{symbol}' 的记录，跳过。")
        return

    # 2. 修改后的检查逻辑：基于时间戳
    # 如果不强制同步，并且在过去30天内已经更新过，则跳过
    # 您可以调整这个时间窗口，例如 timedelta(days=90)
    if not force_resync and security.actions_last_updated_at:
        # 确保 actions_last_updated_at 是时区感知的，以便与 now() 比较
        if security.actions_last_updated_at.tzinfo is None:
            last_update_aware = security.actions_last_updated_at.replace(tzinfo=timezone.utc)
        else:
            last_update_aware = security.actions_last_updated_at

        if last_update_aware > (datetime.now(timezone.utc) - timedelta(days=30)):
            logger.info(
                f"代码 '{symbol}' 的公司行动数据在30天内已更新过 ({last_update_aware.strftime('%Y-%m-%d')})。跳过。")
            logger.trace(f"若要强制更新，请使用 --force-resync 标志。")
            return

    logger.info(f"代码 '{symbol}' (ID: {security.id}) 需要更新公司行动数据，开始查询 yfinance...")

    # 3. 从 yfinance 获取完整的历史数据
    try:
        df = data_source.get_historical_data(symbol, period="max", auto_adjust=False)
        # BUGFIX: 即使df为空，也应视为“成功处理”，并更新时间戳
    except Exception as e:
        logger.error(f"为 '{symbol}' 获取 yfinance 数据时发生错误: {e}")
        # 获取失败，不更新时间戳，直接返回
        return

    # 如果数据源返回空DataFrame，说明没有历史数据（可能是新股或无效代码）
    if df.empty:
        logger.warning(f"数据源未能提供 '{symbol}' 的历史数据。这将被视为一次成功的检查。")
        # 直接跳到最后更新时间戳
    else:
        # 4. 筛选出包含分红或拆股事件的行
        actions_df = df[(df['Dividends'] > 0) | (df['Stock Splits'] > 0)].copy()

        if actions_df.empty:
            logger.info(f"代码 '{symbol}' 在其历史数据中未找到任何分红或拆股事件。")
        else:
            # 5. 准备要插入数据库的数据列表
            actions_to_insert = []
            for event_date, row in actions_df.iterrows():
                if row['Dividends'] > 0:
                    actions_to_insert.append({
                        'security_id': security.id,
                        'event_date': event_date.date(),
                        'event_type': ActionType.DIVIDEND,
                        'value': row['Dividends']
                    })

                if row['Stock Splits'] > 0:
                    actions_to_insert.append({
                        'security_id': security.id,
                        'event_date': event_date.date(),
                        'event_type': ActionType.SPLIT,
                        'value': row['Stock Splits']
                    })

            # 6. 使用 bulk_upsert 批量写入数据库
            if actions_to_insert:
                logger.success(
                    f"为 '{symbol}' (ID: {security.id}) 找到 {len(actions_to_insert)} 个公司行动事件，正在写入数据库...")
                try:
                    db_manager.bulk_upsert(
                        model_class=CorporateAction,
                        data=actions_to_insert,
                        index_elements=['security_id', 'event_date', 'event_type'],
                        constraint='_security_date_type_uc'
                    )
                except Exception as e:
                    logger.error(f"为 '{symbol}' 写入公司行动数据时出错: {e}", exc_info=True)
                    # 写入失败，不更新时间戳，直接返回
                    return

    # 7. 无论是否有事件，只要API调用成功，就更新时间戳
    try:
        db_manager.update_security_actions_timestamp(security.id)
    except Exception as e:
        logger.error(f"为 '{symbol}' (ID: {security.id}) 更新状态时间戳时失败: {e}", exc_info=True)


def main():
    """脚本主入口"""
    setup_logging()

    parser = argparse.ArgumentParser(
        description="从 yfinance 获取并更新公司行动（分红/拆股）数据。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        'symbols',
        nargs='*',
        help="要更新的股票代码列表 (例如: aapl msft 0700.hk)。如果留空，需使用 --all 标志。"
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help="更新数据库中所有标记为活跃的股票（目前限定为美股和港股）。"
    )
    parser.add_argument(
        '--force-resync',
        action='store_true',
        help="强制重新同步所有目标股票的公司行动，忽略时间戳检查。"
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=0.5,
        help="每次API请求之间的延迟秒数，以避免被限制。默认为 0.5 秒。"
    )
    args = parser.parse_args()

    if args.force_resync:
        logger.warning("检测到 --force-resync 标志，将强制为所有目标股票查询API并重新同步事件。")

    # --- 确定要处理的股票列表 ---
    symbols_to_process = []
    if args.symbols:
        symbols_to_process = [s.lower() for s in args.symbols]
        logger.info(f"指定模式：将处理 {len(symbols_to_process)} 个股票: {', '.join(symbols_to_process)}")
    elif args.all:
        logger.info("全量模式：正在从数据库获取所有活跃的美股和港股...")
        try:
            with DatabaseManager().get_session() as session:
                # NEW: 修改查询，优先处理从未更新过的股票
                securities = session.query(Security.symbol).filter(
                    Security.is_active == True,
                    Security.market.in_([MarketType.US, MarketType.HK])
                ).order_by(
                    Security.actions_last_updated_at.asc().nulls_first()
                ).all()
                symbols_to_process = [s[0] for s in securities]
            logger.info(f"将处理数据库中的 {len(symbols_to_process)} 个股票 (优先处理未更新过的)。")
        except Exception as e:
            logger.critical(f"从数据库获取股票列表时失败: {e}", exc_info=True)
            return
    else:
        logger.warning("没有指定任何操作。请提供股票代码或使用 --all 标志。")
        parser.print_help()
        return

    # --- 初始化组件并开始处理 ---
    db_manager = None
    try:
        db_manager = DatabaseManager()
        data_source = YFinanceSource()

        total = len(symbols_to_process)
        for i, symbol in enumerate(symbols_to_process):
            logger.info(f"进度: {i + 1}/{total}")
            sync_actions_for_symbol(db_manager, data_source, symbol, force_resync=args.force_resync)

            if i < total - 1:
                time.sleep(args.delay)

    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        logger.info("脚本执行完毕。")


if __name__ == "__main__":
    main()
