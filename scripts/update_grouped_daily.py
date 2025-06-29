import os
import sys
import time
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

from loguru import logger
from tqdm import tqdm

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, DailyPrice
from data_sources.polygon_source import PolygonSource
from utils.key_rate_limiter import KeyRateLimiter

# --- 配置区 ---
MAX_CONCURRENT_WORKERS = 10
POLYGON_RATE_LIMIT = 5
POLYGON_RATE_SECONDS = 60


# ... (setup_logging, get_dates_to_process 函数保持不变) ...
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
    logger.add(os.path.join(log_dir, f"update_grouped_daily_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("日志记录器设置完成。")


def get_dates_to_process(start_str: str, end_str: str) -> list[date]:
    """根据起止日期字符串，生成一个日期对象列表。"""
    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError:
        logger.critical(f"日期格式错误。请使用 'YYYY-MM-DD' 格式。")
        return []
    if start > end:
        logger.warning(f"起始日期 {start_str} 在结束日期 {end_str} 之后，不执行任何操作。")
        return []
    dates = [start + timedelta(days=x) for x in range((end - start).days + 1)]
    return dates


def process_date(target_date: date, polygon_source: PolygonSource, db_manager: DatabaseManager,
                 symbol_to_id_map: dict) -> tuple[str, str, int]:
    """
    【新逻辑】处理单个日期的行情数据：查询 -> API获取 -> 内存中修改 -> 批量更新。
    """
    date_str = target_date.strftime('%Y-%m-%d')
    try:
        # 1. 查询：从数据库加载当天所有需要更新的 DailyPrice ORM 对象
        with db_manager.get_session() as session:
            # 将记录加载到一个字典中，以 security_id 为键，方便快速查找
            existing_records_map = {
                record.security_id: record
                for record in session.query(DailyPrice).filter(DailyPrice.date == target_date)
            }

        if not existing_records_map:
            logger.info(f"[{date_str}] 数据库中没有任何价格记录，跳过此日期。")
            return date_str, "SKIPPED_NO_EXISTING_DATA", 0

        # 2. API获取：从 Polygon 拉取数据
        daily_aggs = polygon_source.get_grouped_daily_data(date_str)
        if not daily_aggs:
            logger.info(f"[{date_str}] Polygon API 未返回任何行情数据。")
            return date_str, "SUCCESS_NO_API_DATA", 0

        # 3. 内存中修改：遍历API数据，更新从数据库查出的ORM对象
        records_to_update = []
        for agg in daily_aggs:
            symbol = agg.get('T', '').lower()
            security_id = symbol_to_id_map.get(symbol)

            # 检查这条API数据是否对应我们已有的记录
            if security_id in existing_records_map:
                # 获取待更新的 ORM 对象
                record_to_modify = existing_records_map[security_id]

                # 计算成交额
                volume = agg.get('v')
                vwap = agg.get('vw')
                turnover = (volume * vwap) if volume is not None and vwap is not None else None

                # 在内存中直接修改对象的属性
                record_to_modify.open = agg.get('o')
                record_to_modify.high = agg.get('h')
                record_to_modify.low = agg.get('l')
                record_to_modify.close = agg.get('c')
                record_to_modify.volume = volume
                record_to_modify.vwap = vwap
                record_to_modify.turnover = turnover
                # **关键**: turnover_rate 和 adj_factor 等其他字段保持原样，因为我们没有动它们

                records_to_update.append(record_to_modify)

        if not records_to_update:
            logger.info(f"[{date_str}] API数据与数据库现有记录无交集，无需更新。")
            return date_str, "SUCCESS_NO_INTERSECTION", 0

        # 4. 批量更新：调用新的DB方法，将修改后的对象列表一次性提交
        rows_affected = db_manager.bulk_update_records(records_to_update)

        logger.success(f"[{date_str}] 成功刷新 {rows_affected} 条日线数据。")
        return date_str, "SUCCESS", rows_affected

    except Exception as e:
        logger.error(f"处理日期 {date_str} 的行情数据时发生严重错误: {e}", exc_info=True)
        return date_str, "ERROR", 0


def main():
    """脚本主入口"""
    start_time = time.monotonic()
    setup_logging()

    # 硬编码日期范围以满足你的特定需求
    class Args:
        start_date = "2023-06-29"
        end_date = "2025-06-27"
        workers = MAX_CONCURRENT_WORKERS

    args = Args()
    logger.info(f"脚本将刷新从 {args.start_date} 到 {args.end_date} 的数据。")

    db_manager = None
    try:
        # --- 初始化共享资源 ---
        api_keys_str = os.getenv("POLYGON_API_KEYS")
        if not api_keys_str:
            raise ValueError("环境变量 POLYGON_API_KEYS 未设置。")
        api_keys = [key.strip() for key in api_keys_str.split(',') if key.strip()]

        rate_limiter = KeyRateLimiter(keys=api_keys, rate_limit=POLYGON_RATE_LIMIT, per_seconds=POLYGON_RATE_SECONDS)
        polygon_source = PolygonSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        logger.info("正在从数据库加载 'symbol -> security_id' 映射...")
        with db_manager.get_session() as session:
            securities = session.query(Security.id, Security.symbol).all()
            symbol_to_id_map = {s.symbol.lower(): s.id for s in securities}
        logger.success(f"成功加载 {len(symbol_to_id_map)} 个股票的ID映射。")

        dates_to_process = get_dates_to_process(args.start_date, args.end_date)
        if not dates_to_process: return

        total_count = len(dates_to_process)
        logger.info(f"共找到 {total_count} 个日期需要处理。将使用最多 {args.workers} 个并发线程。")

        results_counter = Counter()
        total_records_updated = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_date = {
                executor.submit(process_date, dt, polygon_source, db_manager, symbol_to_id_map): dt
                for dt in dates_to_process
            }
            for future in tqdm(as_completed(future_to_date), total=total_count, desc="刷新每日聚合行情"):
                try:
                    date_str, status, records_count = future.result()
                    results_counter[status] += 1
                    total_records_updated += records_count
                except Exception as exc:
                    dt = future_to_date[future]
                    logger.error(f"任务 {dt.strftime('%Y-%m-%d')} 生成了未捕获的异常: {exc}")
                    results_counter["FATAL_ERROR"] += 1

        logger.info("--- 任务执行统计 ---")
        for status, count in results_counter.items():
            logger.info(f"  {status}: {count} 天")
        logger.info(f"总共刷新记录数: {total_records_updated}")
        logger.info("----------------------")

    except ValueError as e:
        logger.critical(f"初始化失败: {e}")
    except Exception as e:
        logger.critical(f"脚本执行过程中遇到未处理的严重错误: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()
        end_time = time.monotonic()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=end_time - start_time)}")


if __name__ == "__main__":
    main()
