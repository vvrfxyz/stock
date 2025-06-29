# main.py
import os
import sys
import argparse
import time
from datetime import datetime, timedelta

from loguru import logger

# --- 路径设置 (确保所有子模块都能被正确导入) ---
# 将项目根目录添加到 Python 路径中
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- 导入各个功能模块的主函数 ---
# 我们将通过编程方式调用这些脚本的 main 函数
# 为避免命名冲突，使用 'as' 重命名
from scripts.update_details_from_polygon import main as update_details_main
from scripts.update_actions_from_polygon import main as update_actions_main
from scripts.update_em_daily_prices import main as update_em_prices_main
from scripts.update_grouped_daily import main as update_grouped_daily_main
from scripts.migrate_database import main as migrate_main


def setup_logging():
    """配置全局 Loguru 日志记录器"""
    logger.remove()
    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(sys.stderr, level="INFO", format=log_format)
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    logger.add(os.path.join(log_dir, f"main_controller_{{time}}.log"), rotation="10 MB", retention="10 days",
               level="DEBUG")
    logger.info("主控制器日志记录器设置完成。")


def execute_script(main_func, args_list):
    """
    一个辅助函数，用于安全地调用其他脚本的 main 函数。
    它通过临时修改 sys.argv 来模拟命令行调用，确保原始脚本无需任何改动即可被集成。
    """
    original_argv = sys.argv
    script_name = main_func.__module__ + ".py"
    try:
        # 模拟命令行参数，第一个元素通常是脚本名
        sys.argv = [script_name] + args_list
        logger.debug(f"正在执行: {script_name} with args: {args_list}")
        main_func()
    except SystemExit as e:
        # argparse 的 --help 会触发 SystemExit，这是正常行为
        if e.code != 0:
            logger.error(f"脚本 {script_name} 异常退出，退出码: {e.code}")
    finally:
        # 恢复原始的 sys.argv，避免影响后续操作
        sys.argv = original_argv


# ==============================================================================
#  命令处理函数
# ==============================================================================

def run_daily_update(args):
    """
    执行标准的每日更新流程。
    顺序: 详情 -> 公司行动 -> 东方财富增量价格 -> Polygon昨日价格校准
    """
    start_time = time.monotonic()
    logger.info("🚀 ======== 开始执行标准每日更新流程 ======== 🚀")

    market_arg = ['--market', args.market] if args.market else []

    # 步骤 1: 更新股票详情 (增量模式)
    if not args.skip_details:
        logger.info("\n--- [步骤 1/4] 更新股票基本信息 (来自 Polygon) ---")
        execute_script(update_details_main, market_arg)
    else:
        logger.warning("--- [步骤 1/4] 已跳过更新股票基本信息 ---")

    # 步骤 2: 更新公司行动 (分红、拆股)
    if not args.skip_actions:
        logger.info("\n--- [步骤 2/4] 更新公司行动数据 (来自 Polygon) ---")
        execute_script(update_actions_main, market_arg)
    else:
        logger.warning("--- [步骤 2/4] 已跳过更新公司行动数据 ---")

    # 步骤 3: 从东方财富更新价格数据 (增量模式)
    if not args.skip_em_prices:
        logger.info("\n--- [步骤 3/4] 增量更新日线价格 (来自 东方财富) ---")
        execute_script(update_em_prices_main, market_arg)
    else:
        logger.warning("--- [步骤 3/4] 已跳过东方财富价格更新 ---")

    # 步骤 4: 使用 Polygon Grouped Daily API 刷新昨日数据，确保数据完整性
    if not args.skip_polygon_prices:
        logger.info("\n--- [步骤 4/4] 精准刷新昨日日线价格 (来自 Polygon) ---")
        # 自动获取昨天和前天的日期
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        day_before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        # 通常只需要刷新昨天的，但为防止周末/假日，可以多刷新一天
        execute_script(update_grouped_daily_main, ['--start-date', day_before, '--end-date', yesterday])
    else:
        logger.warning("--- [步骤 4/4] 已跳过 Polygon Grouped Daily 价格刷新 ---")

    end_time = time.monotonic()
    logger.success(f"✅ ======== 标准每日更新流程全部完成，总耗时: {timedelta(seconds=end_time - start_time)} ======== ✅")


def run_update_details(args):
    logger.info("执行: 更新股票基本信息")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_details_main, cli_args)


def run_update_actions(args):
    logger.info("执行: 更新公司行动数据")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_actions_main, cli_args)


def run_update_em_prices(args):
    logger.info("执行: 更新东方财富日线价格")
    cli_args = []
    if args.full_refresh: cli_args.append('--full-refresh')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.em_codes)
    execute_script(update_em_prices_main, cli_args)


def run_update_grouped_daily(args):
    logger.info("执行: 刷新 Polygon Grouped Daily 数据")
    cli_args = ['--start-date', args.start_date, '--end-date', args.end_date]
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    execute_script(update_grouped_daily_main, cli_args)


def run_migrate(args):
    logger.info("执行: 数据库迁移")
    execute_script(migrate_main, [])


# ==============================================================================
#  主函数：命令行解析器
# ==============================================================================

def main():
    """主程序入口，负责解析命令行参数并分发任务"""
    setup_logging()

    parser = argparse.ArgumentParser(
        description="美股数据系统中央控制器",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(title="可用命令", dest="command", required=True)

    # --- 定义 'daily_run' 命令 ---
    p_daily = subparsers.add_parser('daily_run', help="执行标准的每日数据更新流程",
                                    description="按顺序执行一系列增量更新任务，是日常运行的首选命令。")
    p_daily.add_argument('--market', type=str, default='US', help="指定要更新的市场 (默认: US)")
    p_daily.add_argument('--skip-details', action='store_true', help="跳过更新股票详情")
    p_daily.add_argument('--skip-actions', action='store_true', help="跳过更新公司行动")
    p_daily.add_argument('--skip-em-prices', action='store_true', help="跳过东方财富价格更新")
    p_daily.add_argument('--skip-polygon-prices', action='store_true', help="跳过Polygon Grouped Daily价格刷新")
    p_daily.set_defaults(func=run_daily_update)

    # --- 定义 'update_details' 命令 ---
    p_details = subparsers.add_parser('update_details', help="单独更新股票的详细信息 (来自Polygon)")
    p_details.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_details.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_details.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_details.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_details.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_details.add_argument('--workers', type=int, help="并发线程数。")
    p_details.set_defaults(func=run_update_details)

    # --- 定义 'update_actions' 命令 ---
    p_actions = subparsers.add_parser('update_actions', help="单独更新股票的公司行动 (分红、拆股)")
    p_actions.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_actions.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_actions.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_actions.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_actions.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_actions.add_argument('--workers', type=int, help="并发线程数。")
    p_actions.set_defaults(func=run_update_actions)

    # --- 定义 'update_em_prices' 命令 ---
    p_em_prices = subparsers.add_parser('update_em_prices', help="单独更新东方财富的日线价格")
    p_em_prices.add_argument('em_codes', nargs='*', help="要更新的股票东方财富代码列表。")
    p_em_prices.add_argument('--full-refresh', action='store_true', help="强制全量刷新。")
    p_em_prices.add_argument('--market', type=str, default='US', help="指定市场 (默认: US)。")
    p_em_prices.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_em_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_em_prices.set_defaults(func=run_update_em_prices)

    # --- 定义 'update_polygon_prices' 命令 ---
    p_poly_prices = subparsers.add_parser('update_polygon_prices', help="使用Grouped Daily API回填/刷新指定日期的价格")
    p_poly_prices.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    p_poly_prices.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    p_poly_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_poly_prices.set_defaults(func=run_update_grouped_daily)

    # --- 定义 'migrate' 命令 ---
    p_migrate = subparsers.add_parser('migrate', help="执行数据库迁移（一次性操作）")
    p_migrate.set_defaults(func=run_migrate)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
