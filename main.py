# main.py
import os
import sys
import argparse
import time
from datetime import timedelta

from loguru import logger

# --- 路径设置 (确保所有子模块都能被正确导入) ---
# 将项目根目录添加到 Python 路径中
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 路径设置完成后再导入项目模块
# --- 导入各个功能模块的主函数 ---
# 我们将通过编程方式调用这些脚本的 main 函数
# 为避免命名冲突，使用 'as' 重命名
from scripts.update_details_from_polygon import main as update_details_main
from scripts.update_actions_from_polygon import main as update_actions_main
from scripts.update_em_daily_prices import main as update_em_prices_main
from scripts.update_grouped_daily import main as update_grouped_daily_main
from scripts.update_polygon_daily_prices import main as update_polygon_prices_main
from scripts.sync_massive_universe import main as sync_massive_universe_main
from scripts.update_massive_shares import main as update_massive_shares_main
from scripts.cleanup_us_universe import main as cleanup_us_universe_main
from scripts.recalc_adj_factor import main as recalc_adj_factor_main
from scripts.backfill_actions_from_yfinance import main as backfill_actions_main
from scripts.backfill_turnover_rate import main as backfill_turnover_rate_main
from scripts.migrate_database import main as migrate_main
from utils.trading_calendar import get_last_completed_trading_date, shift_trading_date


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
    顺序: universe -> 详情 -> 公司行动 -> Massive 个股增量 -> Massive grouped daily -> shares -> turnover
    """
    start_time = time.monotonic()
    logger.info("🚀 ======== 开始执行标准每日更新流程 ======== 🚀")

    if (args.market or "US").upper() != "US":
        logger.critical("Massive 重构后的 daily_run 当前仅支持 US 市场。")
        return

    market_arg = ['--market', args.market] if args.market else []

    # 步骤 1: 同步 Massive universe
    if not getattr(args, "skip_universe", False):
        logger.info("\n--- [步骤 1/7] 同步 Massive Universe ---")
        execute_script(sync_massive_universe_main, market_arg)
    else:
        logger.warning("--- [步骤 1/7] 已跳过 Massive Universe 同步 ---")

    # 步骤 2: 更新股票详情 (增量模式)
    if not args.skip_details:
        logger.info("\n--- [步骤 2/7] 更新股票基本信息 (来自 Massive) ---")
        execute_script(update_details_main, market_arg)
    else:
        logger.warning("--- [步骤 2/7] 已跳过更新股票基本信息 ---")

    # 步骤 3: 更新公司行动 (分红、拆股)
    if not args.skip_actions:
        logger.info("\n--- [步骤 3/7] 更新公司行动数据 (来自 Massive) ---")
        execute_script(update_actions_main, market_arg)
    else:
        logger.warning("--- [步骤 3/7] 已跳过更新公司行动数据 ---")

    if args.skip_em_prices:
        logger.warning("--- `--skip-em-prices` 已废弃；daily_run 默认不再使用东方财富。---")

    # 步骤 4: Massive 个股增量价格
    if not getattr(args, "skip_prices", False):
        logger.info("\n--- [步骤 4/7] 增量更新 Massive 个股日线 ---")
        execute_script(update_polygon_prices_main, market_arg)
    else:
        logger.warning("--- [步骤 4/7] 已跳过 Massive 个股日线更新 ---")

    # 步骤 5: Massive Grouped Daily 回刷最近 5 个已收盘交易日
    if not args.skip_polygon_prices and not getattr(args, "skip_grouped_daily", False):
        logger.info("\n--- [步骤 5/7] Massive Grouped Daily 回刷最近 5 个交易日 ---")
        end_trading_date = get_last_completed_trading_date("US")
        start_trading_date = shift_trading_date("US", end_trading_date, sessions=-4)
        execute_script(
            update_grouped_daily_main,
            [
                "--market",
                "US",
                "--start-date",
                start_trading_date.strftime("%Y-%m-%d"),
                "--end-date",
                end_trading_date.strftime("%Y-%m-%d"),
            ],
        )
    else:
        logger.warning("--- [步骤 5/7] 已跳过 Massive Grouped Daily 回刷 ---")

    # 步骤 6: 增量更新 shares
    if not getattr(args, "skip_shares", False):
        logger.info("\n--- [步骤 6/7] 增量更新 Massive Shares ---")
        execute_script(update_massive_shares_main, market_arg)
    else:
        logger.warning("--- [步骤 6/7] 已跳过 Massive Shares 更新 ---")

    # 步骤 7: 重建最近 5 个交易日 turnover_rate
    if not getattr(args, "skip_turnover", False):
        logger.info("\n--- [步骤 7/7] 重建最近 5 个交易日 turnover_rate ---")
        end_trading_date = get_last_completed_trading_date("US")
        start_trading_date = shift_trading_date("US", end_trading_date, sessions=-4)
        execute_script(
            backfill_turnover_rate_main,
            [
                "--market",
                "US",
                "--start-date",
                start_trading_date.strftime("%Y-%m-%d"),
                "--end-date",
                end_trading_date.strftime("%Y-%m-%d"),
                "--overwrite",
            ],
        )
    else:
        logger.warning("--- [步骤 7/7] 已跳过 turnover_rate 重建 ---")

    end_time = time.monotonic()
    logger.success(f"✅ ======== 标准每日更新流程全部完成，总耗时: {timedelta(seconds=end_time - start_time)} ======== ✅")


def run_update_details(args):
    logger.info("执行: 更新 Massive 股票基本信息")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_details_main, cli_args)


def run_update_actions(args):
    logger.info("执行: 更新 Massive 公司行动数据")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    if getattr(args, 'recalc_adj_factor', False): cli_args.append('--recalc-adj-factor')
    if getattr(args, 'skip_recalc_adj_factor', False): cli_args.append('--skip-recalc-adj-factor')
    cli_args.extend(args.symbols)
    execute_script(update_actions_main, cli_args)

def run_backfill_actions(args):
    logger.info("执行: 使用 YFinance 补全公司行动 (分红/拆股)")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    if args.start_date: cli_args.extend(['--start-date', args.start_date])
    if args.end_date: cli_args.extend(['--end-date', args.end_date])
    if getattr(args, 'recalc_adj_factor', False): cli_args.append('--recalc-adj-factor')
    if getattr(args, 'skip_recalc_adj_factor', False): cli_args.append('--skip-recalc-adj-factor')
    cli_args.extend(args.symbols)
    execute_script(backfill_actions_main, cli_args)


def run_update_em_prices(args):
    logger.info("执行: 更新东方财富日线价格")
    cli_args = []
    if args.full_refresh: cli_args.append('--full-refresh')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.em_codes)
    execute_script(update_em_prices_main, cli_args)

def run_update_polygon_prices(args):
    logger.info("执行: 更新 Massive 日线价格")
    cli_args = []
    if args.full_refresh: cli_args.append('--full-refresh')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_polygon_prices_main, cli_args)

def run_recalc_adj_factor(args):
    logger.info("执行: 重新计算复权因子 (adj_factor)")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(recalc_adj_factor_main, cli_args)


def run_update_historical_shares(args):
    logger.info("执行: 更新 Massive 历史股本 (historical_shares)")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.market: cli_args.extend(['--market', args.market])
    if getattr(args, 'full_refresh', False): cli_args.append('--full-refresh')
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    if args.start_date: cli_args.extend(['--start-date', args.start_date])
    cli_args.extend(args.symbols)
    execute_script(update_massive_shares_main, cli_args)


def run_backfill_turnover_rate(args):
    logger.info("执行: 回填换手率 (turnover_rate)")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    if args.start_date: cli_args.extend(['--start-date', args.start_date])
    if args.end_date: cli_args.extend(['--end-date', args.end_date])
    if args.overwrite: cli_args.append('--overwrite')
    else: cli_args.append('--only-null')
    cli_args.extend(args.symbols)
    execute_script(backfill_turnover_rate_main, cli_args)


def run_sync_massive_universe(args):
    logger.info("执行: 同步 Massive Universe")
    cli_args = []
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if getattr(args, 'skip_mark_missing_inactive', False):
        cli_args.append('--skip-mark-missing-inactive')
    execute_script(sync_massive_universe_main, cli_args)


def run_rebuild_turnover_rate(args):
    logger.info("执行: 全量重建换手率 (turnover_rate)")
    cli_args = ['--clear-first', '--overwrite']
    if args.all:
        cli_args.append('--all')
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.workers:
        cli_args.extend(['--workers', str(args.workers)])
    if args.start_date:
        cli_args.extend(['--start-date', args.start_date])
    if args.end_date:
        cli_args.extend(['--end-date', args.end_date])
    cli_args.extend(args.symbols)
    execute_script(backfill_turnover_rate_main, cli_args)


def run_cleanup_us_universe(args):
    logger.info("执行: 清理 US Universe 中非保留类型证券")
    cli_args = []
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.sample_size:
        cli_args.extend(['--sample-size', str(args.sample_size)])
    if args.apply:
        cli_args.append('--apply')
    execute_script(cleanup_us_universe_main, cli_args)


def run_update_grouped_daily(args):
    logger.info("执行: 刷新 Massive Grouped Daily 数据")
    cli_args = ['--start-date', args.start_date, '--end-date', args.end_date]
    if getattr(args, 'market', None): cli_args.extend(['--market', args.market])
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
    p_daily.add_argument('--skip-universe', action='store_true', help="跳过 Massive universe 同步")
    p_daily.add_argument('--skip-details', action='store_true', help="跳过更新股票详情")
    p_daily.add_argument('--skip-actions', action='store_true', help="跳过更新公司行动")
    p_daily.add_argument('--skip-prices', action='store_true', help="跳过 Massive 个股价格更新")
    p_daily.add_argument('--skip-grouped-daily', action='store_true', help="跳过 Massive Grouped Daily 回刷")
    p_daily.add_argument('--skip-shares', action='store_true', help="跳过 Massive shares 更新")
    p_daily.add_argument('--skip-turnover', action='store_true', help="跳过 turnover_rate 重建")
    p_daily.add_argument('--skip-em-prices', action='store_true', help="兼容旧参数；daily_run 已不再默认使用东方财富")
    p_daily.add_argument('--skip-polygon-prices', action='store_true', help="兼容旧参数；等价于跳过 Grouped Daily")
    p_daily.set_defaults(func=run_daily_update)

    p_sync_universe = subparsers.add_parser('sync_massive_universe', help="同步 Massive 活跃美股 universe")
    p_sync_universe.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_sync_universe.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_sync_universe.add_argument('--skip-mark-missing-inactive', action='store_true', help="跳过 inactive 标记。")
    p_sync_universe.set_defaults(func=run_sync_massive_universe)

    # --- 定义 'update_details' 命令 ---
    p_details = subparsers.add_parser('update_details', help="单独更新股票的详细信息 (来自Massive)")
    p_details.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_details.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_details.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_details.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_details.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_details.add_argument('--workers', type=int, help="并发线程数。")
    p_details.set_defaults(func=run_update_details)

    p_massive_details = subparsers.add_parser('update_massive_details', help="单独更新股票的详细信息 (来自 Massive)")
    p_massive_details.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_details.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_massive_details.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_details.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_massive_details.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_details.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_details.set_defaults(func=run_update_details)

    # --- 定义 'update_actions' 命令 ---
    p_actions = subparsers.add_parser('update_actions', help="单独更新股票的公司行动 (分红、拆股)")
    p_actions.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_actions.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_actions.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_actions.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_actions.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_actions.add_argument('--workers', type=int, help="并发线程数。")
    p_actions_recalc = p_actions.add_mutually_exclusive_group()
    p_actions_recalc.add_argument('--recalc-adj-factor', action='store_true', help="显式启用 actions 后 adj_factor 重算（默认启用；保留该参数用于兼容）。")
    p_actions_recalc.add_argument('--skip-recalc-adj-factor', action='store_true', help="更新 actions 后不自动重算 adj_factor。")
    p_actions.set_defaults(func=run_update_actions)

    p_massive_actions = subparsers.add_parser('update_massive_actions', help="单独更新股票的公司行动 (来自 Massive)")
    p_massive_actions.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_actions.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_massive_actions.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_actions.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_massive_actions.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_actions.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_actions_recalc = p_massive_actions.add_mutually_exclusive_group()
    p_massive_actions_recalc.add_argument('--recalc-adj-factor', action='store_true', help="显式启用 actions 后 adj_factor 重算。")
    p_massive_actions_recalc.add_argument('--skip-recalc-adj-factor', action='store_true', help="跳过 adj_factor 重算。")
    p_massive_actions.set_defaults(func=run_update_actions)

    p_backfill_actions = subparsers.add_parser('backfill_actions', help="使用 YFinance 补全公司行动 (分红/拆股)")
    p_backfill_actions.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_backfill_actions.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_backfill_actions.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_backfill_actions.add_argument('--start-date', type=str, help="开始日期 (YYYY-MM-DD)。")
    p_backfill_actions.add_argument('--end-date', type=str, help="结束日期 (YYYY-MM-DD)。")
    p_backfill_actions.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_backfill_actions.add_argument('--workers', type=int, help="并发线程数。")
    p_backfill_actions_recalc = p_backfill_actions.add_mutually_exclusive_group()
    p_backfill_actions_recalc.add_argument('--recalc-adj-factor', action='store_true', help="补全 actions 后强制重算 adj_factor。")
    p_backfill_actions_recalc.add_argument('--skip-recalc-adj-factor', action='store_true', help="补全 actions 后不自动重算 adj_factor。")
    p_backfill_actions.set_defaults(func=run_backfill_actions)

    # --- 定义 'update_em_prices' 命令 ---
    p_em_prices = subparsers.add_parser('update_em_prices', help="单独更新东方财富的日线价格")
    p_em_prices.add_argument('em_codes', nargs='*', help="要更新的股票东方财富代码列表。")
    p_em_prices.add_argument('--full-refresh', action='store_true', help="强制全量刷新。")
    p_em_prices.add_argument('--market', type=str, default='US', help="指定市场 (默认: US)。")
    p_em_prices.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_em_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_em_prices.set_defaults(func=run_update_em_prices)

    # --- 定义 'update_polygon_prices' 命令 ---
    p_grouped_daily = subparsers.add_parser('update_grouped_daily', help="使用Grouped Daily API回填/刷新指定日期的价格")
    p_grouped_daily.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    p_grouped_daily.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    p_grouped_daily.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_grouped_daily.add_argument('--workers', type=int, help="并发线程数。")
    p_grouped_daily.set_defaults(func=run_update_grouped_daily)

    # --- 定义 'migrate' 命令 ---
    p_migrate = subparsers.add_parser('migrate', help="执行数据库迁移（一次性操作）")
    p_migrate.set_defaults(func=run_migrate)

    # --- 定义 'update_polygon_prices' 命令 ---
    p_poly_prices = subparsers.add_parser('update_polygon_prices', help="单独更新 Massive 的日线价格（兼容旧命令名）")
    p_poly_prices.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_poly_prices.add_argument('--full-refresh', action='store_true', help="强制全量刷新。")
    p_poly_prices.add_argument('--market', type=str, default='US', help="指定市场 (默认: US)。")
    p_poly_prices.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_poly_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_poly_prices.set_defaults(func=run_update_polygon_prices)

    p_massive_prices = subparsers.add_parser('update_massive_prices', help="单独更新 Massive 的日线价格")
    p_massive_prices.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_prices.add_argument('--full-refresh', action='store_true', help="强制全量刷新最近 2 年窗口。")
    p_massive_prices.add_argument('--market', type=str, default='US', help="指定市场 (默认: US)。")
    p_massive_prices.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_prices.set_defaults(func=run_update_polygon_prices)

    p_adj = subparsers.add_parser('recalc_adj_factor', help="重新计算并回填 adj_factor（前复权 + Total Return）")
    p_adj.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_adj.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_adj.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_adj.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_adj.add_argument('--workers', type=int, help="并发线程数。")
    p_adj.set_defaults(func=run_recalc_adj_factor)

    p_shares = subparsers.add_parser('update_historical_shares', help="使用 Massive 更新 historical_shares（兼容旧命令名）")
    p_shares.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_shares.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_shares.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_shares.add_argument('--full-refresh', action='store_true', help="回填最近 2 年季度快照。")
    p_shares.add_argument('--start-date', type=str, default='2010-01-01', help="起始日期 (YYYY-MM-DD)。")
    p_shares.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_shares.add_argument('--workers', type=int, help="并发线程数。")
    p_shares.set_defaults(func=run_update_historical_shares)

    p_massive_shares = subparsers.add_parser('update_massive_shares', help="使用 Massive 更新 historical_shares")
    p_massive_shares.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_massive_shares.add_argument('--all', action='store_true', help="处理所有保留类型证券。")
    p_massive_shares.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_shares.add_argument('--full-refresh', action='store_true', help="回填最近 2 年季度快照。")
    p_massive_shares.add_argument('--start-date', type=str, default='2010-01-01', help="起始日期 (YYYY-MM-DD)。")
    p_massive_shares.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_shares.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_shares.set_defaults(func=run_update_historical_shares)

    p_turnover = subparsers.add_parser('backfill_turnover_rate', help="基于 historical_shares 回填 daily_prices.turnover_rate")
    p_turnover.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_turnover.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_turnover.add_argument('--market', type=str, help="仅处理指定市场的股票。")
    p_turnover.add_argument('--start-date', type=str, help="开始日期 (YYYY-MM-DD)。")
    p_turnover.add_argument('--end-date', type=str, help="结束日期 (YYYY-MM-DD)。")
    p_turnover.add_argument('--only-null', action='store_true', help="只更新 turnover_rate 为 NULL 的行（默认）。")
    p_turnover.add_argument('--overwrite', action='store_true', help="覆盖已有 turnover_rate 值（慎用）。")
    p_turnover.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_turnover.add_argument('--workers', type=int, help="并发线程数。")
    p_turnover.set_defaults(func=run_backfill_turnover_rate)

    p_rebuild_turnover = subparsers.add_parser('rebuild_turnover_rate', help="清空并重建 turnover_rate")
    p_rebuild_turnover.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_rebuild_turnover.add_argument('--all', action='store_true', help="处理所有保留类型证券。")
    p_rebuild_turnover.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_rebuild_turnover.add_argument('--start-date', type=str, help="开始日期 (YYYY-MM-DD)。")
    p_rebuild_turnover.add_argument('--end-date', type=str, help="结束日期 (YYYY-MM-DD)。")
    p_rebuild_turnover.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_rebuild_turnover.add_argument('--workers', type=int, help="并发线程数。")
    p_rebuild_turnover.set_defaults(func=run_rebuild_turnover_rate)

    p_cleanup_us = subparsers.add_parser('cleanup_us_universe', help="清理 US 中非普通股 / ETF / ADR 证券")
    p_cleanup_us.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_cleanup_us.add_argument('--limit', type=int, default=0, help="限制删除数量。")
    p_cleanup_us.add_argument('--sample-size', type=int, default=20, help="dry-run 样例数量。")
    p_cleanup_us.add_argument('--apply', action='store_true', help="执行真实删除。默认 dry-run。")
    p_cleanup_us.set_defaults(func=run_cleanup_us_universe)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
