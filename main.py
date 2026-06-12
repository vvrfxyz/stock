# main.py
import os
import sys
import argparse
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Callable

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
from scripts.update_massive_details import main as update_details_main
from scripts.update_massive_actions import main as update_actions_main
from scripts.update_grouped_daily import main as update_grouped_daily_main
from scripts.update_massive_prices import main as update_massive_prices_main
from scripts.sync_massive_universe import main as sync_massive_universe_main
from scripts.sync_sec_identifiers import main as sync_sec_identifiers_main
from scripts.update_sec_filings import main as update_sec_filings_main
from scripts.update_sec_fundamentals import main as update_sec_fundamentals_main
from scripts.update_insider_transactions import main as update_insider_transactions_main
from scripts.update_institutional_holdings import main as update_institutional_holdings_main
from scripts.update_massive_shares import main as update_massive_shares_main
from scripts.update_massive_events import main as update_massive_events_main
from scripts.update_massive_short_data import main as update_massive_short_data_main
from scripts.update_massive_news import main as update_massive_news_main
from scripts.update_adjustment_factors import main as update_adjustment_factors_main
from scripts.update_open_close_summary import main as update_open_close_summary_main
from scripts.cleanup_us_universe import main as cleanup_us_universe_main
from scripts.migrate_database import main as migrate_main
from utils.script_logging import setup_logging as configure_script_logging
from utils.trading_calendar import get_last_completed_trading_date, shift_trading_date


@dataclass(frozen=True)
class ScheduledStep:
    name: str
    main_func: Callable
    args: list[str]


def setup_logging():
    """配置全局 Loguru 日志记录器"""
    configure_script_logging("main_controller")
    logger.info("主控制器日志记录器设置完成。")


def execute_script(main_func, args_list):
    """
    调用子脚本的 main(argv)，并把非零 int 返回码统一转换为 SystemExit，
    使调度层可以按退出码感知失败。
    """
    script_name = main_func.__module__ + ".py"
    try:
        logger.debug(f"正在执行: {script_name} with args: {args_list}")
        result = main_func(args_list)
        if type(result) is int and result != 0:
            raise SystemExit(result)
    except SystemExit as e:
        # argparse 的 --help 会触发 SystemExit(0)，这是正常行为
        if e.code != 0:
            logger.error(f"脚本 {script_name} 异常退出，退出码: {e.code}")
            raise


# ==============================================================================
#  命令处理函数
# ==============================================================================

def run_update(args):
    """
    自动更新当前需要刷新的核心 raw facts。
    顺序: 详情(按需) -> 公司行动(按需) -> 缺失日线。
    """
    start_time = time.monotonic()
    market = (args.market or "US").upper()
    if market != "US":
        logger.critical("update 当前仅支持 US 市场。")
        return

    symbols = [symbol.lower() for symbol in getattr(args, "symbols", []) if symbol]

    def common_args() -> list[str]:
        cli_args = ["--market", market]
        if getattr(args, "limit", 0) > 0:
            cli_args.extend(["--limit", str(args.limit)])
        if getattr(args, "workers", None):
            cli_args.extend(["--workers", str(args.workers)])
        cli_args.extend(symbols)
        return cli_args

    logger.info("🚀 ======== 开始自动更新 raw truth 数据 ======== 🚀")

    details_args = common_args()
    if getattr(args, "force_details", False):
        details_args.append("--force")
    logger.info("\n--- [1/3] 按需更新股票基本信息 ---")
    execute_script(update_details_main, details_args)

    actions_args = common_args()
    if getattr(args, "force_actions", False):
        actions_args.append("--force")
    logger.info("\n--- [2/3] 按需更新分红/拆股事件 ---")
    execute_script(update_actions_main, actions_args)

    price_args = common_args()
    if getattr(args, "full_refresh_prices", False):
        price_args.append("--full-refresh")
    logger.info("\n--- [3/3] 补齐当前缺失的日线数据 ---")
    execute_script(update_massive_prices_main, price_args)

    logger.success(
        "✅ ======== 自动更新完成，总耗时: {} ======== ✅",
        timedelta(seconds=time.monotonic() - start_time),
    )


def _is_first_weekday_of_month(run_date: date, weekday: int) -> bool:
    return run_date.weekday() == weekday and run_date.day <= 7


def build_scheduled_update_steps(run_date: date, market: str = "US") -> list[ScheduledStep]:
    market = (market or "US").upper()
    end_trading_date = get_last_completed_trading_date(market)
    steps: list[ScheduledStep] = [
        ScheduledStep(
            "update_massive_prices",
            update_massive_prices_main,
            ["--market", market],
        ),
        ScheduledStep(
            "update_massive_short_data",
            update_massive_short_data_main,
            ["--market", market],
        ),
        ScheduledStep(
            "update_massive_actions_recent",
            update_actions_main,
            ["--market", market, "--all", "--recent-days", "14"],
        ),
        ScheduledStep(
            "update_open_close_summary",
            update_open_close_summary_main,
            [
                "--market",
                market,
                "--all",
                "--start-date",
                end_trading_date.isoformat(),
                "--end-date",
                end_trading_date.isoformat(),
            ],
        ),
    ]

    if run_date.weekday() == 5:
        steps.append(
            ScheduledStep(
                "update_massive_shares",
                update_massive_shares_main,
                ["--market", market, "--all"],
            )
        )
        grouped_start = shift_trading_date(market, end_trading_date, sessions=-5)
        steps.append(
            ScheduledStep(
                "update_grouped_daily_recent",
                update_grouped_daily_main,
                [
                    "--market", market,
                    "--start-date", grouped_start.isoformat(),
                    "--end-date", end_trading_date.isoformat(),
                ],
            )
        )
    if run_date.weekday() == 6:
        steps.append(
            ScheduledStep(
                "update_massive_actions",
                update_actions_main,
                ["--market", market, "--all", "--force"],
            )
        )
        steps.append(
            ScheduledStep(
                "sync_sec_identifiers",
                sync_sec_identifiers_main,
                ["--market", market],
            )
        )
        steps.append(
            ScheduledStep(
                "update_sec_filings_recent",
                update_sec_filings_main,
                [
                    "--market", market,
                    "--all",
                    "--since", (run_date - timedelta(days=14)).isoformat(),
                ],
            )
        )
        steps.append(
            ScheduledStep(
                "update_sec_fundamentals_recent",
                update_sec_fundamentals_main,
                [
                    "--market", market,
                    "--all",
                    "--since", (run_date - timedelta(days=14)).isoformat(),
                ],
            )
        )
        steps.append(
            ScheduledStep(
                "update_insider_transactions_recent",
                update_insider_transactions_main,
                [
                    "--market", market,
                    "--all",
                    "--since", (run_date - timedelta(days=21)).isoformat(),
                ],
            )
        )
        steps.append(
            ScheduledStep(
                "update_institutional_holdings_recent",
                update_institutional_holdings_main,
                [
                    "--market", market,
                    "--since", (run_date - timedelta(days=14)).isoformat(),
                ],
            )
        )
    if _is_first_weekday_of_month(run_date, 1):
        steps.append(
            ScheduledStep(
                "update_massive_events",
                update_massive_events_main,
                ["--market", market, "--all", "--force"],
            )
        )
    if _is_first_weekday_of_month(run_date, 2):
        steps.append(
            ScheduledStep(
                "update_massive_details",
                update_details_main,
                ["--market", market, "--all", "--force"],
            )
        )
    return steps


def run_scheduled_update(args):
    run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date() if args.run_date else date.today()
    market = (args.market or "US").upper()
    logger.info("执行: scheduled_update market={} run_date={}", market, run_date)

    steps = build_scheduled_update_steps(run_date, market)
    logger.info("计划执行 {} 个任务: {}", len(steps), ", ".join(step.name for step in steps))
    failed_steps: list[str] = []
    for index, step in enumerate(steps, start=1):
        logger.info("--- [{}/{}] {} ---", index, len(steps), step.name)
        try:
            execute_script(step.main_func, step.args)
        except SystemExit as exc:
            failed_steps.append(f"{step.name}(exit={exc.code})")
            logger.error("步骤 {} 失败（exit={}），继续执行后续步骤。", step.name, exc.code)
        except Exception as exc:
            failed_steps.append(f"{step.name}({type(exc).__name__})")
            logger.opt(exception=exc).error("步骤 {} 发生未捕获异常，继续执行后续步骤: {}", step.name, exc)

    if failed_steps:
        logger.error("scheduled_update 完成，但 {}/{} 个步骤失败: {}", len(failed_steps), len(steps), ", ".join(failed_steps))
        raise SystemExit(1)
    logger.success("scheduled_update 全部 {} 个步骤成功。", len(steps))


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
    if getattr(args, 'recent_days', 0): cli_args.extend(['--recent-days', str(args.recent_days)])
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_actions_main, cli_args)

def run_update_massive_prices(args):
    logger.info("执行: 更新 Massive 日线价格")
    cli_args = []
    if args.full_refresh: cli_args.append('--full-refresh')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_massive_prices_main, cli_args)

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


def run_update_massive_events(args):
    logger.info("执行: 更新 Massive ticker events / symbol history")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_massive_events_main, cli_args)


def run_update_massive_short_data(args):
    logger.info("执行: 更新 Massive short interest / short volume")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_massive_short_data_main, cli_args)


def run_update_massive_news(args):
    logger.info("执行: 更新 Massive news")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.force: cli_args.append('--force')
    if args.market: cli_args.extend(['--market', args.market])
    if args.lookback_days: cli_args.extend(['--lookback-days', str(args.lookback_days)])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.workers: cli_args.extend(['--workers', str(args.workers)])
    cli_args.extend(args.symbols)
    execute_script(update_massive_news_main, cli_args)


def run_update_adjustment_factors(args):
    logger.info("执行: 重建/对账复权因子 reference/cache")
    cli_args = []
    if args.all: cli_args.append('--all')
    if args.market: cli_args.extend(['--market', args.market])
    if args.source: cli_args.extend(['--source', args.source])
    if args.limit > 0: cli_args.extend(['--limit', str(args.limit)])
    if args.methodology_version: cli_args.extend(['--methodology-version', args.methodology_version])
    if args.tolerance: cli_args.extend(['--tolerance', args.tolerance])
    if args.refresh_vendor_daily_bars: cli_args.append('--refresh-vendor-daily-bars')
    if args.daily_start_date: cli_args.extend(['--daily-start-date', args.daily_start_date])
    if args.daily_end_date: cli_args.extend(['--daily-end-date', args.daily_end_date])
    cli_args.extend(args.symbols)
    execute_script(update_adjustment_factors_main, cli_args)


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


def run_sync_sec_identifiers(args):
    logger.info("执行: 同步 SEC CIK 身份映射")
    execute_script(sync_sec_identifiers_main, ['--market', args.market])


def run_update_sec_filings(args):
    logger.info("执行: 同步 SEC filing 索引")
    cli_args = list(args.symbols)
    if args.all:
        cli_args.append('--all')
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.since:
        cli_args.extend(['--since', args.since])
    if args.forms:
        cli_args.extend(['--forms', args.forms])
    if getattr(args, 'all_forms', False):
        cli_args.append('--all-forms')
    if getattr(args, 'include_older_pages', False):
        cli_args.append('--include-older-pages')
    execute_script(update_sec_filings_main, cli_args)


def run_update_sec_fundamentals(args):
    logger.info("执行: 同步 SEC XBRL 基本面事实")
    cli_args = list(args.symbols)
    if args.all:
        cli_args.append('--all')
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.since:
        cli_args.extend(['--since', args.since])
    if getattr(args, 'bulk_zip', None):
        cli_args.extend(['--bulk-zip', args.bulk_zip])
    execute_script(update_sec_fundamentals_main, cli_args)


def run_update_insider_transactions(args):
    logger.info("执行: 解析 SEC Form 3/4/5 内部人交易")
    cli_args = list(args.symbols)
    if args.all:
        cli_args.append('--all')
    if args.market:
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.since:
        cli_args.extend(['--since', args.since])
    if getattr(args, 'reparse', False):
        cli_args.append('--reparse')
    execute_script(update_insider_transactions_main, cli_args)


def run_update_institutional_holdings(args):
    logger.info("执行: 同步 SEC 13F 机构持仓")
    cli_args = []
    if args.since:
        cli_args.extend(['--since', args.since])
    if getattr(args, 'quarter', None):
        cli_args.extend(['--quarter', args.quarter])
    if getattr(args, 'filer_cik', None):
        cli_args.extend(['--filer-cik', args.filer_cik])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if getattr(args, 'reparse', False):
        cli_args.append('--reparse')
    execute_script(update_institutional_holdings_main, cli_args)


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


def run_update_open_close_summary(args):
    logger.info("执行: 回填 Massive 盘前/盘后价格")
    cli_args = ['--start-date', args.start_date, '--end-date', args.end_date]
    if args.all:
        cli_args.append('--all')
    if getattr(args, 'market', None):
        cli_args.extend(['--market', args.market])
    if args.limit > 0:
        cli_args.extend(['--limit', str(args.limit)])
    if args.workers:
        cli_args.extend(['--workers', str(args.workers)])
    if args.overwrite:
        cli_args.append('--overwrite')
    cli_args.extend(args.symbols)
    execute_script(update_open_close_summary_main, cli_args)


def run_rebuild_massive_dataset(args):
    logger.info("执行: Massive-only 数据集重建")
    market = (args.market or "US").upper()
    if market != "US":
        logger.critical("rebuild_massive_dataset 当前仅支持 US。")
        return

    end_trading_date = get_last_completed_trading_date(market)
    start_trading_date = shift_trading_date(market, end_trading_date, sessions=-4)

    execute_script(sync_massive_universe_main, ['--market', market])
    execute_script(update_details_main, ['--market', market, '--all', '--force'])
    execute_script(update_actions_main, ['--market', market, '--all', '--force'])
    execute_script(update_massive_prices_main, ['--market', market, '--full-refresh'])
    execute_script(
        update_grouped_daily_main,
        [
            '--market',
            market,
            '--start-date',
            start_trading_date.strftime('%Y-%m-%d'),
            '--end-date',
            end_trading_date.strftime('%Y-%m-%d'),
        ],
    )
    execute_script(
        update_massive_shares_main,
        [
            '--market',
            market,
            '--all',
            '--full-refresh',
        ],
    )

    if getattr(args, 'with_open_close_summary', False):
        execute_script(
            update_open_close_summary_main,
            [
                '--market',
                market,
                '--all',
                '--start-date',
                end_trading_date.strftime('%Y-%m-%d'),
                '--end-date',
                end_trading_date.strftime('%Y-%m-%d'),
            ],
        )


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

    p_update = subparsers.add_parser(
        'update',
        help="自动更新缺失日线、到期详情和分红拆股",
        description="唯一推荐的日常业务入口：自动判断并更新详情、公司行动和缺失日线。",
    )
    p_update.add_argument('symbols', nargs='*', help="可选：只更新指定股票代码；不传则处理全市场需要更新的证券。")
    p_update.add_argument('--market', type=str, default='US', help="指定市场，当前仅支持 US。")
    p_update.add_argument('--limit', type=int, default=0, help="限制每个步骤处理的证券数量；0 表示不限制。")
    p_update.add_argument('--workers', type=int, help="并发线程数。")
    p_update.add_argument('--force-details', action='store_true', help="强制更新详情，忽略 30 天间隔。")
    p_update.add_argument('--force-actions', action='store_true', help="强制刷新公司行动，使用 Massive 可覆盖窗口。")
    p_update.add_argument('--full-refresh-prices', action='store_true', help="强制刷新价格最近 2 年窗口，而不是只补缺口。")
    p_update.set_defaults(func=run_update)

    p_scheduled = subparsers.add_parser(
        'scheduled_update',
        help="统一每日调度入口：日更任务每天跑，周/月任务按日期错峰跑",
        description=(
            "顺序执行 Massive 采集任务，复用同一进程内的 key 限流状态。\n"
            "每天: 日线、short data、最近交易日盘前/盘后。\n"
            "周六: shares/floats。周日: 分红拆股。\n"
            "每月第一个周二: ticker events。每月第一个周三: details。"
        ),
    )
    p_scheduled.add_argument('--market', type=str, default='US', help="指定市场，当前仅支持 US。")
    p_scheduled.add_argument('--run-date', type=str, help="调度日期 YYYY-MM-DD；默认使用本机当前日期。")
    p_scheduled.set_defaults(func=run_scheduled_update)

    p_sync_universe = subparsers.add_parser('sync_massive_universe', help="同步 Massive 活跃美股 universe")
    p_sync_universe.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_sync_universe.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_sync_universe.add_argument('--skip-mark-missing-inactive', action='store_true', help="跳过 inactive 标记。")
    p_sync_universe.set_defaults(func=run_sync_massive_universe)

    p_sec_ids = subparsers.add_parser('sync_sec_identifiers', help="同步 SEC ticker->CIK 身份映射")
    p_sec_ids.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_sec_ids.set_defaults(func=run_sync_sec_identifiers)

    p_sec_filings = subparsers.add_parser('update_sec_filings', help="同步 SEC EDGAR filing 索引")
    p_sec_filings.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_sec_filings.add_argument('--all', action='store_true', help="处理所有有 CIK 的活跃证券。")
    p_sec_filings.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_sec_filings.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_sec_filings.add_argument('--since', type=str, default=None, help="只保留该日期之后的 filing。")
    p_sec_filings.add_argument('--forms', type=str, default=None, help="逗号分隔 form 列表覆盖默认集。")
    p_sec_filings.add_argument('--all-forms', action='store_true', help="不过滤 form type。")
    p_sec_filings.add_argument('--include-older-pages', action='store_true', help="追加历史分页（深回填）。")
    p_sec_filings.set_defaults(func=run_update_sec_filings)

    p_sec_fund = subparsers.add_parser('update_sec_fundamentals', help="同步 SEC XBRL curated 基本面事实")
    p_sec_fund.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_sec_fund.add_argument('--all', action='store_true', help="处理所有有 CIK 的活跃证券。")
    p_sec_fund.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_sec_fund.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_sec_fund.add_argument('--since', type=str, default=None, help="增量：只处理该日后有财报 filing 的 CIK。")
    p_sec_fund.add_argument('--bulk-zip', type=str, default=None, help="本地 companyfacts.zip 路径（初次回填）。")
    p_sec_fund.set_defaults(func=run_update_sec_fundamentals)

    p_insiders = subparsers.add_parser('update_insider_transactions', help="解析 SEC Form 3/4/5 内部人交易明细")
    p_insiders.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_insiders.add_argument('--all', action='store_true', help="处理全部待解析的 Form 3/4/5。")
    p_insiders.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_insiders.add_argument('--limit', type=int, default=0, help="限制处理 filing 数量。")
    p_insiders.add_argument('--since', type=str, default=None, help="只处理该日期之后的 filing。")
    p_insiders.add_argument('--reparse', action='store_true', help="重解析已有明细的 filing。")
    p_insiders.set_defaults(func=run_update_insider_transactions)

    p_13f = subparsers.add_parser('update_institutional_holdings', help="同步 SEC 13F-HR 机构持仓明细")
    p_13f.add_argument('--since', type=str, default=None, help="按日扫 daily index 的起始日期。")
    p_13f.add_argument('--quarter', type=str, default=None, help="季度全量回填，如 2026Q1。")
    p_13f.add_argument('--filer-cik', type=str, default=None, help="只处理该 filer CIK。")
    p_13f.add_argument('--limit', type=int, default=0, help="限制处理 filing 数量。")
    p_13f.add_argument('--reparse', action='store_true', help="重新解析已入库 filing。")
    p_13f.set_defaults(func=run_update_institutional_holdings)

    p_massive_details = subparsers.add_parser('update_massive_details', help="单独更新股票的详细信息 (来自 Massive)")
    p_massive_details.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_details.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_massive_details.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_details.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_massive_details.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_details.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_details.set_defaults(func=run_update_details)

    p_massive_actions = subparsers.add_parser('update_massive_actions', help="单独更新股票的公司行动 (来自 Massive)")
    p_massive_actions.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_actions.add_argument('--all', action='store_true', help="处理所有活跃股票。")
    p_massive_actions.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_actions.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_massive_actions.add_argument('--recent-days', type=int, default=0, help="只拉取最近 N 天的新事件。")
    p_massive_actions.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_actions.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_actions.set_defaults(func=run_update_actions)

    p_grouped_daily = subparsers.add_parser('update_grouped_daily', help="使用Grouped Daily API回填/刷新指定日期的价格")
    p_grouped_daily.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    p_grouped_daily.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    p_grouped_daily.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_grouped_daily.add_argument('--workers', type=int, help="并发线程数。")
    p_grouped_daily.set_defaults(func=run_update_grouped_daily)

    p_open_close = subparsers.add_parser('update_open_close_summary', help="回填 Massive Daily Ticker Summary 的盘前/盘后价格")
    p_open_close.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_open_close.add_argument('--all', action='store_true', help="处理所有保留类型证券。")
    p_open_close.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_open_close.add_argument('--start-date', type=str, required=True, help="开始日期 (YYYY-MM-DD)")
    p_open_close.add_argument('--end-date', type=str, required=True, help="结束日期 (YYYY-MM-DD)")
    p_open_close.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_open_close.add_argument('--workers', type=int, help="并发线程数。")
    p_open_close.add_argument('--overwrite', action='store_true', help="覆盖已有盘前/盘后价格。")
    p_open_close.set_defaults(func=run_update_open_close_summary)

    # --- 定义 'migrate' 命令 ---
    p_migrate = subparsers.add_parser('migrate', help="执行数据库迁移（一次性操作）")
    p_migrate.set_defaults(func=run_migrate)

    p_massive_prices = subparsers.add_parser('update_massive_prices', help="单独更新 Massive 的日线价格")
    p_massive_prices.add_argument('symbols', nargs='*', help="要更新的股票代码列表。")
    p_massive_prices.add_argument('--full-refresh', action='store_true', help="强制全量刷新最近 2 年窗口。")
    p_massive_prices.add_argument('--market', type=str, default='US', help="指定市场 (默认: US)。")
    p_massive_prices.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_prices.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_prices.set_defaults(func=run_update_massive_prices)

    p_massive_shares = subparsers.add_parser('update_massive_shares', help="使用 Massive 更新 historical_shares")
    p_massive_shares.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_massive_shares.add_argument('--all', action='store_true', help="处理所有保留类型证券。")
    p_massive_shares.add_argument('--market', type=str, default='US', help="仅处理指定市场的股票。")
    p_massive_shares.add_argument('--full-refresh', action='store_true', help="回填最近 2 年季度快照。")
    p_massive_shares.add_argument('--start-date', type=str, default='2010-01-01', help="起始日期 (YYYY-MM-DD)。")
    p_massive_shares.add_argument('--limit', type=int, default=0, help="限制处理的股票数量。")
    p_massive_shares.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_shares.set_defaults(func=run_update_historical_shares)

    p_massive_events = subparsers.add_parser('update_massive_events', help="使用 Massive Ticker Events 更新 symbol history")
    p_massive_events.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_massive_events.add_argument('--all', action='store_true', help="处理所有活跃 CS/ETF。")
    p_massive_events.add_argument('--market', type=str, default='US', help="仅处理指定市场。")
    p_massive_events.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_massive_events.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_massive_events.add_argument('--workers', type=int, help="并发线程数。")
    p_massive_events.set_defaults(func=run_update_massive_events)

    p_short_data = subparsers.add_parser('update_massive_short_data', help="使用 Massive 更新 short interest / short volume")
    p_short_data.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_short_data.add_argument('--all', action='store_true', help="处理所有活跃 CS/ETF。")
    p_short_data.add_argument('--market', type=str, default='US', help="仅处理指定市场。")
    p_short_data.add_argument('--force', action='store_true', help="强制刷新 Massive 可覆盖窗口。")
    p_short_data.add_argument('--limit', type=int, default=0, help="限制处理数量。")
    p_short_data.add_argument('--workers', type=int, help="批次并发数。")
    p_short_data.set_defaults(func=run_update_massive_short_data)

    p_news = subparsers.add_parser('update_massive_news', help="使用 Massive 更新 news / sentiment insights")
    p_news.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_news.add_argument('--all', action='store_true', help="处理所有活跃 CS/ETF。")
    p_news.add_argument('--market', type=str, default='US', help="仅处理指定市场。")
    p_news.add_argument('--force', action='store_true', help="强制更新，忽略时间检查。")
    p_news.add_argument('--lookback-days', type=int, default=7, help="抓取最近 N 天新闻。")
    p_news.add_argument('--limit', type=int, default=0, help="限制处理证券数量。")
    p_news.add_argument('--workers', type=int, help="批次并发数。")
    p_news.set_defaults(func=run_update_massive_news)

    p_adjustment = subparsers.add_parser('update_adjustment_factors', help="重建内部复权因子 cache，并与供应商 reference 对账")
    p_adjustment.add_argument('symbols', nargs='*', help="要处理的股票代码列表。")
    p_adjustment.add_argument('--all', action='store_true', help="处理所有活跃 CS/ETF。")
    p_adjustment.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_adjustment.add_argument('--source', type=str, default='MASSIVE', help="公司行动/供应商因子来源。")
    p_adjustment.add_argument('--limit', type=int, default=0, help="限制处理证券数量。")
    p_adjustment.add_argument('--methodology-version', default='raw_actions_v1', help="内部计算口径版本。")
    p_adjustment.add_argument('--tolerance', default='0.000010', help="对账容忍误差。")
    p_adjustment.add_argument('--refresh-vendor-daily-bars', action='store_true', help="额外拉取 Massive adjusted/raw 日线 reference 因子。")
    p_adjustment.add_argument('--daily-start-date', type=str, help="refresh vendor daily bars 的开始日期。")
    p_adjustment.add_argument('--daily-end-date', type=str, help="refresh vendor daily bars 的结束日期。")
    p_adjustment.set_defaults(func=run_update_adjustment_factors)

    p_cleanup_us = subparsers.add_parser('cleanup_us_universe', help="清理 US 中非普通股 / ETF 证券")
    p_cleanup_us.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_cleanup_us.add_argument('--limit', type=int, default=0, help="限制删除数量。")
    p_cleanup_us.add_argument('--sample-size', type=int, default=20, help="dry-run 样例数量。")
    p_cleanup_us.add_argument('--apply', action='store_true', help="执行真实删除。默认 dry-run。")
    p_cleanup_us.set_defaults(func=run_cleanup_us_universe)

    p_rebuild_massive = subparsers.add_parser('rebuild_massive_dataset', help="按 Massive 免费层能力重建当前可覆盖的数据集")
    p_rebuild_massive.add_argument('--market', type=str, default='US', help="当前仅支持 US。")
    p_rebuild_massive.add_argument('--with-open-close-summary', action='store_true', help="额外回填最近交易日盘前/盘后价格（较耗时）。")
    p_rebuild_massive.set_defaults(func=run_rebuild_massive_dataset)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
