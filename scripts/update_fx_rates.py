"""同步 ECB 每日参考汇率到 fx_rates。

用途：非 USD 分红（CAD/NOK/ILS 等跨上市）在重建复权因子时折算成 USD
（见 update_adjustment_factors）。全历史 CSV 一次请求 ~700KB，幂等 upsert。
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_sources.ecb_fx_source import fetch_ecb_fx_history
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 ECB 每日参考汇率。")
    parser.add_argument("--market", type=str, default="US", help="占位参数，保持调度接口一致。")
    parser.add_argument("--since", type=str, default=None,
                        help="只写入该日期（YYYY-MM-DD）之后的汇率；不传则全历史回填。")
    return parser


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_fx_rates")
    args = create_parser().parse_args(argv)
    since = date.fromisoformat(args.since) if args.since else None

    db_manager = None
    try:
        rows = fetch_ecb_fx_history(since=since)
        if not rows:
            logger.warning("ECB 汇率源未返回任何行（since={}）。", since)
            return 0
        db_manager = DatabaseManager()
        written = db_manager.upsert_fx_rates(rows)
        logger.info("ECB 汇率行写入/更新: {}（解析 {} 行，since={}）。", written, len(rows), since)
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_fx_rates 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
