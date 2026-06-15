"""同步 FRED risk-free reference rates 到 risk_free_rates。"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_sources.fred_source import DEFAULT_SERIES_ID, fetch_fred_series
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 FRED risk-free reference rates。")
    parser.add_argument("--market", type=str, default="US", help="占位参数，保持调度接口一致。")
    parser.add_argument("--series-id", default=DEFAULT_SERIES_ID, help="FRED series id，默认 DTB3。")
    parser.add_argument("--since", type=str, default=None, help="只写入该日期（YYYY-MM-DD）之后的数据；不传则全历史回填。")
    return parser


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_risk_free_rates")
    args = create_parser().parse_args(argv)
    since = date.fromisoformat(args.since) if args.since else None

    db_manager = None
    try:
        rows = fetch_fred_series(args.series_id, since=since)
        if not rows:
            logger.error("FRED {} 未返回任何行（since={}）。", args.series_id, since)
            return 1
        db_manager = DatabaseManager()
        written = db_manager.upsert_risk_free_rates(rows)
        logger.info("FRED {} 行写入/更新: {}（解析 {} 行，since={}）。", args.series_id, written, len(rows), since)
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("update_risk_free_rates 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
