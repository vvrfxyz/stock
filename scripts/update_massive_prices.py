import argparse
import os
import sys
from datetime import timedelta, date

from loguru import logger
from sqlalchemy import or_

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import get_massive_history_floor
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)
from utils.trading_calendar import get_last_completed_trading_date

MAX_CONCURRENT_WORKERS = 18


def _clean_scalar(value, *, cast_int: bool = False):
    if value is None:
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    if cast_int:
        try:
            return int(value)
        except Exception:
            return None
    return value


def _sync_price_latest_date_from_existing_rows(
    security: Security,
    db_manager: DatabaseManager,
) -> date | None:
    """
    覆盖更新场景下，security.price_data_latest_date 可能落后于库里已有历史。
    用 daily_prices 的真实 max(date) 回写 metadata，避免后续增量判断失真。
    """
    actual_max_date = db_manager.get_security_price_max_date(security.id)
    tracked_latest_date = security.price_data_latest_date
    if actual_max_date and (tracked_latest_date is None or actual_max_date > tracked_latest_date):
        db_manager.update_security_price_latest_date(security.id, actual_max_date, is_full_run=False)
        logger.info(
            "[{}] 已对齐 price_data_latest_date: {} -> {}。",
            security.symbol,
            tracked_latest_date,
            actual_max_date,
        )
    return actual_max_date


def _finalize_price_metadata_after_successful_write(
    security: Security,
    db_manager: DatabaseManager,
    actual_max_date: date,
    *,
    is_full_run: bool,
) -> None:
    """
    价格写入成功后，统一回写 price_data_latest_date，并在 full-refresh 时刷新成功时间戳。
    即使 latest_date 没有变化，只要 full-refresh 成功，也应更新 full_data_last_updated_at。
    """
    tracked_latest_date = security.price_data_latest_date
    if not actual_max_date:
        return
    if is_full_run or tracked_latest_date is None or actual_max_date > tracked_latest_date:
        db_manager.update_security_price_latest_date(security.id, actual_max_date, is_full_run=is_full_run)
        if tracked_latest_date is None or actual_max_date > tracked_latest_date:
            logger.info(
                "[{}] 已对齐 price_data_latest_date: {} -> {}。",
                security.symbol,
                tracked_latest_date,
                actual_max_date,
            )


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive Custom Bars 获取美股日线数据并写入数据库。",
        default_workers=MAX_CONCURRENT_WORKERS,
        with_all=False,
    )
    parser.add_argument("--full-refresh", action="store_true", help="强制刷新 Massive 可覆盖的最近 2 年窗口。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace, end_trading_date: date) -> list[Security]:
    def _pending_only(query):
        return query.filter(
            or_(
                Security.price_data_latest_date.is_(None),
                Security.price_data_latest_date < end_trading_date,
            )
        )

    return select_us_securities(
        db_manager,
        args,
        extra_filter=None if args.full_refresh else _pending_only,
        order_column="price_data_latest_date",
    )


def process_security(
    security: Security,
    source: MassiveSource,
    db_manager: DatabaseManager,
    full_refresh: bool,
    end_trading_date: date,
) -> tuple[str, str, int]:
    symbol = security.symbol
    history_floor = get_massive_history_floor(end_trading_date)

    try:
        end_date = end_trading_date.isoformat()
        if full_refresh or security.price_data_latest_date is None:
            start_dt = history_floor
            is_full_run = True
        else:
            next_date = security.price_data_latest_date + timedelta(days=1)
            start_dt = max(next_date, history_floor)
            is_full_run = start_dt == history_floor

        # 死票回收防护：Massive 按 ticker 键控历史，list_date 之前的 bar 属于该
        # symbol 的旧身份，一律不拉（2026-07 gogl/lazr/pinc/spcx/opi/fusd 事故：
        # 回收 ticker 的新证券全量回填吞掉了旧实体两年的行情）。is_full_run 语义
        # 不变——对新证券而言 list_date 起就是它的全部可用历史。
        if security.list_date and security.list_date > start_dt:
            logger.info(
                "[{}] 回填起点从 {} clamp 到 list_date {}（symbol 历史早于本证券上市日）。",
                symbol, start_dt, security.list_date,
            )
            start_dt = security.list_date

        if start_dt > end_trading_date:
            return symbol, "SUCCESS_UP_TO_DATE", 0

        df = source.get_historical_data(symbol=symbol, start=start_dt.isoformat(), end=end_date, adjusted=False)
        if df.empty:
            actual_max_date = _sync_price_latest_date_from_existing_rows(security, db_manager)
            if actual_max_date and actual_max_date >= end_trading_date:
                return symbol, "SUCCESS_UP_TO_DATE", 0
            logger.info("[{}] Massive 在 {} - {} 未返回价格数据。", symbol, start_dt, end_date)
            return symbol, "SUCCESS_NO_NEW_DATA", 0

        df["date"] = df.index
        df.reset_index(drop=True, inplace=True)
        df["security_id"] = security.id
        df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            },
            inplace=True,
        )
        required_cols = [
            "security_id",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
            "otc",
        ]
        rows = []
        for row in df[required_cols].to_dict("records"):
            rows.append(
                {
                    "security_id": row["security_id"],
                    "date": row["date"],
                    "open": _clean_scalar(row["open"]),
                    "high": _clean_scalar(row["high"]),
                    "low": _clean_scalar(row["low"]),
                    "close": _clean_scalar(row["close"]),
                    "volume": _clean_scalar(row["volume"], cast_int=True),
                    "vwap": _clean_scalar(row["vwap"]),
                    "trade_count": _clean_scalar(row["trade_count"], cast_int=True),
                    "otc": _clean_scalar(row["otc"]),
                }
            )
        db_manager.upsert_daily_prices(rows)
        latest_date_in_db = db_manager.get_security_price_max_date(security.id)
        if latest_date_in_db is None:
            latest_date_in_db = df["date"].max()
        _finalize_price_metadata_after_successful_write(
            security,
            db_manager,
            latest_date_in_db,
            is_full_run=is_full_run,
        )
        return symbol, "SUCCESS", len(rows)
    except Exception as e:
        logger.opt(exception=e).error("[{}] 更新 Massive 日线失败: {}", symbol, e)
        return symbol, "ERROR", 0


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    end_trading_date = get_last_completed_trading_date(args.market)
    securities = get_securities_to_update(db_manager, args, end_trading_date)
    if not securities:
        logger.success("没有需要更新 Massive 日线数据的证券。")
        return 0

    logger.info("共 {} 支证券需要更新 Massive 日线，截止交易日 {}。", len(securities), end_trading_date)
    outputs, results_counter = run_concurrently(
        securities,
        lambda security: process_security(
            security, source, db_manager, args.full_refresh, end_trading_date
        ),
        max_workers=args.workers,
        desc="更新 Massive 日线",
    )
    total_rows = 0
    for _symbol, status, count in outputs:
        results_counter[status] += 1
        total_rows += count

    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    logger.info("--- 任务执行统计 ---")
    logger.info("  成功: {}", results_counter["SUCCESS"])
    logger.info("  无新数据: {}", results_counter["SUCCESS_NO_NEW_DATA"])
    logger.info("  已是最新: {}", results_counter["SUCCESS_UP_TO_DATE"])
    logger.info("  错误: {}", errors)
    logger.info("  写入行数: {}", total_rows)
    logger.info("----------------------")
    exit_code = 1 if errors else 0
    stats = {"processed": len(securities), "written": total_rows, "failed": errors}
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_prices", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
