"""XNYS 交易日历维护：exchange_calendars 离线生成 -> trading_calendars upsert。

背景（docs/data_infra_assessment_2026-07.md）：表此前为一次性人工灌入
（2010-01-04..2026-06-25），既够不到 2003 价格地板，又无人保鲜（滞后一周），
基于日历的完整性检查在 7 年面板上失明。exchange_calendars 库（已在依赖里，
utils/trading_calendar 的 fallback 就用它）离线覆盖 XNYS 全史，权威且零 API 成本。

行为：生成 [--start, 今天+180 天] 的全部日历日——交易日 is_open=true 带
开收盘时刻与半日标记，非交易日 is_open=false（周末不写，表语义按既有数据：
只存工作日）；upsert 冲突键 (exchange_mic, trade_date)，source='exchange_calendars'。
幂等，周度调度保鲜。

用法：
    python scripts/update_trading_calendars.py --start 2003-01-01     # 一次回补
    python scripts/update_trading_calendars.py                        # 周度保鲜（近 30 天起）
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

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

EXCHANGE_MIC = "XNYS"
FUTURE_DAYS = 180


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="维护 trading_calendars（XNYS，exchange_calendars 生成）。")
    parser.add_argument("--start", default=None,
                        help="窗口起点(YYYY-MM-DD)；缺省 = 今天 - 30 天（保鲜模式）。")
    return parser


def build_rows(start: date, end: date) -> list[dict]:
    import exchange_calendars as xc
    import pandas as pd

    calendar = xc.get_calendar("XNYS", start=start.isoformat(), side="left")
    sessions = calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    session_set = {s.date() for s in sessions}

    rows: list[dict] = []
    current = start
    while current <= end:
        if current.weekday() >= 5:  # 周末不入表（沿既有数据语义）
            current += timedelta(days=1)
            continue
        if current in session_set:
            ts = pd.Timestamp(current)
            open_at = calendar.session_open(ts).to_pydatetime()
            close_at = calendar.session_close(ts).to_pydatetime()
            rows.append({
                "exchange_mic": EXCHANGE_MIC, "trade_date": current, "is_open": True,
                "is_half_day": _is_half_day(open_at, close_at),
                "open_at": open_at, "close_at": close_at,
                "timezone": "America/New_York", "source": "exchange_calendars",
            })
        else:
            rows.append({
                "exchange_mic": EXCHANGE_MIC, "trade_date": current, "is_open": False,
                "is_half_day": False, "open_at": None, "close_at": None,
                "timezone": "America/New_York", "source": "exchange_calendars",
            })
        current += timedelta(days=1)
    return rows


def _is_half_day(open_at, close_at) -> bool:
    # 常规日 6.5 小时（09:30-16:00 ET）；显著短于此即半日（如 13:00 收盘）
    return (close_at - open_at).total_seconds() < 6 * 3600


def upsert_rows(db_manager: DatabaseManager, rows: list[dict]) -> int:
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from data_models.models import TradingCalendar

    if not rows:
        return 0
    total = 0
    with db_manager.engine.connect() as conn:
        for index in range(0, len(rows), 2000):
            batch = rows[index: index + 2000]
            stmt = pg_insert(TradingCalendar).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange_mic", "trade_date"],
                set_={
                    "is_open": stmt.excluded.is_open,
                    "is_half_day": stmt.excluded.is_half_day,
                    "open_at": stmt.excluded.open_at,
                    "close_at": stmt.excluded.close_at,
                    "timezone": stmt.excluded.timezone,
                    "source": stmt.excluded.source,
                },
            )
            total += conn.execute(stmt).rowcount or 0
        conn.commit()
    return total


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_trading_calendars")
    args = create_parser().parse_args(argv)
    start = date.fromisoformat(args.start) if args.start else date.today() - timedelta(days=30)
    end = date.today() + timedelta(days=FUTURE_DAYS)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        rows = build_rows(start, end)
        sessions = sum(1 for r in rows if r["is_open"])
        written = upsert_rows(db_manager, rows)
        logger.success("trading_calendars 已维护：窗口 [{}, {}]，{} 行（交易日 {}），upsert {}。",
                       start, end, len(rows), sessions, written)
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("update_trading_calendars 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
