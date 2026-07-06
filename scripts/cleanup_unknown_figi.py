"""清理 securities.composite_figi='UNKNOWN' 字面量占位符（vendor 伪 FIGI）。

约 82 只 2025-07 后退市的 CS 被 vendor 写成字面量 'UNKNOWN'——这不是真实
FIGI，会污染以 composite_figi 为键的身份对账（repair_identity 已被迫用正则
`^BBG` 显式排除，否则这批互不相干的公司会被并成一只）。本脚本把该字面量
清成 NULL，并为每只实际被修改的证券写一条 MANUAL 身份审计事件。

只读 dry-run 模式（默认）：列出待清理清单和计数，不写库。

--apply 模式：单条 UPDATE 置 NULL（以 composite_figi='UNKNOWN' 为条件，
天然幂等），事件只对本次 RETURNING 实际更新的行写入——重跑时 UPDATE
命中 0 行、不再写事件，无重复副作用。

用法：
    python scripts/cleanup_unknown_figi.py            # dry-run
    python scripts/cleanup_unknown_figi.py --apply    # 执行清理
"""
import argparse
import json
import os
import sys
import time
from datetime import timedelta

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

UNKNOWN_FIGI_LITERAL = "UNKNOWN"
CLEANUP_ACTION = "clear_unknown_figi_literal"


def setup_logging():
    configure_script_logging("cleanup_unknown_figi")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="清理 securities.composite_figi='UNKNOWN' 字面量为 NULL 并写审计事件。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只列出待清理清单，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="执行清理：UPDATE 置 NULL + 写 MANUAL 身份事件。")
    return parser


def find_unknown_figi_securities(session) -> list[dict]:
    """查出 composite_figi 为字面量 'UNKNOWN' 的证券（dry-run 展示用）。"""
    rows = session.execute(text(
        "SELECT id, symbol, composite_figi FROM securities "
        "WHERE composite_figi = :literal ORDER BY symbol, id"
    ), {"literal": UNKNOWN_FIGI_LITERAL}).all()
    return [
        {"id": row.id, "symbol": row.symbol, "composite_figi": row.composite_figi}
        for row in rows
    ]


def build_identity_events(updated_rows: list[dict]) -> list[dict]:
    """为实际被清理的行构造 MANUAL 身份事件（payload 形状见 insert_identity_events）。"""
    return [{
        "security_id": row["id"],
        "event_type": "MANUAL",
        "resolution_source": "MANUAL",
        "confidence": "HIGH",
        "details": json.dumps({
            "action": CLEANUP_ACTION,
            "previous_composite_figi": UNKNOWN_FIGI_LITERAL,
            "symbol": row["symbol"],
        }, ensure_ascii=False),
    } for row in updated_rows]


def apply_cleanup(db_manager) -> tuple[int, int]:
    """置 NULL 并只对本次实际更新的行写事件。返回 (updated_rows, events_written)。

    UPDATE 以 composite_figi='UNKNOWN' 为条件天然幂等；事件基于 RETURNING
    的实际命中行构造，重跑命中 0 行即不写事件，不会产生重复审计记录。
    """
    with db_manager.engine.connect() as conn:
        result = conn.execute(text(
            "UPDATE securities SET composite_figi = NULL "
            "WHERE composite_figi = :literal RETURNING id, symbol"
        ), {"literal": UNKNOWN_FIGI_LITERAL})
        updated_rows = [{"id": row.id, "symbol": row.symbol} for row in result]
        conn.commit()

    events = build_identity_events(updated_rows)
    events_written = db_manager.insert_identity_events(events) if events else 0
    if events and events_written != len(events):
        logger.warning("身份事件写入数 {} 与更新行数 {} 不一致，请人工核对。",
                       events_written, len(events))
    return len(updated_rows), events_written


def run(args, db_manager) -> int:
    with db_manager.get_session() as session:
        rows = find_unknown_figi_securities(session)

    if not rows:
        logger.success("未发现 composite_figi='UNKNOWN' 的证券，无需清理。")
        return 0

    logger.info("发现 {} 只 composite_figi='UNKNOWN' 的证券：", len(rows))
    for row in rows:
        logger.info("  id={} symbol={} composite_figi={}",
                    row["id"], row["symbol"], row["composite_figi"])

    if not args.apply:
        logger.warning("以上为 dry-run 输出（共 {} 只）。确认无误后加 --apply 执行。", len(rows))
        return 0

    updated, events_written = apply_cleanup(db_manager)
    logger.success("清理完成: {} 行 composite_figi 置 NULL, {} 条 MANUAL 身份事件写入。",
                   updated, events_written)
    return 0


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        return run(args, db_manager)
    except Exception as exc:
        logger.opt(exception=exc).critical("cleanup_unknown_figi 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
