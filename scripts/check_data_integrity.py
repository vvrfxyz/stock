import os
import sys
import time
import argparse
from datetime import timedelta

from loguru import logger
from sqlalchemy import func

# --- 路径设置 ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- 路径设置结束 ---

from db_manager import DatabaseManager
from data_models.models import Security, DailyPrice
from utils.script_logging import setup_logging as configure_script_logging


def setup_logging():
    configure_script_logging("check_data_integrity")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="数据一致性检查（只读）。建议在大规模写入/迁移后运行。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=20, help="每项检查最多输出的样例数量 (默认: 20)")
    return parser


def _report_rows(title: str, total_count: int, rows, limit: int):
    if total_count <= 0:
        logger.success(f"✅ {title}: OK")
        return
    logger.warning(f"⚠️ {title}: 发现问题 {total_count} 条（展示前 {min(limit, len(rows))} 条样例）")
    for row in rows[:limit]:
        logger.warning(f"  - {row}")


def check_price_latest_date_consistency(session, limit: int) -> int:
    """
    检查 Security.price_data_latest_date 是否与 daily_prices 的 MAX(date) 一致。
    返回发现的问题数量（用于最终退出码）。
    """
    latest_subq = (
        session.query(
            DailyPrice.security_id,
            func.max(DailyPrice.date).label("max_date"),
        )
        .group_by(DailyPrice.security_id)
        .subquery("latest_dates")
    )

    # A) 有价格数据但 latest_date 不一致/为空
    mismatch_query = (
        session.query(
            Security.id,
            Security.symbol,
            Security.price_data_latest_date,
            latest_subq.c.max_date,
        )
        .join(latest_subq, Security.id == latest_subq.c.security_id)
        .filter(
            (Security.price_data_latest_date.is_(None))
            | (Security.price_data_latest_date != latest_subq.c.max_date)
        )
    )
    mismatch_count = mismatch_query.count()
    mismatches = mismatch_query.order_by(Security.id.asc()).limit(limit).all()

    # B) latest_date 有值，但实际上没有任何 daily_prices
    orphan_query = (
        session.query(Security.id, Security.symbol, Security.price_data_latest_date)
        .outerjoin(latest_subq, Security.id == latest_subq.c.security_id)
        .filter(latest_subq.c.max_date.is_(None), Security.price_data_latest_date.isnot(None))
    )
    orphan_count = orphan_query.count()
    orphan_latest = orphan_query.order_by(Security.id.asc()).limit(limit).all()

    issue_count = 0
    if mismatch_count > 0:
        issue_count += mismatch_count
        _report_rows(
            "price_data_latest_date 与 daily_prices MAX(date) 不一致（样例）",
            mismatch_count,
            [f"id={r.id} symbol={r.symbol} latest={r.price_data_latest_date} max={r.max_date}" for r in mismatches],
            limit,
        )
    else:
        logger.success("✅ price_data_latest_date 与 daily_prices MAX(date) 一致: OK")

    if orphan_count > 0:
        issue_count += orphan_count
        _report_rows(
            "price_data_latest_date 有值但 daily_prices 为空（样例）",
            orphan_count,
            [f"id={r.id} symbol={r.symbol} latest={r.price_data_latest_date}" for r in orphan_latest],
            limit,
        )
    else:
        logger.success("✅ price_data_latest_date 与 daily_prices 空集一致性: OK")

    return issue_count


def check_symbol_normalization(session, limit: int) -> int:
    bad_query = session.query(Security.id, Security.symbol).filter(Security.symbol != func.lower(Security.symbol))
    bad_count = bad_query.count()
    bad_symbols = bad_query.order_by(Security.id.asc()).limit(limit).all()
    if bad_count <= 0:
        logger.success("✅ Security.symbol 小写规范: OK")
        return 0
    _report_rows(
        "Security.symbol 非小写（样例）",
        bad_count,
        [f"id={r.id} symbol={r.symbol}" for r in bad_symbols],
        limit,
    )
    return bad_count


def main():
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args()

    db_manager = None
    try:
        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            issues = 0
            issues += check_price_latest_date_consistency(session, limit=args.limit)
            issues += check_symbol_normalization(session, limit=args.limit)

            if issues > 0:
                logger.error(f"发现数据一致性问题（样例计数）: {issues}")
                raise SystemExit(2)
            logger.success("🎉 数据一致性检查通过。")
    finally:
        if db_manager:
            db_manager.close()
        logger.info(f"🏁 脚本执行完毕。总耗时: {timedelta(seconds=time.monotonic() - start_time)}")


if __name__ == "__main__":
    main()
