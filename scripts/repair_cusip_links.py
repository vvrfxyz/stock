"""SEC_FTD CUSIP 错链修复工具——基于 audit 反向校验清理坏身份行。

audit_security_identity 的 SEC_FTD 反向校验（find_suspect_ftd_links）会标出
"源期间 symbol 归属可疑"的 CUSIP 映射行。本脚本删除这些 security_identifiers
行，并把经由该错链回填的 institutional_holdings.security_id 置回 NULL——
下次 sync_cusip_identifiers 会在身份稳定后用 PIT 语义重新解析回填。

只读 dry-run 模式（默认）：输出待删清单和受影响 holdings 数。
--apply 模式：执行删除与置 NULL。需人工先确认 dry-run 输出无误。

用法：
    python scripts/repair_cusip_links.py --dry-run       # 只输出 plan
    python scripts/repair_cusip_links.py --apply         # 执行修复
    python scripts/repair_cusip_links.py --dry-run --limit 5
"""
import argparse
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
from scripts.audit_security_identity import find_suspect_ftd_links
from utils.script_logging import setup_logging as configure_script_logging


def setup_logging():
    configure_script_logging("repair_cusip_links")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="清理反向校验标记的 SEC_FTD CUSIP 错链行，并重置受影响的 13F 关联。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只输出修复 plan，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="执行删除与 holdings 置 NULL。")
    parser.add_argument("--limit", type=int, default=200,
                        help="最多处理的可疑行数（默认 200）。")
    return parser


def build_plans(session, limit: int) -> list[dict]:
    """反向校验可疑行 -> 修复 plan（附受影响 holdings 行数，便于 dry-run 评估）。"""
    suspects = find_suspect_ftd_links(session)[:limit]
    plans = []
    for row in suspects:
        affected = session.execute(
            text(
                """
                SELECT count(*) FROM institutional_holdings
                WHERE security_id = :security_id AND upper(cusip) = :cusip
                """
            ),
            {"security_id": row.security_id, "cusip": row.cusip},
        ).scalar() or 0
        plans.append({
            "identifier_id": row.identifier_id,
            "security_id": row.security_id,
            "cusip": row.cusip,
            "symbol": row.current_symbol,
            "start_date": str(row.start_date) if row.start_date else None,
            "linked_at": f"{row.linked_at:%Y-%m-%d}",
            "signals": {
                "rename_event": row.via_rename_event,
                "recycle_event": row.via_recycle_event,
                "symbol_history": row.via_symbol_history,
            },
            "affected_holdings": affected,
        })
    return plans


def apply_plan(db_manager, plan: dict) -> tuple[int, int]:
    """删除坏 identifier 行 + 受影响 holdings 置 NULL。返回 (删除行数, 置 NULL 行数)。

    holdings 只重置"经由该 (CUSIP, security_id) 错链"的行，不碰其他来源的关联。"""
    with db_manager.engine.connect() as conn:
        unlinked = conn.execute(
            text(
                """
                UPDATE institutional_holdings
                SET security_id = NULL, updated_at = now()
                WHERE security_id = :security_id AND upper(cusip) = :cusip
                """
            ),
            {"security_id": plan["security_id"], "cusip": plan["cusip"]},
        ).rowcount or 0
        deleted = conn.execute(
            text("DELETE FROM security_identifiers WHERE id = :identifier_id"),
            {"identifier_id": plan["identifier_id"]},
        ).rowcount or 0
        conn.commit()
    return deleted, unlinked


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)
    is_apply = args.apply

    db_manager = None
    try:
        db_manager = DatabaseManager()

        with db_manager.get_session() as session:
            plans = build_plans(session, limit=args.limit)

        if not plans:
            logger.success("反向校验未标记任何 SEC_FTD 错链行。")
            return 0

        logger.info("发现 {} 行可疑 SEC_FTD CUSIP 映射待处理。", len(plans))
        for i, plan in enumerate(plans, 1):
            logger.info(
                "[{}/{}] identifier_id={} cusip={} -> security_id={} symbol={} "
                "start_date={} linked_at={} signals={} 受影响 holdings={}",
                i, len(plans), plan["identifier_id"], plan["cusip"], plan["security_id"],
                plan["symbol"], plan["start_date"], plan["linked_at"],
                plan["signals"], plan["affected_holdings"],
            )

        if not is_apply:
            logger.warning("以上为 dry-run 输出。确认无误后加 --apply 执行。")
            return 0

        logger.info("=== 开始执行修复 ===")
        total_deleted = 0
        total_unlinked = 0
        for plan in plans:
            deleted, unlinked = apply_plan(db_manager, plan)
            total_deleted += deleted
            total_unlinked += unlinked

        logger.success(
            "修复完成: 删除 identifier {} 行, institutional_holdings 置 NULL {} 行"
            "（等待下次 sync_cusip_identifiers 回填）。",
            total_deleted, total_unlinked,
        )
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("repair_cusip_links 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
