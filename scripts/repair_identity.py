"""存量证券身份修复工具——基于 audit 发现生成合并/拆分 plan。

只读 dry-run 模式（默认）：检测同 FIGI 多 id（身份分裂）和同 symbol 不同 FIGI
（ticker 回收），输出人工确认清单和可执行 SQL plan。

--apply 模式：执行修复并写入 identity events。需人工先确认 dry-run 输出无误。

用法：
    python scripts/repair_identity.py --dry-run         # 只输出 plan
    python scripts/repair_identity.py --apply           # 执行修复
    python scripts/repair_identity.py --dry-run --limit 5
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


def setup_logging():
    configure_script_logging("repair_identity")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="存量证券身份修复：合并分裂身份、标注 ticker 回收。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只输出修复 plan，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="执行修复并写入 identity events。")
    parser.add_argument("--limit", type=int, default=50,
                        help="最多处理的问题组数（默认 50）。")
    return parser


def find_split_identities(session, limit: int) -> list[dict]:
    """找出同一 composite_figi 落在多个 security_id 的分裂身份。"""
    sql = text("""
        SELECT composite_figi,
               array_agg(id ORDER BY id) AS security_ids,
               array_agg(symbol ORDER BY id) AS symbols,
               array_agg(is_active ORDER BY id) AS actives,
               array_agg(coalesce(price_data_latest_date::text, 'NULL') ORDER BY id) AS latest_dates
        FROM securities
        WHERE composite_figi IS NOT NULL AND composite_figi <> ''
        GROUP BY composite_figi
        HAVING count(*) > 1
        ORDER BY composite_figi
        LIMIT :limit
    """)
    rows = session.execute(sql, {"limit": limit}).all()
    plans = []
    for row in rows:
        ids = list(row.security_ids)
        symbols = list(row.symbols)
        actives = list(row.actives)
        latest = list(row.latest_dates)

        # 选择保留的 id：优先活跃行，其次有最新价格数据的
        keep_idx = 0
        for i, (active, ld) in enumerate(zip(actives, latest)):
            if active and not actives[keep_idx]:
                keep_idx = i
            elif active == actives[keep_idx] and ld != 'NULL' and (latest[keep_idx] == 'NULL' or ld > latest[keep_idx]):
                keep_idx = i

        keep_id = ids[keep_idx]
        merge_ids = [i for i in ids if i != keep_id]

        plans.append({
            "type": "MERGE",
            "figi": row.composite_figi,
            "keep_id": keep_id,
            "keep_symbol": symbols[keep_idx],
            "merge_ids": merge_ids,
            "merge_symbols": [symbols[i] for i in range(len(ids)) if ids[i] != keep_id],
            "detail": f"figi={row.composite_figi} ids={ids} symbols={symbols} active={actives} latest={latest}",
        })
    return plans


TABLES_WITH_SECURITY_ID = [
    "daily_prices",
    "corporate_actions",
    "computed_adjustment_factors",
    "vendor_adjustment_factors",
    "historical_shares",
    "historical_floats",
    "short_interests",
    "short_volumes",
    "news_articles",
    "security_symbol_history",
    "security_identifiers",
    "sec_fundamental_facts",
    "insider_transactions",
    "institutional_holdings",
]


def generate_merge_sql(plan: dict) -> list[str]:
    """为一组合并生成可执行 SQL。"""
    keep_id = plan["keep_id"]
    merge_ids = plan["merge_ids"]
    stmts = []
    for merge_id in merge_ids:
        for table in TABLES_WITH_SECURITY_ID:
            stmts.append(
                f"UPDATE {table} SET security_id = {keep_id} "
                f"WHERE security_id = {merge_id} "
                f"AND NOT EXISTS (SELECT 1 FROM {table} t2 "
                f"WHERE t2.security_id = {keep_id} "
                f"AND t2.date = {table}.date) "
                f"/* 仅对有 date 列的表生效，其他表直接 UPDATE */;"
                if table in ("daily_prices", "corporate_actions", "short_volumes", "short_interests")
                else f"UPDATE {table} SET security_id = {keep_id} WHERE security_id = {merge_id};"
            )
        stmts.append(
            f"UPDATE securities SET is_active = false WHERE id = {merge_id};"
        )
    return stmts


def apply_merge(db_manager, plan: dict) -> int:
    """执行合并：迁移数据行、标记旧 id inactive、写 identity event。"""
    keep_id = plan["keep_id"]
    rows_migrated = 0
    with db_manager.engine.connect() as conn:
        for merge_id in plan["merge_ids"]:
            for table in TABLES_WITH_SECURITY_ID:
                savepoint = conn.begin_nested()
                try:
                    result = conn.execute(text(
                        f"UPDATE {table} SET security_id = :keep WHERE security_id = :old"
                    ), {"keep": keep_id, "old": merge_id})
                    rows_migrated += result.rowcount or 0
                    savepoint.commit()
                except Exception as exc:
                    savepoint.rollback()
                    logger.warning(
                        "合并 {} -> {}: {} 表迁移跳过（唯一约束冲突）: {}",
                        merge_id, keep_id, table, type(exc).__name__,
                    )
            conn.execute(text(
                "UPDATE securities SET is_active = false WHERE id = :old"
            ), {"old": merge_id})
        conn.commit()

    db_manager.insert_identity_events([{
        "security_id": keep_id,
        "event_type": "MERGE",
        "old_symbol": ", ".join(plan["merge_symbols"])[:30],
        "new_symbol": plan["keep_symbol"][:30],
        "related_security_id": plan["merge_ids"][0] if len(plan["merge_ids"]) == 1 else None,
        "resolution_source": "AUDIT",
        "confidence": "HIGH",
        "details": json.dumps(plan, ensure_ascii=False, default=str),
    }])
    return rows_migrated


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)
    is_apply = args.apply

    db_manager = None
    try:
        db_manager = DatabaseManager()

        with db_manager.get_session() as session:
            plans = find_split_identities(session, limit=args.limit)

        if not plans:
            logger.success("未发现需要修复的身份分裂。")
            return 0

        logger.info("发现 {} 组身份分裂需处理。", len(plans))

        for i, plan in enumerate(plans, 1):
            logger.info("")
            logger.info("--- [{}/{}] {} ---", i, len(plans), plan["type"])
            logger.info("  FIGI: {}", plan["figi"])
            logger.info("  保留: id={} symbol={}", plan["keep_id"], plan["keep_symbol"])
            logger.info("  合并: ids={} symbols={}", plan["merge_ids"], plan["merge_symbols"])
            logger.info("  详情: {}", plan["detail"])

            if not is_apply:
                sql_stmts = generate_merge_sql(plan)
                logger.info("  -- SQL Plan ({} 条语句) --", len(sql_stmts))
                for stmt in sql_stmts[:5]:
                    logger.info("  {}", stmt)
                if len(sql_stmts) > 5:
                    logger.info("  ... 共 {} 条", len(sql_stmts))

        if not is_apply:
            logger.info("")
            logger.warning("以上为 dry-run 输出。确认无误后加 --apply 执行。")
            return 0

        logger.info("")
        logger.info("=== 开始执行修复 ===")
        total_migrated = 0
        for i, plan in enumerate(plans, 1):
            migrated = apply_merge(db_manager, plan)
            total_migrated += migrated
            logger.info("[{}/{}] 合并 {} -> {}: 迁移 {} 行",
                        i, len(plans), plan["merge_ids"], plan["keep_id"], migrated)

        logger.success("修复完成: {} 组合并, {} 行迁移。", len(plans), total_migrated)
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("repair_identity 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
