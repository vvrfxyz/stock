"""清理 corporate_actions 中 POLYGON 来源的经济重复行。

背景：旧管道迁移时 POLYGON 行被保留，Massive 接管后同一经济事件（同
security/类型/ex_date/金额或拆股比）以 MASSIVE source 再次入库——唯一约束含
source，两行并存。computed_adjustment_factors 只读 MASSIVE 行，因此因子不受
影响；但任何不带 source 过滤的消费方都会把同一事件数双份。

清理原则（保守）：
1. 只删"与某条 MASSIVE 行经济字段完全一致"的 POLYGON 行（IS NOT DISTINCT FROM 逐字段比对）。
2. 同 ex_date 但金额/比例不同的不删——可能是真实的同日多笔事件，需人工甄别。
3. 没有 MASSIVE 对应行的 POLYGON 行不删——多为 730 天窗口外的唯一历史记录。
4. 删除前，把 POLYGON 行独有的 declaration/record/pay 日期回填到对应 MASSIVE 行。

默认 dry-run；--apply 才真正执行。执行前请确认已有备份
（如 logs/corporate_actions_backup_*.dump）。
"""
import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path

from loguru import logger
from sqlalchemy import text

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

# 经济等价的 POLYGON->MASSIVE 配对（每个 POLYGON 行取 id 最小的 MASSIVE 对应行）
PAIR_CTE = """
    SELECT p.id AS polygon_id, MIN(m.id) AS massive_id
    FROM corporate_actions p
    JOIN corporate_actions m
      ON upper(m.source) = 'MASSIVE'
     AND m.security_id = p.security_id
     AND m.action_type = p.action_type
     AND m.ex_date = p.ex_date
     AND m.cash_amount IS NOT DISTINCT FROM p.cash_amount
     AND m.currency IS NOT DISTINCT FROM p.currency
     AND m.split_from IS NOT DISTINCT FROM p.split_from
     AND m.split_to IS NOT DISTINCT FROM p.split_to
    WHERE upper(p.source) = 'POLYGON'
    GROUP BY p.id
"""


def setup_logging():
    configure_script_logging("cleanup_polygon_duplicate_actions")


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = argparse.ArgumentParser(description="清理 POLYGON 来源的经济重复公司行动行。")
    parser.add_argument("--apply", action="store_true", help="真正执行删除；默认 dry-run。")
    parser.add_argument("--sample-size", type=int, default=10, help="dry-run 展示的样例数。")
    args = parser.parse_args()

    db_manager = None
    try:
        db_manager = DatabaseManager()
        with db_manager.engine.connect() as conn:
            dup_count = conn.execute(text(f"SELECT count(*) FROM ({PAIR_CTE}) t")).scalar()
            backfill_count = conn.execute(text(f"""
                SELECT count(*) FROM ({PAIR_CTE}) pair
                JOIN corporate_actions p ON p.id = pair.polygon_id
                JOIN corporate_actions m ON m.id = pair.massive_id
                WHERE (p.declaration_date IS NOT NULL AND m.declaration_date IS NULL)
                   OR (p.record_date IS NOT NULL AND m.record_date IS NULL)
                   OR (p.pay_date IS NOT NULL AND m.pay_date IS NULL)
            """)).scalar()
            conflict_count = conn.execute(text(f"""
                SELECT count(*) FROM corporate_actions p
                WHERE upper(p.source) = 'POLYGON'
                  AND EXISTS (
                    SELECT 1 FROM corporate_actions m
                    WHERE upper(m.source) = 'MASSIVE' AND m.security_id = p.security_id
                      AND m.action_type = p.action_type AND m.ex_date = p.ex_date)
                  AND p.id NOT IN (SELECT polygon_id FROM ({PAIR_CTE}) pr)
            """)).scalar()

            logger.info("经济重复的 POLYGON 行: {} 条（将删除）", dup_count)
            logger.info("删除前需向 MASSIVE 行回填日期字段的: {} 条", backfill_count)
            logger.info("同日同类型但金额/比例不同的 POLYGON 行: {} 条（保留，人工甄别）", conflict_count)

            samples = conn.execute(text(f"""
                SELECT s.symbol, p.action_type, p.ex_date, p.cash_amount, p.split_from, p.split_to
                FROM ({PAIR_CTE}) pair
                JOIN corporate_actions p ON p.id = pair.polygon_id
                JOIN securities s ON s.id = p.security_id
                ORDER BY p.ex_date DESC LIMIT :n
            """), {"n": args.sample_size}).all()
            for r in samples:
                logger.info("  样例: {} {} {} cash={} split={}:{}", r.symbol, r.action_type, r.ex_date,
                            r.cash_amount, r.split_from, r.split_to)

            if not args.apply:
                logger.warning("dry-run 结束；加 --apply 执行。注意确认已有 corporate_actions 备份。")
                return 0

            # 1) 回填 MASSIVE 行缺失的日期字段
            backfilled = conn.execute(text(f"""
                UPDATE corporate_actions m
                SET declaration_date = COALESCE(m.declaration_date, p.declaration_date),
                    record_date      = COALESCE(m.record_date, p.record_date),
                    pay_date         = COALESCE(m.pay_date, p.pay_date),
                    updated_at       = now()
                FROM ({PAIR_CTE}) pair
                JOIN corporate_actions p ON p.id = pair.polygon_id
                WHERE m.id = pair.massive_id
                  AND ((p.declaration_date IS NOT NULL AND m.declaration_date IS NULL)
                    OR (p.record_date IS NOT NULL AND m.record_date IS NULL)
                    OR (p.pay_date IS NOT NULL AND m.pay_date IS NULL))
            """)).rowcount
            # 2) 删除重复的 POLYGON 行
            deleted = conn.execute(text(f"""
                DELETE FROM corporate_actions
                WHERE id IN (SELECT polygon_id FROM ({PAIR_CTE}) t)
            """)).rowcount
            conn.commit()
            logger.success("回填 MASSIVE 行 {} 条；删除 POLYGON 重复行 {} 条。", backfilled, deleted)

            remaining = conn.execute(text(
                "SELECT count(*) FROM corporate_actions WHERE upper(source)='POLYGON'")).scalar()
            logger.info("POLYGON 余量（窗口外历史 + 同日异值待甄别）: {} 条", remaining)
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("cleanup_polygon_duplicate_actions 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
