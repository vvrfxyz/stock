"""存量证券身份对账（只读）：找出 C1 修复无法回溯纠正的历史合并/分裂。

C1（active-only 部分唯一索引 + upsert 身份冲突跳过）只能阻止"今后"的 ticker 回收
静默合并，无法修复建库以来已经发生的：
  - 分裂：同一 composite_figi / cik 落在多个 security_id（FB->META 类改名重复下载）；
  - 回收：同一 symbol/current_symbol 多行且 FIGI/CIK 不一致（旧退市股 + 新公司复用代码）；
  - 映射歧义：同一 CUSIP/CIK/FIGI 身份值映射到多个 security_id（13F/FTD 回填会写错身份）；
  - 可重连的 symbol 历史：security_symbol_history 里的旧 symbol 如今属于另一个活跃 security_id。

输出是人工甄别清单，不写库。发现问题时退出码非零，便于接调度告警。
建议在提高 sync_massive_universe 频率前先跑一次，决定是否需要人工拆分/合并 security_id。
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
from utils.script_logging import setup_logging as configure_script_logging


def setup_logging():
    configure_script_logging("audit_security_identity")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="存量证券身份对账（只读）：检测已发生的合并/分裂/映射歧义。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=30, help="每项检查最多输出的样例数量 (默认: 30)")
    parser.add_argument(
        "--id-types",
        type=str,
        default="CUSIP,CIK,FIGI",
        help="检查映射歧义的 security_identifiers.id_type，逗号分隔 (默认: CUSIP,CIK,FIGI)",
    )
    return parser


def _report_rows(title: str, total_count: int, rows: list[str], limit: int) -> None:
    if total_count <= 0:
        logger.success(f"✅ {title}: OK")
        return
    logger.warning(f"⚠️ {title}: 发现 {total_count} 组（展示前 {min(limit, len(rows))} 组）")
    for row in rows[:limit]:
        logger.warning(f"  - {row}")


def check_shared_identity_column(session, column: str, limit: int) -> int:
    """同一 composite_figi 或 cik 落在多个 security_id —— 疑似身份分裂。

    只看非空值；活跃 + 退市都算（改名后旧行常被误标 inactive，正是分裂的一半）。
    """
    sql = text(
        f"""
        SELECT {column} AS id_value,
               count(*) AS n,
               array_agg(id ORDER BY id) AS security_ids,
               array_agg(symbol ORDER BY id) AS symbols,
               array_agg(is_active ORDER BY id) AS actives
        FROM securities
        WHERE {column} IS NOT NULL AND {column} <> ''
        GROUP BY {column}
        HAVING count(*) > 1
        ORDER BY count(*) DESC, {column}
        """
    )
    rows = session.execute(sql, {}).all()
    _report_rows(
        f"同一 {column} 对应多个 security_id（疑似身份分裂）",
        len(rows),
        [
            f"{column}={r.id_value} -> ids={list(r.security_ids)} symbols={list(r.symbols)} active={list(r.actives)}"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def check_recycled_symbol(session, column: str, limit: int) -> int:
    """同一 symbol/current_symbol 多行且 FIGI/CIK 不一致 —— 疑似 ticker 回收。

    部分唯一索引允许 1 个活跃 + N 个退市同 symbol；这里专挑"身份不一致"的多行，
    把正常的同代码同公司（FIGI/CIK 一致）过滤掉。
    """
    sql = text(
        f"""
        WITH grp AS (
            SELECT {column} AS sym,
                   count(*) AS n,
                   count(DISTINCT coalesce(composite_figi, '')) FILTER (WHERE composite_figi IS NOT NULL) AS figi_variants,
                   count(DISTINCT coalesce(cik, '')) FILTER (WHERE cik IS NOT NULL) AS cik_variants,
                   array_agg(id ORDER BY id) AS security_ids,
                   array_agg(coalesce(composite_figi, '∅') ORDER BY id) AS figis,
                   array_agg(is_active ORDER BY id) AS actives
            FROM securities
            WHERE {column} IS NOT NULL AND {column} <> ''
            GROUP BY {column}
            HAVING count(*) > 1
        )
        SELECT sym, n, security_ids, figis, actives
        FROM grp
        WHERE figi_variants > 1 OR cik_variants > 1
        ORDER BY n DESC, sym
        """
    )
    rows = session.execute(sql, {}).all()
    _report_rows(
        f"同一 {column} 多行且身份(FIGI/CIK)不一致（疑似 ticker 回收）",
        len(rows),
        [
            f"{column}={r.sym} n={r.n} ids={list(r.security_ids)} figis={list(r.figis)} active={list(r.actives)}"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def check_active_symbol_collisions(session, limit: int) -> int:
    """多个活跃行共用 symbol 或 (current_symbol, exchange) —— 部分唯一索引迁移的前置障碍。

    `alembic upgrade head` 创建 active-only 唯一索引前，库内若已有这些重复会直接失败。
    单列输出便于部署前清理。
    """
    issues = 0
    for label, group_cols in (
        ("symbol", "symbol"),
        ("(current_symbol, exchange)", "current_symbol, coalesce(exchange, '')"),
    ):
        sql = text(
            f"""
            SELECT {group_cols.split(',')[0]} AS key1,
                   count(*) AS n,
                   array_agg(id ORDER BY id) AS security_ids,
                   array_agg(symbol ORDER BY id) AS symbols
            FROM securities
            WHERE is_active IS TRUE
            GROUP BY {group_cols}
            HAVING count(*) > 1
            ORDER BY count(*) DESC
            """
        )
        rows = session.execute(sql, {}).all()
        issues += len(rows)
        _report_rows(
            f"活跃行 {label} 重复（阻塞 active-only 唯一索引迁移）",
            len(rows),
            [f"key={r.key1} ids={list(r.security_ids)} symbols={list(r.symbols)}" for r in rows],
            limit,
        )
    return issues


def check_ambiguous_identifier_map(session, id_types: list[str], limit: int) -> int:
    """同一 (id_type, id_value) 映射到多个 security_id —— CUSIP/CIK/FIGI 身份歧义。

    13F/FTD CUSIP 回填、SEC CIK 映射都按 id_value join，歧义会写错身份。
    与 map_unlinked_holdings_to_securities 的 HAVING count(DISTINCT)=1 守卫同口径。
    """
    sql = text(
        """
        SELECT id_type, id_value,
               count(DISTINCT security_id) AS n,
               array_agg(DISTINCT security_id) AS security_ids
        FROM security_identifiers
        WHERE id_type = ANY(:id_types)
        GROUP BY id_type, id_value
        HAVING count(DISTINCT security_id) > 1
        ORDER BY count(DISTINCT security_id) DESC, id_type, id_value
        """
    )
    rows = session.execute(sql, {"id_types": id_types}).all()
    _report_rows(
        "同一身份值映射到多个 security_id（CUSIP/CIK/FIGI 歧义）",
        len(rows),
        [f"{r.id_type}={r.id_value} -> ids={list(r.security_ids)}" for r in rows],
        limit,
    )
    return len(rows)


def check_symbol_history_reconnect(session, limit: int) -> int:
    """security_symbol_history 里的旧 symbol 如今是另一个活跃 security_id 的当前代码。

    这是 split identity 可重连的直接信号：改名前的历史指向 old id，而该 symbol 现在
    挂在另一个（新建的）活跃 id 上，两者很可能是同一证券。
    """
    sql = text(
        """
        SELECT h.security_id AS history_security_id,
               h.symbol AS hist_symbol,
               s.id AS current_active_security_id,
               s.symbol AS current_symbol
        FROM security_symbol_history h
        JOIN securities s
          ON lower(s.symbol) = lower(h.symbol)
         AND s.is_active IS TRUE
         AND s.id <> h.security_id
        GROUP BY h.security_id, h.symbol, s.id, s.symbol
        ORDER BY h.security_id
        """
    )
    rows = session.execute(sql, {}).all()
    _report_rows(
        "symbol 历史指向旧 id，但该代码现属另一活跃 id（疑似可重连的分裂）",
        len(rows),
        [
            f"hist_id={r.history_security_id} hist_symbol={r.hist_symbol} "
            f"-> 现活跃 id={r.current_active_security_id} symbol={r.current_symbol}"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)
    id_types = [item.strip().upper() for item in args.id_types.split(",") if item.strip()]

    db_manager = None
    try:
        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            blocking = 0  # 阻塞迁移/部署的硬问题
            advisory = 0  # 需人工甄别但不阻塞

            # 部署前最关键：active-only 唯一索引迁移会被这些重复挡住
            blocking += check_active_symbol_collisions(session, limit=args.limit)

            advisory += check_shared_identity_column(session, "composite_figi", limit=args.limit)
            advisory += check_shared_identity_column(session, "cik", limit=args.limit)
            advisory += check_recycled_symbol(session, "symbol", limit=args.limit)
            advisory += check_recycled_symbol(session, "current_symbol", limit=args.limit)
            advisory += check_ambiguous_identifier_map(session, id_types, limit=args.limit)
            advisory += check_symbol_history_reconnect(session, limit=args.limit)

            logger.info("--- 身份对账汇总 ---")
            logger.info("  阻塞迁移的活跃行重复: {} 组", blocking)
            logger.info("  需人工甄别的身份异常: {} 组", advisory)
            logger.info("--------------------")

            if blocking > 0:
                logger.error(
                    "存在阻塞 active-only 唯一索引迁移的活跃行重复，"
                    "请先人工处理再 `alembic upgrade head`。"
                )
                return 2
            if advisory > 0:
                logger.warning("发现需人工甄别的存量身份异常（不阻塞），详见上方样例。")
                return 1
            logger.success("🎉 未发现存量身份异常。")
            return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("audit_security_identity 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
