"""数据域健康报告——聚合 pipeline_task_runs、各表 freshness/coverage、身份健康摘要。

输出结构化文本报告，用于日常运维和调度排障。
"""
import argparse
import os
import sys
import time
from datetime import date, timedelta

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging


def setup_logging():
    configure_script_logging("health_report")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="数据域健康报告：freshness / coverage / pipeline 状态 / 身份健康。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--days", type=int, default=7, help="回看天数（pipeline runs 和趋势）。")
    return parser


def _section(title: str) -> None:
    logger.info("")
    logger.info("=" * 60)
    logger.info("  {}", title)
    logger.info("=" * 60)


def report_table_freshness(session) -> int:
    """各事实表的 freshness 和 coverage。"""
    _section("数据表 Freshness / Coverage")
    issues = 0
    checks = [
        ("daily_prices", "SELECT count(*) as n, max(date) as latest FROM daily_prices"),
        ("corporate_actions", "SELECT count(*) as n, max(ex_date) as latest FROM corporate_actions"),
        ("computed_adjustment_factors", "SELECT count(*) as n, max(date) as latest FROM computed_adjustment_factors"),
        ("historical_shares", "SELECT count(*) as n, max(filing_date) as latest FROM historical_shares"),
        ("short_interests", "SELECT count(*) as n, max(settlement_date) as latest FROM short_interests"),
        ("short_volumes", "SELECT count(*) as n, max(date) as latest FROM short_volumes"),
        ("sec_filings", "SELECT count(*) as n, max(filing_date) as latest FROM sec_filings"),
        ("sec_fundamental_facts", "SELECT count(*) as n, max(filed_date) as latest FROM sec_fundamental_facts"),
        ("insider_transactions", "SELECT count(*) as n, max(transaction_date) as latest FROM insider_transactions"),
        ("institutional_holdings", "SELECT count(*) as n, max(period) as latest FROM institutional_holdings"),
        ("news_articles", "SELECT count(*) as n, max(published_utc) as latest FROM news_articles"),
        ("fx_rates", "SELECT count(*) as n, max(rate_date) as latest FROM fx_rates"),
        ("security_symbol_history", "SELECT count(*) as n, max(start_date) as latest FROM security_symbol_history"),
        ("security_identity_events", "SELECT count(*) as n, max(created_at) as latest FROM security_identity_events"),
    ]
    for table_name, sql in checks:
        try:
            row = session.execute(text(sql)).one()
            n, latest = row[0], row[1]
            status = "OK" if n > 0 else "EMPTY"
            if n == 0:
                issues += 1
            logger.info("  {:40s}  rows={:>10,}  latest={}", table_name, n, latest or "NULL")
        except Exception as exc:
            session.rollback()
            logger.warning("  {:40s}  ERROR: {}", table_name, exc)
            issues += 1
            issues += 1
    return issues


def report_securities_summary(session) -> None:
    """证券 universe 概况。"""
    _section("Securities Universe")
    sql = text("""
        SELECT
            count(*) as total,
            count(*) FILTER (WHERE is_active) as active,
            count(*) FILTER (WHERE NOT is_active) as inactive,
            count(*) FILTER (WHERE is_active AND type = 'CS') as active_cs,
            count(*) FILTER (WHERE is_active AND type = 'ETF') as active_etf,
            count(*) FILTER (WHERE composite_figi IS NOT NULL) as has_figi,
            count(*) FILTER (WHERE cik IS NOT NULL AND cik <> '') as has_cik
        FROM securities
        WHERE upper(market) = 'US'
    """)
    row = session.execute(sql).one()
    logger.info("  总数: {}  活跃: {} (CS={}, ETF={})  非活跃: {}",
                row[0], row[1], row[3], row[4], row[2])
    logger.info("  有 FIGI: {}  有 CIK: {}", row[5], row[6])


def report_identity_health(session) -> int:
    """身份健康快速摘要（复用 audit 的核心查询）。"""
    _section("身份健康")
    issues = 0

    # 同 FIGI 多 id
    row = session.execute(text("""
        SELECT count(*) FROM (
            SELECT composite_figi FROM securities
            WHERE composite_figi IS NOT NULL AND composite_figi <> ''
            GROUP BY composite_figi HAVING count(*) > 1
        ) t
    """)).scalar()
    if row > 0:
        issues += row
        logger.warning("  [P0] 同 FIGI 多 security_id: {} 组", row)
    else:
        logger.info("  [P0] 同 FIGI 多 security_id: 0 (OK)")

    # 同 CIK 多 id
    row = session.execute(text("""
        SELECT count(*) FROM (
            SELECT cik FROM securities
            WHERE cik IS NOT NULL AND cik <> ''
            GROUP BY cik HAVING count(*) > 1
        ) t
    """)).scalar()
    if row > 0:
        logger.info("  同 CIK 多 security_id: {} 组（含正常 dual-class）", row)
    else:
        logger.info("  同 CIK 多 security_id: 0 (OK)")

    # 最近 identity events
    row = session.execute(text("""
        SELECT event_type, count(*) FROM security_identity_events
        WHERE created_at > now() - interval '30 days'
        GROUP BY event_type ORDER BY count(*) DESC
    """)).all()
    if row:
        for event_type, cnt in row:
            logger.info("  近 30 天 identity event: {:20s}  {}", event_type, cnt)
    else:
        logger.info("  近 30 天 identity event: 无")

    return issues


def report_pipeline_runs(session, days: int) -> int:
    """最近 N 天的 pipeline task runs 摘要。"""
    _section(f"Pipeline Task Runs（近 {days} 天）")
    issues = 0

    rows = session.execute(text("""
        SELECT task_name, status, count(*),
               max(ended_at - started_at) as max_duration,
               max(ended_at) as last_run
        FROM pipeline_task_runs
        WHERE created_at > now() - make_interval(days => :days)
        GROUP BY task_name, status
        ORDER BY task_name, status
    """), {"days": days}).all()

    if not rows:
        logger.info("  无 pipeline_task_runs 记录（首次运行前正常）。")
        return 0

    current_task = None
    for task_name, status, cnt, max_dur, last_run in rows:
        if task_name != current_task:
            current_task = task_name
            logger.info("")
        mark = "!!" if status == "FAILED" else "  "
        logger.info("{} {:40s}  {:8s}  count={:>3}  max_dur={}  last={}",
                    mark, task_name, status, cnt, max_dur or "-", last_run or "-")
        if status == "FAILED":
            issues += cnt

    # 最近失败样例
    recent_failures = session.execute(text("""
        SELECT task_name, error_sample, started_at
        FROM pipeline_task_runs
        WHERE status = 'FAILED' AND created_at > now() - make_interval(days => :days)
        ORDER BY started_at DESC
        LIMIT 5
    """), {"days": days}).all()
    if recent_failures:
        logger.info("")
        logger.info("  最近失败样例:")
        for task_name, error, started in recent_failures:
            logger.info("    {} [{}] {}", started, task_name, error or "")

    return issues


def report_price_data_consistency(session) -> tuple[int, int]:
    """价格数据一致性快速检查。返回 (p0_issues, p1_issues)。"""
    _section("价格数据一致性")
    p0 = p1 = 0

    # P0: securities.price_data_latest_date 与实际 daily_prices 不一致
    row = session.execute(text("""
        SELECT count(*) FROM securities s
        WHERE s.is_active AND s.price_data_latest_date IS NOT NULL
          AND s.price_data_latest_date <> (
              SELECT max(date) FROM daily_prices WHERE security_id = s.id
          )
    """)).scalar()
    if row > 0:
        p0 += row
        logger.warning("  [P0] price_data_latest_date 与实际不一致: {} 只", row)
    else:
        logger.info("  [P0] price_data_latest_date 一致性: OK")

    # P1: 活跃证券无价格数据
    row = session.execute(text("""
        SELECT count(*) FROM securities s
        WHERE s.is_active AND upper(s.market) = 'US'
          AND s.type IN ('CS', 'ETF')
          AND s.price_data_latest_date IS NULL
    """)).scalar()
    if row > 0:
        p1 += row
        logger.warning("  [P1] 活跃证券无价格数据: {} 只", row)
    else:
        logger.info("  [P1] 活跃证券无价格数据: 0 (OK)")

    return p0, p1


def report_staleness(session) -> int:
    """P2 advisory: 各数据域的新鲜度。"""
    _section("数据新鲜度 (P2 Advisory)")
    p2 = 0
    stale_checks = [
        ("info_last_updated_at", 30, "详情 (details)"),
        ("shares_last_updated_at", 14, "股本 (shares)"),
        ("actions_last_updated_at", 90, "公司行动 (actions)"),
        ("short_data_last_updated_at", 7, "空头数据 (short)"),
    ]
    for col, max_days, label in stale_checks:
        row = session.execute(text(f"""
            SELECT count(*) FROM securities
            WHERE is_active AND upper(market) = 'US'
              AND type IN ('CS', 'ETF')
              AND ({col} IS NULL OR {col} < now() - make_interval(days => :days))
        """), {"days": max_days}).scalar()
        if row > 0:
            p2 += 1
            logger.info("  [P2] {} 超过 {} 天未更新: {} 只", label, max_days, row)
        else:
            logger.info("  [P2] {} 新鲜度: OK", label)
    return p2


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        p0_total = p1_total = p2_total = 0
        with db_manager.get_session() as session:
            sections = [
                ("securities_summary", lambda: report_securities_summary(session)),
                ("table_freshness", lambda: report_table_freshness(session)),
                ("price_consistency", lambda: report_price_data_consistency(session)),
                ("identity_health", lambda: report_identity_health(session)),
                ("pipeline_runs", lambda: report_pipeline_runs(session, args.days)),
                ("staleness", lambda: report_staleness(session)),
            ]
            for name, fn in sections:
                try:
                    result = fn()
                    if name == "table_freshness":
                        p1_total += result
                    elif name == "price_consistency":
                        p0_total += result[0]
                        p1_total += result[1]
                    elif name == "identity_health":
                        p0_total += result
                    elif name == "pipeline_runs":
                        p1_total += result
                    elif name == "staleness":
                        p2_total += result
                except Exception as exc:
                    session.rollback()
                    logger.opt(exception=exc).warning("报告 section {} 执行失败，跳过: {}", name, exc)

        _section("汇总（按严重度分层）")
        logger.info("  P0 BLOCKING : {} 项", p0_total)
        logger.info("  P1 WARNING  : {} 项", p1_total)
        logger.info("  P2 ADVISORY : {} 项", p2_total)
        if p0_total > 0:
            logger.error("  存在 P0 阻塞性问题，需要立即处理。")
            return 2
        if p1_total > 0:
            logger.warning("  存在 P1 告警，建议关注。")
            return 1
        logger.success("  所有检查通过。")
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("health_report 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        from datetime import timedelta as td
        logger.info("耗时: {}", td(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
