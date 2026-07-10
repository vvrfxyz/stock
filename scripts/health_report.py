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
from utils.massive_config import ALLOWED_US_SECURITY_TYPES
from utils.script_logging import setup_logging as configure_script_logging
from utils.trading_calendar import get_last_completed_trading_date, shift_trading_date


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
            if n == 0:
                issues += 1
            logger.info("  {:40s}  rows={:>10,}  latest={}", table_name, n, latest or "NULL")
        except Exception as exc:
            session.rollback()
            logger.warning("  {:40s}  ERROR: {}", table_name, exc)
            issues += 1
    return issues


def report_securities_summary(session) -> None:
    """证券 universe 概况。"""
    _section("Securities Universe")
    # 有意 CS-only/ETF-only 的 KPI 口径：按类型分列的计数是固定看板指标，不随白名单扩展
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

    # 同 FIGI 多个活跃 id（已合并的 inactive 旧行不算）
    row = session.execute(text("""
        SELECT count(*) FROM (
            SELECT composite_figi FROM securities
            WHERE composite_figi IS NOT NULL AND composite_figi <> ''
              AND is_active
            GROUP BY composite_figi HAVING count(*) > 1
        ) t
    """)).scalar()
    if row > 0:
        issues += row
        logger.warning("  [P0] 同 FIGI 多个活跃 security_id: {} 组", row)
    else:
        logger.info("  [P0] 同 FIGI 多个活跃 security_id: 0 (OK)")

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

    # 被 kill 的步骤不会回写 ended_at/status，永久停留 RUNNING；超过 12 小时按失败计 P1
    stuck_rows = session.execute(text("""
        SELECT task_name, run_id, started_at
        FROM pipeline_task_runs
        WHERE status = 'RUNNING'
          AND started_at < now() - interval '12 hours'
        ORDER BY started_at DESC
    """)).all()

    if not rows and not stuck_rows:
        logger.info("  无 pipeline_task_runs 记录（首次运行前正常）。")
        return 0

    latest_failures = session.execute(text("""
        WITH latest AS (
            SELECT DISTINCT ON (task_name)
                   task_name, status, error_sample, started_at
            FROM pipeline_task_runs
            WHERE created_at > now() - make_interval(days => :days)
            ORDER BY task_name, started_at DESC, id DESC
        )
        SELECT task_name, error_sample, started_at
        FROM latest
        WHERE status = 'FAILED'
        ORDER BY started_at DESC
    """), {"days": days}).all()
    latest_failed_names = {row[0] for row in latest_failures}
    issues += len(latest_failures)

    current_task = None
    for task_name, status, cnt, max_dur, last_run in rows:
        if task_name != current_task:
            current_task = task_name
            logger.info("")
        mark = "!!" if status == "FAILED" and task_name in latest_failed_names else "  "
        logger.info("{} {:40s}  {:8s}  count={:>3}  max_dur={}  last={}",
                    mark, task_name, status, cnt, max_dur or "-", last_run or "-")

    if stuck_rows:
        issues += len(stuck_rows)
        logger.warning("  [P1] 停留 RUNNING 超过 12 小时（进程疑似被 kill）: {} 条", len(stuck_rows))
        for task_name, run_id, started_at in stuck_rows[:5]:
            logger.warning("    {} [{}] started={}", task_name, run_id, started_at)

    if latest_failures:
        logger.info("")
        logger.info("  最新一次仍失败的任务:")
        for task_name, error, started in latest_failures[:5]:
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
          AND s.type = ANY(:allowed_types)
          AND s.price_data_latest_date IS NULL
    """), {"allowed_types": list(ALLOWED_US_SECURITY_TYPES)}).scalar()
    if row > 0:
        p1 += row
        logger.warning("  [P1] 活跃证券无价格数据: {} 只", row)
    else:
        logger.info("  [P1] 活跃证券无价格数据: 0 (OK)")

    return p0, p1


def _is_standard_quarter_end(d: date) -> bool:
    """标准 13F 季末：3/6/9/12 月的最后一天。"""
    return d.month in (3, 6, 9, 12) and (d + timedelta(days=1)).month != d.month


def report_institutional_holdings_completeness(session, today: date | None = None) -> int:
    """13F reporting period coverage guardrail."""
    _section("13F 持仓覆盖")
    today = today or date.today()
    p1 = 0
    # 只把标准季末 period 纳入候选，防 EDGAR 畸形 period 以 filings=1 永久占据 LIMIT 4 名额
    rows = session.execute(text("""
        SELECT period,
               count(distinct accession_number) as filings,
               count(*) as rows,
               count(*) FILTER (WHERE security_id IS NOT NULL) as mapped_rows,
               round(
                   100.0 * count(*) FILTER (WHERE security_id IS NOT NULL)
                   / nullif(count(*), 0),
                   2
               ) as mapped_pct
        FROM institutional_holdings
        WHERE period IS NOT NULL
          AND period = (date_trunc('quarter', period) + interval '3 months' - interval '1 day')::date
        GROUP BY period
        ORDER BY period DESC
        LIMIT 4
    """)).all()
    if not rows:
        logger.warning("  [P1] institutional_holdings 无 period 覆盖数据。")
        return 1

    min_filings = 1000
    min_mapped_pct = 85.0
    # 13F 申报截止为 period 后 45 天，加缓冲取 60 天判定申报窗口是否已关闭
    deadline_buffer = timedelta(days=60)
    for period, filings, n_rows, mapped_rows, mapped_pct in reversed(rows):
        if not _is_standard_quarter_end(period):
            logger.info("  period={}  非标准季末（EDGAR 畸形 period），忽略", period)
            continue
        mapped_pct_value = float(mapped_pct or 0.0)
        logger.info(
            "  period={}  filings={:>6,}  rows={:>10,}  mapped={:>10,}  mapped_pct={:.2f}%",
            period,
            filings,
            n_rows,
            mapped_rows,
            mapped_pct_value,
        )
        if period + deadline_buffer >= today:
            logger.info("    在途季度（申报截止未过），仅展示不计入阈值")
            continue
        if filings < min_filings or mapped_pct_value < min_mapped_pct:
            p1 += 1
            logger.warning(
                "    [P1] 13F 覆盖低于阈值: filings={} (min={}), mapped_pct={:.2f}% (min={:.2f}%)",
                filings,
                min_filings,
                mapped_pct_value,
                min_mapped_pct,
            )
    if p1 == 0:
        logger.info("  13F 最近 reporting periods 覆盖: OK")
    return p1


def report_delisting_outcomes(session) -> int:
    """退市结局覆盖护栏（P1 warning，不阻塞）。

    退市超过 90 天仍无 delisting_events 行、或 reason_code 仍为 UNKNOWN 的证券
    数量——build_delisting_events 幂等重跑应逐步压低该值（新数据源/文档抽取迭代）。
    join 按 (security_id, delist_date) 唯一键：delist_date 修订后的残行视同缺失。
    """
    _section("退市结局覆盖 (delisting_events)")
    row = session.execute(text("""
        SELECT count(*) FILTER (WHERE de.security_id IS NULL) AS missing,
               count(*) FILTER (WHERE de.security_id IS NOT NULL
                                AND coalesce(de.reason_code, 'UNKNOWN') = 'UNKNOWN') AS unresolved
        FROM securities s
        LEFT JOIN delisting_events de
          ON de.security_id = s.id AND de.delist_date = s.delist_date
        WHERE NOT s.is_active AND s.delist_date IS NOT NULL
          AND upper(s.market) = 'US'
          AND s.delist_date < current_date - 90
    """)).one()
    missing, unresolved = row[0] or 0, row[1] or 0
    total = missing + unresolved
    if total > 0:
        logger.warning("  [P1] 退市 >90 天仍无结局归因: {} 只（无行 {} + UNKNOWN {}）",
                       total, missing, unresolved)
        logger.warning("       运行 scripts/build_delisting_events.py --apply 重建（幂等）。")
        return 1
    logger.info("  退市 >90 天的结局归因覆盖: OK")
    return 0


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
              AND type = ANY(:allowed_types)
              AND ({col} IS NULL OR {col} < now() - make_interval(days => :days))
        """), {"days": max_days, "allowed_types": list(ALLOWED_US_SECURITY_TYPES)}).scalar()
        if row > 0:
            p2 += 1
            logger.info("  [P2] {} 超过 {} 天未更新: {} 只", label, max_days, row)
        else:
            logger.info("  [P2] {} 新鲜度: OK", label)
    return p2


def report_market_data_freshness(session, market: str) -> int:
    """P1: 日线落后超过 1 session，周更分钟线落后超过 5 sessions。"""
    import requests

    from utils.clickhouse import clickhouse_request_kwargs, clickhouse_url

    _section("市场数据交易日 Freshness")
    expected = get_last_completed_trading_date(market)
    daily_minimum = shift_trading_date(market, expected, sessions=-1)
    minute_minimum = shift_trading_date(market, expected, sessions=-5)
    p1 = 0

    latest_daily = session.execute(text("SELECT max(date) FROM daily_prices")).scalar()
    if latest_daily is None or latest_daily < daily_minimum:
        p1 += 1
        logger.warning("  [P1] daily_prices latest={}，最近完整交易日={}，允许下限={}",
                       latest_daily or "NULL", expected, daily_minimum)
    else:
        logger.info("  daily_prices latest={}，最近完整交易日={} (OK)", latest_daily, expected)

    minute_sql = f"""
        SELECT maxOrNull(toDate(ts, 'America/New_York'))
        FROM stock.minute_bars
        WHERE ts >= toDateTime('{minute_minimum.isoformat()} 00:00:00', 'America/New_York')
    """
    try:
        response = requests.post(
            clickhouse_url(),
            data=minute_sql.encode(),
            timeout=30,
            **clickhouse_request_kwargs(),
        )
    except (requests.RequestException, RuntimeError) as exc:
        logger.warning("  [P1] minute_bars freshness 查询失败: {}", exc)
        return p1 + 1
    if response.status_code != 200:
        logger.warning("  [P1] minute_bars freshness 查询失败: HTTP {} {}",
                       response.status_code, response.text[:300])
        return p1 + 1
    raw = response.text.strip()
    try:
        latest_minute = None if raw in ("", "\\N", "0000-00-00") else date.fromisoformat(raw)
    except ValueError:
        logger.warning("  [P1] minute_bars freshness 返回了无效日期: {!r}", raw[:100])
        return p1 + 1
    if latest_minute is None or latest_minute < minute_minimum:
        p1 += 1
        logger.warning("  [P1] minute_bars latest={}，最近完整交易日={}，允许下限={}",
                       latest_minute or f"<{minute_minimum}", expected, minute_minimum)
    else:
        logger.info("  minute_bars latest={}，最近完整交易日={} (OK)", latest_minute, expected)
    return p1


def summarize(p0_total: int, p1_total: int, p2_total: int) -> int:
    """汇总分层计数并给出退出码。

    只有 P0（阻塞性问题）才以非零退出让 daily run 变红；P1 是 advisory 告警，
    在汇总区显著输出但退出 0——否则 pipeline_task_runs 天天记 FAILED、
    systemd OnFailure 假告警，告警疲劳会淹没真 P0（2026-07-07 调整）。
    """
    _section("汇总（按严重度分层）")
    logger.info("  P0 BLOCKING : {} 项", p0_total)
    logger.info("  P1 WARNING  : {} 项", p1_total)
    logger.info("  P2 ADVISORY : {} 项", p2_total)
    if p0_total > 0:
        logger.error("  存在 P0 阻塞性问题，需要立即处理。")
        return 1
    if p1_total > 0:
        logger.warning("  ⚠️ P1 告警汇总: {} 项待关注（advisory 不阻塞调度，exit 0；明细见上方各 section）。", p1_total)
        return 0
    logger.success("  所有检查通过。")
    return 0


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
                ("institutional_holdings_completeness", lambda: report_institutional_holdings_completeness(session)),
                ("identity_health", lambda: report_identity_health(session)),
                ("delisting_outcomes", lambda: report_delisting_outcomes(session)),
                ("pipeline_runs", lambda: report_pipeline_runs(session, args.days)),
                ("staleness", lambda: report_staleness(session)),
                ("market_data_freshness", lambda: report_market_data_freshness(session, args.market)),
            ]
            for name, fn in sections:
                try:
                    result = fn()
                    if name == "table_freshness":
                        p1_total += result
                    elif name == "price_consistency":
                        p0_total += result[0]
                        p1_total += result[1]
                    elif name == "institutional_holdings_completeness":
                        p1_total += result
                    elif name == "identity_health":
                        p0_total += result
                    elif name == "delisting_outcomes":
                        p1_total += result
                    elif name == "pipeline_runs":
                        p1_total += result
                    elif name == "staleness":
                        p2_total += result
                    elif name == "market_data_freshness":
                        p1_total += result
                except Exception as exc:
                    session.rollback()
                    logger.opt(exception=exc).warning("报告 section {} 执行失败，跳过: {}", name, exc)

        return summarize(p0_total, p1_total, p2_total)
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
