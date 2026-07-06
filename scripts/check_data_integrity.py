import os
import sys
import time
import argparse
from datetime import date, timedelta

from loguru import logger
from sqlalchemy import func, text

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
    parser.add_argument("--window-days", type=int, default=730,
                        help="缺口/OHLC/跳变检查回看的天数 (默认: 730，即近 2 年)")
    parser.add_argument("--jump-threshold", type=float, default=0.5,
                        help="无事件大跳变的 |close/prev_close - 1| 阈值 (默认: 0.5)")
    parser.add_argument("--gap-min-sessions", type=int, default=3,
                        help="缺口检查中连续缺失交易日的最小会话数 (默认: 3，过滤个股正常停牌)")
    parser.add_argument("--ohlc-baseline", type=int, default=0,
                        help="全史 OHLC 包含违规的允许基线 (默认: 0，2026-07-06 repair_ohlc_violations "
                             "已全量订正)；超出即阻塞")
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


def check_ohlc_validity(session, limit: int, window_start: date) -> int:
    """OHLC 合法性：high >= low、high >= open/close、low <= open/close、价格非负、volume 非负。"""
    sql = text(
        """
        SELECT dp.security_id, s.symbol, dp.date, dp.open, dp.high, dp.low, dp.close, dp.volume
        FROM daily_prices dp
        JOIN securities s ON s.id = dp.security_id
        WHERE dp.date >= :window_start
          AND dp.open IS NOT NULL AND dp.high IS NOT NULL
          AND dp.low IS NOT NULL AND dp.close IS NOT NULL
          AND (
                dp.high < dp.low
             OR dp.high < dp.open OR dp.high < dp.close
             OR dp.low > dp.open OR dp.low > dp.close
             OR dp.open <= 0 OR dp.close <= 0 OR dp.low <= 0
             OR dp.volume < 0
          )
        ORDER BY dp.date DESC
        """
    )
    rows = session.execute(sql, {"window_start": window_start}).all()
    _report_rows(
        f"OHLC 合法性（{window_start} 起）",
        len(rows),
        [
            f"{r.symbol} {r.date} o={r.open} h={r.high} l={r.low} c={r.close} v={r.volume}"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def check_active_list_date_coverage(session, limit: int) -> int:
    """活跃 US CS/ETF 的 list_date 覆盖：NULL 数量超阈值即阻塞。

    回归防线：2026-07-06 事故——/v3/reference/tickers 列表响应不带 list_date，
    payload 把 None 原样下发导致每日 universe 同步抹掉全舰队 list_date，
    防回收 clamp 全体失效。修复在 _build_reference_payload 的 None 剥离；
    此检查保证同类回归 24 小时内变红。"""
    sql = text(
        """
        SELECT COUNT(*) AS n
        FROM securities
        WHERE is_active IS TRUE AND upper(market) = 'US'
          AND upper(type) IN ('CS', 'ETF') AND list_date IS NULL
        """
    )
    n = session.execute(sql).scalar_one()
    if n > 50:
        logger.error("活跃 US CS/ETF 中 {} 只 list_date 为 NULL（阈值 50）——疑似每日同步再度抹除。", n)
        return n
    if n:
        logger.info("活跃 US CS/ETF 中 {} 只 list_date 为 NULL（新上市未富化，正常）。", n)
    return 0


def check_ohlc_validity_full_history(session, limit: int, baseline: int) -> int:
    """全史 OHLC 包含不变量计数（windowed 版只看近窗，历史脏行会永远漏网）。

    与 baseline（repair_ohlc_violations 后已知的分钟无覆盖搁置行数）比较：
    超出即有新violated行进入，阻塞；等于或低于只作通报。
    """
    sql = text(
        """
        SELECT COUNT(*) AS n
        FROM daily_prices dp
        WHERE dp.open IS NOT NULL AND dp.high IS NOT NULL
          AND dp.low IS NOT NULL AND dp.close IS NOT NULL
          AND (dp.high < dp.low
               OR dp.high < GREATEST(dp.open, dp.close)
               OR dp.low > LEAST(dp.open, dp.close))
        """
    )
    n = session.execute(sql).scalar_one()
    if n > baseline:
        logger.error("全史 OHLC 包含违规 {} 行（> 已知搁置基线 {}），存在新增脏行。", n, baseline)
        return n - baseline
    logger.info("全史 OHLC 包含违规 {} 行（<= 搁置基线 {}，均为分钟无覆盖的历史遗留）。", n, baseline)
    return 0


def check_vwap_containment(session, limit: int, window_start: date) -> int:
    """近窗 vwap 越界率通报（vwap 不在 [low, high]）：Massive 时代含盘前盘后成交，
    越界属口径而非脏数据（评估实测 2025-03 为 4.2%），只报率不阻塞；
    但越界率突增（>10%）视为 vendor 侧异常，阻塞。"""
    sql = text(
        """
        SELECT COUNT(*) FILTER (WHERE vwap < low OR vwap > high) AS out_n, COUNT(*) AS n
        FROM daily_prices
        WHERE date >= :window_start AND vwap IS NOT NULL
        """
    )
    row = session.execute(sql, {"window_start": window_start}).one()
    if not row.n:
        return 0
    rate = row.out_n / row.n
    if rate > 0.10:
        logger.error("近窗 vwap 越界率 {:.2%}（{}/{}），远超盘后口径正常水平，疑 vendor 异常。",
                     rate, row.out_n, row.n)
        return row.out_n
    logger.info("近窗 vwap 越界率 {:.2%}（{}/{}，盘前盘后成交口径所致，正常）。", rate, row.out_n, row.n)
    return 0


def check_calendar_gaps(session, limit: int, window_start: date, min_sessions: int) -> int:
    """交易日缺口：在证券自身价格覆盖范围内，对照 XNYS 日历找连续缺失 >= min_sessions 的区间。

    只检查活跃 CS/ETF；按"每证券实际 [max(min_date, window_start), max_date] 区间"对照，
    避免把上市前/退市后误报为缺口。短缺口（< min_sessions）多为个股停牌，默认忽略。
        """
    sql = text(
        """
        WITH scope AS (
            SELECT s.id AS security_id, s.symbol,
                   GREATEST(MIN(dp.date), CAST(:window_start AS date)) AS check_start,
                   MAX(dp.date) AS check_end
            FROM securities s
            JOIN daily_prices dp ON dp.security_id = s.id
            WHERE s.is_active IS TRUE
              AND upper(s.type) IN ('CS', 'ETF')
              AND upper(s.market) = 'US'
            GROUP BY s.id, s.symbol
            HAVING MAX(dp.date) >= CAST(:window_start AS date)
        ),
        missing AS (
            SELECT sc.security_id, sc.symbol, tc.trade_date,
                   tc.trade_date
                   - LAG(tc.trade_date) OVER (PARTITION BY sc.security_id ORDER BY tc.trade_date) AS prev_gap
            FROM scope sc
            JOIN trading_calendars tc
              ON tc.exchange_mic = 'XNYS'
             AND tc.is_open IS TRUE
             AND tc.trade_date BETWEEN sc.check_start AND sc.check_end
            LEFT JOIN daily_prices dp
              ON dp.security_id = sc.security_id AND dp.date = tc.trade_date
            WHERE dp.security_id IS NULL
        ),
        runs AS (
            SELECT security_id, symbol, trade_date,
                   SUM(CASE WHEN prev_gap IS NULL OR prev_gap > 7 THEN 1 ELSE 0 END)
                       OVER (PARTITION BY security_id ORDER BY trade_date) AS run_id
            FROM missing
        )
        SELECT security_id, symbol, MIN(trade_date) AS gap_start, MAX(trade_date) AS gap_end,
               COUNT(*) AS missing_sessions
        FROM runs
        GROUP BY security_id, symbol, run_id
        HAVING COUNT(*) >= :min_sessions
        ORDER BY missing_sessions DESC, symbol
        """
    )
    rows = session.execute(sql, {"window_start": window_start, "min_sessions": min_sessions}).all()
    _report_rows(
        f"交易日缺口（{window_start} 起，连续缺失 >= {min_sessions} 个交易日）",
        len(rows),
        [
            f"{r.symbol} {r.gap_start} -> {r.gap_end} 缺 {r.missing_sessions} 个交易日"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def check_split_jump_consistency(session, limit: int, window_start: date) -> int:
    """拆股事件 vs 价格跳变互验：ex_date 当日 close/prev_close 应接近 split_from/split_to。

    比值偏离预期超过 25% 视为可疑（正常波动叠加拆股的容差）。
    """
    sql = text(
        """
        WITH splits AS (
            SELECT ca.security_id, s.symbol, ca.ex_date,
                   ca.split_from, ca.split_to,
                   (ca.split_from / ca.split_to) AS expected_ratio
            FROM corporate_actions ca
            JOIN securities s ON s.id = ca.security_id
            WHERE ca.action_type = 'SPLIT'
              AND ca.ex_date >= :window_start
              AND ca.split_from > 0 AND ca.split_to > 0
              AND s.is_active IS TRUE
        ),
        with_prices AS (
            SELECT sp.*, dp.close AS ex_close,
                   (SELECT dp2.close FROM daily_prices dp2
                    WHERE dp2.security_id = sp.security_id AND dp2.date < sp.ex_date
                      AND dp2.close IS NOT NULL
                    ORDER BY dp2.date DESC LIMIT 1) AS prev_close
            FROM splits sp
            LEFT JOIN daily_prices dp
              ON dp.security_id = sp.security_id AND dp.date = sp.ex_date
        )
        SELECT security_id, symbol, ex_date, split_from, split_to,
               expected_ratio, prev_close, ex_close,
               (ex_close / NULLIF(prev_close, 0)) AS actual_ratio
        FROM with_prices
        WHERE prev_close IS NOT NULL AND ex_close IS NOT NULL
          AND ABS((ex_close / NULLIF(prev_close, 0)) / expected_ratio - 1) > 0.25
        ORDER BY ex_date DESC
        """
    )
    rows = session.execute(sql, {"window_start": window_start}).all()
    _report_rows(
        f"拆股日价格跳变与事件不符（{window_start} 起）",
        len(rows),
        [
            f"{r.symbol} {r.ex_date} {r.split_from}:{r.split_to} 预期比值={float(r.expected_ratio):.4f} "
            f"实际={float(r.actual_ratio):.4f} (prev={r.prev_close} ex={r.ex_close})"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def check_unexplained_jumps(session, limit: int, window_start: date, threshold: float) -> int:
    """无事件大跳变预警：|close/prev_close - 1| > threshold 且 ±1 个事实日内无拆股/分红事件。

    只看活跃 CS/ETF、prev_close >= 1 美元（仙股噪声太大）。结果是"预警"而非"错误"。
    """
    sql = text(
        """
        WITH moves AS (
            SELECT dp.security_id, s.symbol, dp.date, dp.close,
                   LAG(dp.close) OVER (PARTITION BY dp.security_id ORDER BY dp.date) AS prev_close,
                   LAG(dp.date) OVER (PARTITION BY dp.security_id ORDER BY dp.date) AS prev_date
            FROM daily_prices dp
            JOIN securities s ON s.id = dp.security_id
            WHERE dp.date >= :window_start
              AND dp.close IS NOT NULL
              AND s.is_active IS TRUE
              AND upper(s.type) IN ('CS', 'ETF')
              AND upper(s.market) = 'US'
        )
        SELECT m.security_id, m.symbol, m.date, m.prev_close, m.close,
               (m.close / NULLIF(m.prev_close, 0) - 1) AS pct_change
        FROM moves m
        WHERE m.prev_close >= 1
          AND ABS(m.close / NULLIF(m.prev_close, 0) - 1) > :threshold
          AND NOT EXISTS (
              SELECT 1 FROM corporate_actions ca
              WHERE ca.security_id = m.security_id
                AND ca.ex_date BETWEEN m.prev_date AND m.date + 1
          )
        ORDER BY ABS(m.close / NULLIF(m.prev_close, 0) - 1) DESC
        """
    )
    rows = session.execute(sql, {"window_start": window_start, "threshold": threshold}).all()
    if not rows:
        logger.success(f"✅ 无事件大跳变预警（{window_start} 起，阈值 {threshold:.0%}）: OK")
        return 0
    logger.warning(
        f"⚠️ 无事件大跳变预警（{window_start} 起，阈值 {threshold:.0%}）: {len(rows)} 条"
        f"（预警，需人工甄别：可能是真实暴涨暴跌、数据错误或漏录事件）"
    )
    for r in rows[:limit]:
        logger.warning(
            f"  - {r.symbol} {r.date} {r.prev_close} -> {r.close} ({float(r.pct_change):+.1%})"
        )
    return 0  # 预警不计入失败退出码


def check_recycled_symbol_overlap(session, limit: int, window_start: date) -> int:
    """同一 symbol 挂多个 security 且日线区间重叠：ticker 回收串写（阻塞）。

    死票回收后新证券按 symbol 回填会吞掉旧身份的历史（2026-07 gogl/lazr/pinc/
    spcx/opi/fusd 事故）。正常状态下新旧两行的 bar 区间必须不相交。
    只报重叠段延伸到 window_start 之后的对，避免每天重复翻出陈年遗留。
    """
    sql = text(
        """
        WITH dup AS (
            SELECT symbol FROM securities GROUP BY symbol HAVING COUNT(*) > 1
        ),
        spans AS (
            SELECT s.id, s.symbol, MIN(dp.date) AS first_bar, MAX(dp.date) AS last_bar
            FROM securities s
            JOIN dup USING (symbol)
            JOIN daily_prices dp ON dp.security_id = s.id
            GROUP BY s.id, s.symbol
        )
        SELECT a.symbol, a.id AS id_a, b.id AS id_b,
               GREATEST(a.first_bar, b.first_bar) AS overlap_start,
               LEAST(a.last_bar, b.last_bar) AS overlap_end
        FROM spans a
        JOIN spans b ON b.symbol = a.symbol AND b.id > a.id
        WHERE a.first_bar <= b.last_bar AND b.first_bar <= a.last_bar
          AND LEAST(a.last_bar, b.last_bar) >= :window_start
        ORDER BY LEAST(a.last_bar, b.last_bar) DESC
        """
    )
    rows = session.execute(sql, {"window_start": window_start}).all()
    _report_rows(
        f"同 symbol 多证券日线区间重叠（重叠延伸至 {window_start} 后，疑似 ticker 回收串写）",
        len(rows),
        [
            f"{r.symbol} sec={r.id_a} vs sec={r.id_b} 重叠 {r.overlap_start} -> {r.overlap_end}"
            for r in rows
        ],
        limit,
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            window_start = date.today() - timedelta(days=args.window_days)
            issues = 0
            issues += check_price_latest_date_consistency(session, limit=args.limit)
            issues += check_symbol_normalization(session, limit=args.limit)
            issues += check_ohlc_validity(session, limit=args.limit, window_start=window_start)
            issues += check_ohlc_validity_full_history(session, limit=args.limit, baseline=args.ohlc_baseline)
            issues += check_vwap_containment(session, limit=args.limit, window_start=window_start)
            issues += check_active_list_date_coverage(session, limit=args.limit)
            issues += check_recycled_symbol_overlap(session, limit=args.limit, window_start=window_start)
            check_calendar_gaps(
                session, limit=args.limit, window_start=window_start, min_sessions=args.gap_min_sessions
            )
            check_split_jump_consistency(session, limit=args.limit, window_start=window_start)
            check_unexplained_jumps(
                session, limit=args.limit, window_start=window_start, threshold=args.jump_threshold
            )

            if issues > 0:
                logger.error("发现 {} 项阻塞性数据一致性问题。", issues)
                return 2
            logger.success("数据一致性检查通过。")
            return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("check_data_integrity 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
