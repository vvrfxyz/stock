"""OHLC 包含违规订正：用分钟数据重构的真实 high/low 修复日线脏极值。

背景（docs/data_infra_assessment_2026-07.md）：daily_prices 有 706 行violated
high >= max(open, close) AND low <= min(open, close) 的基本不变量（172 只证券，
1981-2022 分布 + NVDA 2024-06-10 拆股日脏 high 一行）。成因是 vendor 侧坏 print
（拆股日未调整的报价混入等）。三来源分布：yfinance 680 行、massive 26 行。

订正策略（保守，逐行独立判定）：
- 仅处理 2003-09-10 起且 ClickHouse 有该证券该 ET 日分钟行的日子：常规时段
  （09:30-16:00 ET）重构 minute_high = max(high)、minute_low = min(low)。
- 只有当分钟重构值本身满足包含不变量（含该行的 open/close：
  minute_high >= max(open, close, minute_low) 且 minute_low <= min(open, close)）
  时才 UPDATE high/low 两列；否则该行进报告不动——分钟数据治不了 open/close
  本身也脏的行。
- 分钟无覆盖（pre-2003 深历史、yfinance-only 证券）只报告。审计证明分钟重构
  O/H/L 与日线在健康行上位级一致（19/20），此替换与既有事实同源同口径。
- 幂等：已满足不变量的行不在候选集中。

用法（253 上）：
    python scripts/repair_ohlc_violations.py --dry-run
    python scripts/repair_ohlc_violations.py --apply
"""
import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from utils.clickhouse import clickhouse_request_kwargs, clickhouse_url
from utils.script_logging import setup_logging as configure_script_logging

MINUTE_FLOOR = date(2003, 9, 10)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用分钟数据订正日线 OHLC 包含违规。")
    parser.add_argument("--apply", action="store_true", help="执行 UPDATE；默认只报告。")
    parser.add_argument("--clamp-unrepairable", action="store_true",
                        help="分钟无覆盖的包含违规行改用保守 clamp：high:=GREATEST(open,high,close)、"
                             "low:=LEAST(open,low,close)——不发明新价格，只让边界包住已断言的成交价。")
    parser.add_argument("--scan-absurd-extremes", action="store_true",
                        help="另扫 NVDA 类离谱极值（high > 1.5*GREATEST(open,close) 或 "
                             "low < 0.5*LEAST(open,close)），用分钟重构值仲裁并订正。")
    parser.add_argument("--report", default="logs/manual_backfill/ohlc_violations_repair.tsv",
                        help="订正/搁置明细输出路径（相对项目根）。")
    return parser


def load_violations(db_manager: DatabaseManager) -> list[dict]:
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT dp.security_id, s.symbol, dp.date, dp.open, dp.high, dp.low, dp.close
            FROM daily_prices dp JOIN securities s ON s.id = dp.security_id
            WHERE dp.open IS NOT NULL AND dp.high IS NOT NULL
              AND dp.low IS NOT NULL AND dp.close IS NOT NULL
              AND (dp.high < dp.low
                   OR dp.high < GREATEST(dp.open, dp.close)
                   OR dp.low > LEAST(dp.open, dp.close))
            ORDER BY dp.date
        """)).all()
    return [dict(r._mapping) for r in rows]


def minute_extremes(security_id: int, day: date) -> tuple[float, float] | None:
    """常规时段分钟重构 (high, low)；无分钟行返回 None。"""
    sql = f"""
        SELECT max(high), min(low), count()
        FROM stock.minute_bars FINAL
        WHERE security_id = {security_id}
          AND toDate(ts, 'America/New_York') = '{day.isoformat()}'
          AND (toHour(ts, 'America/New_York') * 60 + toMinute(ts, 'America/New_York'))
              BETWEEN 570 AND 959
    """
    response = requests.post(clickhouse_url(), data=sql.encode(), timeout=120,
                             **clickhouse_request_kwargs())
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse 查询失败: {response.text[:300]}")
    high_s, low_s, n_s = response.text.strip().split("\t")
    if int(n_s) == 0:
        return None
    return float(high_s), float(low_s)


def load_absurd_extremes(db_manager: DatabaseManager) -> list[dict]:
    """NVDA 类离谱极值：边界远超 open/close 断言的成交区间（含 halts/squeeze 的真实日，
    须分钟仲裁后才动）。只扫分钟覆盖期。"""
    from sqlalchemy import text

    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT dp.security_id, s.symbol, dp.date, dp.open, dp.high, dp.low, dp.close
            FROM daily_prices dp JOIN securities s ON s.id = dp.security_id
            WHERE dp.date >= :floor
              AND dp.open > 0 AND dp.close > 0 AND dp.high > 0 AND dp.low > 0
              AND (dp.high > 1.5 * GREATEST(dp.open, dp.close)
                   OR dp.low < 0.5 * LEAST(dp.open, dp.close))
            ORDER BY dp.date
        """), {"floor": MINUTE_FLOOR}).all()
    return [dict(r._mapping) for r in rows]


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("repair_ohlc_violations")
    from dotenv import load_dotenv
    load_dotenv()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        violations = load_violations(db_manager)
        logger.info("OHLC 包含违规候选：{} 行。", len(violations))

        repaired, clamped, held = [], [], []
        for row in violations:
            day = row["date"]
            extremes = minute_extremes(row["security_id"], day) if day >= MINUTE_FLOOR else None
            if extremes is not None:
                m_high, m_low = extremes
                o, c = float(row["open"]), float(row["close"])
                if m_high >= max(o, c, m_low) and m_low <= min(o, c):
                    repaired.append({**row, "new_high": m_high, "new_low": m_low})
                    continue
            if args.clamp_unrepairable:
                o, h = float(row["open"]), float(row["high"])
                l, c = float(row["low"]), float(row["close"])
                clamped.append({**row, "new_high": max(o, h, c), "new_low": min(o, l, c)})
            else:
                reason = "pre_minute_floor" if day < MINUTE_FLOOR else "no_minute_bars"
                held.append({**row, "reason": reason})

        absurd_repaired, absurd_held = [], []
        if args.scan_absurd_extremes:
            candidates = load_absurd_extremes(db_manager)
            logger.info("离谱极值候选：{} 行（分钟仲裁中…）。", len(candidates))
            for row in candidates:
                extremes = minute_extremes(row["security_id"], row["date"])
                if extremes is None:
                    absurd_held.append({**row, "reason": "no_minute_bars"})
                    continue
                m_high, m_low = extremes
                o, c = float(row["open"]), float(row["close"])
                day_high, day_low = float(row["high"]), float(row["low"])
                # 分钟极值与日线边界差 >20% 判为脏边界；一致则是真实波动日，不动
                bad_high = day_high > 1.2 * m_high and m_high >= max(o, c)
                bad_low = day_low < 0.8 * m_low and m_low <= min(o, c)
                if bad_high or bad_low:
                    absurd_repaired.append({
                        **row,
                        "new_high": m_high if bad_high else day_high,
                        "new_low": m_low if bad_low else day_low,
                    })
                else:
                    absurd_held.append({**row, "reason": "minute_confirms_extreme"})

        to_update = repaired + clamped + absurd_repaired
        if args.apply and to_update:
            from sqlalchemy import text
            with db_manager.get_session() as session:
                for r in to_update:
                    session.execute(text("""
                        UPDATE daily_prices SET high = :h, low = :l
                        WHERE security_id = :sid AND date = :d
                    """), {"h": r["new_high"], "l": r["new_low"],
                           "sid": r["security_id"], "d": r["date"]})
                session.commit()
            logger.success("已订正 {} 行（分钟重构 {} / clamp {} / 离谱极值 {}）。",
                           len(to_update), len(repaired), len(clamped), len(absurd_repaired))
        else:
            logger.info("可订正 {} 行（分钟 {} / clamp {} / 离谱 {}）、搁置 {} 行（--apply 执行）。",
                        len(to_update), len(repaired), len(clamped), len(absurd_repaired),
                        len(held) + len(absurd_held))

        report_path = project_root / args.report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w") as f:
            f.write("action\tsymbol\tdate\topen\thigh\tlow\tclose\tnew_high\tnew_low\treason\n")
            for group, action in ((repaired, "repair_minute"), (clamped, "repair_clamp"),
                                  (absurd_repaired, "repair_absurd")):
                for r in group:
                    f.write(f"{action}\t{r['symbol']}\t{r['date']}\t{r['open']}\t{r['high']}\t"
                            f"{r['low']}\t{r['close']}\t{r['new_high']}\t{r['new_low']}\t\n")
            for r in held + absurd_held:
                f.write(f"hold\t{r['symbol']}\t{r['date']}\t{r['open']}\t{r['high']}\t{r['low']}\t"
                        f"{r['close']}\t\t\t{r['reason']}\n")
        logger.info("明细报告: {}（repair {} / hold {}）", report_path,
                    len(to_update), len(held) + len(absurd_held))
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("repair_ohlc_violations 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
