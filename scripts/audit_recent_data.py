"""近 N 天数据正确性抽样审计（默认 2 年窗口）。

分层抽样（大市值 + 窗口内有拆股 + 窗口内有分红 + 随机），对每支证券做三类核对：

A. raw 对账   : Massive adjusted=false 日线 vs daily_prices —— close/volume 应精确一致，
                日期覆盖双向核对（vendor 有而库缺 = 缺数据；库有而 vendor 无 = 多数据/来源差异）。
B. 拆股复权对账: Massive adjusted=true close vs (库内 raw close × 由 corporate_actions
                独立重算的 split-only 因子) —— Massive 日线的 adjusted 仅做拆股调整，
                这条同时验证价格、拆股事件和因子方向三者互洽。
C. 读取层自检 : utils.adjusted_prices 全因子序列在拆股日不应出现断崖（|跳变|>40% 视为异常）。

结果写入 markdown 报告（默认 docs/audits/<date>-recent-data-audit.md）。
消耗 API 配额：每证券 2 次请求（raw + adjusted）。
"""
import argparse
import os
import random
import sys
import time
from datetime import date, timedelta
from decimal import Decimal

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.adjusted_prices import get_adjusted_daily_bars
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    get_massive_api_keys,
)
from utils.script_logging import setup_logging as configure_script_logging
from utils.trading_calendar import get_last_completed_trading_date

RAW_CLOSE_TOL = Decimal("0.000001")
ADJ_REL_TOL = Decimal("0.001")  # vendor adjusted 值有舍入，0.1% 容忍


def setup_logging():
    configure_script_logging("audit_recent_data")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="近 N 天数据正确性抽样审计。")
    parser.add_argument("symbols", nargs="*", help="显式指定要审计的 symbol；为空则分层抽样。")
    parser.add_argument("--sample-size", type=int, default=32, help="抽样总数（默认 32）。")
    parser.add_argument("--window-days", type=int, default=730, help="审计窗口天数（默认 730）。")
    parser.add_argument("--seed", type=int, default=20260611, help="随机种子，保证可复现。")
    parser.add_argument("--output", type=str, default=None, help="报告输出路径。")
    return parser


def pick_sample(db_manager: DatabaseManager, sample_size: int, window_start: date, seed: int) -> list[str]:
    quota = max(sample_size // 4, 1)
    with db_manager.engine.connect() as conn:
        # 有意 CS-only：大市值分层按普通股口径抽样，不随白名单类型扩展
        mega = [r[0] for r in conn.execute(text(
            """SELECT symbol FROM securities
               WHERE is_active AND upper(type)='CS' AND upper(market)='US' AND market_cap IS NOT NULL
               ORDER BY market_cap DESC LIMIT :n"""), {"n": quota})]
        splitters = [r[0] for r in conn.execute(text(
            """SELECT DISTINCT s.symbol FROM securities s
               JOIN corporate_actions ca ON ca.security_id = s.id
               WHERE s.is_active AND upper(s.type) = ANY(:allowed_types) AND upper(s.market)='US'
                 AND ca.action_type='SPLIT' AND ca.ex_date >= :ws
               ORDER BY s.symbol LIMIT :n"""),
            {"ws": window_start, "n": quota * 3, "allowed_types": list(ALLOWED_US_SECURITY_TYPES)})]
        payers = [r[0] for r in conn.execute(text(
            """SELECT DISTINCT s.symbol FROM securities s
               JOIN corporate_actions ca ON ca.security_id = s.id
               WHERE s.is_active AND upper(s.type) = ANY(:allowed_types) AND upper(s.market)='US'
                 AND ca.action_type='DIVIDEND' AND ca.ex_date >= :ws
               ORDER BY s.symbol LIMIT :n"""),
            {"ws": window_start, "n": quota * 3, "allowed_types": list(ALLOWED_US_SECURITY_TYPES)})]
        universe = [r[0] for r in conn.execute(text(
            """SELECT symbol FROM securities
               WHERE is_active AND upper(type) = ANY(:allowed_types) AND upper(market)='US'
               ORDER BY symbol"""), {"allowed_types": list(ALLOWED_US_SECURITY_TYPES)})]

    rng = random.Random(seed)
    selected: list[str] = []
    for pool, n in ((mega, quota), (rng.sample(splitters, min(quota, len(splitters))), quota),
                    (rng.sample(payers, min(quota, len(payers))), quota)):
        for symbol in pool[:n]:
            if symbol not in selected:
                selected.append(symbol)
    while len(selected) < sample_size and universe:
        candidate = rng.choice(universe)
        if candidate not in selected:
            selected.append(candidate)
    return selected[:sample_size]


def load_db_bars(db_manager: DatabaseManager, symbol: str, window_start: date) -> tuple[int | None, dict[date, dict]]:
    with db_manager.engine.connect() as conn:
        row = conn.execute(text("SELECT id FROM securities WHERE symbol = :s"), {"s": symbol}).first()
        if not row:
            return None, {}
        security_id = row[0]
        bars = conn.execute(text(
            """SELECT date, close, volume FROM daily_prices
               WHERE security_id = :sid AND date >= :ws ORDER BY date"""),
            {"sid": security_id, "ws": window_start}).all()
    return security_id, {r.date: {"close": r.close, "volume": r.volume} for r in bars}


def load_split_events(db_manager: DatabaseManager, security_id: int) -> list[tuple[date, Decimal]]:
    """split-only 单事件因子 (ex_date, split_from/split_to)，独立于 computed_adjustment_factors 重算。

    必须做经济去重：迁移遗留的 POLYGON 行与 MASSIVE 行可能记录同一事件
    （唯一约束含 source），不去重会把同一拆股连乘两次。
    """
    with db_manager.engine.connect() as conn:
        rows = conn.execute(text(
            """SELECT DISTINCT ex_date, split_from, split_to FROM corporate_actions
               WHERE security_id = :sid AND action_type='SPLIT'
                 AND split_from > 0 AND split_to > 0
               ORDER BY ex_date"""), {"sid": security_id}).all()
    return [(r.ex_date, Decimal(r.split_from) / Decimal(r.split_to)) for r in rows]


def split_factor_for(events: list[tuple[date, Decimal]], bar_date: date) -> Decimal:
    factor = Decimal("1")
    for ex_date, single in events:
        if ex_date > bar_date:
            factor *= single
    return factor


def audit_symbol(symbol: str, source: MassiveSource, db_manager: DatabaseManager,
                 window_start: date, end_date: date) -> dict:
    result = {"symbol": symbol, "status": "OK", "notes": []}
    security_id, db_bars = load_db_bars(db_manager, symbol, window_start)
    if security_id is None:
        result.update(status="SKIP", notes=["库中无此 symbol"])
        return result

    raw_df = source.get_historical_data(symbol, start=window_start.isoformat(), end=end_date.isoformat(), adjusted=False)
    adj_df = source.get_historical_data(symbol, start=window_start.isoformat(), end=end_date.isoformat(), adjusted=True)
    if raw_df.empty:
        result.update(status="SKIP", notes=["vendor 未返回 raw 日线"])
        return result

    # --- A. raw 对账 ---
    vendor_raw = {idx: row for idx, row in raw_df.iterrows()}
    vendor_dates, db_dates = set(vendor_raw), set(db_bars)
    missing_in_db = sorted(vendor_dates - db_dates)
    extra_in_db = sorted(d for d in db_dates - vendor_dates if d >= window_start)
    close_mismatches, volume_mismatches, max_close_diff = [], [], Decimal("0")
    for d in sorted(vendor_dates & db_dates):
        v_close, b_close = Decimal(str(vendor_raw[d]["Close"])), Decimal(db_bars[d]["close"])
        diff = abs(v_close - b_close)
        max_close_diff = max(max_close_diff, diff)
        if diff > RAW_CLOSE_TOL:
            close_mismatches.append((d, b_close, v_close))
        v_vol, b_vol = vendor_raw[d]["Volume"], db_bars[d]["volume"]
        if v_vol is not None and b_vol is not None and int(v_vol) != int(b_vol):
            volume_mismatches.append((d, b_vol, int(v_vol)))

    result.update(
        overlap_days=len(vendor_dates & db_dates),
        missing_in_db=len(missing_in_db), extra_in_db=len(extra_in_db),
        raw_close_mismatch=len(close_mismatches), raw_volume_mismatch=len(volume_mismatches),
        max_raw_close_diff=str(max_close_diff),
    )
    if missing_in_db:
        result["notes"].append(f"vendor 有而库缺 {len(missing_in_db)} 天 (如 {missing_in_db[:3]})")
    if close_mismatches:
        result["notes"].append(f"raw close 不一致 {len(close_mismatches)} 天 (如 {close_mismatches[:2]})")
    if volume_mismatches:
        result["notes"].append(f"volume 不一致 {len(volume_mismatches)} 天 (如 {volume_mismatches[:2]})")

    # --- B. 拆股复权对账（vendor adjusted 为 split-only 口径）---
    adj_mismatch, max_adj_rel = 0, Decimal("0")
    if not adj_df.empty:
        split_events = load_split_events(db_manager, security_id)
        vendor_adj = {idx: row for idx, row in adj_df.iterrows()}
        for d in sorted(set(vendor_adj) & db_dates):
            expected = Decimal(db_bars[d]["close"]) * split_factor_for(split_events, d)
            actual = Decimal(str(vendor_adj[d]["Close"]))
            if expected <= 0:
                continue
            rel = abs(actual - expected) / expected
            max_adj_rel = max(max_adj_rel, rel)
            if rel > ADJ_REL_TOL:
                adj_mismatch += 1
        result.update(split_adj_mismatch=adj_mismatch, max_split_adj_rel=f"{max_adj_rel:.6f}",
                      split_events_in_db=len([e for e in split_events if e[0] >= window_start]))
        if adj_mismatch:
            result["notes"].append(f"split-adjusted 偏差 > {ADJ_REL_TOL} 共 {adj_mismatch} 天")

    # --- C. 读取层自检（全因子序列拆股日连续性）---
    with db_manager.get_session() as session:
        adjusted_bars = get_adjusted_daily_bars(session, security_id, start=window_start, end=end_date)
    discontinuities = 0
    for prev, curr in zip(adjusted_bars, adjusted_bars[1:]):
        if prev.close and curr.close and prev.adjustment_factor != curr.adjustment_factor:
            jump = abs(curr.close / prev.close - 1)
            if jump > Decimal("0.4"):
                discontinuities += 1
                result["notes"].append(f"读取层在 {curr.date} 因子切换处跳变 {jump:.1%}")
    result["reader_discontinuities"] = discontinuities

    if result["notes"]:
        result["status"] = "ISSUES"
    return result


def write_report(results: list[dict], output_path: str, window_start: date, end_date: date, elapsed: float):
    ok = [r for r in results if r["status"] == "OK"]
    issues = [r for r in results if r["status"] == "ISSUES"]
    skipped = [r for r in results if r["status"] == "SKIP"]
    lines = [
        f"# 近 2 年数据抽样审计报告",
        "",
        f"- 审计窗口: {window_start} ~ {end_date}",
        f"- 生成时间: {date.today().isoformat()}，耗时 {elapsed:.0f}s",
        f"- 样本: {len(results)} 支（OK {len(ok)} / 有发现 {len(issues)} / 跳过 {len(skipped)}）",
        "",
        "核对口径: A) vendor raw vs daily_prices 精确对账; B) vendor split-adjusted vs",
        "corporate_actions 独立重算的 split-only 因子 (容忍 0.1%); C) 读取层全因子序列拆股日连续性。",
        "",
        "| symbol | 状态 | 重叠天数 | 库缺 | 库多 | rawClose不符 | volume不符 | splitAdj不符 | 读取层断点 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in sorted(results, key=lambda x: (x["status"] != "ISSUES", x["symbol"])):
        lines.append(
            f"| {r['symbol']} | {r['status']} | {r.get('overlap_days', '-')} | {r.get('missing_in_db', '-')} "
            f"| {r.get('extra_in_db', '-')} | {r.get('raw_close_mismatch', '-')} | {r.get('raw_volume_mismatch', '-')} "
            f"| {r.get('split_adj_mismatch', '-')} | {r.get('reader_discontinuities', '-')} |"
        )
    if issues or skipped:
        lines += ["", "## 发现明细", ""]
        for r in issues + skipped:
            lines.append(f"### {r['symbol']} ({r['status']})")
            lines += [f"- {note}" for note in r["notes"]]
            lines.append("")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    logger.success("报告已写入 {}", output_path)


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    source = None
    try:
        end_date = get_last_completed_trading_date("US")
        window_start = end_date - timedelta(days=args.window_days)
        db_manager = DatabaseManager()
        rate_limiter = KeyRateLimiter(get_massive_api_keys(), MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)

        symbols = [s.lower() for s in args.symbols] or pick_sample(db_manager, args.sample_size, window_start, args.seed)
        logger.info("审计 {} 支证券，窗口 {} ~ {}", len(symbols), window_start, end_date)

        results = []
        for index, symbol in enumerate(symbols, 1):
            try:
                result = audit_symbol(symbol, source, db_manager, window_start, end_date)
            except Exception as exc:
                logger.opt(exception=exc).error("[{}] 审计失败: {}", symbol, exc)
                result = {"symbol": symbol, "status": "SKIP", "notes": [f"审计异常: {exc}"]}
            results.append(result)
            logger.info("[{}/{}] {} -> {}", index, len(symbols), symbol, result["status"])

        output = args.output or os.path.join(
            project_root, "docs", "audits", f"{date.today().isoformat()}-recent-data-audit.md")
        write_report(results, output, window_start, end_date, time.monotonic() - start_time)

        issue_count = sum(1 for r in results if r["status"] == "ISSUES")
        logger.info("审计完成: OK={} ISSUES={} SKIP={}",
                    sum(1 for r in results if r["status"] == "OK"), issue_count,
                    sum(1 for r in results if r["status"] == "SKIP"))
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("audit_recent_data 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()


if __name__ == "__main__":
    raise SystemExit(main())
