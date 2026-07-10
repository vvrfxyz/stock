"""Massive 1 分钟聚合 -> ClickHouse stock.minute_bars 增量同步（free key，30 key 限流）。

背景：分钟线归档冻结于 2026-04-23（docs/minute_vw_backfill_2026-07.md），free 档
实测放行 1 分钟聚合（HTTP 200，含盘前盘后，730 天窗口）。本脚本承接归档之后的
增量：一次补缺口（--start 2026-04-24），随后周度调度维持（50k 行/请求 ≈ 52 个
交易日，每票每周 1 个请求，10.5k 票 ≈ 70 分钟）。

口径与防护（与归档装载 import_minute_bars_clickhouse 一致）：
- 未复权原始价、UTC 时间戳、含盘前盘后；source='massive_1m' 与归档 'flatfiles_1m'
  区分血统。
- 防回收 clamp：bar 的 ET 交易日必须落在该证券 [list_date, delist_date] 内，
  否则丢弃计数（Massive 按 symbol 键控，同 update_massive_prices 的口径）。
- 零价 bar 过滤（sub-penny 下溢同类）。
- 幂等：ReplacingMergeTree(ingested_at)，重叠窗口重拉即替换（读取层 FINAL 去重）；
  绝不 DROP PARTITION——月分区里躺着归档数据。
- 选择范围：活跃 US CS/ETF + 窗口内退市的证券（退市后缺口不再增长）。

用法（253 上）：
    python scripts/update_minute_bars.py --start 2026-04-24            # 一次补缺口
    python scripts/update_minute_bars.py --lookback-days 8             # 周度增量
"""
import argparse
import os
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.clickhouse import clickhouse_request_kwargs, clickhouse_url
from utils.massive_config import ALLOWED_US_SECURITY_TYPES
from utils.massive_task import build_standard_parser, run_concurrently, run_massive_task
from utils.trading_calendar import get_last_completed_trading_date

ET = ZoneInfo("America/New_York")
CH_BATCH_ROWS = 50_000
FETCH_CHUNK_SIZE = 256
MAX_BARS_PER_CALENDAR_DAY = 2_000


def ch_insert_rows(rows: list[str]) -> None:
    import requests

    response = requests.post(
        clickhouse_url(),
        params={"query": "INSERT INTO stock.minute_bars "
                         "(security_id, ts, vendor_symbol, open, high, low, close, "
                         "volume, vwap, trade_count, source) FORMAT TabSeparated",
                "input_format_parallel_parsing": "0",
                "max_insert_threads": "1"},
        data="\n".join(rows).encode() + b"\n",
        timeout=600,
        **clickhouse_request_kwargs(),
    )
    if response.status_code != 200:
        raise RuntimeError(f"ClickHouse INSERT 失败: {response.text[:300]}")


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive API 增量同步 1 分钟线到 ClickHouse。",
        default_workers=8,
    )
    parser.add_argument("--start", default=None,
                        help="窗口起点(YYYY-MM-DD)；缺省 = 今天 - lookback-days。")
    parser.add_argument("--lookback-days", type=int, default=8,
                        help="未显式 --start 时的回看天数（默认 8，周度调度覆盖上周）。")
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    """活跃 US CS/ETF + 窗口内退市的证券；分钟同步不设 staleness 门（窗口即增量语义）。"""
    from sqlalchemy import or_

    start = _window_start(args)
    with db_manager.get_session() as session:
        query = (
            session.query(Security)
            .filter(Security.market.ilike("US"))
            .filter(Security.type.in_(ALLOWED_US_SECURITY_TYPES))
            .filter(or_(Security.is_active.is_(True), Security.delist_date >= start))
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([s.lower() for s in args.symbols]))
        securities = query.all()
    if args.limit:
        securities = securities[: args.limit]
    return securities


def _window_start(args: argparse.Namespace) -> date:
    if args.start:
        return date.fromisoformat(args.start)
    return date.today() - timedelta(days=args.lookback_days)


def process_security(
    security: Security,
    source: MassiveSource,
    db_manager: DatabaseManager,
    start: date,
    end: date,
) -> str:
    status, rows = prepare_security_rows(security, source, start, end)
    for index in range(0, len(rows), CH_BATCH_ROWS):
        ch_insert_rows(rows[index: index + CH_BATCH_ROWS])
    return status


def prepare_security_rows(
    security: Security,
    source: MassiveSource,
    start: date,
    end: date,
) -> tuple[str, list[str]]:
    fetch_start = max(start, security.list_date) if security.list_date else start
    fetch_end = min(end, security.delist_date) if security.delist_date else end
    if fetch_start > fetch_end:
        return "SKIP_WINDOW", []
    raw = source.get_minute_aggs(security.symbol, fetch_start.isoformat(), fetch_end.isoformat())
    if not raw:
        return "SUCCESS_NO_BARS", []
    max_expected = ((fetch_end - fetch_start).days + 1) * MAX_BARS_PER_CALENDAR_DAY
    if len(raw) > max_expected:
        raise RuntimeError(
            f"[{security.symbol}] 分钟响应体量异常: {len(raw):,} > {max_expected:,} "
            f"({fetch_start}..{fetch_end})"
        )

    list_floor = security.list_date
    delist_ceiling = security.delist_date
    ingested = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows: list[str] = []
    skipped_tenure = 0
    skipped_zero = 0
    for bar in raw:
        ts_utc = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc)
        et_day = ts_utc.astimezone(ET).date()
        if (list_floor and et_day < list_floor) or (delist_ceiling and et_day > delist_ceiling):
            skipped_tenure += 1
            continue
        o, h = bar.get("o"), bar.get("h")
        l, c = bar.get("l"), bar.get("c")
        if not o or not h or not l or not c or o <= 0 or h <= 0 or l <= 0 or c <= 0:
            skipped_zero += 1
            continue
        rows.append(
            f"{security.id}\t{ts_utc.strftime('%Y-%m-%d %H:%M:%S')}\t{security.symbol.upper()}\t"
            f"{o!r}\t{h!r}\t{l!r}\t{c!r}\t{int(round(bar.get('v') or 0))}\t"
            f"{bar.get('vw') or 0.0!r}\t{int(bar.get('n') or 0)}\tmassive_1m"
        )
    if skipped_tenure:
        logger.info("[{}] 任期外丢弃 {} 根分钟 bar（回收防护）。", security.symbol, skipped_tenure)
    if skipped_zero:
        logger.debug("[{}] 零价丢弃 {} 根。", security.symbol, skipped_zero)
    return ("SUCCESS" if rows else "SUCCESS_NO_BARS"), rows


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> tuple[int, dict]:
    from dotenv import load_dotenv
    load_dotenv()
    start = _window_start(args)
    end = get_last_completed_trading_date(args.market)
    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要同步分钟线的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}
    logger.info("分钟线增量：{} 只证券，窗口 [{}, {}]。", len(securities), start, end)

    results_counter = Counter()
    pending_rows: list[str] = []
    for index in range(0, len(securities), FETCH_CHUNK_SIZE):
        chunk = securities[index:index + FETCH_CHUNK_SIZE]
        outputs, chunk_counter = run_concurrently(
            chunk,
            lambda security: prepare_security_rows(security, source, start, end),
            max_workers=args.workers,
            desc=f"同步分钟线 {index + 1}-{index + len(chunk)}/{len(securities)}",
        )
        results_counter.update(chunk_counter)
        for status, rows in outputs:
            results_counter[status] += 1
            pending_rows.extend(rows)
            while len(pending_rows) >= CH_BATCH_ROWS:
                ch_insert_rows(pending_rows[:CH_BATCH_ROWS])
                del pending_rows[:CH_BATCH_ROWS]
    if pending_rows:
        ch_insert_rows(pending_rows)

    success = results_counter["SUCCESS"]
    no_bars = results_counter["SUCCESS_NO_BARS"] + results_counter["SKIP_WINDOW"]
    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    logger.info("--- 分钟线增量统计 ---")
    logger.info("  有数据: {}  无数据/窗口外: {}  错误: {}", success, no_bars, errors)
    exit_code = 1 if errors else 0
    return exit_code, {"processed": len(securities), "written": success, "failed": errors}


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_minute_bars", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
