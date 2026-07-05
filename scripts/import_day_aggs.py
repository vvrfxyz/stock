"""Polygon day_aggs_v1 flat files -> daily_prices 历史回填（2003-09-10 起的 20 年日线）。

数据形态：day_aggs_v1_YYYY.tgz，内含 YYYY/MM/YYYY-MM-DD.csv.gz（每交易日一件），
列：ticker,volume,open,close,high,low,window_start,transactions（未复权原始价）。

映射规则（防 20 年尺度的 ticker 回收/改名污染，接续 2026-07 回收修复的口径）：
- ticker 大小写敏感：内含小写字母的是优先股(p)/权证(w)/认购权(r)/单位(u)等后缀
  （如 "AAp"），非 CS/ETF，直接跳过——绝不能盲目 lowercase（"AAp"→"aap" 会撞真
  ticker AAP）。只接受 ^[A-Z][A-Z0-9.]*$。
- (ticker, date) 按"代码任期"挂靠 security_id：任期时间线由 security_symbol_history
  的 ticker_change 事件（start_date=该代码生效日）+ 现行 symbol 构成，整体裁剪到
  [list_date, 退市上界]；date 落在恰好一个任期内才写入，0 个记 unmapped、
  多个记 ambiguous，都跳过并计数。
- list_date 为 NULL 的证券不参与（任期起点无从界定，宁缺毋滥——先跑
  update_massive_details --all 补齐）。
- 只导入 date < cutoff（默认取 daily_prices 全局 MIN(date)，即 Massive 在线管道
  的水位下界）；重叠区归在线管道管辖（在线行带 vwap，flat files 无）。
- 幂等：upsert 冲突键 (security_id, date)，重跑安全。

用法（253 上）：
    python scripts/import_day_aggs.py --dir /home/wenruifeng/data/day_aggs --dry-run
    python scripts/import_day_aggs.py --dir /home/wenruifeng/data/day_aggs
    python scripts/import_day_aggs.py --dir ... --years 2003-2013   # 分段跑
"""
import argparse
import csv
import gzip
import io
import re
import sys
import tarfile
import time
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

IMPORTABLE_TICKER = re.compile(r"^[A-Z][A-Z0-9.]*$")
FAR_FUTURE = date(9999, 1, 1)
EXPECTED_HEADER = ["ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"]


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导入 Polygon day_aggs_v1 历史日线到 daily_prices。")
    parser.add_argument("--dir", required=True, help="存放 day_aggs_v1_YYYY.tgz 的目录。")
    parser.add_argument("--years", default=None, help="年份范围，如 2003-2013 或 2015；默认全部。")
    parser.add_argument("--cutoff", default=None,
                        help="只导入早于该日期(YYYY-MM-DD)的 bar；默认取 daily_prices 全局最早日期。")
    parser.add_argument("--dry-run", action="store_true", help="只做映射统计，不写库。")
    parser.add_argument("--unmapped-report", default="logs/manual_backfill/day_aggs_unmapped.tsv",
                        help="未映射 ticker 汇总输出路径（相对项目根）。")
    return parser


def build_tenures(secs, hist) -> tuple[dict[str, list[tuple[int, date, date]]], int]:
    """纯逻辑：securities 行 + symbol_history 行 -> symbol 任期索引。

    secs: 可迭代对象，元素含 id/symbol/list_date/delist_date/is_active/max_bar。
    hist: 可迭代对象，元素含 security_id/symbol(已小写)/start_date/end_date。
    返回 (tenures, skipped_no_list_date)。区间为半开 [start, end)；显式
    end_date（人工修复行）按闭区间语义 +1 天；退市上界日当天的 bar 仍归属该证券。
    """
    hist_by_sec: dict[int, list] = defaultdict(list)
    for row in hist:
        hist_by_sec[row.security_id].append((row.start_date, row.symbol, row.end_date))

    tenures: dict[str, list[tuple[int, date, date]]] = defaultdict(list)
    skipped_no_list_date = 0
    for sec in secs:
        if sec.list_date is None:
            skipped_no_list_date += 1
            continue
        if sec.delist_date is not None:
            upper_exclusive = sec.delist_date + timedelta(days=1)
        elif sec.is_active:
            upper_exclusive = FAR_FUTURE
        else:
            # inactive 且无 delist_date：现有最后一根 bar 是死亡日的可靠代理
            upper_exclusive = (sec.max_bar + timedelta(days=1)) if sec.max_bar else sec.list_date
        events = sorted(hist_by_sec.get(sec.id, []), key=lambda t: (t[0], t[1]))
        current = (sec.symbol or "").lower()
        if not events or events[-1][1] != current:
            # 现行 symbol 缺失于事件线尾部：无事件时从 list_date 起，
            # 否则从最后事件起（该陈旧事件段因零长度自然消失）。
            events.append((events[-1][0] if events else sec.list_date, current, None))
        for i, (start, symbol, explicit_end) in enumerate(events):
            if explicit_end is not None:
                end_exclusive = explicit_end + timedelta(days=1)
            elif i + 1 < len(events):
                end_exclusive = events[i + 1][0]
            else:
                end_exclusive = upper_exclusive
            seg_start = max(start, sec.list_date)
            seg_end = min(end_exclusive, upper_exclusive)
            if seg_start >= seg_end:
                continue
            tenures[symbol].append((sec.id, seg_start, seg_end))
    return dict(tenures), skipped_no_list_date


def load_tenures(db_manager: DatabaseManager) -> dict[str, list[tuple[int, date, date]]]:
    """从库加载 securities/symbol_history 并构建任期索引（见 build_tenures）。"""
    from sqlalchemy import text

    with db_manager.get_session() as session:
        secs = session.execute(text("""
            SELECT s.id, s.symbol, s.list_date, s.delist_date, s.is_active,
                   (SELECT MAX(dp.date) FROM daily_prices dp WHERE dp.security_id = s.id) AS max_bar
            FROM securities s
            WHERE upper(s.market) = 'US' AND upper(s.type) IN ('CS', 'ETF')
        """)).all()
        hist = session.execute(text("""
            SELECT security_id, lower(symbol) AS symbol, start_date, end_date
            FROM security_symbol_history
            WHERE start_date IS NOT NULL
        """)).all()
    tenures, skipped = build_tenures(secs, hist)
    logger.info("任期索引: {} 个 symbol，{} 只证券因 list_date 缺失未参与。", len(tenures), skipped)
    return tenures


def resolve_file_map(
    tickers: list[str],
    file_date: date,
    tenures: dict[str, list[tuple[int, date, date]]],
    stats: Counter,
    unmapped: Counter,
) -> dict[str, int]:
    """一个文件一个交易日：为该日的每个 ticker 解析 security_id。"""
    mapping: dict[str, int] = {}
    for ticker in tickers:
        if not IMPORTABLE_TICKER.match(ticker):
            stats["skipped_suffix_class"] += 1
            continue
        segs = tenures.get(ticker.lower())
        if not segs:
            stats["unmapped_no_symbol"] += 1
            unmapped[ticker] += 1
            continue
        hits = {sid for sid, start, end in segs if start <= file_date < end}
        if len(hits) == 1:
            mapping[ticker] = next(iter(hits))
            stats["mapped"] += 1
        elif not hits:
            stats["unmapped_out_of_tenure"] += 1
            unmapped[ticker] += 1
        else:
            stats["ambiguous"] += 1
            unmapped[f"{ticker}(AMBIG)"] += 1
    return mapping


def iter_day_files(tgz_path: Path):
    """流式迭代年包内的 (file_date, csv_bytes)，不落盘解包。"""
    with tarfile.open(tgz_path, "r:gz") as tar:
        for member in tar:
            if not member.isfile() or not member.name.endswith(".csv.gz"):
                continue
            day_str = Path(member.name).name.removesuffix(".csv.gz")
            try:
                file_date = date.fromisoformat(day_str)
            except ValueError:
                logger.warning("[{}] 无法从文件名解析日期，跳过: {}", tgz_path.name, member.name)
                continue
            raw = tar.extractfile(member).read()
            yield file_date, gzip.decompress(raw)


def parse_rows(csv_bytes: bytes, tgz_name: str, file_date: date) -> list[dict]:
    reader = csv.reader(io.TextIOWrapper(io.BytesIO(csv_bytes), encoding="utf-8"))
    header = next(reader, None)
    if header != EXPECTED_HEADER:
        raise ValueError(f"{tgz_name} {file_date}: 表头异常 {header}")
    return [
        {"ticker": r[0], "volume": r[1], "open": r[2], "close": r[3], "high": r[4], "low": r[5],
         "transactions": r[7]}
        for r in reader if len(r) == 8
    ]


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("import_day_aggs")
    args = create_parser().parse_args(argv)

    data_dir = Path(args.dir)
    archives = sorted(data_dir.glob("day_aggs_v1_*.tgz"))
    if args.years:
        lo, _, hi = args.years.partition("-")
        lo, hi = int(lo), int(hi or lo)
        archives = [p for p in archives if lo <= int(p.stem.rsplit("_", 1)[-1]) <= hi]
    if not archives:
        logger.error("{} 下没有匹配的 day_aggs_v1_*.tgz。", data_dir)
        return 1

    db_manager = None
    try:
        db_manager = DatabaseManager()
        from sqlalchemy import text
        if args.cutoff:
            cutoff = date.fromisoformat(args.cutoff)
        else:
            with db_manager.get_session() as session:
                cutoff = session.execute(text("SELECT MIN(date) FROM daily_prices")).scalar()
            if cutoff is None:
                cutoff = FAR_FUTURE
        logger.info("导入截止（不含）: {}；归档 {} 个: {} .. {}",
                    cutoff, len(archives), archives[0].name, archives[-1].name)

        tenures = load_tenures(db_manager)
        stats: Counter = Counter()
        unmapped: Counter = Counter()
        touched_ids: set[int] = set()

        for tgz_path in archives:
            year_written = 0
            year_files = 0
            for file_date, csv_bytes in iter_day_files(tgz_path):
                if file_date >= cutoff:
                    stats["skipped_after_cutoff_files"] += 1
                    continue
                rows_raw = parse_rows(csv_bytes, tgz_path.name, file_date)
                mapping = resolve_file_map([r["ticker"] for r in rows_raw], file_date, tenures, stats, unmapped)
                if args.dry_run:
                    year_files += 1
                    continue
                batch = []
                for r in rows_raw:
                    sid = mapping.get(r["ticker"])
                    if sid is None:
                        continue
                    batch.append({
                        "security_id": sid,
                        "date": file_date,
                        "open": r["open"], "high": r["high"], "low": r["low"], "close": r["close"],
                        "volume": int(float(r["volume"])),
                        "trade_count": int(float(r["transactions"])) if r["transactions"] else None,
                    })
                if batch:
                    year_written += db_manager.upsert_daily_prices(batch)
                    touched_ids.update(row["security_id"] for row in batch)
                year_files += 1
            logger.info("[{}] 处理 {} 个交易日，写入 {} 行。", tgz_path.name, year_files, year_written)
            stats["rows_written"] += year_written

        logger.info("--- 导入统计 ---")
        for key, value in sorted(stats.items()):
            logger.info("  {}: {}", key, value)

        report_path = project_root / args.unmapped_report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w") as f:
            f.write("ticker\trows\n")
            for ticker, n in unmapped.most_common():
                f.write(f"{ticker}\t{n}\n")
        logger.info("未映射 ticker 报告: {}（{} 个）", report_path, len(unmapped))

        # 水位线自愈：导入历史 bar 一般不会推高 MAX(date)，但个别证券
        # （水位曾为空/滞后）可能变化，统一按事实重算，保住 integrity 检查。
        if not args.dry_run and touched_ids:
            with db_manager.get_session() as session:
                fixed = session.execute(text("""
                    UPDATE securities s SET price_data_latest_date = agg.max_date
                    FROM (SELECT security_id, MAX(date) AS max_date FROM daily_prices
                          WHERE security_id = ANY(:ids) GROUP BY security_id) agg
                    WHERE s.id = agg.security_id
                      AND s.price_data_latest_date IS DISTINCT FROM agg.max_date
                """), {"ids": list(touched_ids)})
                session.commit()
                logger.info("水位线校准: {} 只证券。", fixed.rowcount or 0)
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("import_day_aggs 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
