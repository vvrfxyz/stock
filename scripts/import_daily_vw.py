"""WithVW daily_vw parquet -> daily_prices.vwap 回填（flat files 时代 2003-2023 的 VWAP 补齐）。

数据形态：daily_vw/US/year=YYYY/month=MM/data.parquet（列 date/symbol/open/high/low/
close/volume/vw/transactions，2003-09 起逐月）+ daily_vw/补缺/year=YYYY/data.parquet
（主下载缺失日期的补齐件，同 schema）。来源与 flat files 同为 SIP 汇总口径，
close/volume 与存量行位级一致（导入前审计验证），vw 可安全嫁接。

规则：
- 只 UPDATE，绝不 INSERT：daily_vw 里 PG 没有的 bar（OTC/后缀类/未映射）只计数
  报告，不落库——flat files 才是 bar 存在性的 truth。
- 三指纹守卫：只更新 `vwap IS NULL AND trade_count IS NOT NULL`（flat files 行）；
  yfinance 双 NULL 行与 Massive 行（vwap 非 NULL）物理上不可能被触碰。
- 时代钳制：默认只更新 date < 2024-01-01（--max-date）。2024 起归 Massive 时代，
  其 vwap 由 live sync 负责；即便个别 Massive 行 vwap 为 NULL 也不由本归档补。
- (ticker, date) 按代码任期挂靠 security_id（复用 import_day_aggs 的任期索引与
  ^[A-Z][A-Z0-9.]*$ 过滤），0 命中/多命中跳过计数——绝不猜。
- vw 必须为正有限值；无效值计数跳过。
- 写路径：每文件一个事务——TEMP 表 COPY 装载 -> 聚合 SELECT 记账 -> 带守卫的
  UPDATE JOIN。幂等：已更新的行第二次运行时落在 already_has_vwap 桶。
- 指纹口径影响已核实：import_day_aggs 的 Massive 保护边界是 max(min vwap 日,
  era_start)，本回填只会把 min vwap 日拉早、被 era_start 钳住，行为不变；
  purge 只认双 NULL 指纹，yfinance 行不受影响。CLAUDE.md 指纹描述同步更新为
  "flat files = trade_count 有（vwap 2026-07 起已回填）"。

用法（253 上）：
    python scripts/import_daily_vw.py --dir /home/wenruifeng/data/daily_vw --dry-run
    python scripts/import_daily_vw.py --dir /home/wenruifeng/data/daily_vw
"""
import argparse
import math
import sys
import time
from collections import Counter, defaultdict
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from db_manager import DatabaseManager
from scripts.import_day_aggs import IMPORTABLE_TICKER, load_tenures
from utils.script_logging import setup_logging as configure_script_logging

DEFAULT_MAX_DATE = date(2024, 1, 1)  # Massive 时代起点：该日起 vwap 归 live sync


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="daily_vw parquet -> daily_prices.vwap 回填。")
    parser.add_argument("--dir", required=True, help="daily_vw 根目录（含 US/ 与可选 补缺/）。")
    parser.add_argument("--max-date", default=DEFAULT_MAX_DATE.isoformat(),
                        help="只更新早于该日(YYYY-MM-DD)的行；默认 2024-01-01（Massive 时代起点）。")
    parser.add_argument("--years", default=None, help="年份范围，如 2003-2013 或 2015；默认全部。")
    parser.add_argument("--dry-run", action="store_true", help="只做映射与可更新行统计，不 UPDATE。")
    parser.add_argument("--unmapped-report", default="logs/manual_backfill/daily_vw_unmapped.tsv",
                        help="未映射 ticker 汇总输出路径（相对项目根）。")
    return parser


def iter_parquet_files(root: Path, years: tuple[int, int] | None):
    """主树 US/year=*/month=*/data.parquet 按年月序，补缺/year=*/data.parquet 殿后。"""
    main = sorted(root.glob("US/year=*/month=*/data.parquet"))
    gap = sorted(root.glob("补缺/year=*/data.parquet"))
    for path in main + gap:
        year_part = next(p for p in path.parts if p.startswith("year="))
        year = int(year_part.split("=")[1])
        if years and not (years[0] <= year <= years[1]):
            continue
        yield path


def normalize_rows(frame, max_date: date, tenures, stats: Counter, unmapped: Counter) -> list[tuple]:
    """parquet 行 -> (security_id, date, vw, close)。过滤 + 任期归属 + 计数。

    close 随行携带：UPDATE 谓词要求 dp.close = t.close（审计证实同实体行 close
    位级一致），把任期归属的正确性变成逐行校验的不变量——归属错了的行落进
    entity_mismatch 桶而不是写坏 vwap。
    """
    rows = []
    for rec in frame.itertuples(index=False):
        raw_date = rec.date
        bar_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date)[:10])
        if bar_date >= max_date:
            stats["skipped_after_max_date"] += 1
            continue
        ticker = rec.symbol or ""
        if not IMPORTABLE_TICKER.match(ticker):
            stats["skipped_suffix_class"] += 1
            continue
        vw = rec.vw
        if vw is None or not math.isfinite(vw) or vw <= 0:
            stats["skipped_bad_vw"] += 1
            continue
        segs = tenures.get(ticker.lower())
        if not segs:
            stats["unmapped_no_symbol"] += 1
            unmapped[ticker] += 1
            continue
        hits = {sid for sid, start, end in segs if start <= bar_date < end}
        if len(hits) == 1:
            rows.append((next(iter(hits)), bar_date, vw, rec.close))
            stats["mapped"] += 1
        elif not hits:
            stats["unmapped_out_of_tenure"] += 1
            unmapped[ticker] += 1
        else:
            stats["ambiguous"] += 1
            unmapped[f"{ticker}(AMBIG)"] += 1
    return rows


def stage_and_update(db_manager: DatabaseManager, rows: list[tuple], dry_run: bool, stats: Counter) -> None:
    """一个文件一个事务：TEMP 表 COPY -> 记账 SELECT -> 守卫 UPDATE。"""
    from sqlalchemy import text

    if not rows:
        return
    buf = StringIO()
    for sid, bar_date, vw, close in rows:
        buf.write(f"{sid}\t{bar_date.isoformat()}\t{vw!r}\t{close!r}\n")
    buf.seek(0)

    with db_manager.engine.connect() as conn:
        conn.execute(text("""
            CREATE TEMP TABLE tmp_daily_vw (
                security_id BIGINT, date DATE, vw DOUBLE PRECISION, close DOUBLE PRECISION
            ) ON COMMIT DROP
        """))
        raw = conn.connection.dbapi_connection
        with raw.cursor() as cur:
            cur.copy_expert("COPY tmp_daily_vw FROM STDIN WITH (FORMAT text)", buf)

        counts = conn.execute(text("""
            SELECT
              count(*) FILTER (WHERE dp.security_id IS NOT NULL
                               AND dp.vwap IS NULL AND dp.trade_count IS NOT NULL
                               AND dp.close = t.close::numeric) AS updatable,
              count(*) FILTER (WHERE dp.security_id IS NOT NULL
                               AND dp.vwap IS NULL AND dp.trade_count IS NOT NULL
                               AND dp.close <> t.close::numeric) AS entity_mismatch,
              count(*) FILTER (WHERE dp.security_id IS NOT NULL
                               AND dp.vwap IS NOT NULL) AS already_has_vwap,
              count(*) FILTER (WHERE dp.security_id IS NOT NULL
                               AND dp.vwap IS NULL AND dp.trade_count IS NULL) AS yfinance_untouchable,
              count(*) FILTER (WHERE dp.security_id IS NULL) AS no_pg_row
            FROM tmp_daily_vw t
            LEFT JOIN daily_prices dp
              ON dp.security_id = t.security_id AND dp.date = t.date
        """)).one()
        stats["updatable"] += counts.updatable
        stats["entity_mismatch"] += counts.entity_mismatch
        stats["already_has_vwap"] += counts.already_has_vwap
        stats["yfinance_untouchable"] += counts.yfinance_untouchable
        stats["no_pg_row"] += counts.no_pg_row

        if not dry_run and counts.updatable:
            updated = conn.execute(text("""
                UPDATE daily_prices dp
                SET vwap = t.vw
                FROM tmp_daily_vw t
                WHERE dp.security_id = t.security_id
                  AND dp.date = t.date
                  AND dp.vwap IS NULL
                  AND dp.trade_count IS NOT NULL
                  AND dp.close = t.close::numeric
            """))
            stats["rows_updated"] += updated.rowcount or 0
        conn.commit()


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("import_daily_vw")
    args = create_parser().parse_args(argv)

    import pandas as pd

    root = Path(args.dir)
    if not (root / "US").exists():
        logger.error("{} 下没有 US/ 目录。", root)
        return 1
    max_date = date.fromisoformat(args.max_date)
    years = None
    if args.years:
        lo, _, hi = args.years.partition("-")
        years = (int(lo), int(hi or lo))

    db_manager = None
    try:
        db_manager = DatabaseManager()
        tenures = load_tenures(db_manager)
        stats: Counter = Counter()
        unmapped: Counter = Counter()

        files = list(iter_parquet_files(root, years))
        logger.info("待处理 parquet 文件 {} 个（含补缺件），max_date={}，dry-run={}。",
                    len(files), max_date, args.dry_run)
        for index, path in enumerate(files, 1):
            frame = pd.read_parquet(path, columns=["date", "symbol", "vw", "close"])
            stats["input_rows"] += len(frame)
            rows = normalize_rows(frame, max_date, tenures, stats, unmapped)
            stage_and_update(db_manager, rows, args.dry_run, stats)
            if index % 24 == 0 or index == len(files):
                logger.info("  [{}/{}] {} 累计 updatable={} updated={}",
                            index, len(files), path.parent.name,
                            stats["updatable"], stats["rows_updated"])

        # 记账断言：输入行必有唯一去向
        accounted = (stats["mapped"] + stats["skipped_after_max_date"] + stats["skipped_suffix_class"]
                     + stats["skipped_bad_vw"] + stats["unmapped_no_symbol"]
                     + stats["unmapped_out_of_tenure"] + stats["ambiguous"])
        if accounted != stats["input_rows"]:
            logger.error("记账不平：输入 {} != 去向合计 {}。", stats["input_rows"], accounted)
            return 1
        mapped_accounted = (stats["updatable"] + stats["entity_mismatch"] + stats["already_has_vwap"]
                            + stats["yfinance_untouchable"] + stats["no_pg_row"])
        if mapped_accounted != stats["mapped"]:
            logger.error("映射记账不平：mapped {} != join 分桶合计 {}。", stats["mapped"], mapped_accounted)
            return 1

        logger.info("--- daily_vw 回填统计（dry-run={}）---", args.dry_run)
        for key, value in sorted(stats.items()):
            logger.info("  {}: {}", key, value)

        report_path = project_root / args.unmapped_report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with report_path.open("w") as f:
            f.write("ticker\trows\n")
            for ticker, n in unmapped.most_common():
                f.write(f"{ticker}\t{n}\n")
        logger.info("未映射 ticker 报告: {}（{} 个）", report_path, len(unmapped))
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("import_daily_vw 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
