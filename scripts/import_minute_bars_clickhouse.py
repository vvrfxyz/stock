"""intraday_1m_vw 年包 -> ClickHouse stock.minute_bars（23 年分钟线，含盘前盘后）。

数据形态：year=YYYY.tar.gz，内含 US/year=YYYY/month=MM/data.parquet（UTC 月对齐），
列 datetime(UTC us)/symbol/open/high/low/close/volume/vw/transactions，未复权原始价。
总量 ~135GB 压缩、数十亿行——只进 ClickHouse，PostgreSQL 不碰
（docs/archive/polyglot_persistence_architecture.md 的分钟级预留设计）。

管线（全部在 ClickHouse 内做转换，Python 不逐行搬数据）：
1. --refresh-tenures：从 PostgreSQL 导出代码任期（复用 import_day_aggs.build_tenures），
   在 Python 侧消解同 symbol 重叠区间（重叠段双方出局 = ambiguous 守卫同语义），
   全量替换 stock.symbol_tenures。
2. 每月：DROP PARTITION（幂等）-> parquet 原生 FORMAT Parquet 灌 staging ->
   INSERT SELECT 联 symbol_tenures 转换（ET 交易日定任期；^[A-Z][A-Z0-9.]*$ 过滤
   后缀类，防 'AAp'->'aap' 撞真 ticker）-> TRUNCATE staging。
3. 记账：staging 行数 = 入库 + 后缀剔除 + 未映射/任期外，写 ledger TSV，
   对不上非零退出。

用法（253 上；tar 已 scp 至 --workdir）：
    python scripts/import_minute_bars_clickhouse.py --refresh-tenures
    python scripts/import_minute_bars_clickhouse.py --tar /home/wenruifeng/data/minute_vw/year=2003.tar.gz
编排（本机推年包、逐年调用）见 scripts/run_minute_backfill.sh。
"""
import argparse
import os
import shutil
import sys
import tarfile
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.clickhouse import clickhouse_request_kwargs, clickhouse_url
from utils.script_logging import setup_logging as configure_script_logging

LEDGER_PATH = "logs/manual_backfill/minute_bars_ledger.tsv"
TICKER_RE = "^[A-Z][A-Z0-9.]*$"


TRANSFORM_SQL = f"""
INSERT INTO stock.minute_bars
    (security_id, ts, vendor_symbol, open, high, low, close, volume, vwap, trade_count, source)
SELECT
    t.security_id,
    toDateTime(s.datetime, 'UTC'),
    s.symbol,
    s.open, s.high, s.low, s.close,
    toUInt64(greatest(s.volume, 0)),
    s.vw,
    toUInt32(greatest(s.transactions, 0)),
    'flatfiles_1m'
FROM stock.minute_bars_staging AS s
INNER JOIN stock.symbol_tenures AS t ON t.symbol = lower(s.symbol)
WHERE s.open > 0 AND s.high > 0 AND s.low > 0 AND s.close > 0
  AND match(s.symbol, '{TICKER_RE}')
  AND toDate(s.datetime, 'America/New_York') >= t.start_date
  AND toDate(s.datetime, 'America/New_York') < t.end_date
"""

# 与 TRANSFORM 同谓词的计数版：inserted 必须与之相等（真不变量，非余项推导）。
MAPPABLE_SQL = f"""
SELECT count()
FROM stock.minute_bars_staging AS s
INNER JOIN stock.symbol_tenures AS t ON t.symbol = lower(s.symbol)
WHERE s.open > 0 AND s.high > 0 AND s.low > 0 AND s.close > 0
  AND match(s.symbol, '{TICKER_RE}')
  AND toDate(s.datetime, 'America/New_York') >= t.start_date
  AND toDate(s.datetime, 'America/New_York') < t.end_date
"""

ACCOUNT_SQL = f"""
SELECT
    count() AS staged,
    countIf(open <= 0 OR high <= 0 OR low <= 0 OR close <= 0) AS zero_price,
    countIf(open > 0 AND high > 0 AND low > 0 AND close > 0
            AND NOT match(symbol, '{TICKER_RE}')) AS suffix_class,
    uniqExact(toDate(datetime, 'America/New_York')) AS et_dates
FROM stock.minute_bars_staging
"""


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导入分钟线年包到 ClickHouse stock.minute_bars。")
    parser.add_argument("--tar", help="year=YYYY.tar.gz 路径。")
    parser.add_argument("--refresh-tenures", action="store_true",
                        help="从 PostgreSQL 重建 stock.symbol_tenures 后退出。")
    parser.add_argument("--init-ddl", action="store_true", help="执行 sql/clickhouse/minute_bars.sql 后退出。")
    parser.add_argument("--workdir", default="/home/wenruifeng/data/minute_tmp",
                        help="年包解包工作目录（用后清理）。")
    parser.add_argument("--ledger", default=LEDGER_PATH, help="装载台账 TSV（相对项目根）。")
    return parser


def ch(query: str, *, input_file: Path | None = None, input_bytes: bytes | None = None) -> str:
    """ClickHouse HTTP 接口（8123）；input_file/input_bytes 作 INSERT 数据体流式上传。"""
    if input_file is not None:
        with input_file.open("rb") as f:
            response = requests.post(clickhouse_url(), params={"query": query}, data=f,
                                     timeout=3600, **clickhouse_request_kwargs())
    elif input_bytes is not None:
        response = requests.post(clickhouse_url(), params={"query": query}, data=input_bytes,
                                 timeout=3600, **clickhouse_request_kwargs())
    else:
        response = requests.post(clickhouse_url(), data=query.encode(), timeout=3600,
                                 **clickhouse_request_kwargs())
    if response.status_code != 200:
        raise RuntimeError(f"clickhouse HTTP 失败: {response.text[:500]}")
    return response.text


def non_overlapping_tenures() -> list[tuple[str, int, date, date]]:
    """PG 任期 -> 消解重叠后的段列表。重叠区间双方出局（ambiguous 同语义）。"""
    from db_manager import DatabaseManager
    from scripts.import_day_aggs import load_tenures

    db_manager = DatabaseManager()
    try:
        tenures = load_tenures(db_manager)
    finally:
        db_manager.close()

    ch_max = date(2149, 6, 1)  # ClickHouse Date 上界附近，替代 FAR_FUTURE
    output: list[tuple[str, int, date, date]] = []
    for symbol, segs in tenures.items():
        segs = sorted(segs, key=lambda s: (s[1], s[2]))
        # 收集所有重叠区间，然后从每段里挖掉
        cuts: list[tuple[date, date]] = []
        for i, (_, s1, e1) in enumerate(segs):
            for _, s2, e2 in segs[i + 1:]:
                lo, hi = max(s1, s2), min(e1, e2)
                if lo < hi:
                    cuts.append((lo, hi))
        for sid, start, end in segs:
            pieces = [(start, end)]
            for lo, hi in cuts:
                next_pieces = []
                for ps, pe in pieces:
                    if hi <= ps or lo >= pe:
                        next_pieces.append((ps, pe))
                        continue
                    if ps < lo:
                        next_pieces.append((ps, lo))
                    if hi < pe:
                        next_pieces.append((hi, pe))
                pieces = next_pieces
            for ps, pe in pieces:
                output.append((symbol, sid, ps, min(pe, ch_max)))
    return output


def refresh_tenures() -> int:
    rows = non_overlapping_tenures()
    payload = "".join(f"{s}\t{sid}\t{ps.isoformat()}\t{pe.isoformat()}\n" for s, sid, ps, pe in rows)
    ch("TRUNCATE TABLE stock.symbol_tenures")
    ch("INSERT INTO stock.symbol_tenures FORMAT TabSeparated", input_bytes=payload.encode())
    count = ch("SELECT count() FROM stock.symbol_tenures").strip()
    logger.info("symbol_tenures 已刷新：{} 段（消解重叠后）。", count)
    return 0


def load_month(parquet_path: Path, partition: str, ledger_rows: list[str]) -> None:
    ch(f"ALTER TABLE stock.minute_bars DROP PARTITION '{partition}'")
    ch("TRUNCATE TABLE stock.minute_bars_staging")
    ch("INSERT INTO stock.minute_bars_staging FORMAT Parquet", input_file=parquet_path)
    staged, zero_price, suffix_class, et_dates = (int(x) for x in ch(ACCOUNT_SQL).split())
    mappable = int(ch(MAPPABLE_SQL).strip())
    ch(TRANSFORM_SQL)
    inserted = int(ch(
        f"SELECT count() FROM stock.minute_bars WHERE toYYYYMM(ts) = {partition}").strip())
    ch("TRUNCATE TABLE stock.minute_bars_staging")

    unmapped = staged - zero_price - suffix_class - mappable
    if inserted != mappable or unmapped < 0:
        raise RuntimeError(
            f"{partition} 记账不平: staged={staged} inserted={inserted} mappable={mappable} "
            f"zero_price={zero_price} suffix={suffix_class}")
    ledger_rows.append(
        f"{partition}\t{staged}\t{inserted}\t{zero_price}\t{suffix_class}\t{unmapped}\t{et_dates}")
    logger.info("[{}] staged={} inserted={} zero_price={} suffix={} unmapped={} et_dates={}",
                partition, staged, inserted, zero_price, suffix_class, unmapped, et_dates)


def load_year(tar_path: Path, workdir: Path, ledger_rows: list[str]) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    logger.info("解包 {} -> {}", tar_path.name, workdir)
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(workdir, filter="data")
    months = sorted(workdir.glob("US/year=*/month=*/data.parquet"))
    if not months:
        raise RuntimeError(f"{tar_path} 内没有 month parquet。")
    for parquet_path in months:
        year = next(p for p in parquet_path.parts if p.startswith("year=")).split("=")[1]
        month = next(p for p in parquet_path.parts if p.startswith("month=")).split("=")[1]
        load_month(parquet_path, f"{year}{month}", ledger_rows)
        parquet_path.unlink()
    shutil.rmtree(workdir / "US", ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("import_minute_bars_clickhouse")
    from dotenv import load_dotenv
    load_dotenv()
    args = create_parser().parse_args(argv)

    try:
        if args.init_ddl:
            ddl = (project_root / "sql/clickhouse/minute_bars.sql").read_text()
            for chunk in ddl.split(";"):
                statement = "\n".join(
                    line for line in chunk.splitlines() if not line.strip().startswith("--")
                ).strip()
                if statement:
                    ch(statement)
            logger.success("minute_bars DDL 已执行。")
            return 0
        if args.refresh_tenures:
            return refresh_tenures()
        if not args.tar:
            logger.error("需要 --tar / --refresh-tenures / --init-ddl 之一。")
            return 1

        tar_path = Path(args.tar)
        if not tar_path.exists():
            logger.error("{} 不存在。", tar_path)
            return 1
        ledger_rows: list[str] = []
        load_year(tar_path, Path(args.workdir), ledger_rows)

        ledger_path = project_root / args.ledger
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not ledger_path.exists()
        with ledger_path.open("a") as f:
            if write_header:
                f.write("partition\tstaged\tinserted\tzero_price\tsuffix_class\tunmapped\tet_dates\n")
            for row in ledger_rows:
                f.write(row + "\n")
        logger.success("{} 完成：{} 个月已入库，台账 {}。", tar_path.name, len(ledger_rows), ledger_path)
        return 0
    except Exception as exc:
        logger.opt(exception=exc).critical("import_minute_bars_clickhouse 执行失败: {}", exc)
        return 1
    finally:
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
