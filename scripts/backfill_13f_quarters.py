"""13F 历史回填编排器：按 filing quarter 从新到旧循环 update_institutional_holdings --quarter。

背景（2026-07-04）：institutional_holdings 稠密覆盖仅从 2023Q4（period）开始，
更早年份需按 EDGAR full-index 逐季回填。13F XML 信息表 2013-05-20 起才强制，
故物理下界为 filing quarter 2013Q2（更早的 ASCII 文本 filing 解析为空）。

设计：
- 从新到旧（新数据研究价值高、CUSIP 映射率高），已完整季度靠
  update_institutional_holdings 的 accession 级 filter_pending 幂等跳过，
  重跑一个完整季度只花一次 form.idx 请求 —— "已抓过的年月跳过"。
- 每季度跑两遍（PASS 2 只补第一遍失败的 filing，见
  docs/audits/2026-06-30-13f-factor-coverage-postmortem.md 的先例）。
- 磁盘护栏：/ 剩余空间低于 --min-free-gb 时写 STOPPED_DISK 台账行并退出 3；
  磁盘扩容后重跑本脚本即自动续传（已完成季度秒级跳过）。
- daily-run 避让：scheduled_update 进程存在、或处于每日 01:30-02:10 UTC
  预启动窗口时休眠等待（周日 scheduled_update 含 SEC 步骤，两进程并发
  会超 SEC 10 req/s 上限）。00:30 后不再启动新季度（单季约 1-1.3h）。
- 台账 logs/manual_backfill/13f_backfill_ledger.tsv：每季度一行，
  status=COMPLETE/RESIDUAL_EMPTY/FAILED/STOPPED_DISK；重跑时 COMPLETE 季度
  直接跳过（--force 重验）。RESIDUAL_EMPTY = 季度内有解析为空的 filing
  （文本格式/无信息表），属预期，不算失败。

用法（253 上，wenruifeng 身份）：
    nohup .venv/bin/python scripts/backfill_13f_quarters.py \
        >> logs/manual_backfill/13f_driver.out 2>&1 &
"""
import argparse
import re
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LEDGER_COLUMNS = [
    "quarter", "started_at_utc", "ended_at_utc", "discovered", "pass1_pending",
    "pass1_failed", "pass2_pending", "pass2_failed", "db_accessions", "db_rows",
    "mapped_pct", "disk_free_gb", "status",
]

RE_DISCOVERED = re.compile(r"form index 发现 13F filing (\d+) 个")
RE_PENDING = re.compile(r"待处理 13F filing: (\d+) 个")
RE_NO_PENDING = re.compile(r"没有待处理的 13F filing")
RE_PROCESSED = re.compile(r"filing 处理: (\d+)（失败 (\d+)）")


def quarter_bounds(q: str) -> tuple[date, date]:
    year, quarter = q.upper().split("Q")
    year, quarter = int(year), int(quarter)
    start = date(year, 3 * quarter - 2, 1)
    end = date(year + (quarter == 4), 1 if quarter == 4 else 3 * quarter + 1, 1)
    return start, end


def gen_quarters(newest: str, oldest: str) -> list[str]:
    """含两端，从新到旧。"""
    ny, nq = (int(x) for x in newest.upper().split("Q"))
    oy, oq = (int(x) for x in oldest.upper().split("Q"))
    out = []
    y, q = ny, nq
    while (y, q) >= (oy, oq):
        out.append(f"{y}Q{q}")
        q -= 1
        if q == 0:
            y, q = y - 1, 4
    return out


def disk_free_gb() -> float:
    return shutil.disk_usage(str(PROJECT_ROOT)).free / 1024**3


def scheduled_update_running() -> bool:
    result = subprocess.run(
        ["pgrep", "-f", "main.py scheduled_update"], capture_output=True, text=True
    )
    return result.returncode == 0


def in_daily_run_prestart_window(now: datetime) -> bool:
    """每日 02:00 UTC scheduled_update 的预启动禁跑窗（01:30-02:10）。"""
    minutes = now.hour * 60 + now.minute
    return 90 <= minutes < 130


def too_close_to_daily_run(now: datetime) -> bool:
    """00:30 后不再启动新季度：单季约 1-1.3h，避免跑进 02:00 的 daily-run。"""
    minutes = now.hour * 60 + now.minute
    return minutes >= 30 and minutes < 130


def wait_for_clear_window() -> None:
    while True:
        now = datetime.now(timezone.utc)
        if scheduled_update_running():
            print(f"[{now:%F %T}] scheduled_update 运行中，休眠 5 分钟…", flush=True)
        elif in_daily_run_prestart_window(now) or too_close_to_daily_run(now):
            print(f"[{now:%F %T}] 处于 daily-run 禁跑窗（00:30-02:10 UTC），休眠 5 分钟…", flush=True)
        else:
            return
        time.sleep(300)


def run_pass(quarter: str, limit: int, log_path: Path) -> dict:
    cmd = [str(PROJECT_ROOT / ".venv/bin/python"), "main.py",
           "update_institutional_holdings", "--quarter", quarter]
    if limit:
        cmd += ["--limit", str(limit)]
    stats = {"discovered": None, "pending": None, "failed": None, "exit": None}
    with log_path.open("a") as sink:
        sink.write(f"\n===== {datetime.now(timezone.utc):%F %T} {' '.join(cmd)} =====\n")
        sink.flush()
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for line in proc.stdout:
            sink.write(line)
            if m := RE_DISCOVERED.search(line):
                stats["discovered"] = int(m.group(1))
            if m := RE_PENDING.search(line):
                stats["pending"] = int(m.group(1))
            if RE_NO_PENDING.search(line):
                stats["pending"] = 0
            if m := RE_PROCESSED.search(line):
                stats["failed"] = int(m.group(2))
        proc.wait()
        stats["exit"] = proc.returncode
        sink.write(f"===== exit={proc.returncode} {stats} =====\n")
    return stats


def db_quarter_stats(quarter: str) -> tuple[int, int, float]:
    """(distinct accessions, rows, mapped %) —— 按 filing_date 落在该季度统计。"""
    from dotenv import dotenv_values
    from sqlalchemy import create_engine, text

    cfg = dotenv_values(PROJECT_ROOT / ".env")
    engine = create_engine(cfg["DATABASE_URL"])
    start, end = quarter_bounds(quarter)
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(DISTINCT accession_number), COUNT(*), "
                "COALESCE(AVG((security_id IS NOT NULL)::int), 0) "
                "FROM institutional_holdings "
                "WHERE source = 'SEC_EDGAR' AND filing_date >= :a AND filing_date < :b"
            ), {"a": start, "b": end}).one()
            return int(row[0]), int(row[1]), round(float(row[2]) * 100, 1)
    finally:
        engine.dispose()


def read_complete_quarters(ledger: Path) -> set[str]:
    if not ledger.exists():
        return set()
    done = set()
    for line in ledger.read_text().splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) == len(LEDGER_COLUMNS) and parts[-1] in ("COMPLETE", "RESIDUAL_EMPTY"):
            done.add(parts[0])
    return done


def append_ledger(ledger: Path, row: dict) -> None:
    ledger.parent.mkdir(parents=True, exist_ok=True)
    if not ledger.exists():
        ledger.write_text("\t".join(LEDGER_COLUMNS) + "\n")
    with ledger.open("a") as f:
        f.write("\t".join(str(row.get(col, "")) for col in LEDGER_COLUMNS) + "\n")


def current_quarter() -> str:
    today = date.today()
    return f"{today.year}Q{(today.month - 1) // 3 + 1}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="13F 历史回填编排器（新→旧逐季）。")
    parser.add_argument("--newest", default=current_quarter(), help="起始（最新）filing quarter，默认当前季度。")
    parser.add_argument("--oldest", default="2013Q2",
                        help="终点（最旧）filing quarter。2013Q2 之前无 XML 信息表，回填无产出。")
    parser.add_argument("--min-free-gb", type=float, default=12.0,
                        help="磁盘剩余空间保底（GB），低于则停止（默认 12）。")
    parser.add_argument("--limit-per-quarter", type=int, default=0, help="每季度 filing 上限（冒烟测试用）。")
    parser.add_argument("--force", action="store_true", help="忽略台账中已 COMPLETE 的季度，重新验证。")
    parser.add_argument("--no-wait", action="store_true", help="跳过 daily-run 避让（冒烟测试用）。")
    args = parser.parse_args(argv)

    ledger = PROJECT_ROOT / "logs/manual_backfill/13f_backfill_ledger.tsv"
    quarters = gen_quarters(args.newest, args.oldest)
    done = set() if args.force else read_complete_quarters(ledger)
    todo = [q for q in quarters if q not in done]
    print(f"季度序列 {quarters[0]} → {quarters[-1]} 共 {len(quarters)} 个，"
          f"台账已完成 {len(quarters) - len(todo)} 个，待跑 {len(todo)} 个。", flush=True)

    for quarter in todo:
        free = disk_free_gb()
        if free < args.min_free_gb:
            print(f"磁盘剩余 {free:.1f}GB < 保底 {args.min_free_gb}GB，停止。扩容后重跑本脚本即续传。", flush=True)
            append_ledger(ledger, {
                "quarter": quarter, "started_at_utc": f"{datetime.now(timezone.utc):%F %T}",
                "ended_at_utc": "", "disk_free_gb": round(free, 1), "status": "STOPPED_DISK",
            })
            return 3

        if not args.no_wait:
            wait_for_clear_window()

        started = datetime.now(timezone.utc)
        log_path = PROJECT_ROOT / f"logs/manual_backfill/13f_q_{quarter}.log"
        print(f"[{started:%F %T}] ==== {quarter} PASS 1（磁盘剩余 {free:.1f}GB）====", flush=True)
        p1 = run_pass(quarter, args.limit_per_quarter, log_path)
        print(f"[{datetime.now(timezone.utc):%F %T}] {quarter} PASS 1 完成 {p1}，PASS 2…", flush=True)
        p2 = run_pass(quarter, args.limit_per_quarter, log_path)

        accessions, rows, mapped = db_quarter_stats(quarter)
        if args.limit_per_quarter:
            status = "SMOKE"  # 带 --limit 的试跑不算完成，不进 COMPLETE 跳过集
        elif p1["exit"] != 0 or p2["exit"] != 0:
            status = "FAILED"
        elif (p2["pending"] or 0) == 0:
            status = "COMPLETE"
        elif (p2["failed"] or 0) == 0:
            status = "RESIDUAL_EMPTY"  # 剩余 pending 均为解析为空的文本 filing，预期内
        else:
            status = "FAILED"
        append_ledger(ledger, {
            "quarter": quarter,
            "started_at_utc": f"{started:%F %T}",
            "ended_at_utc": f"{datetime.now(timezone.utc):%F %T}",
            "discovered": p1["discovered"], "pass1_pending": p1["pending"],
            "pass1_failed": p1["failed"], "pass2_pending": p2["pending"],
            "pass2_failed": p2["failed"], "db_accessions": accessions,
            "db_rows": rows, "mapped_pct": mapped,
            "disk_free_gb": round(disk_free_gb(), 1), "status": status,
        })
        print(f"[{datetime.now(timezone.utc):%F %T}] ==== {quarter} {status} "
              f"accessions={accessions} rows={rows} mapped={mapped}% ====", flush=True)
        if status == "FAILED":
            print(f"{quarter} 两遍后仍有失败，继续下一季度（可稍后 --force 重跑本季）。", flush=True)

    print("全部季度处理完毕。", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
