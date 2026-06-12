"""T4：13F-HR -> institutional_holdings（机构持仓明细）。

13-F 由机构 filer 提交，不出现在 issuer 维度的 sec_filings 索引里；发现通道是
EDGAR form index：
- 增量：daily-index（form.YYYYMMDD.idx），--since 起逐日扫（非工作日自动跳过）。
- 回填：--quarter 2026Q1 走 full-index 的季度 form.idx。

每个 filing 抓一次全文提交 .txt（含 primary_doc + information table），~8 req/s。
持仓行以 CUSIP/issuer/class 为主键素材；security_id 仅在 security_identifiers
有 CUSIP 映射时回填，否则为空（映射层校验后再补，见 docs/architecture.md）。
注意 value 单位：2023-01 之前申报为千美元，此后为美元——按申报原值存储，不换算。
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger
from sqlalchemy import text
from tqdm import tqdm

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import SecurityIdentifier
from data_sources.sec_edgar_source import (
    SecEdgarSource,
    parse_form_index,
    parse_thirteenf_submission,
)
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

FORM_13F_TYPES = {"13F-HR", "13F-HR/A"}


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 SEC 13F-HR 机构持仓明细。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--since", type=str, default=None,
                        help="按日扫 daily form index 的起始日期（YYYY-MM-DD）。")
    parser.add_argument("--quarter", type=str, default=None,
                        help="季度全量回填，如 2026Q1（走 full-index，单季约 8 千个 filing）。")
    parser.add_argument("--filer-cik", type=str, default=None, help="只处理该 filer CIK（调试用）。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理 filing 数量。")
    parser.add_argument("--reparse", action="store_true", help="重新解析已入库的 filing。")
    return parser


def discover_filings(source: SecEdgarSource, args: argparse.Namespace) -> list[dict]:
    refs: list[dict] = []
    if args.quarter:
        year, quarter = args.quarter.upper().split("Q")
        index_text = source.fetch_quarterly_form_index(int(year), int(quarter))
        refs.extend(parse_form_index(index_text, FORM_13F_TYPES))
    if args.since:
        day = date.fromisoformat(args.since)
        today = date.today()
        while day <= today:
            if day.weekday() < 5:
                index_text = source.fetch_daily_form_index(day)
                if index_text:
                    refs.extend(parse_form_index(index_text, FORM_13F_TYPES))
            day += timedelta(days=1)

    deduped = {ref["accession_number"]: ref for ref in refs}
    refs = list(deduped.values())
    if args.filer_cik:
        target = args.filer_cik.lstrip("0")
        refs = [r for r in refs if (r["filer_cik"] or "").lstrip("0") == target]
    refs.sort(key=lambda r: (r["filing_date"], r["accession_number"]))
    return refs


def filter_pending(db_manager: DatabaseManager, refs: list[dict]) -> list[dict]:
    """跳过已有持仓行的 accession（幂等增量）。"""
    if not refs:
        return []
    accessions = [r["accession_number"] for r in refs]
    existing: set[str] = set()
    with db_manager.engine.connect() as conn:
        for start in range(0, len(accessions), 5000):
            chunk = accessions[start:start + 5000]
            result = conn.execute(
                text(
                    "SELECT DISTINCT accession_number FROM institutional_holdings "
                    "WHERE source = 'SEC_EDGAR' AND accession_number = ANY(:accessions)"
                ),
                {"accessions": chunk},
            )
            existing.update(row.accession_number for row in result)
    return [r for r in refs if r["accession_number"] not in existing]


def load_cusip_map(db_manager: DatabaseManager) -> dict[str, int]:
    with db_manager.get_session() as session:
        rows = (
            session.query(SecurityIdentifier.id_value, SecurityIdentifier.security_id)
            .filter(SecurityIdentifier.id_type == "CUSIP")
            .all()
        )
    return {value.upper(): security_id for value, security_id in rows}


def process_filing(
    ref: dict,
    source: SecEdgarSource,
    db_manager: DatabaseManager,
    cusip_map: dict[str, int],
) -> tuple[str, int]:
    submission_text = source.fetch_full_submission(ref["file_path"])
    rows = parse_thirteenf_submission(submission_text, ref["accession_number"])
    if not rows:
        return "SUCCESS_EMPTY", 0
    for row in rows:
        row.setdefault("filer_cik", ref["filer_cik"])
        row["filer_cik"] = row["filer_cik"] or ref["filer_cik"]
        row["form_type"] = row.get("form_type") or ref["form_type"]
        row["filing_date"] = ref["filing_date"]
        cusip = (row.get("cusip") or "").upper()
        if cusip in cusip_map:
            row["security_id"] = cusip_map[cusip]
    written = db_manager.upsert_institutional_holdings(rows)
    return "SUCCESS", written


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_institutional_holdings")
    args = create_parser().parse_args(argv)
    if not args.since and not args.quarter:
        logger.error("请提供 --since（日增量）或 --quarter（季度回填）。")
        return 1

    db_manager = None
    try:
        db_manager = DatabaseManager()
        source = SecEdgarSource()

        refs = discover_filings(source, args)
        logger.info("form index 发现 13F filing {} 个。", len(refs))
        if not args.reparse:
            refs = filter_pending(db_manager, refs)
        if args.limit > 0:
            refs = refs[: args.limit]
        if not refs:
            logger.success("没有待处理的 13F filing。")
            return 0
        logger.info("待处理 13F filing: {} 个。", len(refs))

        cusip_map = load_cusip_map(db_manager)
        logger.info("security_identifiers CUSIP 映射: {} 条。", len(cusip_map))

        counters: dict[str, int] = {}
        total_rows = 0
        failed = 0
        for ref in tqdm(refs, desc="同步 13F holdings"):
            try:
                status, written = process_filing(ref, source, db_manager, cusip_map)
            except Exception as e:
                failed += 1
                logger.opt(exception=e).error("[{}] 处理失败: {}", ref["accession_number"], e)
                continue
            counters[status] = counters.get(status, 0) + 1
            total_rows += written

        logger.info("--- 13F holdings 同步统计 ---")
        logger.info("  filing 处理: {}（失败 {}）", len(refs), failed)
        for status, count in sorted(counters.items()):
            logger.info("  {}: {}", status, count)
        logger.info("  持仓行写入/更新: {}", total_rows)
        return 1 if failed and failed == len(refs) else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_institutional_holdings 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
