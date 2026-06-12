"""T3：Form 3/4/5 ownership XML -> insider_transactions（point-in-time 内部人交易明细）。

取数路径：sec_filings 里已索引的 Form 3/4/5（含 /A）按 accession 找原始
ownershipDocument XML（去掉 primaryDocument 的 xsl 渲染前缀），解析后写
insider_transactions。先跑 update_sec_filings 建好索引。

- 默认只处理库内还没有明细行的 filing（pending）；--reparse 强制重解析。
- --since 限制 filing_date 范围（与 scheduled_update 周度增量配合）。
- 对策略最有价值的是公开市场买卖（transaction_code P/S）；授予(A)、行权(M)、
  赠与(G)、税务代扣(F) 等语义不同，消费端必须按 transaction_code 分层。
- 多 owner 合并申报按 owner 复制行；security_id 取 filing 挂的 primary security。
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from loguru import logger
from tqdm import tqdm

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import InsiderTransaction, SecFiling, Security
from data_sources.sec_edgar_source import SecEdgarSource, parse_ownership_document
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

OWNERSHIP_FORMS = ("3", "4", "5", "3/A", "4/A", "5/A")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="解析 SEC Form 3/4/5 内部人交易明细。")
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理 sec_filings 中全部待解析的 Form 3/4/5。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理 filing 数量（测试用）。")
    parser.add_argument("--since", type=str, default=None,
                        help="只处理 filing_date >= 该日期（YYYY-MM-DD）的 filing。")
    parser.add_argument("--forms", type=str, default=None,
                        help="逗号分隔的 form 过滤，默认 3,4,5 及其 /A。")
    parser.add_argument("--reparse", action="store_true",
                        help="重解析已有明细行的 filing（解析器升级后回灌用）。")
    return parser


def get_pending_filings(db_manager: DatabaseManager, args: argparse.Namespace) -> list:
    """返回待解析的 SecFiling 行（含 id/accession/primary_document_url/...）。"""
    forms = (
        {f.strip().upper() for f in args.forms.split(",") if f.strip()}
        if args.forms else set(OWNERSHIP_FORMS)
    )
    with db_manager.get_session() as session:
        query = (
            session.query(
                SecFiling.id,
                SecFiling.security_id,
                SecFiling.accession_number,
                SecFiling.filing_date,
                SecFiling.primary_document_url,
                SecFiling.ticker,
            )
            .filter(SecFiling.source == "SEC_EDGAR")
            .filter(SecFiling.form_type.in_(forms))
            .filter(SecFiling.primary_document_url.isnot(None))
        )
        if args.symbols:
            symbols = [s.lower() for s in args.symbols]
            security_ids = [
                row.id for row in
                session.query(Security.id).filter(Security.symbol.in_(symbols)).all()
            ]
            query = query.filter(SecFiling.security_id.in_(security_ids))
        if args.since:
            query = query.filter(SecFiling.filing_date >= date.fromisoformat(args.since))
        if not args.reparse:
            parsed = session.query(InsiderTransaction.accession_number).filter(
                InsiderTransaction.source == "SEC_EDGAR"
            ).distinct().subquery()
            query = query.filter(SecFiling.accession_number.notin_(parsed.select()))
        query = query.order_by(SecFiling.filing_date.desc(), SecFiling.id.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def process_filing(filing, source: SecEdgarSource, db_manager: DatabaseManager) -> tuple[str, int]:
    """返回 (status, row_count)。无 XML 的早期 filing 记 SKIPPED_NO_XML。"""
    xml_text = source.fetch_ownership_document(filing.primary_document_url)
    if xml_text is None:
        return "SKIPPED_NO_XML", 0
    rows = parse_ownership_document(xml_text, filing.accession_number)
    if not rows:
        return "SUCCESS_EMPTY", 0
    for row in rows:
        row["filing_id"] = filing.id
        row["security_id"] = filing.security_id
        row["filing_date"] = filing.filing_date
    written = db_manager.upsert_insider_transactions(rows)
    return "SUCCESS", written


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_insider_transactions")
    args = create_parser().parse_args(argv)
    if not args.symbols and not args.all:
        logger.error("请提供 symbols 或使用 --all。")
        return 1

    db_manager = None
    try:
        db_manager = DatabaseManager()
        filings = get_pending_filings(db_manager, args)
        if not filings:
            logger.success("没有待解析的 Form 3/4/5 filing（先确认已运行 update_sec_filings）。")
            return 0
        logger.info("待解析 Form 3/4/5 filing: {} 个。", len(filings))

        source = SecEdgarSource()
        counters: dict[str, int] = {}
        total_rows = 0
        failed = 0
        for filing in tqdm(filings, desc="解析 Form 3/4/5"):
            try:
                status, written = process_filing(filing, source, db_manager)
            except Exception as e:
                failed += 1
                logger.opt(exception=e).error(
                    "[{}] 解析失败: {}", filing.accession_number, e
                )
                continue
            counters[status] = counters.get(status, 0) + 1
            total_rows += written

        logger.info("--- insider transactions 解析统计 ---")
        logger.info("  filing 处理: {}（失败 {}）", len(filings), failed)
        for status, count in sorted(counters.items()):
            logger.info("  {}: {}", status, count)
        logger.info("  明细行写入/更新: {}", total_rows)
        return 1 if failed and failed == len(filings) else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_insider_transactions 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
