"""T2：SEC XBRL companyfacts -> sec_fundamental_facts（curated 概念，point-in-time）。

三种取数模式：
1. symbols / --all      ：逐 CIK 调 companyfacts API（~8 req/s；全市场约 45 分钟、传输 ~10GB，
                          仅建议小批量或增量）。
2. --since YYYY-MM-DD   ：与 --all 连用的增量——只处理 sec_filings 中该日后有财报类
                          filing 的 CIK（周度通常几百个，分钟级）。事实行同时按
                          filed >= since 过滤。
3. --bulk-zip PATH      ：从本地 companyfacts.zip（SEC 官方每夜全量包，~1.4GB）流式解析，
                          初次回填用。下载：
                          curl -H "User-Agent: $SEC_USER_AGENT" -o companyfacts.zip \
                            https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip

概念白名单见 utils/sec_concepts.py。多类股共用 CIK 时事实挂 security_id 最小者，
跨类查询用 cik 列 join（与 sec_filings 同一约定）。
"""
import argparse
import io
import json
import sys
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

from loguru import logger
from sqlalchemy import text
from tqdm import tqdm

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import Security, SecurityIdentifier
from data_sources.sec_edgar_source import SecEdgarSource, cik_to_10digit, parse_company_facts
from db_manager import DatabaseManager
from utils.sec_concepts import CURATED_CONCEPTS
from utils.script_logging import setup_logging as configure_script_logging

FINANCIAL_FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F", "6-K", "8-K")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 SEC XBRL curated 基本面事实。")
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理所有有 CIK 的活跃证券。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量（测试用）。")
    parser.add_argument("--since", type=str, default=None,
                        help="增量：只处理该日后有财报类 filing 的 CIK，且只保留 filed >= 该日的事实。")
    parser.add_argument("--bulk-zip", type=str, default=None,
                        help="本地 companyfacts.zip 路径；初次回填用，跳过逐 CIK API。")
    return parser


def resolve_cik_map(db_manager: DatabaseManager, args: argparse.Namespace) -> dict[str, SimpleNamespace]:
    """返回 {cik10: primary_security}。CIK 取 SEC 映射优先、Massive 回退；
    共用 CIK 时取 security_id 最小者为 primary。"""
    with db_manager.get_session() as session:
        query = session.query(Security.id, Security.symbol, Security.cik).filter(Security.market == "US")
        if args.symbols:
            query = query.filter(Security.symbol.in_([s.lower() for s in args.symbols]))
        else:
            query = query.filter(Security.is_active == True)  # noqa: E712
        securities = query.order_by(Security.id.asc()).all()
        sec_cik_by_security = dict(
            session.query(SecurityIdentifier.security_id, SecurityIdentifier.id_value)
            .filter(SecurityIdentifier.id_type == "CIK", SecurityIdentifier.source == "SEC")
            .all()
        )

    cik_map: dict[str, SimpleNamespace] = {}
    for sec in securities:
        cik10 = cik_to_10digit(sec_cik_by_security.get(sec.id) or sec.cik)
        if cik10 and cik10 not in cik_map:
            cik_map[cik10] = SimpleNamespace(id=sec.id, symbol=sec.symbol)
    if args.limit > 0:
        cik_map = dict(list(cik_map.items())[: args.limit])
    return cik_map


def filter_ciks_with_recent_filings(db_manager: DatabaseManager, cik_map: dict, since: date) -> dict:
    """增量模式：用 sec_filings 索引筛出 since 之后有财报类 filing 的 CIK。"""
    with db_manager.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DISTINCT cik FROM sec_filings "
                "WHERE filing_date >= :since AND form_type = ANY(:forms) AND cik IS NOT NULL"
            ),
            {"since": since, "forms": list(FINANCIAL_FORMS)},
        ).fetchall()
    recent = {r.cik for r in rows}
    return {cik: sec for cik, sec in cik_map.items() if cik in recent}


def process_rows(db_manager: DatabaseManager, rows: list[dict], security_id: int) -> int:
    for row in rows:
        row["security_id"] = security_id
    return db_manager.upsert_sec_fundamental_facts(rows) if rows else 0


def run_bulk_zip(db_manager: DatabaseManager, cik_map: dict, zip_path: Path, filed_since: date | None) -> tuple[int, int]:
    """流式解析官方全量包：按文件名先筛 CIK，再解析 JSON，单公司一批写库。"""
    total_rows = 0
    matched = 0
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".json")]
        wanted = [n for n in names if n.removesuffix(".json").removeprefix("CIK") in {c for c in cik_map}]
        logger.info("zip 内 {} 个公司文件，命中 universe {} 个。", len(names), len(wanted))
        for name in tqdm(wanted, desc="解析 companyfacts.zip"):
            cik10 = name.removesuffix(".json").removeprefix("CIK")
            try:
                with zf.open(name) as fh:
                    payload = json.load(io.TextIOWrapper(fh, encoding="utf-8"))
            except Exception as e:
                logger.opt(exception=e).error("[{}] 解析失败: {}", name, e)
                continue
            rows = parse_company_facts(payload, cik10, concepts=CURATED_CONCEPTS, filed_since=filed_since)
            total_rows += process_rows(db_manager, rows, cik_map[cik10].id)
            matched += 1
    return matched, total_rows


def run_api(db_manager: DatabaseManager, cik_map: dict, filed_since: date | None) -> tuple[int, int, int]:
    source = SecEdgarSource()
    total_rows = 0
    failed = 0
    for cik10, sec in tqdm(cik_map.items(), desc="同步 SEC fundamentals"):
        try:
            rows = source.fetch_fundamental_facts(cik10, concepts=CURATED_CONCEPTS, filed_since=filed_since)
        except Exception as e:
            failed += 1
            logger.opt(exception=e).error("[{}] 拉取 companyfacts 失败: {}", sec.symbol, e)
            continue
        total_rows += process_rows(db_manager, rows, sec.id)
    return len(cik_map), total_rows, failed


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_sec_fundamentals")
    args = create_parser().parse_args(argv)
    if not args.symbols and not args.all and not args.bulk_zip:
        logger.error("请提供 symbols，或使用 --all / --bulk-zip。")
        return 1
    since = date.fromisoformat(args.since) if args.since else None

    db_manager = None
    try:
        db_manager = DatabaseManager()
        cik_map = resolve_cik_map(db_manager, args)
        if not cik_map:
            logger.warning("没有可处理的证券（需要先有 CIK：python main.py sync_sec_identifiers）。")
            return 0

        if args.bulk_zip:
            zip_path = Path(args.bulk_zip)
            if not zip_path.exists():
                logger.error("bulk zip 不存在: {}", zip_path)
                return 1
            processed, total_rows = run_bulk_zip(db_manager, cik_map, zip_path, since)
            failed = 0
        else:
            if since and args.all:
                before = len(cik_map)
                cik_map = filter_ciks_with_recent_filings(db_manager, cik_map, since)
                logger.info("增量模式：{} 个 CIK 中 {} 个自 {} 起有财报类 filing。", before, len(cik_map), since)
            processed, total_rows, failed = run_api(db_manager, cik_map, since)

        logger.info("--- SEC fundamentals 同步统计 ---")
        logger.info("  CIK 处理: {}（失败 {}）", processed, failed)
        logger.info("  事实行写入/更新: {}", total_rows)
        return 1 if failed and failed == processed else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_sec_fundamentals 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
