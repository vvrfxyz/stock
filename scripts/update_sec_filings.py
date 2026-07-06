"""T1：SEC EDGAR filing 索引同步到 sec_filings。

数据源是 data.sec.gov/submissions/CIK{cik}.json（每证券 1 请求，最近 1000 条 filing；
--include-older-pages 时追加历史分页）。SEC 全局 ~8 req/s 节流，全市场扫一遍约 18 分钟。

默认只保留与基本面/内部人/机构持仓相关的 form：财报类（10-K/10-Q/8-K/20-F/40-F/6-K）、
代理书（DEF 14A）、内部人（3/4/5）、机构（13F-HR）、大额持股（SC 13D/G）。
--forms 可覆盖，--all-forms 全收。

多 security 共用一个 CIK（多类股，如 GOOG/GOOGL）时按 CIK 去重抓取，
filing 挂到 security_id 最小的那个；跨类查询一律用 cik 列 join。
"""
import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

from loguru import logger
from tqdm import tqdm

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import Security, SecurityIdentifier
from data_sources.sec_edgar_source import SecEdgarSource, normalize_cik
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

DEFAULT_FORMS = {
    "10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A",
    "20-F", "20-F/A", "40-F", "40-F/A", "6-K", "6-K/A",
    "DEF 14A", "DEFA14A",
    "3", "4", "5", "3/A", "4/A", "5/A",
    "13F-HR", "13F-HR/A",
    "SC 13D", "SC 13D/A", "SC 13G", "SC 13G/A",
    "25", "25/A", "25-NSE", "25-NSE/A",
}


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 SEC EDGAR filing 索引。")
    parser.add_argument("symbols", nargs="*", help="要处理的股票代码列表。")
    parser.add_argument("--all", action="store_true", help="处理所有有 CIK 的活跃证券。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理数量（测试用）。")
    parser.add_argument("--since", type=str, default=None,
                        help="只保留该日期（YYYY-MM-DD）之后的 filing；不传则收 recent 窗口全部。")
    parser.add_argument("--forms", type=str, default=None,
                        help="逗号分隔的 form 列表覆盖默认集，如 '10-K,10-Q,4'。")
    parser.add_argument("--all-forms", action="store_true", help="不过滤 form type。")
    parser.add_argument("--include-older-pages", action="store_true",
                        help="追加抓取 submissions 历史分页（深回填用，多数公司不需要）。")
    parser.add_argument("--include-inactive", action="store_true",
                        help="无 symbols 时不再限定 is_active——退市证券也纳入（Form 25 回拉等场景）。")
    return parser


def resolve_forms(args: argparse.Namespace) -> set[str] | None:
    if args.all_forms:
        return None
    if args.forms:
        return {f.strip().upper() for f in args.forms.split(",") if f.strip()}
    return set(DEFAULT_FORMS)


def get_target_securities(db_manager: DatabaseManager, args: argparse.Namespace) -> list:
    """返回 (id, symbol, cik) 列表。CIK 优先取 SEC 官方映射（security_identifiers,
    source='SEC'），其次 securities.cik（Massive，偶见给成子公司 CIK，见 sync 冲突告警）。"""
    with db_manager.get_session() as session:
        query = (
            session.query(Security.id, Security.symbol, Security.cik)
            .filter(Security.market == "US")
        )
        if args.symbols:
            query = query.filter(Security.symbol.in_([s.lower() for s in args.symbols]))
        elif not getattr(args, "include_inactive", False):
            query = query.filter(Security.is_active == True)  # noqa: E712
        query = query.order_by(Security.id.asc())
        if args.limit > 0:
            query = query.limit(args.limit)
        securities = query.all()

        sec_cik_by_security = dict(
            session.query(SecurityIdentifier.security_id, SecurityIdentifier.id_value)
            .filter(SecurityIdentifier.id_type == "CIK", SecurityIdentifier.source == "SEC")
            .all()
        )

    resolved = []
    for sec in securities:
        cik = sec_cik_by_security.get(sec.id) or sec.cik
        if cik:
            resolved.append(SimpleNamespace(id=sec.id, symbol=sec.symbol, cik=cik))
    return resolved


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("update_sec_filings")
    args = create_parser().parse_args(argv)
    if not args.symbols and not args.all:
        logger.error("请提供 symbols 或使用 --all。")
        return 1
    since = date.fromisoformat(args.since) if args.since else None
    forms = resolve_forms(args)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        securities = get_target_securities(db_manager, args)
        if not securities:
            logger.warning("没有可处理的证券（注意需要先有 CIK：python main.py sync_sec_identifiers）。")
            return 0

        # 多类股共用 CIK：按 CIK 去重，filing 挂 security_id 最小者。
        primary_by_cik: dict[str, tuple] = {}
        for sec in securities:
            cik = normalize_cik(sec.cik)
            if cik and cik not in primary_by_cik:
                primary_by_cik[cik] = sec
        logger.info("目标证券 {}，去重后 CIK {} 个，form 过滤: {}。",
                    len(securities), len(primary_by_cik),
                    "全部" if forms is None else f"{len(forms)} 种")

        source = SecEdgarSource()
        total_written = 0
        failed = 0
        for cik, sec in tqdm(primary_by_cik.items(), desc="同步 SEC filings"):
            try:
                rows = source.fetch_filings(
                    cik,
                    forms=forms,
                    since=since,
                    include_older_pages=args.include_older_pages,
                )
            except Exception as e:
                failed += 1
                logger.opt(exception=e).error("[{}] 拉取 submissions 失败: {}", sec.symbol, e)
                continue
            for row in rows:
                row["security_id"] = sec.id
                row["ticker"] = sec.symbol
            if rows:
                total_written += db_manager.upsert_sec_filings(rows)

        logger.info("--- SEC filings 同步统计 ---")
        logger.info("  CIK 处理: {}（失败 {}）", len(primary_by_cik), failed)
        logger.info("  filing 行写入/更新: {}", total_written)
        return 1 if failed and failed == len(primary_by_cik) else 0
    except Exception as e:
        logger.opt(exception=e).critical("update_sec_filings 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
