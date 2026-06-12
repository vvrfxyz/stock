"""T0：用 SEC 官方 company_tickers.json 填充 CIK 身份映射。

- 写 security_identifiers（id_type='CIK', source='SEC'，幂等只插缺失行）；
- securities.cik 为空时回填（Massive 已填的 8.7k 不动）；
- Massive CIK 与 SEC CIK 不一致时只告警，留人工甄别，不覆盖。
"""
import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path

from loguru import logger
from sqlalchemy import update

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import Security
from data_sources.sec_edgar_source import SecEdgarSource, cik_to_10digit, normalize_cik
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 SEC ticker->CIK 身份映射。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    return parser


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("sync_sec_identifiers")
    create_parser().parse_args(argv)

    db_manager = None
    try:
        source = SecEdgarSource()
        sec_rows = source.fetch_ticker_cik_map()
        cik_by_ticker = {row["ticker"]: row["cik"] for row in sec_rows}
        logger.info("SEC company_tickers.json: {} 个 ticker 映射。", len(cik_by_ticker))

        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            securities = (
                session.query(Security.id, Security.symbol, Security.cik)
                .filter(Security.market == "US", Security.is_active == True)  # noqa: E712
                .all()
            )

            identifier_rows = []
            cik_backfills: dict[int, str] = {}
            conflicts = []
            matched = 0
            for sec in securities:
                sec_cik = cik_by_ticker.get(sec.symbol)
                if sec_cik is None:
                    continue
                matched += 1
                identifier_rows.append(
                    {
                        "security_id": sec.id,
                        "id_type": "CIK",
                        "id_value": cik_to_10digit(sec_cik),
                        "source": "SEC",
                        "confidence": "ticker_match",
                    }
                )
                existing_cik = normalize_cik(sec.cik)
                if existing_cik is None:
                    cik_backfills[sec.id] = cik_to_10digit(sec_cik)
                elif existing_cik != sec_cik:
                    conflicts.append((sec.symbol, sec.cik, cik_to_10digit(sec_cik)))

            inserted = db_manager.insert_missing_security_identifiers(identifier_rows)

            backfilled = 0
            if cik_backfills:
                with db_manager.engine.connect() as conn:
                    for security_id, cik in cik_backfills.items():
                        conn.execute(
                            update(Security)
                            .where(Security.id == security_id, Security.cik.is_(None))
                            .values(cik=cik)
                        )
                    conn.commit()
                backfilled = len(cik_backfills)

            logger.info("活跃证券 {}，按 symbol 命中 SEC 映射 {}，未命中 {}。",
                        len(securities), matched, len(securities) - matched)
            logger.info("security_identifiers 新插入 {} 行；securities.cik 回填 {} 个。", inserted, backfilled)
            if conflicts:
                logger.warning("Massive 与 SEC 的 CIK 不一致 {} 个（不覆盖，请甄别）：", len(conflicts))
                for symbol, massive_cik, sec_cik in conflicts[:20]:
                    logger.warning("  {}: massive={} sec={}", symbol, massive_cik, sec_cik)
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("sync_sec_identifiers 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
