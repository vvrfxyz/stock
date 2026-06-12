"""用 SEC fails-to-deliver 文件建立 CUSIP -> security_id 身份映射。

13F information table 只给 CUSIP/issuer/class，不给 ticker；FTD 文件是免费官方的
CUSIP|SYMBOL 对照来源。流程：
1. 取最近 N 个月的 FTD 半月文件，合并唯一 (CUSIP, symbol) 对；
2. symbol 双重匹配回库内证券：精确匹配 + 去点回退（FTD 的 BRKB <-> 库内 brk.b），
   归一化后撞键的符号视为歧义跳过；
3. 写 security_identifiers（id_type='CUSIP', source='SEC_FTD'，幂等只插缺失行）；
4. 回填 institutional_holdings 中 security_id 为 NULL 的行（歧义 CUSIP 跳过）。

新 13F 增量在写入时即用该映射（update_institutional_holdings 的 load_cusip_map）。
"""
import argparse
import sys
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from loguru import logger

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import Security
from data_sources.sec_ftd_source import fetch_ftd_cusip_symbol_pairs, ftd_periods
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用 SEC FTD 数据同步 CUSIP 身份映射。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--months", type=int, default=3,
                        help="回看的 FTD 月数（每月 2 个半月文件；初次回填建议 12）。")
    return parser


def build_symbol_maps(securities: list) -> tuple[dict[str, int], dict[str, int]]:
    """返回 (exact, dotless)。dotless 只收含点 symbol 的去点形式，
    与精确键或其它去点键冲突的一律剔除（歧义不映射）。"""
    exact = {sec.symbol: sec.id for sec in securities}
    dotless: dict[str, int] = {}
    collided: set[str] = set()
    for sec in securities:
        if "." not in sec.symbol:
            continue
        key = sec.symbol.replace(".", "")
        if key in exact or key in collided:
            continue
        if key in dotless:
            del dotless[key]
            collided.add(key)
            continue
        dotless[key] = sec.id
    return exact, dotless


def resolve_cusip_map(
    pairs: set[tuple[str, str]],
    exact: dict[str, int],
    dotless: dict[str, int],
) -> tuple[dict[str, int], int, int]:
    """(cusip, symbol) 对 -> {cusip: security_id}。
    返回 (映射, 未匹配 symbol 数, 歧义 cusip 数)。"""
    by_cusip: dict[str, set[int]] = defaultdict(set)
    unmatched = 0
    for cusip, symbol in pairs:
        security_id = exact.get(symbol) or dotless.get(symbol)
        if security_id is None:
            unmatched += 1
            continue
        by_cusip[cusip].add(security_id)

    resolved = {}
    ambiguous = 0
    for cusip, ids in by_cusip.items():
        if len(ids) == 1:
            resolved[cusip] = next(iter(ids))
        else:
            ambiguous += 1
    return resolved, unmatched, ambiguous


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("sync_cusip_identifiers")
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        pairs: set[tuple[str, str]] = set()
        fetched = 0
        for yyyymm, half in ftd_periods(args.months):
            file_pairs = fetch_ftd_cusip_symbol_pairs(yyyymm, half)
            if file_pairs is None:
                logger.info("FTD {}{} 尚未发布，跳过。", yyyymm, half)
                continue
            fetched += 1
            pairs.update(file_pairs)
        logger.info("FTD 文件 {} 个，唯一 (CUSIP, symbol) 对 {} 个。", fetched, len(pairs))
        if not pairs:
            logger.warning("没有可用的 FTD 数据。")
            return 1

        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            securities = (
                session.query(Security.id, Security.symbol)
                .filter(Security.market == "US")
                .all()
            )
        exact, dotless = build_symbol_maps(securities)
        cusip_map, unmatched, ambiguous = resolve_cusip_map(pairs, exact, dotless)
        logger.info(
            "解析映射 {} 个 CUSIP（symbol 未匹配 {}，歧义跳过 {}）。",
            len(cusip_map), unmatched, ambiguous,
        )

        identifier_rows = [
            {
                "security_id": security_id,
                "id_type": "CUSIP",
                "id_value": cusip,
                "source": "SEC_FTD",
                "confidence": "ftd_symbol_match",
            }
            for cusip, security_id in cusip_map.items()
        ]
        inserted = db_manager.insert_missing_security_identifiers(identifier_rows)
        backfilled = db_manager.map_unlinked_holdings_to_securities()
        logger.info(
            "security_identifiers 新插入 {} 行；institutional_holdings 回填 security_id {} 行。",
            inserted, backfilled,
        )
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("sync_cusip_identifiers 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
