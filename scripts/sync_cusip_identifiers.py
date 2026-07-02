"""用 SEC fails-to-deliver 文件建立 CUSIP -> security_id 身份映射。

13F information table 只给 CUSIP/issuer/class，不给 ticker；FTD 文件是免费官方的
CUSIP|SYMBOL 对照来源。流程：
1. 取最近 N 个月的 FTD 半月文件，合并唯一 (CUSIP, symbol) 对，并记录每对
   最早出现的半月覆盖期起始日（作为身份行的 PIT start_date）；
2. symbol 双重匹配回库内活跃证券：精确匹配 + 去点回退（FTD 的 BRKB <-> 库内
   brk.b），两条路径口径一致——撞键的符号一律视为歧义剔除；
3. FTD 里的 symbol 是最多数月前的历史观测，回看窗口内发生过 RENAME/RECYCLE/
   QUARANTINE 身份事件的 symbol 用当前快照解析必然有错链风险（回收隔离期旧行
   仍 active 占着 symbol），整体跳过，等身份尘埃落定后的下次运行再补；
4. 写 security_identifiers（id_type='CUSIP', source='SEC_FTD'，带 start_date，
   幂等只插缺失行）；
5. 回填 institutional_holdings 中 security_id 为 NULL 的行（歧义 CUSIP 跳过）。

新 13F 增量在写入时即用该映射（update_institutional_holdings 的 load_cusip_map）。
存量错链行由 audit_security_identity 反向校验 + repair_cusip_links 清理。
"""
import argparse
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_models.models import Security
from data_sources.sec_ftd_source import fetch_ftd_cusip_symbol_pairs, ftd_periods
from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

# 这些身份事件意味着 symbol 的归属在事件前后发生（或疑似发生）了变化，
# 用当前 symbol 快照解析历史 FTD 观测不再可靠。
IDENTITY_UNSTABLE_EVENT_TYPES = ("RENAME", "RECYCLE", "QUARANTINE")

# 发布滞后（约 1 个月）+ 半月覆盖期 + 余量：身份事件回看窗口在 FTD 数据
# 覆盖期（months 个月）之外还要加的天数。
FTD_PUBLICATION_LAG_DAYS = 45


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用 SEC FTD 数据同步 CUSIP 身份映射。")
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--months", type=int, default=3,
                        help="回看的 FTD 月数（每月 2 个半月文件；初次回填建议 12）。")
    return parser


def ftd_period_start(yyyymm: str, half: str) -> date:
    """半月文件覆盖期的起始日（a=1 日，b=16 日），作为该期观测的 PIT 下界。"""
    return date(int(yyyymm[:4]), int(yyyymm[4:6]), 1 if half == "a" else 16)


def build_symbol_maps(securities: list) -> tuple[dict[str, int], dict[str, int]]:
    """返回 (exact, dotless)。两条路径口径一致：撞键 symbol 一律剔除（歧义不映射）。
    dotless 只收含点 symbol 的去点形式。"""
    exact: dict[str, int] = {}
    exact_collided: set[str] = set()
    for sec in securities:
        if sec.symbol in exact_collided:
            continue
        if sec.symbol in exact:
            del exact[sec.symbol]
            exact_collided.add(sec.symbol)
            logger.warning("symbol={} 对应多个 security_id，歧义剔除，不参与 FTD 匹配。", sec.symbol)
            continue
        exact[sec.symbol] = sec.id

    dotless: dict[str, int] = {}
    collided: set[str] = set()
    for sec in securities:
        if "." not in sec.symbol or sec.symbol in exact_collided:
            continue
        key = sec.symbol.replace(".", "")
        if key in exact or key in exact_collided or key in collided:
            continue
        if key in dotless:
            del dotless[key]
            collided.add(key)
            continue
        dotless[key] = sec.id
    return exact, dotless


def load_unstable_symbols(session, since: datetime) -> set[str]:
    """回看窗口内出现过身份事件的 symbol 集合（含去点形式，全小写）。"""
    rows = session.execute(
        text(
            """
            SELECT old_symbol, new_symbol
            FROM security_identity_events
            WHERE event_type = ANY(:event_types)
              AND created_at >= :since
            """
        ),
        {"event_types": list(IDENTITY_UNSTABLE_EVENT_TYPES), "since": since},
    ).all()
    symbols: set[str] = set()
    for old_symbol, new_symbol in rows:
        for symbol in (old_symbol, new_symbol):
            if not symbol:
                continue
            symbol = symbol.lower()
            symbols.add(symbol)
            symbols.add(symbol.replace(".", ""))
    return symbols


def resolve_cusip_map(
    pairs: set[tuple[str, str]],
    exact: dict[str, int],
    dotless: dict[str, int],
    unstable_symbols: set[str] | frozenset[str] = frozenset(),
    pair_first_seen: dict[tuple[str, str], date] | None = None,
) -> tuple[dict[str, int], dict[str, date], int, int, int]:
    """(cusip, symbol) 对 -> {cusip: security_id}。
    返回 (映射, cusip->最早观测期起始日, 未匹配 symbol 数, 歧义 cusip 数, 身份不稳跳过数)。

    unstable_symbols 里的 symbol 在匹配前整对跳过；start_date 只取参与匹配成功
    的 (cusip, symbol) 对的最早覆盖期起始日。"""
    by_cusip: dict[str, set[int]] = defaultdict(set)
    first_seen: dict[str, date] = {}
    unmatched = 0
    skipped_unstable = 0
    for cusip, symbol in pairs:
        if symbol in unstable_symbols:
            skipped_unstable += 1
            continue
        security_id = exact.get(symbol) or dotless.get(symbol)
        if security_id is None:
            unmatched += 1
            continue
        by_cusip[cusip].add(security_id)
        if pair_first_seen:
            seen = pair_first_seen.get((cusip, symbol))
            if seen is not None and (cusip not in first_seen or seen < first_seen[cusip]):
                first_seen[cusip] = seen

    resolved = {}
    ambiguous = 0
    for cusip, ids in by_cusip.items():
        if len(ids) == 1:
            resolved[cusip] = next(iter(ids))
        else:
            ambiguous += 1
    start_dates = {cusip: first_seen[cusip] for cusip in resolved if cusip in first_seen}
    return resolved, start_dates, unmatched, ambiguous, skipped_unstable


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("sync_cusip_identifiers")
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        pairs: set[tuple[str, str]] = set()
        pair_first_seen: dict[tuple[str, str], date] = {}
        fetched = 0
        for yyyymm, half in ftd_periods(args.months):
            file_pairs = fetch_ftd_cusip_symbol_pairs(yyyymm, half)
            if file_pairs is None:
                logger.info("FTD {}{} 尚未发布，跳过。", yyyymm, half)
                continue
            fetched += 1
            period_start = ftd_period_start(yyyymm, half)
            for pair in file_pairs:
                prev = pair_first_seen.get(pair)
                if prev is None or period_start < prev:
                    pair_first_seen[pair] = period_start
            pairs.update(file_pairs)
        logger.info("FTD 文件 {} 个，唯一 (CUSIP, symbol) 对 {} 个。", fetched, len(pairs))
        if not pairs:
            logger.warning("没有可用的 FTD 数据。")
            return 1

        db_manager = DatabaseManager()
        with db_manager.get_session() as session:
            securities = (
                session.query(Security.id, Security.symbol)
                .filter(Security.market == "US", Security.is_active.is_(True))
                .all()
            )
            # 窗口 = FTD 数据覆盖期（months 个月）+ 发布滞后的保守并集：
            # 窗口内任何一天的观测都可能落在事件前后，无法用当前快照定归属。
            since = datetime.now(timezone.utc) - timedelta(
                days=args.months * 31 + FTD_PUBLICATION_LAG_DAYS
            )
            unstable_symbols = load_unstable_symbols(session, since)
        if unstable_symbols:
            logger.info("回看窗口内身份不稳的 symbol {} 个（含去点形式），FTD 匹配跳过。", len(unstable_symbols))
        exact, dotless = build_symbol_maps(securities)
        cusip_map, start_dates, unmatched, ambiguous, skipped_unstable = resolve_cusip_map(
            pairs, exact, dotless,
            unstable_symbols=unstable_symbols,
            pair_first_seen=pair_first_seen,
        )
        logger.info(
            "解析映射 {} 个 CUSIP（symbol 未匹配 {}，歧义跳过 {}，身份不稳跳过 {}）。",
            len(cusip_map), unmatched, ambiguous, skipped_unstable,
        )

        identifier_rows = [
            {
                "security_id": security_id,
                "id_type": "CUSIP",
                "id_value": cusip,
                "source": "SEC_FTD",
                "confidence": "ftd_symbol_match",
                # PIT 边界：该 CUSIP 在 FTD 数据中最早被观测到的半月覆盖期起始日
                "start_date": start_dates.get(cusip),
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
