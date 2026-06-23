import argparse
import json
import os
import sys
import time
from datetime import timedelta

from loguru import logger
from sqlalchemy import func, update
from tqdm import tqdm

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.key_rate_limiter import KeyRateLimiter
from utils.massive_config import (
    ALLOWED_US_SECURITY_TYPES,
    MASSIVE_RATE_LIMIT,
    MASSIVE_RATE_SECONDS,
    enforce_us_market,
    get_massive_api_keys,
)
from utils.script_logging import setup_logging as configure_script_logging
from utils.security_identity import SecurityIdentityResolver


def setup_logging():
    configure_script_logging("sync_massive_universe")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="同步 Massive 活跃美股 universe，只保留普通股 / ETF。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--market", type=str, default="US", help="市场，当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制处理的 ticker 数量，用于测试。")
    parser.add_argument(
        "--skip-mark-missing-inactive",
        action="store_true",
        help="跳过将 Massive 活跃列表之外的保留类型证券标记为 inactive。",
    )
    return parser


def _classify_incoming(resolver, upsert_rows):
    """用 resolver 分类每一条 incoming row: rename / recycle / normal。"""
    rename_rows = []
    recycle_rows = []
    normal_rows = []

    results = resolver.resolve_batch(upsert_rows)
    for row, result in zip(upsert_rows, results):
        if result.is_rename:
            rename_rows.append((row, result))
        elif result.is_recycle:
            recycle_rows.append((row, result))
        else:
            normal_rows.append(row)

    return rename_rows, recycle_rows, normal_rows, results


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args(argv)

    db_manager = None
    source = None
    try:
        enforce_us_market(args.market)
        api_keys = get_massive_api_keys()
        rate_limiter = KeyRateLimiter(api_keys, MASSIVE_RATE_LIMIT, MASSIVE_RATE_SECONDS, scope="massive")
        source = MassiveSource(rate_limiter=rate_limiter)
        db_manager = DatabaseManager()

        reference_rows = source.list_active_tickers(allowed_types=ALLOWED_US_SECURITY_TYPES)
        if args.limit > 0:
            reference_rows = reference_rows[: args.limit]
        if not reference_rows:
            logger.warning("Massive 未返回任何可保留的活跃 US ticker。")
            return 0

        upsert_rows = [source._build_reference_payload(item) for item in tqdm(reference_rows, desc="整理 ticker 引用数据")]

        # --- 身份解析 ---
        with db_manager.get_session() as session:
            resolver = SecurityIdentityResolver(session)

        rename_rows, recycle_rows, normal_rows, _ = _classify_incoming(resolver, upsert_rows)

        # 1) 处理改名：更新 symbol + 写 history + 写事件
        identity_events = []
        for row, result in rename_rows:
            existing_symbol = resolver._existing_symbol(result.security_id)
            db_manager.rename_security(
                result.security_id,
                old_symbol=existing_symbol or "",
                new_symbol=row["symbol"],
                exchange=row.get("exchange"),
            )
            identity_events.append({
                "security_id": result.security_id,
                "event_type": "RENAME",
                "old_symbol": existing_symbol,
                "new_symbol": row["symbol"],
                "resolution_source": "AUTO",
                "confidence": result.confidence,
                "details": json.dumps({
                    "matched_field": result.matched_field,
                    "incoming_figi": row.get("composite_figi"),
                    "incoming_cik": row.get("cik"),
                }, ensure_ascii=False),
            })
            # 改名后用 upsert_security_info (以 id 为键) 更新其余元数据，
            # 绕过 upsert_securities_by_symbol 的内层 FIGI/CIK 冲突检测。
            row_with_id = {**row, "id": result.security_id}
            db_manager.upsert_security_info(row_with_id)

        if rename_rows:
            logger.info("检测到 {} 只证券改名，已自动更新 symbol。", len(rename_rows))

        # 2) 处理回收：quarantine + 写事件，不 upsert
        for row, result in recycle_rows:
            identity_events.append({
                "security_id": result.security_id,
                "event_type": "RECYCLE",
                "old_symbol": row["symbol"],
                "new_symbol": row["symbol"],
                "resolution_source": "AUTO",
                "confidence": result.confidence,
                "details": json.dumps({
                    "matched_field": result.matched_field,
                    "incoming_figi": row.get("composite_figi"),
                    "incoming_cik": row.get("cik"),
                    "action": "quarantined — incoming identity differs from active row",
                }, ensure_ascii=False),
            })
            logger.warning(
                "跳过 symbol={} 的 upsert：同代码但身份不一致（疑似 ticker 回收），"
                "incoming figi={} cik={} != 既有 security_id={}。",
                row["symbol"],
                row.get("composite_figi"),
                row.get("cik"),
                result.security_id,
            )

        if recycle_rows:
            logger.warning("跳过 {} 条疑似 ticker 回收的 securities upsert。", len(recycle_rows))

        # 3) 写身份事件
        if identity_events:
            db_manager.insert_identity_events(identity_events)

        # 4) 正常 upsert（含新上市 + 已有证券更新 + 改名后的元数据更新）
        changed = db_manager.upsert_securities_by_symbol(normal_rows, touch_info_timestamp=False)

        # 5) 标记不在活跃列表中的证券为 inactive
        active_symbols = {row["symbol"] for row in upsert_rows}
        marked_inactive = 0
        should_mark_missing_inactive = not args.skip_mark_missing_inactive and args.limit == 0
        if not should_mark_missing_inactive and args.limit > 0 and not args.skip_mark_missing_inactive:
            logger.warning("检测到 --limit，已自动跳过 missing->inactive 标记，避免测试范围外数据被误伤。")
        if should_mark_missing_inactive:
            with db_manager.get_session() as session:
                stmt = (
                    update(Security)
                    .where(func.upper(Security.market) == "US")
                    .where(func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES))
                    .where(Security.is_active == True)
                    .where(~Security.symbol.in_(active_symbols))
                    .values(is_active=False)
                )
                result = session.execute(stmt)
                session.commit()
                marked_inactive = result.rowcount or 0

        logger.success(
            "Massive universe 同步完成: fetched={} upserted={} renamed={} recycled={} marked_inactive={}",
            len(upsert_rows),
            changed,
            len(rename_rows),
            len(recycle_rows),
            marked_inactive,
        )
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("sync_massive_universe 执行失败: {}", e)
        return 1
    finally:
        if source:
            source.close()
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
