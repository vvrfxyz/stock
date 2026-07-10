import argparse
import os
import sys
import time
from collections import Counter
from datetime import timedelta

from loguru import logger
from sqlalchemy import delete, func, or_, update

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import (
    ComputedAdjustmentFactor,
    CorporateAction,
    DailyPrice,
    DelistingEvent,
    HistoricalFloat,
    HistoricalShare,
    InsiderTransaction,
    InstitutionalHolding,
    NewsArticleInsight,
    SecFiling,
    SecFundamentalFact,
    Security,
    SecurityIdentifier,
    SecurityIdentityEvent,
    SecuritySymbolHistory,
    ShortInterest,
    ShortVolume,
    VendorAdjustmentFactor,
)
from db_manager import DatabaseManager
from utils.massive_config import ALLOWED_US_SECURITY_TYPES, enforce_us_market
from utils.script_logging import setup_logging as configure_script_logging

DELETE_BATCH_SIZE = 500

DELETE_MODELS = (
    DailyPrice,
    VendorAdjustmentFactor,
    ComputedAdjustmentFactor,
    CorporateAction,
    DelistingEvent,
    HistoricalShare,
    HistoricalFloat,
    SecuritySymbolHistory,
    SecurityIdentifier,
    SecurityIdentityEvent,
    ShortInterest,
    ShortVolume,
)

UNLINK_MODELS = (
    SecFiling,
    InsiderTransaction,
    InstitutionalHolding,
    NewsArticleInsight,
    SecFundamentalFact,
)


def setup_logging():
    configure_script_logging("cleanup_us_universe")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="清理 US universe 中非普通股 / ETF 的证券及其关联数据。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--market", type=str, default="US", help="当前仅支持 US。")
    parser.add_argument("--limit", type=int, default=0, help="限制待删除证券数量，用于测试。")
    parser.add_argument("--sample-size", type=int, default=20, help="dry-run 时展示的样例数量。")
    parser.add_argument("--apply", action="store_true", help="执行真实删除。默认仅 dry-run。")
    return parser


def _iter_id_batches(ids: list[int], batch_size: int = DELETE_BATCH_SIZE):
    for index in range(0, len(ids), batch_size):
        yield ids[index : index + batch_size]


def get_targets(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    with db_manager.get_session() as session:
        query = (
            session.query(Security)
            .filter(func.upper(Security.market) == enforce_us_market(args.market))
            .filter(or_(Security.type.is_(None), ~func.upper(Security.type).in_(ALLOWED_US_SECURITY_TYPES)))
            .order_by(Security.type.asc().nulls_first(), Security.symbol.asc())
        )
        if args.limit > 0:
            query = query.limit(args.limit)
        return query.all()


def collect_related_counts(db_manager: DatabaseManager, security_ids: list[int]) -> dict[str, int]:
    if not security_ids:
        return {model.__tablename__: 0 for model in (*DELETE_MODELS, *UNLINK_MODELS)}
    with db_manager.get_session() as session:
        return {
            model.__tablename__: (
                session.query(func.count()).select_from(model)
                .filter(model.security_id.in_(security_ids)).scalar() or 0
            )
            for model in (*DELETE_MODELS, *UNLINK_MODELS)
        }


def run_apply(db_manager: DatabaseManager, security_ids: list[int]) -> Counter:
    deleted = Counter()
    for batch_ids in _iter_id_batches(security_ids):
        with db_manager.get_session() as session:
            for model in DELETE_MODELS:
                deleted[model.__tablename__] += session.execute(
                    delete(model).where(model.security_id.in_(batch_ids))
                ).rowcount or 0
            for model in UNLINK_MODELS:
                deleted[f"{model.__tablename__}_unlinked"] += session.execute(
                    update(model).where(model.security_id.in_(batch_ids)).values(security_id=None)
                ).rowcount or 0
            session.execute(
                update(SecurityIdentityEvent)
                .where(SecurityIdentityEvent.related_security_id.in_(batch_ids))
                .values(related_security_id=None)
            )
            deleted["securities"] += session.execute(delete(Security).where(Security.id.in_(batch_ids))).rowcount or 0
            session.commit()
    return deleted


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        targets = get_targets(db_manager, args)
        if not targets:
            logger.success("没有命中需要清理的证券。")
            return 0

        security_ids = [item.id for item in targets]
        type_counter = Counter((item.type or "NULL") for item in targets)
        related_counts = collect_related_counts(db_manager, security_ids)

        logger.info("US universe 清理预览: securities={}", len(targets))
        for type_code, count in type_counter.most_common():
            logger.info("  type={} count={}", type_code, count)
        logger.info("  关联数据: {}", related_counts)
        logger.info("  样例:")
        for item in targets[: args.sample_size]:
            logger.info("    {} | {} | {} | active={}", item.symbol, item.type or "NULL", item.name or "", item.is_active)

        if not args.apply:
            logger.warning("当前为 dry-run；如需真实删除，请添加 --apply。")
            return 0

        deleted = run_apply(db_manager, security_ids)
        logger.success("清理完成: {}", dict(deleted))
        return 0
    except Exception as e:
        logger.opt(exception=e).critical("cleanup_us_universe 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
