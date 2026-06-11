import argparse
import os
import sys
import time
from collections import Counter
from datetime import timedelta

from loguru import logger
from sqlalchemy import delete, func, or_

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import (
    ComputedAdjustmentFactor,
    CorporateAction,
    DailyPrice,
    HistoricalFloat,
    HistoricalShare,
    NewsArticleInsight,
    Security,
    SecuritySymbolHistory,
    ShortInterest,
    ShortVolume,
    VendorAdjustmentFactor,
)
from db_manager import DatabaseManager
from utils.massive_config import ALLOWED_US_SECURITY_TYPES, enforce_us_market
from utils.script_logging import setup_logging as configure_script_logging

DELETE_BATCH_SIZE = 500


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
        return {
            "daily_prices": 0,
            "vendor_adjustment_factors": 0,
            "computed_adjustment_factors": 0,
            "corporate_actions": 0,
            "historical_shares": 0,
            "historical_floats": 0,
            "symbol_history": 0,
            "short_interests": 0,
            "short_volumes": 0,
            "news_article_insights": 0,
        }
    with db_manager.get_session() as session:
        return {
            "daily_prices": session.query(func.count()).select_from(DailyPrice).filter(DailyPrice.security_id.in_(security_ids)).scalar() or 0,
            "vendor_adjustment_factors": session.query(func.count()).select_from(VendorAdjustmentFactor).filter(VendorAdjustmentFactor.security_id.in_(security_ids)).scalar() or 0,
            "computed_adjustment_factors": session.query(func.count()).select_from(ComputedAdjustmentFactor).filter(ComputedAdjustmentFactor.security_id.in_(security_ids)).scalar() or 0,
            "corporate_actions": session.query(func.count()).select_from(CorporateAction).filter(CorporateAction.security_id.in_(security_ids)).scalar() or 0,
            "historical_shares": session.query(func.count()).select_from(HistoricalShare).filter(HistoricalShare.security_id.in_(security_ids)).scalar() or 0,
            "historical_floats": session.query(func.count()).select_from(HistoricalFloat).filter(HistoricalFloat.security_id.in_(security_ids)).scalar() or 0,
            "symbol_history": session.query(func.count()).select_from(SecuritySymbolHistory).filter(SecuritySymbolHistory.security_id.in_(security_ids)).scalar() or 0,
            "short_interests": session.query(func.count()).select_from(ShortInterest).filter(ShortInterest.security_id.in_(security_ids)).scalar() or 0,
            "short_volumes": session.query(func.count()).select_from(ShortVolume).filter(ShortVolume.security_id.in_(security_ids)).scalar() or 0,
            "news_article_insights": session.query(func.count()).select_from(NewsArticleInsight).filter(NewsArticleInsight.security_id.in_(security_ids)).scalar() or 0,
        }


def run_apply(db_manager: DatabaseManager, security_ids: list[int]) -> Counter:
    deleted = Counter()
    for batch_ids in _iter_id_batches(security_ids):
        with db_manager.get_session() as session:
            deleted["daily_prices"] += session.execute(delete(DailyPrice).where(DailyPrice.security_id.in_(batch_ids))).rowcount or 0
            deleted["vendor_adjustment_factors"] += session.execute(delete(VendorAdjustmentFactor).where(VendorAdjustmentFactor.security_id.in_(batch_ids))).rowcount or 0
            deleted["computed_adjustment_factors"] += session.execute(delete(ComputedAdjustmentFactor).where(ComputedAdjustmentFactor.security_id.in_(batch_ids))).rowcount or 0
            deleted["corporate_actions"] += session.execute(delete(CorporateAction).where(CorporateAction.security_id.in_(batch_ids))).rowcount or 0
            deleted["historical_shares"] += session.execute(delete(HistoricalShare).where(HistoricalShare.security_id.in_(batch_ids))).rowcount or 0
            deleted["historical_floats"] += session.execute(delete(HistoricalFloat).where(HistoricalFloat.security_id.in_(batch_ids))).rowcount or 0
            deleted["symbol_history"] += session.execute(delete(SecuritySymbolHistory).where(SecuritySymbolHistory.security_id.in_(batch_ids))).rowcount or 0
            deleted["short_interests"] += session.execute(delete(ShortInterest).where(ShortInterest.security_id.in_(batch_ids))).rowcount or 0
            deleted["short_volumes"] += session.execute(delete(ShortVolume).where(ShortVolume.security_id.in_(batch_ids))).rowcount or 0
            deleted["news_article_insights"] += session.execute(delete(NewsArticleInsight).where(NewsArticleInsight.security_id.in_(batch_ids))).rowcount or 0
            deleted["securities"] += session.execute(delete(Security).where(Security.id.in_(batch_ids))).rowcount or 0
            session.commit()
    return deleted


def main() -> int:
    start_time = time.monotonic()
    setup_logging()
    parser = create_parser()
    args = parser.parse_args()

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
        logger.info(
            "  关联数据: daily_prices={} vendor_adjustment_factors={} computed_adjustment_factors={} corporate_actions={} historical_shares={} historical_floats={} symbol_history={} short_interests={} short_volumes={} news_insights={}",
            related_counts["daily_prices"],
            related_counts["vendor_adjustment_factors"],
            related_counts["computed_adjustment_factors"],
            related_counts["corporate_actions"],
            related_counts["historical_shares"],
            related_counts["historical_floats"],
            related_counts["symbol_history"],
            related_counts["short_interests"],
            related_counts["short_volumes"],
            related_counts["news_article_insights"],
        )
        logger.info("  样例:")
        for item in targets[: args.sample_size]:
            logger.info("    {} | {} | {} | active={}", item.symbol, item.type or "NULL", item.name or "", item.is_active)

        if not args.apply:
            logger.warning("当前为 dry-run；如需真实删除，请添加 --apply。")
            return 0

        deleted = run_apply(db_manager, security_ids)
        logger.success(
            "删除完成: securities={} daily_prices={} vendor_adjustment_factors={} computed_adjustment_factors={} corporate_actions={} historical_shares={} historical_floats={} symbol_history={} short_interests={} short_volumes={} news_insights={}",
            deleted["securities"],
            deleted["daily_prices"],
            deleted["vendor_adjustment_factors"],
            deleted["computed_adjustment_factors"],
            deleted["corporate_actions"],
            deleted["historical_shares"],
            deleted["historical_floats"],
            deleted["symbol_history"],
            deleted["short_interests"],
            deleted["short_volumes"],
            deleted["news_article_insights"],
        )
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
