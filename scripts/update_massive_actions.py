import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_models.models import Security
from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_config import get_massive_history_floor, iter_chunks
from utils.massive_task import (
    build_standard_parser,
    run_concurrently,
    run_massive_task,
    select_us_securities,
)
from utils.trading_calendar import get_last_completed_trading_date

ACTIONS_UPDATE_INTERVAL_DAYS = 90
MAX_CONCURRENT_WORKERS = 8
API_BATCH_SIZE = 100
VENDOR_FACTOR_QUANT = Decimal("1.000000000000")
# 同日冲突拆股隔离的持久工件（追加写），镜像归档路径的 quarantine_detail.tsv——
# 仅靠 WARNING 日志没有人工裁决队列，遗漏的真实拆股会静默悬置
SPLIT_QUARANTINE_TSV = os.path.join(project_root, "logs", "split_conflict_quarantine.tsv")


def _record_split_quarantine(security: Security, ex_date: date, group: list[dict]) -> None:
    from datetime import datetime, timezone
    try:
        os.makedirs(os.path.dirname(SPLIT_QUARANTINE_TSV), exist_ok=True)
        is_new = not os.path.exists(SPLIT_QUARANTINE_TSV)
        with open(SPLIT_QUARANTINE_TSV, "a", encoding="utf-8") as fh:
            if is_new:
                fh.write("recorded_at_utc\tsecurity_id\tsymbol\tex_date\tsource_event_id\tsplit_from\tsplit_to\n")
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            for item in group:
                sf, st = _split_ratio_repr(item)
                fh.write(f"{now}\t{security.id}\t{security.symbol}\t{ex_date}\t"
                         f"{item.get('source_event_id') or ''}\t{sf}\t{st}\n")
    except OSError as e:
        logger.opt(exception=e).error("拆股隔离 TSV 写入失败（不影响本批处理）: {}", SPLIT_QUARANTINE_TSV)


def _infer_currency(security: Security) -> str | None:
    if security.currency:
        return security.currency.upper()
    return "USD"


def create_parser() -> argparse.ArgumentParser:
    parser = build_standard_parser(
        "使用 Massive API 批量更新公司行动（分红、拆股）。",
        default_workers=MAX_CONCURRENT_WORKERS,
    )
    parser.add_argument("--force", action="store_true", help="强制刷新 Massive 可覆盖的最近 2 年窗口。")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=0,
        help="只拉取最近 N 天的新事件（忽略 90 天间隔，选取全部活跃证券）。"
             "用于每日轻量补新，弥补周日全量被跳过时的事件缺口。",
    )
    return parser


def get_securities_to_update(db_manager: DatabaseManager, args: argparse.Namespace) -> list[Security]:
    return select_us_securities(
        db_manager,
        args,
        staleness_column="actions_last_updated_at",
        staleness_days=ACTIONS_UPDATE_INTERVAL_DAYS,
        skip_staleness=bool(args.force or args.recent_days),
    )


def _get_batch_start_date(
    securities: list[Security],
    history_floor: date,
    force: bool,
    recent_days: int = 0,
) -> str:
    if recent_days > 0:
        if any(security.actions_last_updated_at is None for security in securities):
            return history_floor.isoformat()
        return max(history_floor, date.today() - timedelta(days=recent_days)).isoformat()
    if force:
        return history_floor.isoformat()

    candidate_dates = []
    for security in securities:
        if not security.actions_last_updated_at:
            # 批内任一证券从未拉取过 actions 时，整批必须回到可覆盖窗口起点，
            # 否则该证券会只拿到其它证券增量窗口内的事件并被打上时间戳，历史事件永久缺失。
            return history_floor.isoformat()
        candidate_dates.append((security.actions_last_updated_at - timedelta(days=7)).date())
    return max(history_floor, min(candidate_dates)).isoformat()


def _group_by_ticker(rows: list[dict]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        ticker = (row.get("ticker") or "").lower()
        if ticker:
            grouped[ticker].append(row)
    return grouped


def _event_ex_date(item: dict) -> date | None:
    raw = item.get("ex_dividend_date") or item.get("execution_date") or item.get("ex_date")
    if raw is None or isinstance(raw, date):
        return raw
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def _clamp_to_list_date(security: Security, items: list[dict]) -> list[dict]:
    """死票回收防护：Massive 按 ticker 键控事件，list_date 之前的分红/拆股属于
    该 symbol 的旧身份，一律丢弃（同 update_massive_prices 的回填 clamp）。"""
    if not security.list_date or not items:
        return items
    kept = [item for item in items if (_event_ex_date(item) or security.list_date) >= security.list_date]
    dropped = len(items) - len(kept)
    if dropped:
        logger.info(
            "[{}] 丢弃 {} 条早于 list_date {} 的公司行动（属于该 symbol 的旧身份）。",
            security.symbol, dropped, security.list_date,
        )
    return kept


def _clamp_to_delist_date(security: Security, items: list[dict]) -> list[dict]:
    """死票回收防护（上界，与 _clamp_to_list_date 下界成对）：delist_date 之后的
    同名分红/拆股可能属于回收该 symbol 的后继实体，一律丢弃（同 update_massive_prices
    的回填终点 clamp）。活跃证券不受影响（挂着 delist_date 的脏元数据不 clamp）；
    退市但 delist_date 未知的证券也不 clamp——宁多留勿猜。delist_date 当日事件保留。"""
    if security.is_active or security.delist_date is None or not items:
        return items
    kept = [item for item in items if (_event_ex_date(item) or security.delist_date) <= security.delist_date]
    dropped = len(items) - len(kept)
    if dropped:
        logger.info(
            "[{}] 丢弃 {} 条晚于 delist_date {} 的公司行动（属于回收该 symbol 的后继实体）。",
            security.symbol, dropped, security.delist_date,
        )
    return kept


def _clamp_to_identity_window(security: Security, items: list[dict]) -> list[dict]:
    """本证券身份窗口 [list_date, delist_date] 之外的事件全部丢弃。"""
    return _clamp_to_delist_date(security, _clamp_to_list_date(security, items))


def _strip_ticker(rows: list[dict]) -> list[dict]:
    return [{key: value for key, value in row.items() if key != "ticker"} for row in rows]


def _to_adjustment_factor(value) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(VENDOR_FACTOR_QUANT)
    except (InvalidOperation, TypeError, ValueError):
        return None


def _format_factor_key_decimal(value) -> str | None:
    """与 update_adjustment_factors._format_decimal 保持同一规范形式，确保 fallback factor_key 两侧可 join。"""
    if value is None:
        return None
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, TypeError, ValueError):
        return str(value)


def _split_ratio_repr(item: dict) -> tuple[str | None, str | None]:
    """拆股比例的规范化字符串对（15 与 15.0000 同形），同归档 sift_splits 的 _fmt 口径。"""
    return (
        _format_factor_key_decimal(item.get("split_from")),
        _format_factor_key_decimal(item.get("split_to")),
    )


def _sift_same_day_splits(security: Security, items: list[dict], results_counter: Counter) -> list[dict]:
    """同日冲突拆股守卫，镜像归档导入的 R9/R10（import_corporate_actions_archive.sift_splits）。

    vendor 会对同一 (ticker, 执行日) 双发拆股且比例互斥（TSM 实测）：
    - 同日比例位级一致的精确重复只保留 source_event_id 最小的一条（R9 保留规则的
      无 prod 侧回退分支——live 路径不读库，取不到 prod 已存在 id 的优先项）；
    - 同日出现多个不同 (split_from, split_to) 规范形式即冲突，全组不落库、不写
      vendor 因子行，响亮告警留人工裁决（R10）——静默 upsert 任意一条都会污染复权因子链。
    """
    if len(items) < 2:
        return items
    groups: dict[date, list[dict]] = defaultdict(list)
    passthrough: list[dict] = []
    for item in items:
        ex_date = _event_ex_date(item)
        if ex_date is None:
            passthrough.append(item)  # 无执行日的行维持现状：由 upsert_splits 自行丢弃
        else:
            groups[ex_date].append(item)
    kept: list[dict] = []
    for ex_date in sorted(groups):
        group = groups[ex_date]
        if len({_split_ratio_repr(item) for item in group}) > 1:
            results_counter["SPLIT_CONFLICT_QUARANTINED"] += len(group)
            _record_split_quarantine(security, ex_date, group)
            logger.warning(
                "[{}] 同日 {} 出现 {} 条比例互斥的拆股事件，全组隔离不落库，须人工裁决: {}",
                security.symbol, ex_date, len(group),
                "; ".join(
                    f"{item.get('source_event_id') or '?'}"
                    f"={_split_ratio_repr(item)[0]}:{_split_ratio_repr(item)[1]}"
                    for item in group
                ),
            )
            continue
        if len(group) > 1:
            results_counter["SPLIT_DUPLICATE_DROPPED"] += len(group) - 1
            group = [min(group, key=lambda item: str(item.get("source_event_id") or ""))]
        kept.extend(group)
    return kept + passthrough


def _build_vendor_factor_rows(
    security: Security,
    dividends: list[dict],
    splits: list[dict],
    as_of_date: date,
) -> list[dict]:
    rows = []
    for item in dividends:
        adjustment_factor = _to_adjustment_factor(item.get("historical_adjustment_factor"))
        ex_date = item.get("ex_dividend_date") or item.get("ex_date")
        if adjustment_factor is None or not ex_date:
            continue
        source_event_id = item.get("source_event_id")
        factor_key = (
            f"dividend:{source_event_id}"
            if source_event_id
            else f"dividend:{ex_date}:{_format_factor_key_decimal(item.get('cash_amount'))}"
        )
        rows.append(
            {
                "security_id": security.id,
                "date": ex_date,
                "source": "MASSIVE",
                "factor_type": "historical_adjustment",
                "factor_key": factor_key,
                "source_event_id": source_event_id,
                "adjustment_factor": adjustment_factor,
                "as_of_date": as_of_date,
            }
        )

    for item in splits:
        adjustment_factor = _to_adjustment_factor(item.get("historical_adjustment_factor"))
        ex_date = item.get("execution_date") or item.get("ex_date")
        if adjustment_factor is None or not ex_date:
            continue
        source_event_id = item.get("source_event_id")
        factor_key = (
            f"split:{source_event_id}"
            if source_event_id
            else (
                f"split:{ex_date}:"
                f"{_format_factor_key_decimal(item.get('split_from'))}:"
                f"{_format_factor_key_decimal(item.get('split_to'))}"
            )
        )
        rows.append(
            {
                "security_id": security.id,
                "date": ex_date,
                "source": "MASSIVE",
                "factor_type": "historical_adjustment",
                "factor_key": factor_key,
                "source_event_id": source_event_id,
                "adjustment_factor": adjustment_factor,
                "as_of_date": as_of_date,
            }
        )
    return rows


def process_batch(
    securities: list[Security],
    source: MassiveSource,
    db_manager: DatabaseManager,
    history_floor,
    force: bool,
    recent_days: int = 0,
) -> tuple[Counter, list[Security]]:
    results_counter = Counter()
    changed: list[Security] = []
    batch_start = _get_batch_start_date(securities, history_floor, force, recent_days)
    symbols = [security.symbol for security in securities]

    dividends = source.get_dividends_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    splits = source.get_splits_batch(symbols, start_date=batch_start, chunk_size=API_BATCH_SIZE)
    dividends_by_symbol = _group_by_ticker(dividends)
    splits_by_symbol = _group_by_ticker(splits)
    as_of_date = get_last_completed_trading_date("US")

    for security in securities:
        symbol = security.symbol
        try:
            security_dividends = _clamp_to_identity_window(security, _strip_ticker(dividends_by_symbol.get(symbol, [])))
            security_splits = _sift_same_day_splits(
                security,
                _clamp_to_identity_window(security, _strip_ticker(splits_by_symbol.get(symbol, []))),
                results_counter,
            )

            if security_dividends:
                inferred_currency = _infer_currency(security)
                normalized = []
                for item in security_dividends:
                    if not item.get("currency"):
                        item["currency"] = inferred_currency
                    if item.get("currency"):
                        normalized.append(item)
                security_dividends = normalized

            inserted_dividends = db_manager.upsert_dividends(security.id, security_dividends) if security_dividends else 0
            inserted_splits = db_manager.upsert_splits(security.id, security_splits) if security_splits else 0
            inserted_vendor_factors = db_manager.upsert_vendor_adjustment_factors(
                _build_vendor_factor_rows(security, security_dividends, security_splits, as_of_date)
            )
            db_manager.update_security_timestamp(security.id, "actions_last_updated_at")

            if inserted_dividends + inserted_splits + inserted_vendor_factors > 0:
                changed.append(security)
                results_counter["SUCCESS"] += 1
            elif security_dividends or security_splits:
                results_counter["SUCCESS_DUPLICATE_ONLY"] += 1
            else:
                results_counter["SUCCESS_NO_ACTIONS"] += 1
        except Exception as e:
            logger.opt(exception=e).error("[{}] Massive 公司行动落库失败: {}", symbol, e)
            results_counter["ERROR"] += 1
    return results_counter, changed


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> int:
    end_date = get_last_completed_trading_date(args.market)
    history_floor = get_massive_history_floor(end_date)
    securities = get_securities_to_update(db_manager, args)
    if not securities:
        logger.success("没有需要更新 Massive 公司行动的证券。")
        return 0, {"processed": 0, "written": 0, "failed": 0}

    batches = iter_chunks(securities, API_BATCH_SIZE)
    outputs, results_counter = run_concurrently(
        batches,
        lambda batch: process_batch(batch, source, db_manager, history_floor, args.force, args.recent_days),
        max_workers=args.workers,
        desc="更新 Massive 公司行动",
    )
    total_changed = 0
    for batch_counter, changed in outputs:
        results_counter.update(batch_counter)
        total_changed += len(changed)

    logger.info("--- 公司行动统计 ---")
    logger.info("  成功(有新增): {}", results_counter["SUCCESS"])
    logger.info("  成功(仅重复): {}", results_counter["SUCCESS_DUPLICATE_ONLY"])
    logger.info("  成功(无 actions): {}", results_counter["SUCCESS_NO_ACTIONS"])
    logger.info("  错误: {}", results_counter["ERROR"] + results_counter["FATAL_ERROR"])
    if results_counter["SPLIT_CONFLICT_QUARANTINED"]:
        logger.warning(
            "  同日冲突拆股隔离: {} 条（比例互斥未落库，须人工裁决，明细见上方 WARNING）",
            results_counter["SPLIT_CONFLICT_QUARANTINED"],
        )
    logger.info("--------------------")
    errors = results_counter["ERROR"] + results_counter["FATAL_ERROR"]
    exit_code = 1 if errors else 0
    stats = {
        "processed": len(securities),
        "written": total_changed,
        "failed": errors,
        "split_conflicts_quarantined": results_counter["SPLIT_CONFLICT_QUARANTINED"],
    }
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("update_massive_actions", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
