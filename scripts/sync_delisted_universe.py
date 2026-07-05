"""同步 Massive 退市证券名单，补齐 20 年 universe（含已退市 CS/ETF）。

背景：securities 此前只收活跃 universe（+覆盖期内退市的 ~700 只），20 年日线
flat files 里的大量退市 ticker 无行可挂。本脚本分两阶段补齐：

阶段 A（名单同步，~90 个 API 请求）：
  拉取 /v3/reference/tickers?active=false（注意：该查询带 sort 参数会返回空，
  适配器已规避），过滤 US CS/ETF，与既有 securities 按 FIGI→CIK+symbol→
  symbol+退市日邻近 匹配；匹配到活跃行的是改名幽灵（如 FB→META 后 vendor 把
  FB 记作 delisted，FIGI 同 META），跳过；匹配到退市行的做 NULL 字段补齐；
  无匹配的插入新退市行（is_active=False，list_date 先置 NULL）。

阶段 B（时点详情富化，每只 1 个请求）：
  对 list_date IS NULL 的退市行调 /v3/reference/tickers/{T}?date=退市日，
  用退市时点的详情回填 list_date/name/cik/figi。FIGI/CIK 与本行冲突的响应
  视为身份错位丢弃。vendor 无 list_date 的行保持 NULL——import_day_aggs 的
  链式任期推断会兜底。

幂等：阶段 A 匹配逻辑天然幂等（重跑只会重新匹配到自己）；阶段 B 只选
list_date IS NULL 的行，跑过即出队。
"""
import argparse
import json
import os
import sys
from collections import Counter
from datetime import date, timedelta

from loguru import logger

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from data_sources.massive_source import MassiveSource
from db_manager import DatabaseManager
from utils.massive_task import run_concurrently, run_massive_task

SUPPORTED_TYPES = {"CS", "ETF"}
# symbol 兜底匹配时，vendor 退市日与库内 delist_date 允许的最大偏差
DELIST_MATCH_TOLERANCE = timedelta(days=35)
FILLABLE_FIELDS = ("delist_date", "cik", "composite_figi", "share_class_figi", "name", "exchange")
ENRICH_COMMIT_BATCH = 500


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 Massive 退市 CS/ETF 名单并富化上市日。")
    parser.add_argument("--dry-run", action="store_true", help="只统计不写库（阶段 B 一并跳过）。")
    parser.add_argument("--delisted-json", default=None,
                        help="调试：从本地 JSON 文件读取退市名单，跳过名单 API 拉取。")
    parser.add_argument("--skip-enrich", action="store_true", help="跳过阶段 B（时点详情富化）。")
    parser.add_argument("--enrich-limit", type=int, default=0,
                        help="阶段 B 最多处理多少只（0=不限）；配合分批跑。")
    parser.add_argument("--enrich-workers", type=int, default=8, help="阶段 B 并发线程数。")
    return parser


def _entry_key(item: dict) -> tuple:
    """vendor 名单去重键：FIGI 优先，其次 CIK+ticker，最后 ticker+退市日。"""
    figi = item.get("composite_figi")
    if figi:
        return ("figi", figi)
    if item.get("cik"):
        return ("cik", item["cik"], item["ticker"])
    return ("tk", item["ticker"], item.get("delisted_utc"))


def _dedupe_entries(entries: list[dict]) -> list[dict]:
    """同键多条（如同一公司退市-复牌-再退市）保留退市日最晚的一条。"""
    best: dict[tuple, dict] = {}
    for item in entries:
        key = _entry_key(item)
        kept = best.get(key)
        if kept is None or (item.get("delisted_utc") or "") > (kept.get("delisted_utc") or ""):
            best[key] = item
    return list(best.values())


def classify_entries(entries: list[dict], existing_rows) -> tuple[list, list, Counter]:
    """纯逻辑：vendor 退市条目 -> (待插入 payload 列表, 待补齐 (security_id, fills) 列表, 统计)。

    existing_rows: 元素含 id/symbol/is_active/list_date/delist_date/cik/
    composite_figi/share_class_figi/name/exchange。
    匹配优先级 FIGI → CIK+symbol → symbol+退市日邻近（仅退市行）；命中活跃行
    视为改名幽灵跳过。填充只补 NULL 字段，绝不改写既有值。
    """
    by_figi: dict[str, list] = {}
    by_cik_sym: dict[tuple, list] = {}
    by_sym_inactive: dict[str, list] = {}
    for row in existing_rows:
        if row.composite_figi:
            by_figi.setdefault(row.composite_figi, []).append(row)
        if row.cik:
            by_cik_sym.setdefault((row.cik, row.symbol), []).append(row)
        if not row.is_active:
            by_sym_inactive.setdefault(row.symbol, []).append(row)

    stats: Counter = Counter()
    to_insert: list[dict] = []
    to_fill: list[tuple[int, dict]] = []
    filled_ids: set[int] = set()
    for item in entries:
        symbol = item["symbol"]
        delist = item["delist_date"]
        match = None
        candidates = by_figi.get(item.get("composite_figi") or "", [])
        if candidates:
            inactive = [r for r in candidates if not r.is_active]
            match = inactive[0] if inactive else candidates[0]
        if match is None and item.get("cik"):
            candidates = by_cik_sym.get((item["cik"], symbol), [])
            if candidates:
                inactive = [r for r in candidates if not r.is_active]
                match = inactive[0] if inactive else candidates[0]
        if match is None:
            for row in by_sym_inactive.get(symbol, []):
                if row.delist_date is None or delist is None \
                        or abs(row.delist_date - delist) <= DELIST_MATCH_TOLERANCE:
                    match = row
                    break

        if match is not None and match.is_active:
            stats["skipped_active_ghost"] += 1  # 改名幽灵/仍活跃冲突，不动
            continue
        if match is not None:
            if match.id in filled_ids:
                stats["dup_match_same_row"] += 1
                continue
            fills = {f: item.get(f) for f in FILLABLE_FIELDS
                     if getattr(match, f) is None and item.get(f) is not None}
            if fills:
                to_fill.append((match.id, fills))
                filled_ids.add(match.id)
                stats["matched_filled"] += 1
            else:
                stats["matched_noop"] += 1
            continue
        stats["new_delisted"] += 1
        to_insert.append(item)
    return to_insert, to_fill, stats


def _fetch_entries(args: argparse.Namespace, source: MassiveSource) -> tuple[list[dict], Counter]:
    stats: Counter = Counter()
    if args.delisted_json:
        with open(args.delisted_json) as f:
            raw = json.load(f)
        raw = [r for r in raw if (r.get("locale") or "").upper() == "US"]
    else:
        raw = source.list_delisted_tickers()
    stats["vendor_us_total"] = len(raw)
    typed = [r for r in raw if (r.get("type") or "").upper() in SUPPORTED_TYPES]
    stats["vendor_untyped_or_other"] = len(raw) - len(typed)
    typed = _dedupe_entries(typed)
    stats["vendor_cs_etf_deduped"] = len(typed)
    entries = []
    for item in typed:
        payload = source._build_reference_payload(item)
        if not payload.get("delist_date"):
            stats["vendor_no_delist_date"] += 1
            continue
        payload["is_active"] = False
        entries.append(payload)
    return entries, stats


def _load_existing(db_manager: DatabaseManager):
    from sqlalchemy import text
    with db_manager.get_session() as session:
        return session.execute(text("""
            SELECT id, symbol, is_active, list_date, delist_date, cik,
                   composite_figi, share_class_figi, name, exchange
            FROM securities
            WHERE upper(market) = 'US'
        """)).all()


def _apply_fills(db_manager: DatabaseManager, to_fill: list[tuple[int, dict]]) -> int:
    from sqlalchemy import text
    applied = 0
    with db_manager.get_session() as session:
        for sec_id, fills in to_fill:
            sets = ", ".join(f"{col} = :{col}" for col in fills)
            # 只补 NULL：并发防御，绝不覆盖既有值
            guards = " AND ".join(f"{col} IS NULL" for col in fills)
            result = session.execute(
                text(f"UPDATE securities SET {sets} WHERE id = :sec_id AND {guards}"),
                {**fills, "sec_id": sec_id},
            )
            applied += result.rowcount
        session.commit()
    return applied


def _enrich_population(db_manager: DatabaseManager, limit: int):
    from sqlalchemy import text
    with db_manager.get_session() as session:
        rows = session.execute(text("""
            SELECT id, symbol, delist_date, cik, composite_figi
            FROM securities
            WHERE upper(market) = 'US' AND upper(type) IN ('CS', 'ETF')
              AND is_active = false AND list_date IS NULL AND delist_date IS NOT NULL
            ORDER BY delist_date DESC, id
        """)).all()
    return rows[:limit] if limit > 0 else rows


def enrich_check_identity(sec, overview: dict) -> bool:
    """时点详情与本行身份是否一致：FIGI/CIK 任一双边存在且不同 → 错位。"""
    figi = overview.get("composite_figi")
    if figi and sec.composite_figi and figi != sec.composite_figi:
        return False
    cik = overview.get("cik")
    if cik and sec.cik and str(cik) != str(sec.cik):
        return False
    if not (sec.composite_figi or sec.cik):
        # 双方都无强标识：要求退市日一致才敢认
        vendor_delist = overview.get("delisted_utc") or ""
        return vendor_delist[:10] == sec.delist_date.isoformat()
    return True


def _run_enrich(args, source: MassiveSource, db_manager: DatabaseManager, stats: Counter) -> None:
    population = _enrich_population(db_manager, args.enrich_limit)
    stats["enrich_population"] = len(population)
    if not population:
        return
    logger.info("阶段 B：{} 只退市证券待富化 list_date（时点详情，每只 1 请求）。", len(population))

    def worker(sec):
        overview = source.get_ticker_overview(sec.symbol, lookup_date=sec.delist_date, allow_missing=True)
        if not overview:
            return (sec.id, "enrich_no_data", None)
        if not enrich_check_identity(sec, overview):
            return (sec.id, "enrich_identity_mismatch", None)
        payload = source._build_overview_payload(sec.symbol, overview)
        fills = {f: payload.get(f) for f in ("list_date", "cik", "composite_figi", "share_class_figi", "name", "exchange")
                 if payload.get(f) is not None}
        if not fills.get("list_date"):
            return (sec.id, "enrich_vendor_no_list_date", None)
        return (sec.id, "enriched", fills)

    from sqlalchemy import text
    # 分批跑+批间落库：几小时的任务中途被杀时已完成的批次不丢
    for lo in range(0, len(population), ENRICH_COMMIT_BATCH):
        chunk = population[lo:lo + ENRICH_COMMIT_BATCH]
        outputs, counter = run_concurrently(
            chunk, worker, max_workers=args.enrich_workers,
            desc=f"富化退市证券 {lo + 1}-{lo + len(chunk)}/{len(population)}")
        stats["enrich_fatal_error"] += counter.get("FATAL_ERROR", 0)
        with db_manager.get_session() as session:
            for result in outputs:
                if result is None:
                    continue
                sec_id, status, fills = result
                stats[status] += 1
                if status != "enriched":
                    continue
                sets = ", ".join(f"{col} = COALESCE({col}, :{col})" for col in fills)
                session.execute(
                    text(f"UPDATE securities SET {sets} WHERE id = :sec_id"),
                    {**fills, "sec_id": sec_id},
                )
            session.commit()
        logger.info("阶段 B 进度 {}/{}: {}", min(lo + ENRICH_COMMIT_BATCH, len(population)),
                    len(population), {k: v for k, v in stats.items() if k.startswith("enrich")})


def run(args: argparse.Namespace, source: MassiveSource, db_manager: DatabaseManager) -> tuple[int, dict]:
    entries, stats = _fetch_entries(args, source)
    existing = _load_existing(db_manager)
    to_insert, to_fill, cls_stats = classify_entries(entries, existing)
    stats.update(cls_stats)

    if args.dry_run:
        logger.info("[dry-run] 统计: {}", dict(stats))
        return 0, dict(stats)

    if to_fill:
        stats["filled_rows"] = _apply_fills(db_manager, to_fill)
    if to_insert:
        inserted = db_manager.insert_backfilled_securities(to_insert)
        stats["inserted_rows"] = len(inserted)
        events = [{
            "security_id": sec_id,
            "event_type": "NEW_LISTING",
            "new_symbol": symbol,
            "resolution_source": "AUTO",
            "confidence": "HIGH",
            "details": json.dumps({"origin": "massive_delisted_backfill"}),
        } for sec_id, symbol in inserted]
        db_manager.insert_identity_events(events)
        symbol_to_id = dict((s, i) for i, s in inserted)
        cik_rows = [{
            "security_id": symbol_to_id[item["symbol"]],
            "id_type": "CIK",
            "id_value": item["cik"],
            "source": "MASSIVE",
        } for item in to_insert if item.get("cik") and item["symbol"] in symbol_to_id]
        stats["cik_identifier_rows"] = db_manager.insert_missing_security_identifiers(cik_rows)

    if not args.skip_enrich:
        _run_enrich(args, source, db_manager, stats)

    logger.info("同步统计: {}", dict(stats))
    return 0, dict(stats)


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("sync_delisted_universe", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
