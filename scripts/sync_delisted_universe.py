"""同步 Massive 退市证券名单，补齐 20 年 universe（含已退市 CS/ETF）。

背景：securities 此前只收活跃 universe（+覆盖期内退市的 ~700 只），20 年日线
flat files 里的大量退市 ticker 无行可挂。本脚本分两阶段补齐：

阶段 A（名单同步，~30 个 API 请求 + 无类型鉴定每条 1 请求）：
  拉取 /v3/reference/tickers?active=false（注意：该查询带 sort 参数会返回空，
  适配器已规避），过滤 US，与既有 securities 按 FIGI→CIK+symbol→
  symbol+退市日邻近 匹配；匹配到活跃行的是改名幽灵（如 FB→META 后 vendor 把
  FB 记作 delisted，FIGI 同 META），跳过；匹配到退市行的做 NULL 字段补齐；
  无匹配的 CS/ETF 条目插入新退市行（is_active=False，list_date 先置 NULL）。
  vendor 约 29% 的退市条目缺 type：这些先调时点详情鉴定身份与类型，确认
  CS/ETF 才插入（该响应自带 list_date，等于免费做了阶段 B）。

阶段 B（时点详情富化，每只 1-2 个请求）：
  对 list_date IS NULL 的退市行调 /v3/reference/tickers/{T}?date=退市前一日
  （查退市当天会 NOT_FOUND——vendor 的时点视图当天已摘牌；停牌早的回退一周
  再试），用退市时点的详情回填 list_date/name/cik/figi。FIGI/CIK 与本行冲突
  的响应视为身份错位丢弃。vendor 无 list_date 的行保持 NULL——import_day_aggs
  的链式任期推断会兜底。

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
from utils.massive_config import ALLOWED_US_SECURITY_TYPES
from utils.massive_task import run_concurrently, run_massive_task

SUPPORTED_TYPES = frozenset(ALLOWED_US_SECURITY_TYPES)
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


def classify_entries(entries: list[dict], existing_rows,
                     match_log: list[dict] | None = None) -> tuple[list, list, Counter]:
    """纯逻辑：vendor 退市条目 -> (待插入 payload 列表, 待补齐 (security_id, fills) 列表, 统计)。

    existing_rows: 元素含 id/symbol/is_active/list_date/delist_date/cik/
    composite_figi/share_class_figi/name/exchange（可选 type，用于跨类型吸收审计）。
    匹配优先级 FIGI → CIK+symbol → symbol+退市日邻近（仅退市行）；命中活跃行
    视为改名幽灵跳过。填充只补 NULL 字段，绝不改写既有值。
    match_log 若提供，逐条记录被吸收（filled/noop）的匹配明细，供 dry-run
    人工审计跨类型吸收；vendor 类型与 DB 类型都已知且不同时计 matched_cross_type。
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
        matched_via = None
        candidates = by_figi.get(item.get("composite_figi") or "", [])
        if candidates:
            inactive = [r for r in candidates if not r.is_active]
            match = inactive[0] if inactive else candidates[0]
            matched_via = "figi"
        if match is None and item.get("cik"):
            candidates = by_cik_sym.get((item["cik"], symbol), [])
            if candidates:
                inactive = [r for r in candidates if not r.is_active]
                match = inactive[0] if inactive else candidates[0]
                matched_via = "cik_symbol"
        if match is None:
            for row in by_sym_inactive.get(symbol, []):
                if row.delist_date is None or delist is None \
                        or abs(row.delist_date - delist) <= DELIST_MATCH_TOLERANCE:
                    match = row
                    matched_via = "symbol_delist"
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
            vendor_type = (item.get("type") or "").upper() or None
            db_type = (getattr(match, "type", None) or "").upper() or None
            if vendor_type and db_type and vendor_type != db_type:
                stats["matched_cross_type"] += 1
            if match_log is not None:
                match_log.append({
                    "vendor_symbol": symbol,
                    "vendor_type": vendor_type or "",
                    "vendor_delist": delist,
                    "matched_via": matched_via,
                    "security_id": match.id,
                    "db_symbol": match.symbol,
                    "db_type": db_type or "",
                    "db_delist": match.delist_date,
                    "disposition": "filled" if fills else "noop",
                })
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


def _fetch_entries(args: argparse.Namespace, source: MassiveSource) -> tuple[list[dict], list[dict], Counter]:
    """拉取/读取退市名单 -> (CS/ETF 条目, 无类型待鉴定条目, 统计)。"""
    stats: Counter = Counter()
    if args.delisted_json:
        with open(args.delisted_json) as f:
            raw = json.load(f)
        raw = [r for r in raw if (r.get("locale") or "").upper() == "US"]
    else:
        raw = source.list_delisted_tickers()
    stats["vendor_us_total"] = len(raw)
    typed = [r for r in raw if (r.get("type") or "").upper() in SUPPORTED_TYPES]
    untyped_raw = [r for r in raw if not (r.get("type") or "").strip()]
    stats["vendor_untyped"] = len(untyped_raw)
    stats["vendor_other_type"] = len(raw) - len(typed) - len(untyped_raw)
    typed = _dedupe_entries(typed)
    untyped_raw = _dedupe_entries(untyped_raw)
    stats["vendor_cs_etf_deduped"] = len(typed)

    def _payloads(items: list[dict], drop_counter: str) -> list[dict]:
        out = []
        for item in items:
            payload = source._build_reference_payload(item)
            if not payload.get("delist_date"):
                stats[drop_counter] += 1
                continue
            payload["is_active"] = False
            out.append(payload)
        return out

    return _payloads(typed, "vendor_no_delist_date"), _payloads(untyped_raw, "untyped_no_delist_date"), stats


def _load_existing(db_manager: DatabaseManager):
    from sqlalchemy import text
    with db_manager.get_session() as session:
        return session.execute(text("""
            SELECT id, symbol, is_active, list_date, delist_date, cik,
                   composite_figi, share_class_figi, name, exchange, type
            FROM securities
            WHERE upper(market) = 'US'
        """)).all()


def _write_match_audit(match_log: list[dict]) -> str | None:
    """把吸收匹配明细写成 TSV 供人工审计（dry-run 与正式跑都写，整跑覆盖）。"""
    if not match_log:
        return None
    audit_path = os.path.join(project_root, "logs", "delisted_match_audit.tsv")
    try:
        os.makedirs(os.path.dirname(audit_path), exist_ok=True)
        columns = ["vendor_symbol", "vendor_type", "vendor_delist", "matched_via",
                   "security_id", "db_symbol", "db_type", "db_delist", "disposition"]
        with open(audit_path, "w", encoding="utf-8") as fh:
            fh.write("\t".join(columns) + "\n")
            for row in match_log:
                fh.write("\t".join("" if row[c] is None else str(row[c]) for c in columns) + "\n")
    except OSError as e:
        logger.opt(exception=e).error("匹配审计 TSV 写入失败（不影响同步）: {}", audit_path)
        return None
    return audit_path


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
            WHERE upper(market) = 'US' AND upper(type) = ANY(:allowed_types)
              AND is_active = false AND list_date IS NULL AND delist_date IS NOT NULL
            ORDER BY delist_date DESC, id
        """), {"allowed_types": list(ALLOWED_US_SECURITY_TYPES)}).all()
    return rows[:limit] if limit > 0 else rows


def _overview_matches(figi, cik, delist_date, overview: dict) -> bool:
    """时点详情与目标身份是否一致：FIGI/CIK 任一双边存在且不同 → 错位。

    双方都无强标识时：时点查询语义本身保证返回的是"该日期持有此代码的实体"
    （查退市前一天即目标实体），响应处于 PIT 活跃视图时通常不带 delisted_utc，
    此时直接认；带了 delisted_utc 则要求与目标退市日一致。
    """
    o_figi = overview.get("composite_figi")
    if o_figi and figi and o_figi != figi:
        return False
    o_cik = overview.get("cik")
    if o_cik and cik and str(o_cik) != str(cik):
        return False
    if not (figi or cik):
        vendor_delist = (overview.get("delisted_utc") or "")[:10]
        return (not vendor_delist) or vendor_delist == delist_date.isoformat()
    return True


def _pit_overview(source: MassiveSource, symbol: str, delist_date):
    """退市实体的时点详情：查退市当天会 NOT_FOUND（当天已摘牌），须查前一天；
    个别票停牌早于摘牌，前一天也查不到时再回退一周。"""
    for offset in (1, 7):
        overview = source.get_ticker_overview(
            symbol, lookup_date=delist_date - timedelta(days=offset), allow_missing=True)
        if overview:
            return overview
    return None


def enrich_check_identity(sec, overview: dict) -> bool:
    return _overview_matches(sec.composite_figi, sec.cik, sec.delist_date, overview)


def _type_check_untyped(candidates: list[dict], source: MassiveSource,
                        args: argparse.Namespace, stats: Counter) -> list[dict]:
    """无类型退市条目的时点身份鉴定：确认为 CS/ETF 的返回可插入 payload
    （详情响应顺带提供 list_date，省去阶段 B 再查一次）。"""
    if not candidates:
        return []
    logger.info("无类型条目鉴定：{} 条待查（时点详情，每条 1 请求）。", len(candidates))
    confirmed: list[dict] = []

    def worker(entry):
        overview = _pit_overview(source, entry["symbol"], entry["delist_date"])
        if not overview:
            return ("untyped_no_data", None)
        if not _overview_matches(entry.get("composite_figi"), entry.get("cik"), entry["delist_date"], overview):
            return ("untyped_identity_mismatch", None)
        sec_type = (overview.get("type") or "").upper()
        if sec_type not in SUPPORTED_TYPES:
            return ("untyped_other_type", None)
        payload = source._build_overview_payload(entry["symbol"], overview)
        payload["is_active"] = False
        payload.setdefault("delist_date", entry["delist_date"])
        payload.setdefault("market", entry.get("market") or "US")
        return ("untyped_confirmed_cs_etf", payload)

    class _Item(dict):
        # run_concurrently 的标签取 .symbol；给 dict 套个壳
        @property
        def symbol(self):
            return self["symbol"]

    outputs, counter = run_concurrently(
        [_Item(e) for e in candidates], worker,
        max_workers=args.enrich_workers, desc="鉴定无类型退市条目")
    stats["untyped_fatal_error"] = counter.get("FATAL_ERROR", 0)
    for result in outputs:
        if result is None:
            continue
        status, payload = result
        stats[status] += 1
        if payload is not None:
            confirmed.append(payload)
    return confirmed


def _run_enrich(args, source: MassiveSource, db_manager: DatabaseManager, stats: Counter) -> None:
    population = _enrich_population(db_manager, args.enrich_limit)
    stats["enrich_population"] = len(population)
    if not population:
        return
    logger.info("阶段 B：{} 只退市证券待富化 list_date（时点详情，每只 1 请求）。", len(population))

    def worker(sec):
        overview = _pit_overview(source, sec.symbol, sec.delist_date)
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
    typed_entries, untyped_entries, stats = _fetch_entries(args, source)
    existing = _load_existing(db_manager)
    match_log: list[dict] = []
    to_insert, to_fill, cls_stats = classify_entries(typed_entries, existing, match_log=match_log)
    stats.update(cls_stats)

    # 无类型条目：能匹配上既有行的直接补 NULL 字段；匹配不上的走时点鉴定
    u_insert, u_fill, u_stats = classify_entries(untyped_entries, existing, match_log=match_log)
    for key, value in u_stats.items():
        stats[f"untyped_{key}"] += value
    to_fill.extend(u_fill)

    audit_path = _write_match_audit(match_log)
    cross_type = stats.get("matched_cross_type", 0) + stats.get("untyped_matched_cross_type", 0)
    if cross_type:
        logger.warning("检测到 {} 条跨类型吸收匹配（vendor 类型 != 库内类型），须人工审计: {}",
                       cross_type, audit_path)
    elif audit_path:
        logger.info("匹配审计明细已写入: {}", audit_path)

    if args.dry_run:
        stats["untyped_pending_type_check"] = len(u_insert)
        logger.info("[dry-run] 统计: {}", dict(stats))
        return 0, dict(stats)

    confirmed = _type_check_untyped(u_insert, source, args, stats)
    seen_keys = {(row["symbol"], row["delist_date"]) for row in to_insert}
    for row in confirmed:
        if (row["symbol"], row["delist_date"]) in seen_keys:
            stats["untyped_dup_of_typed"] += 1
            continue
        to_insert.append(row)

    if to_fill:
        stats["filled_rows"] = _apply_fills(db_manager, to_fill)
    deduped_insert = []
    ins_seen = set()
    for row in to_insert:
        key = (row["symbol"], row["delist_date"])
        if key in ins_seen:
            stats["insert_key_collision_dropped"] += 1
            continue
        ins_seen.add(key)
        deduped_insert.append(row)
    to_insert = deduped_insert
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
        } for sec_id, symbol, _ in inserted]
        db_manager.insert_identity_events(events)
        key_to_id = {(s, d): i for i, s, d in inserted}
        cik_rows = [{
            "security_id": key_to_id[(item["symbol"], item["delist_date"])],
            "id_type": "CIK",
            "id_value": item["cik"],
            "source": "MASSIVE",
        } for item in to_insert
            if item.get("cik") and (item["symbol"], item["delist_date"]) in key_to_id]
        stats["cik_identifier_rows"] = db_manager.insert_missing_security_identifiers(cik_rows)

    if not args.skip_enrich:
        _run_enrich(args, source, db_manager, stats)

    logger.info("同步统计: {}", dict(stats))
    return 0, dict(stats)


def main(argv: list[str] | None = None) -> int:
    return run_massive_task("sync_delisted_universe", argv, create_parser, run)


if __name__ == "__main__":
    raise SystemExit(main())
