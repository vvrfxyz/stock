"""用 OpenFIGI mapping API 补链 13F 未映射 CUSIP -> security_id。

sync_cusip_identifiers（SEC FTD）只覆盖发生过交收失败的证券；剩余
institutional_holdings 中 security_id 为空的 CUSIP 用 OpenFIGI 兜底：

1. 候选集：holdings 中 security_id IS NULL 且 cusip 非空（9 位）的 distinct CUSIP；
2. 查询过滤（openfigi_cusip_lookups 缓存）：MATCHED 永不重查——CUSIP->FIGI 是
   稳定映射；NOT_FOUND / AMBIGUOUS 负缓存超过 --refresh-days 才重查；
   --limit 截断本次 API 查询量（0=不限）；
3. 按 source.batch_size 分批调 map_cusips，逐批落缓存——单批失败不丢已得批次，
   只有全批失败才以退出码 1 触发调度重试；
4. 解析落链（每次对全部 MATCHED 缓存行全量重跑，纯本地零 API 开销——新上市
   证券能让旧缓存命中）：按 composite_figi 关联 securities，优先 is_active 行；
   无活跃但唯一退市行也链接（退市股持仓历史同样有研究价值）；多候选歧义跳过
   并告警（身份合并后 husk 保留 FIGI，靠"优先活跃"规避）。composite_figi 无命中
   时回退 share_class_figi 同规则；
5. 写 security_identifiers（source='OPENFIGI', id_type='CUSIP', start_date=None
   快照语义——CUSIP 不像 ticker 会回收；只插缺失行，不覆盖 SEC_FTD 已有映射），
   最后 map_unlinked_holdings_to_securities() 回填 holdings。
"""
import argparse
import math
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger
from sqlalchemy import text

project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from data_sources.openfigi_source import OpenFigiSource
from db_manager import DatabaseManager
from utils.massive_task import TaskResult
from utils.script_logging import setup_logging as configure_script_logging

STATUS_MATCHED = "MATCHED"


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="用 OpenFIGI 同步 13F 未映射 CUSIP 的身份映射。")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制本次实际调 OpenFIGI 查询的 CUSIP 数量（0=不限）。")
    parser.add_argument("--refresh-days", type=int, default=90,
                        help="NOT_FOUND/AMBIGUOUS 负缓存的重查间隔天数；MATCHED 永不重查。")
    parser.add_argument("--batch-log-every", type=int, default=20,
                        help="每多少个 API 批次打一条进度日志。")
    return parser


def load_candidate_cusips(db_manager: DatabaseManager) -> list[str]:
    """institutional_holdings 中未链接 security 的 distinct CUSIP（大写、9 位）。

    非 9 位的值不是合法 CUSIP（也放不进 String(9) 缓存主键），直接排除；
    固定排序保证 --limit 截断可复现。"""
    sql = text(
        """
        SELECT DISTINCT upper(btrim(cusip)) AS cusip
        FROM institutional_holdings
        WHERE security_id IS NULL
          AND cusip IS NOT NULL
          AND char_length(btrim(cusip)) = 9
        ORDER BY cusip
        """
    )
    with db_manager.engine.connect() as conn:
        return [row.cusip for row in conn.execute(sql)]


def load_lookup_cache(db_manager: DatabaseManager, cusips: list[str]) -> dict[str, tuple[str, datetime]]:
    """候选 CUSIP 的缓存现状：{cusip: (status, queried_at)}。"""
    cache: dict[str, tuple[str, datetime]] = {}
    with db_manager.engine.connect() as conn:
        for start in range(0, len(cusips), 5000):
            chunk = cusips[start:start + 5000]
            rows = conn.execute(
                text(
                    "SELECT cusip, status, queried_at FROM openfigi_cusip_lookups "
                    "WHERE cusip = ANY(:cusips)"
                ),
                {"cusips": chunk},
            )
            for row in rows:
                cache[row.cusip] = (row.status, row.queried_at)
    return cache


def select_cusips_to_query(
    candidates: list[str],
    cache: dict[str, tuple[str, datetime]],
    refresh_days: int,
    now: datetime | None = None,
) -> list[str]:
    """缓存过滤：MATCHED 永不重查（API 层面 CUSIP->FIGI 是稳定映射）；
    NOT_FOUND/AMBIGUOUS 超过 refresh_days 才重查；不在缓存的照常查。"""
    now = now or datetime.now(timezone.utc)
    stale_before = now - timedelta(days=refresh_days)
    pending: list[str] = []
    for cusip in candidates:
        cached = cache.get(cusip)
        if cached is None:
            pending.append(cusip)
            continue
        status, queried_at = cached
        if status == STATUS_MATCHED:
            continue
        if queried_at <= stale_before:
            pending.append(cusip)
    return pending


def query_and_cache(
    source: OpenFigiSource,
    db_manager: DatabaseManager,
    pending: list[str],
    batch_log_every: int,
) -> tuple[Counter, int, int]:
    """分批查询并逐批落缓存。返回 (状态计数, 失败批次数, 总批次数)。

    单批失败（重试耗尽的限流/网络异常）跳过继续，已入库批次不回滚。"""
    counter: Counter = Counter()
    failed_batches = 0
    batch_size = max(1, source.batch_size)
    total_batches = math.ceil(len(pending) / batch_size)
    for batch_index, start in enumerate(range(0, len(pending), batch_size), start=1):
        chunk = pending[start:start + batch_size]
        try:
            results = source.map_cusips(chunk)
        except Exception as e:
            failed_batches += 1
            logger.opt(exception=e).error(
                "OpenFIGI 批次 {}/{} 查询失败（{} 个 CUSIP）: {}",
                batch_index, total_batches, len(chunk), e,
            )
            continue
        rows = [{"cusip": cusip, **result} for cusip, result in results.items()]
        db_manager.upsert_openfigi_lookups(rows)
        for result in results.values():
            counter[result["status"]] += 1
        if batch_log_every > 0 and batch_index % batch_log_every == 0:
            logger.info(
                "OpenFIGI 查询进度: {}/{} 批，累计 MATCHED={} NOT_FOUND={} AMBIGUOUS={} 失败批={}",
                batch_index, total_batches,
                counter["MATCHED"], counter["NOT_FOUND"], counter["AMBIGUOUS"], failed_batches,
            )
    return counter, failed_batches, total_batches


def load_matched_lookups(db_manager: DatabaseManager) -> list[tuple[str, str | None, str | None]]:
    """缓存中全部 MATCHED 行: [(cusip, composite_figi, share_class_figi)]。"""
    sql = text(
        "SELECT cusip, composite_figi, share_class_figi FROM openfigi_cusip_lookups "
        "WHERE status = :status ORDER BY cusip"
    )
    with db_manager.engine.connect() as conn:
        return [
            (row.cusip, row.composite_figi, row.share_class_figi)
            for row in conn.execute(sql, {"status": STATUS_MATCHED})
        ]


def build_figi_maps(
    securities: list[tuple[int, str | None, str | None, bool]],
) -> tuple[dict[str, list[tuple[int, bool]]], dict[str, list[tuple[int, bool]]]]:
    """securities 行 (id, composite_figi, share_class_figi, is_active)
    -> (composite_figi 索引, share_class_figi 索引)，值为 [(security_id, is_active)]。"""
    by_composite: dict[str, list[tuple[int, bool]]] = {}
    by_share_class: dict[str, list[tuple[int, bool]]] = {}
    for security_id, composite_figi, share_class_figi, is_active in securities:
        if composite_figi:
            by_composite.setdefault(composite_figi.strip().upper(), []).append((security_id, bool(is_active)))
        if share_class_figi:
            by_share_class.setdefault(share_class_figi.strip().upper(), []).append((security_id, bool(is_active)))
    return by_composite, by_share_class


def pick_security(candidates: list[tuple[int, bool]]) -> tuple[int | None, str]:
    """同一 FIGI 的多候选裁决：唯一活跃 -> 链接；无活跃但唯一退市 -> 链接；
    多活跃 / 多退市无活跃 -> 歧义跳过。身份合并后 husk（inactive 但保留 FIGI）
    与承接方共存时，"优先活跃"保证链到承接方。"""
    actives = [security_id for security_id, is_active in candidates if is_active]
    if len(actives) == 1:
        return actives[0], "linked_active"
    if len(actives) > 1:
        return None, "multiple_active"
    if len(candidates) == 1:
        return candidates[0][0], "linked_inactive"
    return None, "multiple_inactive"


def resolve_links(
    matched_rows: list[tuple[str, str | None, str | None]],
    by_composite: dict[str, list[tuple[int, bool]]],
    by_share_class: dict[str, list[tuple[int, bool]]],
) -> tuple[dict[str, int], Counter]:
    """MATCHED 缓存行 -> {cusip: security_id}。composite_figi 无命中时回退
    share_class_figi 同规则；歧义（多活跃/多退市）跳过并告警。"""
    links: dict[str, int] = {}
    outcomes: Counter = Counter()
    for cusip, composite_figi, share_class_figi in matched_rows:
        figi_used = (composite_figi or "").strip().upper()
        candidates = by_composite.get(figi_used) if figi_used else None
        if not candidates:
            figi_used = (share_class_figi or "").strip().upper()
            candidates = by_share_class.get(figi_used) if figi_used else None
        if not candidates:
            outcomes["no_security_match"] += 1
            continue
        security_id, reason = pick_security(candidates)
        outcomes[reason] += 1
        if security_id is None:
            logger.warning(
                "CUSIP {} 的 FIGI {} 命中多个 security（{}），歧义跳过: {}",
                cusip, figi_used, reason,
                [(sid, active) for sid, active in candidates],
            )
            continue
        links[cusip] = security_id
    return links, outcomes


def load_securities_figi(db_manager: DatabaseManager) -> list[tuple[int, str | None, str | None, bool]]:
    sql = text(
        """
        SELECT id, composite_figi, share_class_figi, is_active
        FROM securities
        WHERE composite_figi IS NOT NULL OR share_class_figi IS NOT NULL
        """
    )
    with db_manager.engine.connect() as conn:
        return [
            (row.id, row.composite_figi, row.share_class_figi, row.is_active)
            for row in conn.execute(sql)
        ]


def run(args: argparse.Namespace, source: OpenFigiSource, db_manager: DatabaseManager) -> tuple[int, dict]:
    # --- 阶段 1：候选集 + 缓存过滤 + API 查询落缓存 ---
    candidates = load_candidate_cusips(db_manager)
    cache = load_lookup_cache(db_manager, candidates)
    pending = select_cusips_to_query(candidates, cache, args.refresh_days)
    if args.limit > 0:
        pending = pending[: args.limit]
    logger.info(
        "未映射 CUSIP 候选 {} 个，缓存过滤后待查 {} 个（MATCHED 永不重查，负缓存 TTL {} 天）。",
        len(candidates), len(pending), args.refresh_days,
    )

    counter, failed_batches, total_batches = query_and_cache(
        source, db_manager, pending, args.batch_log_every
    )
    if failed_batches:
        logger.warning("OpenFIGI 查询批次失败 {}/{}，已得批次照常落链。", failed_batches, total_batches)

    # --- 阶段 2：全量 MATCHED 缓存解析落链（纯本地，不耗 API 配额）---
    matched_rows = load_matched_lookups(db_manager)
    by_composite, by_share_class = build_figi_maps(load_securities_figi(db_manager))
    links, outcomes = resolve_links(matched_rows, by_composite, by_share_class)
    logger.info(
        "MATCHED 缓存 {} 行 -> 解析链接 {} 个（活跃 {}，退市 {}；多活跃歧义 {}，多退市歧义 {}，无库内命中 {}）。",
        len(matched_rows), len(links),
        outcomes["linked_active"], outcomes["linked_inactive"],
        outcomes["multiple_active"], outcomes["multiple_inactive"], outcomes["no_security_match"],
    )

    identifier_rows = [
        {
            "security_id": security_id,
            "id_type": "CUSIP",
            "id_value": cusip,
            "source": "OPENFIGI",
            "confidence": "openfigi_figi_match",
            # 快照语义：CUSIP 不像 ticker 会回收，不设 PIT 起点
            "start_date": None,
        }
        for cusip, security_id in links.items()
    ]
    inserted = db_manager.insert_missing_security_identifiers(identifier_rows)
    backfilled = db_manager.map_unlinked_holdings_to_securities()
    logger.info(
        "security_identifiers 新插入 {} 行；institutional_holdings 回填 security_id {} 行。",
        inserted, backfilled,
    )

    stats = {
        "candidates": len(candidates),
        "queried": sum(counter.values()),
        "matched": counter["MATCHED"],
        "not_found": counter["NOT_FOUND"],
        "ambiguous": counter["AMBIGUOUS"],
        "failed_batches": failed_batches,
        "linked": len(links),
        "identifiers_inserted": inserted,
        "holdings_backfilled": backfilled,
    }
    # 全批失败才算失败运行；部分失败保留已得成果，靠 stats 观测
    exit_code = 1 if total_batches and failed_batches == total_batches else 0
    return exit_code, stats


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("sync_openfigi_identifiers")
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        source = OpenFigiSource()  # key 从 OPENFIGI_API_KEY 读取；无 key 走匿名限额
        exit_code, stats = run(args, source, db_manager)
        logger.info("任务统计: {}", stats)
        return TaskResult(exit_code, stats)
    except Exception as e:
        logger.opt(exception=e).critical("sync_openfigi_identifiers 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
