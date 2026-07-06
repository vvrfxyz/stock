"""Archive ticker_events 驱动的身份史增厚（CRSP-grade 任务 4 步骤 1-2）。

数据源：fundamentals 归档的 ticker_events.parquet（Massive ticker_events 2026-04 快照，
30,431 行；默认路径 /tmp/fundamentals_massive/fundamentals/ticker_events/US/）。
归档快照与库内 live 数据冲突时一律 live 胜（FBL 教训：vendor 会事后挪用快照 id/字段）。

三个可独立运行的阶段（--stage renames|figi|identifiers|all，默认 all；
默认 dry-run 只出报告，--apply 才写库）：

1. renames — RENAME 考据：
   - epoch 哨兵日期（1969-12-31，46.6% 行）直接丢弃，只用于本阶段的事件链；
   - 按 queried_ticker 分组（组内 cik/composite_figi 恒定，为被查实体的属性），
     解析优先级 composite_figi -> cik，要求唯一命中；歧义/锚冲突进报告桶，绝不猜写；
   - 事件链尾部 ticker 必须等于库内 symbol/current_symbol（快照后改名、实体错配
     一律 tail_mismatch 报告——live 胜）；与 live symbol history 起始日冲突的实体
     整组隔离（live_date_conflict）；
   - 相邻事件 ticker 变化 = 一次 RENAME（old->new @ 后一事件日期），写
     security_identity_events（resolution_source='AUDIT'；confidence HIGH=FIGI+CIK
     双锚同一证券且 ticker 可被库内佐证，MEDIUM=单锚或双锚不可佐证）；幂等：
     (security_id, old, new, details.event_date) 已存在即跳过；
   - 同一证据为【退市】证券补 symbol history 任期行（source='MASSIVE_ARCHIVE'，
     唯一键含 source 永不与 live 行相撞；(security_id, symbol) 已有 live 行则跳过）。

2. figi — 退市 FIGI 补链：
   inactive 且 composite_figi IS NULL 的证券，用 parquet 的 cik->composite_figi
   唯一映射回填。本阶段用【全部】parquet 行（含 epoch 哨兵行）——哨兵只毒化日期列，
   cik/figi 列仍是有效的实体属性。fill-never-overwrite：UPDATE ... WHERE
   composite_figi IS NULL。护栏（全部只报告不写）：cik 在 parquet 映射多个 FIGI、
   cik 在库内命中多只证券（多 share class 无法分摊）、目标 FIGI 已被其它证券持有
   （会制造 dup-FIGI 合并候选）、批内两只证券抢同一 FIGI。成功回填的行同时物化
   security_identifiers FIGI 行（source='MASSIVE_ARCHIVE'，快照语义 start_date=NULL）。

3. identifiers — PIT FIGI 物化：
   为所有 composite_figi 非空、且尚无该值 FIGI identifier 行（任意 source）的证券
   插入 security_identifiers（id_type='FIGI', source='MASSIVE', start_date=NULL
   快照语义，镜像 sync_openfigi_identifiers 的 CUSIP 快照行口径）。
   注意 --stage all 顺序为 renames -> figi -> identifiers：figi 阶段回填的值在
   回填时即以 MASSIVE_ARCHIVE 物化，identifiers 阶段只补 securities 表既有值。

报告：各阶段计数 + 验收指标（RENAME 覆盖率 / 退市 FIGI 覆盖率）打 stdout，
明细桶写 logs/backfill_rename_events_report_*.json 供人工裁决。

用法：
    python scripts/backfill_rename_events.py                          # 全阶段 dry-run
    python scripts/backfill_rename_events.py --stage renames --apply
    python scripts/backfill_rename_events.py --parquet /path/ticker_events.parquet --apply
"""
import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

EPOCH_SENTINEL = date(1969, 12, 31)
ARCHIVE_SOURCE = "MASSIVE_ARCHIVE"
LIVE_SOURCE = "MASSIVE"
DEFAULT_PARQUET = "/tmp/fundamentals_massive/fundamentals/ticker_events/US/ticker_events.parquet"
STAGES = ("renames", "figi", "identifiers")
BUCKET_DETAIL_CAP = 2000
IDENTIFIER_INSERT_CHUNK = 5000
BOTH_ANCHORS = frozenset({"figi", "cik"})


# --------------------------------------------------------------------------- #
# 归一化助手（纯函数）
# --------------------------------------------------------------------------- #
def _text_or_none(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and value != value:  # NaN
        return None
    stripped = str(value).strip()
    return stripped or None


def _norm_symbol(value) -> str | None:
    stripped = _text_or_none(value)
    return stripped.lower() if stripped else None


def _norm_figi(value) -> str | None:
    stripped = _text_or_none(value)
    return stripped.upper() if stripped else None


def _norm_cik(value) -> str | None:
    """CIK 归一化：去前导零后比较（库内为 10 位零填充，归档同）。"""
    stripped = _text_or_none(value)
    if not stripped:
        return None
    return stripped.lstrip("0") or None


def _coerce_date(value) -> date | None:
    if value is None or value != value:  # None / NaN / NaT（NaT != NaT 为真）
        return None
    if isinstance(value, datetime):  # 必须先于 date 判断（datetime 是 date 子类）
        try:
            return value.date()
        except ValueError:           # pandas NaT
            return None
    if isinstance(value, date):
        return value
    stripped = _text_or_none(value)
    if not stripped:
        return None
    try:
        return date.fromisoformat(stripped[:10])
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# 归档解析（纯函数，单元可测）
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SecurityInfo:
    id: int
    symbol: str | None            # lower
    current_symbol: str | None    # lower
    cik: str | None               # 去前导零
    figi: str | None              # upper
    is_active: bool
    delist_date: date | None


@dataclass
class ArchiveGroup:
    """一次 queried_ticker 抓取返回的实体事件链（组内 cik/figi 为实体属性）。"""
    queried_ticker: str | None
    cik: str | None
    figi: str | None
    events: list                  # [(date, ticker_lower)] 升序，连续同 ticker 已合并
    same_date_conflict: bool = False


def load_archive_rows(parquet_path: str) -> tuple[list[dict], Counter]:
    """读归档 parquet -> 行字典列表（保留全部行含 epoch 哨兵，丢弃口径由各阶段决定）。"""
    import pandas as pd

    frame = pd.read_parquet(
        parquet_path,
        columns=["cik", "composite_figi", "queried_ticker", "event_date",
                 "event_type", "ticker_at_event", "event_payload"],
    )
    counts: Counter = Counter(parquet_rows_total=len(frame))
    rows: list[dict] = []
    for rec in frame.itertuples(index=False):
        if _text_or_none(rec.event_type) != "ticker_change":
            counts["non_ticker_change_dropped"] += 1
            continue
        ticker = _norm_symbol(rec.ticker_at_event)
        if ticker is None:
            payload = _text_or_none(rec.event_payload)
            if payload:
                try:
                    ticker = _norm_symbol((json.loads(payload).get("ticker_change") or {}).get("ticker"))
                except (ValueError, AttributeError):
                    ticker = None
        event_date = _coerce_date(rec.event_date)
        if ticker is None or event_date is None:
            counts["invalid_dropped"] += 1
            continue
        rows.append({
            "queried_ticker": _norm_symbol(rec.queried_ticker),
            "cik": _norm_cik(rec.cik),
            "figi": _norm_figi(rec.composite_figi),
            "event_date": event_date,
            "ticker": ticker,
        })
    return rows, counts


def drop_epoch_rows(rows: list[dict]) -> tuple[list[dict], int]:
    """epoch 哨兵日期行直接丢弃（todo 文档坑：46.6% 行是 1969-12-31）。"""
    kept = [row for row in rows if row["event_date"] != EPOCH_SENTINEL]
    return kept, len(rows) - len(kept)


def build_groups(rows: list[dict]) -> list[ArchiveGroup]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row["queried_ticker"]].append(row)
    groups: list[ArchiveGroup] = []
    for queried, members in sorted(grouped.items(), key=lambda kv: kv[0] or ""):
        cik = next((m["cik"] for m in members if m["cik"]), None)
        figi = next((m["figi"] for m in members if m["figi"]), None)
        pairs = sorted({(m["event_date"], m["ticker"]) for m in members})
        seen_dates: dict[date, str] = {}
        conflict = False
        for event_date, ticker in pairs:
            if event_date in seen_dates and seen_dates[event_date] != ticker:
                conflict = True
            seen_dates[event_date] = ticker
        events: list[tuple[date, str]] = []
        for event_date, ticker in pairs:
            if events and events[-1][1] == ticker:
                continue
            events.append((event_date, ticker))
        groups.append(ArchiveGroup(queried, cik, figi, events, conflict))
    return groups


# --------------------------------------------------------------------------- #
# 身份解析（纯函数）
# --------------------------------------------------------------------------- #
@dataclass
class GroupResolution:
    group: ArchiveGroup
    security: SecurityInfo | None = None
    anchors: frozenset = frozenset()
    bucket: str | None = None
    note: str = ""


def resolve_group(
    group: ArchiveGroup,
    by_figi: dict[str, list[SecurityInfo]],
    by_cik: dict[str, list[SecurityInfo]],
) -> GroupResolution:
    """composite_figi 优先、cik 次之，要求唯一命中；歧义/锚冲突进报告桶。"""
    figi_candidates = by_figi.get(group.figi, []) if group.figi else []
    cik_candidates = by_cik.get(group.cik, []) if group.cik else []

    if group.figi and len(figi_candidates) > 1:
        return GroupResolution(
            group, bucket="figi_ambiguous",
            note=f"figi 命中多只 security: {sorted(s.id for s in figi_candidates)}",
        )
    figi_sec = figi_candidates[0] if len(figi_candidates) == 1 else None
    cik_sec = cik_candidates[0] if len(cik_candidates) == 1 else None

    if figi_sec is not None and cik_sec is not None:
        if figi_sec.id == cik_sec.id:
            return GroupResolution(group, security=figi_sec, anchors=BOTH_ANCHORS)
        return GroupResolution(
            group, bucket="anchor_conflict",
            note=f"figi->{figi_sec.id} 与 cik->{cik_sec.id} 指向不同 security",
        )
    if figi_sec is not None:
        if len(cik_candidates) > 1 and figi_sec.id not in {s.id for s in cik_candidates}:
            return GroupResolution(
                group, bucket="anchor_conflict",
                note=f"figi->{figi_sec.id} 不在 cik 多候选集 {sorted(s.id for s in cik_candidates)} 内",
            )
        return GroupResolution(group, security=figi_sec, anchors=frozenset({"figi"}))
    if cik_sec is not None:
        return GroupResolution(group, security=cik_sec, anchors=frozenset({"cik"}))
    if group.cik and len(cik_candidates) > 1:
        return GroupResolution(
            group, bucket="cik_ambiguous",
            note=f"cik 命中多只 security: {sorted(s.id for s in cik_candidates)}",
        )
    return GroupResolution(group, bucket="unresolved", note="figi/cik 均无唯一库内命中")


@dataclass
class EntityEvidence:
    """多个 queried_ticker 组可解析到同一 security，此处按 security 合并证据。"""
    security: SecurityInfo
    anchors: set = field(default_factory=set)
    queried_tickers: list = field(default_factory=list)
    archive_figis: set = field(default_factory=set)
    archive_ciks: set = field(default_factory=set)
    events: list = field(default_factory=list)
    same_date_conflict: bool = False


def merge_by_security(resolutions: list[GroupResolution]) -> dict[int, EntityEvidence]:
    entities: dict[int, EntityEvidence] = {}
    raw_events: dict[int, set] = defaultdict(set)
    for res in resolutions:
        if res.security is None:
            continue
        sid = res.security.id
        ent = entities.get(sid)
        if ent is None:
            ent = EntityEvidence(security=res.security)
            entities[sid] = ent
        ent.anchors |= set(res.anchors)
        if res.group.queried_ticker:
            ent.queried_tickers.append(res.group.queried_ticker)
        if res.group.figi:
            ent.archive_figis.add(res.group.figi)
        if res.group.cik:
            ent.archive_ciks.add(res.group.cik)
        ent.same_date_conflict = ent.same_date_conflict or res.group.same_date_conflict
        raw_events[sid].update(res.group.events)
    for sid, ent in entities.items():
        pairs = sorted(raw_events[sid])
        seen_dates: dict[date, str] = {}
        for event_date, ticker in pairs:
            if event_date in seen_dates and seen_dates[event_date] != ticker:
                ent.same_date_conflict = True
            seen_dates[event_date] = ticker
        events: list[tuple[date, str]] = []
        for event_date, ticker in pairs:
            if events and events[-1][1] == ticker:
                continue
            events.append((event_date, ticker))
        ent.events = events
    return entities


# --------------------------------------------------------------------------- #
# 阶段 1：RENAME 事件 + 退市任期计划（纯函数）
# --------------------------------------------------------------------------- #
@dataclass
class RenamePlan:
    events: list = field(default_factory=list)     # security_identity_events 行
    tenures: list = field(default_factory=list)    # security_symbol_history 行
    buckets: dict = field(default_factory=lambda: defaultdict(list))
    counts: Counter = field(default_factory=Counter)


def plan_renames(
    entities: dict[int, EntityEvidence],
    live_history: dict[int, dict[str, set]],
    existing_rename_keys: dict[tuple, set],
) -> RenamePlan:
    """live 胜原则：尾部 ticker 不符 / 与 live history 日期冲突的实体整组只报告不写。"""
    plan = RenamePlan()
    for sid in sorted(entities):
        ent = entities[sid]
        sec = ent.security
        history_syms = live_history.get(sid, {})
        known_symbols = set(history_syms)
        if sec.symbol:
            known_symbols.add(sec.symbol)
        if sec.current_symbol:
            known_symbols.add(sec.current_symbol)

        def _entity_detail(extra: dict | None = None) -> dict:
            detail = {
                "security_id": sid,
                "db_symbol": sec.symbol,
                "db_current_symbol": sec.current_symbol,
                "is_active": sec.is_active,
                "queried_tickers": sorted(set(ent.queried_tickers)),
                "archive_events": [(d.isoformat(), t) for d, t in ent.events],
                "anchors": sorted(ent.anchors),
            }
            detail.update(extra or {})
            return detail

        if not ent.events:
            continue
        if ent.same_date_conflict:
            plan.counts["entities_same_date_conflict"] += 1
            plan.buckets["same_date_conflict"].append(_entity_detail())
            continue
        tail = ent.events[-1][1]
        if tail != sec.symbol and tail != sec.current_symbol:
            # 快照后改名 / 实体错配 / vendor 杂音（如 A->AWD）——live 胜，只报告
            plan.counts["entities_tail_mismatch"] += 1
            plan.buckets["tail_mismatch"].append(_entity_detail({"archive_tail": tail}))
            continue

        transitions = [
            (ent.events[i - 1][1], ent.events[i][1], ent.events[i][0])
            for i in range(1, len(ent.events))
        ]
        date_conflicts = []
        for old, new, tdate in transitions:
            live_starts = history_syms.get(new)
            if live_starts and None not in live_starts and tdate not in live_starts:
                date_conflicts.append({
                    "old": old, "new": new, "archive_date": tdate.isoformat(),
                    "live_start_dates": sorted(d.isoformat() for d in live_starts),
                })
        if date_conflicts:
            plan.counts["entities_live_date_conflict"] += 1
            plan.buckets["live_date_conflict"].append(_entity_detail({"conflicts": date_conflicts}))
            continue

        plan.counts["entities_gated"] += 1
        for old, new, tdate in transitions:
            plan.counts["transitions_total"] += 1
            existing_dates = existing_rename_keys.get((sid, old, new))
            if existing_dates is not None:
                if None in existing_dates or tdate in existing_dates:
                    plan.counts["events_already_exist"] += 1
                    continue
                # 同符号对但日期不同的既有事件：live 胜，只报告
                plan.counts["events_date_mismatch"] += 1
                plan.buckets["event_date_mismatch"].append(_entity_detail({
                    "old": old, "new": new, "archive_date": tdate.isoformat(),
                    "existing_event_dates": sorted(
                        d.isoformat() if d else "NULL" for d in existing_dates
                    ),
                }))
                continue
            corroborated = old in known_symbols or new in known_symbols
            confidence = "HIGH" if ent.anchors == set(BOTH_ANCHORS) and corroborated else "MEDIUM"
            plan.counts[f"events_planned_{confidence.lower()}"] += 1
            plan.events.append({
                "security_id": sid,
                "event_type": "RENAME",
                "old_symbol": old,
                "new_symbol": new,
                "resolution_source": "AUDIT",
                "confidence": confidence,
                "details": json.dumps({
                    "script": "backfill_rename_events",
                    "source": ARCHIVE_SOURCE,
                    "archive_file": "ticker_events.parquet",
                    "event_date": tdate.isoformat(),
                    "anchors": sorted(ent.anchors),
                    "archive_figis": sorted(ent.archive_figis),
                    "archive_ciks": sorted(ent.archive_ciks),
                    "queried_tickers": sorted(set(ent.queried_tickers)),
                    "corroborated": corroborated,
                }, ensure_ascii=False),
            })

        # 退市证券补任期行（active 证券的 live 管道自会维护自己的 history）
        if not sec.is_active:
            for i, (start, sym) in enumerate(ent.events):
                if sym in history_syms:
                    plan.counts["tenures_skipped_live_covered"] += 1
                    continue
                if i + 1 < len(ent.events):
                    end = ent.events[i + 1][0]
                elif sec.delist_date and sec.delist_date >= start:
                    end = sec.delist_date
                else:
                    end = None
                plan.counts["tenures_planned"] += 1
                plan.tenures.append({
                    "security_id": sid,
                    "symbol": sym,
                    "source": ARCHIVE_SOURCE,
                    "source_event_id": f"{sid}:{sym}:{start.isoformat()}",
                    "event_type": "ticker_change",
                    "start_date": start,
                    "end_date": end,
                })
    return plan


# --------------------------------------------------------------------------- #
# 阶段 2：退市 FIGI 补链计划（纯函数）
# --------------------------------------------------------------------------- #
@dataclass
class FigiPlan:
    fills: list = field(default_factory=list)      # [(security_id, figi)]
    buckets: dict = field(default_factory=lambda: defaultdict(list))
    counts: Counter = field(default_factory=Counter)


def build_cik_figi_map(rows: list[dict]) -> dict[str, set]:
    """全部归档行（含 epoch 哨兵——哨兵只毒化日期，cik/figi 仍有效）-> cik: {figi}。"""
    mapping: dict[str, set] = defaultdict(set)
    for row in rows:
        if row["cik"] and row["figi"]:
            mapping[row["cik"]].add(row["figi"])
    return mapping


def plan_figi_fills(cik_figi_map: dict[str, set], securities: list[SecurityInfo]) -> FigiPlan:
    plan = FigiPlan()
    by_cik_all: dict[str, list[SecurityInfo]] = defaultdict(list)
    figi_holders: dict[str, list[int]] = defaultdict(list)
    for sec in securities:
        if sec.cik:
            by_cik_all[sec.cik].append(sec)
        if sec.figi:
            figi_holders[sec.figi].append(sec.id)

    candidates = [s for s in securities if not s.is_active and s.figi is None and s.cik]
    plan.counts["candidates"] = len(candidates)
    tentative: dict[str, list[SecurityInfo]] = defaultdict(list)
    for sec in sorted(candidates, key=lambda s: s.id):
        figis = cik_figi_map.get(sec.cik)
        if not figis:
            plan.counts["no_parquet_match"] += 1
            continue
        if len(figis) > 1:
            plan.counts["parquet_multi_figi"] += 1
            plan.buckets["parquet_multi_figi"].append({
                "security_id": sec.id, "symbol": sec.symbol, "cik": sec.cik,
                "figis": sorted(figis),
            })
            continue
        figi = next(iter(figis))
        cik_peers = by_cik_all.get(sec.cik, [])
        if len(cik_peers) > 1:
            # 同 CIK 多只证券（多 share class）：单一 FIGI 无法归属到某一类，报告
            plan.counts["db_multi_security_cik"] += 1
            plan.buckets["db_multi_security_cik"].append({
                "security_id": sec.id, "symbol": sec.symbol, "cik": sec.cik,
                "figi": figi, "peer_ids": sorted(p.id for p in cik_peers),
            })
            continue
        if figi in figi_holders:
            # 该 FIGI 已被其它证券持有：写入会制造 dup-FIGI 合并候选，报告
            plan.counts["figi_already_held"] += 1
            plan.buckets["figi_already_held"].append({
                "security_id": sec.id, "symbol": sec.symbol, "cik": sec.cik,
                "figi": figi, "held_by": sorted(figi_holders[figi]),
            })
            continue
        tentative[figi].append(sec)

    for figi, secs in sorted(tentative.items()):
        if len(secs) > 1:
            plan.counts["dup_figi_in_batch"] += len(secs)
            plan.buckets["dup_figi_in_batch"].append({
                "figi": figi, "security_ids": sorted(s.id for s in secs),
            })
            continue
        plan.fills.append((secs[0].id, figi))
    plan.counts["fills_planned"] = len(plan.fills)
    return plan


# --------------------------------------------------------------------------- #
# 阶段 3：PIT FIGI 物化计划（纯函数）
# --------------------------------------------------------------------------- #
def plan_identifier_rows(
    securities: list[SecurityInfo],
    existing_figi_pairs: set,
) -> tuple[list[dict], Counter]:
    """(security_id, id_value) 已有任意 source 的 FIGI 行则跳过——figi 阶段回填的值
    在回填时已以 MASSIVE_ARCHIVE 物化，此处不重复造 MASSIVE 行。"""
    rows: list[dict] = []
    counts: Counter = Counter()
    for sec in sorted(securities, key=lambda s: s.id):
        if not sec.figi:
            continue
        counts["securities_with_figi"] += 1
        if (sec.id, sec.figi) in existing_figi_pairs:
            counts["already_materialized"] += 1
            continue
        rows.append({
            "security_id": sec.id,
            "id_type": "FIGI",
            "id_value": sec.figi,
            "source": LIVE_SOURCE,
            "start_date": None,   # 快照语义：镜像 sync_openfigi_identifiers 的 CUSIP 行
            "confidence": "securities_snapshot",
        })
    counts["rows_planned"] = len(rows)
    return rows, counts


# --------------------------------------------------------------------------- #
# DB 读取
# --------------------------------------------------------------------------- #
def load_securities(engine) -> list[SecurityInfo]:
    sql = text(
        "SELECT id, symbol, current_symbol, cik, composite_figi, is_active, delist_date "
        "FROM securities"
    )
    with engine.connect() as conn:
        return [
            SecurityInfo(
                id=row.id,
                symbol=_norm_symbol(row.symbol),
                current_symbol=_norm_symbol(row.current_symbol),
                cik=_norm_cik(row.cik),
                figi=_norm_figi(row.composite_figi),
                is_active=bool(row.is_active),
                delist_date=row.delist_date,
            )
            for row in conn.execute(sql)
        ]


def build_security_indexes(
    securities: list[SecurityInfo],
) -> tuple[dict[str, list[SecurityInfo]], dict[str, list[SecurityInfo]]]:
    by_figi: dict[str, list[SecurityInfo]] = defaultdict(list)
    by_cik: dict[str, list[SecurityInfo]] = defaultdict(list)
    for sec in securities:
        if sec.figi:
            by_figi[sec.figi].append(sec)
        if sec.cik:
            by_cik[sec.cik].append(sec)
    return dict(by_figi), dict(by_cik)


def load_live_history(engine) -> dict[int, dict[str, set]]:
    """live（非 MASSIVE_ARCHIVE）symbol history：{security_id: {symbol: {start_date|None}}}。"""
    sql = text(
        "SELECT security_id, symbol, start_date FROM security_symbol_history "
        "WHERE source <> :archive"
    )
    history: dict[int, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    with engine.connect() as conn:
        for row in conn.execute(sql, {"archive": ARCHIVE_SOURCE}):
            sym = _norm_symbol(row.symbol)
            if sym:
                history[row.security_id][sym].add(row.start_date)
    return {sid: dict(syms) for sid, syms in history.items()}


def load_existing_rename_keys(engine) -> dict[tuple, set]:
    """既有 RENAME 事件 -> {(security_id, old, new): {details.event_date|None}}。"""
    sql = text(
        "SELECT security_id, old_symbol, new_symbol, details "
        "FROM security_identity_events WHERE event_type = 'RENAME'"
    )
    keys: dict[tuple, set] = defaultdict(set)
    with engine.connect() as conn:
        for row in conn.execute(sql):
            event_date = None
            if row.details:
                try:
                    event_date = _coerce_date(json.loads(row.details).get("event_date"))
                except (ValueError, AttributeError):
                    event_date = None
            keys[(row.security_id, _norm_symbol(row.old_symbol), _norm_symbol(row.new_symbol))].add(event_date)
    return dict(keys)


def load_existing_figi_pairs(engine) -> set:
    sql = text("SELECT security_id, id_value FROM security_identifiers WHERE id_type = 'FIGI'")
    with engine.connect() as conn:
        return {(row.security_id, _norm_figi(row.id_value)) for row in conn.execute(sql)}


def load_acceptance_metrics(engine) -> dict:
    """验收指标基线：live 口径改名分母（873 量级）、RENAME 事件数、退市 FIGI 覆盖。"""
    with engine.connect() as conn:
        denom = conn.execute(text(
            "SELECT count(*) AS securities, coalesce(sum(n - 1), 0) AS transitions FROM ("
            "  SELECT security_id, count(DISTINCT symbol) AS n"
            "  FROM security_symbol_history WHERE source <> :archive"
            "  GROUP BY security_id HAVING count(DISTINCT symbol) > 1) t"
        ), {"archive": ARCHIVE_SOURCE}).one()
        multi_sids = {row.security_id for row in conn.execute(text(
            "SELECT security_id FROM security_symbol_history WHERE source <> :archive "
            "GROUP BY security_id HAVING count(DISTINCT symbol) > 1"
        ), {"archive": ARCHIVE_SOURCE})}
        rename_total = conn.execute(text(
            "SELECT count(*) FROM security_identity_events WHERE event_type = 'RENAME'"
        )).scalar() or 0
        rename_sids = {row.security_id for row in conn.execute(text(
            "SELECT DISTINCT security_id FROM security_identity_events WHERE event_type = 'RENAME'"
        ))}
        figi_cov = conn.execute(text(
            "SELECT count(*) FILTER (WHERE NOT is_active) AS inactive_total, "
            "count(*) FILTER (WHERE NOT is_active AND composite_figi IS NOT NULL) AS inactive_with_figi "
            "FROM securities"
        )).one()
    return {
        "denominator_securities": int(denom.securities),
        "denominator_transitions": int(denom.transitions),
        "multi_symbol_sids": multi_sids,
        "rename_events_total": int(rename_total),
        "rename_covered_securities": len(multi_sids & rename_sids),
        "rename_event_sids": rename_sids,
        "inactive_total": int(figi_cov.inactive_total),
        "inactive_with_figi": int(figi_cov.inactive_with_figi),
    }


# --------------------------------------------------------------------------- #
# 写入（--apply）
# --------------------------------------------------------------------------- #
def apply_figi_fills(db_manager: DatabaseManager, fills: list) -> tuple[list, int]:
    """fill-never-overwrite：composite_figi 补空走 enrich_security_identity
    （NULL-only 守卫收口进 db_manager）；返回 (实际回填, 竞态跳过数)。
    rowcount=0（行不存在或 figi 已被他人补入）计竞态跳过，与旧
    WHERE composite_figi IS NULL 直写语义一致。"""
    applied: list[tuple[int, str]] = []
    raced = 0
    for sid, figi in fills:
        if db_manager.enrich_security_identity(sid, {"composite_figi": figi}):
            applied.append((sid, figi))
        else:
            raced += 1
    if applied:
        db_manager.insert_missing_security_identifiers([
            {
                "security_id": sid,
                "id_type": "FIGI",
                "id_value": figi,
                "source": ARCHIVE_SOURCE,
                "start_date": None,
                "confidence": "cik_unique_match",
            }
            for sid, figi in applied
        ])
    return applied, raced


# --------------------------------------------------------------------------- #
# 报告
# --------------------------------------------------------------------------- #
def _capped_buckets(buckets: dict) -> dict:
    return {
        name: {"count": len(items), "items": items[:BUCKET_DETAIL_CAP]}
        for name, items in sorted(buckets.items())
    }


def write_report(report: dict, report_dir: str) -> str:
    os.makedirs(report_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(report_dir, f"backfill_rename_events_report_{stamp}.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2, default=str)
    return path


def _pct(numerator: int, denominator: int) -> str:
    if not denominator:
        return "n/a"
    return f"{100.0 * numerator / denominator:.1f}%"


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #
def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="归档 ticker_events 驱动的 RENAME 溯源 + 退市 FIGI 补链 + PIT FIGI 物化。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--parquet", default=DEFAULT_PARQUET,
                        help=f"归档 ticker_events.parquet 路径（默认 {DEFAULT_PARQUET}）。")
    parser.add_argument("--stage", choices=[*STAGES, "all"], default="all",
                        help="只跑某一阶段（默认 all，顺序 renames -> figi -> identifiers）。")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只出计划与报告，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="执行写入。先跑 dry-run 人工确认报告桶。")
    parser.add_argument("--report-dir", default=os.path.join(project_root, "logs"),
                        help="JSON 明细报告输出目录（默认 logs/）。")
    return parser


def run(args: argparse.Namespace, db_manager: DatabaseManager) -> tuple[int, dict]:
    apply_mode = bool(args.apply)
    stages = STAGES if args.stage == "all" else (args.stage,)
    mode_label = "APPLY" if apply_mode else "dry-run"

    all_rows, load_counts = load_archive_rows(args.parquet)
    securities = load_securities(db_manager.engine)
    metrics_before = load_acceptance_metrics(db_manager.engine)

    report: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode_label,
        "parquet": args.parquet,
        "stages_run": list(stages),
        "load": dict(load_counts),
        "stages": {},
    }
    stats: dict = {"mode": mode_label, **load_counts}
    summary_lines = [
        f"================ backfill_rename_events 报告（{mode_label}） ================",
        f"[load] {dict(load_counts)}",
    ]

    # ---- 阶段 1：renames ----
    if "renames" in stages:
        event_rows, epoch_dropped = drop_epoch_rows(all_rows)
        groups = build_groups(event_rows)
        by_figi, by_cik = build_security_indexes(securities)
        resolutions = [resolve_group(group, by_figi, by_cik) for group in groups]

        resolution_counts: Counter = Counter()
        resolution_buckets: dict = defaultdict(list)
        for res in resolutions:
            if res.security is not None:
                resolution_counts["groups_resolved"] += 1
                resolution_counts[f"groups_anchor_{'+'.join(sorted(res.anchors))}"] += 1
            else:
                resolution_counts[f"groups_{res.bucket}"] += 1
                resolution_buckets[res.bucket].append({
                    "queried_ticker": res.group.queried_ticker,
                    "figi": res.group.figi,
                    "cik": res.group.cik,
                    "events": [(d.isoformat(), t) for d, t in res.group.events],
                    "note": res.note,
                })

        entities = merge_by_security(resolutions)
        live_history = load_live_history(db_manager.engine)
        existing_rename_keys = load_existing_rename_keys(db_manager.engine)
        plan = plan_renames(entities, live_history, existing_rename_keys)

        events_written = tenures_written = 0
        if apply_mode:
            if plan.events:
                events_written = db_manager.insert_identity_events(plan.events)
            if plan.tenures:
                tenures_written = db_manager.upsert_symbol_history(plan.tenures)

        stage_counts = {
            "epoch_dropped": epoch_dropped,
            "groups_total": len(groups),
            **resolution_counts,
            "entities_resolved": len(entities),
            **plan.counts,
            "events_planned_total": len(plan.events),
            "tenures_planned_total": len(plan.tenures),
            "events_written": events_written,
            "tenures_written": tenures_written,
        }
        report["stages"]["renames"] = {
            "counts": stage_counts,
            "buckets": _capped_buckets({**resolution_buckets, **plan.buckets}),
        }
        stats.update({f"renames_{k}": v for k, v in stage_counts.items()})
        summary_lines.append(
            f"[renames] epoch_dropped={epoch_dropped} groups={len(groups)} "
            f"resolved={resolution_counts['groups_resolved']} entities={len(entities)} "
            f"gated={plan.counts['entities_gated']}"
        )
        summary_lines.append(
            f"          事件: planned HIGH={plan.counts['events_planned_high']} "
            f"MEDIUM={plan.counts['events_planned_medium']} "
            f"already_exist={plan.counts['events_already_exist']} "
            f"date_mismatch={plan.counts['events_date_mismatch']} written={events_written}"
        )
        summary_lines.append(
            f"          任期: planned={plan.counts['tenures_planned']} "
            f"skipped_live_covered={plan.counts['tenures_skipped_live_covered']} written={tenures_written}"
        )
        all_buckets = {**resolution_buckets, **plan.buckets}
        if all_buckets:
            bucket_line = ", ".join(
                f"{name}={len(items)}" for name, items in sorted(all_buckets.items())
            )
        else:
            bucket_line = "无"
        summary_lines.append(f"          报告桶: {bucket_line}")
        planned_event_sids = {row["security_id"] for row in plan.events}
    else:
        plan = None
        planned_event_sids = set()

    # ---- 阶段 2：figi ----
    figi_plan = None
    applied_fills: list = []
    if "figi" in stages:
        cik_figi_map = build_cik_figi_map(all_rows)  # 全部行：哨兵只毒化日期列
        figi_plan = plan_figi_fills(cik_figi_map, securities)
        raced = 0
        if apply_mode and figi_plan.fills:
            applied_fills, raced = apply_figi_fills(db_manager, figi_plan.fills)
        stage_counts = {
            **figi_plan.counts,
            "fills_applied": len(applied_fills),
            "fills_raced_skipped": raced,
        }
        report["stages"]["figi"] = {
            "counts": stage_counts,
            "buckets": _capped_buckets(figi_plan.buckets),
            "fills_planned": figi_plan.fills[:BUCKET_DETAIL_CAP],
        }
        stats.update({f"figi_{k}": v for k, v in stage_counts.items()})
        summary_lines.append(
            f"[figi] candidates={figi_plan.counts['candidates']} "
            f"fills_planned={figi_plan.counts['fills_planned']} applied={len(applied_fills)} "
            f"no_parquet_match={figi_plan.counts['no_parquet_match']} "
            f"buckets: parquet_multi_figi={figi_plan.counts['parquet_multi_figi']} "
            f"db_multi_security_cik={figi_plan.counts['db_multi_security_cik']} "
            f"figi_already_held={figi_plan.counts['figi_already_held']} "
            f"dup_figi_in_batch={figi_plan.counts['dup_figi_in_batch']}"
        )

    # ---- 阶段 3：identifiers ----
    if "identifiers" in stages:
        # figi 阶段可能刚回填过，重读快照保证本阶段看到最新值
        current_securities = load_securities(db_manager.engine) if apply_mode else securities
        existing_pairs = load_existing_figi_pairs(db_manager.engine)
        identifier_rows, id_counts = plan_identifier_rows(current_securities, existing_pairs)
        identifiers_written = 0
        if apply_mode and identifier_rows:
            # 分块插入：全量物化约 1.3 万行，避免单条超大 multi-VALUES 语句
            for start in range(0, len(identifier_rows), IDENTIFIER_INSERT_CHUNK):
                identifiers_written += db_manager.insert_missing_security_identifiers(
                    identifier_rows[start:start + IDENTIFIER_INSERT_CHUNK]
                )
        stage_counts = {**id_counts, "identifiers_written": identifiers_written}
        report["stages"]["identifiers"] = {"counts": stage_counts}
        stats.update({f"identifiers_{k}": v for k, v in stage_counts.items()})
        summary_lines.append(
            f"[identifiers] with_figi={id_counts['securities_with_figi']} "
            f"already={id_counts['already_materialized']} "
            f"planned={id_counts['rows_planned']} written={identifiers_written}"
        )
        if not apply_mode and figi_plan is not None and figi_plan.fills:
            summary_lines.append(
                f"              （dry-run 口径：figi 阶段待回填的 {len(figi_plan.fills)} 只"
                f"将在 figi --apply 时以 {ARCHIVE_SOURCE} 物化，不计入本行）"
            )

    # ---- 验收指标 ----
    metrics_after = load_acceptance_metrics(db_manager.engine) if apply_mode else None
    denom_secs = metrics_before["denominator_securities"]
    denom_trans = metrics_before["denominator_transitions"]
    if apply_mode:
        rename_total_after = metrics_after["rename_events_total"]
        covered_after = metrics_after["rename_covered_securities"]
        inactive_figi_after = metrics_after["inactive_with_figi"]
    else:
        rename_total_after = metrics_before["rename_events_total"] + (len(plan.events) if plan else 0)
        covered_after = len(
            metrics_before["multi_symbol_sids"]
            & (metrics_before["rename_event_sids"] | planned_event_sids)
        )
        inactive_figi_after = metrics_before["inactive_with_figi"] + (
            len(figi_plan.fills) if figi_plan else 0
        )
    acceptance = {
        "rename_events_before": metrics_before["rename_events_total"],
        "rename_events_after": rename_total_after,
        "denominator_securities_live": denom_secs,
        "denominator_transitions_live": denom_trans,
        "rename_covered_securities_before": metrics_before["rename_covered_securities"],
        "rename_covered_securities_after": covered_after,
        "rename_security_coverage_after": _pct(covered_after, denom_secs),
        "inactive_total": metrics_before["inactive_total"],
        "inactive_with_figi_before": metrics_before["inactive_with_figi"],
        "inactive_with_figi_after": inactive_figi_after,
        "delisted_figi_coverage_before": _pct(
            metrics_before["inactive_with_figi"], metrics_before["inactive_total"]),
        "delisted_figi_coverage_after": _pct(
            inactive_figi_after, metrics_before["inactive_total"]),
    }
    report["acceptance"] = acceptance
    stats.update({k: v for k, v in acceptance.items()})
    projected = "" if apply_mode else "（dry-run 推算值）"
    summary_lines.append(
        f"[acceptance]{projected} RENAME 事件: {acceptance['rename_events_before']} -> "
        f"{acceptance['rename_events_after']}；live 分母 {denom_secs} 只 / {denom_trans} 次改名，"
        f"证券覆盖率 {_pct(metrics_before['rename_covered_securities'], denom_secs)} -> "
        f"{acceptance['rename_security_coverage_after']}"
    )
    summary_lines.append(
        f"             退市 FIGI 覆盖: {acceptance['inactive_with_figi_before']}/"
        f"{acceptance['inactive_total']} ({acceptance['delisted_figi_coverage_before']}) -> "
        f"{acceptance['inactive_with_figi_after']}/{acceptance['inactive_total']} "
        f"({acceptance['delisted_figi_coverage_after']})"
    )

    report_path = write_report(report, args.report_dir)
    summary_lines.append(f"明细报告: {report_path}")
    if not apply_mode:
        summary_lines.append("dry-run 模式未写库；确认报告桶后加 --apply 执行。")
    summary = "\n".join(summary_lines)
    print(summary)
    logger.info("\n{}", summary)
    stats["report_path"] = report_path
    return 0, stats


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    configure_script_logging("backfill_rename_events")
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        exit_code, stats = run(args, db_manager)
        logger.info("任务统计: {}", {k: v for k, v in stats.items() if not k.startswith("report")})
        return exit_code
    except Exception as e:
        logger.opt(exception=e).critical("backfill_rename_events 执行失败: {}", e)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
