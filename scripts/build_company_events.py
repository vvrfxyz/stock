#!/usr/bin/env python3
"""公司世系/并购边表构建器（company_events）。

从两类证据缝合公司实体之间的有向世系边，写入 company_events：

  边 (a) —— Alphabet 2015 CIK 断层：
      2015-10-02 Google 重组为 Alphabet 控股结构，SEC 申报主体从旧 Google CIK
      0001288776 切到新 Alphabet CIK 0001652044。goog/googl 两只证券现挂在
      Alphabet 公司实体上，旧 Google 实体在 companies 里可能压根没有行——本脚本
      按需以旧 CIK 建一个 companies 行（name='Google Inc.'），再写
      旧-Google → Alphabet 的 CIK_CHANGE 边。重组 8-K accession 若能在 sec_filings
      里定位则记入 evidence，否则记 'alphabet_2015_reorg' 标记。

  边 (b) —— delisting_events 已解析的并购：
      reason_code ∈ (MERGER / ACQUISITION_CASH / ACQUISITION_STOCK) 且 evidence
      里带 'acquirer_security=<symbol>#<sid>' 令牌的行（收购方证券已由退市分类器
      名字精确匹配唯一解析）。predecessor = 被并证券的 securities.company_id，
      successor = 收购方 sid 的 company_id；任一为 NULL 或两者相等则跳过（同一
      公司实体内部的多类股/身份重复不是世系边）。event_type=MERGER，
      event_date=delist_date，evidence 记 consideration_docs accession + 令牌，
      source='DELISTING'。

幂等：upsert 语义，重跑用最新证据刷新已有边，不产生重复。批量 SQL，绝不对
生产库逐行循环。

用法：
    python scripts/build_company_events.py            # dry-run，只打印计划（默认）
    python scripts/build_company_events.py --apply    # 写库（先确认 dry-run 输出）
"""
import argparse
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta

from loguru import logger
from sqlalchemy import text

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_manager import DatabaseManager
from utils.script_logging import setup_logging as configure_script_logging

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

# Alphabet 2015 重组：旧 Google 申报主体 CIK 与切换日。
OLD_GOOGLE_CIK = "0001288776"
OLD_GOOGLE_NAME = "Google Inc."
ALPHABET_CIK = "0001652044"
ALPHABET_REORG_DATE = date(2015, 10, 2)
ALPHABET_REORG_MARKER = "alphabet_2015_reorg"
# 重组 8-K 在 Alphabet CIK 名下的检索窗口（申报可能略滞后切换日）。
ALPHABET_8K_WINDOW_DAYS = 21

MERGER_REASON_CODES = ("MERGER", "ACQUISITION_CASH", "ACQUISITION_STOCK")

# acquirer_security=<symbol>#<sid>：symbol 允许字母/数字/点/连字符（如 brk.a），
# sid 为纯数字；令牌以 '|' 与其余证据分隔，故 symbol 段排除 '#' 与 '|'。
_ACQUIRER_TOKEN_RE = re.compile(r"acquirer_security=([^#|]+)#(\d+)")
_CONSIDERATION_DOCS_RE = re.compile(r"consideration_docs=([^|]+)")


# ---------------------------------------------------------------------------
# 纯解析层（无 DB 依赖，供单测直接调用）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AcquirerToken:
    """从 delisting_events.evidence 解析出的收购方证券令牌。"""
    symbol: str
    security_id: int


def parse_acquirer_security_token(evidence: str | None) -> AcquirerToken | None:
    """从退市证据串里解析 acquirer_security=<symbol>#<sid> 令牌；无则 None。

    退市分类器宁缺毋滥，只在名字精确匹配唯一时才注入该令牌，故一行至多一个。"""
    if not evidence:
        return None
    match = _ACQUIRER_TOKEN_RE.search(evidence)
    if not match:
        return None
    return AcquirerToken(symbol=match.group(1), security_id=int(match.group(2)))


def parse_consideration_docs(evidence: str | None) -> list[str]:
    """从退市证据串里解析 consideration_docs= 的 accession 列表（逗号分隔）；无则 []。"""
    if not evidence:
        return []
    match = _CONSIDERATION_DOCS_RE.search(evidence)
    if not match:
        return []
    return [tok.strip() for tok in match.group(1).split(",") if tok.strip()]


def build_merger_evidence(token: AcquirerToken, consideration_docs: list[str]) -> str:
    """拼装并购边的 evidence 串：收购方令牌 + 对价文档 accession（有则附）。"""
    parts = [f"acquirer_security={token.symbol}#{token.security_id}"]
    if consideration_docs:
        parts.append("consideration_docs=" + ",".join(consideration_docs))
    return "|".join(parts)


# ---------------------------------------------------------------------------
# 生产库只读查询（批量，绝不逐行循环）
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MergerEdgeCandidate:
    """一条待评估的并购边：被并证券 + 解析出的收购方令牌 + delist_date + 对价证据。"""
    delisted_security_id: int
    delist_date: date
    reason_code: str
    token: AcquirerToken
    consideration_docs: list[str]


def load_merger_edge_candidates(session) -> list[MergerEdgeCandidate]:
    """从 delisting_events 批量拉并购族且带 acquirer_security 令牌的行，解析成候选。"""
    rows = session.execute(
        text(
            """
            SELECT security_id, delist_date, reason_code, evidence
            FROM delisting_events
            WHERE reason_code = ANY(:codes)
              AND evidence LIKE '%acquirer_security=%'
            ORDER BY security_id, delist_date
            """
        ),
        {"codes": list(MERGER_REASON_CODES)},
    ).fetchall()

    candidates: list[MergerEdgeCandidate] = []
    for security_id, delist_date, reason_code, evidence in rows:
        token = parse_acquirer_security_token(evidence)
        if token is None:
            continue
        candidates.append(
            MergerEdgeCandidate(
                delisted_security_id=security_id,
                delist_date=delist_date,
                reason_code=reason_code,
                token=token,
                consideration_docs=parse_consideration_docs(evidence),
            )
        )
    return candidates


def load_company_ids_for_securities(session, security_ids: list[int]) -> dict[int, int | None]:
    """批量取 securities.company_id 映射（一次 ANY 查询）；缺失的 sid 不进 dict。"""
    if not security_ids:
        return {}
    rows = session.execute(
        text("SELECT id, company_id FROM securities WHERE id = ANY(:ids)"),
        {"ids": list(set(security_ids))},
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def find_alphabet_reorg_accession(session) -> str | None:
    """在 Alphabet CIK 名下 2015-10-02 附近找一份 8-K 作为重组证据 accession；无则 None。

    生产库当前 Alphabet 申报只回填到 2023 起，此路多半落空——落空即由调用方
    记 alphabet_2015_reorg 标记，不阻塞建边。"""
    lo = ALPHABET_REORG_DATE - timedelta(days=ALPHABET_8K_WINDOW_DAYS)
    hi = ALPHABET_REORG_DATE + timedelta(days=ALPHABET_8K_WINDOW_DAYS)
    row = session.execute(
        text(
            """
            SELECT accession_number
            FROM sec_filings
            WHERE cik = :cik
              AND form_type LIKE '8-K%'
              AND filing_date BETWEEN :lo AND :hi
            ORDER BY abs(filing_date - :anchor)
            LIMIT 1
            """
        ),
        {"cik": ALPHABET_CIK, "lo": lo, "hi": hi, "anchor": ALPHABET_REORG_DATE},
    ).fetchone()
    return row[0] if row else None


def resolve_alphabet_company_ids(session) -> tuple[int | None, int | None]:
    """返回 (旧 Google 公司实体 id, Alphabet 公司实体 id)。

    Alphabet 实体经 goog/googl 的 company_id 定位（应一致）；旧 Google 实体经旧 CIK
    查 companies——不存在时返回 None，由调用方在 --apply 时按需创建。"""
    old_google_id = session.execute(
        text("SELECT id FROM companies WHERE cik = :cik"),
        {"cik": OLD_GOOGLE_CIK},
    ).scalar_one_or_none()
    alphabet_id = session.execute(
        text(
            """
            SELECT DISTINCT company_id
            FROM securities
            WHERE lower(symbol) IN ('goog', 'googl')
              AND company_id IS NOT NULL
            """
        )
    ).scalars().all()
    alphabet_company_id = alphabet_id[0] if len(alphabet_id) == 1 else None
    if len(alphabet_id) > 1:
        logger.warning(
            "goog/googl 挂在多个公司实体上，无法确定 Alphabet 实体: {}", alphabet_id
        )
    return old_google_id, alphabet_company_id


# ---------------------------------------------------------------------------
# 边构建
# ---------------------------------------------------------------------------

@dataclass
class BuildResult:
    edges: list[dict]
    stats: Counter


def build_merger_edges(session) -> tuple[list[dict], Counter]:
    """构建边 (b)：delisting_events 里已解析收购方的并购边。"""
    stats: Counter = Counter()
    candidates = load_merger_edge_candidates(session)
    stats["candidates"] = len(candidates)
    if not candidates:
        return [], stats

    all_sids: list[int] = []
    for cand in candidates:
        all_sids.append(cand.delisted_security_id)
        all_sids.append(cand.token.security_id)
    company_map = load_company_ids_for_securities(session, all_sids)

    edges: list[dict] = []
    seen: set[tuple] = set()
    for cand in candidates:
        predecessor = company_map.get(cand.delisted_security_id)
        successor = company_map.get(cand.token.security_id)
        if predecessor is None:
            stats["skip_predecessor_null"] += 1
            continue
        if successor is None:
            stats["skip_successor_null"] += 1
            continue
        if predecessor == successor:
            stats["skip_same_company"] += 1
            continue
        key = (predecessor, successor, cand.delist_date, "MERGER")
        if key in seen:
            stats["skip_duplicate"] += 1
            continue
        seen.add(key)
        edges.append(
            {
                "predecessor_company_id": predecessor,
                "successor_company_id": successor,
                "event_date": cand.delist_date,
                "event_type": "MERGER",
                "evidence": build_merger_evidence(cand.token, cand.consideration_docs),
                "source": "DELISTING",
            }
        )
    stats["merger_edges"] = len(edges)
    return edges, stats


def build_alphabet_edge(session, db_manager, *, is_apply: bool) -> tuple[list[dict], Counter]:
    """构建边 (a)：Alphabet 2015 CIK 断层。

    --apply 时若旧 Google 公司实体缺失，经 upsert_companies 按旧 CIK 建行再取回 id。
    dry-run 只报告缺失、不建实体、也不产出依赖新实体 id 的边（predecessor 未知）。"""
    stats: Counter = Counter()
    old_google_id, alphabet_company_id = resolve_alphabet_company_ids(session)

    if alphabet_company_id is None:
        logger.warning("未能定位 Alphabet 公司实体（goog/googl company_id 缺失或不唯一），跳过边 (a)。")
        stats["alphabet_skipped_no_successor"] = 1
        return [], stats

    accession = find_alphabet_reorg_accession(session)
    evidence = accession if accession else ALPHABET_REORG_MARKER
    if accession:
        stats["alphabet_accession_found"] = 1
    else:
        stats["alphabet_marker_used"] = 1

    if old_google_id is None:
        if is_apply:
            db_manager.upsert_companies([{"cik": OLD_GOOGLE_CIK, "name": OLD_GOOGLE_NAME}])
            old_google_id = db_manager.get_company_id_by_cik(OLD_GOOGLE_CIK)
            stats["alphabet_old_company_created"] = 1
            logger.info("为旧 Google CIK {} 建公司实体 id={}", OLD_GOOGLE_CIK, old_google_id)
        else:
            logger.info(
                "旧 Google 公司实体（CIK {}）不存在，--apply 时将按需创建；"
                "dry-run 不产出边 (a)。", OLD_GOOGLE_CIK,
            )
            stats["alphabet_old_company_missing"] = 1
            return [], stats

    if old_google_id == alphabet_company_id:
        logger.warning("旧 Google 与 Alphabet 解析为同一实体 id={}，跳过边 (a)。", old_google_id)
        stats["alphabet_skipped_same_company"] = 1
        return [], stats

    edge = {
        "predecessor_company_id": old_google_id,
        "successor_company_id": alphabet_company_id,
        "event_date": ALPHABET_REORG_DATE,
        "event_type": "CIK_CHANGE",
        "evidence": evidence,
        "source": "MANUAL",
    }
    stats["alphabet_edge"] = 1
    return [edge], stats


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------

def _print_plan(edges: list[dict], stats: Counter) -> None:
    logger.info("=== company_events 构建计划 ===")
    for key in sorted(stats):
        logger.info("  {}: {}", key, stats[key])
    by_type = Counter(edge["event_type"] for edge in edges)
    logger.info("待写入边合计: {}（{}）", len(edges),
                ", ".join(f"{k}={v}" for k, v in sorted(by_type.items())) or "空")
    for edge in edges[:20]:
        logger.info(
            "  {} {} -> {} @ {} [{}] {}",
            edge["event_type"], edge["predecessor_company_id"],
            edge["successor_company_id"], edge["event_date"],
            edge["source"], (edge.get("evidence") or "")[:80],
        )
    if len(edges) > 20:
        logger.info("  ...（另 {} 条略）", len(edges) - 20)


def run(args, db_manager) -> int:
    is_apply = args.apply

    with db_manager.get_session() as session:
        merger_edges, merger_stats = build_merger_edges(session)
        alphabet_edges, alphabet_stats = build_alphabet_edge(
            session, db_manager, is_apply=is_apply
        )

    edges = alphabet_edges + merger_edges
    stats = Counter()
    stats.update(merger_stats)
    stats.update(alphabet_stats)

    _print_plan(edges, stats)

    if not is_apply:
        logger.warning("以上为 dry-run 输出，未写库。确认无误后加 --apply 执行。")
        return 0

    written = db_manager.upsert_company_events(edges)
    logger.success("company_events 写入完成：upsert 影响 {} 行（计划 {} 条边）。",
                   written, len(edges))
    return 0


def setup_logging():
    configure_script_logging("build_company_events")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="公司世系/并购边表构建器：缝 Alphabet CIK 断层 + delisting 并购边，写 company_events。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只打印构建计划，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="写入 company_events（幂等 upsert）。")
    return parser


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        return run(args, db_manager)
    except Exception as exc:
        logger.opt(exception=exc).critical("build_company_events 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
