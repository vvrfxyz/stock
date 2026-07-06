"""退市结局分类器——为每只退市证券构建 delisting_events 行（终价提取 + 退市原因分层归因）。

对 `is_active = false AND delist_date IS NOT NULL` 的全部证券：

1. final_price：delist_date ±5 自然日窗口内最后一根 close > 0 的日线
   （含 yfinance 双 NULL 的 OTC 尾巴——对退市尾段它们是唯一来源）。
   提取失败按证据桶记入 evidence：
   - NO_PRICE_HISTORY                     完全无日线；
   - PRICE_TRUNCATED_2025-08-01_COHORT    max(date) 恰为 2025-08-01 且 delist_date >
                                          2025-08-06（管道休眠伪影，Massive 重拉修复
                                          队列在途——本脚本幂等重跑即可升级为真终价）；
   - PRICE_TRUNCATED                      其他提前 >5 天断流；
   - NO_RELIABLE_BAR_IN_WINDOW            有日线但窗口内无 close>0 的 bar（零价/错位）。
   绝不用窗口外的陈旧 bar 充当 final_price。

2. reason 分类按证据强度分层（宁缺毋滥，归不出就是 UNKNOWN）：
   - HIGH   同 CIK 的 8-K item 2.01（并购完成）在 delist_date ±30 天内 → MERGER；
            Form 25/25-NSE（delist_date -90/+30 天）叠加 8-K 为最强证据；
            Form 25 单独出现时只有解析出 12d2-2 规则段（--fetch-form25-docs）才定性：
            (a) 证券消灭/并购 → MERGER，(b) 交易所摘牌 → EXCHANGE_DROP，
            (c) 发行人自愿 → VOLUNTARY；解析不出则 accession 记 evidence、降层。
            证据 join 一律走 CIK 列（sec_filings.security_id 锚定同 CIK 最小 id，禁用）。
   - MEDIUM security_identity_events 的 MERGE 事件（身份合并，持仓延续到 keep 侧）
            → MERGER；type='ETF' → FUND_CLOSURE（发行人清盘模式；最终 NAV 分配常在
            退市后数周，final_price 已含预期，期望 return≈0——只记 evidence 不写数值）。
   - LOW    终价形态推断（source=PRICE_INFERRED，delisting_return 恒 NULL）：
            终价 <$1 且持续阴跌 → 疑似 EXCHANGE_DROP/BANKRUPTCY；
            终价稳定贴整（半美元格点）且成交萎缩 → 疑似现金并购 ACQUISITION_CASH。
   - 其余  UNKNOWN（confidence NULL）——UNKNOWN 清单是一等输出（--unknown-csv 可导出）。

3. delisting_return 只在有实据时写：现金并购 = (对价 - final_price)/final_price（本迭代
   未做 8-K 对价抽取，恒 NULL）；BANKRUPTCY 的 -1.0 需要法院/文件级硬证据（本迭代不产出）。
   自检：现金并购类 return 分布（p10/p50/p90）应聚在 0 附近，每次运行打印。

写路径 db_manager.upsert_delisting_events 为幂等全量重建语义（冲突时全 payload 列
原位覆盖，未提供字段清 NULL），本脚本每行都给全所有列；source='MANUAL' 的存量行
视为人工裁决，永不覆盖、永不删除。

用法：
    python scripts/build_delisting_events.py                          # dry-run（默认，不写库）
    python scripts/build_delisting_events.py --limit 100              # 测试子集
    python scripts/build_delisting_events.py --fetch-form25-docs      # 附加 Form 25 原文规则段解析
    python scripts/build_delisting_events.py --apply                  # 写库（先确认 dry-run 输出）
"""
import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from statistics import fmean, median

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

FINAL_PRICE_WINDOW_DAYS = 5
# 2025-08-01 截断队列：管道休眠伪影（评估已计数 417 只），Massive 重拉修复在途
COHORT_TRUNCATION_MAX_DATE = date(2025, 8, 1)
COHORT_TRUNCATION_DELIST_AFTER = date(2025, 8, 6)

BUCKET_NO_PRICE_HISTORY = "NO_PRICE_HISTORY"
BUCKET_COHORT_2025_08 = "PRICE_TRUNCATED_2025-08-01_COHORT"
BUCKET_TRUNCATED = "PRICE_TRUNCATED"
BUCKET_NO_RELIABLE_BAR = "NO_RELIABLE_BAR_IN_WINDOW"

FORM25_TYPES = ("25", "25/A", "25-NSE", "25-NSE/A")
FORM25_WINDOW_BEFORE_DAYS = 90
FORM25_WINDOW_AFTER_DAYS = 30
EIGHTK_WINDOW_DAYS = 30

# Form 25 的 12d2-2 规则段 → reason（任务交接口径：(a) 证券消灭/并购，
# (b) 交易所摘牌不达标，(c) 发行人自愿退市）
FORM25_RULE_REASON = {"a": "MERGER", "b": "EXCHANGE_DROP", "c": "VOLUNTARY"}

# source 取"用到的最强证据层"（FORM25 > 8K > TICKER_EVENT > PRICE_INFERRED > MANUAL）

# LOW 层终价形态参数（定性推断，参数写死保证幂等可复现）
PATTERN_LOOKBACK_BARS = 60
PATTERN_TAIL_BARS = 10
PATTERN_DISTRESS_MIN_BARS = 15
PATTERN_DISTRESS_PRICE_CEILING = 1.0
PATTERN_DISTRESS_DECLINE_RATIO = 0.5   # 终价 <= 参考期中位数的一半才算"持续阴跌"
PATTERN_CASH_MIN_BARS = 20
PATTERN_CASH_MIN_PRICE = 5.0           # 低价票贴整多为噪声，不参与现金并购推断
PATTERN_CASH_REL_RANGE = 0.01          # 尾段 10 根 close 极差 / 终价 <= 1%
PATTERN_CASH_ROUND_TOLERANCE = 0.02    # 距最近半美元格点 <= 2 美分
PATTERN_CASH_VOLUME_SHRINK = 0.6       # 尾段均量 < 前段均量的 60%

EVIDENCE_ACCESSION_CAP = 3             # evidence 里每类 accession 最多记 3 个
FORM25_DOC_FAILURE_ABORT = 5           # 连续抓取失败即判定离线，跳过整个文档阶段
UPSERT_CHUNK_SIZE = 500


def setup_logging():
    configure_script_logging("build_delisting_events")


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="退市结局分类器：final_price 提取 + reason 分层归因，写 delisting_events。",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="只输出分层统计与 UNKNOWN 清单，不写库（默认）。")
    parser.add_argument("--apply", action="store_true",
                        help="写入 delisting_events（幂等全量重建）。")
    parser.add_argument("--limit", type=int, default=None,
                        help="最多处理的退市证券数（按 security_id 升序，测试用）。")
    parser.add_argument("--fetch-form25-docs", action="store_true",
                        help="抓取 Form 25 原文解析 12d2-2 规则段（需 SEC_USER_AGENT；\n"
                             "离线/失败自动跳过，不影响其余分层）。")
    parser.add_argument("--unknown-csv", type=str, default=None,
                        help="把完整 UNKNOWN 清单导出到该 CSV 路径（一等输出，供人工与后续数据源迭代）。")
    return parser


# ---------------------------------------------------------------------------
# 数据载体
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DelistedSecurity:
    id: int
    symbol: str
    type: str | None
    cik: str | None
    delist_date: date


@dataclass(frozen=True)
class Filing:
    accession_number: str
    form_type: str
    filing_date: date
    primary_document_url: str | None = None


@dataclass(frozen=True)
class MergeEvent:
    event_id: int
    keep_security_id: int | None
    keep_symbol: str | None


@dataclass
class Evidence:
    form25: list[Filing] = field(default_factory=list)
    eightk_201: list[Filing] = field(default_factory=list)
    merge_events: list[MergeEvent] = field(default_factory=list)
    form25_rule: str | None = None  # 'a' / 'b' / 'c'，仅 --fetch-form25-docs 解析成功时


# ---------------------------------------------------------------------------
# final_price 提取（纯函数，SQL 只负责取窗口 bar）
# ---------------------------------------------------------------------------

def select_final_bar(
    bars: list[tuple[date, Decimal | None]],
    delist_date: date,
) -> tuple[Decimal, date] | None:
    """窗口 [delist_date-5, delist_date+5]（自然日）内最后一根 close > 0 的 bar。

    bars 可乱序；yfinance 双 NULL 行不做任何过滤——close 有效即可用。
    """
    window_start = delist_date - timedelta(days=FINAL_PRICE_WINDOW_DAYS)
    window_end = delist_date + timedelta(days=FINAL_PRICE_WINDOW_DAYS)
    best: tuple[Decimal, date] | None = None
    for bar_date, close in bars:
        if close is None or close <= 0:
            continue
        if bar_date < window_start or bar_date > window_end:
            continue
        if best is None or bar_date > best[1]:
            best = (close, bar_date)
    return best


def classify_price_failure(
    has_price_history: bool,
    max_price_date: date | None,
    delist_date: date,
) -> str:
    """final_price 提取失败的证据桶。绝不回退用窗口外陈旧 bar。"""
    if not has_price_history or max_price_date is None:
        return BUCKET_NO_PRICE_HISTORY
    if (
        max_price_date == COHORT_TRUNCATION_MAX_DATE
        and delist_date > COHORT_TRUNCATION_DELIST_AFTER
    ):
        return BUCKET_COHORT_2025_08
    if max_price_date < delist_date - timedelta(days=FINAL_PRICE_WINDOW_DAYS):
        return BUCKET_TRUNCATED
    return BUCKET_NO_RELIABLE_BAR


# ---------------------------------------------------------------------------
# LOW 层：终价形态推断（定性，delisting_return 恒 NULL）
# ---------------------------------------------------------------------------

def infer_price_pattern(
    bars: list[tuple[date, Decimal, int | None]],
    final_price: Decimal,
) -> tuple[str, str] | None:
    """bars 为升序 (date, close, volume)，close 均 > 0，止于 final_price_date。

    返回 (reason_code, 推断依据文本) 或 None。两个形态在价格域互斥
    （阴跌要求 <$1，现金并购要求 >=$5）。
    """
    closes = [float(close) for _, close, _ in bars]
    price = float(final_price)

    # 疑似交易所摘牌/破产：终价 <$1 且相对前期中位数持续阴跌
    if len(closes) >= PATTERN_DISTRESS_MIN_BARS and price < PATTERN_DISTRESS_PRICE_CEILING:
        reference = closes[:-PATTERN_TAIL_BARS]
        ref_median = median(reference)
        if ref_median > 0 and price <= PATTERN_DISTRESS_DECLINE_RATIO * ref_median:
            basis = (
                f"final={price:.4f}<$1 sustained decline "
                f"(median of prior {len(reference)} bars {ref_median:.4f} -> {price:.4f}); "
                "suspected EXCHANGE_DROP/BANKRUPTCY (qualitative only)"
            )
            return "EXCHANGE_DROP", basis

    # 疑似现金并购：终价稳定贴半美元格点 + 成交萎缩（对价 ≈ 终价，但 LOW 层不定量）
    if len(closes) >= PATTERN_CASH_MIN_BARS and price >= PATTERN_CASH_MIN_PRICE:
        tail = closes[-PATTERN_TAIL_BARS:]
        rel_range = (max(tail) - min(tail)) / price
        near_round = abs(price * 2 - round(price * 2)) / 2 <= PATTERN_CASH_ROUND_TOLERANCE
        if rel_range <= PATTERN_CASH_REL_RANGE and near_round:
            tail_volumes = [volume for _, _, volume in bars[-PATTERN_TAIL_BARS:]]
            prior_volumes = [volume for _, _, volume in bars[:-PATTERN_TAIL_BARS]]
            if all(v is not None for v in tail_volumes + prior_volumes) and prior_volumes:
                prior_mean = fmean(prior_volumes)
                if prior_mean > 0 and fmean(tail_volumes) < PATTERN_CASH_VOLUME_SHRINK * prior_mean:
                    basis = (
                        f"final={price:.4f} stable near half-dollar grid "
                        f"(tail rel_range={rel_range:.4f}) with shrinking volume "
                        f"(tail mean {fmean(tail_volumes):.0f} < {PATTERN_CASH_VOLUME_SHRINK} x "
                        f"prior mean {prior_mean:.0f}); suspected cash acquisition, "
                        "consideration not extracted (qualitative only)"
                    )
                    return "ACQUISITION_CASH", basis
    return None


# ---------------------------------------------------------------------------
# Form 25 原文 12d2-2 规则段解析（--fetch-form25-docs）
# ---------------------------------------------------------------------------

# 引用形态两种：正文 "17 CFR 240.12d2-2(b)"；XML 标签风格 "rule12d2-2b"
_RULE_CITATION_RE = re.compile(r"12d2[-_]2\s*\(\s*([abc])\s*\)", re.IGNORECASE)
_RULE_TAG_RE = re.compile(r"rule12d2[-_]?2([abc])\b", re.IGNORECASE)


def parse_form25_rule(doc_text: str) -> str | None:
    """从 Form 25 原文抽 12d2-2 规则段字母。

    只有全文出现的规则字母唯一时才采信（HTML 模板会把 (a)(b)(c) 三段全列出来
    当选项——多字母即不可判定，宁缺毋滥返回 None）。
    """
    letters = {m.group(1).lower() for m in _RULE_CITATION_RE.finditer(doc_text)}
    letters |= {m.group(1).lower() for m in _RULE_TAG_RE.finditer(doc_text)}
    if len(letters) == 1:
        return letters.pop()
    return None


def pick_form25_doc_candidate(evidence: Evidence, delist_date: date) -> Filing | None:
    """选最接近 delist_date 且带 primary_document_url 的 Form 25 去抓原文。"""
    candidates = [f for f in evidence.form25 if f.primary_document_url]
    if not candidates:
        return None
    return min(candidates, key=lambda f: abs((f.filing_date - delist_date).days))


# ---------------------------------------------------------------------------
# reason 分类器（纯函数决策表）
# ---------------------------------------------------------------------------

def needs_price_pattern(security: DelistedSecurity, evidence: Evidence) -> bool:
    """HIGH/MEDIUM 都归不出、且可能落到 LOW 层的证券才去取形态 bar。"""
    if evidence.eightk_201:
        return False
    if evidence.form25 and evidence.form25_rule:
        return False
    if evidence.merge_events:
        return False
    if (security.type or "").upper() == "ETF":
        return False
    return True


def classify(
    security: DelistedSecurity,
    evidence: Evidence,
    final_price: Decimal | None,
    final_price_date: date | None,
    price_bucket: str | None,
    price_pattern: tuple[str, str] | None,
) -> dict:
    """决策表输出一行完整 payload（full-rebuild 语义：所有列显式给值）。

    source 取产生 reason 的证据层；8-K 与 Form 25 同时在场时 FORM25 为最强层。
    UNKNOWN 行若 evidence 里有 Form 25 accession，source 仍记 FORM25（证据在、
    定性不了——留给人工与后续数据源迭代）。
    """
    tokens: list[str] = []

    if evidence.form25:
        listed = ",".join(
            f"{f.accession_number}:{f.form_type}:{f.filing_date.isoformat()}"
            for f in evidence.form25[:EVIDENCE_ACCESSION_CAP]
        )
        suffix = f"(+{len(evidence.form25) - EVIDENCE_ACCESSION_CAP} more)" if len(evidence.form25) > EVIDENCE_ACCESSION_CAP else ""
        tokens.append(f"form25={listed}{suffix}")
    if evidence.form25_rule:
        tokens.append(f"form25_rule=12d2-2({evidence.form25_rule})")
    if evidence.eightk_201:
        listed = ",".join(
            f"{f.accession_number}:{f.filing_date.isoformat()}"
            for f in evidence.eightk_201[:EVIDENCE_ACCESSION_CAP]
        )
        suffix = f"(+{len(evidence.eightk_201) - EVIDENCE_ACCESSION_CAP} more)" if len(evidence.eightk_201) > EVIDENCE_ACCESSION_CAP else ""
        tokens.append(f"8k_item201={listed}{suffix}")

    reason_code: str | None = None
    confidence: str | None = None
    source: str | None = None

    # --- HIGH ---
    if evidence.eightk_201:
        # 同 CIK 的并购完成公告（item 2.01）在退市日 ±30 天内
        reason_code, confidence = "MERGER", "HIGH"
        source = "FORM25" if evidence.form25 else "8K"
    elif evidence.form25 and evidence.form25_rule:
        reason_code = FORM25_RULE_REASON[evidence.form25_rule]
        confidence = "HIGH"
        source = "FORM25"

    # --- MEDIUM ---
    if reason_code is None and evidence.merge_events:
        # 身份合并事件：该退市行的持仓延续到 keep 侧证券（非现金退出）
        reason_code, confidence, source = "MERGER", "MEDIUM", "TICKER_EVENT"
        tokens.append(
            "identity_merge=" + ",".join(
                f"event#{m.event_id}->keep {m.keep_symbol or '?'}#{m.keep_security_id or '?'}"
                for m in evidence.merge_events[:EVIDENCE_ACCESSION_CAP]
            )
        )
    if reason_code is None and (security.type or "").upper() == "ETF":
        reason_code, confidence, source = "FUND_CLOSURE", "MEDIUM", "TICKER_EVENT"
        tokens.append(
            "etf_liquidation_pattern=type ETF issuer liquidation; final NAV distribution "
            "usually settles weeks after delist, final_price already converges to NAV so "
            "expected delisting_return~0 is CORRECT (not written: no per-fund evidence)"
        )

    # --- LOW（source=PRICE_INFERRED，delisting_return 恒 NULL）---
    if reason_code is None and price_pattern is not None:
        reason_code, confidence, source = price_pattern[0], "LOW", "PRICE_INFERRED"
        tokens.append(f"price_pattern={price_pattern[1]}")

    # --- 兜底 ---
    if reason_code is None:
        reason_code, confidence = "UNKNOWN", None
        source = "FORM25" if evidence.form25 else None

    if final_price is None and price_bucket:
        tokens.append(f"final_price_bucket={price_bucket}")

    return {
        "security_id": security.id,
        "delist_date": security.delist_date,
        "reason_code": reason_code,
        "reason_confidence": confidence,
        "acquirer_name": None,               # 对价/收购方抽取留待 --fetch-8k-docs 迭代
        "consideration_cash": None,
        "consideration_stock_ratio": None,
        "final_price": final_price,
        "final_price_date": final_price_date,
        # 本迭代无对价抽取与破产硬证据，恒 NULL；经验假设是读取层的事
        "delisting_return": None,
        "source": source,
        "evidence": "|".join(tokens) if tokens else None,
    }


# ---------------------------------------------------------------------------
# DB 取数
# ---------------------------------------------------------------------------

def count_inactive_without_delist_date(session) -> int:
    return session.execute(text("""
        SELECT count(*) FROM securities
        WHERE NOT is_active AND delist_date IS NULL AND upper(market) = 'US'
    """)).scalar() or 0


def load_population(session, limit: int | None) -> list[DelistedSecurity]:
    sql = """
        SELECT id, symbol, type, cik, delist_date
        FROM securities
        WHERE NOT is_active AND delist_date IS NOT NULL AND upper(market) = 'US'
        ORDER BY id
    """
    params: dict = {}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = limit
    return [
        DelistedSecurity(id=row.id, symbol=row.symbol, type=row.type,
                         cik=row.cik, delist_date=row.delist_date)
        for row in session.execute(text(sql), params)
    ]


def load_window_bars(session, security_ids: list[int]) -> dict[int, list[tuple[date, Decimal | None]]]:
    """窗口内全部 bar（含 yfinance 双 NULL 行——不加任何来源过滤）。"""
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT d.security_id, d.date, d.close
        FROM daily_prices d
        JOIN securities s ON s.id = d.security_id
        WHERE d.security_id = ANY(:ids)
          AND d.date BETWEEN s.delist_date - :w AND s.delist_date + :w
    """), {"ids": security_ids, "w": FINAL_PRICE_WINDOW_DAYS}).all()
    bars: dict[int, list[tuple[date, Decimal | None]]] = {}
    for security_id, bar_date, close in rows:
        bars.setdefault(security_id, []).append((bar_date, close))
    return bars


def load_max_price_dates(session, security_ids: list[int]) -> dict[int, date | None]:
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT s.id,
               (SELECT max(d.date) FROM daily_prices d WHERE d.security_id = s.id) AS max_date
        FROM securities s
        WHERE s.id = ANY(:ids)
    """), {"ids": security_ids}).all()
    return {security_id: max_date for security_id, max_date in rows}


def load_form25_filings(session, security_ids: list[int]) -> dict[int, list[Filing]]:
    """Form 25 证据，按 CIK 列 join（sec_filings.security_id 锚定同 CIK 最小 id，禁用）。"""
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT s.id, f.accession_number, f.form_type, f.filing_date, f.primary_document_url
        FROM securities s
        JOIN sec_filings f ON ltrim(f.cik, '0') = ltrim(s.cik, '0')
        WHERE s.id = ANY(:ids)
          AND s.cik IS NOT NULL AND s.cik <> ''
          AND f.form_type = ANY(:forms)
          AND f.filing_date BETWEEN s.delist_date - :before AND s.delist_date + :after
        ORDER BY s.id, f.filing_date
    """), {
        "ids": security_ids,
        "forms": list(FORM25_TYPES),
        "before": FORM25_WINDOW_BEFORE_DAYS,
        "after": FORM25_WINDOW_AFTER_DAYS,
    }).all()
    filings: dict[int, list[Filing]] = {}
    for security_id, accession, form_type, filing_date, doc_url in rows:
        filings.setdefault(security_id, []).append(
            Filing(accession, form_type, filing_date, doc_url)
        )
    return filings


def load_eightk_201_filings(session, security_ids: list[int]) -> dict[int, list[Filing]]:
    """同 CIK 的 8-K item 2.01（items 为逗号分隔码表，精确元素匹配防 '12.01' 误中）。"""
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT s.id, f.accession_number, f.form_type, f.filing_date
        FROM securities s
        JOIN sec_filings f ON ltrim(f.cik, '0') = ltrim(s.cik, '0')
        WHERE s.id = ANY(:ids)
          AND s.cik IS NOT NULL AND s.cik <> ''
          AND f.form_type = '8-K'
          AND '2.01' = ANY(string_to_array(replace(coalesce(f.items, ''), ' ', ''), ','))
          AND f.filing_date BETWEEN s.delist_date - :w AND s.delist_date + :w
        ORDER BY s.id, f.filing_date
    """), {"ids": security_ids, "w": EIGHTK_WINDOW_DAYS}).all()
    filings: dict[int, list[Filing]] = {}
    for security_id, accession, form_type, filing_date in rows:
        filings.setdefault(security_id, []).append(Filing(accession, form_type, filing_date))
    return filings


def load_merge_events(session) -> dict[int, list[MergeEvent]]:
    """MERGE 身份事件按 husk（被合并、退市的那只）security_id 建索引。

    related_security_id 即 husk；多 husk 合并时该列为 NULL，从 details JSON
    的 merge_ids 解析（repair_identity 写入的 plan 快照）。
    """
    rows = session.execute(text("""
        SELECT id, security_id, related_security_id, new_symbol, details
        FROM security_identity_events
        WHERE event_type = 'MERGE'
    """)).all()
    events: dict[int, list[MergeEvent]] = {}
    for event_id, keep_id, related_id, new_symbol, details in rows:
        husk_ids: list[int] = []
        keep_symbol = new_symbol
        if related_id is not None:
            husk_ids = [related_id]
        if details:
            try:
                payload = json.loads(details)
                keep_symbol = payload.get("keep_symbol") or keep_symbol
                if not husk_ids:
                    husk_ids = [i for i in payload.get("merge_ids", []) if isinstance(i, int)]
            except (ValueError, TypeError):
                pass
        for husk_id in husk_ids:
            events.setdefault(husk_id, []).append(MergeEvent(event_id, keep_id, keep_symbol))
    return events


def load_pattern_bars(session, security_id: int, final_price_date: date) -> list[tuple[date, Decimal, int | None]]:
    """LOW 层形态推断用：止于 final_price_date 的最近 60 根 close>0 bar（升序返回）。"""
    rows = session.execute(text("""
        SELECT date, close, volume
        FROM daily_prices
        WHERE security_id = :sid AND date <= :fpd AND close IS NOT NULL AND close > 0
        ORDER BY date DESC
        LIMIT :n
    """), {"sid": security_id, "fpd": final_price_date, "n": PATTERN_LOOKBACK_BARS}).all()
    return [(bar_date, close, volume) for bar_date, close, volume in reversed(rows)]


# ---------------------------------------------------------------------------
# --fetch-form25-docs 阶段（可选、可离线降级）
# ---------------------------------------------------------------------------

def fetch_form25_rules(
    securities: list[DelistedSecurity],
    evidences: dict[int, Evidence],
    fetch_text=None,
) -> dict[str, int]:
    """对"仅有 Form 25、无 8-K"的证券抓原文解析规则段，结果写回 evidence.form25_rule。

    fetch_text 可注入（测试 mock）；默认走 SecEdgarSource 的节流 getter。
    离线/UA 未配置/连续失败 —— 全部优雅跳过，只降层不报错。
    """
    stats = {"candidates": 0, "fetched": 0, "parsed": 0, "failed": 0, "no_doc_url": 0}

    if fetch_text is None:
        try:
            from data_sources.sec_edgar_source import SecEdgarSource
            edgar = SecEdgarSource()
            # 节流 getter：8 req/s + retry + 自报 UA，全在 _get_text 里
            fetch_text = edgar._get_text
        except Exception as exc:
            logger.warning("SEC EDGAR source 不可用（{}），跳过 Form 25 原文阶段。", exc)
            return stats

    consecutive_failures = 0
    for security in securities:
        evidence = evidences.get(security.id)
        if evidence is None or not evidence.form25 or evidence.eightk_201:
            continue  # 无 Form 25 或已有更强 8-K 证据，无需原文
        stats["candidates"] += 1
        candidate = pick_form25_doc_candidate(evidence, security.delist_date)
        if candidate is None:
            stats["no_doc_url"] += 1
            continue
        try:
            doc_text = fetch_text(candidate.primary_document_url)
            consecutive_failures = 0
        except Exception as exc:
            stats["failed"] += 1
            consecutive_failures += 1
            logger.warning("Form 25 原文抓取失败 {} ({}): {}",
                           security.symbol, candidate.accession_number, exc)
            if consecutive_failures >= FORM25_DOC_FAILURE_ABORT:
                logger.warning("连续 {} 次抓取失败，判定离线，跳过剩余 Form 25 原文。",
                               FORM25_DOC_FAILURE_ABORT)
                break
            continue
        stats["fetched"] += 1
        rule = parse_form25_rule(doc_text or "")
        if rule:
            stats["parsed"] += 1
            evidence.form25_rule = rule
    return stats


# ---------------------------------------------------------------------------
# 写库（--apply）
# ---------------------------------------------------------------------------

def write_events(db_manager, rows: list[dict]) -> dict[str, int]:
    """幂等全量重建写入。source='MANUAL' 的存量行是人工裁决：跳过覆盖、豁免清理；
    本次计算范围内 (security_id, delist_date) 已不匹配的非 MANUAL 旧行删除
    （delist_date 修订后的残行）。"""
    stats = {"written": 0, "skipped_manual": 0, "stale_deleted": 0}
    computed_ids = [row["security_id"] for row in rows]
    with db_manager.get_session() as session:
        manual_ids = {
            row[0] for row in session.execute(text(
                "SELECT security_id FROM delisting_events WHERE source = 'MANUAL'"
            ))
        }
        existing = session.execute(text("""
            SELECT id, security_id, delist_date, source
            FROM delisting_events
            WHERE security_id = ANY(:ids)
        """), {"ids": computed_ids}).all()

    to_write = [row for row in rows if row["security_id"] not in manual_ids]
    stats["skipped_manual"] = len(rows) - len(to_write)

    computed_pairs = {(row["security_id"], row["delist_date"]) for row in to_write}
    computed_id_set = {row["security_id"] for row in to_write}
    stale_ids = [
        event_id
        for event_id, security_id, delist_date, source in existing
        if source != "MANUAL"
        and security_id in computed_id_set
        and (security_id, delist_date) not in computed_pairs
    ]
    if stale_ids:
        with db_manager.engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM delisting_events WHERE id = ANY(:ids)"),
                {"ids": stale_ids},
            )
            conn.commit()
            stats["stale_deleted"] = result.rowcount or 0

    for start in range(0, len(to_write), UPSERT_CHUNK_SIZE):
        chunk = to_write[start:start + UPSERT_CHUNK_SIZE]
        stats["written"] += db_manager.upsert_delisting_events(chunk)
    return stats


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------

def _percentiles(values: list[float]) -> tuple[float, float, float]:
    ordered = sorted(values)
    n = len(ordered)

    def pick(q: float) -> float:
        return ordered[min(n - 1, max(0, int(round(q * (n - 1)))))]

    return pick(0.10), pick(0.50), pick(0.90)


def report(
    rows: list[dict],
    securities: list[DelistedSecurity],
    skipped_null_delist: int,
    price_buckets: Counter,
    doc_stats: dict[str, int] | None,
    unknown_csv: str | None,
) -> None:
    by_id = {s.id: s for s in securities}
    total = len(rows)
    priced = [r for r in rows if r["final_price"] is not None]

    logger.info("")
    logger.info("=== final_price 提取 ===")
    logger.info("  population: {}（inactive 且无 delist_date 被跳过: {} 只，仅计数不建行）",
                total, skipped_null_delist)
    logger.info("  final_price 命中: {} ({:.1f}%)",
                len(priced), 100.0 * len(priced) / total if total else 0.0)
    for bucket, count in price_buckets.most_common():
        logger.info("    失败桶 {}: {}", bucket, count)

    logger.info("")
    logger.info("=== reason 分层 ===")
    tier_counter = Counter((r["reason_code"], r["reason_confidence"]) for r in rows)
    for (reason, confidence), count in sorted(tier_counter.items(), key=lambda kv: -kv[1]):
        logger.info("  {:18s} confidence={:6s} : {}", reason, str(confidence), count)
    source_counter = Counter(r["source"] for r in rows)
    for source, count in sorted(source_counter.items(), key=lambda kv: -kv[1]):
        logger.info("  source={:15s} : {}", str(source), count)
    non_unknown = sum(1 for r in rows if r["reason_code"] != "UNKNOWN")
    logger.info("  非 UNKNOWN 覆盖率: {}/{} ({:.1f}%)  [验收线 >=70%]",
                non_unknown, total, 100.0 * non_unknown / total if total else 0.0)

    if doc_stats is not None:
        logger.info("")
        logger.info("=== Form 25 原文阶段 ===")
        logger.info("  candidates={candidates} fetched={fetched} parsed={parsed} "
                    "failed={failed} no_doc_url={no_doc_url}", **doc_stats)

    unknown_rows = [r for r in rows if r["reason_code"] == "UNKNOWN"]
    logger.info("")
    logger.info("=== UNKNOWN 清单（一等输出，共 {} 只，示例前 30）===", len(unknown_rows))
    for row in unknown_rows[:30]:
        security = by_id[row["security_id"]]
        logger.info("  id={:>7} {:10s} type={:4s} delist={} final={} evidence={}",
                    security.id, security.symbol, str(security.type),
                    security.delist_date, row["final_price"], row["evidence"])
    if unknown_csv:
        with open(unknown_csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["security_id", "symbol", "type", "delist_date",
                             "final_price", "final_price_date", "evidence"])
            for row in unknown_rows:
                security = by_id[row["security_id"]]
                writer.writerow([security.id, security.symbol, security.type,
                                 security.delist_date, row["final_price"],
                                 row["final_price_date"], row["evidence"]])
        logger.info("  完整 UNKNOWN 清单已写入 {}", unknown_csv)

    # 自检：现金并购类 delisting_return 应聚在 0 附近（本迭代未抽对价时为空，属预期）
    logger.info("")
    logger.info("=== delisting_return 自检（现金并购类应聚在 0 附近）===")
    merger_returns = [
        float(r["delisting_return"]) for r in rows
        if r["delisting_return"] is not None
        and r["reason_code"] in ("ACQUISITION_CASH", "MERGER")
    ]
    if merger_returns:
        p10, p50, p90 = _percentiles(merger_returns)
        logger.info("  n={}  p10={:+.4f}  p50={:+.4f}  p90={:+.4f}",
                    len(merger_returns), p10, p50, p90)
        if abs(p50) > 0.05:
            logger.warning("  p50 偏离 0 超过 5%——检查对价抽取/分类是否污染。")
    else:
        logger.info("  无样本（本迭代未做 8-K 对价抽取，delisting_return 恒 NULL，属预期）。")


# ---------------------------------------------------------------------------
# 编排
# ---------------------------------------------------------------------------

def run(args, db_manager) -> int:
    is_apply = args.apply

    with db_manager.get_session() as session:
        skipped_null_delist = count_inactive_without_delist_date(session)
        securities = load_population(session, args.limit)
        if not securities:
            logger.warning("没有符合条件的退市证券（is_active=false 且 delist_date 非空）。")
            return 0
        logger.info("退市证券 population: {} 只（--limit={}）", len(securities), args.limit)

        security_ids = [s.id for s in securities]
        window_bars = load_window_bars(session, security_ids)
        max_dates = load_max_price_dates(session, security_ids)
        form25_map = load_form25_filings(session, security_ids)
        eightk_map = load_eightk_201_filings(session, security_ids)
        merge_map = load_merge_events(session)

    evidences: dict[int, Evidence] = {
        s.id: Evidence(
            form25=form25_map.get(s.id, []),
            eightk_201=eightk_map.get(s.id, []),
            merge_events=merge_map.get(s.id, []),
        )
        for s in securities
    }

    # final_price
    final_prices: dict[int, tuple[Decimal, date] | None] = {}
    price_buckets: Counter = Counter()
    bucket_by_id: dict[int, str | None] = {}
    for s in securities:
        picked = select_final_bar(window_bars.get(s.id, []), s.delist_date)
        final_prices[s.id] = picked
        if picked is None:
            bucket = classify_price_failure(
                has_price_history=max_dates.get(s.id) is not None,
                max_price_date=max_dates.get(s.id),
                delist_date=s.delist_date,
            )
            price_buckets[bucket] += 1
            bucket_by_id[s.id] = bucket
        else:
            bucket_by_id[s.id] = None

    # 可选网络阶段（放在 session 外，避免长时间占用连接）
    doc_stats = None
    if args.fetch_form25_docs:
        doc_stats = fetch_form25_rules(securities, evidences)
        logger.info("Form 25 原文阶段: {}", doc_stats)

    # LOW 层形态 bar 只对可能落层的证券取数（依赖 form25_rule，须在文档阶段之后）
    patterns: dict[int, tuple[str, str] | None] = {}
    with db_manager.get_session() as session:
        for s in securities:
            patterns[s.id] = None
            picked = final_prices[s.id]
            if picked is None or not needs_price_pattern(s, evidences[s.id]):
                continue
            bars = load_pattern_bars(session, s.id, picked[1])
            patterns[s.id] = infer_price_pattern(bars, picked[0])

    rows = []
    for s in securities:
        picked = final_prices[s.id]
        rows.append(classify(
            s,
            evidences[s.id],
            final_price=picked[0] if picked else None,
            final_price_date=picked[1] if picked else None,
            price_bucket=bucket_by_id[s.id],
            price_pattern=patterns[s.id],
        ))

    report(rows, securities, skipped_null_delist, price_buckets, doc_stats, args.unknown_csv)

    if not is_apply:
        logger.info("")
        logger.warning("以上为 dry-run 输出，未写库。确认无误后加 --apply 执行。")
        return 0

    stats = write_events(db_manager, rows)
    logger.success(
        "写入完成: upsert {written} 行, 跳过 MANUAL {skipped_manual} 只, "
        "清理 delist_date 失配残行 {stale_deleted} 行。", **stats,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    start_time = time.monotonic()
    setup_logging()
    args = create_parser().parse_args(argv)

    db_manager = None
    try:
        db_manager = DatabaseManager()
        return run(args, db_manager)
    except Exception as exc:
        logger.opt(exception=exc).critical("build_delisting_events 执行失败: {}", exc)
        return 1
    finally:
        if db_manager:
            db_manager.close()
        logger.info("耗时: {}", timedelta(seconds=time.monotonic() - start_time))


if __name__ == "__main__":
    raise SystemExit(main())
