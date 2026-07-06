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
            Form 25 单独出现时只有解析出 12d2-2 规则段（--fetch-form25-docs）才定性。
            解析两分支：25-NSE 原始 XML 读结构化 <ruleProvision>（primary_document_url
            指向 /xslF25X 渲染视图时剥掉该段取原文 XML）；form '25' HTML 模板把全部
            条款列成选项，须靠 checkbox 标记（☒/U+2612 选中、☐/U+2610 未选，含
            [X]/[x]/(X) ASCII 变体）——条款 token 回看 90 字符找最近标记，只收选中项。
            规则映射：(a)(3)/(a)(4)/裸 (a) 证券消灭换股/并购 → MERGER，
            (a)(1)/(a)(2) 全类赎回/退休 → LIQUIDATION（CS 类多为 SPAC 赎回清算，
            evidence 记 redemption_provision），(b) 交易所摘牌 → EXCHANGE_DROP，
            (c) 发行人自愿 → VOLUNTARY。类描述守卫：一司可按类各报一份 Form 25，
            notes/preferred 类的文档绝不为 CS 证券定性（只剩非 CS 类时记 evidence
            降层如旧；ETF 份额描述宽松放行，(a)(1)/(c) 把 FUND_CLOSURE 升 HIGH）。
            8-K item 2.01 的 MERGER 证据强于 Form 25 (c)（并购常伴自愿撤牌），
            两者同在保持 MERGER，evidence 双证据并记。解析不出则 accession 记
            evidence、降层。
            证据 join 一律走 CIK 列（sec_filings.security_id 锚定同 CIK 最小 id，禁用）。
   - MEDIUM security_identity_events 的 MERGE 事件（身份合并，持仓延续到 keep 侧）
            → MERGER；type='ETF' → FUND_CLOSURE（发行人清盘模式；最终 NAV 分配常在
            退市后数周，final_price 已含预期，期望 return≈0——只记 evidence 不写数值）。
   - LOW    终价形态推断（source=PRICE_INFERRED，delisting_return 恒 NULL）：
            终价 <$1 且持续阴跌 → 疑似 EXCHANGE_DROP/BANKRUPTCY；
            终价稳定贴整（半美元格点）且成交萎缩 → 疑似现金并购 ACQUISITION_CASH。
   - 其余  UNKNOWN（confidence NULL）——UNKNOWN 清单是一等输出（--unknown-csv 可导出）。

3. 对价抽取（--fetch-8k-docs，可选网络阶段）：对 HIGH 层并购族候选（同 CIK 8-K
   item 2.01 在 delist_date ±30 天内）抓取至多 3 份主文档（优先 item 2.01 的 8-K，
   其次 item 3.01，再次 delist 前 120 天内的 DEFM14A），从原文抽每股现金对价 /
   收购方 / 换股比。宁缺毋滥：
   - 现金金额收集全部候选，只有存在明确众数才采信；再过 final_price 闸门
     [0.2x, 5x]，出界记 evidence 不写数值；
   - 现金独占 → 升级 ACQUISITION_CASH；换股独占 → ACQUISITION_STOCK；混合对价
     保持 MERGER 并同时写两个对价字段（本迭代不为含股票对价的交易算 return）；
   - acquirer 只认保守触发短语且全文档唯一，拿不准置 NULL。

4. delisting_return 只在有实据时写：现金并购 = (对价 - final_price)/final_price，
   仅现金独占且 final_price 在场时计算；BANKRUPTCY 的 -1.0 需要法院/文件级硬证据
   （本迭代不产出）。自检：现金并购类 return 分布（p10/p50/p90）应聚在 0 附近，
   每次运行连同抽取漏斗（candidates/docs fetched/cash extracted/gated out）打印。

写路径 db_manager.upsert_delisting_events 为幂等全量重建语义（冲突时全 payload 列
原位覆盖，未提供字段清 NULL），本脚本每行都给全所有列；source='MANUAL' 的存量行
视为人工裁决，永不覆盖、永不删除。

用法：
    python scripts/build_delisting_events.py                          # dry-run（默认，不写库）
    python scripts/build_delisting_events.py --limit 100              # 测试子集
    python scripts/build_delisting_events.py --fetch-form25-docs      # 附加 Form 25 原文规则段解析
    python scripts/build_delisting_events.py --fetch-8k-docs          # 附加 8-K/DEFM14A 对价抽取
    python scripts/build_delisting_events.py --apply                  # 写库（先确认 dry-run 输出）
"""
import argparse
import csv
import html
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
DEFM14A_WINDOW_BEFORE_DAYS = 120

# Form 25 的 12d2-2 规则段 → reason。子款：(a)(1) 全类赎回 / (a)(2) 到期退休
# → LIQUIDATION（CS 类多为 SPAC 赎回清算）；(a)(3)/(a)(4) 全类换成另一证券 →
# MERGER；裸 (a) 无子款（文档级歧义）按并购主口径 → MERGER；(b) 交易所摘牌
# 不达标 → EXCHANGE_DROP；(c) 发行人自愿退市 → VOLUNTARY。
FORM25_RULE_REASON = {
    "a": "MERGER",
    "a1": "LIQUIDATION",
    "a2": "LIQUIDATION",
    "a3": "MERGER",
    "a4": "MERGER",
    "b": "EXCHANGE_DROP",
    "c": "VOLUNTARY",
}
FORM25_DOCS_PER_SECURITY = 3     # 一司可按类各报一份 Form 25（notes/preferred/CS）
FORM25_CHECKBOX_LOOKBACK = 90    # HTML 分支：条款 token 回看窗口（字符）找最近 checkbox

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
EIGHTK_DOC_FAILURE_ABORT = 5           # --fetch-8k-docs 同款离线保险丝
EIGHTK_DOCS_PER_SECURITY = 3           # 每只候选最多抓 3 份主文档
# 对价现金 sanity 闸门：现金并购终价应已收敛到对价附近，出这个区间的抽取值
# 大概率是误中（分红、总价、其他证券的价格），记 evidence 不写数值。
CASH_SANITY_FLOOR_RATIO = 0.2
CASH_SANITY_CEIL_RATIO = 5.0
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
    parser.add_argument("--fetch-8k-docs", action="store_true",
                        help="抓取并购族候选的 8-K/DEFM14A 主文档抽对价（现金/换股比/收购方，\n"
                             "需 SEC_USER_AGENT；离线/失败自动跳过；可与 --fetch-form25-docs 叠加）。")
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
class ConsiderationExtraction:
    """--fetch-8k-docs 阶段对一只候选证券的抽取结果（宁缺毋滥，拿不准全 NULL）。

    note 记录不采信的原因（ambiguous_* / cash_gated_out / no_primary_document_url），
    连同 accessions 一起进 evidence，保证重跑输出稳定、留人工复核线索。
    """
    cash: Decimal | None = None
    stock_ratio: Decimal | None = None
    acquirer: str | None = None
    accessions: list[str] = field(default_factory=list)  # 实际解析过的文档
    note: str | None = None


@dataclass
class Evidence:
    form25: list[Filing] = field(default_factory=list)
    eightk_201: list[Filing] = field(default_factory=list)
    merge_events: list[MergeEvent] = field(default_factory=list)
    # 'a' / 'a1'..'a4' / 'b' / 'c'，仅 --fetch-form25-docs 解析成功时
    form25_rule: str | None = None
    form25_rule_accession: str | None = None  # 采信文档的 accession（证据可追溯）
    form25_rule_note: str | None = None       # 解析歧义留痕（多选中/无选中等）
    form25_class: str | None = None           # 采信文档的证券类描述
    form25_skipped_classes: list[str] = field(default_factory=list)  # 类守卫拒绝的非 CS 类文档
    eightk_301: list[Filing] = field(default_factory=list)   # 仅 --fetch-8k-docs 加载
    defm14a: list[Filing] = field(default_factory=list)      # 仅 --fetch-8k-docs 加载
    consideration: ConsiderationExtraction | None = None     # 仅 --fetch-8k-docs 产出


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

# 引用形态：正文 "17 CFR 240.12d2-2(a)(2)"（子款只对 (a) 有意义，(b)/(c) 的
# 程序性子引用如 "(c)(2)(ii)" 一律坍缩到族字母）；legacy 标签风格 "rule12d2-2b"
_RULE_CITATION_RE = re.compile(
    r"12d2[-_]2\s*\(\s*([abc])\s*\)(?:\s*\(\s*([1-4])\s*\))?", re.IGNORECASE)
_RULE_TAG_RE = re.compile(r"rule12d2[-_]?2([abc])\b", re.IGNORECASE)

# 25-NSE 原始 XML 的结构化字段（最可靠的分支）
_XML_RULE_PROVISION_RE = re.compile(
    r"<\s*ruleProvision\s*>\s*(.*?)\s*<\s*/\s*ruleProvision\s*>", re.IGNORECASE | re.DOTALL)
_XML_CLASS_DESC_RE = re.compile(
    r"<\s*descriptionClassSecurity\s*>\s*(.*?)\s*<\s*/\s*descriptionClassSecurity\s*>",
    re.IGNORECASE | re.DOTALL)

# HTML checkbox 标记（strip_html 已把 &#9746;/&#9744; 实体 unescape 成单字符）。
# 选中：☒/☑/[X]/[x]/(X)；未选：☐/[ ]/[_]。不收 '(x)'——法律文本的罗马数字
# 列表项 "(x)" 会误中。
_CHECKED_MARK_RE = re.compile(r"[☒☑]|\[[Xx]\]|\(X\)")
_UNCHECKED_MARK_RE = re.compile(r"☐|\[\s*\]|\[_+\]")

# Form 25 HTML 表头的类描述紧邻其说明文字 "(Description of class of securities)"
_HTML_CLASS_LABEL_RE = re.compile(
    r"\(\s*Description\s+of\s+class(?:es)?\s+of\s+securities\s*\)", re.IGNORECASE)

# 类描述守卫：CS 证券绝不采信 notes/preferred/warrant 等非普通股类的 Form 25。
# 判定前先剥括号附注——"Common Stock (and associated Preferred Stock Purchase
# Rights)" 的毒丸附注不是独立类。
_FORM25_NON_CS_CLASS_RE = re.compile(
    r"\b(notes?|debentures?|preferred|preference|warrants?|bonds?|rights|units?|"
    r"depositary)\b|\d+(?:\.\d+)?\s*%",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Form25Parse:
    """单份 Form 25 文档的解析结果。provision=None 即不可判定（宁缺毋滥）。"""
    provision: str | None            # 'a' / 'a1'..'a4' / 'b' / 'c'
    class_description: str | None = None
    branch: str = "html"             # 'xml' | 'html'
    note: str | None = None          # 歧义留痕（多族选中 / 同族多款坍缩 / 无选中）


_XSL_VIEWER_SEGMENT_RE = re.compile(r"/xslF25X\d+/")


def normalize_form25_doc_url(url: str) -> str:
    """25-NSE 的 primary_document_url 常指向 XSL 渲染视图
    （.../xslF25X02/primary_doc.xml）；剥掉 viewer 段即得原始 XML。"""
    return _XSL_VIEWER_SEGMENT_RE.sub("/", url)


def _provision_key(letter: str, sub: str | None) -> str:
    """规范化条款键：只有 (a) 的子款参与语义区分，(b)/(c) 坍缩到族字母。"""
    letter = letter.lower()
    if letter == "a" and sub:
        return letter + sub
    return letter


def format_provision(key: str) -> str:
    """'a2' -> '(a)(2)'，'b' -> '(b)'（evidence 展示用）。"""
    if len(key) == 2:
        return f"({key[0]})({key[1]})"
    return f"({key})"


def _resolve_provisions(provisions: list[str]) -> tuple[str | None, str | None]:
    """条款集合 -> (rule, note)。唯一即采信；同族多款坍缩到族字母（裸 (a) 按
    MERGER 主口径）；跨族多款不可判定——宁缺毋滥。"""
    distinct = sorted(set(provisions))
    if not distinct:
        return None, None
    if len(distinct) == 1:
        return distinct[0], None
    families = {p[0] for p in distinct}
    if len(families) > 1:
        return None, "multiple_families_checked=" + ",".join(distinct)
    return families.pop(), "multiple_checked_same_family=" + ",".join(distinct)


def _checked_state(window: str) -> bool | None:
    """回看窗口内最近一个 checkbox 标记的状态；无标记返回 None。"""
    last_checked = None
    for m in _CHECKED_MARK_RE.finditer(window):
        last_checked = m.end()
    last_unchecked = None
    for m in _UNCHECKED_MARK_RE.finditer(window):
        last_unchecked = m.end()
    if last_checked is None and last_unchecked is None:
        return None
    if last_unchecked is None:
        return True
    if last_checked is None:
        return False
    return last_checked > last_unchecked


def _extract_html_class_description(text_: str) -> str | None:
    """HTML 表头：类描述紧靠 '(Description of class of securities)' 之前，
    上一个括号说明段（如 '(Commission File Number)'）之后。"""
    m = _HTML_CLASS_LABEL_RE.search(text_)
    if m is None:
        return None
    prefix = text_[:m.start()].rstrip()
    cut = prefix.rfind(")")
    desc = (prefix[cut + 1:] if cut != -1 else prefix[-100:]).strip(" ,;:")
    return desc[:255] or None


def parse_form25_document(doc_text: str) -> Form25Parse:
    """两分支解析一份 Form 25 原文。

    - XML 分支（25-NSE 原始 XML）：<ruleProvision> 是结构化字段，直接抽；
      可能出现 0/1/多次，多次跨族即不可判定。同时抓 <descriptionClassSecurity>。
    - HTML 分支（form '25' 模板）：模板把全部条款列成选项，只有 checkbox
      标记（☒ 选中 / ☐ 未选）指出适用项——每个条款 token 回看
      FORM25_CHECKBOX_LOOKBACK 字符找最近标记，只收选中项。全文无任何
      checkbox 标记的 legacy 文本退回"全文条款唯一才采信"的老口径。
    """
    # XML 分支
    if re.search(r"<\s*(ruleProvision|descriptionClassSecurity)\b", doc_text, re.IGNORECASE):
        provisions = []
        for m in _XML_RULE_PROVISION_RE.finditer(doc_text):
            cm = _RULE_CITATION_RE.search(m.group(1))
            if cm:
                provisions.append(_provision_key(cm.group(1), cm.group(2)))
        class_desc = None
        cd = _XML_CLASS_DESC_RE.search(doc_text)
        if cd:
            class_desc = html.unescape(cd.group(1)).strip()[:255] or None
        provision, note = _resolve_provisions(provisions)
        return Form25Parse(provision, class_desc, "xml", note)

    # legacy 标签风格（"rule12d2-2b"）——须在 HTML 剥标签前匹配
    tag_provisions = [m.group(1).lower() for m in _RULE_TAG_RE.finditer(doc_text)]
    if tag_provisions:
        provision, note = _resolve_provisions(tag_provisions)
        return Form25Parse(provision, None, "html", note)

    # HTML 分支
    text_ = strip_html(doc_text)
    citations = [
        (m.start(), _provision_key(m.group(1), m.group(2)))
        for m in _RULE_CITATION_RE.finditer(text_)
    ]
    class_desc = _extract_html_class_description(text_)
    if not citations:
        return Form25Parse(None, class_desc, "html", None)
    if _CHECKED_MARK_RE.search(text_) or _UNCHECKED_MARK_RE.search(text_):
        checked = []
        for pos, key in citations:
            window = text_[max(0, pos - FORM25_CHECKBOX_LOOKBACK):pos]
            if _checked_state(window) is True:
                checked.append(key)
        provision, note = _resolve_provisions(checked)
        if provision is None and note is None:
            note = "no_checked_provision"
        return Form25Parse(provision, class_desc, "html", note)
    # 无任何 checkbox 标记的 legacy 文本：全文条款唯一才采信
    provision, note = _resolve_provisions([key for _, key in citations])
    return Form25Parse(provision, class_desc, "html", note)


def parse_form25_rule(doc_text: str) -> str | None:
    """兼容入口：从 Form 25 原文抽 12d2-2 规则段键（'a'/'a1'..'c'）。"""
    return parse_form25_document(doc_text).provision


def form25_class_matches_security(class_description: str | None,
                                  security_type: str | None) -> bool:
    """类守卫：CS 证券拒绝明确的非普通股类描述；ETF 份额描述五花八门宽松放行；
    无类描述（legacy HTML 抽不出）无从否定，放行。"""
    if (security_type or "").upper() == "ETF":
        return True
    if not class_description:
        return True
    primary = re.sub(r"\([^)]*\)", " ", class_description)
    return _FORM25_NON_CS_CLASS_RE.search(primary) is None


def pick_form25_doc_candidates(evidence: Evidence, delist_date: date) -> list[Filing]:
    """按贴近 delist_date 排序的 Form 25 抓取清单（一司可按类各报一份——
    notes/preferred/CS，类守卫在解析后拒绝非 CS 类），至多
    FORM25_DOCS_PER_SECURITY 份。"""
    candidates = [f for f in evidence.form25 if f.primary_document_url]
    candidates.sort(key=lambda f: (abs((f.filing_date - delist_date).days), f.accession_number))
    return candidates[:FORM25_DOCS_PER_SECURITY]


# ---------------------------------------------------------------------------
# 8-K/DEFM14A 对价抽取（--fetch-8k-docs）：纯函数层
# ---------------------------------------------------------------------------

def strip_html(doc: str) -> str:
    """8-K/DEFM14A 是 HTML：去 script/style 与标签、解实体（含花引号）、归一空白
    （含标签替换成空格后残留的"标点前空格"）。"""
    text_ = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", doc)
    text_ = re.sub(r"(?s)<[^>]+>", " ", text_)
    text_ = html.unescape(text_)
    text_ = re.sub(r"\s+", " ", text_)
    return re.sub(r"\s+([,.;:)])", r"\1", text_).strip()


# 金额：允许千分位逗号（"1,264.00"）
_CASH_AMOUNT = r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)"

# 每股现金对价的保守触发短语（任务口径：这组正则相当可靠）。
# "without interest" 可出现在金额与 "in cash" 之间或末尾；"per share" 位置两可。
_CASH_PATTERNS = [
    # "right to receive $26.50 in cash(, without interest)" /
    # "right to receive $26.50 per share in cash"
    re.compile(
        r"right\s+to\s+receive\s+\$\s?" + _CASH_AMOUNT
        + r"(?:\s+per\s+share)?(?:\s*,?\s*without\s+interest\s*,?)?\s+in\s+cash",
        re.IGNORECASE,
    ),
    # "$26.50 per share in cash"
    re.compile(
        r"\$\s?" + _CASH_AMOUNT
        + r"\s+per\s+share(?:\s*,?\s*without\s+interest\s*,?)?\s+in\s+cash",
        re.IGNORECASE,
    ),
    # "cash in an amount equal to $8.25 per share" /
    # "an amount in cash equal to $12.00 per share"
    re.compile(
        r"cash\s+(?:in\s+an\s+amount\s+)?equal\s+to\s+\$\s?" + _CASH_AMOUNT + r"\s+per\s+share",
        re.IGNORECASE,
    ),
]
_AGGREGATE_GUARD_WINDOW = 60  # 匹配起点回看窗口


def extract_cash_amounts(doc_text: str) -> list[Decimal]:
    """全部每股现金对价候选（含重复出现——重复正是众数判定的信号）。

    - 同一处文本被多个 pattern 命中只计一次（按金额 group 起点去重）；
    - 匹配段落自身不含 "per share" 时回看 60 字符，出现 "aggregate" 即判为
      总价语境丢弃（"aggregate purchase price of $X in cash" 绝不能当每股价）。
    """
    seen_positions: set[int] = set()
    amounts: list[Decimal] = []
    for pattern in _CASH_PATTERNS:
        for match in pattern.finditer(doc_text):
            pos = match.start(1)
            if pos in seen_positions:
                continue
            if "per share" not in match.group(0).lower():
                lookback = doc_text[max(0, match.start() - _AGGREGATE_GUARD_WINDOW):match.start()]
                if re.search(r"aggregate", lookback, re.IGNORECASE):
                    continue
            seen_positions.add(pos)
            try:
                amount = Decimal(match.group(1).replace(",", ""))
            except ArithmeticError:
                continue
            if amount > 0:
                amounts.append(amount)
    return amounts


def pick_clear_mode(amounts: list[Decimal]) -> Decimal | None:
    """明确众数：出现次数严格高于第二名（或唯一候选值）才采信，平票即不可判定。"""
    if not amounts:
        return None
    ranked = Counter(amounts).most_common()
    if len(ranked) == 1 or ranked[0][1] > ranked[1][1]:
        return ranked[0][0]
    return None


def cash_within_sanity_gate(cash: Decimal, final_price: Decimal) -> bool:
    """现金并购终价应已收敛到对价附近：闸门 [0.2x, 5x] final_price（含边界）。"""
    return (
        Decimal(str(CASH_SANITY_FLOOR_RATIO)) * final_price
        <= cash
        <= Decimal(str(CASH_SANITY_CEIL_RATIO)) * final_price
    )


# 公司名 token：首字符大写/数字，中间允许 &.-'’（"Corp."、"O'Reilly"、"S&P"）
_COMPANY_TOKEN = r"[A-Z0-9][A-Za-z0-9&.\-'’]*"
_COMPANY_CAPTURE = (
    r"(" + _COMPANY_TOKEN + r"(?:\s+(?:" + _COMPANY_TOKEN + r"|of|and|&)){0,6}"
    r"(?:,\s*(?:Inc|Incorporated|Corp|Corporation|Co|Company|Ltd|Limited|LLC|"
    r"L\.L\.C|L\.P|LP|plc|PLC|N\.V|S\.A)\.?)?)"
)

# 保守触发短语（大小写敏感的名字捕获——触发词自身写两种大小写即可）
_ACQUIRER_PATTERNS = [
    re.compile(r"[Mm]erger\s+with\s+" + _COMPANY_CAPTURE),
    re.compile(r"[Aa]cquired\s+by\s+" + _COMPANY_CAPTURE),
    re.compile(r"[Ww]holly[\s-]owned\s+subsidiary\s+of\s+" + _COMPANY_CAPTURE),
    # 定义术语解析："Falcon Bidco Corp. (“Parent”)" —— 名字紧跟 Parent 定义
    re.compile(_COMPANY_CAPTURE + r"\s*\((?:the\s+)?[“\"']Parent[”\"']\)"),
]

# 名字以公司后缀 token 结束是自然右边界（防误吞下一句的句首大写词）
_CORP_SUFFIX_TOKENS = {
    "inc", "incorporated", "corp", "corporation", "llc", "l.l.c", "ltd",
    "limited", "plc", "lp", "l.p", "n.v", "s.a", "co",
}
# 定义术语/占位词不是收购方实名
_ACQUIRER_BLOCKLIST = {
    "parent", "purchaser", "buyer", "company", "the company", "sub",
    "merger sub", "merger subsidiary", "acquiror", "acquirer",
}
_SENTENCE_LEADIN_TOKENS = {"The", "A", "An", "This", "It", "In", "On", "As", "At"}


def _clean_company_name(raw: str) -> str | None:
    """收敛捕获噪声：截断到首个公司后缀 token、去掉误吞的句首词、置信校验。"""
    tokens = raw.strip().split()
    for i, token in enumerate(tokens):
        if token.lower().strip(".,") in _CORP_SUFFIX_TOKENS and i >= 1:
            tokens = tokens[:i + 1]
            break
    while tokens and tokens[-1] in _SENTENCE_LEADIN_TOKENS:
        tokens.pop()
    name = " ".join(tokens).strip(" ,;")
    if not 3 <= len(name) <= 100:
        return None
    normalized = name.lower().rstrip(".")
    if normalized in _ACQUIRER_BLOCKLIST or normalized.startswith("merger sub"):
        return None
    return name


def extract_acquirer_names(doc_text: str) -> list[str]:
    """保守收购方候选（已清洗）；拿不准的匹配被 _clean_company_name 拒绝。"""
    names: list[str] = []
    for pattern in _ACQUIRER_PATTERNS:
        for match in pattern.finditer(doc_text):
            cleaned = _clean_company_name(match.group(1))
            if cleaned:
                names.append(cleaned)
    return names


# 换股比："0.7136 shares of Acquirer Inc. common stock ... for each share"
# 要求小数形态（整数 "100 shares of" 是持仓/授权数语境，不是换股比）
_STOCK_RATIO_RE = re.compile(
    r"(\d+\.\d{1,6})\s+(?:validly\s+issued[\w\s,\-]{0,60}?)?shares?\s+of\s+"
    r".{0,100}?for\s+each\s+share",
    re.IGNORECASE,
)


def extract_stock_ratios(doc_text: str) -> list[Decimal]:
    return [Decimal(m.group(1)) for m in _STOCK_RATIO_RE.finditer(doc_text)]


def pick_merger_doc_candidates(evidence: Evidence, delist_date: date) -> list[Filing]:
    """并购族候选的抓取清单：优先 item 2.01 的 8-K，其次 item 3.01，再次 DEFM14A；
    只要带 primary_document_url 的，按贴近 delist_date 排序，同 accession 去重
    （一份 8-K 常同时带 2.01/3.01 两个 item），至多 EIGHTK_DOCS_PER_SECURITY 份。"""
    def by_proximity(filings: list[Filing]) -> list[Filing]:
        return sorted(
            (f for f in filings if f.primary_document_url),
            key=lambda f: (abs((f.filing_date - delist_date).days), f.accession_number),
        )

    ordered = (
        by_proximity(evidence.eightk_201)
        + by_proximity(evidence.eightk_301)
        + by_proximity(evidence.defm14a)
    )
    seen: set[str] = set()
    picked: list[Filing] = []
    for filing in ordered:
        if filing.accession_number in seen:
            continue
        seen.add(filing.accession_number)
        picked.append(filing)
        if len(picked) >= EIGHTK_DOCS_PER_SECURITY:
            break
    return picked


def extract_consideration(
    fetched_docs: list[tuple[Filing, str]],
    final_price: Decimal | None,
    stats: dict[str, int] | None = None,
) -> ConsiderationExtraction:
    """跨文档汇总抽取（文本须已过 strip_html）。宁缺毋滥：

    - 现金：全部候选金额求明确众数，再过 final_price 闸门 [0.2x, 5x]（final_price
      缺席时闸门不适用——对价可写但 return 反正算不出）；
    - 换股比：全文档唯一值才采信；
    - 收购方：清洗归一后唯一名字才采信。
    不采信的分支都在 note 里留痕（进 evidence，重跑输出稳定）。
    """
    stats = stats if stats is not None else Counter()
    amounts: list[Decimal] = []
    ratios: list[Decimal] = []
    acquirers: dict[str, str] = {}  # 归一键 -> 首见原文
    accessions: list[str] = []
    for filing, doc_text in fetched_docs:
        accessions.append(filing.accession_number)
        amounts.extend(extract_cash_amounts(doc_text))
        ratios.extend(extract_stock_ratios(doc_text))
        for name in extract_acquirer_names(doc_text):
            acquirers.setdefault(name.lower().rstrip("."), name)

    notes: list[str] = []
    cash = pick_clear_mode(amounts)
    if amounts and cash is None:
        notes.append("ambiguous_cash_candidates=" + ",".join(sorted({str(a) for a in amounts})))
        stats["cash_ambiguous"] += 1
    if cash is not None and final_price is not None and final_price > 0:
        if not cash_within_sanity_gate(cash, final_price):
            notes.append(
                f"cash_gated_out={cash} vs final_price={final_price} "
                f"(outside [{CASH_SANITY_FLOOR_RATIO}x, {CASH_SANITY_CEIL_RATIO}x])"
            )
            cash = None
            stats["cash_gated_out"] += 1
    if cash is not None:
        stats["cash_extracted"] += 1

    ratio: Decimal | None = None
    distinct_ratios = sorted(set(ratios))
    if len(distinct_ratios) == 1:
        ratio = distinct_ratios[0]
        stats["stock_extracted"] += 1
    elif len(distinct_ratios) > 1:
        notes.append("ambiguous_stock_ratios=" + ",".join(str(r) for r in distinct_ratios))

    acquirer: str | None = None
    if len(acquirers) == 1:
        acquirer = next(iter(acquirers.values()))[:255]
        stats["acquirer_extracted"] += 1
    elif len(acquirers) > 1:
        notes.append("ambiguous_acquirers=" + ";".join(sorted(acquirers.values())))

    return ConsiderationExtraction(
        cash=cash,
        stock_ratio=ratio,
        acquirer=acquirer,
        accessions=accessions,
        note="; ".join(notes) or None,
    )


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
    8-K item 2.01 的 MERGER 定性压过 Form 25 规则段（含 (c)——并购常伴自愿撤牌
    流程件），两证据同记 evidence。
    UNKNOWN 行若 evidence 里有 Form 25 accession，source 仍记 FORM25（证据在、
    定性不了——留给人工与后续数据源迭代）。

    对价抽取（evidence.consideration，仅 --fetch-8k-docs 产出，且只发生在
    HIGH 层 8-K 并购族候选上）：现金独占 → MERGER 升级 ACQUISITION_CASH，
    换股独占 → ACQUISITION_STOCK，混合保持 MERGER 同时写两个对价字段；
    delisting_return 只为"现金独占 + final_price 在场"计算，含股票对价的
    交易本迭代不算 return。
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
        tokens.append(f"form25_rule=12d2-2{format_provision(evidence.form25_rule)}")
        if evidence.form25_rule_accession:
            tokens.append(f"form25_rule_accession={evidence.form25_rule_accession}")
        if evidence.form25_rule in ("a1", "a2"):
            # (a)(1)/(a)(2) 全类赎回/退休——CS 类多为 SPAC 赎回清算
            tokens.append("redemption_provision")
        elif evidence.form25_rule == "a":
            tokens.append("form25_bare_a=no sub-provision in doc, MERGER by dominant usage")
    if evidence.form25_rule_note:
        tokens.append(f"form25_note={evidence.form25_rule_note}")
    if evidence.form25_class:
        tokens.append(f"form25_class={evidence.form25_class}")
    if evidence.form25_skipped_classes:
        listed = ";".join(evidence.form25_skipped_classes[:EVIDENCE_ACCESSION_CAP])
        tokens.append(f"form25_wrong_class_skipped={listed}")
    if evidence.eightk_201:
        listed = ",".join(
            f"{f.accession_number}:{f.filing_date.isoformat()}"
            for f in evidence.eightk_201[:EVIDENCE_ACCESSION_CAP]
        )
        suffix = f"(+{len(evidence.eightk_201) - EVIDENCE_ACCESSION_CAP} more)" if len(evidence.eightk_201) > EVIDENCE_ACCESSION_CAP else ""
        tokens.append(f"8k_item201={listed}{suffix}")

    consideration = evidence.consideration
    if consideration is not None:
        if consideration.accessions:
            tokens.append("consideration_docs=" + ",".join(consideration.accessions))
        if consideration.cash is not None:
            tokens.append(f"consideration_cash={consideration.cash}")
        if consideration.stock_ratio is not None:
            tokens.append(f"consideration_stock_ratio={consideration.stock_ratio}")
        if consideration.acquirer:
            tokens.append(f"acquirer={consideration.acquirer}")
        if consideration.note:
            tokens.append(f"consideration_note={consideration.note}")

    reason_code: str | None = None
    confidence: str | None = None
    source: str | None = None

    # --- HIGH ---
    if evidence.eightk_201:
        # 同 CIK 的并购完成公告（item 2.01）在退市日 ±30 天内
        reason_code, confidence = "MERGER", "HIGH"
        source = "FORM25" if evidence.form25 else "8K"
        if consideration is not None:
            if consideration.cash is not None and consideration.stock_ratio is None:
                reason_code = "ACQUISITION_CASH"   # 现金独占抽取成功 → 升级
            elif consideration.stock_ratio is not None and consideration.cash is None:
                reason_code = "ACQUISITION_STOCK"
            # 混合对价（cash + stock 同时在场）：保持 MERGER，两字段都写
    elif evidence.form25 and evidence.form25_rule:
        reason_code = FORM25_RULE_REASON[evidence.form25_rule]
        confidence = "HIGH"
        source = "FORM25"
        if (security.type or "").upper() == "ETF" and evidence.form25_rule in ("a1", "c"):
            # ETF 的全类赎回/自愿撤牌就是基金清盘——FUND_CLOSURE 升 HIGH
            reason_code = "FUND_CLOSURE"
            tokens.append("etf_form25_upgrade=fund closure confirmed by Form 25 rule provision")

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

    # delisting_return 只在有实据时写：现金独占对价 + final_price 在场。
    # 含股票对价（独占或混合）本迭代不算 return；经验假设仍是读取层的事。
    delisting_return: Decimal | None = None
    if (
        consideration is not None
        and consideration.cash is not None
        and consideration.stock_ratio is None
        and final_price is not None
        and final_price > 0
    ):
        delisting_return = (
            (consideration.cash - final_price) / final_price
        ).quantize(Decimal("1E-8"))

    return {
        "security_id": security.id,
        "delist_date": security.delist_date,
        "reason_code": reason_code,
        "reason_confidence": confidence,
        "acquirer_name": consideration.acquirer if consideration else None,
        "consideration_cash": consideration.cash if consideration else None,
        "consideration_stock_ratio": consideration.stock_ratio if consideration else None,
        "final_price": final_price,
        "final_price_date": final_price_date,
        "delisting_return": delisting_return,
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
    return _load_eightk_item_filings(session, security_ids, "2.01")


def load_eightk_301_filings(session, security_ids: list[int]) -> dict[int, list[Filing]]:
    """8-K item 3.01（退市/摘牌通知），--fetch-8k-docs 的次优文档来源。"""
    return _load_eightk_item_filings(session, security_ids, "3.01")


def _load_eightk_item_filings(session, security_ids: list[int], item: str) -> dict[int, list[Filing]]:
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT s.id, f.accession_number, f.form_type, f.filing_date, f.primary_document_url
        FROM securities s
        JOIN sec_filings f ON ltrim(f.cik, '0') = ltrim(s.cik, '0')
        WHERE s.id = ANY(:ids)
          AND s.cik IS NOT NULL AND s.cik <> ''
          AND f.form_type = '8-K'
          AND :item = ANY(string_to_array(replace(coalesce(f.items, ''), ' ', ''), ','))
          AND f.filing_date BETWEEN s.delist_date - :w AND s.delist_date + :w
        ORDER BY s.id, f.filing_date
    """), {"ids": security_ids, "w": EIGHTK_WINDOW_DAYS, "item": item}).all()
    filings: dict[int, list[Filing]] = {}
    for security_id, accession, form_type, filing_date, doc_url in rows:
        filings.setdefault(security_id, []).append(
            Filing(accession, form_type, filing_date, doc_url)
        )
    return filings


def load_defm14a_filings(session, security_ids: list[int]) -> dict[int, list[Filing]]:
    """并购委托书 DEFM14A（delist 前 120 天窗口），对价抽取的兜底文档来源。"""
    if not security_ids:
        return {}
    rows = session.execute(text("""
        SELECT s.id, f.accession_number, f.form_type, f.filing_date, f.primary_document_url
        FROM securities s
        JOIN sec_filings f ON ltrim(f.cik, '0') = ltrim(s.cik, '0')
        WHERE s.id = ANY(:ids)
          AND s.cik IS NOT NULL AND s.cik <> ''
          AND f.form_type = 'DEFM14A'
          AND f.filing_date BETWEEN s.delist_date - :before AND s.delist_date
        ORDER BY s.id, f.filing_date
    """), {"ids": security_ids, "before": DEFM14A_WINDOW_BEFORE_DAYS}).all()
    filings: dict[int, list[Filing]] = {}
    for security_id, accession, form_type, filing_date, doc_url in rows:
        filings.setdefault(security_id, []).append(
            Filing(accession, form_type, filing_date, doc_url)
        )
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
# --fetch-form25-docs / --fetch-8k-docs 阶段（可选、可离线降级）
# ---------------------------------------------------------------------------

def _edgar_fetch_text():
    """SecEdgarSource 的节流 getter（8 req/s + retry + 自报 UA，全在 _get_text 里）；
    离线/UA 未配置时返回 None，调用方优雅跳过整个文档阶段。"""
    try:
        from data_sources.sec_edgar_source import SecEdgarSource
        return SecEdgarSource()._get_text
    except Exception as exc:
        logger.warning("SEC EDGAR source 不可用（{}），跳过文档抓取阶段。", exc)
        return None


def fetch_form25_rules(
    securities: list[DelistedSecurity],
    evidences: dict[int, Evidence],
    fetch_text=None,
) -> dict[str, int]:
    """对"仅有 Form 25、无 8-K"的证券抓原文解析规则段，结果写回 evidence.form25_rule。

    每只至多尝试 FORM25_DOCS_PER_SECURITY 份文档（一司可按类各报一份 Form 25）：
    25-NSE 的 xsl 视图 URL 先剥成原始 XML 再抓；解析出条款后过类描述守卫——
    notes/preferred 类的文档绝不为 CS 证券定性（计 wrong_class、记 evidence、
    继续试下一份）；8-K 在场的证券整体跳过（更强证据已定性，分类器层面
    8-K MERGER 也压过 Form 25 (c)）。

    fetch_text 可注入（测试 mock）；默认走 SecEdgarSource 的节流 getter。
    离线/UA 未配置/连续失败 —— 全部优雅跳过，只降层不报错。
    """
    stats = {
        "candidates": 0, "fetched": 0, "parsed": 0, "parsed_xml": 0,
        "parsed_html": 0, "wrong_class": 0, "indeterminate": 0,
        "failed": 0, "no_doc_url": 0,
    }

    if fetch_text is None:
        fetch_text = _edgar_fetch_text()
        if fetch_text is None:
            return stats

    consecutive_failures = 0
    offline = False
    for security in securities:
        if offline:
            break
        evidence = evidences.get(security.id)
        if evidence is None or not evidence.form25 or evidence.eightk_201:
            continue  # 无 Form 25 或已有更强 8-K 证据，无需原文
        stats["candidates"] += 1
        candidates = pick_form25_doc_candidates(evidence, security.delist_date)
        if not candidates:
            stats["no_doc_url"] += 1
            continue
        last_note: str | None = None
        for candidate in candidates:
            try:
                doc_text = fetch_text(normalize_form25_doc_url(candidate.primary_document_url))
                consecutive_failures = 0
            except Exception as exc:
                stats["failed"] += 1
                consecutive_failures += 1
                logger.warning("Form 25 原文抓取失败 {} ({}): {}",
                               security.symbol, candidate.accession_number, exc)
                if consecutive_failures >= FORM25_DOC_FAILURE_ABORT:
                    logger.warning("连续 {} 次抓取失败，判定离线，跳过剩余 Form 25 原文。",
                                   FORM25_DOC_FAILURE_ABORT)
                    offline = True
                    break
                continue
            stats["fetched"] += 1
            parsed = parse_form25_document(doc_text or "")
            if parsed.provision is None:
                stats["indeterminate"] += 1
                if parsed.note:
                    last_note = f"{candidate.accession_number}:{parsed.note}"
                continue
            if not form25_class_matches_security(parsed.class_description, security.type):
                stats["wrong_class"] += 1
                evidence.form25_skipped_classes.append(
                    f"{candidate.accession_number}:{parsed.class_description}")
                continue
            stats["parsed"] += 1
            stats["parsed_xml" if parsed.branch == "xml" else "parsed_html"] += 1
            evidence.form25_rule = parsed.provision
            evidence.form25_rule_accession = candidate.accession_number
            evidence.form25_rule_note = parsed.note
            evidence.form25_class = parsed.class_description
            break
        if evidence.form25_rule is None and last_note:
            evidence.form25_rule_note = last_note  # 不可判定的留痕（进 evidence 供人工）
    return stats


def fetch_merger_considerations(
    securities: list[DelistedSecurity],
    evidences: dict[int, Evidence],
    final_prices: dict[int, Decimal | None],
    fetch_text=None,
) -> dict[str, int]:
    """--fetch-8k-docs 阶段：对 HIGH 层并购族候选（evidence.eightk_201 非空——
    分类器会把它们归 MERGER HIGH，含 Form25 12d2-2(a) 叠加 8-K 的情形）抓至多
    3 份主文档抽对价，结果写回 evidence.consideration。

    fetch_text 可注入（测试 mock）；默认走 SecEdgarSource 的节流 getter；
    离线/连续失败优雅中止，已抓到的文档照常解析（部分结果仍然可信）。
    """
    stats: Counter = Counter({
        "candidates": 0, "docs_fetched": 0, "docs_failed": 0, "no_doc_url": 0,
        "cash_extracted": 0, "cash_ambiguous": 0, "cash_gated_out": 0,
        "stock_extracted": 0, "acquirer_extracted": 0,
    })

    if fetch_text is None:
        fetch_text = _edgar_fetch_text()
        if fetch_text is None:
            return dict(stats)

    consecutive_failures = 0
    offline = False
    for security in securities:
        evidence = evidences.get(security.id)
        if evidence is None or not evidence.eightk_201:
            continue  # 候选 = HIGH 层 8-K item 2.01 并购族
        stats["candidates"] += 1
        if offline:
            continue  # 仍计数 candidates，便于漏斗对账
        docs = pick_merger_doc_candidates(evidence, security.delist_date)
        if not docs:
            stats["no_doc_url"] += 1
            evidence.consideration = ConsiderationExtraction(note="no_primary_document_url")
            continue
        fetched: list[tuple[Filing, str]] = []
        for filing in docs:
            try:
                raw = fetch_text(filing.primary_document_url)
                consecutive_failures = 0
            except Exception as exc:
                stats["docs_failed"] += 1
                consecutive_failures += 1
                logger.warning("8-K/DEFM14A 原文抓取失败 {} ({}): {}",
                               security.symbol, filing.accession_number, exc)
                if consecutive_failures >= EIGHTK_DOC_FAILURE_ABORT:
                    logger.warning("连续 {} 次抓取失败，判定离线，跳过剩余对价抽取。",
                                   EIGHTK_DOC_FAILURE_ABORT)
                    offline = True
                    break
                continue
            stats["docs_fetched"] += 1
            fetched.append((filing, strip_html(raw or "")))
        if fetched:
            evidence.consideration = extract_consideration(
                fetched, final_prices.get(security.id), stats,
            )
    return dict(stats)


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
    consideration_stats: dict[str, int] | None = None,
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
                    "(xml={parsed_xml} html={parsed_html}) wrong_class={wrong_class} "
                    "indeterminate={indeterminate} failed={failed} no_doc_url={no_doc_url}",
                    **doc_stats)

    if consideration_stats is not None:
        logger.info("")
        logger.info("=== 8-K/DEFM14A 对价抽取漏斗（--fetch-8k-docs）===")
        logger.info("  candidates={candidates} docs_fetched={docs_fetched} "
                    "docs_failed={docs_failed} no_doc_url={no_doc_url}",
                    **consideration_stats)
        logger.info("  cash_extracted={cash_extracted} cash_ambiguous={cash_ambiguous} "
                    "cash_gated_out={cash_gated_out} stock_extracted={stock_extracted} "
                    "acquirer_extracted={acquirer_extracted}", **consideration_stats)

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

    # 自检：现金并购类 delisting_return 应聚在 0 附近（终价已收敛到对价）
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
        logger.info("  无样本（未启用 --fetch-8k-docs，或现金对价均未通过抽取/闸门）。")


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
        # --fetch-8k-docs 的次优/兜底文档来源只对候选（有 8-K 2.01 者）加载
        eightk_301_map: dict[int, list[Filing]] = {}
        defm14a_map: dict[int, list[Filing]] = {}
        if args.fetch_8k_docs:
            candidate_ids = sorted(eightk_map.keys())
            eightk_301_map = load_eightk_301_filings(session, candidate_ids)
            defm14a_map = load_defm14a_filings(session, candidate_ids)

    evidences: dict[int, Evidence] = {
        s.id: Evidence(
            form25=form25_map.get(s.id, []),
            eightk_201=eightk_map.get(s.id, []),
            merge_events=merge_map.get(s.id, []),
            eightk_301=eightk_301_map.get(s.id, []),
            defm14a=defm14a_map.get(s.id, []),
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

    consideration_stats = None
    if args.fetch_8k_docs:
        consideration_stats = fetch_merger_considerations(
            securities, evidences,
            final_prices={
                sid: picked[0] if picked else None
                for sid, picked in final_prices.items()
            },
        )
        logger.info("8-K/DEFM14A 对价抽取阶段: {}", consideration_stats)

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

    report(rows, securities, skipped_null_delist, price_buckets, doc_stats,
           args.unknown_csv, consideration_stats=consideration_stats)

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
