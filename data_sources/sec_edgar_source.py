"""SEC EDGAR 官方数据适配器。

数据来源（全部免费、无 key，硬性要求自报 User-Agent）：
- https://www.sec.gov/files/company_tickers.json          ticker -> CIK 全量映射
- https://data.sec.gov/submissions/CIK{cik:0>10}.json     单公司 filing 索引（最近 1000 条 + 分页文件）

速率约束：SEC 公平使用上限 10 req/s。这里用保守的进程内节流（默认 8 req/s），
不复用 Massive 的 per-key limiter（SEC 无 key 概念）。
"""
from __future__ import annotations

import hashlib
import threading
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.sec_config import get_sec_user_agent

_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/{filename}"
_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"
_DAILY_FORM_INDEX_URL = "https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/form.{ymd}.idx"
_QUARTERLY_FORM_INDEX_URL = "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/form.idx"
_ARCHIVES_ROOT = "https://www.sec.gov/Archives"

_DEFAULT_TIMEOUT = 30
_MIN_REQUEST_INTERVAL = 1.0 / 8  # 8 req/s，低于 SEC 10 req/s 上限


def normalize_cik(value: Any) -> str | None:
    """CIK 规范形式：去前导零的纯数字字符串；securities.cik 沿用 Massive 的 10 位补零格式，
    两边对账时都先过这个函数。"""
    if value is None:
        return None
    text = str(value).strip().lstrip("0")
    if not text.isdigit() and text != "":
        return None
    return text or "0"


def cik_to_10digit(value: Any) -> str | None:
    normalized = normalize_cik(value)
    if normalized is None:
        return None
    return normalized.zfill(10)


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        d = date.fromisoformat(str(value)[:10])
        # 2-digit year XML values (e.g. "15-06-19") parse as year 15
        if d.year < 1900:
            return None
        return d
    except ValueError:
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


class SecEdgarSource:
    """SEC EDGAR 只读适配器。线程安全的简单节流；无分页 token，分页靠 submissions 附加文件。"""

    def __init__(self, session: requests.Session | None = None, user_agent: str | None = None):
        self._user_agent = user_agent or get_sec_user_agent()
        self._session = session or self._build_session()
        self._throttle_lock = threading.Lock()
        self._last_request_at = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=4,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": self._user_agent,
                "Accept-Encoding": "gzip, deflate",
            }
        )
        return session

    def _get_json(self, url: str) -> dict:
        self._throttle()
        response = self._session.get(url, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _get_text(self, url: str) -> str:
        self._throttle()
        response = self._session.get(url, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.text

    def _throttle(self) -> None:
        with self._throttle_lock:
            wait = self._last_request_at + _MIN_REQUEST_INTERVAL - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

    # ------------------------------------------------------------------
    # T0: ticker -> CIK 映射
    # ------------------------------------------------------------------

    def fetch_ticker_cik_map(self) -> list[dict]:
        """返回 [{'ticker': 'nvda', 'cik': '1045810', 'title': 'NVIDIA CORP'}, ...]。
        ticker 统一小写（库内 symbol 即小写）；SEC 用 '-' 表示份额类别（BRK-B），
        库内用 '.'（brk.b），此处统一转为库内形式。"""
        payload = self._get_json(_TICKER_MAP_URL)
        rows = []
        for item in payload.values():
            ticker = (item.get("ticker") or "").strip().lower().replace("-", ".")
            cik = normalize_cik(item.get("cik_str"))
            if not ticker or cik is None:
                continue
            rows.append({"ticker": ticker, "cik": cik, "title": item.get("title")})
        return rows

    # ------------------------------------------------------------------
    # T1: 单公司 filing 索引
    # ------------------------------------------------------------------

    def fetch_filings(
        self,
        cik: str,
        *,
        forms: set[str] | None = None,
        since: date | None = None,
        include_older_pages: bool = False,
    ) -> list[dict]:
        """拉取一家公司的 filing 索引行。

        - forms: 只保留这些 form type（如 {'10-K','10-Q','8-K','4'}）；None 表示全部。
        - since: 只保留 filing_date >= since 的行；用于增量。
        - include_older_pages: 是否追加抓取 submissions 的历史分页文件（>1000 条时）。
        """
        cik10 = cik_to_10digit(cik)
        if cik10 is None:
            return []
        payload = self._get_json(_SUBMISSIONS_URL.format(filename=f"CIK{cik10}.json"))
        rows = list(self._rows_from_recent(payload, cik10, forms=forms, since=since))

        if include_older_pages:
            for page in payload.get("filings", {}).get("files", []) or []:
                page_to = _parse_date(page.get("filingTo"))
                if since and page_to and page_to < since:
                    continue
                page_payload = self._get_json(_SUBMISSIONS_URL.format(filename=page["name"]))
                rows.extend(
                    self._rows_from_block(page_payload, cik10, forms=forms, since=since)
                )
        return rows

    def _rows_from_recent(self, payload: dict, cik10: str, **kwargs) -> Iterable[dict]:
        recent = payload.get("filings", {}).get("recent", {})
        yield from self._rows_from_block(recent, cik10, issuer_name=payload.get("name"), **kwargs)

    def _rows_from_block(
        self,
        block: dict,
        cik10: str,
        *,
        forms: set[str] | None,
        since: date | None,
        issuer_name: str | None = None,
    ) -> Iterable[dict]:
        accession_numbers = block.get("accessionNumber") or []
        get = lambda key, i: (block.get(key) or [None] * len(accession_numbers))[i]  # noqa: E731
        for i, accession in enumerate(accession_numbers):
            form_type = (get("form", i) or "").strip()
            if forms and form_type not in forms:
                continue
            filing_date = _parse_date(get("filingDate", i))
            if filing_date is None:
                continue
            if since and filing_date < since:
                continue
            accession_clean = accession.replace("-", "")
            primary_doc = get("primaryDocument", i)
            cik_int = int(cik10)
            yield {
                "source": "SEC_EDGAR",
                "cik": cik10,
                "issuer_name": issuer_name,
                "form_type": form_type,
                "accession_number": accession,
                "filing_date": filing_date,
                "accepted_at": _parse_datetime(get("acceptanceDateTime", i)),
                "period_of_report": _parse_date(get("reportDate", i)),
                "filing_url": f"{_ARCHIVES_BASE}/{cik_int}/{accession_clean}/{accession}-index.htm",
                "primary_document_url": (
                    f"{_ARCHIVES_BASE}/{cik_int}/{accession_clean}/{primary_doc}" if primary_doc else None
                ),
            }

    # ------------------------------------------------------------------
    # T2: XBRL companyfacts 基本面事实
    # ------------------------------------------------------------------

    def fetch_fundamental_facts(
        self,
        cik: str,
        *,
        concepts: dict[str, set[str]],
        filed_since: date | None = None,
    ) -> list[dict]:
        """拉取一家公司的 curated XBRL 事实行。

        404 表示该 CIK 没有 XBRL 财务数据（基金/信托/SPAC 壳），返回 []。
        """
        cik10 = cik_to_10digit(cik)
        if cik10 is None:
            return []
        try:
            payload = self._get_json(_COMPANYFACTS_URL.format(cik10=cik10))
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return []
            raise
        return parse_company_facts(payload, cik10, concepts=concepts, filed_since=filed_since)

    # ------------------------------------------------------------------
    # T3: Form 3/4/5 ownership 文档
    # ------------------------------------------------------------------

    def fetch_ownership_document(self, primary_document_url: str) -> str | None:
        """抓取 Form 3/4/5 的原始 ownershipDocument XML。

        sec_filings 里的 primaryDocument 带 xsl 渲染前缀（如 xslF345X06/form4.xml），
        去掉该路径段即原始 XML。无 XML（早期纸质 filing）或 404 时返回 None。
        """
        xml_url = raw_ownership_xml_url(primary_document_url)
        if xml_url is None:
            return None
        try:
            return self._get_text(xml_url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    # ------------------------------------------------------------------
    # T4: 13F-HR（EDGAR form index 发现 + 全文提交抓取）
    # ------------------------------------------------------------------

    def fetch_daily_form_index(self, day: date) -> str | None:
        """抓取某交易日的 daily form index 文本；非工作日/未发布返回 None。"""
        quarter = (day.month - 1) // 3 + 1
        url = _DAILY_FORM_INDEX_URL.format(year=day.year, quarter=quarter, ymd=day.strftime("%Y%m%d"))
        try:
            return self._get_text(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in (403, 404):
                return None
            raise

    def fetch_quarterly_form_index(self, year: int, quarter: int) -> str:
        return self._get_text(_QUARTERLY_FORM_INDEX_URL.format(year=year, quarter=quarter))

    def fetch_full_submission(self, file_path: str) -> str:
        """按 form index 给出的相对路径（edgar/data/.../accession.txt）抓全文提交。"""
        return self._get_text(f"{_ARCHIVES_ROOT}/{file_path.lstrip('/')}")


def parse_company_facts(
    payload: dict,
    cik10: str,
    *,
    concepts: dict[str, set[str]],
    filed_since: date | None = None,
) -> list[dict]:
    """companyfacts JSON -> curated 事实行（API 与 bulk zip 共用）。

    - concepts: {taxonomy: {concept,...}} 白名单（见 utils/sec_concepts.py）。
    - filed_since: 只保留 filed >= 该日的事实；用于增量。
    - instant 型事实（无 start）将 period_start 置为 period_end，is_instant=True，
      保证唯一键 (cik,taxonomy,concept,unit,period_start,period_end,accession) 非空。
    """
    rows = []
    facts = payload.get("facts") or {}
    for taxonomy, wanted in concepts.items():
        tax_facts = facts.get(taxonomy) or {}
        for concept in wanted:
            node = tax_facts.get(concept)
            if not node:
                continue
            for unit, unit_facts in (node.get("units") or {}).items():
                for fact in unit_facts:
                    filed = _parse_date(fact.get("filed"))
                    period_end = _parse_date(fact.get("end"))
                    value = fact.get("val")
                    accession = fact.get("accn")
                    if filed is None or period_end is None or value is None or not accession:
                        continue
                    if filed_since and filed < filed_since:
                        continue
                    period_start = _parse_date(fact.get("start"))
                    rows.append(
                        {
                            "cik": cik10,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "unit": unit,
                            "period_start": period_start or period_end,
                            "period_end": period_end,
                            "is_instant": period_start is None,
                            "value": value,
                            "fiscal_year": fact.get("fy"),
                            "fiscal_period": fact.get("fp"),
                            "form_type": fact.get("form"),
                            "accession_number": accession,
                            "filed_date": filed,
                            "frame": fact.get("frame"),
                        }
                    )
    return rows


# ----------------------------------------------------------------------
# Form 3/4/5 ownershipDocument XML 解析
# ----------------------------------------------------------------------

def raw_ownership_xml_url(primary_document_url: str | None) -> str | None:
    """xslF345X06/form4.xml 渲染路径 -> 原始 XML URL；非 .xml 文档返回 None。"""
    if not primary_document_url or not primary_document_url.lower().endswith(".xml"):
        return None
    base, _, doc = primary_document_url.rpartition("/")
    if base.rsplit("/", 1)[-1].lower().startswith("xsl"):
        base = base.rsplit("/", 1)[0]
    return f"{base}/{doc}"


def _text(node: ET.Element | None, path: str | None = None) -> str | None:
    """取 path 下的文本；Form 345 值多嵌一层 <value>。"""
    if node is None:
        return None
    target = node.find(path) if path else node
    if target is None:
        return None
    value_node = target.find("value")
    raw = (value_node.text if value_node is not None else target.text) or ""
    raw = raw.strip()
    return raw or None


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.strip().lower() in ("1", "true", "yes")


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def _footnote_text(node: ET.Element | None, footnotes: dict[str, str]) -> list[str]:
    """收集子树里所有 footnoteId 引用的脚注文本。"""
    if node is None:
        return []
    ids = [fn.get("id") for fn in node.iter("footnoteId") if fn.get("id")]
    return [footnotes[i] for i in ids if i in footnotes]


def _row_hash(parts: Iterable[Any]) -> str:
    joined = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def parse_ownership_document(xml_text: str, accession_number: str) -> list[dict]:
    """ownershipDocument XML -> insider_transactions 行。

    每个 nonDerivative/derivative 的 transaction/holding × reporting owner 出一行；
    record_type 区分 TRANSACTION/HOLDING，security_type 区分 NON_DERIVATIVE/DERIVATIVE。
    多 owner filing（合并申报）按 owner 复制行，行哈希含 owner_cik 防互撞。
    """
    root = ET.fromstring(xml_text)
    form_type = (root.findtext("documentType") or "").strip() or None
    period_of_report = _parse_date(root.findtext("periodOfReport"))
    aff_10b5_one = _parse_bool(root.findtext("aff10b5One"))
    remarks = (root.findtext("remarks") or "").strip() or None

    issuer = root.find("issuer")
    issuer_cik = cik_to_10digit(_text(issuer, "issuerCik"))
    issuer_name = _text(issuer, "issuerName")
    issuer_symbol = (_text(issuer, "issuerTradingSymbol") or "").lower() or None

    footnotes = {
        fn.get("id"): (fn.text or "").strip()
        for fn in root.findall("footnotes/footnote")
        if fn.get("id")
    }

    owners = []
    for owner_node in root.findall("reportingOwner"):
        rel = owner_node.find("reportingOwnerRelationship")
        owners.append(
            {
                "owner_cik": cik_to_10digit(_text(owner_node, "reportingOwnerId/rptOwnerCik")),
                "owner_name": _text(owner_node, "reportingOwnerId/rptOwnerName"),
                "is_director": _parse_bool(_text(rel, "isDirector")),
                "is_officer": _parse_bool(_text(rel, "isOfficer")),
                "is_ten_percent_owner": _parse_bool(_text(rel, "isTenPercentOwner")),
                "is_other": _parse_bool(_text(rel, "isOther")),
                "officer_title": _text(rel, "officerTitle"),
            }
        )
    if not owners:
        owners = [{"owner_cik": None, "owner_name": None}]

    common = {
        "source": "SEC_EDGAR",
        "accession_number": accession_number,
        "form_type": form_type,
        "period_of_report": period_of_report,
        "issuer_cik": issuer_cik,
        "issuer_name": issuer_name,
        "issuer_trading_symbol": issuer_symbol,
        "aff_10b5_one": aff_10b5_one,
        "remarks": remarks,
    }

    entries = []  # (security_type, record_type, node)
    for table, security_type in (("nonDerivativeTable", "NON_DERIVATIVE"), ("derivativeTable", "DERIVATIVE")):
        table_node = root.find(table)
        if table_node is None:
            continue
        for child in table_node:
            record_type = "TRANSACTION" if child.tag.endswith("Transaction") else "HOLDING"
            entries.append((security_type, record_type, child))

    rows = []
    for entry_index, (security_type, record_type, node) in enumerate(entries):
        coding = node.find("transactionCoding")
        amounts = node.find("transactionAmounts")
        post = node.find("postTransactionAmounts")
        nature = node.find("ownershipNature")
        underlying = node.find("underlyingSecurity")

        shares = _parse_decimal(_text(amounts, "transactionShares"))
        price = _parse_decimal(_text(amounts, "transactionPricePerShare"))
        entry_fields = {
            "security_title": _text(node, "securityTitle"),
            "transaction_date": _parse_date(_text(node, "transactionDate")),
            "deemed_execution_date": _parse_date(_text(node, "deemedExecutionDate")),
            "transaction_code": _text(coding, "transactionCode"),
            "equity_swap_involved": _parse_bool(_text(coding, "equitySwapInvolved")),
            "transaction_timeliness": _text(node, "transactionTimeliness"),
            "transaction_shares": shares,
            "transaction_price_per_share": price,
            "transaction_acquired_disposed": _text(amounts, "transactionAcquiredDisposedCode"),
            "shares_owned_following_transaction": _parse_decimal(
                _text(post, "sharesOwnedFollowingTransaction")
            ),
            "transaction_value": (shares * price) if shares is not None and price is not None else None,
            "exercise_date": _parse_date(_text(node, "exerciseDate")),
            "expiration_date": _parse_date(_text(node, "expirationDate")),
            "underlying_security_title": _text(underlying, "underlyingSecurityTitle"),
            "underlying_security_shares": _parse_decimal(_text(underlying, "underlyingSecurityShares")),
            "direct_or_indirect": _text(nature, "directOrIndirectOwnership"),
            "security_type": security_type,
            "record_type": record_type,
        }
        notes = _footnote_text(node, footnotes)
        entry_fields["footnotes"] = "\n".join(notes) if notes else None

        for owner in owners:
            row = dict(common)
            row.update(owner)
            row.update(entry_fields)
            row["source_row_hash"] = _row_hash(
                [
                    accession_number,
                    owner.get("owner_cik"),
                    security_type,
                    record_type,
                    entry_index,
                ]
            )
            rows.append(row)
    return rows


# ----------------------------------------------------------------------
# 13F-HR 解析（form index 行 + 全文提交）
# ----------------------------------------------------------------------

def parse_form_index(index_text: str, forms: set[str]) -> list[dict]:
    """解析 EDGAR form.idx（daily 或 quarterly），返回指定 form 的行。

    固定列宽格式不可靠（不同年份宽度不同），按右侧字段回退解析：
    最后一段是文件路径，倒数第二段是日期，倒数第三段是 CIK，
    行首到 CIK 之间为 form type + company name（form type 不含两个连续空格）。
    日期两种格式：daily 索引用 YYYYMMDD，quarterly 索引用 YYYY-MM-DD。
    """
    rows = []
    for line in index_text.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        file_path = parts[-1]
        if not file_path.startswith("edgar/data/"):
            continue
        filing_date = _parse_index_date(parts[-2])
        cik_str = parts[-3]
        if filing_date is None or not cik_str.isdigit():
            continue
        form_type = line.split("  ", 1)[0].strip()
        if form_type not in forms:
            continue
        accession = file_path.rsplit("/", 1)[-1].removesuffix(".txt")
        rows.append(
            {
                "form_type": form_type,
                "filer_cik": cik_to_10digit(cik_str),
                "filing_date": filing_date,
                "file_path": file_path,
                "accession_number": accession,
            }
        )
    return rows


def _parse_index_date(value: str) -> date | None:
    if len(value) == 8 and value.isdigit():
        return date(int(value[:4]), int(value[4:6]), int(value[6:8]))
    if len(value) == 10:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _find_ns(node: ET.Element | None, name: str) -> ET.Element | None:
    """忽略命名空间按本地名找第一个子孙节点。"""
    if node is None:
        return None
    for child in node.iter():
        if _strip_ns(child.tag) == name:
            return child
    return None


def _text_ns(node: ET.Element | None, name: str) -> str | None:
    found = _find_ns(node, name)
    if found is None or found.text is None:
        return None
    return found.text.strip() or None


def _parse_us_date(value: str | None) -> date | None:
    """13F primary_doc 用 MM-DD-YYYY。"""
    if not value:
        return None
    try:
        month, day, year = value.strip().split("-")
        return date(int(year), int(month), int(day))
    except (ValueError, AttributeError):
        return None


def _extract_xml_documents(submission_text: str) -> list[tuple[str, str]]:
    """从全文提交 .txt 中提取 (filename, xml_text) 列表。"""
    documents = []
    pos = 0
    while True:
        doc_start = submission_text.find("<DOCUMENT>", pos)
        if doc_start < 0:
            break
        doc_end = submission_text.find("</DOCUMENT>", doc_start)
        if doc_end < 0:
            break
        block = submission_text[doc_start:doc_end]
        pos = doc_end + len("</DOCUMENT>")
        filename = ""
        fn_start = block.find("<FILENAME>")
        if fn_start >= 0:
            filename = block[fn_start + len("<FILENAME>"):].split("\n", 1)[0].strip()
        xml_start = block.find("<XML>")
        xml_end = block.find("</XML>")
        if xml_start < 0 or xml_end < 0:
            continue
        xml_text = block[xml_start + len("<XML>"):xml_end].strip()
        documents.append((filename, xml_text))
    return documents


def parse_thirteenf_submission(submission_text: str, accession_number: str) -> list[dict]:
    """13F-HR 全文提交 -> institutional_holdings 行。

    primary_doc（edgarSubmission）给 filer/period 元数据，informationTable 给逐持仓行。
    行哈希用 accession + 表内序号（同一 filing 中可能存在完全相同的持仓行，
    内容哈希会误去重）。
    """
    primary_root = None
    info_root = None
    for _filename, xml_text in _extract_xml_documents(submission_text):
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            continue
        tag = _strip_ns(root.tag)
        if tag == "edgarSubmission" and primary_root is None:
            primary_root = root
        elif tag == "informationTable" and info_root is None:
            info_root = root
    if info_root is None:
        return []

    filer_cik = None
    period = None
    filer_name = None
    form_type = None
    file_number = None
    if primary_root is not None:
        filer_cik = cik_to_10digit(_text_ns(primary_root, "cik"))
        period = _parse_us_date(_text_ns(primary_root, "periodOfReport"))
        form_type = _text_ns(primary_root, "submissionType")
        file_number = _text_ns(primary_root, "form13FFileNumber")
        manager = _find_ns(primary_root, "filingManager")
        filer_name = _text_ns(manager, "name")

    rows = []
    entry_index = 0
    for node in info_root.iter():
        if _strip_ns(node.tag) != "infoTable":
            continue
        shrs = _find_ns(node, "shrsOrPrnAmt")
        voting = _find_ns(node, "votingAuthority")
        other_managers_text = _text_ns(node, "otherManager")
        rows.append(
            {
                "source": "SEC_EDGAR",
                "accession_number": accession_number,
                "source_row_hash": _row_hash([accession_number, entry_index]),
                "filer_cik": filer_cik,
                "form_type": form_type,
                "period": period,
                "issuer_name": _text_ns(node, "nameOfIssuer"),
                "title_of_class": _text_ns(node, "titleOfClass"),
                "cusip": _text_ns(node, "cusip"),
                "market_value": _parse_decimal(_text_ns(node, "value")),
                "shares_or_principal_amount": _parse_decimal(_text_ns(shrs, "sshPrnamt")),
                "shares_or_principal_type": _text_ns(shrs, "sshPrnamtType"),
                "put_call": _text_ns(node, "putCall"),
                "investment_discretion": _text_ns(node, "investmentDiscretion"),
                "other_managers": [other_managers_text] if other_managers_text else None,
                "voting_authority_sole": _parse_decimal(_text_ns(voting, "Sole")),
                "voting_authority_shared": _parse_decimal(_text_ns(voting, "Shared")),
                "voting_authority_none": _parse_decimal(_text_ns(voting, "None")),
                "file_number": file_number,
                "filer_name": filer_name,
            }
        )
        entry_index += 1
    return rows
