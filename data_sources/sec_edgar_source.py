"""SEC EDGAR 官方数据适配器。

数据来源（全部免费、无 key，硬性要求自报 User-Agent）：
- https://www.sec.gov/files/company_tickers.json          ticker -> CIK 全量映射
- https://data.sec.gov/submissions/CIK{cik:0>10}.json     单公司 filing 索引（最近 1000 条 + 分页文件）

速率约束：SEC 公平使用上限 10 req/s。这里用保守的进程内节流（默认 8 req/s），
不复用 Massive 的 per-key limiter（SEC 无 key 概念）。
"""
from __future__ import annotations

import threading
import time
from datetime import date, datetime
from typing import Any, Iterable

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.sec_config import get_sec_user_agent

_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/{filename}"
_ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

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
        return date.fromisoformat(str(value)[:10])
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
        with self._throttle_lock:
            wait = self._last_request_at + _MIN_REQUEST_INTERVAL - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()
        response = self._session.get(url, timeout=_DEFAULT_TIMEOUT)
        response.raise_for_status()
        return response.json()

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
