"""OpenFIGI v3 mapping API 适配器（CUSIP -> FIGI/ticker）。

接口免费，可选 API key（HTTP 头 ``X-OPENFIGI-APIKEY``，环境变量 ``OPENFIGI_API_KEY``）：
- 匿名：10 jobs/请求，25 请求/分钟；
- 带 key：100 jobs/请求，25 请求/6 秒。

本模块只负责查询、限速与候选归并；缓存写入（openfigi_cusip_lookups，含负缓存）
由 scripts/sync_openfigi_identifiers.py 编排。

候选归并口径：一个 CUSIP 常返回多交易所多行，按 compositeFIGI 去重——
唯一 compositeFIGI 视为 MATCHED；多个不同 compositeFIGI 时先做 US composite
消歧（跨市场多上市——ADR 典型——会带出他国 composite，exchCode='US' 的行
即美国 composite；恰有一个不同的 US composite -> 以它 MATCHED），仍无法
唯一化才视为 AMBIGUOUS（figi/ticker 等字段全置 None，仅保留第一条的 name
供诊断）；空 data / warning / error 视为 NOT_FOUND。
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Optional

import requests
from loguru import logger
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3.util.retry import Retry

from utils.secret_masking import mask_api_keys_in_text

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
_API_KEY_ENV = "OPENFIGI_API_KEY"
_DEFAULT_TIMEOUT = 30

# 官方限额：匿名 10 jobs/请求、25 请求/分钟；带 key 100 jobs/请求、25 请求/6 秒。
_ANON_BATCH_SIZE = 10
_ANON_MIN_INTERVAL_SECONDS = 60.0 / 25
_KEYED_BATCH_SIZE = 100
_KEYED_MIN_INTERVAL_SECONDS = 6.0 / 25

# 429 无 Retry-After 时的保守退避（keyed 窗口 6 秒，匿名窗口 60 秒，取折中）。
_DEFAULT_429_DELAY_SECONDS = 10.0

_CUSIP_LENGTH = 9

# 候选行字段名是 OpenFIGI 的驼峰命名。
_CANDIDATE_FIELD_MAP = {
    "share_class_figi": "shareClassFIGI",
    "ticker": "ticker",
    "name": "name",
    "security_type": "securityType",
    "market_sector": "marketSector",
    "exch_code": "exchCode",
}


def _empty_result(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "composite_figi": None,
        "share_class_figi": None,
        "ticker": None,
        "name": None,
        "security_type": None,
        "market_sector": None,
        "exch_code": None,
    }


def _matched_result(composite_figi: Any, candidate: dict[str, Any]) -> dict[str, Any]:
    result = _empty_result("MATCHED")
    result["composite_figi"] = composite_figi
    for field_name, vendor_key in _CANDIDATE_FIELD_MAP.items():
        result[field_name] = candidate.get(vendor_key)
    return result


def _pick_unique_us_composite(candidates: list[Any]) -> dict[str, Any] | None:
    """多 compositeFIGI 消歧：exchCode='US' 的行是美国 composite 本体
    （UN/UW 等是其下属交易所行，带同一 compositeFIGI）。恰有一个不同的
    US composite -> 返回该行；零个或多个不同 -> None（保持 AMBIGUOUS）。
    compositeFIGI 缺失的 US 行不构成锚点。"""
    us_rows = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and (candidate.get("exchCode") or "").strip().upper() == "US"
        and candidate.get("compositeFIGI")
    ]
    if len({row["compositeFIGI"] for row in us_rows}) != 1:
        return None
    return us_rows[0]


def _pick_unique_share_class(candidates: list[Any]) -> dict[str, Any] | None:
    """二级消歧：全部候选共享唯一非空 shareClassFIGI -> 返回携带它的首行。

    退市 ADR 的美国场所行会从 OpenFIGI 消失，只剩各国 venue composite——但同一
    存托凭证的股份类 FIGI 跨场所不变。缺 shareClassFIGI 的候选行不投票（它们
    本身构不成锚点）；出现第二个不同值 -> None（保持 AMBIGUOUS）。"""
    values = {
        candidate.get("shareClassFIGI")
        for candidate in candidates
        if isinstance(candidate, dict) and candidate.get("shareClassFIGI")
    }
    if len(values) != 1:
        return None
    anchor_value = next(iter(values))
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("shareClassFIGI") == anchor_value:
            return candidate
    return None


class OpenFigiSource:
    """OpenFIGI 只读适配器。线程安全的简单时间窗节流；批大小随有无 key 自动切换。"""

    def __init__(
        self,
        api_key: str | None = None,
        session: Optional[requests.Session] = None,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
    ):
        if api_key is None:
            api_key = os.environ.get(_API_KEY_ENV)
        self._api_key = (api_key or "").strip() or None
        self.batch_size = _KEYED_BATCH_SIZE if self._api_key else _ANON_BATCH_SIZE
        self._min_request_interval = (
            _KEYED_MIN_INTERVAL_SECONDS if self._api_key else _ANON_MIN_INTERVAL_SECONDS
        )
        self._session = session or self._build_session()
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self._throttle_lock = threading.Lock()
        self._last_request_at = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",),  # mapping 查询幂等，POST 重试安全
            respect_retry_after_header=True,
            raise_on_status=False,  # 重试耗尽后把 429/5xx 响应交回应用层处理
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=2, pool_maxsize=4)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "stock-pipeline openfigi sync"})
        return session

    def _throttle(self) -> None:
        with self._throttle_lock:
            wait = self._last_request_at + self._min_request_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

    def _mask(self, value: Any) -> str:
        """异常/响应文本进日志或异常消息前脱敏。key 在请求头不在 URL，
        但 requests 异常偶见 dump 请求头，故再兜底替换明文 key。"""
        text = mask_api_keys_in_text(value)
        if self._api_key:
            text = text.replace(self._api_key, "***")
        return text

    @staticmethod
    def _retry_after_seconds(response: Any) -> float:
        headers = getattr(response, "headers", None) or {}
        raw = headers.get("Retry-After") if hasattr(headers, "get") else None
        if raw:
            try:
                return max(0.0, float(raw))
            except (TypeError, ValueError):
                pass
        return _DEFAULT_429_DELAY_SECONDS

    def _post_mapping(self, jobs: list[dict[str, str]]) -> list[Any]:
        headers = {"X-OPENFIGI-APIKEY": self._api_key} if self._api_key else None
        for attempt in range(self._max_retries + 1):
            self._throttle()
            try:
                response = self._session.post(
                    OPENFIGI_MAPPING_URL,
                    json=jobs,
                    headers=headers,
                    timeout=self._timeout,
                )
            except RequestException as exc:
                # 原始异常消息可能携带请求上下文；掩码后重抛并用 from None
                # 切断异常链，防止 traceback 渲染泄漏明文 key。
                if attempt >= self._max_retries:
                    raise RuntimeError(f"OpenFIGI 请求网络异常: {self._mask(exc)}") from None
                delay = self._retry_backoff_seconds * (2 ** attempt)
                logger.warning(
                    "OpenFIGI 请求网络异常，{:.1f} 秒后重试({}/{}): {}",
                    delay,
                    attempt + 1,
                    self._max_retries,
                    self._mask(exc),
                )
                time.sleep(delay)
                continue

            if response.status_code == 429:
                if attempt >= self._max_retries:
                    raise RuntimeError(
                        f"OpenFIGI 限流(429)重试耗尽: {self._mask(getattr(response, 'text', ''))[:200]}"
                    ) from None
                delay = self._retry_after_seconds(response)
                logger.warning(
                    "OpenFIGI 返回 429，{:.1f} 秒后重试同批({}/{})",
                    delay,
                    attempt + 1,
                    self._max_retries,
                )
                time.sleep(delay)
                continue

            try:
                response.raise_for_status()
            except HTTPError:
                raise RuntimeError(
                    f"OpenFIGI 请求失败: {response.status_code} - "
                    f"{self._mask(getattr(response, 'text', ''))[:500]}"
                ) from None

            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError(f"OpenFIGI 响应格式异常（期望数组）: {self._mask(payload)[:200]}")
            return payload

        raise RuntimeError("OpenFIGI 重试耗尽")  # 防御分支，正常不可达

    @staticmethod
    def _merge_candidates(item: Any) -> dict[str, Any]:
        """归并单个 job 的响应：{"data": [...]} 或 {"warning"/"error": ...}。"""
        if not isinstance(item, dict):
            return _empty_result("NOT_FOUND")
        candidates = item.get("data") or []
        if not candidates:
            # 空 data / warning / error 一律负缓存为 NOT_FOUND
            return _empty_result("NOT_FOUND")

        by_figi: dict[Any, dict[str, Any]] = {}
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            by_figi.setdefault(candidate.get("compositeFIGI"), candidate)
        if not by_figi:
            return _empty_result("NOT_FOUND")

        if len(by_figi) > 1:
            # 多个不同 compositeFIGI：先尝试 US composite 消歧——跨市场多上市
            # （退市旗舰 ADR 的 13F 挂链唯一通道）常见他国 composite 混入。
            us_composite = _pick_unique_us_composite(candidates)
            if us_composite is not None:
                return _matched_result(us_composite.get("compositeFIGI"), us_composite)
            # US composite 命不中的退市旗舰 ADR（LFC/PTR：美国那只 ADR 已摘牌，
            # OpenFIGI 不再返回 exchCode=US 行，只剩各国 composite）——若全部候选
            # 共享唯一 shareClassFIGI，用它做锚点：securities 行的 share_class_figi
            # 与之相等即挂链（resolve_links 的 by_share_class 回退路径消费）。
            shared_scf = _pick_unique_share_class(candidates)
            if shared_scf is not None:
                anchor = _matched_result(None, shared_scf)
                anchor["composite_figi"] = None  # 无可信 composite，仅凭 share class 挂链
                return anchor
            # 零个或多个不同 US composite 且无共享 share class：真歧义。figi/ticker
            # 等字段全置 None，仅保留第一条候选的 name 供人工诊断。
            result = _empty_result("AMBIGUOUS")
            first = candidates[0] if isinstance(candidates[0], dict) else {}
            result["name"] = first.get("name")
            return result

        composite_figi, first = next(iter(by_figi.items()))
        return _matched_result(composite_figi, first)

    def map_cusips(self, cusips: list[str]) -> dict[str, dict]:
        """批量映射 CUSIP -> FIGI。返回 {cusip: 归并结果}，key 为清洗后的大写 CUSIP。

        - 去空白转大写、去重；长度 != 9 的直接标 NOT_FOUND，不消耗配额；
        - 清洗后为空串的输入直接跳过（不是合法 CUSIP，也不适合做缓存主键）。
        """
        results: dict[str, dict] = {}
        pending: list[str] = []
        for raw in cusips:
            cleaned = (raw or "").strip().upper()
            if not cleaned or cleaned in results:
                continue
            if len(cleaned) != _CUSIP_LENGTH:
                results[cleaned] = _empty_result("NOT_FOUND")
                continue
            results[cleaned] = _empty_result("NOT_FOUND")  # 占位，防重复入队
            pending.append(cleaned)

        for start in range(0, len(pending), self.batch_size):
            chunk = pending[start : start + self.batch_size]
            jobs = [{"idType": "ID_CUSIP", "idValue": cusip} for cusip in chunk]
            items = self._post_mapping(jobs)
            # 响应数组与请求 job 按下标对齐；缺项按 NOT_FOUND 兜底
            for index, cusip in enumerate(chunk):
                item = items[index] if index < len(items) else None
                results[cusip] = self._merge_candidates(item)
        return results
