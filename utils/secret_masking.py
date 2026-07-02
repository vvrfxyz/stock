"""共享的凭证掩码工具：URL / 任意文本进日志或异常消息前先脱敏 API key。

两类载体分别处理：
- URL query 参数（``apiKey`` / ``api_key``，大小写不敏感）走结构化解析；
- 任意文本（requests 的异常消息常拼接完整请求 URL）走正则替换。
"""
from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

_API_KEY_PARAM_NAMES = {"apikey", "api_key"}
# 值一律吞到空白/& 为止：宁可多掩掉异常文本里紧跟的标点，也不留半截明文。
_API_KEY_QUERY_RE = re.compile(r"(?i)(api_?key=)[^&\s]+")


def mask_api_key_in_url(raw_url: Optional[str]) -> Optional[str]:
    """把 URL query 中的 api key 参数值替换为 ***；解析失败时原样返回。"""
    if not raw_url:
        return raw_url
    try:
        parsed = urlparse(raw_url)
        query_items = parse_qsl(parsed.query, keep_blank_values=True)
        masked = []
        changed = False
        for key, value in query_items:
            if key.lower() in _API_KEY_PARAM_NAMES:
                masked.append((key, "***"))
                changed = True
            else:
                masked.append((key, value))
        if not changed:
            return raw_url
        return urlunparse(parsed._replace(query=urlencode(masked, doseq=True)))
    except Exception:
        return raw_url


def mask_api_keys_in_text(raw_text: Any) -> str:
    """把任意文本（含异常对象 str 化结果）里嵌入的 api key 值打码。"""
    return _API_KEY_QUERY_RE.sub(r"\1***", str(raw_text))
