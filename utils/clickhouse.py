"""ClickHouse HTTP connection settings shared by scripts and research code."""
from __future__ import annotations

import os


def clickhouse_url(url: str | None = None) -> str:
    resolved = url or os.environ.get("RESEARCH_CLICKHOUSE_URL") or os.environ.get("CLICKHOUSE_URL")
    if not resolved:
        raise RuntimeError("需要 RESEARCH_CLICKHOUSE_URL 或 CLICKHOUSE_URL")
    return resolved.rstrip("/")


def clickhouse_request_kwargs() -> dict:
    user = os.environ.get("RESEARCH_CLICKHOUSE_USER") or os.environ.get("CLICKHOUSE_USER")
    password = os.environ.get("RESEARCH_CLICKHOUSE_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD")
    if not user or not password:
        raise RuntimeError("需要 CLICKHOUSE_USER/CLICKHOUSE_PASSWORD 或对应的 RESEARCH_ 覆盖配置")
    return {"auth": (user, password)}
