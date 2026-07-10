from __future__ import annotations

import os
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest
import requests

from research import minute_bars
from utils.clickhouse import clickhouse_request_kwargs, clickhouse_url


def test_clickhouse_settings_require_url(monkeypatch):
    for name in ("RESEARCH_CLICKHOUSE_URL", "CLICKHOUSE_URL"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError):
        clickhouse_url()


def test_clickhouse_auth_uses_research_override(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_USER", "default")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "base")
    monkeypatch.setenv("RESEARCH_CLICKHOUSE_USER", "reader")
    monkeypatch.setenv("RESEARCH_CLICKHOUSE_PASSWORD", "secret")
    assert clickhouse_request_kwargs() == {"auth": ("reader", "secret")}


@pytest.mark.parametrize("missing", ["CLICKHOUSE_USER", "CLICKHOUSE_PASSWORD"])
def test_clickhouse_auth_requires_complete_credentials(monkeypatch, missing):
    for name in (
        "RESEARCH_CLICKHOUSE_USER",
        "RESEARCH_CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    configured = "CLICKHOUSE_PASSWORD" if missing == "CLICKHOUSE_USER" else "CLICKHOUSE_USER"
    monkeypatch.setenv(configured, "configured")

    with pytest.raises(RuntimeError, match="CLICKHOUSE_USER/CLICKHOUSE_PASSWORD"):
        clickhouse_request_kwargs()


def test_query_df_threads_auth(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_URL", "http://127.0.0.1:8123")
    monkeypatch.setenv("CLICKHOUSE_USER", "reader")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "secret")
    seen = {}

    def fake_post(url, **kwargs):
        seen.update({"url": url, **kwargs})
        return SimpleNamespace(status_code=200, content=b"n\n1\n", text="n\n1\n")

    monkeypatch.setattr(minute_bars.requests, "post", fake_post)
    result = minute_bars.query_df("SELECT 1 AS n")

    assert seen["auth"] == ("reader", "secret")
    assert result.equals(pd.DataFrame({"n": [1]}))


def test_load_minute_bars_rejects_implausible_result(monkeypatch):
    from datetime import date

    oversized = pd.DataFrame(
        {"ts": pd.date_range("2026-07-08", periods=minute_bars.MAX_BARS_PER_SECURITY_DAY + 1,
                             freq="s", tz="UTC")}
    )
    monkeypatch.setattr(minute_bars, "query_df", lambda *a, **kw: oversized)
    with pytest.raises(RuntimeError, match="结果体量异常"):
        minute_bars.load_minute_bars([1], date(2026, 7, 8), date(2026, 7, 8))


def test_load_minute_bars_empty_security_list_does_not_query(monkeypatch):
    from datetime import date

    monkeypatch.setattr(
        minute_bars,
        "query_df",
        lambda *args, **kwargs: pytest.fail("empty selection must not query ClickHouse"),
    )
    result = minute_bars.load_minute_bars([], date(2026, 7, 8), date(2026, 7, 8))
    assert result.empty


@pytest.mark.clickhouse_integration
def test_clickhouse_http_round_trip(monkeypatch):
    url = os.environ.get("TEST_CLICKHOUSE_URL")
    if not url:
        pytest.skip("TEST_CLICKHOUSE_URL 未设置")
    monkeypatch.setenv("CLICKHOUSE_URL", url)
    table = f"stock_test_{uuid4().hex}"
    auth = clickhouse_request_kwargs()
    try:
        response = requests.post(
            url,
            data=f"CREATE TABLE {table} (id UInt64, value String) ENGINE=Memory".encode(),
            timeout=30,
            **auth,
        )
        assert response.status_code == 200, response.text
        response = requests.post(
            url,
            params={"query": f"INSERT INTO {table} FORMAT TabSeparated"},
            data=b"1\tlocked\n",
            timeout=30,
            **auth,
        )
        assert response.status_code == 200, response.text
        result = minute_bars.query_df(f"SELECT id, value FROM {table}")
        assert result.to_dict("records") == [{"id": 1, "value": "locked"}]
    finally:
        requests.post(url, data=f"DROP TABLE IF EXISTS {table}".encode(), timeout=30, **auth)
