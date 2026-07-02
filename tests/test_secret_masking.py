"""utils.secret_masking 共享掩码工具与 fred_source 掩码重抛测试。"""
import traceback

import pytest
import requests

from data_sources.fred_source import fetch_fred_series
from utils.secret_masking import mask_api_key_in_url, mask_api_keys_in_text


class TestMaskApiKeyInUrl:
    def test_masks_camel_case_apikey_param(self):
        url = "https://api.massive.com/v3/reference/tickers?date=2026-01-01&apiKey=secret-key-123&limit=10"

        masked = mask_api_key_in_url(url)

        assert "secret-key-123" not in masked
        assert "date=2026-01-01" in masked
        assert "limit=10" in masked

    def test_masks_underscore_api_key_param(self):
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=DTB3&api_key=fred-abc&file_type=json"

        masked = mask_api_key_in_url(url)

        assert "fred-abc" not in masked
        assert "series_id=DTB3" in masked

    def test_param_name_match_is_case_insensitive(self):
        masked = mask_api_key_in_url("https://x.example/path?APIKEY=upper-secret")

        assert "upper-secret" not in masked

    def test_url_without_key_returned_unchanged(self):
        url = "https://x.example/path?cursor=abc&limit=5"

        assert mask_api_key_in_url(url) == url

    def test_none_and_empty_pass_through(self):
        assert mask_api_key_in_url(None) is None
        assert mask_api_key_in_url("") == ""


class TestMaskApiKeysInText:
    def test_masks_camel_case_variant(self):
        raw = "HTTPSConnectionPool: /v2/aggs?date=2026-01-01&apiKey=secret-key-123&limit=10"

        masked = mask_api_keys_in_text(raw)

        assert "apiKey=***" in masked
        assert "secret-key-123" not in masked

    def test_masks_underscore_variant(self):
        raw = "observations?series_id=DTB3&api_key=fred-secret&file_type=json"

        masked = mask_api_keys_in_text(raw)

        assert "api_key=***" in masked
        assert "fred-secret" not in masked

    def test_masks_case_insensitively(self):
        assert "SECRET" not in mask_api_keys_in_text("API_KEY=SECRET")
        assert "secret2" not in mask_api_keys_in_text("ApiKey=secret2")

    def test_accepts_exception_objects(self):
        exc = requests.exceptions.ConnectionError("refused: /obs?api_key=exc-secret&x=1")

        masked = mask_api_keys_in_text(exc)

        assert "exc-secret" not in masked
        assert "api_key=***" in masked

    def test_text_without_key_unchanged(self):
        assert mask_api_keys_in_text("plain message") == "plain message"


class _RaisingSession:
    def __init__(self, exc):
        self.exc = exc

    def get(self, url, params=None, timeout=None, headers=None):
        raise self.exc


class _FailingResponse:
    def __init__(self, message):
        self.message = message

    def raise_for_status(self):
        raise requests.HTTPError(self.message)


class _OkGetSession:
    def __init__(self, response):
        self.response = response

    def get(self, url, params=None, timeout=None, headers=None):
        return self.response


class TestFetchFredSeriesMasking:
    def test_connection_error_rethrown_masked_with_severed_chain(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fred-secret-key")
        exc = requests.exceptions.ConnectionError(
            "HTTPSConnectionPool: /fred/series/observations?series_id=DTB3&api_key=fred-secret-key&file_type=json"
        )

        with pytest.raises(RuntimeError) as excinfo:
            fetch_fred_series("DTB3", session=_RaisingSession(exc))

        assert "api_key=***" in str(excinfo.value)
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__suppress_context__ is True
        rendered = "".join(traceback.format_exception(excinfo.value))
        assert "fred-secret-key" not in rendered

    def test_http_error_rethrown_masked(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "fred-secret-key")
        response = _FailingResponse(
            "400 Client Error for url: https://api.stlouisfed.org/fred/series/observations?api_key=fred-secret-key"
        )

        with pytest.raises(RuntimeError) as excinfo:
            fetch_fred_series("DTB3", session=_OkGetSession(response))

        rendered = "".join(traceback.format_exception(excinfo.value))
        assert "fred-secret-key" not in rendered
        assert "api_key=***" in str(excinfo.value)
