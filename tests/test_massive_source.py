import traceback
import unittest
from datetime import date

from loguru import logger
from requests import HTTPError
from requests.exceptions import ConnectionError as RequestsConnectionError, RequestException

from data_sources.massive_source import MassiveSource, _mask_api_keys_in_text


class DummyRateLimiter:
    def acquire_key(self):
        return "test-key"


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(response=self)

    def json(self):
        return self.payload

    def close(self):
        return None


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class MassiveSourceTests(unittest.TestCase):
    def test_mask_api_keys_in_text_redacts_embedded_urls(self):
        raw = (
            "HTTPSConnectionPool: /v3/reference/tickers/AAPL?"
            "date=2026-01-01&apiKey=secret-key-123&limit=10"
        )

        masked = _mask_api_keys_in_text(raw)

        self.assertIn("apiKey=***", masked)
        self.assertNotIn("secret-key-123", masked)

    def test_pagination_uses_next_url_and_appends_api_key(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [{"ticker": "AAPL", "locale": "us", "type": "CS"}],
                        "next_url": "https://api.massive.com/v3/reference/tickers?cursor=abc",
                    }
                ),
                FakeResponse({"status": "OK", "results": [{"ticker": "MSFT", "locale": "us", "type": "CS"}]}),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        rows = source.list_active_tickers()

        self.assertEqual([item["ticker"] for item in rows], ["AAPL", "MSFT"])
        self.assertEqual(session.calls[0]["params"]["apiKey"], "test-key")
        self.assertIn("apiKey=test-key", session.calls[1]["url"])

    def test_pagination_overwrites_api_key_in_next_url(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [{"ticker": "AAPL", "locale": "us", "type": "CS"}],
                        "next_url": "https://api.massive.com/v3/reference/tickers?cursor=abc&apiKey=old-key",
                    }
                ),
                FakeResponse({"status": "OK", "results": [{"ticker": "MSFT", "locale": "us", "type": "CS"}]}),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        rows = source.list_active_tickers()

        self.assertEqual([item["ticker"] for item in rows], ["AAPL", "MSFT"])
        self.assertIn("apiKey=test-key", session.calls[1]["url"])
        self.assertNotIn("old-key", session.calls[1]["url"])
        self.assertEqual(session.calls[1]["url"].count("apiKey="), 1)

    def test_list_active_tickers_filters_us_locale_and_allowed_type(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {"ticker": "AAPL", "locale": "us", "type": "CS"},
                            {"ticker": "7203", "locale": "global", "type": "CS"},
                            {"ticker": "TESTP", "locale": "us", "type": "PFD"},
                        ],
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        rows = source.list_active_tickers(allowed_types=["CS", "ETF"])

        self.assertEqual([item["ticker"] for item in rows], ["AAPL"])

    def test_get_security_info_retries_with_historical_date(self):
        session = FakeSession(
            [
                FakeResponse({}, status_code=404),
                FakeResponse(
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "AKRO",
                            "active": True,
                            "name": "Akero Therapeutics, Inc.",
                            "primary_exchange": "XNAS",
                            "currency_name": "usd",
                            "locale": "us",
                            "type": "CS",
                            "list_date": "2019-06-20",
                        },
                    }
                ),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        payload = source.get_security_info("akro", fallback_date=date(2025, 12, 8))

        self.assertEqual(payload["symbol"], "akro")
        self.assertEqual(payload["current_symbol"], "akro")
        self.assertEqual(payload["name"], "Akero Therapeutics, Inc.")
        self.assertFalse(payload["is_active"])
        self.assertEqual(session.calls[1]["params"]["date"], "2025-12-08")

    def test_retry_exhaustion_masks_message_and_severs_exception_chain(self):
        session = FakeSession(
            [
                RequestsConnectionError(
                    "HTTPSConnectionPool: /v2/aggs/ticker/AAPL?adjusted=false&apiKey=plain-secret-key refused"
                ),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session, max_retries=0, retry_backoff_seconds=0)

        with self.assertRaises(RuntimeError) as ctx:
            source._request_json(path="/v2/aggs/ticker/AAPL/range/1/day/2020-01-01/2020-01-02")

        exc = ctx.exception
        self.assertIn("apiKey=***", str(exc))
        self.assertNotIn("plain-secret-key", str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertTrue(exc.__suppress_context__)
        rendered = "".join(traceback.format_exception(exc))
        self.assertNotIn("plain-secret-key", rendered)

    def test_generic_request_exception_is_masked_on_rethrow(self):
        session = FakeSession([RequestException("stream broken apiKey=other-secret-key end")])
        source = MassiveSource(DummyRateLimiter(), session=session, max_retries=0, retry_backoff_seconds=0)

        with self.assertRaises(RuntimeError) as ctx:
            source._request_json(path="/v3/reference/tickers")

        self.assertNotIn("other-secret-key", "".join(traceback.format_exception(ctx.exception)))
        self.assertIn("apiKey=***", str(ctx.exception))

    def test_generic_request_exception_is_retried(self):
        session = FakeSession(
            [
                RequestException("chunked encoding broke"),
                FakeResponse({"status": "OK", "results": []}),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session, max_retries=1, retry_backoff_seconds=0)

        payload = source._request_json(path="/v3/reference/tickers")

        self.assertEqual(payload["status"], "OK")
        self.assertEqual(len(session.calls), 2)

    def test_http_error_logs_masked_response_text(self):
        session = FakeSession(
            [FakeResponse({"error": "unauthorized apiKey=leaky-secret-key"}, status_code=401)]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)
        records = []
        sink_id = logger.add(lambda message: records.append(str(message)), level="ERROR")
        try:
            with self.assertRaises(HTTPError):
                source._request_json(path="/v3/reference/tickers")
        finally:
            logger.remove(sink_id)

        joined = "".join(records)
        self.assertIn("apiKey=***", joined)
        self.assertNotIn("leaky-secret-key", joined)

    def test_request_json_retries_on_transient_connection_error(self):
        session = FakeSession(
            [
                RequestsConnectionError("connection aborted"),
                FakeResponse(
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "MDIA",
                            "active": True,
                            "name": "Mediaco Holding Inc.",
                            "primary_exchange": "XNAS",
                            "currency_name": "usd",
                            "locale": "us",
                            "type": "CS",
                        },
                    }
                ),
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session, max_retries=1, retry_backoff_seconds=0)

        payload = source.get_security_info("mdia")

        self.assertEqual(payload["symbol"], "mdia")
        self.assertEqual(payload["name"], "Mediaco Holding Inc.")
        self.assertEqual(len(session.calls), 2)

    def test_get_historical_data_normalizes_fractional_volume(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": 1577941200000,
                                "o": 10,
                                "h": 11,
                                "l": 9,
                                "c": 10.5,
                                "v": 25933.6,
                                "vw": 10.25,
                                "n": 321,
                            }
                        ],
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        df = source.get_historical_data("aapl", start="2020-01-02", end="2020-01-02")

        row = df.iloc[0]
        self.assertEqual(row["Volume"], 25934)
        self.assertEqual(row["trade_count"], 321)
        self.assertNotIn("turnover", df.columns)

    def test_get_historical_data_drops_out_of_range_bigint_fields(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": 1577941200000,
                                "o": 10,
                                "h": 11,
                                "l": 9,
                                "c": 10.5,
                                "v": 2**70,
                                "vw": 10.25,
                                "n": 2**70,
                            }
                        ],
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        df = source.get_historical_data("aapl", start="2020-01-02", end="2020-01-02")

        row = df.iloc[0]
        self.assertIsNone(row["Volume"])
        self.assertIsNone(row["trade_count"])
        self.assertNotIn("turnover", df.columns)

    def test_get_open_close_data_returns_session_fields(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "symbol": "AAPL",
                        "from": "2026-03-13",
                        "open": 210.0,
                        "high": 212.0,
                        "low": 209.0,
                        "close": 211.5,
                        "volume": 1000000,
                        "preMarket": 209.8,
                        "afterHours": 211.9,
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        payload = source.get_open_close_data("aapl", "2026-03-13")

        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["preMarket"], 209.8)
        self.assertEqual(payload["afterHours"], 211.9)

    def test_get_dividends_batch_includes_extended_fields(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "ticker": "AAPL",
                                "id": "div-1",
                                "ex_dividend_date": "2026-02-10",
                                "declaration_date": "2026-01-31",
                                "record_date": "2026-02-11",
                                "pay_date": "2026-02-15",
                                "cash_amount": 0.26,
                                "currency": "USD",
                                "frequency": 4,
                                "distribution_type": "recurring",
                                "historical_adjustment_factor": 0.9987,
                                "split_adjusted_cash_amount": 0.26,
                            }
                        ],
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        rows = source.get_dividends_batch(["aapl"])

        self.assertEqual(rows[0]["source_event_id"], "div-1")
        self.assertEqual(rows[0]["distribution_type"], "recurring")
        self.assertEqual(rows[0]["historical_adjustment_factor"], 0.9987)
        self.assertEqual(rows[0]["split_adjusted_cash_amount"], 0.26)

    def test_get_splits_batch_includes_extended_fields(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "ticker": "AAPL",
                                "id": "split-1",
                                "execution_date": "2026-02-10",
                                "split_to": 2,
                                "split_from": 1,
                                "adjustment_type": "forward_split",
                                "historical_adjustment_factor": 0.5,
                            }
                        ],
                    }
                )
            ]
        )
        source = MassiveSource(DummyRateLimiter(), session=session)

        rows = source.get_splits_batch(["aapl"])

        self.assertEqual(rows[0]["source_event_id"], "split-1")
        self.assertEqual(rows[0]["adjustment_type"], "forward_split")
        self.assertEqual(rows[0]["historical_adjustment_factor"], 0.5)


if __name__ == "__main__":
    unittest.main()
