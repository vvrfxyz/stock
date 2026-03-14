import unittest
from datetime import date

from requests import HTTPError

from data_sources.massive_source import MassiveSource


class DummyRateLimiter:
    def acquire_key(self):
        return "test-key"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(response=self)

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self.responses.pop(0)


class MassiveSourceTests(unittest.TestCase):
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

        rows = source.list_active_tickers(allowed_types=["CS", "ETF", "ADRC"])

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
        self.assertEqual(payload["name"], "Akero Therapeutics, Inc.")
        self.assertFalse(payload["is_active"])
        self.assertEqual(session.calls[1]["params"]["date"], "2025-12-08")

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
        self.assertEqual(float(row["turnover"]), 265823.5)


if __name__ == "__main__":
    unittest.main()
