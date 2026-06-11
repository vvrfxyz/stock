import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace

from data_sources.massive_source import MassiveSource
from scripts.update_massive_details import get_massive_reference_fallback_date


class DummyRateLimiter:
    def acquire_key(self):
        return "test-key"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)
        self.headers = {}

    def raise_for_status(self):
        return None

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
        return self.responses.pop(0)


class MassiveDetailsFallbackTests(unittest.TestCase):
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

        result = source.get_security_info("akro", fallback_date=date(2025, 12, 8))

        self.assertTrue(session.calls[0]["url"].endswith("/v3/reference/tickers/AKRO"))
        self.assertEqual(session.calls[1]["params"]["date"], "2025-12-08")
        self.assertEqual(result["symbol"], "akro")
        self.assertEqual(result["name"], "Akero Therapeutics, Inc.")
        self.assertFalse(result["is_active"])

    def test_get_massive_reference_fallback_date_prefers_latest_price_date(self):
        security = SimpleNamespace(
            price_data_latest_date=date(2026, 1, 7),
            info_last_updated_at=datetime(2025, 6, 27, tzinfo=timezone.utc),
            list_date=date(2004, 9, 2),
        )

        fallback_date = get_massive_reference_fallback_date(security)

        self.assertEqual(fallback_date, date(2026, 1, 7))


if __name__ == "__main__":
    unittest.main()
