import unittest
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from polygon.exceptions import BadResponse

from data_sources.polygon_source import PolygonSource
from scripts.update_details_from_polygon import get_polygon_reference_fallback_date


class DummyRateLimiter:
    def acquire_key(self):
        return "test-key"


class RecordingClient:
    def __init__(self, details):
        self.details = details
        self.calls = []

    def get_ticker_details(self, symbol, date=None):
        self.calls.append((symbol, date))
        if date is None:
            raise BadResponse('{"status":"NOT_FOUND","request_id":"abc123"}')
        return self.details


class PolygonDetailsFallbackTests(unittest.TestCase):
    def test_get_security_info_retries_with_historical_date(self):
        details = SimpleNamespace(
            active=True,
            name="Akero Therapeutics, Inc.",
            primary_exchange="XNAS",
            currency_name="USD",
            locale="us",
            type="CS",
            list_date="2019-06-20",
            delisted_utc=None,
        )
        client = RecordingClient(details)
        source = PolygonSource(DummyRateLimiter())
        source._get_client = Mock(return_value=client)

        result = source.get_security_info("akro", fallback_date=date(2025, 12, 8))

        self.assertEqual(client.calls, [("AKRO", None), ("AKRO", "2025-12-08")])
        self.assertEqual(result["symbol"], "akro")
        self.assertEqual(result["name"], "Akero Therapeutics, Inc.")
        self.assertFalse(result["is_active"])

    def test_get_polygon_reference_fallback_date_prefers_latest_price_date(self):
        security = SimpleNamespace(
            price_data_latest_date=date(2026, 1, 7),
            info_last_updated_at=datetime(2025, 6, 27, tzinfo=timezone.utc),
            list_date=date(2004, 9, 2),
        )

        fallback_date = get_polygon_reference_fallback_date(security)

        self.assertEqual(fallback_date, date(2026, 1, 7))


if __name__ == "__main__":
    unittest.main()
