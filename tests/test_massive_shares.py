import unittest
from datetime import date

from scripts.update_massive_shares import _attach_float_fields, _extract_total_shares


class MassiveSharesTests(unittest.TestCase):
    def test_extract_total_shares_prefers_share_class_over_weighted(self):
        overview = {
            "share_class_shares_outstanding": 314112458,
            "weighted_shares_outstanding": 333752708,
        }
        self.assertEqual(_extract_total_shares(overview), 314112458)

    def test_extract_total_shares_falls_back_to_weighted(self):
        overview = {
            "share_class_shares_outstanding": None,
            "weighted_shares_outstanding": 333752708,
        }
        self.assertEqual(_extract_total_shares(overview), 333752708)


class AttachFloatFieldsTests(unittest.TestCase):
    def test_uses_latest_float_effective_on_or_before_filing_date(self):
        rows = [
            {"security_id": 1, "filing_date": date(2025, 6, 30), "float_shares": None, "free_float_percent": None},
        ]
        floats_by_symbol = {
            "aapl": [
                {"effective_date": date(2025, 3, 1), "free_float": 100, "free_float_percent": 50},
                {"effective_date": date(2025, 6, 1), "free_float": 120, "free_float_percent": 60},
                {"effective_date": date(2025, 9, 1), "free_float": 140, "free_float_percent": 70},
            ]
        }

        matched = _attach_float_fields(rows, floats_by_symbol, {1: "aapl"})

        self.assertEqual(matched, 1)
        self.assertEqual(rows[0]["float_shares"], 120)
        self.assertEqual(rows[0]["free_float_percent"], 60)

    def test_does_not_backfill_future_float_into_historical_snapshot(self):
        rows = [
            {"security_id": 1, "filing_date": date(2024, 12, 31), "float_shares": None, "free_float_percent": None},
        ]
        floats_by_symbol = {
            "aapl": [
                {"effective_date": date(2025, 9, 1), "free_float": 140, "free_float_percent": 70},
            ]
        }

        matched = _attach_float_fields(rows, floats_by_symbol, {1: "aapl"})

        self.assertEqual(matched, 0)
        self.assertIsNone(rows[0]["float_shares"])
        self.assertIsNone(rows[0]["free_float_percent"])


if __name__ == "__main__":
    unittest.main()
